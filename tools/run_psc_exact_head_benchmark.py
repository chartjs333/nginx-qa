"""Run the frozen PSC-A05/A16/A17 workload on one exact clean Git head.

The benchmark is intentionally slow and sequential.  It accepts only a new,
explicit, isolated run root; never discovers fixtures; never deletes or garbage
collects durable archives or blobs; and treats any attempted access beneath a
lexical or device-equivalent forbidden root as a terminal error.  Internal
exclusive-create, temporary-file cleanup, and atomic control-file replacement
remain permitted storage-protocol operations; their final footprint and absence
of leftover temporary files are measured.  The output is
deterministic canonical JSON (apart from the measurements it records) with an
embedded payload digest and a detached exact-file digest.

This is qualification tooling, not a production migration or runtime launcher.
It enables PAS1 authoritative mode only inside the supplied isolated run root.
"""

from __future__ import annotations

import argparse
import base64
import builtins
import ctypes
import dataclasses
import datetime as dt
import hashlib
import io
import json
import ntpath
import os
import platform
import re
import stat
import subprocess
import sys
import time
import tracemalloc
from pathlib import Path
from typing import Any, Mapping, Sequence


REPOSITORY_ROOT = Path(os.path.abspath(__file__)).parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT))

_CANDIDATE_MODULES_LOADED = False
_CANDIDATE_NATIVE_WRAPPERS_INSTALLED = False
_ACTIVE_ACCESS_GUARD = None


def _load_candidate_modules() -> None:
    """Import candidate runtime code only after access guards are active."""

    global _CANDIDATE_MODULES_LOADED
    global ArchiveOccurrence
    global PromptArchiveIncremental
    global response_id_v1
    global ExternalLatestCoordinator
    global PromptArchiveRuntime
    global PromptArchiveRuntimeConfig
    global PromptArchiveRuntimeRegistry
    global canonical_json_bytes_v1
    global CandidateIncrementalModule
    global CandidateOccurrenceStoreModule
    global CandidateStorageModule
    if _CANDIDATE_MODULES_LOADED:
        return
    from nginx_qa.prompt_archive_incremental import (
        ArchiveOccurrence as _ArchiveOccurrence,
        PromptArchiveIncremental as _PromptArchiveIncremental,
        response_id_v1 as _response_id_v1,
    )
    from nginx_qa.prompt_archive_latest import (
        ExternalLatestCoordinator as _ExternalLatestCoordinator,
    )
    from nginx_qa.prompt_archive_runtime import (
        PromptArchiveRuntime as _PromptArchiveRuntime,
        PromptArchiveRuntimeConfig as _PromptArchiveRuntimeConfig,
        PromptArchiveRuntimeRegistry as _PromptArchiveRuntimeRegistry,
    )
    from nginx_qa.prompt_archive_storage import (
        canonical_json_bytes_v1 as _canonical_json_bytes_v1,
    )
    import nginx_qa.prompt_archive_incremental as _incremental_module
    import nginx_qa.prompt_archive_occurrence_store as _occurrence_store_module
    import nginx_qa.prompt_archive_storage as _storage_module

    ArchiveOccurrence = _ArchiveOccurrence
    PromptArchiveIncremental = _PromptArchiveIncremental
    response_id_v1 = _response_id_v1
    ExternalLatestCoordinator = _ExternalLatestCoordinator
    PromptArchiveRuntime = _PromptArchiveRuntime
    PromptArchiveRuntimeConfig = _PromptArchiveRuntimeConfig
    PromptArchiveRuntimeRegistry = _PromptArchiveRuntimeRegistry
    canonical_json_bytes_v1 = _canonical_json_bytes_v1
    CandidateIncrementalModule = _incremental_module
    CandidateOccurrenceStoreModule = _occurrence_store_module
    CandidateStorageModule = _storage_module
    _CANDIDATE_MODULES_LOADED = True


PROJECT_ID = "9000"
MAIN_SPRINT = "sprint-psc-a05-a16-a17-append"
MIDDLE_SPRINT = "sprint-psc-a05-middle-ordinal"
OUTPUT_NAME = "psc-a05-a16-a17-exact-head.json"
OUTPUT_DIGEST_NAME = OUTPUT_NAME + ".sha256"
RUNNER_PATH = "tools/run_psc_exact_head_benchmark.py"
PREDECESSOR_EVIDENCE_PATH = (
    "docs/evidence/prompt-storage-compaction-v1/"
    "psc-006-a05-a16-a17-benchmark.raw.json.base64"
)
PREDECESSOR_DECODED_SHA256 = (
    "d036665e1512640091d0d82e9d47becedecd3dbb59d67e681fd9e66af93603d1"
)
PREDECESSOR_DECODED_BYTES = 16_163
BUILTIN_FORBIDDEN_ROOTS = (("live_prompt", r"D:\Prompt"),)
REQUIRED_FORBIDDEN_ROOT_LABELS = frozenset(
    {"live_prompt", "service_runtime", "service_prompt", "service_state", "service_root"}
)
ROOT_LABEL_RE = re.compile(r"^[a-z][a-z0-9_]{0,31}$", re.ASCII)
LOCAL_DRIVE_PATH_RE = re.compile(r"^[A-Za-z]:[\\/]", re.ASCII)
RESERVED_WINDOWS_COMPONENT_RE = re.compile(
    r"^(?:CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(?:\..*)?$", re.ASCII | re.IGNORECASE
)
HEX_40 = re.compile(r"^[0-9a-f]{40}$", re.ASCII)
PATH_AUDIT_EVENT_PATH_INDICES = {
    "open": (0,),
    "os.listdir": (0,),
    "os.scandir": (0,),
    "os.stat": (0,),
    "os.remove": (0,),
    "os.rename": (0, 1),
    "os.replace": (0, 1),
    "os.rmdir": (0,),
    "os.mkdir": (0,),
    "os.chdir": (0,),
    "os.chmod": (0,),
    "os.utime": (0,),
    "os.link": (0, 1),
    "os.symlink": (0, 1),
    "os.truncate": (0,),
}
PATH_AUDIT_EVENT_DIR_FD_INDICES = {
    "os.remove": (1,),
    "os.rename": (2, 3),
    "os.rmdir": (1,),
    "os.mkdir": (2,),
    "os.chmod": (2,),
    "os.utime": (3,),
    "os.link": (2, 3),
    "os.symlink": (2,),
}
FROZEN_INPUTS = {
    "append-0065-baseline": (
        333_496,
        "b9dc308a4b8f56334f3845c84b2aa68cdab40d654ef5c471f151d575a426d99d",
    ),
    "append-0066-pure": (
        338_364,
        "db221fea9d75e0e3c89a30ec34643eaaa0e76ee7323f6e22e4435c737fc4ea3f",
    ),
    "append-0067-alias-move": (
        343_232,
        "88c47b55afbe0ad05bd0edb80bd35391df0430fbbc307e0c00ce75f3792ce86c",
    ),
    "append-0068-pure": (
        348_100,
        "46829511fa756ec335076f1e252632d51519e6ce89986e51a0d03a53b02c6e21",
    ),
    "middle-0065-baseline": (
        316_643,
        "7b7381a9842854c62456bad6b8860cfe4b045dcd9b25fecd9a55e6717b0815b3",
    ),
    "middle-0066-insert-at-32": (
        321_105,
        "58b75968022191d913273776ad6dcb902bde6b4acee146b13c9a7f1717a3da64",
    ),
    "middle-0065-delete-at-32": (
        316_643,
        "ae97f0fab548fdeb8c1b3945ac7257af8c80fd425e8866111718adb6df26171d",
    ),
}
FROZEN_WORKLOAD_MANIFEST_SHA256 = (
    "d33a5f95407e68c4db1c69d1111cb43594377db9e095fc03977f262855659ac1"
)
# Frozen qualification bounds derived with margin from the committed predecessor
# evidence.  They make the append assertion quantitative: an implementation that
# rewrites the roughly 15,000 middle-case objects cannot pass merely because its
# three append samples happen to have similar cost.
FROZEN_APPEND_MAX_PUT_ATTEMPTS = 1_024
FROZEN_APPEND_MAX_CAS_BYTES_PER_LOGICAL_DELTA = 24.0
FROZEN_APPEND_MAX_DURABLE_BYTES_PER_LOGICAL_DELTA = 64.0
FROZEN_APPEND_MAX_ALLOCATION_BYTES_PER_LOGICAL_DELTA = 1_024.0
FROZEN_APPEND_MAX_WRITE_API_PAYLOAD_BYTES_PER_LOGICAL_DELTA = 512.0
FROZEN_MIDDLE_TO_APPEND_MIN_PUT_ATTEMPT_RATIO = 10.0


class BenchmarkFailure(RuntimeError):
    """Payload-free, stable fail-closed benchmark error."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def seal_evidence(document: Mapping[str, Any]) -> dict[str, Any]:
    """Return a copy with a digest over the canonical unsealed payload."""

    if "payload_sha256" in document:
        raise BenchmarkFailure("BENCHMARK_EVIDENCE_ALREADY_SEALED")
    result = dict(document)
    result["payload_sha256"] = _sha256(_canonical_json(result))
    return result


def verify_sealed_evidence_bytes(raw: bytes) -> dict[str, Any]:
    if not raw.endswith(b"\n") or raw.count(b"\n") != 1:
        raise BenchmarkFailure("BENCHMARK_EVIDENCE_INTEGRITY_FAILED")
    try:
        document = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise BenchmarkFailure("BENCHMARK_EVIDENCE_INTEGRITY_FAILED") from None
    if type(document) is not dict or type(document.get("payload_sha256")) is not str:
        raise BenchmarkFailure("BENCHMARK_EVIDENCE_INTEGRITY_FAILED")
    expected = document.pop("payload_sha256")
    if expected != _sha256(_canonical_json(document)):
        raise BenchmarkFailure("BENCHMARK_EVIDENCE_INTEGRITY_FAILED")
    document["payload_sha256"] = expected
    if _canonical_json(document) + b"\n" != raw:
        raise BenchmarkFailure("BENCHMARK_EVIDENCE_INTEGRITY_FAILED")
    return document


def _git(*arguments: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(REPOSITORY_ROOT), *arguments],
        input="",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        text=True,
        encoding="utf-8",
    )
    if completed.returncode != 0:
        raise BenchmarkFailure("BENCHMARK_GIT_PRECONDITION_FAILED")
    return completed.stdout.strip()


def _git_bytes(*arguments: str) -> bytes:
    completed = subprocess.run(
        ["git", "-C", str(REPOSITORY_ROOT), *arguments],
        input=b"",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if completed.returncode != 0:
        raise BenchmarkFailure("BENCHMARK_GIT_PRECONDITION_FAILED")
    return completed.stdout


def _candidate_snapshot() -> dict[str, Any]:
    status = _git("status", "--porcelain=v1", "--untracked-files=all")
    return {
        "commit": _git("rev-parse", "HEAD"),
        "tree": _git("rev-parse", "HEAD^{tree}"),
        "branch": _git("branch", "--show-current"),
        "clean": status == "",
        "status_sha256": _sha256(status.encode("utf-8")),
        "runner_git_blob": _git("rev-parse", f"HEAD:{RUNNER_PATH}"),
        "predecessor_evidence_git_blob": _git(
            "rev-parse", f"HEAD:{PREDECESSOR_EVIDENCE_PATH}"
        ),
    }


def _validate_candidate(
    snapshot: Mapping[str, Any], expected_head: str, expected_tree: str
) -> None:
    if (
        snapshot.get("commit") != expected_head
        or snapshot.get("tree") != expected_tree
        or snapshot.get("clean") is not True
    ):
        raise BenchmarkFailure("BENCHMARK_EXACT_HEAD_PRECONDITION_FAILED")


def _decode_predecessor_evidence() -> dict[str, Any]:
    carrier = _git_bytes("show", f"HEAD:{PREDECESSOR_EVIDENCE_PATH}")
    try:
        compact = b"".join(carrier.split())
        decoded = base64.b64decode(compact, validate=True)
        document = json.loads(decoded)
    except (ValueError, TypeError, json.JSONDecodeError):
        raise BenchmarkFailure("BENCHMARK_PREDECESSOR_EVIDENCE_INVALID") from None
    if (
        len(decoded) != PREDECESSOR_DECODED_BYTES
        or _sha256(decoded) != PREDECESSOR_DECODED_SHA256
        or type(document) is not dict
    ):
        raise BenchmarkFailure("BENCHMARK_PREDECESSOR_EVIDENCE_INVALID")
    transitions = {
        transition.get("label"): (
            transition.get("logical_input_bytes"),
            transition.get("logical_sha256"),
        )
        for group in (
            document.get("append_series", {}).get("transitions", []),
            document.get("middle_insert_delete", {}).get("transitions", []),
        )
        for transition in group
        if type(transition) is dict
    }
    if transitions != FROZEN_INPUTS:
        raise BenchmarkFailure("BENCHMARK_PREDECESSOR_WORKLOAD_MISMATCH")
    predecessor_sequence = [
        {
            "label": transition.get("label"),
            "change_kind": transition.get("change_kind"),
            "record_count": transition.get("record_count"),
            "raw_byte_size": transition.get("logical_input_bytes"),
            "raw_sha256": transition.get("logical_sha256"),
        }
        for group in (
            document.get("append_series", {}).get("transitions", []),
            document.get("middle_insert_delete", {}).get("transitions", []),
        )
        for transition in group
        if type(transition) is dict
    ]
    current_sequence = [
        {
            key: item[key]
            for key in (
                "label",
                "change_kind",
                "record_count",
                "raw_byte_size",
                "raw_sha256",
            )
        }
        for item in frozen_workload_manifest()["transitions"]
    ]
    if predecessor_sequence != current_sequence:
        raise BenchmarkFailure("BENCHMARK_PREDECESSOR_WORKLOAD_MISMATCH")
    return document


def _lexical_windows_path(value: str | os.PathLike[str]) -> str:
    try:
        raw = os.fsdecode(os.fspath(value))
    except (TypeError, ValueError, UnicodeError):
        raise BenchmarkFailure("BENCHMARK_PATH_INVALID") from None
    if not raw or "\x00" in raw:
        raise BenchmarkFailure("BENCHMARK_PATH_INVALID")
    return ntpath.normcase(ntpath.abspath(raw)).rstrip("\\")


def _strict_local_windows_path(value: str | os.PathLike[str]) -> str:
    try:
        raw = os.fsdecode(os.fspath(value))
    except (TypeError, ValueError, UnicodeError):
        raise BenchmarkFailure("BENCHMARK_PATH_INVALID") from None
    if (
        not raw
        or "\x00" in raw
        or not LOCAL_DRIVE_PATH_RE.match(raw)
        or raw.startswith(("\\\\", "\\?\\", "\\.\\"))
    ):
        raise BenchmarkFailure("BENCHMARK_PATH_ALIAS_UNSUPPORTED")
    drive, tail = ntpath.splitdrive(raw)
    if len(drive) != 2 or drive[1] != ":" or not tail.startswith(("\\", "/")):
        raise BenchmarkFailure("BENCHMARK_PATH_ALIAS_UNSUPPORTED")
    for component in re.split(r"[\\/]+", tail):
        if not component:
            continue
        if (
            component.endswith((" ", "."))
            or ":" in component
            or "~" in component
            or RESERVED_WINDOWS_COMPONENT_RE.fullmatch(component) is not None
        ):
            raise BenchmarkFailure("BENCHMARK_PATH_ALIAS_UNSUPPORTED")
    normalized = ntpath.normcase(ntpath.normpath(raw))
    return normalized if normalized.endswith(":\\") else normalized.rstrip("\\")


def _query_dos_device(drive: str) -> str:
    if os.name != "nt":
        raise BenchmarkFailure("BENCHMARK_WINDOWS_REQUIRED")
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    query = kernel32.QueryDosDeviceW
    query.argtypes = (wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD)
    query.restype = wintypes.DWORD
    buffer = ctypes.create_unicode_buffer(32_768)
    result = query(drive.upper(), buffer, len(buffer))
    if result == 0:
        raise BenchmarkFailure("BENCHMARK_PATH_DEVICE_QUERY_FAILED")
    return buffer.value


def _device_lexical_windows_path(
    value: str | os.PathLike[str],
    *,
    device_query=_query_dos_device,
    _depth: int = 0,
) -> str:
    if _depth > 8:
        raise BenchmarkFailure("BENCHMARK_PATH_ALIAS_UNSUPPORTED")
    local = _strict_local_windows_path(value)
    drive, tail = ntpath.splitdrive(local)
    mapping = device_query(drive)
    mapping_folded = mapping.casefold()
    if any(
        marker in mapping_folded
        for marker in ("\\device\\mup", "lanmanredirector", "webdavredirector")
    ):
        raise BenchmarkFailure("BENCHMARK_PATH_ALIAS_UNSUPPORTED")
    if mapping.startswith("\\??\\"):
        target = mapping[4:]
        if LOCAL_DRIVE_PATH_RE.match(target):
            return _device_lexical_windows_path(
                ntpath.join(target, tail.lstrip("\\/")),
                device_query=device_query,
                _depth=_depth + 1,
            )
        raise BenchmarkFailure("BENCHMARK_PATH_ALIAS_UNSUPPORTED")
    if not mapping_folded.startswith("\\device\\"):
        raise BenchmarkFailure("BENCHMARK_PATH_ALIAS_UNSUPPORTED")
    return ntpath.normcase(mapping.rstrip("\\") + "\\" + tail.lstrip("\\/"))


def _under(candidate: str, root: str) -> bool:
    return candidate == root or candidate.startswith(root + "\\")


def paths_overlap(first: str | os.PathLike[str], second: str | os.PathLike[str]) -> bool:
    one = _lexical_windows_path(first)
    two = _lexical_windows_path(second)
    return _under(one, two) or _under(two, one)


def parse_forbidden_roots(
    values: Sequence[str], *, device_query=_query_dos_device
) -> tuple[dict[str, str], ...]:
    parsed: dict[str, str] = {
        label: _strict_local_windows_path(path)
        for label, path in BUILTIN_FORBIDDEN_ROOTS
    }
    for value in values:
        if type(value) is not str or "=" not in value:
            raise BenchmarkFailure("BENCHMARK_FORBIDDEN_ROOT_INVALID")
        label, raw_path = value.split("=", 1)
        if (
            ROOT_LABEL_RE.fullmatch(label) is None
            or label in parsed
        ):
            raise BenchmarkFailure("BENCHMARK_FORBIDDEN_ROOT_INVALID")
        parsed[label] = _strict_local_windows_path(raw_path)
    if not REQUIRED_FORBIDDEN_ROOT_LABELS.issubset(parsed):
        raise BenchmarkFailure("BENCHMARK_FORBIDDEN_ROOT_SET_INCOMPLETE")
    return tuple(
        {
            "label": label,
            "lexical_path": path,
            "device_path": _device_lexical_windows_path(
                path, device_query=device_query
            ),
        }
        for label, path in sorted(parsed.items())
    )


def _assert_existing_chain_not_redirected(path: Path) -> None:
    local = _strict_local_windows_path(path)
    drive, tail = ntpath.splitdrive(local)
    current = Path(drive + "\\")
    chain = [current]
    for component in tuple(part for part in tail.split("\\") if part):
        current = current / component
        chain.append(current)
    for index, item in enumerate(chain):
        try:
            metadata = os.lstat(item)
        except FileNotFoundError:
            if index == 0:
                raise BenchmarkFailure(
                    "BENCHMARK_RUN_ROOT_PARENT_UNAVAILABLE"
                ) from None
            return
        except OSError:
            raise BenchmarkFailure("BENCHMARK_RUN_ROOT_PARENT_UNAVAILABLE") from None
        if stat.S_ISLNK(metadata.st_mode) or bool(
            getattr(metadata, "st_file_attributes", 0) & 0x400
        ):
            raise BenchmarkFailure("BENCHMARK_RUN_ROOT_REDIRECTED")


def _forms_overlap(first: Mapping[str, str], second: Mapping[str, str]) -> bool:
    return any(
        _under(first[key], second[key]) or _under(second[key], first[key])
        for key in ("lexical_path", "device_path")
    )


def _path_forms(
    value: str | os.PathLike[str], *, device_query=_query_dos_device
) -> dict[str, str]:
    lexical = _strict_local_windows_path(value)
    return {
        "lexical_path": lexical,
        "device_path": _device_lexical_windows_path(
            lexical, device_query=device_query
        ),
    }


def _preflight_path_isolation(
    run_root: Path,
    forbidden: Sequence[Mapping[str, str]],
    *,
    device_query=_query_dos_device,
) -> dict[str, dict[str, str]]:
    run = _path_forms(run_root, device_query=device_query)
    repository = _path_forms(REPOSITORY_ROOT, device_query=device_query)
    for item in forbidden:
        if _forms_overlap(repository, item):
            raise BenchmarkFailure("BENCHMARK_REPOSITORY_OVERLAPS_FORBIDDEN")
        if _forms_overlap(run, item):
            raise BenchmarkFailure("BENCHMARK_RUN_ROOT_OVERLAPS_FORBIDDEN")
    if _forms_overlap(run, repository):
        raise BenchmarkFailure("BENCHMARK_RUN_ROOT_OVERLAPS_REPOSITORY")
    return {"run_root": run, "repository": repository}


def _prepare_run_root(
    run_root: Path, isolation: Mapping[str, Mapping[str, str]]
) -> Path:
    if _strict_local_windows_path(run_root) != isolation["run_root"]["lexical_path"]:
        raise BenchmarkFailure("BENCHMARK_PATH_PRECONDITION_CHANGED")
    # Never resolve through an alias.  Preflight compared both drive and device
    # forms; this walk then rejects each existing reparse point root-to-leaf.
    _assert_existing_chain_not_redirected(run_root.parent)
    try:
        os.lstat(run_root)
    except FileNotFoundError:
        pass
    except OSError:
        raise BenchmarkFailure("BENCHMARK_RUN_ROOT_PARENT_UNAVAILABLE") from None
    else:
        raise BenchmarkFailure("BENCHMARK_RUN_ROOT_ALREADY_EXISTS")
    try:
        run_root.mkdir(parents=True, exist_ok=False)
    except OSError:
        raise BenchmarkFailure("BENCHMARK_RUN_ROOT_CREATE_FAILED") from None
    _assert_existing_chain_not_redirected(run_root)
    return run_root


def _allocated_bytes(path: Path, metadata: os.stat_result) -> int:
    blocks = getattr(metadata, "st_blocks", None)
    if type(blocks) is int:
        return blocks * 512
    if os.name != "nt":
        return metadata.st_size
    _native_guard_check("benchmark.FileStandardInfo", path)
    from ctypes import wintypes

    class FILE_STANDARD_INFO(ctypes.Structure):
        _fields_ = (
            ("AllocationSize", ctypes.c_longlong),
            ("EndOfFile", ctypes.c_longlong),
            ("NumberOfLinks", wintypes.DWORD),
            ("DeletePending", wintypes.BOOL),
            ("Directory", wintypes.BOOL),
        )

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    create_file = kernel32.CreateFileW
    create_file.argtypes = (
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    )
    create_file.restype = wintypes.HANDLE
    get_info = kernel32.GetFileInformationByHandleEx
    get_info.argtypes = (
        wintypes.HANDLE,
        ctypes.c_int,
        wintypes.LPVOID,
        wintypes.DWORD,
    )
    get_info.restype = wintypes.BOOL
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = (wintypes.HANDLE,)
    close_handle.restype = wintypes.BOOL
    handle = create_file(
        str(path),
        0x0080,  # FILE_READ_ATTRIBUTES
        0x00000001 | 0x00000002 | 0x00000004,  # share read/write/delete
        None,
        3,  # OPEN_EXISTING
        0x02000000,  # FILE_FLAG_BACKUP_SEMANTICS
        None,
    )
    invalid = ctypes.c_void_p(-1).value
    if handle == invalid:
        raise BenchmarkFailure("BENCHMARK_ALLOCATION_QUERY_FAILED")
    info = FILE_STANDARD_INFO()
    try:
        if not get_info(handle, 1, ctypes.byref(info), ctypes.sizeof(info)):
            raise BenchmarkFailure("BENCHMARK_ALLOCATION_QUERY_FAILED")
    finally:
        close_handle(handle)
    return int(info.AllocationSize)


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            while True:
                block = stream.read(1024 * 1024)
                if not block:
                    break
                digest.update(block)
    except OSError:
        raise BenchmarkFailure("BENCHMARK_INVENTORY_FAILED") from None
    return digest.hexdigest()


def _inventory_common(metadata: os.stat_result, kind: str) -> dict[str, Any]:
    return {
        "kind": kind,
        "mode": stat.S_IMODE(metadata.st_mode),
        "size": metadata.st_size,
        "mtime_ns": metadata.st_mtime_ns,
        "ctime_ns": metadata.st_ctime_ns,
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
        "file_attributes": getattr(metadata, "st_file_attributes", 0),
        "reparse_tag": getattr(metadata, "st_reparse_tag", 0),
    }


def _reject_redirected_inventory_entry(metadata: os.stat_result) -> None:
    if stat.S_ISLNK(metadata.st_mode) or bool(
        getattr(metadata, "st_file_attributes", 0) & 0x400
    ):
        raise BenchmarkFailure("BENCHMARK_FIXTURE_REDIRECTED")


def _inventory(roots: Mapping[str, Path]) -> dict[str, Any]:
    entries: dict[str, dict[str, Any]] = {}
    try:
        for label, root in sorted(roots.items()):
            try:
                root_before = os.lstat(root)
            except FileNotFoundError:
                continue
            _reject_redirected_inventory_entry(root_before)
            if not stat.S_ISDIR(root_before.st_mode):
                raise BenchmarkFailure("BENCHMARK_FIXTURE_TYPE_INVALID")
            root_entry = _inventory_common(root_before, "directory")
            entries[f"{label}/."] = root_entry
            for path in sorted(root.rglob("*"), key=lambda item: str(item).casefold()):
                metadata = os.lstat(path)
                _reject_redirected_inventory_entry(metadata)
                relative = path.relative_to(root).as_posix()
                key = f"{label}/{relative}"
                if stat.S_ISDIR(metadata.st_mode):
                    entries[key] = _inventory_common(metadata, "directory")
                elif stat.S_ISREG(metadata.st_mode):
                    entries[key] = {
                        **_inventory_common(metadata, "file"),
                        "allocated": _allocated_bytes(path, metadata),
                        "sha256": _hash_file(path),
                    }
                else:
                    raise BenchmarkFailure("BENCHMARK_FIXTURE_TYPE_INVALID")
            root_after = os.lstat(root)
            _reject_redirected_inventory_entry(root_after)
            if _inventory_common(root_after, "directory") != root_entry:
                raise BenchmarkFailure("BENCHMARK_INVENTORY_ROOT_CHANGED")
    except BenchmarkFailure:
        raise
    except OSError:
        raise BenchmarkFailure("BENCHMARK_INVENTORY_FAILED") from None
    temporary = [
        name
        for name, item in entries.items()
        if item["kind"] == "file"
        if ".tmp" in Path(name).name.lower() or Path(name).suffix.lower() == ".tmp"
    ]
    files = [item for item in entries.values() if item["kind"] == "file"]
    return {
        "entries": entries,
        "summary": {
            "file_count": len(files),
            "directory_count": len(entries) - len(files),
            "content_bytes": sum(item["size"] for item in files),
            "allocated_bytes": sum(item["allocated"] for item in files),
            "inventory_sha256": _sha256(_canonical_json(entries)),
            "temporary_file_count": len(temporary),
        },
    }


def _inventory_delta(before: Mapping[str, Any], after: Mapping[str, Any]) -> dict[str, Any]:
    previous = before["entries"]
    current = after["entries"]
    added = sorted(set(current) - set(previous))
    removed = sorted(set(previous) - set(current))
    changed = sorted(
        key for key in set(current) & set(previous) if current[key] != previous[key]
    )
    touched_files = [
        key
        for key in added + changed
        if current[key].get("kind") == "file"
    ]
    return {
        "added_path_count": len(added),
        "changed_path_count": len(changed),
        "removed_path_count": len(removed),
        "removed_path_name_sha256": [_sha256(name.encode("utf-8")) for name in removed],
        "final_touched_content_footprint_bytes": sum(
            current[key]["size"] for key in touched_files
        ),
        "final_touched_allocation_footprint_bytes": sum(
            current[key]["allocated"] for key in touched_files
        ),
        "storage_content_growth_bytes": (
            after["summary"]["content_bytes"] - before["summary"]["content_bytes"]
        ),
        "storage_allocated_growth_bytes": (
            after["summary"]["allocated_bytes"]
            - before["summary"]["allocated_bytes"]
        ),
    }


def full_inventory_equal(first: Mapping[str, Any], second: Mapping[str, Any]) -> bool:
    """Compare every file, directory, metadata field, and aggregate exactly."""

    return first == second


def _percentile(values: Sequence[int], quantile: float) -> int:
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int((len(ordered) - 1) * quantile + 0.5)))
    return ordered[index]


def _entropy(label: str, byte_count: int) -> str:
    parts: list[str] = []
    counter = 0
    size = 0
    while size < byte_count:
        part = hashlib.sha256(f"{label}:{counter}".encode()).hexdigest()
        parts.append(part)
        size += len(part)
        counter += 1
    return "".join(parts)[:byte_count]


def _nested_value(label: str, payload_bytes: int, depth: int) -> dict[str, Any]:
    value: dict[str, Any] = {
        "ordinal_token": label,
        "payload": _entropy(label, payload_bytes),
    }
    for level in reversed(range(depth)):
        value = {f"future_level_{level:02d}": value}
    return value


def _checkpoint(index: int) -> dict[str, Any]:
    return {
        "checkpoint_id": f"checkpoint-{index:04d}",
        "snapshot": {
            "revision": index,
            "execution": _nested_value(f"checkpoint-{index:04d}", 4096, 12),
        },
    }


def _event(index: int) -> dict[str, Any]:
    payload = _entropy(f"event-{index:04d}", 256)
    return {
        "event_id": f"event-{index:04d}",
        "kind": "checkpoint_appended",
        "payload": payload,
        "payload_sha256": _sha256(payload.encode()),
    }


CHECKPOINTS = tuple(_checkpoint(index) for index in range(70))
EVENTS = tuple(_event(index) for index in range(70))
ALIAS_VALUE = {
    "kind": "unknown-future-moved-alias",
    "schema_hint": "not-on-the-promotion-allowlist",
    "body": _nested_value("moved-alias", 16_384, 16),
}


def _encoded(document: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(document, ensure_ascii=False, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def _main_document(count: int, alias_location: str) -> dict[str, Any]:
    assignment = {
        "agent_id": "psc-benchmark-agent",
        "execution_audit": {
            "revision": count,
            "checkpoints": list(CHECKPOINTS[:count]),
        },
    }
    project_state = {
        "recent_activity": list(EVENTS[:count]),
        "status": "active",
    }
    if alias_location == "project_state":
        project_state["future_moved_alias"] = ALIAS_VALUE
    elif alias_location == "assignment":
        assignment["future_moved_alias"] = ALIAS_VALUE
    else:
        raise BenchmarkFailure("BENCHMARK_WORKLOAD_INVALID")
    return {
        "assignment": assignment,
        "project_state": project_state,
        "active_task": {"known": True, "message": "PSC isolated qualification"},
    }


def _middle_document(checkpoints: Sequence[Mapping[str, Any]], revision: int) -> dict[str, Any]:
    return {
        "assignment": {
            "agent_id": "psc-middle-case-agent",
            "execution_audit": {
                "revision": revision,
                "checkpoints": list(checkpoints),
            },
        },
        "project_state": {"recent_activity": list(EVENTS[:65]), "status": "active"},
        "active_task": {
            "known": True,
            "message": "PSC isolated middle ordinal case",
        },
    }


def frozen_workload() -> tuple[dict[str, Any], ...]:
    base_middle = CHECKPOINTS[:65]
    inserted_middle = (*base_middle[:32], _checkpoint(9000), *base_middle[32:])
    definitions = (
        (
            "append-0065-baseline",
            "initial_long_series",
            65,
            MAIN_SPRINT,
            _encoded(_main_document(65, "project_state")),
        ),
        (
            "append-0066-pure",
            "right_append",
            66,
            MAIN_SPRINT,
            _encoded(_main_document(66, "project_state")),
        ),
        (
            "append-0067-alias-move",
            "right_append_plus_unknown_alias_move",
            67,
            MAIN_SPRINT,
            _encoded(_main_document(67, "assignment")),
        ),
        (
            "append-0068-pure",
            "right_append",
            68,
            MAIN_SPRINT,
            _encoded(_main_document(68, "assignment")),
        ),
        (
            "middle-0065-baseline",
            "initial_middle_series",
            100,
            MIDDLE_SPRINT,
            _encoded(_middle_document(base_middle, 100)),
        ),
        (
            "middle-0066-insert-at-32",
            "middle_insert_explicit_o_n",
            101,
            MIDDLE_SPRINT,
            _encoded(_middle_document(inserted_middle, 101)),
        ),
        (
            "middle-0065-delete-at-32",
            "middle_delete_explicit_o_n",
            102,
            MIDDLE_SPRINT,
            _encoded(_middle_document(base_middle, 102)),
        ),
    )
    return tuple(
        {
            "label": label,
            "change_kind": change_kind,
            "revision": revision,
            "sprint_id": sprint_id,
            "raw": raw,
            "record_count": (
                revision
                if sprint_id == MAIN_SPRINT
                else len(json.loads(raw)["assignment"]["execution_audit"]["checkpoints"])
            ),
        }
        for label, change_kind, revision, sprint_id, raw in definitions
    )


def validate_frozen_workload() -> None:
    observed = {
        item["label"]: (len(item["raw"]), _sha256(item["raw"]))
        for item in frozen_workload()
    }
    if observed != FROZEN_INPUTS:
        raise BenchmarkFailure("BENCHMARK_FROZEN_WORKLOAD_MISMATCH")
    if _sha256(_canonical_json(frozen_workload_manifest())) != (
        FROZEN_WORKLOAD_MANIFEST_SHA256
    ):
        raise BenchmarkFailure("BENCHMARK_FROZEN_WORKLOAD_MISMATCH")


def _timestamp(revision: int, offset: int = 0) -> str:
    value = dt.datetime(2026, 10, 21, 1, 0, tzinfo=dt.timezone.utc) + dt.timedelta(
        seconds=revision * 3 + offset
    )
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def _pas1_frame(value: str | None) -> bytes:
    if value is None:
        return b"\xff"
    encoded = value.encode("utf-8", errors="strict")
    return b"\x00" + len(encoded).to_bytes(8, "big") + encoded


def _frozen_response_id(
    project_id: str,
    sprint_id: str,
    response_kind: str,
    occurrence_kind: str,
    occurrence_id: str,
) -> str:
    preimage = b"nginx-qa-pas1-id-v1\x00" + _pas1_frame("response-id")
    for name, value in (
        ("project_id", project_id),
        ("sprint_id", sprint_id),
        ("response_kind", response_kind),
        ("occurrence_kind", occurrence_kind),
        ("occurrence_id", occurrence_id),
    ):
        preimage += _pas1_frame(name) + _pas1_frame(value)
    return "parresp1-" + _sha256(preimage)


def _occurrence_fields(
    commit: str, sprint_id: str, label: str, revision: int
) -> dict[str, Any]:
    occurrence_id = f"psc-benchmark-{label}-{revision:04d}"
    return {
        "project_id": PROJECT_ID,
        "sprint_id": sprint_id,
        "response_id": _frozen_response_id(
            PROJECT_ID,
            sprint_id,
            "transition-response",
            "transition",
            occurrence_id,
        ),
        "response_kind": "transition-response",
        "occurrence_kind": "transition",
        "occurrence_id": occurrence_id,
        "transition_id": occurrence_id,
        "source": {
            "source_kind": "native_runtime",
            "source_commit": commit,
            "assignment_id": "assignment-psc-isolated-benchmark",
        },
        "authorization": {
            "auth_model_version": "prompt-archive-authorization-v1",
            "principal_kind": "synthetic_test",
            "principal_id": "psc-qualification-benchmark",
            "binding_id": None,
            "scope_source": "synthetic_test",
            "effective_scope_revision": None,
            "effective_scope_sha256": None,
            "decision_occurrence_id": f"decision-{label}-{revision:04d}",
        },
        "created_at": _timestamp(revision),
        "verified_at": _timestamp(revision, 1),
        "state_revision": revision,
        "source_response_key": None,
        "source_byte_size": None,
        "source_sha256": None,
        "source_metadata_projection": None,
        "source_metadata_receipt": None,
    }


def _occurrence(commit: str, sprint_id: str, label: str, revision: int):
    if not _CANDIDATE_MODULES_LOADED:
        raise BenchmarkFailure("BENCHMARK_CANDIDATE_IMPORT_MISSING")
    fields = _occurrence_fields(commit, sprint_id, label, revision)
    occurrence = ArchiveOccurrence(**fields)
    candidate_response_id = response_id_v1(
        PROJECT_ID,
        sprint_id,
        "transition-response",
        "transition",
        fields["occurrence_id"],
    )
    if candidate_response_id != fields["response_id"]:
        raise BenchmarkFailure("BENCHMARK_FROZEN_ID_ALGORITHM_MISMATCH")
    return occurrence


def frozen_workload_manifest() -> dict[str, Any]:
    transitions: list[dict[str, Any]] = []
    for item in frozen_workload():
        occurrence = _occurrence_fields(
            "0" * 40,
            item["sprint_id"],
            item["label"],
            item["revision"],
        )
        occurrence["source"]["source_commit"] = "<exact-candidate-commit>"
        transitions.append(
            {
                "label": item["label"],
                "change_kind": item["change_kind"],
                "sprint_id": item["sprint_id"],
                "revision": item["revision"],
                "record_count": item["record_count"],
                "raw_byte_size": len(item["raw"]),
                "raw_sha256": _sha256(item["raw"]),
                "occurrence": occurrence,
            }
        )
    return {
        "schema_version": 1,
        "kind": "psc_a05_a16_a17_frozen_workload_manifest",
        "transitions": transitions,
    }


class _CompatibilityPublisher:
    """Closed isolated adapter for the authoritative descriptor boundary."""

    def __init__(self, root: Path, runtime: PromptArchiveRuntime) -> None:
        root.mkdir(parents=True, exist_ok=True)
        self.root = root
        self.transition_path = root / "unprepared.json"
        self.coordinator = ExternalLatestCoordinator(
            root / "latest-response.json",
            project_id=PROJECT_ID,
            archive_owner_verifier=runtime.verify_authoritative_archive_owner,
        )

    @property
    def target_authority_id(self) -> str:
        return self.coordinator.target_authority_id

    def prepare_authoritative(
        self,
        *,
        descriptor_bytes: bytes,
        descriptor_ref: Mapping[str, Any],
        occurrence: ArchiveOccurrence,
        operation_id: str,
        published_at: str,
    ) -> Mapping[str, Any]:
        self.transition_path = self.root / (
            f"{occurrence.response_kind}-{occurrence.response_id}.json"
        )
        return self.coordinator.prepare_descriptor_intent(
            descriptor_bytes,
            transition_stub_path=self.transition_path,
            occurrence_id=occurrence.occurrence_id,
            operation_id=operation_id,
            published_at=published_at,
            source_response_id=occurrence.response_id,
            source_sprint_id=occurrence.sprint_id,
            source_revision=occurrence.state_revision,
            descriptor_ref=descriptor_ref,
        )

    def publish_authoritative(
        self, compatibility_publication: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        descriptor = compatibility_publication["descriptor"]["document"]
        raw = canonical_json_bytes_v1(descriptor, maximum=64 * 1024)
        result = self.coordinator.publish_prepared_descriptor(
            raw,
            prepared_intent=compatibility_publication["latest_intent"],
            transition_stub_path=self.transition_path,
            archive_owner=compatibility_publication["archive_owner"],
            expected_target_authority_id=compatibility_publication[
                "target_authority_id"
            ],
        )
        return dataclasses.asdict(result)

    def verify_authoritative(
        self,
        compatibility_publication: Mapping[str, Any],
        publication: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        result = self.coordinator.verify_prepared_publication(
            prepared_intent=compatibility_publication["latest_intent"],
            publication=publication,
            archive_owner=compatibility_publication["archive_owner"],
        )
        return dataclasses.asdict(result)


def _make_runtime(
    base: Path, protected: Path
) -> tuple[
    PromptArchiveRuntimeRegistry,
    PromptArchiveRuntime,
    _CompatibilityPublisher,
    dict[str, Path],
]:
    archive_root = base / "archive"
    prompt_root = base / "compatibility"
    config = PromptArchiveRuntimeConfig(
        mode="authoritative",
        root=archive_root,
        max_hydrated_bytes=8 * 1024 * 1024,
        operator_gate="prompt-archive-authoritative-v1",
        protected_roots=(protected,),
    )
    registry = PromptArchiveRuntimeRegistry(config)
    runtime = registry.for_project(PROJECT_ID)
    publisher = _CompatibilityPublisher(prompt_root, runtime)
    return registry, runtime, publisher, {
        "archive": archive_root,
        "compatibility": prompt_root,
    }


def _alias_value_root(
    runtime: PromptArchiveRuntime, sprint_id: str, archive_id: str
) -> str:
    selected = runtime.lookup_archive(sprint_id=sprint_id, archive_id=archive_id)
    manifest = runtime.read_manifest(selected)
    archive = PromptArchiveIncremental(runtime.storage, PROJECT_ID, sprint_id)
    sequence = archive._receipt_from_root(
        manifest["roots"]["current_fields"], "current_fields"
    )
    matches: list[str] = []
    for leaf in archive.sequence_leaves(sequence):
        raw = archive._hydrate_record(leaf.ref, expected_record_type="current_field")
        try:
            value = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if value == ALIAS_VALUE:
            record = json.loads(runtime.storage.read_blob(leaf.ref).decode("utf-8"))
            matches.append(record["payload"]["value_root"]["digest"])
    if len(matches) != 1:
        raise BenchmarkFailure("BENCHMARK_ALIAS_PROOF_INVALID")
    return matches[0]


def _run_transition(
    *,
    commit: str,
    runtime: PromptArchiveRuntime,
    publisher: _CompatibilityPublisher,
    roots: Mapping[str, Path],
    previous_inventory: Mapping[str, Any],
    item: Mapping[str, Any],
    logical_change_bytes: int,
    inspect_alias: bool,
    write_telemetry: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    raw = item["raw"]
    write_current = tracemalloc.get_traced_memory()[0]
    tracemalloc.reset_peak()
    write_started = time.perf_counter_ns()
    write_telemetry["by_transition"][item["label"]] = {
        "write_calls": 0,
        "payload_bytes": 0,
    }
    write_telemetry["active_transition"] = item["label"]
    try:
        with runtime.storage.account_cas_writes() as accounting:
            publication = runtime.publish_authoritative(
                raw,
                _occurrence(
                    commit,
                    item["sprint_id"],
                    item["label"],
                    item["revision"],
                ),
                compatibility_publisher=publisher,
            )
    finally:
        write_telemetry["active_transition"] = None
    write_latency = time.perf_counter_ns() - write_started
    _write_after, write_peak = tracemalloc.get_traced_memory()
    cas = dataclasses.asdict(accounting.snapshot())
    after_write = _inventory(roots)

    selected = runtime.lookup_archive(
        sprint_id=item["sprint_id"], archive_id=publication.archive_id
    )
    read_current = tracemalloc.get_traced_memory()[0]
    tracemalloc.reset_peak()
    read_started = time.perf_counter_ns()
    hydrated = runtime.hydrate_content(selected)
    read_latency = time.perf_counter_ns() - read_started
    _read_after, read_peak = tracemalloc.get_traced_memory()
    alias_root = (
        _alias_value_root(runtime, item["sprint_id"], publication.archive_id)
        if inspect_alias
        else None
    )
    after_read = _inventory(roots)

    exact = hydrated == raw and _sha256(hydrated) == _sha256(raw)
    nonmutating = after_write["entries"] == after_read["entries"]
    delta = _inventory_delta(previous_inventory, after_read)
    logical_delta = max(1, logical_change_bytes)
    result = {
        "label": item["label"],
        "change_kind": item["change_kind"],
        "record_count": item["record_count"],
        "logical_input_bytes": len(raw),
        "logical_change_bytes": logical_change_bytes,
        "logical_sha256": _sha256(raw),
        "archive_id": publication.archive_id,
        "exact_roundtrip": exact,
        "read_caused_no_filesystem_mutation": nonmutating,
        "cas": cas,
        "write_api": dict(write_telemetry["by_transition"][item["label"]]),
        "dedup_ratio": (
            cas["dedup_hits"] / cas["put_attempts"] if cas["put_attempts"] else 0.0
        ),
        "physical": {
            **delta,
            "after": after_read["summary"],
            "non_cas_final_touched_content_footprint_bytes": max(
                0,
                delta["final_touched_content_footprint_bytes"]
                - cas["new_stored_bytes"],
            ),
        },
        "amplification": {
            "cas_new_stored_per_logical_change": (
                cas["new_stored_bytes"] / logical_delta
            ),
            "final_touched_content_footprint_per_logical_change": (
                delta["final_touched_content_footprint_bytes"] / logical_delta
            ),
            "storage_growth_per_logical_change": (
                delta["storage_content_growth_bytes"] / logical_delta
            ),
        },
        "write_latency_ns": write_latency,
        "read_latency_ns": read_latency,
        "write_peak_tracemalloc_increment_bytes": max(0, write_peak - write_current),
        "read_peak_tracemalloc_increment_bytes": max(0, read_peak - read_current),
        "moved_alias_value_root_digest": alias_root,
    }
    if not exact or not nonmutating:
        raise BenchmarkFailure("BENCHMARK_ROUNDTRIP_INVARIANT_FAILED")
    if delta["removed_path_count"] or after_read["summary"]["temporary_file_count"]:
        raise BenchmarkFailure("BENCHMARK_APPEND_ONLY_INVARIANT_FAILED")
    if cas["new_stored_bytes"] > delta["final_touched_content_footprint_bytes"]:
        raise BenchmarkFailure("BENCHMARK_PHYSICAL_ACCOUNTING_INVALID")
    return after_read, result


def evaluate_a05_append_bounds(
    initial: Mapping[str, Any],
    append_records: Sequence[Mapping[str, Any]],
    middle_records: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, bool], dict[str, Any]]:
    """Evaluate frozen quantitative A05 bounds without timing assumptions."""

    append_attempts = [item["cas"]["put_attempts"] for item in append_records]
    append_cas = [item["cas"]["new_stored_bytes"] for item in append_records]
    append_footprints = [
        item["physical"]["final_touched_content_footprint_bytes"]
        for item in append_records
    ]
    append_allocations = [
        item["physical"]["final_touched_allocation_footprint_bytes"]
        for item in append_records
    ]
    append_write_payloads = [
        item["write_api"]["payload_bytes"] for item in append_records
    ]
    append_deltas = [max(1, item["logical_change_bytes"]) for item in append_records]
    middle_attempts = [item["cas"]["put_attempts"] for item in middle_records]
    if not append_attempts or not middle_attempts:
        raise BenchmarkFailure("BENCHMARK_A05_SAMPLE_SET_INVALID")
    max_append_attempts = max(append_attempts)
    minimum_middle_ratio = min(middle_attempts) / max(1, max_append_attempts)
    maximum_cas_ratio = max(
        value / delta for value, delta in zip(append_cas, append_deltas)
    )
    maximum_footprint_ratio = max(
        value / delta for value, delta in zip(append_footprints, append_deltas)
    )
    maximum_allocation_ratio = max(
        value / delta for value, delta in zip(append_allocations, append_deltas)
    )
    maximum_write_payload_ratio = max(
        value / delta for value, delta in zip(append_write_payloads, append_deltas)
    )
    checks = {
        "append_put_attempts_within_frozen_bound": (
            max_append_attempts <= FROZEN_APPEND_MAX_PUT_ATTEMPTS
        ),
        "append_cas_amplification_within_frozen_bound": (
            maximum_cas_ratio <= FROZEN_APPEND_MAX_CAS_BYTES_PER_LOGICAL_DELTA
        ),
        "append_final_touched_footprint_within_frozen_bound": (
            maximum_footprint_ratio
            <= FROZEN_APPEND_MAX_DURABLE_BYTES_PER_LOGICAL_DELTA
        ),
        "append_final_touched_allocation_within_frozen_bound": (
            maximum_allocation_ratio
            <= FROZEN_APPEND_MAX_ALLOCATION_BYTES_PER_LOGICAL_DELTA
        ),
        "append_write_api_payload_within_frozen_bound": (
            maximum_write_payload_ratio
            <= FROZEN_APPEND_MAX_WRITE_API_PAYLOAD_BYTES_PER_LOGICAL_DELTA
        ),
        "append_write_api_telemetry_nonzero": all(
            item["write_api"]["write_calls"] > 0
            and item["write_api"]["payload_bytes"] > 0
            for item in append_records
        ),
        "append_cas_below_initial": (
            max(append_cas) < initial["cas"]["new_stored_bytes"]
        ),
        "append_final_touched_footprint_below_initial": (
            max(append_footprints)
            < initial["physical"]["final_touched_content_footprint_bytes"]
        ),
        "middle_put_attempts_materially_exceed_append": (
            minimum_middle_ratio >= FROZEN_MIDDLE_TO_APPEND_MIN_PUT_ATTEMPT_RATIO
        ),
    }
    observations = {
        "append_max_put_attempts": max_append_attempts,
        "append_max_cas_bytes_per_logical_delta": maximum_cas_ratio,
        "append_max_final_touched_footprint_per_logical_delta": (
            maximum_footprint_ratio
        ),
        "append_max_final_touched_allocation_per_logical_delta": (
            maximum_allocation_ratio
        ),
        "append_max_write_api_payload_per_logical_delta": (
            maximum_write_payload_ratio
        ),
        "middle_minimum_to_append_max_put_attempt_ratio": minimum_middle_ratio,
        "bounds": {
            "append_max_put_attempts": FROZEN_APPEND_MAX_PUT_ATTEMPTS,
            "append_max_cas_bytes_per_logical_delta": (
                FROZEN_APPEND_MAX_CAS_BYTES_PER_LOGICAL_DELTA
            ),
            "append_max_final_touched_footprint_per_logical_delta": (
                FROZEN_APPEND_MAX_DURABLE_BYTES_PER_LOGICAL_DELTA
            ),
            "append_max_final_touched_allocation_per_logical_delta": (
                FROZEN_APPEND_MAX_ALLOCATION_BYTES_PER_LOGICAL_DELTA
            ),
            "append_max_write_api_payload_per_logical_delta": (
                FROZEN_APPEND_MAX_WRITE_API_PAYLOAD_BYTES_PER_LOGICAL_DELTA
            ),
            "middle_minimum_to_append_max_put_attempt_ratio": (
                FROZEN_MIDDLE_TO_APPEND_MIN_PUT_ATTEMPT_RATIO
            ),
        },
    }
    return checks, observations


def _create_protected_fixture(root: Path) -> None:
    nested = root / "nested"
    nested.mkdir(parents=True)
    (root / "sentinel.json").write_bytes(
        b'{"immutable":true,"line_endings":"CRLF\\r\\n"}\r\n'
    )
    (nested / "canary.bin").write_bytes(bytes(range(256)) * 8)
    (nested / "unicode.txt").write_text(
        "generated fixture: Без потерь\n", encoding="utf-8", newline=""
    )
    fixed_ns = 1_792_530_000_000_000_000
    for fixture_file in root.rglob("*"):
        if fixture_file.is_file():
            os.utime(fixture_file, ns=(fixed_ns, fixed_ns))


class _AccessGuard:
    """Deny lexical or device-equivalent roots before probes reach the OS.

    CPython does not emit audit events for every ``stat``/``lstat`` call, so
    the hook alone cannot prove PSC-A17.  The direct wrappers below close that
    gap for the Python filesystem primitives exercised by this benchmark;
    the hook independently covers built-in/``io.open`` and rename/remove
    operations.  No forbidden root is resolved or probed while installing the
    policy.
    """

    def __init__(
        self,
        *,
        forbidden: Sequence[Mapping[str, str]],
        protected: Path,
        audit: dict[str, Any],
        device_query=_query_dos_device,
    ) -> None:
        self.forbidden = tuple(
            (item["label"], dict(item)) for item in forbidden
        )
        self.device_query = device_query
        self.protected = _path_forms(protected, device_query=device_query)
        self.audit = audit

    def _candidate_forms(self, value: Any) -> dict[str, str]:
        try:
            raw = os.fsdecode(os.fspath(value))
        except (TypeError, ValueError, UnicodeError):
            raise BenchmarkFailure("BENCHMARK_PATH_ALIAS_UNSUPPORTED") from None
        if not raw or "\x00" in raw:
            raise BenchmarkFailure("BENCHMARK_PATH_ALIAS_UNSUPPORTED")
        if LOCAL_DRIVE_PATH_RE.match(raw):
            local = raw
        elif ntpath.isabs(raw) or raw.startswith(("\\", "/")):
            raise BenchmarkFailure("BENCHMARK_PATH_ALIAS_UNSUPPORTED")
        else:
            local = ntpath.join(os.getcwd(), raw)
        return _path_forms(local, device_query=self.device_query)

    def check(self, event: str, value: Any) -> None:
        if value is None:
            value = os.getcwd()
        if isinstance(value, int):
            descriptor_checks = self.audit["descriptor_checks_by_event"]
            descriptor_checks[event] = descriptor_checks.get(event, 0) + 1
            return
        try:
            candidate = self._candidate_forms(value)
        except BenchmarkFailure:
            self.audit["unsafe_alias_attempts"].append({"event": event})
            raise PermissionError("PSC benchmark denied unsafe path alias") from None
        checks = self.audit["guard_checks_by_event"]
        checks[event] = checks.get(event, 0) + 1
        for label, root in self.forbidden:
            if any(
                _under(candidate[key], root[key])
                for key in ("lexical_path", "device_path")
            ):
                self.audit["forbidden_attempts"].append(
                    {"event": event, "root_label": label}
                )
                raise PermissionError("PSC benchmark denied forbidden-root access")
        if any(
            _under(candidate[key], self.protected[key])
            for key in ("lexical_path", "device_path")
        ):
            if self.audit["phase"] == "benchmark":
                self.audit["protected_fixture_benchmark_attempts"].append(
                    {"event": event}
                )
                raise PermissionError("PSC benchmark denied protected-fixture access")
            if self.audit["phase"] in {
                "fixture_setup",
                "pre_inventory",
                "post_inventory",
            }:
                self.audit["authorized_fixture_inventory_events"] += 1

    def deny_unsupported_descriptor(self, event: str) -> None:
        self.audit["unsafe_alias_attempts"].append({"event": event})
        raise PermissionError("PSC benchmark denied unbound file descriptor")


def _native_guard_check(event: str, path: str | os.PathLike[str]) -> None:
    if _ACTIVE_ACCESS_GUARD is None:
        raise BenchmarkFailure("BENCHMARK_NATIVE_GUARD_MISSING")
    _ACTIVE_ACCESS_GUARD.check(event, path)
    checks = _ACTIVE_ACCESS_GUARD.audit["native_path_checks_by_entrypoint"]
    checks[event] = checks.get(event, 0) + 1


def _install_candidate_native_guards(audit: dict[str, Any]) -> None:
    """Guard the absolute native path entrypoints used by the PAS1 runtime.

    Its remaining NtCreateFile calls use validated single-leaf names relative
    to directory handles created by these guarded entrypoints.
    """

    global _CANDIDATE_NATIVE_WRAPPERS_INSTALLED
    if not _CANDIDATE_MODULES_LOADED:
        raise BenchmarkFailure("BENCHMARK_CANDIDATE_IMPORT_MISSING")
    entrypoints = (
        (
            CandidateStorageModule,
            "_windows_open_pinned_directory",
            "prompt_archive_storage.CreateFileW",
        ),
        (
            CandidateOccurrenceStoreModule,
            "_windows_open_pinned_directory",
            "prompt_archive_occurrence_store.CreateFileW",
        ),
        (
            CandidateIncrementalModule,
            "_allocated_size",
            "prompt_archive_incremental.GetCompressedFileSizeW",
        ),
    )
    audit["native_guard_entrypoints"] = [item[2] for item in entrypoints]
    if _CANDIDATE_NATIVE_WRAPPERS_INSTALLED:
        return
    for module, attribute, event in entrypoints:
        original = getattr(module, attribute)
        if attribute == "_allocated_size":

            def guarded_allocation(path, metadata, _original=original, _event=event):
                _native_guard_check(_event, path)
                return _original(path, metadata)

            setattr(module, attribute, guarded_allocation)
        else:

            def guarded_root_open(path, _original=original, _event=event):
                _native_guard_check(_event, path)
                return _original(path)

            setattr(module, attribute, guarded_root_open)
    _CANDIDATE_NATIVE_WRAPPERS_INSTALLED = True


class _WriteTelemetryStream:
    """Transparent stream proxy that counts successful write payload bytes."""

    def __init__(self, stream, record_write) -> None:
        self._stream = stream
        self._record_write = record_write

    def __getattr__(self, name: str):
        return getattr(self._stream, name)

    def __enter__(self):
        self._stream.__enter__()
        return self

    def __exit__(self, *arguments):
        return self._stream.__exit__(*arguments)

    def __iter__(self):
        return iter(self._stream)

    def write(self, payload):
        result = self._stream.write(payload)
        if isinstance(payload, str):
            byte_count = len(
                payload.encode(getattr(self._stream, "encoding", None) or "utf-8")
            )
        else:
            try:
                view = memoryview(payload)
                try:
                    byte_count = view.nbytes
                finally:
                    view.release()
            except (TypeError, ValueError, BufferError):
                byte_count = int(result) if isinstance(result, int) else 0
        self._record_write(byte_count)
        return result

    def writelines(self, lines) -> None:
        for line in lines:
            self.write(line)


def _install_access_guards(
    *,
    forbidden: Sequence[Mapping[str, str]],
    protected: Path,
    audit: dict[str, Any],
    device_query=_query_dos_device,
) -> _AccessGuard:
    global _ACTIVE_ACCESS_GUARD
    guard = _AccessGuard(
        forbidden=forbidden,
        protected=protected,
        audit=audit,
        device_query=device_query,
    )
    _ACTIVE_ACCESS_GUARD = guard

    def hook(event: str, arguments: tuple[Any, ...]) -> None:
        for path_argument in audit_path_arguments(event, arguments):
            guard.check(event, path_argument)
        for index in PATH_AUDIT_EVENT_DIR_FD_INDICES.get(event, ()):
            if index < len(arguments) and arguments[index] not in (None, -1):
                guard.deny_unsupported_descriptor(event)

    sys.addaudithook(hook)

    original_stat = os.stat
    original_lstat = os.lstat
    original_listdir = os.listdir
    original_scandir = os.scandir
    original_open = os.open
    original_write = os.write
    original_readlink = os.readlink
    original_access = os.access
    original_builtin_open = builtins.open
    original_io_open = io.open

    def record_write(byte_count: int) -> None:
        telemetry = audit["write_telemetry"]
        label = telemetry["active_transition"]
        if label is None:
            return
        record = telemetry["by_transition"].setdefault(
            label, {"write_calls": 0, "payload_bytes": 0}
        )
        record["write_calls"] += 1
        record["payload_bytes"] += max(0, int(byte_count))

    def wrap_writable(stream, arguments, keywords):
        mode = arguments[0] if arguments else keywords.get("mode", "r")
        if (
            isinstance(mode, str)
            and any(marker in mode for marker in ("w", "a", "x", "+"))
            and not isinstance(stream, _WriteTelemetryStream)
        ):
            return _WriteTelemetryStream(stream, record_write)
        return stream

    def guarded_stat(path, *arguments, **keywords):
        if keywords.get("dir_fd") is not None:
            guard.deny_unsupported_descriptor("os.stat")
        guard.check("os.stat", path)
        return original_stat(path, *arguments, **keywords)

    def guarded_lstat(path, *arguments, **keywords):
        if keywords.get("dir_fd") is not None:
            guard.deny_unsupported_descriptor("os.lstat")
        guard.check("os.lstat", path)
        return original_lstat(path, *arguments, **keywords)

    def guarded_listdir(path=None):
        guard.check("os.listdir", path)
        return original_listdir(path) if path is not None else original_listdir()

    def guarded_scandir(path=None):
        guard.check("os.scandir", path)
        return original_scandir(path) if path is not None else original_scandir()

    def guarded_open(path, flags, mode=0o777, *, dir_fd=None):
        if dir_fd is not None:
            guard.deny_unsupported_descriptor("os.open")
        guard.check("os.open", path)
        return original_open(path, flags, mode, dir_fd=dir_fd)

    def guarded_write(descriptor, payload):
        written = original_write(descriptor, payload)
        record_write(written)
        return written

    def guarded_readlink(path, *, dir_fd=None):
        if dir_fd is not None:
            guard.deny_unsupported_descriptor("os.readlink")
        guard.check("os.readlink", path)
        return original_readlink(path, dir_fd=dir_fd)

    def guarded_access(path, mode, *, dir_fd=None, effective_ids=False, follow_symlinks=True):
        if dir_fd is not None:
            guard.deny_unsupported_descriptor("os.access")
        guard.check("os.access", path)
        return original_access(
            path,
            mode,
            dir_fd=dir_fd,
            effective_ids=effective_ids,
            follow_symlinks=follow_symlinks,
        )

    def guarded_builtin_open(file, *arguments, **keywords):
        guard.check("open", file)
        return wrap_writable(
            original_builtin_open(file, *arguments, **keywords),
            arguments,
            keywords,
        )

    def guarded_io_open(file, *arguments, **keywords):
        guard.check("io.open", file)
        return wrap_writable(
            original_io_open(file, *arguments, **keywords),
            arguments,
            keywords,
        )

    os.stat = guarded_stat
    os.lstat = guarded_lstat
    os.listdir = guarded_listdir
    os.scandir = guarded_scandir
    os.open = guarded_open
    os.write = guarded_write
    os.readlink = guarded_readlink
    os.access = guarded_access
    builtins.open = guarded_builtin_open
    io.open = guarded_io_open
    return guard


def audit_path_arguments(event: str, arguments: Sequence[Any]) -> tuple[Any, ...]:
    """Extract only path-bearing fields from a CPython filesystem audit event."""

    return tuple(
        arguments[index]
        for index in PATH_AUDIT_EVENT_PATH_INDICES.get(event, ())
        if index < len(arguments)
    )


def _write_exclusive(path: Path, raw: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_BINARY"):
        flags |= os.O_BINARY
    try:
        descriptor = os.open(path, flags, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError:
        raise BenchmarkFailure("BENCHMARK_EVIDENCE_ALREADY_EXISTS") from None
    except OSError:
        raise BenchmarkFailure("BENCHMARK_EVIDENCE_WRITE_FAILED") from None


def _run(args: argparse.Namespace) -> dict[str, Any]:
    expected_head = args.expected_head.lower()
    expected_tree = args.expected_tree.lower()
    if HEX_40.fullmatch(expected_head) is None or HEX_40.fullmatch(expected_tree) is None:
        raise BenchmarkFailure("BENCHMARK_GIT_ID_INVALID")
    forbidden = parse_forbidden_roots(args.forbid_root)
    isolation = _preflight_path_isolation(args.run_root, forbidden)
    protected = args.run_root / "isolated-protected-fixture"
    audit: dict[str, Any] = {
        "phase": "guard_installation",
        "forbidden_attempts": [],
        "unsafe_alias_attempts": [],
        "protected_fixture_benchmark_attempts": [],
        "authorized_fixture_inventory_events": 0,
        "guard_checks_by_event": {},
        "descriptor_checks_by_event": {},
        "native_path_checks_by_entrypoint": {},
        "native_guard_entrypoints": [],
        "write_telemetry": {"active_transition": None, "by_transition": {}},
    }
    _install_access_guards(forbidden=forbidden, protected=protected, audit=audit)
    audit["phase"] = "candidate_precondition"
    _assert_existing_chain_not_redirected(REPOSITORY_ROOT)
    _assert_existing_chain_not_redirected(REPOSITORY_ROOT / "nginx_qa")
    before = _candidate_snapshot()
    _validate_candidate(before, expected_head, expected_tree)
    predecessor = _decode_predecessor_evidence()
    validate_frozen_workload()
    audit["phase"] = "candidate_import"
    _load_candidate_modules()
    _install_candidate_native_guards(audit)
    imported_snapshot = _candidate_snapshot()
    _validate_candidate(imported_snapshot, expected_head, expected_tree)
    if imported_snapshot != before:
        raise BenchmarkFailure("BENCHMARK_CANDIDATE_CHANGED")
    audit["phase"] = "run_root_setup"
    run_root = _prepare_run_root(args.run_root, isolation)
    audit["phase"] = "fixture_setup"
    _create_protected_fixture(protected)
    audit["phase"] = "pre_inventory"
    protected_pre = _inventory({"protected": protected})
    if protected_pre["summary"]["file_count"] != 3:
        raise BenchmarkFailure("BENCHMARK_PROTECTED_FIXTURE_INVALID")

    audit["phase"] = "fixture_setup"
    _main_registry, main_runtime, main_publisher, main_roots = _make_runtime(
        run_root / "append-case", protected
    )
    _middle_registry, middle_runtime, middle_publisher, middle_roots = _make_runtime(
        run_root / "middle-case", protected
    )
    main_inventory = _inventory(main_roots)
    middle_inventory = _inventory(middle_roots)

    started = time.perf_counter()
    audit["phase"] = "benchmark"
    tracemalloc.start(1)
    workload = frozen_workload()
    main_records: list[dict[str, Any]] = []
    middle_records: list[dict[str, Any]] = []
    alias_roots: list[str] = []
    prior_size = {MAIN_SPRINT: 0, MIDDLE_SPRINT: 0}
    try:
        for item in workload:
            sprint_id = item["sprint_id"]
            logical_change = (
                len(item["raw"])
                if not prior_size[sprint_id]
                else abs(len(item["raw"]) - prior_size[sprint_id])
            )
            if sprint_id == MAIN_SPRINT:
                main_inventory, record = _run_transition(
                    commit=expected_head,
                    runtime=main_runtime,
                    publisher=main_publisher,
                    roots=main_roots,
                    previous_inventory=main_inventory,
                    item=item,
                    logical_change_bytes=logical_change,
                    inspect_alias=True,
                    write_telemetry=audit["write_telemetry"],
                )
                main_records.append(record)
                alias_roots.append(record["moved_alias_value_root_digest"])
            else:
                middle_inventory, record = _run_transition(
                    commit=expected_head,
                    runtime=middle_runtime,
                    publisher=middle_publisher,
                    roots=middle_roots,
                    previous_inventory=middle_inventory,
                    item=item,
                    logical_change_bytes=logical_change,
                    inspect_alias=False,
                    write_telemetry=audit["write_telemetry"],
                )
                middle_records.append(record)
            prior_size[sprint_id] = len(item["raw"])
    finally:
        tracemalloc.stop()

    audit["phase"] = "post_inventory"
    protected_post = _inventory({"protected": protected})
    audit["phase"] = "candidate_postcondition"
    after = _candidate_snapshot()
    _validate_candidate(after, expected_head, expected_tree)
    if before != after:
        raise BenchmarkFailure("BENCHMARK_CANDIDATE_CHANGED")

    append_bound = main_records[1:]
    initial = main_records[0]
    insert_case, delete_case = middle_records[1:]
    append_attempts = [item["cas"]["put_attempts"] for item in append_bound]
    all_records = main_records + middle_records
    total_attempts = sum(item["cas"]["put_attempts"] for item in all_records)
    total_hits = sum(item["cas"]["dedup_hits"] for item in all_records)
    protected_equal = full_inventory_equal(protected_pre, protected_post)
    append_checks, append_observations = evaluate_a05_append_bounds(
        initial,
        append_bound,
        (insert_case, delete_case),
    )

    a05_checks = {
        "all_roundtrips_exact": all(item["exact_roundtrip"] for item in all_records),
        "all_reads_non_mutating": all(
            item["read_caused_no_filesystem_mutation"] for item in all_records
        ),
        "no_removed_paths": all(
            item["physical"]["removed_path_count"] == 0 for item in all_records
        ),
        "no_temporary_artifacts": all(
            item["physical"]["after"]["temporary_file_count"] == 0
            for item in all_records
        ),
        "unknown_moved_alias_value_root_reused": len(set(alias_roots)) == 1,
        **append_checks,
        "middle_cases_disclosed_separately": all(
            "explicit_o_n" in item["change_kind"]
            for item in (insert_case, delete_case)
        ),
    }
    a16_checks = {
        "logical_input_recorded": all(
            item["logical_input_bytes"] > 0 for item in all_records
        ),
        "all_physical_content_and_allocation_recorded": (
            main_inventory["summary"]["content_bytes"]
            + middle_inventory["summary"]["content_bytes"]
            > 0
            and main_inventory["summary"]["allocated_bytes"]
            + middle_inventory["summary"]["allocated_bytes"]
            > 0
        ),
        "dedup_recorded": total_attempts > 0 and total_hits > 0,
        "write_latency_recorded": all(
            item["write_latency_ns"] > 0 for item in all_records
        ),
        "read_latency_recorded": all(
            item["read_latency_ns"] > 0 for item in all_records
        ),
        "peak_memory_recorded": all(
            item["write_peak_tracemalloc_increment_bytes"] > 0
            and item["read_peak_tracemalloc_increment_bytes"] > 0
            for item in all_records
        ),
        "write_api_payload_and_call_count_recorded": all(
            item["write_api"]["write_calls"] > 0
            and item["write_api"]["payload_bytes"] > 0
            for item in all_records
        ),
        "cas_reconciled_inside_final_touched_footprint": all(
            item["cas"]["new_stored_bytes"]
            <= item["physical"]["final_touched_content_footprint_bytes"]
            for item in all_records
        ),
    }
    a17_checks = {
        "forbidden_root_attempt_count_zero": len(audit["forbidden_attempts"]) == 0,
        "unsafe_alias_attempt_count_zero": len(audit["unsafe_alias_attempts"]) == 0,
        "benchmark_protected_fixture_attempt_count_zero": (
            len(audit["protected_fixture_benchmark_attempts"]) == 0
        ),
        "protected_full_inventory_equal": protected_equal,
        "protected_path_size_allocation_mtime_sha_equal": protected_equal,
        "protected_fixture_has_exactly_three_files": (
            protected_pre["summary"]["file_count"] == 3
        ),
        "protected_fixture_root_entry_inventoried": (
            protected_pre["entries"].get("protected/.", {}).get("kind")
            == "directory"
        ),
        "protected_fixture_all_files_have_full_sha256": all(
            item.get("kind") != "file"
            or re.fullmatch(r"[0-9a-f]{64}", item.get("sha256", "")) is not None
            for item in protected_pre["entries"].values()
        ),
        "access_guard_health_events_observed": all(
            audit["guard_checks_by_event"].get(event, 0) > 0
            for event in ("os.stat", "os.lstat", "os.scandir", "io.open")
        ),
        "authorized_fixture_inventory_events_observed": (
            audit["authorized_fixture_inventory_events"] > 0
        ),
        "candidate_native_absolute_path_entrypoints_guarded": (
            len(audit["native_guard_entrypoints"]) == 3
        ),
        "candidate_native_guard_health_events_observed": (
            sum(
                count
                for event, count in audit[
                    "native_path_checks_by_entrypoint"
                ].items()
                if event.startswith("prompt_archive_")
            )
            > 0
        ),
    }
    acceptance = {
        "PSC-A05": {"pass": all(a05_checks.values()), "checks": a05_checks},
        "PSC-A16": {"pass": all(a16_checks.values()), "checks": a16_checks},
        "PSC-A17": {"pass": all(a17_checks.values()), "checks": a17_checks},
    }
    acceptance["overall_pass"] = all(
        acceptance[name]["pass"] for name in ("PSC-A05", "PSC-A16", "PSC-A17")
    )
    if acceptance["overall_pass"] is not True:
        raise BenchmarkFailure("BENCHMARK_MANDATORY_ACCEPTANCE_FAILED")

    final_content = (
        main_inventory["summary"]["content_bytes"]
        + middle_inventory["summary"]["content_bytes"]
    )
    final_allocated = (
        main_inventory["summary"]["allocated_bytes"]
        + middle_inventory["summary"]["allocated_bytes"]
    )
    forbidden_inventory = [
        {
            "label": item["label"],
            "lexical_path": item["lexical_path"],
            "device_path": item["device_path"],
            "lexical_path_sha256": _sha256(item["lexical_path"].encode("utf-8")),
            "device_path_sha256": _sha256(item["device_path"].encode("utf-8")),
        }
        for item in forbidden
    ]
    audit["phase"] = "complete"
    evidence = {
        "schema_version": 2,
        "kind": "psc_a05_a16_a17_exact_head_benchmark_evidence",
        "candidate": before,
        "predecessor_evidence": {
            "path": PREDECESSOR_EVIDENCE_PATH,
            "decoded_byte_size": PREDECESSOR_DECODED_BYTES,
            "decoded_sha256": PREDECESSOR_DECODED_SHA256,
            "candidate_commit": predecessor["candidate"]["commit"],
            "candidate_tree": predecessor["candidate"]["tree"],
            "role": "frozen workload and predecessor measurements only",
        },
        "workload_identity": {
            "schema_version": 1,
            "manifest_sha256": FROZEN_WORKLOAD_MANIFEST_SHA256,
            "transition_count": len(all_records),
            "binds_order_group_revision_occurrence_and_raw_input": True,
        },
        "execution_policy": {
            "isolated_new_run_root": True,
            "run_root_lexical_sha256": _sha256(
                _lexical_windows_path(run_root).encode("utf-8")
            ),
            "generated_fixture_only": True,
            "sequential": True,
            "timeout": None,
            "durable_archive_or_blob_delete": False,
            "storage_gc": False,
            "protocol_temp_cleanup_and_atomic_control_replace_allowed": True,
            "no_temporary_artifacts_postcondition_checked": True,
            "write_api_payload_bytes_measured_per_transition": True,
            "final_touched_footprint_distinct_from_write_api_bytes": True,
            "candidate_native_absolute_path_entrypoints_guarded": True,
            "services_called_or_restarted": False,
            "production_authoritative_mode_enabled": False,
            "isolated_authoritative_mode_only": True,
            "forbidden_roots": forbidden_inventory,
            "forbidden_root_values_published": True,
            "invocation": {
                "runner": RUNNER_PATH,
                "run_root": "<redacted:sha256-in-parent>",
                "expected_head": expected_head,
                "expected_tree": expected_tree,
                "forbidden_root_count": len(forbidden),
            },
        },
        "environment": {
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "filesystem_allocation_method": (
                "st_blocks*512 or Windows FileStandardInfo.AllocationSize; "
                "regular files only, directory allocation excluded"
            ),
            "peak_memory_method": (
                "tracemalloc one-frame incremental peak per timed operation"
            ),
        },
        "append_series": {
            "model": (
                "65 baseline -> pure append 66 -> append plus unknown alias "
                "move 67 -> pure append 68"
            ),
            "checkpoint_depth": 12,
            "unknown_alias_depth": 16,
            "unknown_alias_root_digests": alias_roots,
            "frozen_quantitative_bound": append_observations,
            "transitions": main_records,
        },
        "middle_insert_delete": {
            "classification": (
                "explicit V1 O(N) ordinal-index metadata cases; excluded from "
                "the append bound"
            ),
            "minimum_middle_to_maximum_append_put_attempt_ratio": (
                min(
                    insert_case["cas"]["put_attempts"],
                    delete_case["cas"]["put_attempts"],
                )
                / max(append_attempts)
            ),
            "transitions": middle_records,
        },
        "aggregate": {
            "transition_count": len(all_records),
            "logical_input_bytes_total": sum(
                item["logical_input_bytes"] for item in all_records
            ),
            "cas_put_attempts": total_attempts,
            "cas_new_object_count": sum(
                item["cas"]["new_object_count"] for item in all_records
            ),
            "cas_new_stored_bytes": sum(
                item["cas"]["new_stored_bytes"] for item in all_records
            ),
            "cas_dedup_hits": total_hits,
            "cas_dedup_ratio": total_hits / total_attempts,
            "final_touched_content_footprint_bytes_total": sum(
                item["physical"]["final_touched_content_footprint_bytes"]
                for item in all_records
            ),
            "write_api_payload_bytes_total": sum(
                item["write_api"]["payload_bytes"] for item in all_records
            ),
            "write_api_call_count_total": sum(
                item["write_api"]["write_calls"] for item in all_records
            ),
            "final_all_physical_content_bytes": final_content,
            "final_all_physical_allocated_bytes": final_allocated,
            "write_latency_ns_p50": _percentile(
                [item["write_latency_ns"] for item in all_records], 0.50
            ),
            "write_latency_ns_p95": _percentile(
                [item["write_latency_ns"] for item in all_records], 0.95
            ),
            "read_latency_ns_p50": _percentile(
                [item["read_latency_ns"] for item in all_records], 0.50
            ),
            "read_latency_ns_p95": _percentile(
                [item["read_latency_ns"] for item in all_records], 0.95
            ),
            "max_write_peak_tracemalloc_increment_bytes": max(
                item["write_peak_tracemalloc_increment_bytes"] for item in all_records
            ),
            "max_read_peak_tracemalloc_increment_bytes": max(
                item["read_peak_tracemalloc_increment_bytes"] for item in all_records
            ),
            "main_final_inventory": main_inventory["summary"],
            "middle_final_inventory": middle_inventory["summary"],
        },
        "forbidden_root_and_fixture_invariance": {
            "audit": audit,
            "protected_pre": protected_pre,
            "protected_post": protected_post,
            "note": (
                "Forbidden roots are lexical and drive-device deny values and "
                "are never opened, listed, statted, or resolved; no filesystem "
                "content or metadata beneath them is hashed. Their normalized "
                "path strings have identifying hashes. Only the generated "
                "three-file protected fixture is inventoried, including its "
                "directory entries."
            ),
        },
        "acceptance": acceptance,
        "duration_seconds": time.perf_counter() - started,
        "restrictions": {
            "live_data_read_or_written": False,
            "runtime_state_or_sqlite_mutated": False,
            "service_restart": False,
            "merge_or_deploy": False,
            "start_from_git_or_sprint_import": False,
            "generated_run_root_deleted": False,
        },
    }
    sealed = seal_evidence(evidence)
    raw = _canonical_json(sealed) + b"\n"
    digest = _sha256(raw)
    _write_exclusive(run_root / OUTPUT_NAME, raw)
    _write_exclusive(
        run_root / OUTPUT_DIGEST_NAME,
        f"{digest}  {OUTPUT_NAME}\n".encode("ascii"),
    )
    try:
        published_raw = (run_root / OUTPUT_NAME).read_bytes()
        published_digest = (run_root / OUTPUT_DIGEST_NAME).read_text(
            encoding="ascii"
        )
    except OSError:
        raise BenchmarkFailure("BENCHMARK_EVIDENCE_WRITE_FAILED") from None
    verify_sealed_evidence_bytes(published_raw)
    if published_raw != raw or published_digest != f"{digest}  {OUTPUT_NAME}\n":
        raise BenchmarkFailure("BENCHMARK_EVIDENCE_INTEGRITY_FAILED")
    return {
        "kind": "psc_exact_head_benchmark_receipt",
        "status": "PASS",
        "candidate_commit": expected_head,
        "candidate_tree": expected_tree,
        "evidence_file": OUTPUT_NAME,
        "evidence_sha256": digest,
        "payload_sha256": sealed["payload_sha256"],
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-root", required=True, type=Path)
    parser.add_argument("--expected-head", required=True)
    parser.add_argument("--expected-tree", required=True)
    parser.add_argument(
        "--forbid-root",
        action="append",
        default=[],
        help=(
            "Additional live/configured root that must remain a lexical and "
            "device-equivalent deny "
            "value only, as LABEL=ABSOLUTE_PATH. Required labels are "
            "service_runtime, service_prompt, service_state, and service_root; "
            "live_prompt=D:\\Prompt is built in. Additional labels are allowed."
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
        receipt = _run(args)
    except BenchmarkFailure as exc:
        print(
            _canonical_json(
                {
                    "schema_version": 1,
                    "kind": "psc_exact_head_benchmark_error",
                    "code": exc.code,
                }
            ).decode("utf-8"),
            file=sys.stderr,
        )
        return 2
    except Exception as exc:
        candidate_code = getattr(exc, "code", None)
        safe_code = (
            candidate_code
            if type(candidate_code) is str
            and re.fullmatch(r"PROMPT_ARCHIVE_[A-Z0-9_]+", candidate_code)
            else "BENCHMARK_FAILED_CLOSED"
        )
        print(
            _canonical_json(
                {
                    "schema_version": 1,
                    "kind": "psc_exact_head_benchmark_error",
                    "code": safe_code,
                }
            ).decode("utf-8"),
            file=sys.stderr,
        )
        return 2
    print(_canonical_json(receipt).decode("utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
