#!/usr/bin/env python3
"""Reproducible, fail-closed PSC-006 unittest qualification ledger.

This tool deliberately does not implement the long PSC-A05/A16/A17 benchmark.
It binds independently produced exact-head evidence during ``finalize``.

The public workflow is:

* ``collect`` imports the explicitly selected modules, selects only TestCase
  classes defined by each module, and freezes the sorted unique test IDs.
* ``plan`` produces deterministic sequential worker shards.  The expensive
  runtime-integration module is split into singleton workers.
* ``run`` executes those shards, preserving immutable start, result, and
  controller receipts so an interrupted run can resume without accepting a
  stale or failed attempt.
* ``finalize`` requires every mandatory worker and external evidence item to
  pass at the exact collected commit/tree.
* ``verify`` checks the canonical payload hash and detached exact-byte hash.

No command deletes an existing artifact.  Run roots must be outside the Git
workspace and outside every required named protected/live root.
"""

from __future__ import annotations

import argparse
import builtins
import codecs
import contextlib
import copy
import ctypes
import functools
import hashlib
import importlib
import importlib.metadata
import inspect
import io
import json
import ntpath
import os
import pathlib
import platform
import re
import secrets
import shutil
import stat
import subprocess
import sys
import threading
import time
import traceback
import unittest
from collections.abc import Iterable, Mapping, Sequence
from typing import Any


_ORIGINAL_LSTAT = os.lstat


SCHEMA_VERSION = 1
COLLECTION_KIND = "psc_qualification_collection"
PLAN_KIND = "psc_qualification_plan"
WORKER_KIND = "psc_qualification_worker_result"
START_KIND = "psc_qualification_worker_start"
CONTROLLER_KIND = "psc_qualification_controller_receipt"
ABANDONMENT_KIND = "psc_qualification_attempt_abandonment"
LEDGER_KIND = "psc_exact_head_qualification_ledger"
CANONICALIZATION = "python-json-sort-keys-utf8-v1"
RUNNER_RELATIVE_PATH = "tools/run_psc_qualification.py"
DEFAULT_SELECTOR_RELATIVE_PATH = (
    "orchestration/sprints/prompt-storage-compaction-v1/"
    "qualification-selectors-v1.json"
)
SAFE_PACKAGES = (
    "fastapi",
    "httpx",
    "jsonschema",
    "pydantic",
    "referencing",
    "starlette",
    "uvicorn",
)
SECRET_PATTERN = re.compile(
    r"(?i)(authorization|api[_-]?key|password|passwd|secret|token)"
    r"\s*[:=]\s*[^\s,;]{4,}"
)
HEX_40_OR_64 = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")
BUILTIN_PROTECTED_ROOTS = {"live_prompt": r"D:\Prompt"}
REQUIRED_PROTECTED_ROOT_LABELS = frozenset(
    {
        "live_prompt",
        "service_root",
        "service_state",
        "service_runtime",
        "service_prompt",
    }
)
ROOT_LABEL_RE = re.compile(r"^[a-z][a-z0-9_]{0,31}$", re.ASCII)
LOCAL_DRIVE_PATH_RE = re.compile(r"^[A-Za-z]:[\\/]", re.ASCII)
RESERVED_WINDOWS_COMPONENT_RE = re.compile(
    r"^(?:CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(?:\..*)?$",
    re.ASCII | re.IGNORECASE,
)
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
NATIVE_GUARD_ENTRYPOINTS = (
    (
        "nginx_qa.prompt_archive_storage",
        "_windows_open_pinned_directory",
        "prompt_archive_storage.CreateFileW",
    ),
    (
        "nginx_qa.prompt_archive_occurrence_store",
        "_windows_open_pinned_directory",
        "prompt_archive_occurrence_store.CreateFileW",
    ),
    (
        "nginx_qa.prompt_archive_migration",
        "_windows_open_pinned_directory",
        "prompt_archive_migration.CreateFileW",
    ),
    (
        "nginx_qa.prompt_archive_incremental",
        "_allocated_size",
        "prompt_archive_incremental.GetCompressedFileSizeW",
    ),
)


class QualificationError(RuntimeError):
    """Stable, non-secret qualification failure."""

    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(code if not detail else f"{code}: {detail}")
        self.code = code
        self.detail = detail


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def _canonical_json_bytes(value: Any) -> bytes:
    """Canonical JSON bytes without the ledger's trailing newline."""

    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _read_bytes(path: pathlib.Path) -> bytes:
    try:
        _assert_existing_chain_not_redirected(path)
        return path.read_bytes()
    except (OSError, ValueError) as exc:
        raise QualificationError("PSC_QUALIFICATION_ARTIFACT_UNREADABLE") from exc


def _read_json(path: pathlib.Path) -> dict[str, Any]:
    try:
        value = json.loads(_read_bytes(path).decode("utf-8"))
    except (UnicodeError, ValueError, json.JSONDecodeError) as exc:
        raise QualificationError("PSC_QUALIFICATION_JSON_INVALID") from exc
    if type(value) is not dict:
        raise QualificationError("PSC_QUALIFICATION_JSON_INVALID")
    return value


def _fsync_directory(path: pathlib.Path) -> None:
    if os.name == "nt":
        # Windows publication below uses MOVEFILE_WRITE_THROUGH, which flushes
        # the same-directory move before returning.
        return
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _publish_temp_no_replace(temp_path: pathlib.Path, final_path: pathlib.Path) -> None:
    """Atomically publish a complete same-directory temp without overwrite."""

    if os.name == "nt":
        from ctypes import wintypes

        move = ctypes.WinDLL("kernel32", use_last_error=True).MoveFileExW
        move.argtypes = (wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD)
        move.restype = wintypes.BOOL
        if not move(str(temp_path), str(final_path), 0x00000008):
            error = ctypes.get_last_error()
            if error in {80, 183}:
                raise FileExistsError(error, "destination exists", str(final_path))
            raise OSError(error, "MOVEFILE_WRITE_THROUGH failed", str(final_path))
        return
    os.link(temp_path, final_path, follow_symlinks=False)
    _fsync_directory(final_path.parent)
    os.unlink(temp_path)
    _fsync_directory(final_path.parent)


def _read_existing_immutable(path: pathlib.Path) -> bytes | None:
    try:
        metadata = os.lstat(path)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise QualificationError("PSC_QUALIFICATION_ARTIFACT_UNREADABLE") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or bool(getattr(metadata, "st_file_attributes", 0) & 0x400)
    ):
        raise QualificationError("PSC_QUALIFICATION_IMMUTABLE_ARTIFACT_CONFLICT")
    return _read_bytes(path)


def _immutable_exists(path: pathlib.Path) -> bool:
    return _read_existing_immutable(path) is not None


def _write_immutable(path: pathlib.Path, data: bytes) -> None:
    """Crash-safely publish one immutable artifact without partial finals."""

    _assert_existing_chain_not_redirected(path.parent)
    path.parent.mkdir(parents=True, exist_ok=True)
    _assert_existing_chain_not_redirected(path.parent)
    existing = _read_existing_immutable(path)
    if existing is not None:
        if existing == data:
            return
        raise QualificationError(
            "PSC_QUALIFICATION_IMMUTABLE_ARTIFACT_CONFLICT", path.name
        )
    temp_path = path.parent / (
        f".{path.name}.psc-owned-{os.getpid()}-{secrets.token_hex(16)}.tmp"
    )
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_BINARY"):
        flags |= os.O_BINARY
    descriptor: int | None = None
    published = False
    try:
        descriptor = os.open(temp_path, flags, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = None
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            _publish_temp_no_replace(temp_path, path)
            published = True
        except FileExistsError:
            if _read_existing_immutable(path) != data:
                raise QualificationError(
                    "PSC_QUALIFICATION_IMMUTABLE_ARTIFACT_CONFLICT", path.name
                ) from None
        if _read_existing_immutable(path) != data:
            raise QualificationError("PSC_QUALIFICATION_ARTIFACT_VERIFY_FAILED")
    except QualificationError:
        raise
    except OSError as exc:
        raise QualificationError("PSC_QUALIFICATION_ARTIFACT_WRITE_FAILED") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if not published:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass


def _with_integrity(document: Mapping[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(dict(document))
    integrity = dict(result.get("integrity") or {})
    integrity.pop("payload_sha256", None)
    integrity["algorithm"] = "sha256"
    integrity["canonicalization"] = CANONICALIZATION
    result["integrity"] = integrity
    payload = copy.deepcopy(result)
    payload["integrity"].pop("payload_sha256", None)
    result["integrity"]["payload_sha256"] = _sha256(_canonical_bytes(payload))
    return result


def _verify_integrity(document: Mapping[str, Any]) -> None:
    integrity = document.get("integrity")
    if type(integrity) is not dict:
        raise QualificationError("PSC_QUALIFICATION_INTEGRITY_MISSING")
    expected = integrity.get("payload_sha256")
    if type(expected) is not str or not re.fullmatch(r"[0-9a-f]{64}", expected):
        raise QualificationError("PSC_QUALIFICATION_INTEGRITY_INVALID")
    payload = copy.deepcopy(dict(document))
    payload_integrity = dict(payload["integrity"])
    payload_integrity.pop("payload_sha256", None)
    payload["integrity"] = payload_integrity
    if _sha256(_canonical_bytes(payload)) != expected:
        raise QualificationError("PSC_QUALIFICATION_INTEGRITY_MISMATCH")


def _write_json(path: pathlib.Path, document: Mapping[str, Any]) -> dict[str, Any]:
    bound = _with_integrity(document)
    _write_immutable(path, _canonical_bytes(bound))
    return bound


@contextlib.contextmanager
def _run_key_lease(run_root: pathlib.Path, run_key: str):
    """Hold an OS-enforced exclusive controller lease for one run key."""

    if re.fullmatch(r"[0-9a-f]{64}", run_key) is None:
        raise QualificationError("PSC_QUALIFICATION_RUN_KEY_INVALID")
    lease_root = run_root / ".leases"
    lease_root.mkdir(parents=True, exist_ok=True)
    _assert_existing_chain_not_redirected(lease_root)
    lease_path = lease_root / f"{run_key}.lock"
    try:
        handle = lease_path.open("a+b")
    except OSError as exc:
        raise QualificationError("PSC_QUALIFICATION_RUN_KEY_LEASE_FAILED") from exc
    owner_id = secrets.token_hex(16)
    locked = False
    try:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
            os.fsync(handle.fileno())
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:  # pragma: no cover - qualification currently runs on Windows
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            locked = True
        except OSError as exc:
            raise QualificationError("PSC_QUALIFICATION_RUN_KEY_BUSY") from exc
        yield {
            "mechanism": "msvcrt_byte_range" if os.name == "nt" else "flock",
            "owner_id": owner_id,
        }
    finally:
        if locked:
            try:
                handle.seek(0)
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:  # pragma: no cover - qualification currently runs on Windows
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
        handle.close()


def _require_exact_keys(
    value: Mapping[str, Any], required: set[str], *, code: str
) -> None:
    if set(value) != required:
        raise QualificationError(code)


def _string_list(value: Any, *, code: str, nonempty: bool = True) -> list[str]:
    if type(value) is not list or (nonempty and not value):
        raise QualificationError(code)
    result: list[str] = []
    for item in value:
        if type(item) is not str or not item.strip() or item != item.strip():
            raise QualificationError(code)
        result.append(item)
    if len(result) != len(set(result)):
        raise QualificationError(code)
    return result


def _load_selector(path: pathlib.Path) -> dict[str, Any]:
    selector = _read_json(path)
    _require_exact_keys(
        selector,
        {
            "schema_version",
            "kind",
            "suite_id",
            "isolation_policy_version",
            "expected_unique_test_count",
            "expected_test_ids_sha256",
            "required_protected_root_labels",
            "builtin_protected_roots",
            "coverage_partitions",
            "categories",
            "priority_test_ids",
            "singleton_modules",
            "pass_policy",
            "external_evidence",
            "acceptance_map",
        },
        code="PSC_QUALIFICATION_SELECTOR_INVALID",
    )
    if selector["schema_version"] != SCHEMA_VERSION or selector["kind"] != (
        "psc_qualification_selectors"
    ):
        raise QualificationError("PSC_QUALIFICATION_SELECTOR_INVALID")
    if type(selector["suite_id"]) is not str or not selector["suite_id"]:
        raise QualificationError("PSC_QUALIFICATION_SELECTOR_INVALID")
    if (
        type(selector["expected_unique_test_count"]) is not int
        or selector["expected_unique_test_count"] <= 0
    ):
        raise QualificationError("PSC_QUALIFICATION_SELECTOR_INVALID")
    if not re.fullmatch(
        r"[0-9a-f]{64}", str(selector["expected_test_ids_sha256"])
    ):
        raise QualificationError("PSC_QUALIFICATION_SELECTOR_INVALID")
    protected_labels = _string_list(
        selector["required_protected_root_labels"],
        code="PSC_QUALIFICATION_SELECTOR_INVALID",
    )
    builtin_roots = selector["builtin_protected_roots"]
    if (
        set(protected_labels) != REQUIRED_PROTECTED_ROOT_LABELS
        or type(builtin_roots) is not dict
        or builtin_roots != BUILTIN_PROTECTED_ROOTS
    ):
        raise QualificationError("PSC_QUALIFICATION_SELECTOR_INVALID")
    priorities = _string_list(
        selector["priority_test_ids"], code="PSC_QUALIFICATION_SELECTOR_INVALID"
    )
    singletons = _string_list(
        selector["singleton_modules"], code="PSC_QUALIFICATION_SELECTOR_INVALID"
    )

    categories = selector["categories"]
    if type(categories) is not list or not categories:
        raise QualificationError("PSC_QUALIFICATION_SELECTOR_INVALID")
    category_ids: list[str] = []
    modules: list[str] = []
    for category in categories:
        if type(category) is not dict:
            raise QualificationError("PSC_QUALIFICATION_SELECTOR_INVALID")
        _require_exact_keys(
            category,
            {
                "id",
                "expected_owned_test_count",
                "expected_test_ids_sha256",
                "modules",
            },
            code="PSC_QUALIFICATION_SELECTOR_INVALID",
        )
        if type(category["id"]) is not str or not category["id"]:
            raise QualificationError("PSC_QUALIFICATION_SELECTOR_INVALID")
        if (
            type(category["expected_owned_test_count"]) is not int
            or category["expected_owned_test_count"] <= 0
        ):
            raise QualificationError("PSC_QUALIFICATION_SELECTOR_INVALID")
        if not re.fullmatch(
            r"[0-9a-f]{64}", str(category["expected_test_ids_sha256"])
        ):
            raise QualificationError("PSC_QUALIFICATION_SELECTOR_INVALID")
        category_ids.append(category["id"])
        modules.extend(
            _string_list(
                category["modules"], code="PSC_QUALIFICATION_SELECTOR_INVALID"
            )
        )
    if len(category_ids) != len(set(category_ids)) or len(modules) != len(
        set(modules)
    ):
        raise QualificationError("PSC_QUALIFICATION_SELECTOR_INVALID")
    if any(module not in modules for module in singletons):
        raise QualificationError("PSC_QUALIFICATION_SELECTOR_INVALID")

    partitions = selector["coverage_partitions"]
    if type(partitions) is not dict or set(partitions) != {
        "psc_a15_regression",
        "qualification_tooling_preflight",
    }:
        raise QualificationError("PSC_QUALIFICATION_SELECTOR_INVALID")
    partition_categories: list[str] = []
    for partition in partitions.values():
        if type(partition) is not dict:
            raise QualificationError("PSC_QUALIFICATION_SELECTOR_INVALID")
        _require_exact_keys(
            partition,
            {
                "category_ids",
                "expected_unique_test_count",
                "expected_test_ids_sha256",
            },
            code="PSC_QUALIFICATION_SELECTOR_INVALID",
        )
        mapped = _string_list(
            partition["category_ids"], code="PSC_QUALIFICATION_SELECTOR_INVALID"
        )
        if (
            any(item not in category_ids for item in mapped)
            or type(partition["expected_unique_test_count"]) is not int
            or partition["expected_unique_test_count"] <= 0
            or re.fullmatch(
                r"[0-9a-f]{64}", str(partition["expected_test_ids_sha256"])
            )
            is None
        ):
            raise QualificationError("PSC_QUALIFICATION_SELECTOR_INVALID")
        partition_categories.extend(mapped)
    if sorted(partition_categories) != sorted(category_ids):
        raise QualificationError("PSC_QUALIFICATION_SELECTOR_INVALID")

    pass_policy = selector["pass_policy"]
    if type(pass_policy) is not dict or set(pass_policy) != {
        "required_unique_test_count",
        "maximum_failures",
        "maximum_errors",
        "maximum_skips",
        "maximum_expected_failures",
        "maximum_unexpected_successes",
        "maximum_loader_errors",
        "maximum_fixture_errors",
        "maximum_process_errors",
        "maximum_forbidden_root_attempts",
    }:
        raise QualificationError("PSC_QUALIFICATION_SELECTOR_INVALID")
    if any(type(value) is not int or value < 0 for value in pass_policy.values()):
        raise QualificationError("PSC_QUALIFICATION_SELECTOR_INVALID")
    if pass_policy["required_unique_test_count"] != selector[
        "expected_unique_test_count"
    ]:
        raise QualificationError("PSC_QUALIFICATION_SELECTOR_INVALID")
    if any(
        pass_policy[name] != 0
        for name in pass_policy
        if name != "required_unique_test_count"
    ):
        raise QualificationError("PSC_QUALIFICATION_SELECTOR_INVALID")

    evidence = selector["external_evidence"]
    if type(evidence) is not list:
        raise QualificationError("PSC_QUALIFICATION_SELECTOR_INVALID")
    evidence_ids: list[str] = []
    for item in evidence:
        if type(item) is not dict:
            raise QualificationError("PSC_QUALIFICATION_SELECTOR_INVALID")
        _require_exact_keys(
            item,
            {
                "id",
                "required",
                "acceptance_ids",
                "candidate_commit_pointer",
                "candidate_tree_pointer",
                "expected_schema_version",
                "expected_kind",
                "seal_field",
                "required_pointer_values",
            },
            code="PSC_QUALIFICATION_SELECTOR_INVALID",
        )
        if (
            type(item["id"]) is not str
            or not item["id"]
            or type(item["required"]) is not bool
            or type(item["expected_schema_version"]) is not int
            or item["expected_schema_version"] <= 0
            or type(item["expected_kind"]) is not str
            or not item["expected_kind"]
            or type(item["seal_field"]) is not str
            or not item["seal_field"]
        ):
            raise QualificationError("PSC_QUALIFICATION_SELECTOR_INVALID")
        evidence_ids.append(item["id"])
        _string_list(
            item["acceptance_ids"], code="PSC_QUALIFICATION_SELECTOR_INVALID"
        )
        for pointer in (
            item["candidate_commit_pointer"],
            item["candidate_tree_pointer"],
        ):
            if type(pointer) is not str or not pointer.startswith("/"):
                raise QualificationError("PSC_QUALIFICATION_SELECTOR_INVALID")
        pointer_values = item["required_pointer_values"]
        if type(pointer_values) is not dict or not pointer_values:
            raise QualificationError("PSC_QUALIFICATION_SELECTOR_INVALID")
        for pointer, expected in pointer_values.items():
            if (
                type(pointer) is not str
                or not pointer.startswith("/")
                or type(expected) not in {bool, int, str}
            ):
                raise QualificationError("PSC_QUALIFICATION_SELECTOR_INVALID")
    if len(evidence_ids) != len(set(evidence_ids)):
        raise QualificationError("PSC_QUALIFICATION_SELECTOR_INVALID")

    acceptance = selector["acceptance_map"]
    expected_acceptance = {f"PSC-A{number:02d}" for number in range(1, 21)}
    if type(acceptance) is not dict or set(acceptance) != expected_acceptance:
        raise QualificationError("PSC_QUALIFICATION_SELECTOR_INVALID")
    for value in acceptance.values():
        if type(value) is not dict:
            raise QualificationError("PSC_QUALIFICATION_SELECTOR_INVALID")
        _require_exact_keys(
            value,
            {"categories", "modules", "external_evidence", "documents"},
            code="PSC_QUALIFICATION_SELECTOR_INVALID",
        )
        mapped_categories = _string_list(
            value["categories"],
            code="PSC_QUALIFICATION_SELECTOR_INVALID",
            nonempty=False,
        )
        mapped_modules = _string_list(
            value["modules"],
            code="PSC_QUALIFICATION_SELECTOR_INVALID",
            nonempty=False,
        )
        mapped_evidence = _string_list(
            value["external_evidence"],
            code="PSC_QUALIFICATION_SELECTOR_INVALID",
            nonempty=False,
        )
        _string_list(
            value["documents"],
            code="PSC_QUALIFICATION_SELECTOR_INVALID",
            nonempty=False,
        )
        if any(item not in category_ids for item in mapped_categories):
            raise QualificationError("PSC_QUALIFICATION_SELECTOR_INVALID")
        if any(item not in modules for item in mapped_modules):
            raise QualificationError("PSC_QUALIFICATION_SELECTOR_INVALID")
        if any(item not in evidence_ids for item in mapped_evidence):
            raise QualificationError("PSC_QUALIFICATION_SELECTOR_INVALID")
    if any(not test_id.startswith("tests.") for test_id in priorities):
        raise QualificationError("PSC_QUALIFICATION_SELECTOR_INVALID")
    return selector


def _run_git(workspace: pathlib.Path, *arguments: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(workspace), *arguments],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if completed.returncode != 0:
        raise QualificationError("PSC_QUALIFICATION_GIT_FAILED")
    return completed.stdout.rstrip("\r\n")


def _git_state(workspace: pathlib.Path) -> dict[str, Any]:
    head = _run_git(workspace, "rev-parse", "HEAD")
    tree = _run_git(workspace, "rev-parse", "HEAD^{tree}")
    status = _run_git(
        workspace, "status", "--porcelain=v2", "--untracked-files=all"
    )
    branch = _run_git(workspace, "branch", "--show-current")
    if not HEX_40_OR_64.fullmatch(head) or not HEX_40_OR_64.fullmatch(tree):
        raise QualificationError("PSC_QUALIFICATION_GIT_FAILED")
    return {
        "head": head,
        "tree": tree,
        "branch": branch,
        "clean": status == "",
        "status_sha256": _sha256(status.encode("utf-8")),
        "status_line_count": 0 if not status else len(status.splitlines()),
    }


def _assert_git_binding(
    workspace: pathlib.Path,
    expected: Mapping[str, Any],
    *,
    require_clean: bool = True,
) -> dict[str, Any]:
    current = _git_state(workspace)
    if current["head"] != expected["head"] or current["tree"] != expected["tree"]:
        raise QualificationError("PSC_QUALIFICATION_HEAD_CHANGED")
    if require_clean and not current["clean"]:
        raise QualificationError("PSC_QUALIFICATION_WORKTREE_DIRTY")
    return current


def _file_binding(workspace: pathlib.Path, path: pathlib.Path) -> dict[str, Any]:
    absolute = path.resolve(strict=True)
    try:
        relative = absolute.relative_to(workspace.resolve(strict=True)).as_posix()
    except ValueError as exc:
        raise QualificationError("PSC_QUALIFICATION_PATH_OUTSIDE_WORKSPACE") from exc
    data = _read_bytes(absolute)
    blob: str | None
    try:
        blob = _run_git(workspace, "rev-parse", f"HEAD:{relative}")
    except QualificationError:
        blob = None
    return {
        "path": relative,
        "byte_size": len(data),
        "sha256": _sha256(data),
        "git_blob": blob,
    }


def _toolchain() -> dict[str, Any]:
    executable = pathlib.Path(sys.executable)
    packages: dict[str, str | None] = {}
    for name in SAFE_PACKAGES:
        try:
            packages[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            packages[name] = None
    git_version = subprocess.run(
        ["git", "--version"],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        encoding="utf-8",
        errors="replace",
    ).stdout.strip()
    return {
        "python_version": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "python_executable_name": executable.name,
        "python_executable_sha256": _sha256(_read_bytes(executable)),
        "platform_system": platform.system(),
        "platform_release": platform.release(),
        "platform_version": platform.version(),
        "platform_machine": platform.machine(),
        "filesystem_encoding": sys.getfilesystemencoding(),
        "stdout_encoding": sys.stdout.encoding,
        "git_version": git_version,
        "packages": packages,
    }


def _flatten_suite(suite: unittest.TestSuite) -> Iterable[unittest.TestCase]:
    for item in suite:
        if isinstance(item, unittest.TestSuite):
            yield from _flatten_suite(item)
        else:
            yield item


def _collect_owned_tests(
    selector: Mapping[str, Any],
) -> tuple[list[dict[str, str]], list[dict[str, Any]], list[str]]:
    tests: list[dict[str, str]] = []
    category_results: list[dict[str, Any]] = []
    loader_errors: list[str] = []
    for category in selector["categories"]:
        category_ids: list[str] = []
        module_results: list[dict[str, Any]] = []
        for module_name in category["modules"]:
            module = importlib.import_module(module_name)
            module_ids: list[str] = []
            for _name, candidate in inspect.getmembers(module, inspect.isclass):
                if (
                    issubclass(candidate, unittest.TestCase)
                    and candidate.__module__ == module_name
                ):
                    loader = unittest.TestLoader()
                    suite = loader.loadTestsFromTestCase(candidate)
                    loader_errors.extend(loader.errors)
                    module_ids.extend(test.id() for test in _flatten_suite(suite))
            module_ids.sort()
            if len(module_ids) != len(set(module_ids)):
                raise QualificationError("PSC_QUALIFICATION_DUPLICATE_TEST_ID")
            module_results.append(
                {
                    "module": module_name,
                    "owned_test_count": len(module_ids),
                    "test_ids_sha256": _sha256(
                        "".join(f"{test_id}\n" for test_id in module_ids).encode(
                            "utf-8"
                        )
                    ),
                }
            )
            category_ids.extend(module_ids)
            tests.extend(
                {
                    "id": test_id,
                    "module": module_name,
                    "category": category["id"],
                }
                for test_id in module_ids
            )
        category_ids.sort()
        category_hash = _sha256(
            "".join(f"{test_id}\n" for test_id in category_ids).encode("utf-8")
        )
        if len(category_ids) != category["expected_owned_test_count"]:
            raise QualificationError("PSC_QUALIFICATION_CATEGORY_COUNT_MISMATCH")
        if category_hash != category["expected_test_ids_sha256"]:
            raise QualificationError("PSC_QUALIFICATION_CATEGORY_HASH_MISMATCH")
        category_results.append(
            {
                "id": category["id"],
                "test_count": len(category_ids),
                "test_ids_sha256": category_hash,
                "modules": module_results,
            }
        )
    tests.sort(key=lambda item: item["id"])
    if len(tests) != len({item["id"] for item in tests}):
        raise QualificationError("PSC_QUALIFICATION_DUPLICATE_TEST_ID")
    return tests, category_results, loader_errors


def _coverage_partition_results(
    selector: Mapping[str, Any], tests: Sequence[Mapping[str, str]]
) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}
    for partition_id, specification in selector["coverage_partitions"].items():
        categories = set(specification["category_ids"])
        ids = sorted(item["id"] for item in tests if item["category"] in categories)
        digest = _sha256(
            "".join(f"{test_id}\n" for test_id in ids).encode("utf-8")
        )
        if (
            len(ids) != specification["expected_unique_test_count"]
            or digest != specification["expected_test_ids_sha256"]
        ):
            raise QualificationError("PSC_QUALIFICATION_PARTITION_MISMATCH")
        results[partition_id] = {
            "category_ids": specification["category_ids"],
            "unique_test_count": len(ids),
            "test_ids_sha256": digest,
        }
    return results


def _path_key_lexical(value: str) -> tuple[str, str]:
    """Compatibility helper for component-boundary comparisons in tests."""

    if not value or "\x00" in value:
        raise QualificationError("PSC_QUALIFICATION_PATH_INVALID")
    if re.match(r"^(?:[A-Za-z]:[\\/]|\\\\)", value):
        normalized = ntpath.normcase(ntpath.abspath(value.replace("/", "\\")))
        return normalized.rstrip("\\"), "\\"
    normalized = os.path.normcase(os.path.abspath(value))
    return normalized.rstrip(os.sep), os.sep


def _lexically_within(path_value: str, root_value: str) -> bool:
    path, separator = _path_key_lexical(path_value)
    root, root_separator = _path_key_lexical(root_value)
    return separator == root_separator and (
        path == root or path.startswith(root + separator)
    )


def _strict_local_windows_path(value: str | os.PathLike[str]) -> str:
    try:
        raw = os.fsdecode(os.fspath(value))
    except (TypeError, ValueError, UnicodeError):
        raise QualificationError("PSC_QUALIFICATION_PATH_INVALID") from None
    if (
        not raw
        or "\x00" in raw
        or LOCAL_DRIVE_PATH_RE.match(raw) is None
        or raw.startswith(("\\\\", "\\?\\", "\\.\\"))
    ):
        raise QualificationError("PSC_QUALIFICATION_PATH_ALIAS_UNSUPPORTED")
    drive, tail = ntpath.splitdrive(raw)
    if len(drive) != 2 or drive[1] != ":" or not tail.startswith(("\\", "/")):
        raise QualificationError("PSC_QUALIFICATION_PATH_ALIAS_UNSUPPORTED")
    for component in re.split(r"[\\/]+", tail):
        if not component:
            continue
        if (
            component.endswith((" ", "."))
            or ":" in component
            or "~" in component
            or "*" in component
            or "?" in component
            or RESERVED_WINDOWS_COMPONENT_RE.fullmatch(component) is not None
        ):
            raise QualificationError("PSC_QUALIFICATION_PATH_ALIAS_UNSUPPORTED")
    normalized = ntpath.normcase(ntpath.normpath(raw))
    return normalized if normalized.endswith(":\\") else normalized.rstrip("\\")


def _query_dos_device(drive: str) -> str:
    if os.name != "nt":
        raise QualificationError("PSC_QUALIFICATION_WINDOWS_REQUIRED")
    from ctypes import wintypes

    query = ctypes.WinDLL("kernel32", use_last_error=True).QueryDosDeviceW
    query.argtypes = (wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD)
    query.restype = wintypes.DWORD
    buffer = ctypes.create_unicode_buffer(32_768)
    if query(drive.upper(), buffer, len(buffer)) == 0:
        raise QualificationError("PSC_QUALIFICATION_PATH_DEVICE_QUERY_FAILED")
    return buffer.value


def _get_long_path_name(path: str) -> str:
    """Resolve an existing Windows path without accepting an 8.3 spelling."""

    if os.name != "nt":
        raise QualificationError("PSC_QUALIFICATION_WINDOWS_REQUIRED")
    from ctypes import wintypes

    resolve = ctypes.WinDLL("kernel32", use_last_error=True).GetLongPathNameW
    resolve.argtypes = (wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD)
    resolve.restype = wintypes.DWORD
    required = resolve(path, None, 0)
    if required == 0:
        raise QualificationError("PSC_QUALIFICATION_PATH_ALIAS_UNSUPPORTED")
    buffer = ctypes.create_unicode_buffer(required)
    written = resolve(path, buffer, required)
    if written == 0 or written >= required:
        raise QualificationError("PSC_QUALIFICATION_PATH_ALIAS_UNSUPPORTED")
    return buffer.value


def _device_lexical_windows_path(
    value: str | os.PathLike[str],
    *,
    device_query=_query_dos_device,
    _depth: int = 0,
) -> str:
    if _depth > 8:
        raise QualificationError("PSC_QUALIFICATION_PATH_ALIAS_UNSUPPORTED")
    local = _strict_local_windows_path(value)
    drive, tail = ntpath.splitdrive(local)
    mapping = device_query(drive)
    folded = mapping.casefold()
    if any(
        marker in folded
        for marker in ("\\device\\mup", "lanmanredirector", "webdavredirector")
    ):
        raise QualificationError("PSC_QUALIFICATION_PATH_ALIAS_UNSUPPORTED")
    if mapping.startswith("\\??\\"):
        target = mapping[4:]
        if LOCAL_DRIVE_PATH_RE.match(target):
            return _device_lexical_windows_path(
                ntpath.join(target, tail.lstrip("\\/")),
                device_query=device_query,
                _depth=_depth + 1,
            )
        raise QualificationError("PSC_QUALIFICATION_PATH_ALIAS_UNSUPPORTED")
    if not folded.startswith("\\device\\"):
        raise QualificationError("PSC_QUALIFICATION_PATH_ALIAS_UNSUPPORTED")
    return ntpath.normcase(mapping.rstrip("\\") + "\\" + tail.lstrip("\\/"))


def _under(candidate: str, root: str) -> bool:
    return candidate == root or candidate.startswith(root + "\\")


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


def _forms_overlap(first: Mapping[str, str], second: Mapping[str, str]) -> bool:
    return any(
        _under(first[key], second[key]) or _under(second[key], first[key])
        for key in ("lexical_path", "device_path")
    )


def _parse_protected_roots(
    values: Sequence[str], *, device_query=_query_dos_device
) -> tuple[dict[str, str], ...]:
    parsed = dict(BUILTIN_PROTECTED_ROOTS)
    for value in values:
        if type(value) is not str or "=" not in value:
            raise QualificationError("PSC_QUALIFICATION_PROTECTED_ROOT_INVALID")
        label, raw_path = value.split("=", 1)
        if ROOT_LABEL_RE.fullmatch(label) is None or label in parsed:
            raise QualificationError("PSC_QUALIFICATION_PROTECTED_ROOT_INVALID")
        parsed[label] = _strict_local_windows_path(raw_path)
    if set(parsed) != REQUIRED_PROTECTED_ROOT_LABELS:
        raise QualificationError("PSC_QUALIFICATION_PROTECTED_ROOT_SET_INCOMPLETE")
    roots = tuple(
        {
            "label": label,
            **_path_forms(path, device_query=device_query),
        }
        for label, path in sorted(parsed.items())
    )
    identities = [
        (item["lexical_path"], item["device_path"]) for item in roots
    ]
    if len(identities) != len(set(identities)):
        raise QualificationError("PSC_QUALIFICATION_PROTECTED_ROOT_INVALID")
    return roots


def _coerce_root_forms(
    roots: Sequence[Any], *, device_query=_query_dos_device
) -> tuple[dict[str, str], ...]:
    result: list[dict[str, str]] = []
    for index, root in enumerate(roots):
        if type(root) is dict and {
            "label",
            "lexical_path",
            "device_path",
        }.issubset(root):
            result.append(
                {
                    "label": str(root["label"]),
                    "lexical_path": str(root["lexical_path"]),
                    "device_path": str(root["device_path"]),
                }
            )
        else:
            result.append(
                {
                    "label": f"legacy_{index}",
                    **_path_forms(str(root), device_query=device_query),
                }
            )
    return tuple(result)


def _assert_existing_chain_not_redirected(
    path: pathlib.Path, *, lstat_function=None
) -> None:
    if lstat_function is None:
        # The qualification guard replaces os.lstat irreversibly.  Internal
        # chain validation must still inspect the ancestors of an explicitly
        # allowed managed workspace even when service_state is its protected
        # parent; candidate calls continue through the guarded public API.
        lstat_function = _ORIGINAL_LSTAT
    local = _strict_local_windows_path(path)
    drive, tail = ntpath.splitdrive(local)
    current = pathlib.Path(drive + "\\")
    chain = [current]
    for component in (item for item in tail.split("\\") if item):
        current = current / component
        chain.append(current)
    for index, item in enumerate(chain):
        try:
            metadata = lstat_function(item)
        except FileNotFoundError:
            if index == 0:
                raise QualificationError("PSC_QUALIFICATION_PATH_PARENT_UNAVAILABLE")
            return
        except OSError:
            raise QualificationError(
                "PSC_QUALIFICATION_PATH_PARENT_UNAVAILABLE"
            ) from None
        if stat.S_ISLNK(metadata.st_mode) or bool(
            getattr(metadata, "st_file_attributes", 0) & 0x400
        ):
            raise QualificationError("PSC_QUALIFICATION_PATH_REDIRECTED")


def _validate_path_isolation(
    run_root: pathlib.Path,
    workspace: pathlib.Path,
    protected_roots: Sequence[Any],
    *,
    device_query=_query_dos_device,
    inspect_existing: bool = True,
) -> tuple[dict[str, str], tuple[dict[str, str], ...]]:
    protected = _coerce_root_forms(protected_roots, device_query=device_query)
    run = _path_forms(run_root, device_query=device_query)
    work = _path_forms(workspace, device_query=device_query)
    if _forms_overlap(run, work) or any(
        _forms_overlap(run, root) for root in protected
    ):
        raise QualificationError("PSC_QUALIFICATION_RUN_ROOT_OVERLAP")
    # The exact managed workspace is an explicit allowed subtree.  It may
    # live below the service checkout root or below the managed service-state
    # root, but never below live prompt/runtime/prompt.  The access guard only
    # exempts this exact workspace subtree, not siblings or either parent.
    for root in protected:
        overlaps = _forms_overlap(work, root)
        workspace_is_strictly_below = all(
            _under(work[key], root[key]) and work[key] != root[key]
            for key in ("lexical_path", "device_path")
        )
        if overlaps and (
            root["label"] not in {"service_root", "service_state"}
            or not workspace_is_strictly_below
        ):
            raise QualificationError("PSC_QUALIFICATION_WORKSPACE_OVERLAP")
    if inspect_existing:
        _assert_existing_chain_not_redirected(workspace)
        _assert_existing_chain_not_redirected(run_root.parent)
        try:
            os.lstat(run_root)
        except FileNotFoundError:
            pass
        except OSError:
            raise QualificationError(
                "PSC_QUALIFICATION_PATH_PARENT_UNAVAILABLE"
            ) from None
        else:
            _assert_existing_chain_not_redirected(run_root)
    return work, protected


def _validate_run_root(
    run_root: pathlib.Path,
    workspace: pathlib.Path,
    protected_roots: Sequence[Any],
) -> None:
    _validate_path_isolation(run_root, workspace, protected_roots)


def _reject_protected_path(
    path: pathlib.Path,
    protected_roots: Sequence[Any],
    *,
    device_query=_query_dos_device,
) -> None:
    candidate = _path_forms(path, device_query=device_query)
    for root in _coerce_root_forms(protected_roots, device_query=device_query):
        if any(
            _under(candidate[key], root[key])
            for key in ("lexical_path", "device_path")
        ):
            raise QualificationError("PSC_QUALIFICATION_PROTECTED_ROOT_ACCESS")


def _reject_forbidden_path(path: pathlib.Path, roots: Sequence[Any]) -> None:
    """Backward-compatible alias with hardened lexical/device semantics."""

    _reject_protected_path(path, roots)


def _require_path_below(path: pathlib.Path, root: pathlib.Path) -> None:
    candidate = _strict_local_windows_path(path.absolute())
    boundary = _strict_local_windows_path(root.absolute())
    if candidate == boundary or not _under(candidate, boundary):
        raise QualificationError("PSC_QUALIFICATION_OUTPUT_OUTSIDE_RUN_ROOT")
    _assert_existing_chain_not_redirected(root)
    _assert_existing_chain_not_redirected(path.parent)


class QualificationAccessGuard:
    """Deny protected aliases and network access before a selected test runs."""

    def __init__(
        self,
        *,
        protected_roots: Sequence[Mapping[str, str]],
        allowed_roots: Sequence[pathlib.Path],
        device_query=_query_dos_device,
        long_path_query=_get_long_path_name,
    ) -> None:
        self.protected_roots = tuple(dict(item) for item in protected_roots)
        self.allowed_roots = tuple(
            _path_forms(item, device_query=device_query) for item in allowed_roots
        )
        self.device_query = device_query
        self.long_path_query = long_path_query
        self.protected_attempts: list[dict[str, str]] = []
        self.unsafe_alias_attempts: list[dict[str, str]] = []
        self.network_attempts: list[dict[str, str]] = []
        self.guard_checks_by_event: dict[str, int] = {}
        self.descriptor_checks_by_event: dict[str, int] = {}
        self.native_path_checks_by_entrypoint: dict[str, int] = {}
        self.native_guard_entrypoints: list[str] = []
        self._installed = False
        self._original_lstat = _ORIGINAL_LSTAT

    def _candidate_forms(self, value: Any) -> dict[str, str]:
        try:
            raw = os.fsdecode(os.fspath(value))
        except (TypeError, ValueError, UnicodeError):
            raise QualificationError(
                "PSC_QUALIFICATION_PATH_ALIAS_UNSUPPORTED"
            ) from None
        if not raw or "\x00" in raw:
            raise QualificationError("PSC_QUALIFICATION_PATH_ALIAS_UNSUPPORTED")
        if LOCAL_DRIVE_PATH_RE.match(raw):
            local = raw
        elif ntpath.isabs(raw) or raw.startswith(("\\", "/")):
            raise QualificationError("PSC_QUALIFICATION_PATH_ALIAS_UNSUPPORTED")
        else:
            local = ntpath.join(os.getcwd(), raw)
        if "~" in local:
            # Some third-party installations contain literal stale dist-info
            # names beginning with ``~``.  Treat them as ordinary only when
            # the fully non-aliased prefix is disjoint from every protected
            # and allowed boundary and its existing chain has no reparse
            # point.  A short-name form near either boundary remains denied
            # before the aliased component is probed.
            normalized = ntpath.normcase(ntpath.normpath(local))
            drive, tail = ntpath.splitdrive(normalized)
            components = [part for part in tail.split("\\") if part]
            if any(
                part.endswith((" ", "."))
                or ":" in part
                or "*" in part
                or "?" in part
                or RESERVED_WINDOWS_COMPONENT_RE.fullmatch(part) is not None
                for part in components
            ):
                raise QualificationError(
                    "PSC_QUALIFICATION_PATH_ALIAS_UNSUPPORTED"
                )
            alias_index = next(
                (index for index, part in enumerate(components) if "~" in part),
                None,
            )
            if alias_index is None:
                raise QualificationError(
                    "PSC_QUALIFICATION_PATH_ALIAS_UNSUPPORTED"
                )
            partial = drive + "\\"
            for component in components:
                partial = ntpath.join(partial, component)
                if "~" not in component:
                    continue
                resolved = self.long_path_query(partial)
                if ntpath.normcase(ntpath.normpath(resolved)) != ntpath.normcase(
                    ntpath.normpath(partial)
                ):
                    raise QualificationError(
                        "PSC_QUALIFICATION_PATH_ALIAS_UNSUPPORTED"
                    )
            prefix = drive + "\\" + "\\".join(components[:alias_index])
            prefix_forms = _path_forms(prefix, device_query=self.device_query)
            boundaries = (*self.protected_roots, *self.allowed_roots)
            if any(_forms_overlap(prefix_forms, item) for item in boundaries):
                raise QualificationError(
                    "PSC_QUALIFICATION_PATH_ALIAS_UNSUPPORTED"
                )
            _assert_existing_chain_not_redirected(
                pathlib.Path(prefix_forms["lexical_path"]),
                lstat_function=self._original_lstat,
            )
            device_root = _device_lexical_windows_path(
                drive + "\\", device_query=self.device_query
            )
            return {
                "lexical_path": normalized.rstrip("\\"),
                "device_path": ntpath.normcase(
                    device_root.rstrip("\\") + "\\" + tail.lstrip("\\")
                ).rstrip("\\"),
            }
        return _path_forms(local, device_query=self.device_query)

    def check(self, event: str, value: Any) -> None:
        if value is None:
            value = os.getcwd()
        if isinstance(value, int):
            self.descriptor_checks_by_event[event] = (
                self.descriptor_checks_by_event.get(event, 0) + 1
            )
            return
        try:
            raw_value = os.fsdecode(os.fspath(value))
        except (TypeError, ValueError, UnicodeError):
            raw_value = ""
        if raw_value.casefold() == os.devnull.casefold():
            self.guard_checks_by_event[event] = (
                self.guard_checks_by_event.get(event, 0) + 1
            )
            return
        try:
            candidate = self._candidate_forms(value)
        except QualificationError:
            self.unsafe_alias_attempts.append({"event": event})
            raise PermissionError("PSC qualification denied unsafe path alias") from None
        self.guard_checks_by_event[event] = self.guard_checks_by_event.get(event, 0) + 1
        allowed = any(
            all(_under(candidate[key], allowed[key]) for key in ("lexical_path", "device_path"))
            for allowed in self.allowed_roots
        )
        matching_protected = None
        for root in self.protected_roots:
            if any(
                _under(candidate[key], root[key])
                for key in ("lexical_path", "device_path")
            ):
                matching_protected = root
                break
        if matching_protected is not None and not allowed:
            self.protected_attempts.append(
                {"event": event, "root_label": matching_protected["label"]}
            )
            raise PermissionError("PSC qualification denied protected-root access")
        # Disjoint lexical/device paths can still be junctions into a protected
        # root.  Inspect every permitted candidate chain with the original
        # lstat so the guard itself is not recursively invoked.  The candidate
        # was already syntax/device validated, including the narrowly allowed
        # literal-tilde case.
        drive, tail = ntpath.splitdrive(candidate["lexical_path"])
        current = pathlib.Path(drive + "\\")
        try:
            chain = [current]
            for component in (part for part in tail.split("\\") if part):
                current = current / component
                chain.append(current)
            for index, item in enumerate(chain):
                try:
                    metadata = self._original_lstat(item)
                except FileNotFoundError:
                    if index == 0:
                        raise QualificationError(
                            "PSC_QUALIFICATION_PATH_PARENT_UNAVAILABLE"
                        ) from None
                    break
                if stat.S_ISLNK(metadata.st_mode) or bool(
                    getattr(metadata, "st_file_attributes", 0) & 0x400
                ):
                    raise QualificationError("PSC_QUALIFICATION_PATH_REDIRECTED")
        except (QualificationError, OSError):
            self.unsafe_alias_attempts.append({"event": event})
            raise PermissionError(
                "PSC qualification denied redirected path"
            ) from None

    def check_network(self, event: str) -> None:
        self.network_attempts.append({"event": event})
        raise PermissionError("PSC qualification denied network access")

    def evidence(self) -> dict[str, Any]:
        return {
            "protected_root_attempts": list(self.protected_attempts),
            "unsafe_alias_attempts": list(self.unsafe_alias_attempts),
            "network_attempts": list(self.network_attempts),
            "guard_checks_by_event": dict(sorted(self.guard_checks_by_event.items())),
            "descriptor_checks_by_event": dict(
                sorted(self.descriptor_checks_by_event.items())
            ),
            "native_path_checks_by_entrypoint": dict(
                sorted(self.native_path_checks_by_entrypoint.items())
            ),
            "native_guard_entrypoints": sorted(self.native_guard_entrypoints),
        }

    def install(self) -> None:
        if self._installed:
            return

        def hook(event: str, arguments: tuple[Any, ...]) -> None:
            if event in {"socket.connect", "socket.bind"}:
                self.check_network(event)
            for index in PATH_AUDIT_EVENT_PATH_INDICES.get(event, ()):
                if index < len(arguments):
                    self.check(event, arguments[index])
            for index in PATH_AUDIT_EVENT_DIR_FD_INDICES.get(event, ()):
                if index < len(arguments) and arguments[index] not in (None, -1):
                    self.unsafe_alias_attempts.append({"event": event})
                    raise PermissionError(
                        "PSC qualification denied unbound directory descriptor"
                    )

        sys.addaudithook(hook)
        original_stat = os.stat
        original_lstat = os.lstat
        original_listdir = os.listdir
        original_scandir = os.scandir
        original_open = os.open
        original_readlink = os.readlink
        original_access = os.access
        original_builtin_open = builtins.open
        original_io_open = io.open

        def guarded_stat(path, *arguments, **keywords):
            self.check("os.stat", path)
            return original_stat(path, *arguments, **keywords)

        def guarded_lstat(path, *arguments, **keywords):
            self.check("os.lstat", path)
            return original_lstat(path, *arguments, **keywords)

        def guarded_listdir(path=None):
            self.check("os.listdir", path)
            return original_listdir(path) if path is not None else original_listdir()

        def guarded_scandir(path=None):
            self.check("os.scandir", path)
            return original_scandir(path) if path is not None else original_scandir()

        def guarded_open(path, flags, mode=0o777, *, dir_fd=None):
            if dir_fd is not None:
                self.unsafe_alias_attempts.append({"event": "os.open"})
                raise PermissionError(
                    "PSC qualification denied unbound directory descriptor"
                )
            self.check("os.open", path)
            return original_open(path, flags, mode)

        def guarded_readlink(path, *, dir_fd=None):
            if dir_fd is not None:
                self.unsafe_alias_attempts.append({"event": "os.readlink"})
                raise PermissionError(
                    "PSC qualification denied unbound directory descriptor"
                )
            self.check("os.readlink", path)
            return original_readlink(path)

        def guarded_access(
            path, mode, *, dir_fd=None, effective_ids=False, follow_symlinks=True
        ):
            if dir_fd is not None:
                self.unsafe_alias_attempts.append({"event": "os.access"})
                raise PermissionError(
                    "PSC qualification denied unbound directory descriptor"
                )
            self.check("os.access", path)
            return original_access(
                path,
                mode,
                effective_ids=effective_ids,
                follow_symlinks=follow_symlinks,
            )

        def guarded_builtin_open(file, *arguments, **keywords):
            self.check("open", file)
            return original_builtin_open(file, *arguments, **keywords)

        def guarded_io_open(file, *arguments, **keywords):
            self.check("io.open", file)
            return original_io_open(file, *arguments, **keywords)

        os.stat = guarded_stat
        os.lstat = guarded_lstat
        os.listdir = guarded_listdir
        os.scandir = guarded_scandir
        os.open = guarded_open
        os.readlink = guarded_readlink
        os.access = guarded_access
        builtins.open = guarded_builtin_open
        io.open = guarded_io_open
        self._installed = True


def _install_candidate_native_guards(guard: QualificationAccessGuard) -> None:
    for module_name, attribute, event in NATIVE_GUARD_ENTRYPOINTS:
        module = sys.modules.get(module_name)
        if module is None or not hasattr(module, attribute):
            continue
        original = getattr(module, attribute)
        if getattr(original, "_psc_qualification_guard", False):
            continue
        if attribute == "_allocated_size":

            @functools.wraps(original)
            def guarded(path, metadata, _original=original, _event=event):
                guard.check(_event, path)
                guard.native_path_checks_by_entrypoint[_event] = (
                    guard.native_path_checks_by_entrypoint.get(_event, 0) + 1
                )
                return _original(path, metadata)

        else:

            @functools.wraps(original)
            def guarded(path, _original=original, _event=event):
                guard.check(_event, path)
                guard.native_path_checks_by_entrypoint[_event] = (
                    guard.native_path_checks_by_entrypoint.get(_event, 0) + 1
                )
                return _original(path)

        guarded._psc_qualification_guard = True
        setattr(module, attribute, guarded)
        guard.native_guard_entrypoints.append(event)


def _assert_protected_root_binding(
    configured: Sequence[Mapping[str, str]], collection: Mapping[str, Any]
) -> None:
    recorded = collection.get("environment", {}).get("protected_roots")
    if _canonical_bytes(configured) != _canonical_bytes(recorded):
        raise QualificationError("PSC_QUALIFICATION_PROTECTED_ROOT_BINDING_MISMATCH")


def _module_path(module: str) -> str:
    return module.replace(".", "/") + ".py"


def _ensure_workspace_import_path(workspace: pathlib.Path) -> None:
    workspace_value = str(workspace.resolve(strict=True))
    normalized = os.path.normcase(workspace_value)
    existing = {
        os.path.normcase(os.path.abspath(value or os.curdir)) for value in sys.path
    }
    if normalized not in existing:
        sys.path.insert(0, workspace_value)


def _collection_run_key(binding: Mapping[str, Any]) -> str:
    return _sha256(_canonical_bytes(binding))


def _collect_command(args: argparse.Namespace) -> dict[str, Any]:
    workspace_argument = pathlib.Path(args.workspace).absolute()
    selection_argument = pathlib.Path(args.selection).absolute()
    protected_roots = _parse_protected_roots(args.protected_root or [])
    run_root = pathlib.Path(args.run_root).absolute()
    _validate_path_isolation(run_root, workspace_argument, protected_roots)
    workspace = workspace_argument.resolve(strict=True)
    selection_path = selection_argument.resolve(strict=True)
    if not _lexically_within(str(selection_path), str(workspace)):
        raise QualificationError("PSC_QUALIFICATION_PATH_OUTSIDE_WORKSPACE")
    guard = QualificationAccessGuard(
        protected_roots=protected_roots,
        allowed_roots=(workspace, run_root),
    )
    selector = _load_selector(selection_path)
    if set(selector["required_protected_root_labels"]) != {
        item["label"] for item in protected_roots
    }:
        raise QualificationError("PSC_QUALIFICATION_PROTECTED_ROOT_SET_INCOMPLETE")
    git_before = _git_state(workspace)
    if args.expected_head and git_before["head"] != args.expected_head:
        raise QualificationError("PSC_QUALIFICATION_HEAD_CHANGED")
    if args.expected_tree and git_before["tree"] != args.expected_tree:
        raise QualificationError("PSC_QUALIFICATION_HEAD_CHANGED")
    if not args.allow_dirty and not git_before["clean"]:
        raise QualificationError("PSC_QUALIFICATION_WORKTREE_DIRTY")

    guard.install()
    # Package metadata enumeration is part of the evidence boundary.  The
    # guard distinguishes existing literal-tilde package directories from DOS
    # 8.3 aliases, so toolchain discovery remains instrumented as well.
    toolchain = _toolchain()
    _ensure_workspace_import_path(workspace)
    tests, categories, loader_errors = _collect_owned_tests(selector)
    if loader_errors:
        raise QualificationError("PSC_QUALIFICATION_LOADER_ERROR")
    test_ids = [item["id"] for item in tests]
    ids_bytes = "".join(f"{test_id}\n" for test_id in test_ids).encode("utf-8")
    ids_hash = _sha256(ids_bytes)
    if len(test_ids) != selector["expected_unique_test_count"]:
        raise QualificationError("PSC_QUALIFICATION_TEST_COUNT_MISMATCH")
    if ids_hash != selector["expected_test_ids_sha256"]:
        raise QualificationError("PSC_QUALIFICATION_TEST_HASH_MISMATCH")
    if args.expected_unique_count is not None and len(test_ids) != (
        args.expected_unique_count
    ):
        raise QualificationError("PSC_QUALIFICATION_TEST_COUNT_MISMATCH")
    if args.expected_id_sha256 and ids_hash != args.expected_id_sha256:
        raise QualificationError("PSC_QUALIFICATION_TEST_HASH_MISMATCH")
    if any(test_id not in set(test_ids) for test_id in selector["priority_test_ids"]):
        raise QualificationError("PSC_QUALIFICATION_PRIORITY_TEST_MISSING")
    coverage_partitions = _coverage_partition_results(selector, tests)

    files = [
        _file_binding(workspace, workspace / RUNNER_RELATIVE_PATH),
        _file_binding(workspace, selection_path),
    ]
    for module in dict.fromkeys(item["module"] for item in tests):
        files.append(_file_binding(workspace, workspace / _module_path(module)))
    if not args.allow_dirty and any(item["git_blob"] is None for item in files):
        raise QualificationError("PSC_QUALIFICATION_SOURCE_NOT_COMMITTED")
    binding = {
        "suite_id": selector["suite_id"],
        "candidate_head": git_before["head"],
        "candidate_tree": git_before["tree"],
        "status_sha256": git_before["status_sha256"],
        "runner_sha256": files[0]["sha256"],
        "runner_git_blob": files[0]["git_blob"],
        "selector_path": files[1]["path"],
        "selector_sha256": files[1]["sha256"],
        "selector_git_blob": files[1]["git_blob"],
        "test_ids_sha256": ids_hash,
        "python_executable_sha256": toolchain["python_executable_sha256"],
        "python_version": toolchain["python_version"],
        "packages": toolchain["packages"],
        "toolchain_sha256": _sha256(_canonical_bytes(toolchain)),
        "protected_roots_sha256": _sha256(_canonical_bytes(protected_roots)),
        "isolation_policy_version": selector["isolation_policy_version"],
    }
    run_key = _collection_run_key(binding)
    git_after = _git_state(workspace)
    if git_after["head"] != git_before["head"] or git_after["tree"] != git_before[
        "tree"
    ]:
        raise QualificationError("PSC_QUALIFICATION_HEAD_CHANGED")
    if not args.allow_dirty and not git_after["clean"]:
        raise QualificationError("PSC_QUALIFICATION_WORKTREE_DIRTY")
    guard_evidence = guard.evidence()
    if (
        guard_evidence["protected_root_attempts"]
        or guard_evidence["unsafe_alias_attempts"]
        or guard_evidence["network_attempts"]
    ):
        raise QualificationError("PSC_QUALIFICATION_ISOLATION_VIOLATION")

    collection = {
        "schema_version": SCHEMA_VERSION,
        "kind": COLLECTION_KIND,
        "suite_id": selector["suite_id"],
        "run_key": run_key,
        "candidate": git_before,
        "selector": selector,
        "selection": {
            "unique_test_count": len(test_ids),
            "test_ids_sha256": ids_hash,
            "test_ids_file": "test-ids.txt",
            "categories": categories,
            "coverage_partitions": coverage_partitions,
            "tests": tests,
            "loader_error_count": 0,
        },
        "source_files": files,
        "toolchain": toolchain,
        "environment": {
            "protected_roots": protected_roots,
            "workspace_allow_subtree": _path_forms(workspace),
            "collection_guard_evidence": guard_evidence,
            "environment_values_captured": False,
        },
        "command": {
            "argv": list(sys.argv),
            "cwd": str(pathlib.Path.cwd()),
        },
        "run_binding": binding,
    }
    run_root.mkdir(parents=True, exist_ok=True)
    _write_immutable(run_root / "test-ids.txt", ids_bytes)
    _write_json(run_root / "collection.json", collection)
    return {
        "run_key": run_key,
        "unique_test_count": len(test_ids),
        "test_ids_sha256": ids_hash,
        "collection": str(run_root / "collection.json"),
    }


def _load_collection(path: pathlib.Path) -> dict[str, Any]:
    collection = _read_json(path)
    _verify_integrity(collection)
    if collection.get("schema_version") != SCHEMA_VERSION or collection.get(
        "kind"
    ) != COLLECTION_KIND:
        raise QualificationError("PSC_QUALIFICATION_COLLECTION_INVALID")
    selection = collection.get("selection")
    if type(selection) is not dict or type(selection.get("tests")) is not list:
        raise QualificationError("PSC_QUALIFICATION_COLLECTION_INVALID")
    ids = [item.get("id") for item in selection["tests"] if type(item) is dict]
    if any(type(test_id) is not str for test_id in ids):
        raise QualificationError("PSC_QUALIFICATION_COLLECTION_INVALID")
    if ids != sorted(ids) or len(ids) != len(set(ids)):
        raise QualificationError("PSC_QUALIFICATION_COLLECTION_INVALID")
    ids_bytes = "".join(f"{test_id}\n" for test_id in ids).encode("utf-8")
    if len(ids) != selection.get("unique_test_count") or _sha256(
        ids_bytes
    ) != selection.get("test_ids_sha256"):
        raise QualificationError("PSC_QUALIFICATION_COLLECTION_INVALID")
    if _collection_run_key(collection.get("run_binding") or {}) != collection.get(
        "run_key"
    ):
        raise QualificationError("PSC_QUALIFICATION_COLLECTION_INVALID")
    binding = collection["run_binding"]
    candidate = collection.get("candidate")
    selector = collection.get("selector")
    source_files = collection.get("source_files")
    toolchain = collection.get("toolchain")
    environment = collection.get("environment")
    if (
        type(candidate) is not dict
        or type(selector) is not dict
        or type(source_files) is not list
        or type(toolchain) is not dict
        or type(environment) is not dict
        or binding.get("suite_id") != collection.get("suite_id")
        or binding.get("candidate_head") != candidate.get("head")
        or binding.get("candidate_tree") != candidate.get("tree")
        or binding.get("status_sha256") != candidate.get("status_sha256")
        or binding.get("test_ids_sha256") != selection.get("test_ids_sha256")
        or selector.get("expected_unique_test_count")
        != selection.get("unique_test_count")
        or selector.get("expected_test_ids_sha256")
        != selection.get("test_ids_sha256")
        or selection.get("coverage_partitions")
        != _coverage_partition_results(selector, selection["tests"])
        or binding.get("toolchain_sha256")
        != _sha256(_canonical_bytes(toolchain))
        or binding.get("protected_roots_sha256")
        != _sha256(_canonical_bytes(environment.get("protected_roots", [])))
        or {
            item.get("label")
            for item in environment.get("protected_roots", [])
            if type(item) is dict
        }
        != REQUIRED_PROTECTED_ROOT_LABELS
        or type(environment.get("workspace_allow_subtree")) is not dict
        or type(environment.get("collection_guard_evidence")) is not dict
        or environment["collection_guard_evidence"].get(
            "protected_root_attempts"
        )
        != []
        or environment["collection_guard_evidence"].get("unsafe_alias_attempts")
        != []
        or environment["collection_guard_evidence"].get("network_attempts") != []
    ):
        raise QualificationError("PSC_QUALIFICATION_COLLECTION_INVALID")
    files_by_path = {
        item.get("path"): item for item in source_files if type(item) is dict
    }
    runner_binding = files_by_path.get(RUNNER_RELATIVE_PATH)
    selector_binding = files_by_path.get(binding.get("selector_path"))
    if (
        type(runner_binding) is not dict
        or type(selector_binding) is not dict
        or binding.get("runner_sha256") != runner_binding.get("sha256")
        or binding.get("runner_git_blob") != runner_binding.get("git_blob")
        or binding.get("selector_sha256") != selector_binding.get("sha256")
        or binding.get("selector_git_blob") != selector_binding.get("git_blob")
    ):
        raise QualificationError("PSC_QUALIFICATION_COLLECTION_INVALID")
    return collection


def _plan_document(collection: Mapping[str, Any]) -> dict[str, Any]:
    selector = collection["selector"]
    test_rows = collection["selection"]["tests"]
    by_id = {item["id"]: item for item in test_rows}
    used: set[str] = set()
    raw_shards: list[dict[str, Any]] = []
    priority = selector["priority_test_ids"]
    if priority:
        raw_shards.append(
            {"kind": "priority", "module": None, "test_ids": list(priority)}
        )
        used.update(priority)
    for module in selector["singleton_modules"]:
        for test_id in sorted(
            item["id"]
            for item in test_rows
            if item["module"] == module and item["id"] not in used
        ):
            raw_shards.append(
                {"kind": "singleton", "module": module, "test_ids": [test_id]}
            )
            used.add(test_id)
    module_order = [
        module
        for category in selector["categories"]
        for module in category["modules"]
        if module not in selector["singleton_modules"]
    ]
    for module in module_order:
        ids = sorted(
            item["id"]
            for item in test_rows
            if item["module"] == module and item["id"] not in used
        )
        if ids:
            raw_shards.append({"kind": "module", "module": module, "test_ids": ids})
            used.update(ids)
    if used != set(by_id):
        raise QualificationError("PSC_QUALIFICATION_PLAN_INCOMPLETE")
    shards: list[dict[str, Any]] = []
    for index, shard in enumerate(raw_shards, 1):
        ids_hash = _sha256(
            "".join(f"{test_id}\n" for test_id in shard["test_ids"]).encode(
                "utf-8"
            )
        )
        shards.append(
            {
                "ordinal": index,
                "id": f"shard-{index:04d}-{ids_hash[:12]}",
                "kind": shard["kind"],
                "module": shard["module"],
                "test_count": len(shard["test_ids"]),
                "test_ids_sha256": ids_hash,
                "test_ids": shard["test_ids"],
            }
        )
    return _with_integrity(
        {
            "schema_version": SCHEMA_VERSION,
            "kind": PLAN_KIND,
            "suite_id": collection["suite_id"],
            "run_key": collection["run_key"],
            "candidate": collection["candidate"],
            "unique_test_count": len(by_id),
            "test_ids_sha256": collection["selection"]["test_ids_sha256"],
            "shard_count": len(shards),
            "sequential": True,
            "shards": shards,
        }
    )


def _plan_command(args: argparse.Namespace) -> dict[str, Any]:
    run_root = pathlib.Path(args.run_root).absolute()
    workspace_argument = pathlib.Path(args.workspace).absolute()
    protected_roots = _parse_protected_roots(args.protected_root or [])
    _validate_path_isolation(run_root, workspace_argument, protected_roots)
    workspace = workspace_argument.resolve(strict=True)
    collection = _load_collection(run_root / "collection.json")
    _assert_protected_root_binding(protected_roots, collection)
    _assert_git_binding(workspace, collection["candidate"])
    plan = _plan_document(collection)
    _write_immutable(run_root / "plan.json", _canonical_bytes(plan))
    return {
        "run_key": collection["run_key"],
        "shard_count": plan["shard_count"],
        "unique_test_count": plan["unique_test_count"],
        "plan": str(run_root / "plan.json"),
    }


def _load_plan(path: pathlib.Path, collection: Mapping[str, Any]) -> dict[str, Any]:
    plan = _read_json(path)
    _verify_integrity(plan)
    expected = _plan_document(collection)
    if _canonical_bytes(plan) != _canonical_bytes(expected):
        raise QualificationError("PSC_QUALIFICATION_PLAN_INVALID")
    return plan


def _error_fingerprint(error: tuple[type[BaseException], BaseException, Any]) -> dict[str, str]:
    rendered = "".join(traceback.format_exception(*error)).encode(
        "utf-8", errors="replace"
    )
    return {
        "exception_type": error[0].__name__,
        "traceback_sha256": _sha256(rendered),
    }


class QualificationTestResult(unittest.TestResult):
    """A non-verbose result that counts every parent and subtest outcome."""

    def __init__(self, expected_ids: Sequence[str]) -> None:
        super().__init__()
        self.expected_ids = set(expected_ids)
        self.records: dict[str, dict[str, Any]] = {
            test_id: {
                "id": test_id,
                "outcome": None,
                "duration_ns": None,
                "subtests": [],
            }
            for test_id in expected_ids
        }
        self.started_ns: dict[str, int] = {}
        self.fixture_errors: list[dict[str, str]] = []
        self.unexpected_parent_ids: list[str] = []

    @staticmethod
    def _parent_id(test: Any) -> str:
        parent = getattr(test, "test_case", None)
        return parent.id() if parent is not None else test.id()

    def _record(self, test: Any) -> dict[str, Any] | None:
        test_id = self._parent_id(test)
        record = self.records.get(test_id)
        if record is None:
            self.unexpected_parent_ids.append(test_id)
        return record

    def startTest(self, test: unittest.TestCase) -> None:  # noqa: N802
        super().startTest(test)
        test_id = test.id()
        self.started_ns[test_id] = time.perf_counter_ns()
        if test_id not in self.records:
            self.unexpected_parent_ids.append(test_id)

    def stopTest(self, test: unittest.TestCase) -> None:  # noqa: N802
        test_id = test.id()
        record = self.records.get(test_id)
        started = self.started_ns.pop(test_id, None)
        if record is not None and started is not None:
            record["duration_ns"] = time.perf_counter_ns() - started
        super().stopTest(test)

    def addSuccess(self, test: unittest.TestCase) -> None:  # noqa: N802
        super().addSuccess(test)
        record = self._record(test)
        if record is not None:
            record["outcome"] = "success"

    def addFailure(self, test: unittest.TestCase, err: Any) -> None:  # noqa: N802
        super().addFailure(test, err)
        record = self._record(test)
        if record is not None:
            record["outcome"] = "failure"
            record["error"] = _error_fingerprint(err)

    def addError(self, test: unittest.TestCase, err: Any) -> None:  # noqa: N802
        super().addError(test, err)
        record = self._record(test)
        if record is not None:
            record["outcome"] = "error"
            record["error"] = _error_fingerprint(err)
        else:
            self.fixture_errors.append(
                {"id_sha256": _sha256(test.id().encode("utf-8")), **_error_fingerprint(err)}
            )

    def addSkip(self, test: unittest.TestCase, reason: str) -> None:  # noqa: N802
        super().addSkip(test, reason)
        record = self._record(test)
        if record is None:
            return
        if getattr(test, "test_case", None) is not None:
            record["subtests"].append(
                {
                    "ordinal": len(record["subtests"]) + 1,
                    "outcome": "skip",
                    "reason_sha256": _sha256(reason.encode("utf-8")),
                }
            )
            record["outcome"] = "subtest_skip"
        else:
            record["outcome"] = "skip"
            record["reason_sha256"] = _sha256(reason.encode("utf-8"))

    def addExpectedFailure(self, test: unittest.TestCase, err: Any) -> None:  # noqa: N802
        super().addExpectedFailure(test, err)
        record = self._record(test)
        if record is not None:
            record["outcome"] = "expected_failure"
            record["error"] = _error_fingerprint(err)

    def addUnexpectedSuccess(self, test: unittest.TestCase) -> None:  # noqa: N802
        super().addUnexpectedSuccess(test)
        record = self._record(test)
        if record is not None:
            record["outcome"] = "unexpected_success"

    def addSubTest(self, test: unittest.TestCase, subtest: Any, err: Any) -> None:  # noqa: N802
        super().addSubTest(test, subtest, err)
        record = self._record(test)
        if record is None:
            return
        if err is None:
            outcome = "success"
            detail: dict[str, Any] = {}
        elif issubclass(err[0], test.failureException):
            outcome = "failure"
            detail = {"error": _error_fingerprint(err)}
            record["outcome"] = "subtest_failure"
        else:
            outcome = "error"
            detail = {"error": _error_fingerprint(err)}
            record["outcome"] = "subtest_error"
        record["subtests"].append(
            {
                "ordinal": len(record["subtests"]) + 1,
                "outcome": outcome,
                **detail,
            }
        )

    def summary(self) -> dict[str, Any]:
        parent_counts = {
            name: sum(1 for record in self.records.values() if record["outcome"] == name)
            for name in (
                "success",
                "failure",
                "error",
                "skip",
                "expected_failure",
                "unexpected_success",
                "subtest_failure",
                "subtest_error",
                "subtest_skip",
            )
        }
        parent_counts["not_run"] = sum(
            1 for record in self.records.values() if record["outcome"] is None
        )
        subtest_counts = {
            name: sum(
                1
                for record in self.records.values()
                for subtest in record["subtests"]
                if subtest["outcome"] == name
            )
            for name in ("success", "failure", "error", "skip")
        }
        subtest_counts["total"] = sum(subtest_counts.values())
        return {
            "tests_run": self.testsRun,
            "parent": parent_counts,
            "subtests": subtest_counts,
            "fixture_error_count": len(self.fixture_errors),
            "unexpected_parent_id_count": len(set(self.unexpected_parent_ids)),
        }


class _StreamingSecretDetector:
    """Incrementally recognize SECRET_PATTERN even across write boundaries."""

    _keyword = re.compile(
        r"(?i)(?:authorization|api[_-]?key|password|passwd|secret|token)$"
    )

    def __init__(self) -> None:
        self.state = "search"
        self.tail = ""
        self.value_characters = 0
        self.found = False

    def feed(self, text: str) -> None:
        for character in text:
            self._feed_character(character)

    def _search_character(self, character: str) -> None:
        self.tail = (self.tail + character)[-16:]
        match = self._keyword.search(self.tail)
        if match is not None:
            self.state = "after_keyword"
            self.tail = ""

    def _feed_character(self, character: str) -> None:
        if self.found:
            return
        if self.state == "search":
            self._search_character(character)
            return
        if self.state == "after_keyword":
            if character.isspace():
                return
            if character in ":=":
                self.state = "before_value"
                return
            self.state = "search"
            self._search_character(character)
            return
        if self.state == "before_value":
            if character.isspace():
                return
            if character in ",;":
                self.state = "search"
                self._search_character(character)
                return
            self.state = "value"
            self.value_characters = 1
            return
        if character.isspace() or character in ",;":
            self.state = "search"
            self.value_characters = 0
            self._search_character(character)
            return
        self.value_characters += 1
        if self.value_characters >= 4:
            self.found = True


class _BoundedOutputCapture(io.TextIOBase):
    """Hash a stream while retaining only bounded, secret-safe text."""

    def __init__(self, *, excerpt_limit: int = 2048) -> None:
        super().__init__()
        self.excerpt_limit = excerpt_limit
        self._head_limit = excerpt_limit // 2
        self._tail_limit = excerpt_limit - self._head_limit
        self._head = ""
        self._tail = ""
        self._character_count = 0
        self._byte_size = 0
        self._digest = hashlib.sha256()
        self._detector = _StreamingSecretDetector()
        self._decoder = codecs.getincrementaldecoder("utf-8")("replace")
        self._decoder_finalized = False

    @property
    def encoding(self) -> str:  # type: ignore[override]
        return "utf-8"

    def writable(self) -> bool:
        return True

    def _retain_text(self, text: str) -> None:
        if not text:
            return
        self._detector.feed(text)
        self._character_count += len(text)
        if len(self._head) < self._head_limit:
            remaining = self._head_limit - len(self._head)
            self._head += text[:remaining]
        self._tail = (self._tail + text)[-self._tail_limit :]

    def write(self, text: str) -> int:
        if not isinstance(text, str):
            raise TypeError("text output required")
        data = text.encode("utf-8", errors="replace")
        self._digest.update(data)
        self._byte_size += len(data)
        self._retain_text(text)
        return len(text)

    def write_bytes(self, data: bytes) -> None:
        self._digest.update(data)
        self._byte_size += len(data)
        self._retain_text(self._decoder.decode(data, final=False))

    def snapshot(self, replacements: Mapping[str, str]) -> dict[str, Any]:
        if not self._decoder_finalized:
            self._retain_text(self._decoder.decode(b"", final=True))
            self._decoder_finalized = True
        if self._character_count <= self.excerpt_limit:
            missing = max(0, self._character_count - len(self._head))
            candidate = self._head + (self._tail[-missing:] if missing else "")
        else:
            candidate = self._head + "\n...\n" + self._tail
        for source, replacement in replacements.items():
            candidate = candidate.replace(source, replacement)
        # Excerpts are diagnostic convenience, not a security boundary.  Do
        # not persist arbitrary test output even when the bounded sample does
        # not match the conservative secret detector.
        status = (
            "omitted_secret_pattern"
            if self._detector.found or SECRET_PATTERN.search(candidate)
            else "omitted_by_policy"
        )
        return {
            "byte_size": self._byte_size,
            "sha256": self._digest.hexdigest(),
            "excerpt_status": status,
            "excerpt": None,
        }


def _run_process_captured(
    command: Sequence[str],
    *,
    cwd: pathlib.Path,
    environment: Mapping[str, str],
    replacements: Mapping[str, str],
) -> tuple[int, int, dict[str, Any], dict[str, Any]]:
    stdout_capture = _BoundedOutputCapture()
    stderr_capture = _BoundedOutputCapture()
    started = time.perf_counter_ns()
    process = subprocess.Popen(
        list(command),
        cwd=str(cwd),
        env=dict(environment),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    errors: list[BaseException] = []

    def drain(stream: Any, capture: _BoundedOutputCapture) -> None:
        try:
            while True:
                chunk = stream.read(64 * 1024)
                if not chunk:
                    break
                capture.write_bytes(chunk)
        except BaseException as exc:  # pragma: no cover - defensive pipe failure
            errors.append(exc)
        finally:
            try:
                stream.close()
            except Exception:
                pass

    threads = [
        threading.Thread(target=drain, args=(process.stdout, stdout_capture), daemon=True),
        threading.Thread(target=drain, args=(process.stderr, stderr_capture), daemon=True),
    ]
    for thread in threads:
        thread.start()
    try:
        return_code = process.wait()
    except BaseException:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        raise
    finally:
        for thread in threads:
            thread.join()
    duration = time.perf_counter_ns() - started
    if errors:
        raise QualificationError("PSC_QUALIFICATION_PROCESS_CAPTURE_FAILED")
    return (
        return_code,
        duration,
        stdout_capture.snapshot(replacements),
        stderr_capture.snapshot(replacements),
    )


def _safe_worker_environment(
    run_root: pathlib.Path, shard_id: str, attempt: int
) -> dict[str, str]:
    shard_root = run_root / "work" / shard_id / f"attempt-{attempt:04d}"
    temp_root = shard_root / "temp"
    home_root = shard_root / "home"
    temp_root.mkdir(parents=True, exist_ok=True)
    home_root.mkdir(parents=True, exist_ok=True)
    environment: dict[str, str] = {}
    for name in ("SystemRoot", "WINDIR", "ComSpec", "PATHEXT"):
        value = os.environ.get(name)
        if value:
            environment[name] = value
    executable_dirs = [str(pathlib.Path(sys.executable).parent)]
    system_root = os.environ.get("SystemRoot") or os.environ.get("WINDIR")
    if system_root:
        executable_dirs.append(str(pathlib.Path(system_root) / "System32"))
    for program in ("git", "powershell", "pwsh"):
        resolved = shutil.which(program)
        if resolved:
            executable_dirs.append(str(pathlib.Path(resolved).parent))
    environment.update(
        {
            "PATH": os.pathsep.join(dict.fromkeys(executable_dirs)),
            "TEMP": str(temp_root),
            "TMP": str(temp_root),
            "HOME": str(home_root),
            "USERPROFILE": str(home_root),
            "PYTHONUTF8": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONHASHSEED": "0",
            "GIT_CONFIG_GLOBAL": "NUL" if os.name == "nt" else "/dev/null",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_AUTHOR_NAME": "PSC Qualification",
            "GIT_AUTHOR_EMAIL": "psc-qualification@example.invalid",
            "GIT_COMMITTER_NAME": "PSC Qualification",
            "GIT_COMMITTER_EMAIL": "psc-qualification@example.invalid",
        }
    )
    return environment


def _worker_command(args: argparse.Namespace) -> dict[str, Any]:
    workspace_argument = pathlib.Path(args.workspace).absolute()
    protected_roots = _parse_protected_roots(args.protected_root or [])
    workspace = workspace_argument.resolve(strict=True)
    collection_argument = pathlib.Path(args.collection).absolute()
    run_root = collection_argument.parent
    _validate_path_isolation(run_root, workspace, protected_roots)
    collection_path = collection_argument.resolve(strict=True)
    plan_argument = pathlib.Path(args.plan).absolute()
    plan_path = plan_argument.resolve(strict=True)
    output_path = pathlib.Path(args.output).absolute()
    if plan_path != (run_root / "plan.json").resolve(strict=True):
        raise QualificationError("PSC_QUALIFICATION_PLAN_PATH_INVALID")
    collection = _load_collection(collection_path)
    _assert_protected_root_binding(protected_roots, collection)
    _require_path_below(output_path, run_root)
    plan = _load_plan(plan_path, collection)
    shard = next((item for item in plan["shards"] if item["id"] == args.shard), None)
    if shard is None:
        raise QualificationError("PSC_QUALIFICATION_SHARD_UNKNOWN")
    expected_parent = run_root / "attempts" / shard["id"]
    if (
        output_path.parent != expected_parent
        or re.fullmatch(r"attempt-\d{4}\.result\.json", output_path.name) is None
    ):
        raise QualificationError("PSC_QUALIFICATION_WORKER_OUTPUT_INVALID")
    before = _assert_git_binding(workspace, collection["candidate"])
    guard = QualificationAccessGuard(
        protected_roots=protected_roots,
        allowed_roots=(workspace, run_root),
    )
    guard.install()
    _ensure_workspace_import_path(workspace)
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromNames(shard["test_ids"])
    loaded_ids = [test.id() for test in _flatten_suite(suite)]
    _install_candidate_native_guards(guard)
    loader_error_hashes = [_sha256(error.encode("utf-8")) for error in loader.errors]
    load_exact = loaded_ids == shard["test_ids"]
    result = QualificationTestResult(shard["test_ids"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    stdout_capture = _BoundedOutputCapture()
    stderr_capture = _BoundedOutputCapture()
    started = time.perf_counter_ns()
    execution_exception: dict[str, str] | None = None
    if not loader.errors and load_exact:
        try:
            with contextlib.redirect_stdout(stdout_capture), contextlib.redirect_stderr(
                stderr_capture
            ):
                suite.run(result)
        except BaseException as exc:  # fail closed even for non-TestCase failures
            rendered = "".join(
                traceback.format_exception(type(exc), exc, exc.__traceback__)
            ).encode("utf-8", errors="replace")
            execution_exception = {
                "exception_type": type(exc).__name__,
                "traceback_sha256": _sha256(rendered),
            }
    duration = time.perf_counter_ns() - started
    after = _git_state(workspace)
    summary = result.summary()
    prohibited = (
        len(loader.errors)
        + (0 if load_exact else 1)
        + len(result.fixture_errors)
        + len(set(result.unexpected_parent_ids))
        + len(guard.protected_attempts)
        + len(guard.unsafe_alias_attempts)
        + len(guard.network_attempts)
        + (0 if execution_exception is None else 1)
        + (0 if after == before and after["clean"] else 1)
        + sum(
            summary["parent"][name]
            for name in (
                "failure",
                "error",
                "skip",
                "expected_failure",
                "unexpected_success",
                "subtest_failure",
                "subtest_error",
                "subtest_skip",
                "not_run",
            )
        )
        + summary["subtests"]["failure"]
        + summary["subtests"]["error"]
        + summary["subtests"]["skip"]
    )
    replacements = {str(workspace): "<WORKSPACE>", str(output_path.parent): "<RUN_ROOT>"}
    document = {
        "schema_version": SCHEMA_VERSION,
        "kind": WORKER_KIND,
        "suite_id": collection["suite_id"],
        "run_key": collection["run_key"],
        "candidate": collection["candidate"],
        "shard": shard,
        "status": "PASS" if prohibited == 0 else "FAIL",
        "duration_ns": duration,
        "loader": {
            "requested_ids_equal_loaded_ids": load_exact,
            "loaded_test_count": len(loaded_ids),
            "error_count": len(loader.errors),
            "error_hashes": loader_error_hashes,
        },
        "outcomes": summary,
        "tests": [result.records[test_id] for test_id in shard["test_ids"]],
        "fixture_errors": result.fixture_errors,
        "unexpected_parent_id_count": len(set(result.unexpected_parent_ids)),
        "guard_evidence": guard.evidence(),
        "execution_exception": execution_exception,
        "git_before": before,
        "git_after": after,
        "captured_output": {
            "stdout": stdout_capture.snapshot(replacements),
            "stderr": stderr_capture.snapshot(replacements),
        },
        "safe_environment": {
            name: os.environ.get(name)
            for name in (
                "PYTHONUTF8",
                "PYTHONDONTWRITEBYTECODE",
                "PYTHONHASHSEED",
                "GIT_CONFIG_NOSYSTEM",
            )
        },
    }
    written = _write_json(output_path, document)
    return {
        "status": written["status"],
        "result": str(output_path),
        "sha256": _sha256(_read_bytes(output_path)),
    }


def _load_worker_result(
    path: pathlib.Path,
    collection: Mapping[str, Any],
    shard: Mapping[str, Any],
) -> dict[str, Any]:
    result = _read_json(path)
    _verify_integrity(result)
    if (
        result.get("kind") != WORKER_KIND
        or result.get("run_key") != collection["run_key"]
        or result.get("shard") != shard
        or result.get("candidate") != collection["candidate"]
    ):
        raise QualificationError("PSC_QUALIFICATION_WORKER_RESULT_INVALID")
    if result.get("status") not in {"PASS", "FAIL"}:
        raise QualificationError("PSC_QUALIFICATION_WORKER_RESULT_INVALID")
    return result


def _validate_worker_pass(result: Mapping[str, Any]) -> None:
    shard = result["shard"]
    loader = result.get("loader")
    outcomes = result.get("outcomes")
    tests = result.get("tests")
    if (
        result.get("status") != "PASS"
        or type(loader) is not dict
        or loader.get("requested_ids_equal_loaded_ids") is not True
        or loader.get("loaded_test_count") != shard["test_count"]
        or loader.get("error_count") != 0
        or loader.get("error_hashes") != []
        or type(outcomes) is not dict
        or type(outcomes.get("parent")) is not dict
        or type(outcomes.get("subtests")) is not dict
        or outcomes.get("tests_run") != shard["test_count"]
        or outcomes.get("fixture_error_count") != 0
        or outcomes.get("unexpected_parent_id_count") != 0
        or type(tests) is not list
        or [item.get("id") for item in tests if type(item) is dict]
        != shard["test_ids"]
        or any(
            type(item) is not dict or item.get("outcome") != "success"
            for item in tests
        )
        or result.get("fixture_errors") != []
        or result.get("unexpected_parent_id_count") != 0
        or type(result.get("guard_evidence")) is not dict
        or result["guard_evidence"].get("protected_root_attempts") != []
        or result["guard_evidence"].get("unsafe_alias_attempts") != []
        or result["guard_evidence"].get("network_attempts") != []
        or result.get("execution_exception") is not None
        or result.get("git_before") != result.get("candidate")
        or result.get("git_after") != result.get("candidate")
    ):
        raise QualificationError("PSC_QUALIFICATION_WORKER_PASS_INVALID")
    prohibited_parent = {
        "failure",
        "error",
        "skip",
        "expected_failure",
        "unexpected_success",
        "subtest_failure",
        "subtest_error",
        "subtest_skip",
        "not_run",
    }
    if outcomes["parent"].get("success") != shard["test_count"] or any(
        outcomes["parent"].get(name, 0) != 0 for name in prohibited_parent
    ):
        raise QualificationError("PSC_QUALIFICATION_WORKER_PASS_INVALID")
    if any(
        outcomes["subtests"].get(name, 0) != 0
        for name in ("failure", "error", "skip")
    ):
        raise QualificationError("PSC_QUALIFICATION_WORKER_PASS_INVALID")
    recorded_subtests = sum(
        len(item.get("subtests", [])) for item in tests if type(item) is dict
    )
    if (
        outcomes["subtests"].get("total") != recorded_subtests
        or outcomes["subtests"].get("success") != recorded_subtests
    ):
        raise QualificationError("PSC_QUALIFICATION_WORKER_PASS_INVALID")


def _validate_controller(
    controller: Mapping[str, Any],
    *,
    collection: Mapping[str, Any],
    shard: Mapping[str, Any],
    attempt: int,
    result_path: pathlib.Path,
    result: Mapping[str, Any],
    require_pass: bool,
) -> None:
    _verify_integrity(controller)
    if (
        controller.get("kind") != CONTROLLER_KIND
        or controller.get("run_key") != collection["run_key"]
        or controller.get("candidate") != collection["candidate"]
        or controller.get("shard_id") != shard["id"]
        or controller.get("attempt") != attempt
        or controller.get("worker_result_sha256")
        != _sha256(_read_bytes(result_path))
        or controller.get("status") not in {"PASS", "FAIL"}
        or type(controller.get("return_code")) is not int
        or not re.fullmatch(r"[0-9a-f]{32}", str(controller.get("lease_owner_id")))
        or controller.get("lease_mechanism") not in {"msvcrt_byte_range", "flock"}
    ):
        raise QualificationError("PSC_QUALIFICATION_CONTROLLER_INVALID")
    if controller["status"] == "PASS":
        if controller["return_code"] != 0 or result.get("status") != "PASS":
            raise QualificationError("PSC_QUALIFICATION_CONTROLLER_INVALID")
        _validate_worker_pass(result)
    elif require_pass:
        raise QualificationError("PSC_QUALIFICATION_CONTROLLER_INVALID")


def _validate_start_receipt(
    path: pathlib.Path,
    *,
    collection: Mapping[str, Any],
    shard: Mapping[str, Any],
    attempt: int,
) -> None:
    try:
        start = _read_json(path)
    except QualificationError as exc:
        raise QualificationError("PSC_QUALIFICATION_START_RECEIPT_INVALID") from exc
    _verify_integrity(start)
    if (
        start.get("kind") != START_KIND
        or start.get("run_key") != collection["run_key"]
        or start.get("candidate") != collection["candidate"]
        or start.get("shard_id") != shard["id"]
        or start.get("attempt") != attempt
        or type(start.get("command")) is not list
        or not start["command"]
        or not re.fullmatch(r"[0-9a-f]{32}", str(start.get("lease_owner_id")))
        or start.get("lease_mechanism") not in {"msvcrt_byte_range", "flock"}
    ):
        raise QualificationError("PSC_QUALIFICATION_START_RECEIPT_INVALID")


def _existing_attempts(attempt_root: pathlib.Path) -> list[int]:
    result: set[int] = set()
    try:
        metadata = os.lstat(attempt_root)
    except FileNotFoundError:
        return []
    except OSError as exc:
        raise QualificationError("PSC_QUALIFICATION_ARTIFACT_UNREADABLE") from exc
    if stat.S_ISDIR(metadata.st_mode):
        _assert_existing_chain_not_redirected(attempt_root)
        for path in attempt_root.iterdir():
            match = re.match(
                r"attempt-(\d{4})\.(?:start|result|controller|abandonment)\.json$",
                path.name,
            )
            if match:
                result.add(int(match.group(1)))
    else:
        raise QualificationError("PSC_QUALIFICATION_ATTEMPT_HISTORY_INVALID")
    return sorted(result)


def _validate_abandonment(
    path: pathlib.Path,
    *,
    start_path: pathlib.Path,
    collection: Mapping[str, Any],
    shard: Mapping[str, Any],
    attempt: int,
) -> dict[str, Any]:
    abandonment = _read_json(path)
    _verify_integrity(abandonment)
    if (
        abandonment.get("kind") != ABANDONMENT_KIND
        or abandonment.get("run_key") != collection["run_key"]
        or abandonment.get("candidate") != collection["candidate"]
        or abandonment.get("shard_id") != shard["id"]
        or abandonment.get("attempt") != attempt
        or abandonment.get("start_receipt_sha256")
        != _sha256(_read_bytes(start_path))
        or abandonment.get("reason")
        not in {
            "controller_interrupted",
            "worker_result_missing",
            "worker_reported_failure",
        }
        or not re.fullmatch(
            r"[0-9a-f]{32}", str(abandonment.get("lease_owner_id"))
        )
        or abandonment.get("lease_mechanism")
        not in {"msvcrt_byte_range", "flock"}
        or type(abandonment.get("observed_artifacts")) is not list
    ):
        raise QualificationError("PSC_QUALIFICATION_ABANDONMENT_INVALID")
    expected_observed: list[dict[str, Any]] = []
    for suffix in ("result", "controller"):
        artifact = path.parent / f"attempt-{attempt:04d}.{suffix}.json"
        raw = _read_existing_immutable(artifact)
        if raw is not None:
            expected_observed.append(
                {
                    "name": artifact.name,
                    "byte_size": len(raw),
                    "sha256": _sha256(raw),
                }
            )
    if abandonment["observed_artifacts"] != expected_observed:
        raise QualificationError("PSC_QUALIFICATION_ABANDONMENT_INVALID")
    return abandonment


def _write_abandonment(
    attempt_root: pathlib.Path,
    *,
    collection: Mapping[str, Any],
    shard: Mapping[str, Any],
    attempt: int,
    lease: Mapping[str, str],
    reason: str,
) -> dict[str, Any]:
    start_path = attempt_root / f"attempt-{attempt:04d}.start.json"
    _validate_start_receipt(
        start_path, collection=collection, shard=shard, attempt=attempt
    )
    observed: list[dict[str, Any]] = []
    for suffix in ("result", "controller"):
        artifact = attempt_root / f"attempt-{attempt:04d}.{suffix}.json"
        raw = _read_existing_immutable(artifact)
        if raw is not None:
            observed.append(
                {
                    "name": artifact.name,
                    "byte_size": len(raw),
                    "sha256": _sha256(raw),
                }
            )
    document = _write_json(
        attempt_root / f"attempt-{attempt:04d}.abandonment.json",
        {
            "schema_version": SCHEMA_VERSION,
            "kind": ABANDONMENT_KIND,
            "run_key": collection["run_key"],
            "candidate": collection["candidate"],
            "shard_id": shard["id"],
            "attempt": attempt,
            "start_receipt_sha256": _sha256(_read_bytes(start_path)),
            "lease_owner_id": lease["owner_id"],
            "lease_mechanism": lease["mechanism"],
            "reason": reason,
            "observed_artifacts": observed,
        },
    )
    return document


def _inspect_attempt_history(
    attempt_root: pathlib.Path,
    *,
    collection: Mapping[str, Any],
    shard: Mapping[str, Any],
) -> dict[str, Any]:
    attempts = _existing_attempts(attempt_root)
    if attempts and attempts != list(range(1, max(attempts) + 1)):
        raise QualificationError("PSC_QUALIFICATION_ATTEMPT_HISTORY_INVALID")
    completed: list[tuple[int, pathlib.Path, dict[str, Any], dict[str, Any]]] = []
    abandoned: list[dict[str, Any]] = []
    unsettled: list[int] = []
    terminal_failure = False
    for attempt in attempts:
        start_path = attempt_root / f"attempt-{attempt:04d}.start.json"
        result_path = attempt_root / f"attempt-{attempt:04d}.result.json"
        controller_path = attempt_root / f"attempt-{attempt:04d}.controller.json"
        abandonment_path = attempt_root / f"attempt-{attempt:04d}.abandonment.json"
        _validate_start_receipt(
            start_path, collection=collection, shard=shard, attempt=attempt
        )
        abandonment_present = _immutable_exists(abandonment_path)
        result_present = _immutable_exists(result_path)
        controller_present = _immutable_exists(controller_path)
        if abandonment_present:
            if controller_present:
                raise QualificationError("PSC_QUALIFICATION_ATTEMPT_HISTORY_INVALID")
            abandonment = _validate_abandonment(
                abandonment_path,
                start_path=start_path,
                collection=collection,
                shard=shard,
                attempt=attempt,
            )
            abandoned.append(
                {
                    "attempt": attempt,
                    "receipt_sha256": _sha256(_read_bytes(abandonment_path)),
                    "reason": abandonment["reason"],
                }
            )
            continue
        if result_present and controller_present:
            result = _load_worker_result(result_path, collection, shard)
            controller = _read_json(controller_path)
            _validate_controller(
                controller,
                collection=collection,
                shard=shard,
                attempt=attempt,
                result_path=result_path,
                result=result,
                require_pass=False,
            )
            completed.append((attempt, result_path, result, controller))
            if result["status"] != "PASS" or controller["status"] != "PASS":
                terminal_failure = True
            continue
        if controller_present and not result_present:
            raise QualificationError("PSC_QUALIFICATION_ATTEMPT_HISTORY_INVALID")
        unsettled.append(attempt)
    if len(completed) > 1:
        raise QualificationError("PSC_QUALIFICATION_ATTEMPT_HISTORY_INVALID")
    return {
        "attempts": attempts,
        "completed": completed,
        "abandoned": abandoned,
        "unsettled": unsettled,
        "terminal_failure": terminal_failure,
    }


def _protected_root_cli(protected_roots: Sequence[Mapping[str, str]]) -> list[str]:
    arguments: list[str] = []
    for item in protected_roots:
        if item["label"] == "live_prompt":
            continue
        arguments.extend(
            ["--protected-root", f"{item['label']}={item['lexical_path']}"]
        )
    return arguments


def _run_command(args: argparse.Namespace) -> dict[str, Any]:
    run_root = pathlib.Path(args.run_root).absolute()
    workspace_argument = pathlib.Path(args.workspace).absolute()
    protected_roots = _parse_protected_roots(args.protected_root or [])
    _validate_path_isolation(run_root, workspace_argument, protected_roots)
    workspace = workspace_argument.resolve(strict=True)
    collection = _load_collection(run_root / "collection.json")
    _assert_protected_root_binding(protected_roots, collection)
    plan = _load_plan(run_root / "plan.json", collection)
    _assert_git_binding(workspace, collection["candidate"])
    with _run_key_lease(run_root, collection["run_key"]) as lease:
        completed_count = 0
        reused_count = 0
        runner = pathlib.Path(__file__).resolve(strict=True)
        for shard in plan["shards"]:
            _assert_git_binding(workspace, collection["candidate"])
            attempt_root = run_root / "attempts" / shard["id"]
            history = _inspect_attempt_history(
                attempt_root, collection=collection, shard=shard
            )
            if history["terminal_failure"]:
                raise QualificationError("PSC_QUALIFICATION_TERMINAL_SHARD_FAILURE")
            for incomplete in history["unsettled"]:
                partial_result = (
                    attempt_root / f"attempt-{incomplete:04d}.result.json"
                )
                reason = "controller_interrupted"
                if _immutable_exists(partial_result):
                    partial = _load_worker_result(partial_result, collection, shard)
                    if partial["status"] == "FAIL":
                        reason = "worker_reported_failure"
                _write_abandonment(
                    attempt_root,
                    collection=collection,
                    shard=shard,
                    attempt=incomplete,
                    lease=lease,
                    reason=reason,
                )
            if history["unsettled"]:
                history = _inspect_attempt_history(
                    attempt_root, collection=collection, shard=shard
                )
            if history["terminal_failure"] or any(
                item["reason"] == "worker_reported_failure"
                for item in history["abandoned"]
            ):
                raise QualificationError("PSC_QUALIFICATION_TERMINAL_SHARD_FAILURE")
            if history["completed"]:
                reused_count += 1
                completed_count += 1
                continue
            attempts = history["attempts"]
            attempt = (max(attempts) + 1) if attempts else 1
            result_path = attempt_root / f"attempt-{attempt:04d}.result.json"
            start_path = attempt_root / f"attempt-{attempt:04d}.start.json"
            controller_path = attempt_root / f"attempt-{attempt:04d}.controller.json"
            command = [
                sys.executable,
                "-B",
                str(runner),
                "_worker",
                "--workspace",
                str(workspace),
                "--collection",
                str(run_root / "collection.json"),
                "--plan",
                str(run_root / "plan.json"),
                "--shard",
                shard["id"],
                "--output",
                str(result_path),
                *_protected_root_cli(protected_roots),
            ]
            _write_json(
                start_path,
                {
                    "schema_version": SCHEMA_VERSION,
                    "kind": START_KIND,
                    "run_key": collection["run_key"],
                    "candidate": collection["candidate"],
                    "shard_id": shard["id"],
                    "attempt": attempt,
                    "command": command,
                    "lease_owner_id": lease["owner_id"],
                    "lease_mechanism": lease["mechanism"],
                },
            )
            environment = _safe_worker_environment(run_root, shard["id"], attempt)
            replacements = {
                str(workspace): "<WORKSPACE>",
                str(run_root): "<RUN_ROOT>",
            }
            try:
                return_code, duration, process_stdout, process_stderr = (
                    _run_process_captured(
                        command,
                        cwd=workspace,
                        environment=environment,
                        replacements=replacements,
                    )
                )
            except BaseException as exc:
                _write_abandonment(
                    attempt_root,
                    collection=collection,
                    shard=shard,
                    attempt=attempt,
                    lease=lease,
                    reason="controller_interrupted",
                )
                if isinstance(exc, QualificationError):
                    raise
                raise QualificationError("PSC_QUALIFICATION_WORKER_PROCESS_FAILED") from exc
            if not _immutable_exists(result_path):
                _write_abandonment(
                    attempt_root,
                    collection=collection,
                    shard=shard,
                    attempt=attempt,
                    lease=lease,
                    reason="worker_result_missing",
                )
                raise QualificationError("PSC_QUALIFICATION_WORKER_RESULT_MISSING")
            result = _load_worker_result(result_path, collection, shard)
            controller = _write_json(
                controller_path,
                {
                    "schema_version": SCHEMA_VERSION,
                    "kind": CONTROLLER_KIND,
                    "run_key": collection["run_key"],
                    "candidate": collection["candidate"],
                    "shard_id": shard["id"],
                    "attempt": attempt,
                    "status": (
                        "PASS"
                        if return_code == 0 and result["status"] == "PASS"
                        else "FAIL"
                    ),
                    "return_code": return_code,
                    "duration_ns": duration,
                    "worker_result_sha256": _sha256(_read_bytes(result_path)),
                    "process_stdout": process_stdout,
                    "process_stderr": process_stderr,
                    "lease_owner_id": lease["owner_id"],
                    "lease_mechanism": lease["mechanism"],
                },
            )
            _validate_controller(
                controller,
                collection=collection,
                shard=shard,
                attempt=attempt,
                result_path=result_path,
                result=result,
                require_pass=False,
            )
            if controller["status"] != "PASS":
                raise QualificationError("PSC_QUALIFICATION_TERMINAL_SHARD_FAILURE")
            completed_count += 1
        _assert_git_binding(workspace, collection["candidate"])
        return {
            "run_key": collection["run_key"],
            "shard_count": plan["shard_count"],
            "completed_shard_count": completed_count,
            "reused_shard_count": reused_count,
            "status": "PASS",
            "lease_mechanism": lease["mechanism"],
        }


def _json_pointer(document: Any, pointer: str) -> Any:
    current = document
    for raw_part in pointer.lstrip("/").split("/") if pointer != "/" else [""]:
        part = raw_part.replace("~1", "/").replace("~0", "~")
        if type(current) is dict and part in current:
            current = current[part]
        elif type(current) is list and part.isdigit() and int(part) < len(current):
            current = current[int(part)]
        else:
            raise QualificationError("PSC_QUALIFICATION_EVIDENCE_POINTER_INVALID")
    return current


def _verify_external_evidence(
    evidence: Mapping[str, Any], specification: Mapping[str, Any]
) -> None:
    if (
        evidence.get("schema_version") != specification["expected_schema_version"]
        or evidence.get("kind") != specification["expected_kind"]
    ):
        raise QualificationError("PSC_QUALIFICATION_EVIDENCE_CONTRACT_INVALID")
    seal_field = specification["seal_field"]
    seal = evidence.get(seal_field)
    if type(seal) is not str or not re.fullmatch(r"[0-9a-f]{64}", seal):
        raise QualificationError("PSC_QUALIFICATION_EVIDENCE_INTEGRITY_INVALID")
    payload = copy.deepcopy(dict(evidence))
    payload.pop(seal_field, None)
    if _sha256(_canonical_json_bytes(payload)) != seal:
        raise QualificationError("PSC_QUALIFICATION_EVIDENCE_INTEGRITY_MISMATCH")
    for pointer, expected in specification["required_pointer_values"].items():
        if _json_pointer(evidence, pointer) != expected:
            raise QualificationError("PSC_QUALIFICATION_EVIDENCE_NOT_PASSING")


def _parse_external_evidence(
    values: Sequence[str],
    protected_roots: Sequence[Any] = (),
) -> dict[str, pathlib.Path]:
    result: dict[str, pathlib.Path] = {}
    for value in values:
        evidence_id, separator, path = value.partition("=")
        if not separator or not evidence_id or not path or evidence_id in result:
            raise QualificationError("PSC_QUALIFICATION_EVIDENCE_ARGUMENT_INVALID")
        if protected_roots:
            _reject_protected_path(pathlib.Path(path), protected_roots)
        try:
            supplied_path = pathlib.Path(path).absolute()
            _assert_existing_chain_not_redirected(supplied_path)
            resolved = supplied_path.resolve(strict=True)
            _assert_existing_chain_not_redirected(resolved)
            if protected_roots:
                _reject_protected_path(resolved, protected_roots)
            result[evidence_id] = resolved
        except (OSError, ValueError) as exc:
            raise QualificationError(
                "PSC_QUALIFICATION_EVIDENCE_UNREADABLE"
            ) from exc
    return result


def _derive_restrictions(
    collection: Mapping[str, Any],
    worker_summaries: Sequence[Mapping[str, Any]],
    plan: Mapping[str, Any],
    lease: Mapping[str, str],
) -> dict[str, Any]:
    evidence = [collection["environment"]["collection_guard_evidence"]]
    evidence.extend(item["guard_evidence"] for item in worker_summaries)
    protected_count = sum(
        len(item.get("protected_root_attempts", [])) for item in evidence
    )
    unsafe_alias_count = sum(
        len(item.get("unsafe_alias_attempts", [])) for item in evidence
    )
    network_count = sum(len(item.get("network_attempts", [])) for item in evidence)
    return {
        "protected_root_access": {
            "status": "enforced_and_observed_zero"
            if protected_count == 0
            else "violation_observed",
            "attempt_count": protected_count,
            "unsafe_alias_attempt_count": unsafe_alias_count,
            "required_root_labels": sorted(REQUIRED_PROTECTED_ROOT_LABELS),
            "scope": "qualification_python_processes_only",
        },
        "python_process_network_access": {
            "status": "enforced_and_observed_zero"
            if network_count == 0
            else "violation_observed",
            "attempt_count": network_count,
            "scope": "qualification_python_processes_only",
        },
        "controller_concurrency": {
            "status": "os_lease_enforced",
            "mechanism": lease["mechanism"],
            "run_key_bound": True,
        },
        "worker_schedule": {
            "status": "sequential_plan_enforced"
            if plan.get("sequential") is True
            else "not_enforced",
            "maximum_controller_started_workers": 1,
        },
        "claims_not_made": [
            "child_process_protected_root_access",
            "child_process_network_or_service_mutation",
            "external_api_mutation",
            "remote_repository_merge_or_deploy",
            "system_wide_state_or_sqlite_mutation",
        ],
    }


def _finalize_command(args: argparse.Namespace) -> dict[str, Any]:
    run_root = pathlib.Path(args.run_root).absolute()
    workspace_argument = pathlib.Path(args.workspace).absolute()
    protected_roots = _parse_protected_roots(args.protected_root or [])
    _validate_path_isolation(run_root, workspace_argument, protected_roots)
    workspace = workspace_argument.resolve(strict=True)
    collection = _load_collection(run_root / "collection.json")
    _assert_protected_root_binding(protected_roots, collection)
    plan = _load_plan(run_root / "plan.json", collection)
    _assert_git_binding(workspace, collection["candidate"])
    with _run_key_lease(run_root, collection["run_key"]) as lease:
        return _finalize_under_lease(
            args,
            run_root=run_root,
            workspace=workspace,
            protected_roots=protected_roots,
            collection=collection,
            plan=plan,
            lease=lease,
        )


def _finalize_under_lease(
    args: argparse.Namespace,
    *,
    run_root: pathlib.Path,
    workspace: pathlib.Path,
    protected_roots: Sequence[Mapping[str, str]],
    collection: Mapping[str, Any],
    plan: Mapping[str, Any],
    lease: Mapping[str, str],
) -> dict[str, Any]:
    worker_summaries: list[dict[str, Any]] = []
    abandonment_summaries: list[dict[str, Any]] = []
    aggregate_parent: dict[str, int] = {}
    aggregate_subtests: dict[str, int] = {}
    for shard in plan["shards"]:
        attempt_root = run_root / "attempts" / shard["id"]
        history = _inspect_attempt_history(
            attempt_root, collection=collection, shard=shard
        )
        if history["unsettled"]:
            raise QualificationError("PSC_QUALIFICATION_ATTEMPT_UNSETTLED")
        if history["terminal_failure"] or any(
            item["reason"] == "worker_reported_failure"
            for item in history["abandoned"]
        ):
            raise QualificationError("PSC_QUALIFICATION_TERMINAL_SHARD_FAILURE")
        if not history["completed"]:
            raise QualificationError("PSC_QUALIFICATION_SHARD_NOT_PASSED")
        _attempt, result_path, result, controller = history["completed"][0]
        abandonment_summaries.extend(
            {"shard_id": shard["id"], **item} for item in history["abandoned"]
        )
        for name, value in result["outcomes"]["parent"].items():
            aggregate_parent[name] = aggregate_parent.get(name, 0) + value
        for name, value in result["outcomes"]["subtests"].items():
            aggregate_subtests[name] = aggregate_subtests.get(name, 0) + value
        worker_summaries.append(
            {
                "shard_id": shard["id"],
                "test_count": shard["test_count"],
                "worker_result_sha256": _sha256(_read_bytes(result_path)),
                "controller_payload_sha256": controller["integrity"]["payload_sha256"],
                "duration_ns": result["duration_ns"],
                "outcomes": result["outcomes"],
                "captured_output": result["captured_output"],
                "guard_evidence": result["guard_evidence"],
            }
        )

    supplied = _parse_external_evidence(
        args.external_evidence or [], protected_roots
    )
    expected_evidence_ids = {
        item["id"] for item in collection["selector"]["external_evidence"]
    }
    if not set(supplied).issubset(expected_evidence_ids):
        raise QualificationError("PSC_QUALIFICATION_EVIDENCE_ARGUMENT_INVALID")
    external_results: list[dict[str, Any]] = []
    for specification in collection["selector"]["external_evidence"]:
        path = supplied.get(specification["id"])
        if path is None:
            if specification["required"]:
                raise QualificationError("PSC_QUALIFICATION_EVIDENCE_MISSING")
            continue
        raw_evidence = _read_bytes(path)
        try:
            evidence = json.loads(raw_evidence.decode("utf-8"))
        except (UnicodeError, ValueError, json.JSONDecodeError) as exc:
            raise QualificationError("PSC_QUALIFICATION_JSON_INVALID") from exc
        if type(evidence) is not dict:
            raise QualificationError("PSC_QUALIFICATION_JSON_INVALID")
        _verify_external_evidence(evidence, specification)
        if _json_pointer(evidence, specification["candidate_commit_pointer"]) != (
            collection["candidate"]["head"]
        ) or _json_pointer(evidence, specification["candidate_tree_pointer"]) != (
            collection["candidate"]["tree"]
        ):
            raise QualificationError("PSC_QUALIFICATION_EVIDENCE_HEAD_MISMATCH")
        external_results.append(
            {
                "id": specification["id"],
                "path_name": path.name,
                "byte_size": len(raw_evidence),
                "sha256": _sha256(raw_evidence),
                "acceptance_ids": specification["acceptance_ids"],
            }
        )

    prohibited_parent_names = (
        "failure",
        "error",
        "skip",
        "expected_failure",
        "unexpected_success",
        "subtest_failure",
        "subtest_error",
        "subtest_skip",
        "not_run",
    )
    if any(aggregate_parent.get(name, 0) for name in prohibited_parent_names):
        raise QualificationError("PSC_QUALIFICATION_MANDATORY_OUTCOME_FAILED")
    if any(aggregate_subtests.get(name, 0) for name in ("failure", "error", "skip")):
        raise QualificationError("PSC_QUALIFICATION_MANDATORY_OUTCOME_FAILED")
    if aggregate_parent.get("success", 0) != plan["unique_test_count"]:
        raise QualificationError("PSC_QUALIFICATION_TEST_COUNT_MISMATCH")

    final_git = _assert_git_binding(workspace, collection["candidate"])
    ledger = {
        "schema_version": SCHEMA_VERSION,
        "kind": LEDGER_KIND,
        "suite_id": collection["suite_id"],
        "status": "PASS",
        "run_key": collection["run_key"],
        "candidate": collection["candidate"],
        "final_git": final_git,
        "selection": collection["selection"],
        "toolchain": collection["toolchain"],
        "plan": {
            "shard_count": plan["shard_count"],
            "sequential": True,
            "singleton_modules": collection["selector"]["singleton_modules"],
            "priority_test_ids": collection["selector"]["priority_test_ids"],
        },
        "aggregate_outcomes": {
            "parent": aggregate_parent,
            "subtests": aggregate_subtests,
        },
        "workers": worker_summaries,
        "abandoned_attempts": abandonment_summaries,
        "external_evidence": external_results,
        "acceptance_map": collection["selector"]["acceptance_map"],
        "restrictions": _derive_restrictions(
            collection, worker_summaries, plan, lease
        ),
    }
    ledger_path = pathlib.Path(args.output).absolute()
    _require_path_below(ledger_path, run_root)
    written = _write_json(ledger_path, ledger)
    exact_bytes = _read_bytes(ledger_path)
    detached_path = ledger_path.with_suffix(ledger_path.suffix + ".sha256")
    _write_immutable(
        detached_path,
        f"{_sha256(exact_bytes)}  {ledger_path.name}\n".encode("ascii"),
    )
    return {
        "status": written["status"],
        "ledger": str(ledger_path),
        "ledger_sha256": _sha256(exact_bytes),
        "detached_sha256": str(detached_path),
    }


def _verify_command(args: argparse.Namespace) -> dict[str, Any]:
    ledger_argument = pathlib.Path(args.ledger).absolute()
    _reject_protected_path(
        ledger_argument, tuple(BUILTIN_PROTECTED_ROOTS.values())
    )
    ledger_path = ledger_argument.resolve(strict=True)
    ledger = _read_json(ledger_path)
    _verify_integrity(ledger)
    if ledger.get("kind") != LEDGER_KIND or ledger.get("status") != "PASS":
        raise QualificationError("PSC_QUALIFICATION_LEDGER_INVALID")
    detached_path = ledger_path.with_suffix(ledger_path.suffix + ".sha256")
    detached = _read_bytes(detached_path).decode("ascii", errors="strict").strip()
    expected = f"{_sha256(_read_bytes(ledger_path))}  {ledger_path.name}"
    if detached != expected:
        raise QualificationError("PSC_QUALIFICATION_DETACHED_HASH_MISMATCH")
    return {
        "status": "PASS",
        "ledger": str(ledger_path),
        "ledger_sha256": _sha256(_read_bytes(ledger_path)),
        "unique_test_count": ledger["selection"]["unique_test_count"],
        "subtest_count": ledger["aggregate_outcomes"]["subtests"]["total"],
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    collect = commands.add_parser("collect")
    collect.add_argument("--workspace", required=True)
    collect.add_argument(
        "--selection", default=DEFAULT_SELECTOR_RELATIVE_PATH, required=False
    )
    collect.add_argument("--run-root", required=True)
    collect.add_argument("--expected-head")
    collect.add_argument("--expected-tree")
    collect.add_argument("--expected-unique-count", type=int)
    collect.add_argument("--expected-id-sha256")
    collect.add_argument("--protected-root", action="append", default=[])
    collect.add_argument("--allow-dirty", action="store_true")

    plan = commands.add_parser("plan")
    plan.add_argument("--workspace", required=True)
    plan.add_argument("--run-root", required=True)
    plan.add_argument("--protected-root", action="append", default=[])

    run = commands.add_parser("run")
    run.add_argument("--workspace", required=True)
    run.add_argument("--run-root", required=True)
    run.add_argument("--protected-root", action="append", default=[])

    worker = commands.add_parser("_worker")
    worker.add_argument("--workspace", required=True)
    worker.add_argument("--collection", required=True)
    worker.add_argument("--plan", required=True)
    worker.add_argument("--shard", required=True)
    worker.add_argument("--output", required=True)
    worker.add_argument("--protected-root", action="append", default=[])

    finalize = commands.add_parser("finalize")
    finalize.add_argument("--workspace", required=True)
    finalize.add_argument("--run-root", required=True)
    finalize.add_argument("--external-evidence", action="append", default=[])
    finalize.add_argument("--output", required=True)
    finalize.add_argument("--protected-root", action="append", default=[])

    verify = commands.add_parser("verify")
    verify.add_argument("--ledger", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "collect":
            result = _collect_command(args)
        elif args.command == "plan":
            result = _plan_command(args)
        elif args.command == "run":
            result = _run_command(args)
        elif args.command == "_worker":
            result = _worker_command(args)
        elif args.command == "finalize":
            result = _finalize_command(args)
        elif args.command == "verify":
            result = _verify_command(args)
        else:  # pragma: no cover - argparse enforces the command set
            raise QualificationError("PSC_QUALIFICATION_COMMAND_INVALID")
    except QualificationError as exc:
        print(
            json.dumps(
                {"status": "FAIL", "code": exc.code},
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 2
    except Exception:
        print(
            json.dumps(
                {
                    "status": "FAIL",
                    "code": "PSC_QUALIFICATION_INTERNAL_ERROR",
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 2
    print(
        json.dumps(
            {"status": "PASS", **result},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
