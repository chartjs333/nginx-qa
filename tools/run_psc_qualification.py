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
workspace and outside every configured forbidden/live root.
"""

from __future__ import annotations

import argparse
import contextlib
import copy
import hashlib
import importlib
import importlib.metadata
import inspect
import json
import ntpath
import os
import pathlib
import platform
import re
import shutil
import subprocess
import sys
import time
import traceback
import unittest
from collections.abc import Iterable, Mapping, Sequence
from typing import Any


SCHEMA_VERSION = 1
COLLECTION_KIND = "psc_qualification_collection"
PLAN_KIND = "psc_qualification_plan"
WORKER_KIND = "psc_qualification_worker_result"
START_KIND = "psc_qualification_worker_start"
CONTROLLER_KIND = "psc_qualification_controller_receipt"
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
BUILTIN_FORBIDDEN_ROOTS = (r"D:\Prompt",)


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


def _write_immutable(path: pathlib.Path, data: bytes) -> None:
    """Create one artifact without overwriting an existing byte sequence."""

    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if _read_bytes(path) == data:
            return
        raise QualificationError(
            "PSC_QUALIFICATION_IMMUTABLE_ARTIFACT_CONFLICT", path.name
        )
    try:
        with path.open("xb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError:
        if _read_bytes(path) != data:
            raise QualificationError(
                "PSC_QUALIFICATION_IMMUTABLE_ARTIFACT_CONFLICT", path.name
            ) from None
    except OSError as exc:
        raise QualificationError("PSC_QUALIFICATION_ARTIFACT_WRITE_FAILED") from exc


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
            "default_forbidden_roots",
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
    _string_list(
        selector["default_forbidden_roots"],
        code="PSC_QUALIFICATION_SELECTOR_INVALID",
    )
    if any(
        not any(
            _path_key_lexical(configured) == _path_key_lexical(required)
            for configured in selector["default_forbidden_roots"]
        )
        for required in BUILTIN_FORBIDDEN_ROOTS
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


def _path_key_lexical(value: str) -> tuple[str, str]:
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
    if separator != root_separator:
        return False
    return path == root or path.startswith(root + separator)


def _validate_run_root(
    run_root: pathlib.Path,
    workspace: pathlib.Path,
    forbidden_roots: Sequence[str],
) -> None:
    run_value = str(run_root.absolute())
    workspace_value = str(workspace.absolute())
    if _lexically_within(run_value, workspace_value) or _lexically_within(
        workspace_value, run_value
    ):
        raise QualificationError("PSC_QUALIFICATION_RUN_ROOT_OVERLAP")
    for forbidden in forbidden_roots:
        if _lexically_within(run_value, forbidden) or _lexically_within(
            forbidden, run_value
        ):
            raise QualificationError("PSC_QUALIFICATION_RUN_ROOT_OVERLAP")


def _reject_forbidden_path(path: pathlib.Path, forbidden_roots: Sequence[str]) -> None:
    path_value = str(path.absolute())
    if any(_lexically_within(path_value, root) for root in forbidden_roots):
        raise QualificationError("PSC_QUALIFICATION_FORBIDDEN_ROOT_ACCESS")


def _require_path_below(path: pathlib.Path, root: pathlib.Path) -> None:
    if path.absolute() == root.absolute() or not _lexically_within(
        str(path.absolute()), str(root.absolute())
    ):
        raise QualificationError("PSC_QUALIFICATION_OUTPUT_OUTSIDE_RUN_ROOT")


class LexicalForbiddenRootGuard:
    """Best-effort CPython audit guard without resolving/statting live roots."""

    _PATH_EVENTS: dict[str, tuple[int, ...]] = {
        "open": (0,),
        "os.chdir": (0,),
        "os.listdir": (0,),
        "os.scandir": (0,),
        "os.remove": (0,),
        "os.rmdir": (0,),
        "os.mkdir": (0,),
        "os.rename": (0, 1),
        "os.replace": (0, 1),
        "os.truncate": (0,),
        "os.chmod": (0,),
        "os.link": (0, 1),
        "os.symlink": (0, 1),
        "os.utime": (0,),
    }

    def __init__(self, roots: Sequence[str]) -> None:
        self.roots = tuple(roots)
        self.attempts: list[dict[str, str]] = []
        self._installed = False

    def _check(self, event: str, candidate: Any) -> None:
        if isinstance(candidate, bytes):
            candidate = os.fsdecode(candidate)
        if not isinstance(candidate, str) or not candidate:
            return
        for index, root in enumerate(self.roots):
            try:
                matched = _lexically_within(candidate, root)
            except QualificationError:
                continue
            if matched:
                self.attempts.append(
                    {
                        "event": event,
                        "root_index": str(index),
                        "candidate_sha256": _sha256(candidate.encode("utf-8")),
                    }
                )
                raise PermissionError("PSC_FORBIDDEN_ROOT_ACCESS")

    def _hook(self, event: str, arguments: tuple[Any, ...]) -> None:
        for index in self._PATH_EVENTS.get(event, ()):
            if index < len(arguments):
                self._check(event, arguments[index])
        if event == "subprocess.Popen":
            for value in arguments[:3]:
                if isinstance(value, (str, bytes)):
                    self._check(event, value)
                elif isinstance(value, (tuple, list)):
                    for item in value:
                        self._check(event, item)

    def install(self) -> None:
        if not self._installed:
            sys.addaudithook(self._hook)
            self._installed = True


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
    _reject_forbidden_path(workspace_argument, BUILTIN_FORBIDDEN_ROOTS)
    _reject_forbidden_path(selection_argument, BUILTIN_FORBIDDEN_ROOTS)
    workspace = workspace_argument.resolve(strict=True)
    selection_path = selection_argument.resolve(strict=True)
    run_root = pathlib.Path(args.run_root).absolute()
    selector = _load_selector(selection_path)
    forbidden_roots = list(selector["default_forbidden_roots"])
    forbidden_roots.extend(args.forbid_root or [])
    if len(forbidden_roots) != len(set(forbidden_roots)):
        raise QualificationError("PSC_QUALIFICATION_FORBIDDEN_ROOT_INVALID")
    _validate_run_root(run_root, workspace, forbidden_roots)
    git_before = _git_state(workspace)
    if args.expected_head and git_before["head"] != args.expected_head:
        raise QualificationError("PSC_QUALIFICATION_HEAD_CHANGED")
    if args.expected_tree and git_before["tree"] != args.expected_tree:
        raise QualificationError("PSC_QUALIFICATION_HEAD_CHANGED")
    if not args.allow_dirty and not git_before["clean"]:
        raise QualificationError("PSC_QUALIFICATION_WORKTREE_DIRTY")

    guard = LexicalForbiddenRootGuard(forbidden_roots)
    guard.install()
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

    files = [
        _file_binding(workspace, workspace / RUNNER_RELATIVE_PATH),
        _file_binding(workspace, selection_path),
    ]
    for module in dict.fromkeys(item["module"] for item in tests):
        files.append(_file_binding(workspace, workspace / _module_path(module)))
    if not args.allow_dirty and any(item["git_blob"] is None for item in files):
        raise QualificationError("PSC_QUALIFICATION_SOURCE_NOT_COMMITTED")
    toolchain = _toolchain()
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
        "forbidden_roots_sha256": _sha256(
            _canonical_bytes(
                sorted(
                    _path_key_lexical(root)[0] for root in forbidden_roots
                )
            )
        ),
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
    if guard.attempts:
        raise QualificationError("PSC_QUALIFICATION_FORBIDDEN_ROOT_ACCESS")

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
            "tests": tests,
            "loader_error_count": 0,
        },
        "source_files": files,
        "toolchain": toolchain,
        "environment": {
            "forbidden_roots": forbidden_roots,
            "forbidden_root_attempt_count": 0,
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
        or binding.get("toolchain_sha256")
        != _sha256(_canonical_bytes(toolchain))
        or binding.get("forbidden_roots_sha256")
        != _sha256(
            _canonical_bytes(
                sorted(
                    _path_key_lexical(root)[0]
                    for root in environment.get("forbidden_roots", [])
                )
            )
        )
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
    _reject_forbidden_path(workspace_argument, BUILTIN_FORBIDDEN_ROOTS)
    workspace = workspace_argument.resolve(strict=True)
    _validate_run_root(run_root, workspace, BUILTIN_FORBIDDEN_ROOTS)
    collection = _load_collection(run_root / "collection.json")
    _validate_run_root(
        run_root, workspace, collection["environment"]["forbidden_roots"]
    )
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


def _output_digest(path: pathlib.Path, replacements: Mapping[str, str]) -> dict[str, Any]:
    data = _read_bytes(path)
    text = data.decode("utf-8", errors="replace")
    for source, replacement in replacements.items():
        text = text.replace(source, replacement)
    if SECRET_PATTERN.search(text):
        excerpt: str | None = None
        excerpt_status = "omitted_secret_pattern"
    else:
        excerpt = text if len(text) <= 2048 else text[:1024] + "\n...\n" + text[-1024:]
        excerpt_status = "complete" if len(text) <= 2048 else "bounded_head_tail"
    return {
        "byte_size": len(data),
        "sha256": _sha256(data),
        "excerpt_status": excerpt_status,
        "excerpt": excerpt,
    }


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
    _reject_forbidden_path(workspace_argument, BUILTIN_FORBIDDEN_ROOTS)
    workspace = workspace_argument.resolve(strict=True)
    collection_argument = pathlib.Path(args.collection).absolute()
    run_root = collection_argument.parent
    _validate_run_root(run_root, workspace, BUILTIN_FORBIDDEN_ROOTS)
    collection_path = collection_argument.resolve(strict=True)
    plan_argument = pathlib.Path(args.plan).absolute()
    _reject_forbidden_path(plan_argument, BUILTIN_FORBIDDEN_ROOTS)
    plan_path = plan_argument.resolve(strict=True)
    output_path = pathlib.Path(args.output).absolute()
    if plan_path != (run_root / "plan.json").resolve(strict=True):
        raise QualificationError("PSC_QUALIFICATION_PLAN_PATH_INVALID")
    collection = _load_collection(collection_path)
    _validate_run_root(
        run_root,
        workspace,
        collection["environment"]["forbidden_roots"],
    )
    _require_path_below(output_path, run_root)
    plan = _load_plan(plan_path, collection)
    shard = next((item for item in plan["shards"] if item["id"] == args.shard), None)
    if shard is None:
        raise QualificationError("PSC_QUALIFICATION_SHARD_UNKNOWN")
    before = _assert_git_binding(workspace, collection["candidate"])
    guard = LexicalForbiddenRootGuard(collection["environment"]["forbidden_roots"])
    guard.install()
    _ensure_workspace_import_path(workspace)
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromNames(shard["test_ids"])
    loaded_ids = [test.id() for test in _flatten_suite(suite)]
    loader_error_hashes = [_sha256(error.encode("utf-8")) for error in loader.errors]
    load_exact = loaded_ids == shard["test_ids"]
    result = QualificationTestResult(shard["test_ids"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    stdout_path = output_path.with_suffix(".test.stdout")
    stderr_path = output_path.with_suffix(".test.stderr")
    started = time.perf_counter_ns()
    execution_exception: dict[str, str] | None = None
    if not loader.errors and load_exact:
        try:
            with stdout_path.open("w", encoding="utf-8", newline="\n") as stdout_handle:
                with stderr_path.open("w", encoding="utf-8", newline="\n") as stderr_handle:
                    with contextlib.redirect_stdout(stdout_handle), contextlib.redirect_stderr(
                        stderr_handle
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
    else:
        _write_immutable(stdout_path, b"")
        _write_immutable(stderr_path, b"")
    duration = time.perf_counter_ns() - started
    after = _git_state(workspace)
    summary = result.summary()
    prohibited = (
        len(loader.errors)
        + (0 if load_exact else 1)
        + len(result.fixture_errors)
        + len(set(result.unexpected_parent_ids))
        + len(guard.attempts)
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
        "forbidden_root_attempts": guard.attempts,
        "execution_exception": execution_exception,
        "git_before": before,
        "git_after": after,
        "captured_output": {
            "stdout": _output_digest(stdout_path, replacements),
            "stderr": _output_digest(stderr_path, replacements),
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
        or result.get("forbidden_root_attempts") != []
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
    if not path.exists():
        raise QualificationError("PSC_QUALIFICATION_START_RECEIPT_INVALID")
    start = _read_json(path)
    _verify_integrity(start)
    if (
        start.get("kind") != START_KIND
        or start.get("run_key") != collection["run_key"]
        or start.get("candidate") != collection["candidate"]
        or start.get("shard_id") != shard["id"]
        or start.get("attempt") != attempt
        or type(start.get("command")) is not list
        or not start["command"]
    ):
        raise QualificationError("PSC_QUALIFICATION_START_RECEIPT_INVALID")


def _existing_attempts(attempt_root: pathlib.Path) -> list[int]:
    result: set[int] = set()
    if attempt_root.exists():
        for path in attempt_root.iterdir():
            match = re.match(r"attempt-(\d{4})\.(?:start|result|controller)\.json$", path.name)
            if match:
                result.add(int(match.group(1)))
    return sorted(result)


def _run_command(args: argparse.Namespace) -> dict[str, Any]:
    run_root = pathlib.Path(args.run_root).absolute()
    workspace_argument = pathlib.Path(args.workspace).absolute()
    _reject_forbidden_path(workspace_argument, BUILTIN_FORBIDDEN_ROOTS)
    workspace = workspace_argument.resolve(strict=True)
    _validate_run_root(run_root, workspace, BUILTIN_FORBIDDEN_ROOTS)
    collection = _load_collection(run_root / "collection.json")
    _validate_run_root(
        run_root, workspace, collection["environment"]["forbidden_roots"]
    )
    plan = _load_plan(run_root / "plan.json", collection)
    _assert_git_binding(workspace, collection["candidate"])
    completed_count = 0
    reused_count = 0
    runner = pathlib.Path(__file__).resolve(strict=True)
    for shard in plan["shards"]:
        _assert_git_binding(workspace, collection["candidate"])
        attempt_root = run_root / "attempts" / shard["id"]
        attempts = _existing_attempts(attempt_root)
        reusable: tuple[int, pathlib.Path, dict[str, Any]] | None = None
        terminal_failure = False
        completed_attempt_count = 0
        for attempt in attempts:
            result_path = attempt_root / f"attempt-{attempt:04d}.result.json"
            controller_path = attempt_root / f"attempt-{attempt:04d}.controller.json"
            if result_path.exists() and controller_path.exists():
                _validate_start_receipt(
                    attempt_root / f"attempt-{attempt:04d}.start.json",
                    collection=collection,
                    shard=shard,
                    attempt=attempt,
                )
                completed_attempt_count += 1
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
                if result["status"] == "PASS" and controller.get("status") == "PASS":
                    reusable = (attempt, result_path, result)
                else:
                    terminal_failure = True
        if completed_attempt_count > 1:
            raise QualificationError("PSC_QUALIFICATION_ATTEMPT_HISTORY_INVALID")
        if terminal_failure:
            raise QualificationError("PSC_QUALIFICATION_TERMINAL_SHARD_FAILURE")
        if reusable is not None:
            reused_count += 1
            completed_count += 1
            continue
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
        ]
        start = {
            "schema_version": SCHEMA_VERSION,
            "kind": START_KIND,
            "run_key": collection["run_key"],
            "candidate": collection["candidate"],
            "shard_id": shard["id"],
            "attempt": attempt,
            "command": command,
        }
        _write_json(start_path, start)
        environment = _safe_worker_environment(run_root, shard["id"], attempt)
        started = time.perf_counter_ns()
        completed = subprocess.run(
            command,
            cwd=str(workspace),
            env=environment,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        duration = time.perf_counter_ns() - started
        if not result_path.exists():
            raise QualificationError("PSC_QUALIFICATION_WORKER_RESULT_MISSING")
        result = _load_worker_result(result_path, collection, shard)
        stdout_hash = _sha256(completed.stdout)
        stderr_hash = _sha256(completed.stderr)
        controller = {
            "schema_version": SCHEMA_VERSION,
            "kind": CONTROLLER_KIND,
            "run_key": collection["run_key"],
            "candidate": collection["candidate"],
            "shard_id": shard["id"],
            "attempt": attempt,
            "status": (
                "PASS"
                if completed.returncode == 0 and result["status"] == "PASS"
                else "FAIL"
            ),
            "return_code": completed.returncode,
            "duration_ns": duration,
            "worker_result_sha256": _sha256(_read_bytes(result_path)),
            "process_stdout": {
                "byte_size": len(completed.stdout),
                "sha256": stdout_hash,
            },
            "process_stderr": {
                "byte_size": len(completed.stderr),
                "sha256": stderr_hash,
            },
        }
        controller = _write_json(controller_path, controller)
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
    forbidden_roots: Sequence[str] = BUILTIN_FORBIDDEN_ROOTS,
) -> dict[str, pathlib.Path]:
    result: dict[str, pathlib.Path] = {}
    for value in values:
        evidence_id, separator, path = value.partition("=")
        if not separator or not evidence_id or not path or evidence_id in result:
            raise QualificationError("PSC_QUALIFICATION_EVIDENCE_ARGUMENT_INVALID")
        _reject_forbidden_path(pathlib.Path(path), forbidden_roots)
        try:
            result[evidence_id] = pathlib.Path(path).resolve(strict=True)
        except (OSError, ValueError) as exc:
            raise QualificationError(
                "PSC_QUALIFICATION_EVIDENCE_UNREADABLE"
            ) from exc
    return result


def _finalize_command(args: argparse.Namespace) -> dict[str, Any]:
    run_root = pathlib.Path(args.run_root).absolute()
    workspace_argument = pathlib.Path(args.workspace).absolute()
    _reject_forbidden_path(workspace_argument, BUILTIN_FORBIDDEN_ROOTS)
    workspace = workspace_argument.resolve(strict=True)
    _validate_run_root(run_root, workspace, BUILTIN_FORBIDDEN_ROOTS)
    collection = _load_collection(run_root / "collection.json")
    _validate_run_root(
        run_root, workspace, collection["environment"]["forbidden_roots"]
    )
    plan = _load_plan(run_root / "plan.json", collection)
    _assert_git_binding(workspace, collection["candidate"])
    worker_summaries: list[dict[str, Any]] = []
    aggregate_parent: dict[str, int] = {}
    aggregate_subtests: dict[str, int] = {}
    for shard in plan["shards"]:
        attempt_root = run_root / "attempts" / shard["id"]
        accepted: tuple[pathlib.Path, dict[str, Any], dict[str, Any]] | None = None
        terminal_failure = False
        completed_attempt_count = 0
        for attempt in _existing_attempts(attempt_root):
            result_path = attempt_root / f"attempt-{attempt:04d}.result.json"
            controller_path = attempt_root / f"attempt-{attempt:04d}.controller.json"
            if not result_path.exists() or not controller_path.exists():
                continue
            _validate_start_receipt(
                attempt_root / f"attempt-{attempt:04d}.start.json",
                collection=collection,
                shard=shard,
                attempt=attempt,
            )
            completed_attempt_count += 1
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
            if result["status"] == "PASS" and controller.get("status") == "PASS":
                accepted = (result_path, result, controller)
            else:
                terminal_failure = True
        if completed_attempt_count > 1:
            raise QualificationError("PSC_QUALIFICATION_ATTEMPT_HISTORY_INVALID")
        if terminal_failure:
            raise QualificationError("PSC_QUALIFICATION_TERMINAL_SHARD_FAILURE")
        if accepted is None:
            raise QualificationError("PSC_QUALIFICATION_SHARD_NOT_PASSED")
        result_path, result, controller = accepted
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
            }
        )

    supplied = _parse_external_evidence(
        args.external_evidence or [], collection["environment"]["forbidden_roots"]
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
        for forbidden in collection["environment"]["forbidden_roots"]:
            if _lexically_within(str(path), forbidden):
                raise QualificationError("PSC_QUALIFICATION_FORBIDDEN_ROOT_ACCESS")
        evidence = _read_json(path)
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
                "byte_size": path.stat().st_size,
                "sha256": _sha256(_read_bytes(path)),
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
        "external_evidence": external_results,
        "acceptance_map": collection["selector"]["acceptance_map"],
        "restrictions": {
            "live_root_access_attempts": 0,
            "api_or_service_mutation": False,
            "state_or_sqlite_mutation": False,
            "merge_or_deploy": False,
            "parallel_test_execution": False,
        },
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
    _reject_forbidden_path(ledger_argument, BUILTIN_FORBIDDEN_ROOTS)
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
    collect.add_argument("--forbid-root", action="append", default=[])
    collect.add_argument("--allow-dirty", action="store_true")

    plan = commands.add_parser("plan")
    plan.add_argument("--workspace", required=True)
    plan.add_argument("--run-root", required=True)

    run = commands.add_parser("run")
    run.add_argument("--workspace", required=True)
    run.add_argument("--run-root", required=True)

    worker = commands.add_parser("_worker")
    worker.add_argument("--workspace", required=True)
    worker.add_argument("--collection", required=True)
    worker.add_argument("--plan", required=True)
    worker.add_argument("--shard", required=True)
    worker.add_argument("--output", required=True)

    finalize = commands.add_parser("finalize")
    finalize.add_argument("--workspace", required=True)
    finalize.add_argument("--run-root", required=True)
    finalize.add_argument("--external-evidence", action="append", default=[])
    finalize.add_argument("--output", required=True)

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
