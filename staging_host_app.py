"""Fail-closed staging entry point with legacy mutable state outside source."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Final


_REPARSE_POINT_ATTRIBUTE: Final = 0x400
_MUTABLE_BINDINGS: Final = {
    "history_path": "conversation_log.jsonl",
    "git_config_path": "port_git_map.json",
    "email_routes_path": "email_routes.json",
    "agents_path": "agents.json",
    "sprint_history_path": "project_sprints.json",
    "pending_sprints_path": "pending_project_sprints.json",
    "specializations_path": "specializations.json",
    "attachments_path": "attachments",
    "screenshot_folders_path": "screenshot_folders",
    "evidence_folders_path": "evidence_folders",
}
_DIRECTORY_BINDINGS: Final = {
    "attachments_path",
    "screenshot_folders_path",
    "evidence_folders_path",
}


def _required_environment_path(name: str) -> Path:
    value = os.environ.get(name)
    if not value or not value.strip():
        raise RuntimeError(f"missing required staging environment variable: {name}")
    candidate = Path(value)
    if not candidate.is_absolute():
        raise RuntimeError(f"{name} must be an absolute path")
    return Path(os.path.abspath(candidate))


def _path_key(path: Path) -> str:
    return os.path.normcase(os.path.abspath(path))


def _paths_overlap(first: Path, second: Path) -> bool:
    try:
        common = os.path.commonpath((_path_key(first), _path_key(second)))
    except ValueError:
        return False
    return common in {_path_key(first), _path_key(second)}


def _require_canonical_directory(path: Path, label: str) -> Path:
    if not path.is_dir():
        raise RuntimeError(f"{label} must be an existing directory: {path}")
    resolved = path.resolve(strict=True)
    if _path_key(path) != _path_key(resolved):
        raise RuntimeError(f"{label} contains a filesystem alias or reparse point")
    attributes = int(getattr(os.lstat(path), "st_file_attributes", 0) or 0)
    if attributes & _REPARSE_POINT_ATTRIBUTE:
        raise RuntimeError(f"{label} must not be a reparse point")
    return path


def _load_protected_roots() -> tuple[Path, ...]:
    raw = os.environ.get("NGINX_QA_PROTECTED_ROOTS")
    try:
        values = json.loads(raw or "")
    except json.JSONDecodeError as exc:
        raise RuntimeError("NGINX_QA_PROTECTED_ROOTS must be a JSON array") from exc
    if not isinstance(values, list) or not values:
        raise RuntimeError("NGINX_QA_PROTECTED_ROOTS must be a non-empty JSON array")
    roots: list[Path] = []
    for value in values:
        if not isinstance(value, str) or not value.strip():
            raise RuntimeError("every protected root must be a non-empty string")
        root = Path(value)
        if not root.is_absolute():
            raise RuntimeError("every protected root must be absolute")
        roots.append(Path(os.path.abspath(root)))
    return tuple(roots)


def _require_safe_existing_path(
    path: Path, *, expect_directory: bool, label: str
) -> Path:
    if not os.path.lexists(path):
        return path
    stat_result = os.lstat(path)
    attributes = int(getattr(stat_result, "st_file_attributes", 0) or 0)
    if path.is_symlink() or attributes & _REPARSE_POINT_ATTRIBUTE:
        raise RuntimeError(f"staging mutable path is a reparse point: {label}")
    resolved = path.resolve(strict=True)
    if _path_key(path) != _path_key(resolved):
        raise RuntimeError(f"staging mutable path is a filesystem alias: {label}")
    if expect_directory:
        if not path.is_dir():
            raise RuntimeError(f"staging mutable directory has the wrong type: {label}")
    elif not path.is_file():
        raise RuntimeError(f"staging mutable file has the wrong type: {label}")
    return path


def _require_non_reparse_tree(root: Path, label: str) -> None:
    def fail_walk(error: OSError) -> None:
        raise RuntimeError(f"cannot inspect {label}: {error}") from error

    for current, directory_names, file_names in os.walk(
        root,
        topdown=True,
        onerror=fail_walk,
        followlinks=False,
    ):
        current_path = Path(current)
        _require_safe_existing_path(
            current_path,
            expect_directory=True,
            label=label,
        )
        for name in directory_names:
            _require_safe_existing_path(
                current_path / name,
                expect_directory=True,
                label=f"{label}/{name}",
            )
        for name in file_names:
            _require_safe_existing_path(
                current_path / name,
                expect_directory=False,
                label=f"{label}/{name}",
            )


def _require_safe_mutable_binding(
    path: Path,
    *,
    legacy_root: Path,
    protected_roots: tuple[Path, ...],
    expect_directory: bool,
    label: str,
) -> Path:
    if path.parent != legacy_root or path.name in {"", ".", ".."}:
        raise RuntimeError(f"unsafe staging mutable binding: {label}")
    if any(_paths_overlap(path, root) for root in protected_roots):
        raise RuntimeError(f"staging mutable binding overlaps a protected root: {label}")
    return _require_safe_existing_path(
        path,
        expect_directory=expect_directory,
        label=label,
    )


def _resolve_staging_legacy_state() -> tuple[Path, dict[str, Path]]:
    code_root = Path(__file__).resolve().parent
    service_root = _require_canonical_directory(
        _required_environment_path("NGINX_QA_SERVICE_ROOT"),
        "staging service root",
    )
    if _path_key(service_root) != _path_key(code_root):
        raise RuntimeError("NGINX_QA_SERVICE_ROOT does not identify this checkout")

    state_base = _require_canonical_directory(
        _required_environment_path("NGINX_QA_STAGING_STATE_BASE"),
        "staging state base",
    )
    marker = state_base / ".nginx-qa-staging-owner.json"
    if not marker.is_file() or marker.is_symlink():
        raise RuntimeError("staging ownership marker is missing or unsafe")

    legacy_root = _require_canonical_directory(
        state_base / "legacy",
        "staging legacy state root",
    )
    _require_non_reparse_tree(legacy_root, "staging legacy state tree")
    if legacy_root.parent != state_base:
        raise RuntimeError("staging legacy state root must be state-local")
    if _paths_overlap(service_root, state_base):
        raise RuntimeError("staging service and state roots overlap")
    protected_roots = _load_protected_roots()
    for protected_root in protected_roots:
        if _paths_overlap(state_base, protected_root):
            raise RuntimeError("staging state base overlaps a protected root")

    runtime_state_root = _require_safe_mutable_binding(
        legacy_root / "runtime_state",
        legacy_root=legacy_root,
        protected_roots=protected_roots,
        expect_directory=True,
        label="runtime_state",
    )
    _require_safe_existing_path(
        runtime_state_root / "sequential-prompt-settings.json",
        expect_directory=False,
        label="sequential-prompt-settings.json",
    )

    bindings: dict[str, Path] = {}
    for name, relative in _MUTABLE_BINDINGS.items():
        path = legacy_root / relative
        if _paths_overlap(path, service_root):
            raise RuntimeError(f"unsafe staging mutable binding: {name}")
        bindings[name] = _require_safe_mutable_binding(
            path,
            legacy_root=legacy_root,
            protected_roots=protected_roots,
            expect_directory=name in _DIRECTORY_BINDINGS,
            label=name,
        )
    return legacy_root, bindings


STAGING_LEGACY_ROOT, STAGING_MUTABLE_PATHS = _resolve_staging_legacy_state()

# Validate every staging boundary before importing the legacy application module.
# The current module has no import-time state writes; keeping the import after the
# gate also prevents a future import-time side effect from reaching the checkout.
import main as _main  # noqa: E402

for _name, _path in STAGING_MUTABLE_PATHS.items():
    setattr(_main, _name, _path)
app = _main.app
