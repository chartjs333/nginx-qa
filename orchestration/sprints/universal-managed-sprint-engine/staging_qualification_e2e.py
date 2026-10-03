#!/usr/bin/env python3
"""Reproducible, fail-closed staging qualification for managed child processes.

The runner deliberately has four phases.  ``prepare`` provisions a unique
project context, starts the source-bound manifest, proves four healthy child
owners, and writes an evidence checkpoint.  An operator then restarts the
staging host with the normal staging launcher.  ``verify-after-restart`` proves
that the listener PID changed, the four child identities were adopted without
replacement or duplicate leases, and the original start request replays
idempotently.  After the operator stops only the staging host, ``cleanup``
authenticates and stops the four owned Jobs and proves their leases released.
Finally, ``finalize`` compares a fresh external live after-snapshot and is the
only phase that can write ``status=passed``.

This program never starts or stops the staging host and never sends an
application API request outside the exact staging endpoint.  Qualification
reads SQLite without mutation; the explicit cleanup phase is the sole exception
and uses the production supervisor's authenticated stop operation.  Git source
advertisement is verified with ``git ls-remote`` before staging performs its own
fetch.  A required pair of externally captured live snapshots proves that the
protected instance was not changed without contacting live from this process.
"""

from __future__ import annotations

import argparse
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
import os
import platform
from pathlib import Path, PurePosixPath
import re
import sqlite3
import subprocess
import sys
import time
from typing import Any, Callable, Mapping, Sequence
import urllib.error
import urllib.parse
import urllib.request
from uuid import UUID


EVIDENCE_SCHEMA_VERSION = 1
QUALIFICATION_ID = "managed-staging-four-child-adoption"
EXPECTED_CHILD_COUNT = 4
DEFAULT_STAGING_URL = "http://127.0.0.1:18025"
DEFAULT_STAGING_PORTS = (18025,)
DEFAULT_CHILD_PORTS = tuple(range(18100, 18200))
DEFAULT_FORBIDDEN_PORTS = (8025, 8026)
EXPECTED_LIVE_ROOT = Path("D:/nginx-qa")
EXPECTED_SERVICE_ROOT = Path("D:/nginx-qa-staging/universal-managed-sprint-engine")
EXPECTED_STATE_BASE = Path("C:/nginx-qa-staging-state/umse-007")
EXPECTED_VENV_ROOT = EXPECTED_STATE_BASE / ".venv"
EXPECTED_CHILD_PYTHON = EXPECTED_VENV_ROOT / "Scripts" / "python.exe"
EXPECTED_RUNTIME_ROOT = EXPECTED_STATE_BASE / "runtime_state"
EXPECTED_PROMPT_ROOT = EXPECTED_STATE_BASE / "prompt"
EXPECTED_MANAGED_ROOT = EXPECTED_STATE_BASE / "managed"
EXPECTED_OWNERSHIP_MARKER = EXPECTED_STATE_BASE / ".nginx-qa-staging-owner.json"
EXPECTED_PROTECTED_ROOTS = (
    EXPECTED_LIVE_ROOT,
    Path("D:/nginx-qa-umse"),
    Path("D:/Prompt"),
)
EXPECTED_INSTANCE_ID = "universal-managed-sprint-engine-staging"
STAGING_HTTP_PORT = 18025
LIVE_HTTP_PORT = 8025
CHILD_PORT_START = 18100
CHILD_PORT_END = 18199
STAGING_INSTANCE_ID = "universal-managed-sprint-engine-staging"
CHILD_FIXTURE_PATH = "tests/fixtures/managed_child_service.py"
EXPECTED_RESOURCE_LIMITS = {
    "wall_time_seconds": 3600,
    "memory_bytes": 268435456,
    "cpu_percent": 100,
    "process_count": 8,
}
EXPECTED_MANIFEST_SHA256 = (
    "41160737c713116d898bee907a35a3b393912c02444ecc140ae41112ad3a70fa"
)
MAX_HTTP_BODY_BYTES = 4 * 1024 * 1024
MAX_SNAPSHOT_AGE_SECONDS = 15 * 60
MAX_PROCESS_CHAIN_DEPTH = 16
SENSITIVE_KEYS = {
    "api_key",
    "api_token",
    "authorization",
    "cookie",
    "credentials",
    "password",
    "refresh_token",
    "secret",
    "token",
}
NON_SECRET_TOKEN_KEYS = {"fencing_token"}
HEX_SHA_RE = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")
RUN_ID_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?$")
REF_RE = re.compile(r"^refs/(?:heads|tags)/[^\\\x00-\x1f\x7f]+$")


class QualificationError(RuntimeError):
    """A qualification invariant failed."""


@dataclass(frozen=True)
class PrepareConfig:
    repo_root: Path
    expected_sha: str
    ref: str
    manifest_path: str
    git_address: str
    expected_child_python: Path
    expected_runtime_root: Path
    expected_managed_root: Path
    ownership_marker: Path
    run_id: str
    staging_url: str
    managed_db: Path
    evidence_path: Path
    protected_roots: tuple[Path, ...]
    timeout_seconds: float
    live_baseline_path: Path


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, file_pointer, code, message, headers, new_url):
        return None


class LoopbackJsonClient:
    """Small JSON client that rejects redirects and every non-127.0.0.1 URL."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 10.0,
    ) -> None:
        self.base_url, self.port = validate_loopback_url(base_url)
        self.timeout = timeout
        self._opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}),
            _NoRedirect(),
        )

    def request(
        self,
        method: str,
        path: str,
        payload: Mapping[str, Any] | None = None,
    ) -> tuple[int, dict[str, Any]]:
        if not path.startswith("/") or path.startswith("//"):
            raise QualificationError("HTTP path must be an absolute local path")
        url = self.base_url + path
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme != "http" or parsed.hostname != "127.0.0.1":
            raise QualificationError("refusing a non-loopback HTTP request")
        body = None
        headers = {"Accept": "application/json"}
        if payload is not None:
            body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            url,
            data=body,
            headers=headers,
            method=method.upper(),
        )
        try:
            with self._opener.open(request, timeout=self.timeout) as response:
                final = urllib.parse.urlsplit(response.geturl())
                if response.geturl() != url or final.port != STAGING_HTTP_PORT:
                    raise QualificationError("HTTP response URL differs from exact staging URL")
                encoded = response.read(MAX_HTTP_BODY_BYTES + 1)
                status = int(response.status)
        except urllib.error.HTTPError as exc:
            encoded = exc.read(MAX_HTTP_BODY_BYTES + 1)
            detail = _safe_error_code(encoded)
            raise QualificationError(
                f"staging HTTP {exc.code} for {method.upper()} {path}: {detail}"
            ) from exc
        except (OSError, urllib.error.URLError) as exc:
            raise QualificationError(
                f"staging HTTP request failed for {method.upper()} {path}"
            ) from exc
        if len(encoded) > MAX_HTTP_BODY_BYTES:
            raise QualificationError("staging JSON response exceeds the size limit")
        try:
            decoded = json.loads(encoded.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise QualificationError("staging returned invalid JSON") from exc
        if not isinstance(decoded, dict):
            raise QualificationError("staging JSON response must be an object")
        return status, decoded


class ManagedStateReader:
    """Read one consistent managed-state snapshot without opening SQLite writable."""

    def __init__(self, database_path: Path) -> None:
        path = database_path.resolve(strict=False)
        if not path.is_absolute() or not path.is_file():
            raise QualificationError("managed SQLite database must be an existing file")
        self.database_path = path

    def read(self, project_id: str, sprint_id: str) -> dict[str, Any] | None:
        uri = self.database_path.as_uri() + "?mode=ro"
        try:
            connection = sqlite3.connect(uri, uri=True, timeout=5.0)
        except sqlite3.Error as exc:
            raise QualificationError("cannot open managed SQLite database read-only") from exc
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA query_only = ON")
            connection.execute("BEGIN")
            row = connection.execute(
                """
                SELECT project_id, sprint_id, status, fencing_token, state_json
                FROM managed_sprints WHERE project_id = ? AND sprint_id = ?
                """,
                (project_id, sprint_id),
            ).fetchone()
            if row is None:
                return None
            try:
                state = json.loads(row["state_json"])
            except (TypeError, json.JSONDecodeError) as exc:
                raise QualificationError("managed sprint state is corrupt") from exc
            if not isinstance(state, dict):
                raise QualificationError("managed sprint state is not an object")
            process_ids = _string_ids(state.get("processes"), "process_id")
            lease_ids = _string_ids(state.get("port_leases"), "lease_id")
            process_rows = _selected_owner_rows(
                connection,
                table="managed_process_owners",
                identifiers=process_ids,
            )
            lease_rows = _selected_owner_rows(
                connection,
                table="managed_port_leases",
                identifiers=lease_ids,
            )
            assignment_ids = _string_ids(state.get("assignments"), "assignment_id")
            live_assignment_processes = _live_assignment_owner_rows(
                connection,
                table="managed_process_owners",
                live_statuses=("PREPARED", "STARTING", "HEALTHY", "STOPPING"),
                assignment_ids=assignment_ids,
            )
            live_assignment_leases = _live_assignment_owner_rows(
                connection,
                table="managed_port_leases",
                live_statuses=("reserved", "bound"),
                assignment_ids=assignment_ids,
            )
            queue_items = _queue_items_for_sprint(connection, sprint_id)
            connection.commit()
            return {
                "row": {
                    "project_id": row["project_id"],
                    "sprint_id": row["sprint_id"],
                    "status": row["status"],
                    "fencing_token": row["fencing_token"],
                },
                "state": state,
                "owner_processes": process_rows,
                "owner_leases": lease_rows,
                "live_assignment_processes": live_assignment_processes,
                "live_assignment_leases": live_assignment_leases,
                "queue_items": queue_items,
            }
        except sqlite3.Error as exc:
            raise QualificationError("managed SQLite read failed") from exc
        finally:
            connection.close()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def validate_loopback_url(
    value: str,
) -> tuple[str, int]:
    parsed = urllib.parse.urlsplit(str(value).strip())
    if (
        parsed.scheme != "http"
        or parsed.hostname != "127.0.0.1"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
    ):
        raise QualificationError(
            "staging URL must be exactly http://127.0.0.1:<port>"
        )
    try:
        port = parsed.port
    except ValueError as exc:
        raise QualificationError("staging URL contains an invalid port") from exc
    if port is None or not 1 <= port <= 65535:
        raise QualificationError("staging URL must contain an explicit valid port")
    if port != STAGING_HTTP_PORT:
        raise QualificationError("URL does not use the exact staging port")
    return f"http://127.0.0.1:{port}", port


def canonical_repository_key(git_address: str) -> str:
    address = str(git_address).strip()
    if not address or len(address) > 2048:
        raise QualificationError("git address is missing or too long")
    if any(ord(character) < 32 or ord(character) == 127 for character in address):
        raise QualificationError("git address contains control characters")
    scp = None
    if "://" not in address:
        scp = re.fullmatch(
            r"(?:[^@\s/:]+@)?(?P<host>[A-Za-z0-9.-]+):(?P<path>[^\s]+)",
            address,
        )
    if scp:
        host = scp.group("host").lower()
        path = scp.group("path").strip("/")
    else:
        parsed = urllib.parse.urlsplit(address)
        if parsed.scheme not in {"http", "https", "ssh", "git"}:
            raise QualificationError("git address must be a supported network URL")
        if (
            parsed.hostname is None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise QualificationError("git address is invalid or contains credentials")
        if parsed.scheme in {"http", "https", "git"} and parsed.username is not None:
            raise QualificationError("git address must not contain credentials")
        host = parsed.hostname.lower()
        try:
            port = parsed.port
        except ValueError as exc:
            raise QualificationError("git address contains an invalid port") from exc
        default_port = {"http": 80, "https": 443, "ssh": 22, "git": 9418}[
            parsed.scheme
        ]
        if port is not None and port != default_port:
            host = f"{host}:{port}"
        path = parsed.path.strip("/")
    if path.lower().endswith(".git"):
        path = path[:-4]
    if not path:
        raise QualificationError("git address has no repository path")
    if host.rstrip(".").lower() == "github.com":
        path = path.lower()
    return f"{host}/{path}"


def build_context_key(git_address: str, run_id: str) -> str:
    normalized_run_id = str(run_id).strip().casefold()
    if not RUN_ID_RE.fullmatch(normalized_run_id):
        raise QualificationError("run id must be a lowercase alphanumeric slug")
    return (
        f"{canonical_repository_key(git_address)}"
        f"#staging-qualification-{normalized_run_id}"
    )


def validate_manifest_path(value: str) -> str:
    path = str(value).strip()
    pure = PurePosixPath(path)
    if (
        not path
        or path.startswith("/")
        or path.endswith("/")
        or "//" in path
        or "\\" in path
        or any(character in '<>:"|?*' for character in path)
        or any(ord(character) < 32 or ord(character) == 127 for character in path)
        or any(part in {"", ".", ".."} for part in pure.parts)
    ):
        raise QualificationError("manifest path must be a safe relative POSIX path")
    return path


def _resolved(path: Path) -> Path:
    if not path.is_absolute():
        raise QualificationError(f"path must be absolute: {path}")
    return path.resolve(strict=False)


def _same_or_within(path: Path, root: Path) -> bool:
    candidate = os.path.normcase(str(path.resolve(strict=False)))
    parent = os.path.normcase(str(root.resolve(strict=False)))
    try:
        return os.path.commonpath((candidate, parent)) == parent
    except ValueError:
        return False


def _paths_overlap(first: Path, second: Path) -> bool:
    return _same_or_within(first, second) or _same_or_within(second, first)


def _is_exact_int(
    value: Any,
    *,
    expected: int | None = None,
    minimum: int | None = None,
) -> bool:
    return (
        type(value) is int
        and (expected is None or value == expected)
        and (minimum is None or value >= minimum)
    )


def _scalar_exact_equal(first: Any, second: Any) -> bool:
    return type(first) is type(second) and first == second


def _validate_timeout_seconds(value: Any) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or value <= 0
    ):
        raise QualificationError("timeout must be a finite positive number")
    return float(value)


def _json_exact_equal(first: Any, second: Any) -> bool:
    try:
        return json.dumps(
            first,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ) == json.dumps(
            second,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError):
        return False


def _is_safe_path_component(value: Any) -> bool:
    return (
        isinstance(value, str)
        and bool(value)
        and value not in {".", ".."}
        and value.rstrip(" .") == value
        and re.search(r'[<>:"/\\|?*\x00-\x1f]', value) is None
        and Path(value).name == value
    )


def validate_config(config: PrepareConfig) -> PrepareConfig:
    repo_root = _resolved(config.repo_root)
    managed_db = _resolved(config.managed_db)
    evidence_path = _resolved(config.evidence_path)
    expected_child_python = _resolved(config.expected_child_python)
    expected_runtime_root = _resolved(config.expected_runtime_root)
    expected_managed_root = _resolved(config.expected_managed_root)
    ownership_marker = _resolved(config.ownership_marker)
    live_baseline_path = _resolved(config.live_baseline_path)
    protected = tuple(_resolved(path) for path in config.protected_roots)
    if [_path_key(str(path)) for path in protected] != [
        _path_key(str(path)) for path in EXPECTED_PROTECTED_ROOTS
    ]:
        raise QualificationError("qualification does not use the exact protected roots")
    if len({_path_key(str(path)) for path in protected}) != len(protected):
        raise QualificationError("protected live roots must be unique")
    if any(
        _paths_overlap(root, other)
        for index, root in enumerate(protected)
        for other in protected[index + 1 :]
    ):
        raise QualificationError("protected live roots must not overlap")
    exact_paths = (
        ("service root", repo_root, EXPECTED_SERVICE_ROOT),
        ("state base", ownership_marker.parent, EXPECTED_STATE_BASE),
        ("child Python", expected_child_python, EXPECTED_CHILD_PYTHON),
        ("runtime root", expected_runtime_root, EXPECTED_RUNTIME_ROOT),
        ("managed root", expected_managed_root, EXPECTED_MANAGED_ROOT),
        ("ownership marker", ownership_marker, EXPECTED_OWNERSHIP_MARKER),
    )
    if any(
        _path_key(str(actual)) != _path_key(str(expected))
        for _, actual, expected in exact_paths
    ):
        raise QualificationError("qualification paths do not match the exact staging boundary")
    if not any(
        _path_key(str(root)) == _path_key(str(EXPECTED_LIVE_ROOT))
        for root in protected
    ):
        raise QualificationError("protected roots omit the exact live checkout")
    if not repo_root.is_dir() or not (repo_root / ".git").exists():
        raise QualificationError("repo root must be an existing Git checkout")
    if not managed_db.is_file():
        raise QualificationError("managed SQLite database does not exist")
    if not expected_child_python.is_file():
        raise QualificationError("expected child Python must be an existing file")
    if not expected_runtime_root.is_dir() or not expected_managed_root.is_dir():
        raise QualificationError("expected staging runtime/managed roots must exist")
    if not ownership_marker.is_file():
        raise QualificationError("staging ownership marker does not exist")
    if not live_baseline_path.is_file():
        raise QualificationError("external live baseline snapshot does not exist")
    expected_database = (
        expected_runtime_root / "leases" / "managed-import.sqlite3"
    ).resolve(strict=False)
    if os.path.normcase(str(managed_db)) != os.path.normcase(str(expected_database)):
        raise QualificationError("managed database is not the exact staging database")
    if _paths_overlap(evidence_path, repo_root):
        raise QualificationError("evidence must be outside the source checkout")
    for label, protected_file in (
        ("managed database", managed_db),
        ("expected child Python", expected_child_python),
        ("external live baseline", live_baseline_path),
        ("ownership marker", ownership_marker),
    ):
        if os.path.normcase(str(evidence_path)) == os.path.normcase(
            str(protected_file)
        ):
            raise QualificationError(f"evidence must not replace {label}")
    evidence_root = ownership_marker.parent / "evidence"
    if (
        not _same_or_within(evidence_path, evidence_root)
        or not _same_or_within(live_baseline_path, evidence_root)
        or os.path.normcase(str(evidence_path))
        == os.path.normcase(str(live_baseline_path))
    ):
        raise QualificationError(
            "evidence and live snapshots must be distinct files in the owned evidence root"
        )
    for root in protected:
        for label, path in (
            ("repo root", repo_root),
            ("managed database", managed_db),
            ("evidence", evidence_path),
            ("expected child Python", expected_child_python),
            ("expected runtime root", expected_runtime_root),
            ("expected managed root", expected_managed_root),
            ("ownership marker", ownership_marker),
        ):
            if _paths_overlap(path, root):
                raise QualificationError(f"{label} overlaps a protected live root")
        if _paths_overlap(live_baseline_path, root):
            raise QualificationError(
                "external live snapshot file must be outside protected live roots"
            )
    expected_venv_root = expected_child_python.parent.parent
    if (
        expected_child_python.name.casefold() != "python.exe"
        or expected_child_python.parent.name.casefold() != "scripts"
    ):
        raise QualificationError("expected child Python is not a Windows venv interpreter")
    isolated_roots = (
        ("service", repo_root),
        ("venv", expected_venv_root),
        ("runtime", expected_runtime_root),
        ("managed", expected_managed_root),
    )
    for index, (label, root) in enumerate(isolated_roots):
        for other_label, other_root in isolated_roots[index + 1 :]:
            if _paths_overlap(root, other_root):
                raise QualificationError(
                    f"staging {label}/{other_label} roots overlap"
                )
    sha = config.expected_sha.strip().lower()
    if not HEX_SHA_RE.fullmatch(sha):
        raise QualificationError("expected SHA must be a full lowercase Git object id")
    if not REF_RE.fullmatch(config.ref):
        raise QualificationError("ref must be a full refs/heads or refs/tags name")
    timeout_seconds = _validate_timeout_seconds(config.timeout_seconds)
    validate_loopback_url(config.staging_url)
    validate_manifest_path(config.manifest_path)
    build_context_key(config.git_address, config.run_id)
    return PrepareConfig(
        repo_root=repo_root,
        expected_sha=sha,
        ref=config.ref,
        manifest_path=config.manifest_path,
        git_address=config.git_address,
        expected_child_python=expected_child_python,
        expected_runtime_root=expected_runtime_root,
        expected_managed_root=expected_managed_root,
        ownership_marker=ownership_marker,
        run_id=config.run_id.casefold(),
        staging_url=validate_loopback_url(config.staging_url)[0],
        managed_db=managed_db,
        evidence_path=evidence_path,
        protected_roots=protected,
        timeout_seconds=timeout_seconds,
        live_baseline_path=live_baseline_path,
    )


def _git(repo_root: Path, *arguments: str, timeout: float = 120.0) -> bytes:
    try:
        completed = subprocess.run(
            ("git", "-C", str(repo_root), *arguments),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise QualificationError("Git validation command failed") from exc
    if completed.returncode != 0:
        raise QualificationError(f"Git validation failed: {' '.join(arguments[:2])}")
    return completed.stdout


def validate_git_source(config: PrepareConfig) -> dict[str, Any]:
    head = _git(config.repo_root, "rev-parse", "HEAD").decode("ascii").strip().lower()
    if head != config.expected_sha:
        raise QualificationError("local HEAD does not equal expected SHA")
    status = _git(
        config.repo_root,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
    )
    if status.strip():
        raise QualificationError("source checkout is not clean")
    local_ref = _git(
        config.repo_root,
        "rev-parse",
        f"{config.ref}^{{commit}}",
    ).decode("ascii").strip().lower()
    if local_ref != config.expected_sha:
        raise QualificationError("local requested ref does not equal expected SHA")
    remote_url = _git(config.repo_root, "remote", "get-url", "origin").decode(
        "utf-8"
    ).strip()
    if canonical_repository_key(remote_url) != canonical_repository_key(config.git_address):
        raise QualificationError("origin and requested git address identify different repos")
    advertised = _git(
        config.repo_root,
        "ls-remote",
        "--exit-code",
        "origin",
        config.ref,
        timeout=max(120.0, config.timeout_seconds),
    ).decode("utf-8")
    advertised_matches = [
        line.split("\t", 1)[0].strip().lower()
        for line in advertised.splitlines()
        if "\t" in line and line.split("\t", 1)[1].strip() == config.ref
    ]
    if advertised_matches != [config.expected_sha]:
        raise QualificationError("origin does not advertise expected SHA at requested ref")
    manifest_bytes = _git(
        config.repo_root,
        "show",
        f"{config.expected_sha}:{config.manifest_path}",
    )
    try:
        manifest = json.loads(manifest_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise QualificationError("source-bound manifest is not valid UTF-8 JSON") from exc
    if not isinstance(manifest, dict):
        raise QualificationError("source-bound manifest must be an object")
    if canonical_repository_key(str(manifest.get("git_address") or "")) != (
        canonical_repository_key(config.git_address)
    ):
        raise QualificationError("manifest git_address does not match requested repo")
    manifest_git = manifest.get("git")
    if not isinstance(manifest_git, dict):
        raise QualificationError("manifest.git is missing")
    if manifest_git.get("source_ref") != config.ref:
        raise QualificationError("manifest source_ref does not match requested ref")
    declared_commit = manifest_git.get("expected_source_commit")
    if declared_commit not in {None, config.expected_sha}:
        raise QualificationError("manifest declares a different expected source commit")
    execution = manifest.get("execution")
    start_nodes = execution.get("start_nodes") if isinstance(execution, dict) else None
    if (
        not isinstance(execution, dict)
        or execution.get("mode") != "parallel"
        or not isinstance(start_nodes, list)
        or len(start_nodes) != EXPECTED_CHILD_COUNT
    ):
        raise QualificationError("manifest must declare exactly four start nodes")
    if len(set(start_nodes)) != EXPECTED_CHILD_COUNT:
        raise QualificationError("manifest start nodes must be unique")
    raw_nodes = manifest.get("nodes")
    nodes = (
        {
            item.get("id"): item
            for item in raw_nodes
            if isinstance(item, dict) and isinstance(item.get("id"), str)
        }
        if isinstance(raw_nodes, list)
        else {}
    )
    if not isinstance(raw_nodes, list) or len(nodes) != len(raw_nodes):
        raise QualificationError("manifest node identities are invalid or duplicated")
    expected_command_tail = [CHILD_FIXTURE_PATH, "--mode", "serve"]
    expected_process_keys = {
        "command",
        "cwd",
        "environment",
        "health_path",
        "restart_policy",
        "max_restart_attempts",
        "restart_backoff_seconds",
        "resource_limits",
    }
    for node_id in start_nodes:
        node = nodes.get(node_id)
        workspace = node.get("workspace") if isinstance(node, dict) else None
        process = workspace.get("process") if isinstance(workspace, dict) else None
        command = process.get("command") if isinstance(process, dict) else None
        if (
            not isinstance(workspace, dict)
            or set(workspace) != {"access", "process"}
            or workspace.get("access") != "read"
            or not isinstance(process, dict)
            or set(process) != expected_process_keys
            or not isinstance(command, list)
            or len(command) != 4
            or _path_key(command[0]) != _path_key(str(config.expected_child_python))
            or command[1:] != expected_command_tail
            or process.get("cwd") != "."
            or process.get("environment") != {}
            or process.get("health_path") != "/health"
            or process.get("restart_policy") != "on_failure"
            or process.get("max_restart_attempts") != 1
            or process.get("restart_backoff_seconds") != 0
            or process.get("resource_limits") != EXPECTED_RESOURCE_LIMITS
        ):
            raise QualificationError(
                "manifest child process spec is not the exact staging fixture"
            )
    declared_files = manifest.get("files")
    if (
        not isinstance(declared_files, list)
        or len(declared_files) != 1
        or not isinstance(declared_files[0], dict)
        or set(declared_files[0]) != {"path", "sha256"}
        or declared_files[0].get("path") != CHILD_FIXTURE_PATH
        or not isinstance(declared_files[0].get("sha256"), str)
    ):
        raise QualificationError("manifest fixture declaration is not exact")
    fixture_bytes = _git(
        config.repo_root,
        "show",
        f"{config.expected_sha}:{CHILD_FIXTURE_PATH}",
    )
    fixture_sha256 = hashlib.sha256(fixture_bytes).hexdigest()
    if declared_files[0]["sha256"] != fixture_sha256:
        raise QualificationError("manifest fixture SHA does not match Git blob")
    manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
    if manifest_sha256 != EXPECTED_MANIFEST_SHA256:
        raise QualificationError("manifest does not equal the exact approved graph")
    return {
        "head": head,
        "ref": config.ref,
        "advertised_commit": advertised_matches[0],
        "canonical_remote": canonical_repository_key(remote_url),
        "manifest_sha256": manifest_sha256,
        "fixture_sha256": fixture_sha256,
        "expected_child_python": str(config.expected_child_python),
        "worktree": "clean",
    }


def _owner_query_spec(table: str) -> tuple[str, str, tuple[str, ...]]:
    if table == "managed_process_owners":
        return (
            "process_id",
            "state",
            (
                "process_id",
                "assignment_id",
                "port_lease_id",
                "pid",
                "state",
                "process_json",
            ),
        )
    if table == "managed_port_leases":
        return (
            "lease_id",
            "status",
            (
                "lease_id",
                "instance_id",
                "network_namespace_id",
                "assignment_id",
                "process_id",
                "host",
                "port",
                "status",
                "lease_json",
            ),
        )
    raise AssertionError("unsafe SQLite owner table")


def _decode_owner_rows(
    rows: Sequence[sqlite3.Row], table: str
) -> list[dict[str, Any]]:
    _, _, columns = _owner_query_spec(table)
    json_column = "process_json" if table == "managed_process_owners" else "lease_json"
    decoded: list[dict[str, Any]] = []
    for row in rows:
        try:
            value = json.loads(row[json_column])
        except (TypeError, json.JSONDecodeError) as exc:
            raise QualificationError(f"{table} contains corrupt JSON") from exc
        if not isinstance(value, dict):
            raise QualificationError(f"{table} JSON is not an object")
        decoded.append(
            {
                "json": value,
                "relational": {
                    column: row[column]
                    for column in columns
                    if column != json_column
                },
            }
        )
    return decoded


def _selected_owner_rows(
    connection: sqlite3.Connection,
    *,
    table: str,
    identifiers: Sequence[str],
) -> list[dict[str, Any]]:
    if not identifiers:
        return []
    id_column, _, columns = _owner_query_spec(table)
    placeholders = ",".join("?" for _ in identifiers)
    rows = connection.execute(
        f"SELECT {', '.join(columns)} FROM {table} "
        f"WHERE {id_column} IN ({placeholders}) ORDER BY {id_column}",
        tuple(identifiers),
    ).fetchall()
    return _decode_owner_rows(rows, table)


def _live_assignment_owner_rows(
    connection: sqlite3.Connection,
    *,
    table: str,
    live_statuses: Sequence[str],
    assignment_ids: Sequence[str],
) -> list[dict[str, Any]]:
    if not assignment_ids:
        return []
    _, status_column, columns = _owner_query_spec(table)
    assignment_placeholders = ",".join("?" for _ in assignment_ids)
    status_placeholders = ",".join("?" for _ in live_statuses)
    rows = connection.execute(
        f"SELECT {', '.join(columns)} FROM {table} "
        f"WHERE assignment_id IN ({assignment_placeholders}) "
        f"AND {status_column} IN ({status_placeholders}) ORDER BY assignment_id",
        (*assignment_ids, *live_statuses),
    ).fetchall()
    return _decode_owner_rows(rows, table)


def _queue_items_for_sprint(
    connection: sqlite3.Connection, sprint_id: str
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT dedupe_key, event_id, event_type, payload_json, receipt_id, created_at
        FROM managed_queue_items ORDER BY dedupe_key
        """
    ).fetchall()
    selected: list[dict[str, Any]] = []
    for row in rows:
        try:
            payload = json.loads(row["payload_json"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise QualificationError("managed queue item contains corrupt JSON") from exc
        if not isinstance(payload, dict):
            raise QualificationError("managed queue item payload is not an object")
        if payload.get("sprint_id") != sprint_id:
            continue
        selected.append(
            {
                "dedupe_key": row["dedupe_key"],
                "event_id": row["event_id"],
                "event_type": row["event_type"],
                "payload": payload,
                "receipt_id": row["receipt_id"],
                "created_at": row["created_at"],
            }
        )
    return selected


def _string_ids(value: Any, key: str) -> list[str]:
    if not isinstance(value, list):
        return []
    identifiers = []
    for item in value:
        if isinstance(item, dict) and isinstance(item.get(key), str) and item[key]:
            identifiers.append(item[key])
    return identifiers


def _objects_by(items: Any, key: str, label: str) -> dict[str, dict[str, Any]]:
    if not isinstance(items, list):
        raise QualificationError(f"managed state {label} must be a list")
    result: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            raise QualificationError(f"managed state {label} item is invalid")
        identifier = item.get(key)
        if not isinstance(identifier, str) or not identifier or identifier in result:
            raise QualificationError(f"managed state {label} identity is invalid")
        result[identifier] = item
    return result


def _owner_rows_by(
    items: Any, key: str, label: str
) -> dict[str, dict[str, Any]]:
    if not isinstance(items, list):
        raise QualificationError(f"managed state {label} must be a list")
    result: dict[str, dict[str, Any]] = {}
    for wrapper in items:
        if not isinstance(wrapper, dict):
            raise QualificationError(f"managed state {label} row is invalid")
        value = wrapper.get("json")
        relational = wrapper.get("relational")
        if not isinstance(value, dict) or not isinstance(relational, dict):
            raise QualificationError(f"managed state {label} row lacks parity data")
        identifier = value.get(key)
        if not isinstance(identifier, str) or not identifier or identifier in result:
            raise QualificationError(f"managed state {label} identity is invalid")
        if relational.get(key) != identifier:
            raise QualificationError(f"managed state {label} relational identity differs")
        result[identifier] = wrapper
    return result


def _path_key(value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise QualificationError("managed state contains an invalid path")
    path = Path(value)
    if not path.is_absolute():
        raise QualificationError("managed state path is not absolute")
    return os.path.normcase(str(path.resolve(strict=False)))


def _stable_value_proof(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, (list, dict)):
        raise QualificationError(f"managed state {label} is not a collection")
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return {"count": len(value), "sha256": hashlib.sha256(encoded).hexdigest()}


def _parse_delivery_timestamp(value: Any) -> datetime:
    if not isinstance(value, str) or not value or value.strip() != value:
        raise QualificationError("initial assignment delivery timestamp is invalid")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise QualificationError(
            "initial assignment delivery timestamp is invalid"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise QualificationError("initial assignment delivery timestamp is invalid")
    return parsed.astimezone(timezone.utc)


def _validate_initial_assignment_delivery(
    state: Mapping[str, Any],
    queue_items: Sequence[Mapping[str, Any]],
    *,
    sprint_id: str,
    expected_assignments: set[str],
) -> None:
    """Require the initial assignment outbox to be durably and exactly drained."""

    outbox = state.get("outbox")
    if not isinstance(outbox, list):
        raise QualificationError("managed sprint outbox is missing")
    if (
        len(expected_assignments) != EXPECTED_CHILD_COUNT
        or len(outbox) != EXPECTED_CHILD_COUNT
        or len(queue_items) != EXPECTED_CHILD_COUNT
    ):
        raise QualificationError("initial assignment delivery is not quiescent")

    assignments = _objects_by(state.get("assignments"), "assignment_id", "assignments")
    if set(assignments) != expected_assignments:
        raise QualificationError("initial assignment delivery set is invalid")

    outbox_keys = {
        "event_id",
        "dedupe_key",
        "event_type",
        "payload",
        "status",
        "created_at",
        "delivered_at",
        "queue_receipt_id",
    }
    queue_keys = {
        "dedupe_key",
        "event_id",
        "event_type",
        "payload",
        "receipt_id",
        "created_at",
    }
    payload_keys = {
        "sprint_id",
        "graph_revision",
        "assignment_id",
        "node_id",
        "agent_phone",
    }
    delivered_by_assignment: dict[str, Mapping[str, Any]] = {}
    queued_by_assignment: dict[str, Mapping[str, Any]] = {}
    outbox_event_ids: set[str] = set()
    outbox_receipt_ids: set[str] = set()
    queue_event_ids: set[str] = set()
    queue_receipt_ids: set[str] = set()

    for item in outbox:
        if not isinstance(item, Mapping) or set(item) != outbox_keys:
            raise QualificationError("initial assignment outbox row is invalid")
        payload = item.get("payload")
        if not isinstance(payload, Mapping) or set(payload) != payload_keys:
            raise QualificationError("initial assignment outbox payload is invalid")
        assignment_id = payload.get("assignment_id")
        assignment = assignments.get(assignment_id) if isinstance(assignment_id, str) else None
        if not isinstance(assignment, Mapping):
            raise QualificationError("initial assignment outbox set is invalid")
        expected_payload = {
            "sprint_id": sprint_id,
            "graph_revision": assignment.get("graph_revision"),
            "assignment_id": assignment_id,
            "node_id": assignment.get("node_id"),
            "agent_phone": assignment.get("agent_phone"),
        }
        expected_dedupe_key = f"enqueue:assignment:{sprint_id}:{assignment_id}"
        event_id = item.get("event_id")
        receipt_id = item.get("queue_receipt_id")
        created_at = _parse_delivery_timestamp(item.get("created_at"))
        delivered_at = _parse_delivery_timestamp(item.get("delivered_at"))
        if (
            assignment_id not in expected_assignments
            or assignment_id in delivered_by_assignment
            or not _is_exact_int(assignment.get("graph_revision"), minimum=1)
            or not isinstance(assignment.get("node_id"), str)
            or not assignment["node_id"].strip()
            or not isinstance(assignment.get("agent_phone"), str)
            or not assignment["agent_phone"].strip()
            or item.get("event_type") != "ASSIGNMENT_ENQUEUE"
            or item.get("dedupe_key") != expected_dedupe_key
            or not _json_exact_equal(payload, expected_payload)
            or item.get("status") != "delivered"
            or delivered_at < created_at
            or not isinstance(event_id, str)
            or not event_id.strip()
            or event_id in outbox_event_ids
            or not isinstance(receipt_id, str)
            or not receipt_id.strip()
            or receipt_id in outbox_receipt_ids
        ):
            raise QualificationError("initial assignment outbox identity is invalid")
        delivered_by_assignment[assignment_id] = item
        outbox_event_ids.add(event_id)
        outbox_receipt_ids.add(receipt_id)

    for item in queue_items:
        if not isinstance(item, Mapping) or set(item) != queue_keys:
            raise QualificationError("initial assignment queue row is invalid")
        payload = item.get("payload")
        if not isinstance(payload, Mapping) or set(payload) != payload_keys:
            raise QualificationError("initial assignment queue payload is invalid")
        assignment_id = payload.get("assignment_id")
        assignment = assignments.get(assignment_id) if isinstance(assignment_id, str) else None
        if not isinstance(assignment, Mapping):
            raise QualificationError("initial assignment queue set is invalid")
        expected_payload = {
            "sprint_id": sprint_id,
            "graph_revision": assignment.get("graph_revision"),
            "assignment_id": assignment_id,
            "node_id": assignment.get("node_id"),
            "agent_phone": assignment.get("agent_phone"),
        }
        expected_dedupe_key = f"enqueue:assignment:{sprint_id}:{assignment_id}"
        event_id = item.get("event_id")
        receipt_id = item.get("receipt_id")
        _parse_delivery_timestamp(item.get("created_at"))
        if (
            assignment_id not in expected_assignments
            or assignment_id in queued_by_assignment
            or item.get("event_type") != "ASSIGNMENT_ENQUEUE"
            or item.get("dedupe_key") != expected_dedupe_key
            or not _json_exact_equal(payload, expected_payload)
            or not isinstance(item.get("created_at"), str)
            or not item["created_at"].strip()
            or not isinstance(event_id, str)
            or not event_id.strip()
            or event_id in queue_event_ids
            or not isinstance(receipt_id, str)
            or not receipt_id.strip()
            or receipt_id in queue_receipt_ids
        ):
            raise QualificationError("initial assignment queue identity is invalid")
        queued_by_assignment[assignment_id] = item
        queue_event_ids.add(event_id)
        queue_receipt_ids.add(receipt_id)

    if (
        set(delivered_by_assignment) != expected_assignments
        or set(queued_by_assignment) != expected_assignments
    ):
        raise QualificationError("initial assignment delivery set is invalid")

    for assignment_id in sorted(expected_assignments):
        outbox_item = delivered_by_assignment[assignment_id]
        queue_item = queued_by_assignment[assignment_id]
        if (
            not _json_exact_equal(outbox_item.get("payload"), queue_item.get("payload"))
            or outbox_item.get("event_id") != queue_item.get("event_id")
            or outbox_item.get("dedupe_key") != queue_item.get("dedupe_key")
            or outbox_item.get("queue_receipt_id") != queue_item.get("receipt_id")
            or outbox_item.get("created_at") != queue_item.get("created_at")
        ):
            raise QualificationError("initial assignment delivery parity is invalid")


def validate_runtime_snapshot(
    bundle: Mapping[str, Any],
    *,
    expected_sha: str,
    expected_ref: str,
    project_id: str,
    start_response: Mapping[str, Any],
    expected_child_python: Path,
    expected_runtime_root: Path,
    expected_managed_root: Path,
    expected_service_root: Path,
    expected_protected_roots: Sequence[Path],
    expected_canonical_remote: str,
) -> dict[str, Any]:
    row = bundle.get("row")
    state = bundle.get("state")
    if not isinstance(row, Mapping) or not isinstance(state, Mapping):
        raise QualificationError("managed runtime snapshot is incomplete")
    sprint_id = start_response.get("sprint_id")
    if (
        row.get("project_id") != project_id
        or row.get("sprint_id") != sprint_id
        or row.get("status") != "active"
        or not _is_exact_int(row.get("fencing_token"), minimum=1)
        or state.get("sprint_id") != sprint_id
        or state.get("status") != "active"
    ):
        raise QualificationError("managed runtime row identity/status mismatch")
    response_identity = start_response.get("identity")
    state_identity = state.get("identity")
    if not isinstance(response_identity, Mapping) or not isinstance(state_identity, Mapping):
        raise QualificationError("managed source identity is missing")
    for identity in (response_identity, state_identity):
        if identity.get("commit") != expected_sha or identity.get("project_id") != project_id:
            raise QualificationError("imported identity does not equal expected source")
    for key in (
        "project_id",
        "repository_id",
        "commit",
        "manifest_path",
        "manifest_sha256",
    ):
        if not response_identity.get(key) or state_identity.get(key) != response_identity.get(key):
            raise QualificationError(f"durable source identity changed {key}")
    if (
        start_response.get("workspace_source_commit") != expected_sha
        or state.get("workspace_source_commit") != expected_sha
        or state.get("requested_ref") != expected_ref
    ):
        raise QualificationError("workspace source commit/ref is not exact")
    assignment_ids = start_response.get("initial_assignment_ids")
    if (
        not isinstance(assignment_ids, list)
        or len(assignment_ids) != EXPECTED_CHILD_COUNT
        or len(set(assignment_ids)) != EXPECTED_CHILD_COUNT
        or any(not isinstance(item, str) or not item for item in assignment_ids)
    ):
        raise QualificationError("start response does not contain four assignments")
    expected_assignments = set(assignment_ids)
    assignments = _objects_by(state.get("assignments"), "assignment_id", "assignments")
    workspaces = _objects_by(state.get("workspaces"), "workspace_id", "workspaces")
    processes = _objects_by(state.get("processes"), "process_id", "processes")
    leases = _objects_by(state.get("port_leases"), "lease_id", "port leases")
    if any(len(items) != EXPECTED_CHILD_COUNT for items in (assignments, workspaces, processes, leases)):
        raise QualificationError("managed state does not contain exactly four resources")
    if set(assignments) != expected_assignments:
        raise QualificationError("managed assignment set differs from start response")
    workspace_assignments = {item.get("assignment_id") for item in workspaces.values()}
    process_assignments = {item.get("assignment_id") for item in processes.values()}
    lease_assignments = {item.get("assignment_id") for item in leases.values()}
    if not all(
        item == expected_assignments
        for item in (workspace_assignments, process_assignments, lease_assignments)
    ):
        raise QualificationError("resource ownership is not one-to-one by assignment")
    if any(
        assignment.get("source_commit") != expected_sha
        or assignment.get("initial_head_commit") != expected_sha
        for assignment in assignments.values()
    ):
        raise QualificationError("assignment source commit is not exact")
    repository = state.get("repository")
    canonical_remote = (
        repository.get("canonical_remote") if isinstance(repository, Mapping) else None
    )
    if canonical_remote != expected_canonical_remote:
        raise QualificationError("managed repository identity is not exact")
    runtime_config = state.get("runtime_config")
    if not isinstance(runtime_config, Mapping):
        raise QualificationError("managed runtime config is missing")
    runtime_root = Path(str(runtime_config.get("runtime_root") or ""))
    runtime_base = Path(str(runtime_config.get("process_runtime_root") or ""))
    managed_base = Path(str(runtime_config.get("managed_root") or ""))
    if (
        not runtime_root.is_absolute()
        or not runtime_base.is_absolute()
        or not managed_base.is_absolute()
    ):
        raise QualificationError("managed runtime roots are not absolute")
    if (
        _path_key(str(runtime_root)) != _path_key(str(expected_runtime_root))
        or _path_key(str(runtime_base))
        != _path_key(str(expected_runtime_root / "processes"))
        or _path_key(str(managed_base)) != _path_key(str(expected_managed_root))
        or _path_key(str(runtime_config.get("lease_root") or ""))
        != _path_key(str(expected_runtime_root / "leases"))
        or _path_key(str(runtime_config.get("log_root") or ""))
        != _path_key(str(expected_runtime_root / "logs"))
        or _path_key(str(runtime_config.get("pid_root") or ""))
        != _path_key(str(expected_runtime_root / "pids"))
        or _path_key(str(runtime_config.get("service_root") or ""))
        != _path_key(str(expected_service_root))
        or _path_key(str(runtime_config.get("prompt_root") or ""))
        != _path_key(str(expected_runtime_root.parent / "prompt"))
        or runtime_config.get("http_host") != "127.0.0.1"
        or not _is_exact_int(runtime_config.get("http_port"), expected=STAGING_HTTP_PORT)
        or not _is_exact_int(
            runtime_config.get("child_port_start"), expected=CHILD_PORT_START
        )
        or not _is_exact_int(
            runtime_config.get("child_port_end"), expected=CHILD_PORT_END
        )
        or runtime_config.get("instance_id") != STAGING_INSTANCE_ID
        or runtime_config.get("disable_telegram") is not True
        or runtime_config.get("disable_tunnel") is not True
    ):
        raise QualificationError("managed runtime does not use the exact staging boundary")
    actual_protected_roots = runtime_config.get("protected_roots")
    if not isinstance(actual_protected_roots, list) or [
        _path_key(str(item)) for item in actual_protected_roots
    ] != [_path_key(str(item)) for item in expected_protected_roots]:
        raise QualificationError("managed runtime protected roots are not exact")
    workspace_paths: set[str] = set()
    workspace_by_assignment: dict[str, dict[str, Any]] = {}
    for workspace in workspaces.values():
        if (
            workspace.get("source_commit") != expected_sha
            or workspace.get("initial_head_commit") != expected_sha
            or workspace.get("working_tree_state") != "clean"
            or workspace.get("repository_remote") != canonical_remote
        ):
            raise QualificationError("workspace source/provenance mismatch")
        expected_root = _path_key(workspace.get("expected_root"))
        actual_root = _path_key(workspace.get("actual_git_toplevel"))
        if expected_root != actual_root or not _same_or_within(Path(actual_root), managed_base):
            raise QualificationError("workspace root is redirected or outside managed root")
        if actual_root in workspace_paths:
            raise QualificationError("workspace roots are not unique")
        workspace_paths.add(actual_root)
        workspace_by_assignment[str(workspace["assignment_id"])] = workspace
    process_pids: set[int] = set()
    process_roots: set[str] = set()
    process_by_assignment: dict[str, dict[str, Any]] = {}
    lease_by_assignment = {
        str(item["assignment_id"]): item for item in leases.values()
    }
    ports: set[int] = set()
    instance_ids: set[str] = set()
    namespaces: set[str] = set()
    for process in processes.values():
        assignment_id = str(process["assignment_id"])
        process_id = process.get("process_id")
        workspace = workspace_by_assignment[assignment_id]
        lease = lease_by_assignment[assignment_id]
        if process.get("state") != "HEALTHY":
            raise QualificationError("a managed child is not HEALTHY")
        command = process.get("command_redacted")
        if (
            not isinstance(command, list)
            or len(command) != 4
            or _path_key(command[0]) != _path_key(str(expected_child_python))
            or command[1:] != [CHILD_FIXTURE_PATH, "--mode", "serve"]
            or _path_key(str(process.get("executable_path") or ""))
            != _path_key(str(expected_child_python))
            or process.get("environment_redacted") != {}
            or process.get("restart_policy") != "on_failure"
            or not _is_exact_int(process.get("restart_attempt"), minimum=0)
            or not _is_exact_int(process.get("max_restart_attempts"), expected=1)
            or not _is_exact_int(process.get("restart_backoff_seconds"), expected=0)
            or not isinstance(process.get("resource_limits"), dict)
            or set(process["resource_limits"]) != set(EXPECTED_RESOURCE_LIMITS)
            or any(
                not _is_exact_int(
                    process["resource_limits"].get(name), expected=expected
                )
                for name, expected in EXPECTED_RESOURCE_LIMITS.items()
            )
            or not isinstance(process.get("launch_nonce"), str)
            or not process["launch_nonce"]
        ):
            raise QualificationError(
                "managed child command does not use expected staging interpreter"
            )
        pid = process.get("pid")
        if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0 or pid in process_pids:
            raise QualificationError("managed child PIDs are invalid or duplicated")
        process_pids.add(pid)
        if not _is_safe_path_component(process_id):
            raise QualificationError("managed child process identity is not path-safe")
        expected_process_root = (runtime_base / str(process_id)).resolve(strict=False)
        actual_process_root = Path(str(process.get("runtime_root") or "")).resolve(
            strict=False
        )
        runtime_root = _path_key(str(actual_process_root))
        if (
            runtime_root != _path_key(str(expected_process_root))
            or _path_key(str(actual_process_root.parent)) != _path_key(str(runtime_base))
            or not _same_or_within(actual_process_root, runtime_base)
            or any(
                _paths_overlap(actual_process_root, protected)
                for protected in expected_protected_roots
            )
            or runtime_root in process_roots
        ):
            raise QualificationError("process runtime roots are invalid or duplicated")
        process_roots.add(runtime_root)
        if _path_key(process.get("cwd")) != _path_key(workspace.get("actual_git_toplevel")):
            raise QualificationError("process cwd does not equal its owned workspace")
        if (
            process.get("workspace_id") != workspace.get("workspace_id")
            or process.get("port_lease_id") != lease.get("lease_id")
            or lease.get("process_id") != process.get("process_id")
            or lease.get("status") != "bound"
            or lease.get("bind_verified") is not True
            or lease.get("host") != "127.0.0.1"
        ):
            raise QualificationError("process/lease/workspace ownership link is invalid")
        endpoint = process.get("health_endpoint")
        if not isinstance(endpoint, Mapping):
            raise QualificationError("managed child health endpoint is missing")
        port = lease.get("port")
        if (
            type(port) is not int
            or not 1 <= port <= 65535
            or endpoint.get("host") != "127.0.0.1"
            or not _is_exact_int(endpoint.get("port"), expected=port)
            or endpoint.get("path") != "/health"
        ):
            raise QualificationError("managed child endpoint does not match its lease")
        if port in ports:
            raise QualificationError("managed child ports are not unique")
        ports.add(port)
        instance_ids.add(str(lease.get("instance_id") or ""))
        namespaces.add(str(lease.get("network_namespace_id") or ""))
        process_by_assignment[assignment_id] = process
    if (
        instance_ids != {STAGING_INSTANCE_ID}
        or namespaces != {"host"}
    ):
        raise QualificationError("lease instance/namespace ownership is invalid")
    start_port = runtime_config.get("child_port_start")
    end_port = runtime_config.get("child_port_end")
    if (
        type(start_port) is not int
        or type(end_port) is not int
        or any(not start_port <= port <= end_port for port in ports)
    ):
        raise QualificationError("child port is outside staging allocation")
    owner_processes = _owner_rows_by(
        bundle.get("owner_processes"), "process_id", "owner process rows"
    )
    owner_leases = _owner_rows_by(
        bundle.get("owner_leases"), "lease_id", "owner lease rows"
    )
    if set(owner_processes) != set(processes) or set(owner_leases) != set(leases):
        raise QualificationError("durable owner tables do not exactly match sprint resources")
    for process_id, process in processes.items():
        owner_wrapper = owner_processes[process_id]
        owner = owner_wrapper["json"]
        relational = owner_wrapper["relational"]
        if not _json_exact_equal(owner, process) or any(
            not _scalar_exact_equal(relational.get(key), process.get(key))
            for key in (
                "process_id",
                "assignment_id",
                "pid",
                "state",
                "port_lease_id",
            )
        ):
            raise QualificationError("durable process owner differs from sprint state")
    for lease_id, lease in leases.items():
        owner_wrapper = owner_leases[lease_id]
        owner = owner_wrapper["json"]
        relational = owner_wrapper["relational"]
        if not _json_exact_equal(owner, lease) or any(
            not _scalar_exact_equal(relational.get(key), lease.get(key))
            for key in (
                "lease_id",
                "assignment_id",
                "process_id",
                "host",
                "port",
                "status",
                "instance_id",
                "network_namespace_id",
            )
        ):
            raise QualificationError("durable lease owner differs from sprint state")
    live_assignment_processes = _owner_rows_by(
        bundle.get("live_assignment_processes"),
        "process_id",
        "live assignment process owners",
    )
    live_assignment_leases = _owner_rows_by(
        bundle.get("live_assignment_leases"),
        "lease_id",
        "live assignment lease owners",
    )
    if (
        set(live_assignment_processes) != set(processes)
        or set(live_assignment_leases) != set(leases)
        or len(live_assignment_processes) != EXPECTED_CHILD_COUNT
        or len(live_assignment_leases) != EXPECTED_CHILD_COUNT
        or not _json_exact_equal(live_assignment_processes, owner_processes)
        or not _json_exact_equal(live_assignment_leases, owner_leases)
    ):
        raise QualificationError(
            "duplicate or unexpected live process/lease ownership exists"
        )
    queue_items = bundle.get("queue_items")
    if not isinstance(queue_items, list):
        raise QualificationError("managed sprint queue snapshot is missing")
    _validate_initial_assignment_delivery(
        state,
        queue_items,
        sprint_id=sprint_id,
        expected_assignments=expected_assignments,
    )
    side_effects = {
        key: _stable_value_proof(state.get(key), key)
        for key in (
            "import_attempts",
            "outbox",
            "result_receipts",
            "review_assignments",
            "reviews",
            "reworks",
            "transition_journal",
        )
    }
    side_effects["queue_items"] = _stable_value_proof(queue_items, "queue_items")
    return {
        "sprint_id": sprint_id,
        "project_id": project_id,
        "identity_commit": expected_sha,
        "workspace_source_commit": expected_sha,
        "fencing_token": row.get("fencing_token"),
        "side_effects": side_effects,
        "assignments": sorted(expected_assignments),
        "workspaces": [
            {
                "assignment_id": assignment_id,
                "workspace_id": workspace_by_assignment[assignment_id]["workspace_id"],
                "root": workspace_by_assignment[assignment_id]["actual_git_toplevel"],
            }
            for assignment_id in sorted(expected_assignments)
        ],
        "processes": [
            {
                "assignment_id": assignment_id,
                "process_id": process_by_assignment[assignment_id]["process_id"],
                "workspace_id": process_by_assignment[assignment_id]["workspace_id"],
                "pid": process_by_assignment[assignment_id]["pid"],
                "restart_attempt": process_by_assignment[assignment_id].get(
                    "restart_attempt"
                ),
                "runtime_root": process_by_assignment[assignment_id]["runtime_root"],
                "health_endpoint": deepcopy(
                    process_by_assignment[assignment_id]["health_endpoint"]
                ),
                "launch_nonce_sha256": hashlib.sha256(
                    str(process_by_assignment[assignment_id].get("launch_nonce") or "").encode(
                        "utf-8"
                    )
                ).hexdigest(),
            }
            for assignment_id in sorted(expected_assignments)
        ],
        "leases": [
            {
                "assignment_id": assignment_id,
                "lease_id": lease_by_assignment[assignment_id]["lease_id"],
                "process_id": lease_by_assignment[assignment_id]["process_id"],
                "host": lease_by_assignment[assignment_id]["host"],
                "port": lease_by_assignment[assignment_id]["port"],
                "status": lease_by_assignment[assignment_id]["status"],
                "instance_id": lease_by_assignment[assignment_id]["instance_id"],
                "network_namespace_id": lease_by_assignment[assignment_id][
                    "network_namespace_id"
                ],
            }
            for assignment_id in sorted(expected_assignments)
        ],
        "_raw_processes": deepcopy(process_by_assignment),
        "_raw_workspaces": deepcopy(workspace_by_assignment),
        "_expected_child_python": str(expected_child_python),
    }


def verify_child_health(
    summary: Mapping[str, Any],
    *,
    getter: Callable[[str, int, str], Mapping[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    get_health = getter or _get_child_health
    raw_processes = summary.get("_raw_processes")
    raw_workspaces = summary.get("_raw_workspaces")
    if not isinstance(raw_processes, Mapping) or not isinstance(raw_workspaces, Mapping):
        raise QualificationError("runtime summary lacks child verification context")
    results: list[dict[str, Any]] = []
    worker_pids: set[int] = set()
    for assignment_id in sorted(raw_processes):
        process = raw_processes[assignment_id]
        workspace = raw_workspaces[assignment_id]
        endpoint = process["health_endpoint"]
        payload = get_health(endpoint["host"], endpoint["port"], endpoint["path"])
        worker_pid = payload.get("pid")
        if (
            payload.get("status") != "ok"
            or payload.get("process_id") != process.get("process_id")
            or payload.get("identity") != process.get("process_id")
            or _path_key(payload.get("runtime_root"))
            != _path_key(process.get("runtime_root"))
            or _path_key(payload.get("cwd"))
            != _path_key(workspace.get("actual_git_toplevel"))
            or payload.get("host") != "127.0.0.1"
            or not _is_exact_int(payload.get("port"), expected=endpoint["port"])
            or payload.get("launch_nonce") != process.get("launch_nonce")
            or not isinstance(worker_pid, int)
            or isinstance(worker_pid, bool)
            or worker_pid <= 0
            or worker_pid in worker_pids
        ):
            raise QualificationError("child health identity/ownership proof failed")
        worker_pids.add(worker_pid)
        results.append(
            {
                "assignment_id": assignment_id,
                "process_id": process["process_id"],
                "leader_pid": process["pid"],
                "worker_pid": worker_pid,
                "port": endpoint["port"],
                "runtime_root": process["runtime_root"],
                "workspace_root": workspace["actual_git_toplevel"],
            }
        )
    return results


def verify_child_os_ownership(
    summary: Mapping[str, Any],
    health: Sequence[Mapping[str, Any]],
    *,
    identity_fn: Callable[[int], Any] | None = None,
    port_owner_fn: Callable[[str, int], Sequence[int]] | None = None,
    job_open_fn: Callable[[str], Any] | None = None,
    all_listener_fn: Callable[[int], Mapping[str, Sequence[int]]] | None = None,
    os_name: str | None = None,
) -> list[dict[str, Any]]:
    if (os_name or os.name) != "nt":
        raise QualificationError("managed child OS ownership proof requires Windows")
    port_owner_was_injected = port_owner_fn is not None
    if identity_fn is None or port_owner_fn is None or job_open_fn is None:
        try:
            from nginx_qa.process_supervisor import (  # pylint: disable=import-outside-toplevel
                _WindowsJob,
                port_owner_pids,
                process_identity,
            )
        except ImportError as exc:
            raise QualificationError("Windows Job ownership provider is unavailable") from exc
        identity_fn = identity_fn or process_identity
        port_owner_fn = port_owner_fn or port_owner_pids
        job_open_fn = job_open_fn or _WindowsJob.open
    if all_listener_fn is None:
        if port_owner_was_injected:
            assert port_owner_fn is not None

            def injected_exact_listeners(port: int) -> Mapping[str, Sequence[int]]:
                return {"127.0.0.1": tuple(port_owner_fn("127.0.0.1", port))}

            all_listener_fn = injected_exact_listeners
        else:
            listener_snapshot = _windows_listener_snapshot()

            def snapshot_listeners(port: int) -> Mapping[str, Sequence[int]]:
                return listener_snapshot.get(port, {})

            all_listener_fn = snapshot_listeners
    raw_processes = summary.get("_raw_processes")
    expected_child_python = summary.get("_expected_child_python")
    if not isinstance(raw_processes, Mapping) or not isinstance(
        expected_child_python, str
    ):
        raise QualificationError("runtime summary lacks OS ownership context")
    health_by_assignment = {
        str(item.get("assignment_id")): item
        for item in health
        if isinstance(item, Mapping)
    }
    expected_ports = {
        int(process["health_endpoint"]["port"])
        for process in raw_processes.values()
    }
    owners_by_port: dict[int, tuple[int, ...]] = {}
    for port in range(CHILD_PORT_START, CHILD_PORT_END + 1):
        all_listeners = {
            str(address): tuple(sorted({int(pid) for pid in pids}))
            for address, pids in all_listener_fn(port).items()
            if pids
        }
        if any(address != "127.0.0.1" for address in all_listeners):
            raise QualificationError(
                f"non-loopback or wildcard listener exists in staging child range at {port}"
            )
        owners = tuple(sorted({int(pid) for pid in port_owner_fn("127.0.0.1", port)}))
        if owners != all_listeners.get("127.0.0.1", ()):
            raise QualificationError("child listener ownership scan is inconsistent")
        if owners:
            owners_by_port[port] = owners
            if port not in expected_ports:
                raise QualificationError(
                    f"orphan listener exists in staging child range at {port}"
                )
    if set(owners_by_port) != expected_ports:
        raise QualificationError("not every managed child owns its declared listener")
    proofs: list[dict[str, Any]] = []
    for assignment_id in sorted(raw_processes):
        process = raw_processes[assignment_id]
        process_id = str(process.get("process_id") or "")
        launch_nonce = str(process.get("launch_nonce") or "")
        expected_job = "Global\\nginx-qa-managed-" + hashlib.sha256(
            (process_id + "\0" + launch_nonce).encode("utf-8")
        ).hexdigest()[:40]
        if (
            not process_id
            or not launch_nonce
            or process.get("job_object_id") != expected_job
            or process.get("process_group_id") != expected_job
        ):
            raise QualificationError("managed child named Job identity is invalid")
        leader_pid = process.get("pid")
        if not _is_exact_int(leader_pid, minimum=1):
            raise QualificationError("managed child durable leader PID is invalid")
        identity = identity_fn(leader_pid)
        if identity is None:
            raise QualificationError("managed child durable leader is not alive")
        birth_token = process.get("os_process_birth_token")
        if (
            not isinstance(birth_token, str)
            or not birth_token
            or getattr(identity, "pid", None) != leader_pid
            or identity.birth_token != birth_token
            or _path_key(str(identity.executable_path))
            != _path_key(str(process.get("executable_path") or ""))
            or _path_key(str(identity.executable_path))
            != _path_key(expected_child_python)
            or (
                identity.cwd is not None
                and _path_key(str(identity.cwd))
                != _path_key(str(process.get("cwd") or ""))
            )
        ):
            raise QualificationError(
                "managed child durable leader birth/executable/cwd differs"
            )
        job = job_open_fn(expected_job)
        if job is None:
            raise QualificationError("managed child named Windows Job is unavailable")
        port = int(process["health_endpoint"]["port"])
        port_owners = owners_by_port[port]
        try:
            if not job.contains_exact(identity):
                raise QualificationError("durable leader is outside its named Windows Job")
            if any(not job.contains(owner_pid) for owner_pid in port_owners):
                raise QualificationError("child listener owner is outside its named Windows Job")
        finally:
            job.close()
        health_item = health_by_assignment.get(assignment_id)
        if not isinstance(health_item, Mapping) or health_item.get("worker_pid") not in port_owners:
            raise QualificationError("health worker is not an authenticated port owner")
        proofs.append(
            {
                "assignment_id": assignment_id,
                "process_id": process_id,
                "leader_pid": leader_pid,
                "leader_birth_token_sha256": hashlib.sha256(
                    birth_token.encode("utf-8")
                ).hexdigest(),
                "leader_executable": str(identity.executable_path),
                "leader_cwd": None if identity.cwd is None else str(identity.cwd),
                "job_object_id": expected_job,
                "port": port,
                "port_owner_pids": list(port_owners),
            }
        )
    return proofs


def _get_child_health(host: str, port: int, path: str) -> Mapping[str, Any]:
    if (
        host != "127.0.0.1"
        or not isinstance(port, int)
        or isinstance(port, bool)
        or not CHILD_PORT_START <= port <= CHILD_PORT_END
        or port == LIVE_HTTP_PORT
        or path != "/health"
    ):
        raise QualificationError("refusing non-exact child health endpoint")
    url = f"http://127.0.0.1:{port}/health"
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json"},
        method="GET",
    )
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({}),
        _NoRedirect(),
    )
    try:
        with opener.open(request, timeout=5.0) as response:
            if response.status != 200 or response.geturl() != url:
                raise QualificationError("child health endpoint is not exact HTTP 200")
            encoded = response.read(MAX_HTTP_BODY_BYTES + 1)
    except urllib.error.HTTPError as exc:
        raise QualificationError("child health endpoint returned an HTTP error") from exc
    except (OSError, urllib.error.URLError) as exc:
        raise QualificationError("child health endpoint request failed") from exc
    if len(encoded) > MAX_HTTP_BODY_BYTES:
        raise QualificationError("child health response exceeds size limit")
    try:
        payload = json.loads(encoded.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise QualificationError("child health endpoint returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise QualificationError("child health payload is not an object")
    return payload


def _evidence_summary(summary: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: deepcopy(value)
        for key, value in summary.items()
        if not str(key).startswith("_raw_")
    }


def validate_adoption(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    *,
    host_pid_before: int,
    host_pid_after: int,
) -> None:
    if (
        type(host_pid_before) is not int
        or type(host_pid_after) is not int
        or host_pid_before <= 0
        or host_pid_after <= 0
        or host_pid_before == host_pid_after
    ):
        raise QualificationError("staging host listener PID did not change")
    for key in (
        "sprint_id",
        "project_id",
        "identity_commit",
        "workspace_source_commit",
        "fencing_token",
        "side_effects",
        "assignments",
        "workspaces",
        "processes",
        "leases",
    ):
        if not _json_exact_equal(before.get(key), after.get(key)):
            raise QualificationError(f"managed child adoption changed {key}")
    child_pids = {
        int(process["pid"])
        for process in after.get("processes", [])
        if isinstance(process, Mapping) and type(process.get("pid")) is int
    }
    if host_pid_before in child_pids or host_pid_after in child_pids:
        raise QualificationError("staging host listener aliases a child leader")


def _windows_listener_snapshot() -> dict[int, dict[str, tuple[int, ...]]]:
    """Return one fail-closed snapshot of every Windows TCP listener owner."""

    if os.name != "nt":
        raise QualificationError(
            "automatic listener PID proof currently requires Windows; "
            "run this qualification on the staging host"
        )
    script = (
        "$ErrorActionPreference='Stop';"
        "$all=@(Get-NetTCPConnection -State Listen -ErrorAction Stop);"
        "$r=@($all | ForEach-Object {[ordered]@{port=[int]$_.LocalPort;"
        "address=[string]$_.LocalAddress;"
        "pid=[int]$_.OwningProcess}});"
        "ConvertTo-Json -InputObject $r -Compress"
    )
    try:
        completed = subprocess.run(
            ("powershell.exe", "-NoProfile", "-NonInteractive", "-Command", script),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=15.0,
            text=True,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise QualificationError("cannot inspect staging listener ownership") from exc
    if completed.returncode != 0 or not completed.stdout.strip():
        raise QualificationError("cannot enumerate Windows TCP listeners")
    try:
        decoded = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise QualificationError("staging listener ownership output is invalid") from exc
    if not isinstance(decoded, list):
        raise QualificationError("staging listener ownership output is invalid")
    owners: dict[int, dict[str, set[int]]] = {}
    for row in decoded:
        if not isinstance(row, Mapping):
            raise QualificationError("staging listener ownership row is invalid")
        port = row.get("port")
        address = row.get("address")
        pid = row.get("pid")
        if (
            not _is_exact_int(port, minimum=1)
            or port > 65535
            or not isinstance(address, str)
            or not address
            or not _is_exact_int(pid, minimum=1)
        ):
            raise QualificationError("staging listener ownership row is invalid")
        owners.setdefault(port, {}).setdefault(address, set()).add(pid)
    return {
        port: {
            address: tuple(sorted(pids))
            for address, pids in sorted(addresses.items())
        }
        for port, addresses in sorted(owners.items())
    }


def _windows_listener_owners(port: int) -> dict[str, tuple[int, ...]]:
    """Return every owner for one port from a fail-closed listener snapshot."""

    if not _is_exact_int(port, minimum=1) or port > 65535:
        raise QualificationError("listener inspection port is invalid")
    return _windows_listener_snapshot().get(port, {})


def staging_listener_pid(port: int) -> int:
    """Return the sole PID bound only to staging's exact loopback address."""

    owners = _windows_listener_owners(port)
    if set(owners) != {"127.0.0.1"} or len(owners["127.0.0.1"]) != 1:
        raise QualificationError("staging port does not have exactly one loopback owner")
    return owners["127.0.0.1"][0]


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
    except OSError as exc:
        raise QualificationError(f"cannot hash owned file: {path}") from exc
    return digest.hexdigest()


def _assert_not_reparse(path: Path, label: str) -> None:
    try:
        stat_result = os.lstat(path)
    except OSError as exc:
        raise QualificationError(f"cannot inspect {label}") from exc
    attributes = int(getattr(stat_result, "st_file_attributes", 0) or 0)
    if attributes & 0x400:
        raise QualificationError(f"{label} must not be a reparse point")


def validate_ownership_marker(config: PrepareConfig) -> dict[str, Any]:
    marker = config.ownership_marker
    if marker.name != ".nginx-qa-staging-owner.json":
        raise QualificationError("ownership marker must use the fixed launcher name")
    if marker.stat().st_size > 32768:
        raise QualificationError("ownership marker is unexpectedly large")
    _assert_not_reparse(marker, "ownership marker")
    try:
        value = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise QualificationError("ownership marker is invalid JSON") from exc
    if not isinstance(value, dict) or set(value) != {
        "schema_version",
        "initialization_id",
        "ownership",
    }:
        raise QualificationError("ownership marker schema is not exact")
    raw_initialization_id = value.get("initialization_id")
    try:
        if not isinstance(raw_initialization_id, str):
            raise ValueError("initialization id is not a string")
        initialization_id = UUID(raw_initialization_id)
    except (TypeError, ValueError, AttributeError) as exc:
        raise QualificationError("ownership marker initialization id is invalid") from exc
    if (
        initialization_id.int == 0
        or str(initialization_id) != raw_initialization_id
        or not _is_exact_int(value.get("schema_version"), expected=1)
    ):
        raise QualificationError("ownership marker identity is invalid")
    ownership = value.get("ownership")
    expected_keys = {
        "instance_id",
        "approved_commit",
        "service_root",
        "origin",
        "branch",
        "state_base",
        "venv_root",
        "base_python",
        "base_python_prefix",
        "base_python_version",
        "base_python_sha256",
        "runtime_root",
        "prompt_root",
        "managed_root",
        "legacy_runtime_root",
        "http_host",
        "http_port",
        "child_port_range",
        "protected_roots",
    }
    if not isinstance(ownership, dict) or set(ownership) != expected_keys:
        raise QualificationError("ownership marker payload is not exact")
    state_base = marker.parent.resolve(strict=False)
    _assert_not_reparse(state_base, "ownership marker state base")
    venv_root = config.expected_child_python.parent.parent.resolve(strict=False)
    prompt_root = Path(str(ownership.get("prompt_root") or ""))
    base_python = Path(str(ownership.get("base_python") or ""))
    base_prefix = Path(str(ownership.get("base_python_prefix") or ""))
    if (
        _path_key(str(ownership.get("service_root") or ""))
        != _path_key(str(config.repo_root))
        or ownership.get("origin") != config.git_address
        or ownership.get("branch") != config.ref.removeprefix("refs/heads/")
        or ownership.get("approved_commit") != config.expected_sha
        or _path_key(str(ownership.get("state_base") or ""))
        != _path_key(str(state_base))
        or _path_key(str(ownership.get("venv_root") or ""))
        != _path_key(str(venv_root))
        or _path_key(str(ownership.get("runtime_root") or ""))
        != _path_key(str(config.expected_runtime_root))
        or _path_key(str(ownership.get("managed_root") or ""))
        != _path_key(str(config.expected_managed_root))
        or _path_key(str(ownership.get("prompt_root") or ""))
        != _path_key(str(EXPECTED_PROMPT_ROOT))
        or _path_key(str(ownership.get("legacy_runtime_root") or ""))
        != _path_key(str(config.repo_root / "runtime_state"))
        or ownership.get("instance_id") != STAGING_INSTANCE_ID
        or ownership.get("http_host") != "127.0.0.1"
        or not _is_exact_int(ownership.get("http_port"), expected=STAGING_HTTP_PORT)
        or ownership.get("child_port_range") != "18100-18199"
    ):
        raise QualificationError("ownership marker does not match this qualification")
    for label, root in (
        ("venv", venv_root),
        ("runtime", config.expected_runtime_root),
        ("managed", config.expected_managed_root),
        ("prompt", prompt_root),
    ):
        if not root.is_absolute() or root.parent.resolve(strict=False) != state_base:
            raise QualificationError(f"ownership marker {label} root is not state-local")
        _assert_not_reparse(root, f"ownership marker {label} root")
    owned_roots = (venv_root, config.expected_runtime_root, config.expected_managed_root, prompt_root)
    if any(
        _paths_overlap(first, second)
        for index, first in enumerate(owned_roots)
        for second in owned_roots[index + 1 :]
    ):
        raise QualificationError("ownership marker mutable roots overlap")
    marker_protected = ownership.get("protected_roots")
    if not isinstance(marker_protected, list) or [
        _path_key(str(item)) for item in marker_protected
    ] != [_path_key(str(item)) for item in config.protected_roots]:
        raise QualificationError("ownership marker protected roots differ")
    if (
        not base_python.is_absolute()
        or not base_python.is_file()
        or not base_prefix.is_absolute()
        or not base_prefix.is_dir()
    ):
        raise QualificationError("ownership marker base Python provenance is invalid")
    if any(
        _paths_overlap(path, boundary)
        for path in (base_python, base_prefix)
        for boundary in (config.repo_root, state_base, *config.protected_roots)
    ):
        raise QualificationError("ownership marker base Python overlaps an isolated boundary")
    _assert_not_reparse(base_python, "ownership marker base Python")
    _assert_not_reparse(base_prefix, "ownership marker base Python prefix")
    declared_base_hash = ownership.get("base_python_sha256")
    if (
        not isinstance(declared_base_hash, str)
        or re.fullmatch(r"[0-9a-f]{64}", declared_base_hash) is None
        or _hash_file(base_python) != declared_base_hash
        or not isinstance(ownership.get("base_python_version"), str)
        or ownership.get("base_python_version") != platform.python_version()
        or not _same_or_within(base_python, base_prefix)
    ):
        raise QualificationError("ownership marker base Python hash/version differs")
    for protected in config.protected_roots:
        if any(_paths_overlap(root, protected) for root in (*owned_roots, state_base)):
            raise QualificationError("ownership marker overlaps a protected root")
    return {
        "marker_path": str(marker),
        "marker_sha256": _hash_file(marker),
        "initialization_id": str(initialization_id),
        "instance_id": STAGING_INSTANCE_ID,
        "approved_commit": config.expected_sha,
        "origin": config.git_address,
        "branch": config.ref.removeprefix("refs/heads/"),
        "service_root": str(config.repo_root),
        "state_base": str(state_base),
        "venv_root": str(venv_root),
        "runtime_root": str(config.expected_runtime_root),
        "prompt_root": str(prompt_root.resolve(strict=False)),
        "managed_root": str(config.expected_managed_root),
        "base_python": str(base_python.resolve(strict=False)),
        "base_python_prefix": str(base_prefix.resolve(strict=False)),
        "base_python_version": str(ownership["base_python_version"]),
        "base_python_sha256": str(declared_base_hash),
        "http_host": "127.0.0.1",
        "http_port": STAGING_HTTP_PORT,
        "child_port_range": "18100-18199",
        "protected_roots": [str(path) for path in config.protected_roots],
    }


def _windows_process_chain(pid: int) -> list[dict[str, Any]]:
    if os.name != "nt":
        raise QualificationError("staging ownership proof requires Windows")
    script = (
        "$ErrorActionPreference='Stop';$r=@();$id=" + str(int(pid)) + ";"
        "for($i=0;$i -lt " + str(MAX_PROCESS_CHAIN_DEPTH) + " -and $id -gt 0;$i++){"
        "$p=Get-CimInstance Win32_Process -Filter ('ProcessId = '+$id);"
        "if($null -eq $p){break};$r+=[pscustomobject]@{pid=[int]$p.ProcessId;"
        "parent_pid=[int]$p.ParentProcessId;executable_path=[string]$p.ExecutablePath;"
        "command_line=[string]$p.CommandLine};$id=[int]$p.ParentProcessId};"
        "$r|ConvertTo-Json -Compress"
    )
    try:
        completed = subprocess.run(
            ("powershell.exe", "-NoProfile", "-NonInteractive", "-Command", script),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=20.0,
            text=True,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise QualificationError("cannot inspect staging process ancestry") from exc
    if completed.returncode != 0 or not completed.stdout.strip():
        raise QualificationError("staging process ancestry is unavailable")
    try:
        decoded = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise QualificationError("staging process ancestry output is invalid") from exc
    values = decoded if isinstance(decoded, list) else [decoded]
    if not values or any(not isinstance(item, dict) for item in values):
        raise QualificationError("staging process ancestry is empty")
    return values


def _command_line_argv(command_line: str) -> list[str]:
    if os.name != "nt" or not command_line:
        raise QualificationError("staging process command line is unavailable")
    import ctypes  # imported lazily for the Windows-only qualification
    from ctypes import wintypes

    shell32 = ctypes.WinDLL("shell32", use_last_error=True)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    shell32.CommandLineToArgvW.argtypes = [
        wintypes.LPCWSTR,
        ctypes.POINTER(ctypes.c_int),
    ]
    shell32.CommandLineToArgvW.restype = ctypes.POINTER(ctypes.c_wchar_p)
    kernel32.LocalFree.argtypes = [wintypes.HLOCAL]
    kernel32.LocalFree.restype = wintypes.HLOCAL
    argument_count = ctypes.c_int()
    pointer = shell32.CommandLineToArgvW(command_line, ctypes.byref(argument_count))
    if not pointer:
        raise QualificationError("cannot parse staging process command line")
    try:
        return [pointer[index] for index in range(argument_count.value)]
    finally:
        kernel32.LocalFree(ctypes.cast(pointer, wintypes.HLOCAL))


def _windows_process_cwd(pid: int) -> str:
    """Read a same-bitness Windows process CWD from its immutable PEB snapshot."""

    if os.name != "nt" or not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        raise QualificationError("staging process CWD proof requires a Windows PID")
    import ctypes  # imported lazily for the Windows-only qualification
    from ctypes import wintypes

    class ProcessBasicInformation(ctypes.Structure):
        _fields_ = [
            ("reserved1", ctypes.c_void_p),
            ("peb_base_address", ctypes.c_void_p),
            ("reserved2", ctypes.c_void_p * 2),
            ("unique_process_id", ctypes.c_size_t),
            ("reserved3", ctypes.c_void_p),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    ntdll = ctypes.WinDLL("ntdll", use_last_error=True)
    kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.ReadProcessMemory.argtypes = [
        wintypes.HANDLE,
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_size_t),
    ]
    kernel32.ReadProcessMemory.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    ntdll.NtQueryInformationProcess.argtypes = [
        wintypes.HANDLE,
        wintypes.ULONG,
        ctypes.c_void_p,
        wintypes.ULONG,
        ctypes.POINTER(wintypes.ULONG),
    ]
    ntdll.NtQueryInformationProcess.restype = wintypes.LONG

    handle = kernel32.OpenProcess(0x0400 | 0x0010, False, pid)
    if not handle:
        raise QualificationError("cannot open staging listener for CWD proof")

    def read_memory(address: int, size: int) -> bytes:
        if address <= 0 or size <= 0 or size > 32768:
            raise QualificationError("staging process CWD memory descriptor is invalid")
        buffer = ctypes.create_string_buffer(size)
        read = ctypes.c_size_t()
        if not kernel32.ReadProcessMemory(
            handle,
            ctypes.c_void_p(address),
            buffer,
            size,
            ctypes.byref(read),
        ) or read.value != size:
            raise QualificationError("cannot read staging listener CWD")
        return buffer.raw

    try:
        basic = ProcessBasicInformation()
        returned = wintypes.ULONG()
        status = int(
            ntdll.NtQueryInformationProcess(
                handle,
                0,
                ctypes.byref(basic),
                ctypes.sizeof(basic),
                ctypes.byref(returned),
            )
        )
        if status != 0 or not basic.peb_base_address:
            raise QualificationError("cannot query staging listener PEB")
        pointer_size = ctypes.sizeof(ctypes.c_void_p)
        peb_process_parameters_offset = 0x20 if pointer_size == 8 else 0x10
        current_directory_offset = 0x38 if pointer_size == 8 else 0x24
        pointer_format_size = 8 if pointer_size == 8 else 4
        process_parameters = int.from_bytes(
            read_memory(
                int(basic.peb_base_address) + peb_process_parameters_offset,
                pointer_format_size,
            ),
            "little",
        )
        unicode_header_size = 16 if pointer_size == 8 else 8
        unicode_header = read_memory(
            process_parameters + current_directory_offset,
            unicode_header_size,
        )
        length = int.from_bytes(unicode_header[0:2], "little")
        maximum_length = int.from_bytes(unicode_header[2:4], "little")
        buffer_offset = 8 if pointer_size == 8 else 4
        buffer_address = int.from_bytes(
            unicode_header[buffer_offset : buffer_offset + pointer_size],
            "little",
        )
        if length <= 0 or length % 2 or maximum_length < length or maximum_length > 32768:
            raise QualificationError("staging process CWD descriptor is invalid")
        try:
            cwd = read_memory(buffer_address, length).decode("utf-16-le")
        except UnicodeDecodeError as exc:
            raise QualificationError("staging process CWD is not valid UTF-16") from exc
        if not cwd or not Path(cwd).is_absolute():
            raise QualificationError("staging process CWD is not absolute")
        return str(Path(cwd).resolve(strict=False))
    finally:
        kernel32.CloseHandle(handle)


def _default_process_identity(pid: int) -> Any:
    try:
        from nginx_qa.process_supervisor import process_identity
    except ImportError as exc:
        raise QualificationError("process identity provider is unavailable") from exc
    return process_identity(pid)


def validate_host_listener_ownership(
    config: PrepareConfig,
    marker_proof: Mapping[str, Any],
    *,
    listener_pid_fn: Callable[[int], int] = staging_listener_pid,
    identity_fn: Callable[[int], Any] = _default_process_identity,
    chain_fn: Callable[[int], list[dict[str, Any]]] = _windows_process_chain,
    argv_fn: Callable[[str], list[str]] = _command_line_argv,
    cwd_fn: Callable[[int], str] = _windows_process_cwd,
) -> dict[str, Any]:
    listener_pid = listener_pid_fn(STAGING_HTTP_PORT)
    identity = identity_fn(listener_pid)
    if identity is None or getattr(identity, "pid", None) != listener_pid:
        raise QualificationError("staging listener process identity is unavailable")
    executable = _path_key(str(identity.executable_path))
    observed_cwd = (
        str(identity.cwd)
        if isinstance(identity.cwd, str) and identity.cwd.strip()
        else cwd_fn(listener_pid)
    )
    cwd = _path_key(observed_cwd)
    base_python = _path_key(str(marker_proof.get("base_python") or ""))
    expected_python = _path_key(str(config.expected_child_python))
    if executable not in {base_python, expected_python} or cwd != _path_key(
        str(config.repo_root)
    ):
        raise QualificationError("staging listener executable/cwd ownership differs")
    chain = chain_fn(listener_pid)
    if not chain or any(not isinstance(item, dict) for item in chain):
        raise QualificationError("staging listener ancestry is inconsistent")
    chain_pids: list[int] = []
    chain_parent_pids: list[int] = []
    for item in chain:
        chain_pid = item.get("pid")
        parent_pid = item.get("parent_pid")
        if (
            type(chain_pid) is not int
            or chain_pid <= 0
            or type(parent_pid) is not int
            or parent_pid < 0
        ):
            raise QualificationError("staging listener ancestry is inconsistent")
        chain_pids.append(chain_pid)
        chain_parent_pids.append(parent_pid)
    if chain_pids[0] != listener_pid:
        raise QualificationError("staging listener ancestry begins at a different PID")
    terminal_parent_pid = chain_parent_pids[-1]
    if (
        len(chain) > MAX_PROCESS_CHAIN_DEPTH
        or len(set(chain_pids)) != len(chain_pids)
        or any(
            chain_parent_pids[index] != chain_pids[index + 1]
            for index in range(len(chain) - 1)
        )
        or terminal_parent_pid in chain_pids
        or (
            terminal_parent_pid > 0
            and len(chain) == MAX_PROCESS_CHAIN_DEPTH
        )
        or _path_key(str(chain[0].get("executable_path") or "")) != executable
    ):
        raise QualificationError("staging listener ancestry is inconsistent")
    argv = argv_fn(str(chain[0].get("command_line") or ""))
    expected_tail = [
        "-E",
        "-s",
        "-B",
        "-m",
        "uvicorn",
        "staging_host_app:app",
        "--host",
        "127.0.0.1",
        "--port",
        "18025",
    ]
    if (
        len(argv) != len(expected_tail) + 1
        or _path_key(argv[0]) not in {base_python, expected_python}
        or argv[1:] != expected_tail
    ):
        raise QualificationError("staging listener command line is not exact")
    ancestry_executables = {
        _path_key(str(item.get("executable_path")))
        for item in chain
        if isinstance(item.get("executable_path"), str)
        and str(item.get("executable_path")).strip()
    }
    if expected_python not in ancestry_executables and _path_key(argv[0]) != expected_python:
        raise QualificationError("staging listener lacks owned venv ancestry")
    birth_token = getattr(identity, "birth_token", None)
    if not isinstance(birth_token, str) or not birth_token:
        raise QualificationError("staging listener birth token is unavailable")
    if listener_pid_fn(STAGING_HTTP_PORT) != listener_pid:
        raise QualificationError("staging listener changed during ownership proof")
    final_identity = identity_fn(listener_pid)
    if (
        final_identity is None
        or getattr(final_identity, "pid", None) != listener_pid
        or getattr(final_identity, "birth_token", None) != birth_token
        or _path_key(str(final_identity.executable_path)) != executable
    ):
        raise QualificationError("staging listener identity changed during ownership proof")
    return {
        "pid": listener_pid,
        "birth_token_sha256": hashlib.sha256(
            birth_token.encode("utf-8")
        ).hexdigest(),
        "executable_path": str(identity.executable_path),
        "cwd": observed_cwd,
        "venv_ancestry": True,
        "command_sha256": hashlib.sha256(
            json.dumps(argv, separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
    }


def _safe_error_code(encoded: bytes) -> str:
    try:
        value = json.loads(encoded.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return "non-JSON error"
    if isinstance(value, dict):
        detail = value.get("detail")
        if isinstance(detail, dict) and isinstance(detail.get("error"), str):
            return detail["error"]
        if isinstance(value.get("error"), str):
            return value["error"]
    return "unclassified error"


def _scan_sensitive_keys(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, nested in value.items():
            clean_key = str(key).strip().casefold().replace("-", "_")
            if clean_key not in NON_SECRET_TOKEN_KEYS and (
                clean_key in SENSITIVE_KEYS
                or clean_key.endswith("_password")
                or clean_key.endswith("_secret")
                or clean_key.endswith("_api_key")
                or clean_key.endswith("_token")
                or clean_key.endswith("_access_token")
                or clean_key.endswith("_refresh_token")
            ):
                raise QualificationError(f"qualification data contains sensitive key at {path}")
            _scan_sensitive_keys(nested, f"{path}.{key}")
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            _scan_sensitive_keys(nested, f"{path}[{index}]")


def validate_external_live_snapshot(value: Mapping[str, Any]) -> dict[str, Any]:
    expected_top = {"schema_version", "captured_at", "listener", "health", "git"}
    if set(value) != expected_top or not _is_exact_int(
        value.get("schema_version"), expected=2
    ):
        raise QualificationError("external live snapshot schema is invalid")
    captured_at = value.get("captured_at")
    listener = value.get("listener")
    health = value.get("health")
    git = value.get("git")
    if not isinstance(captured_at, str) or not captured_at.strip():
        raise QualificationError("external live snapshot capture time is missing")
    if not isinstance(listener, Mapping) or set(listener) != {
        "host",
        "port",
        "pid",
        "started_at",
    }:
        raise QualificationError("external live listener snapshot is invalid")
    listener_pid = listener.get("pid")
    if (
        listener.get("host") != "0.0.0.0"
        or not _is_exact_int(listener.get("port"), expected=LIVE_HTTP_PORT)
        or not _is_exact_int(listener_pid, minimum=1)
        or not isinstance(listener.get("started_at"), str)
        or not str(listener.get("started_at")).strip()
    ):
        raise QualificationError("external live listener identity is invalid")
    if not isinstance(health, Mapping) or set(health) != {
        "endpoint",
        "method",
        "status_code",
        "content_type",
    }:
        raise QualificationError("external live health snapshot is invalid")
    if (
        health.get("endpoint") != "http://127.0.0.1:8025/"
        or health.get("method") != "GET"
        or not _is_exact_int(health.get("status_code"), expected=200)
        or health.get("content_type") != "text/html"
    ):
        raise QualificationError("external live health identity is invalid")
    if not isinstance(git, Mapping) or set(git) != {"head", "branch"}:
        raise QualificationError("external live Git snapshot is invalid")
    if (
        not isinstance(git.get("head"), str)
        or HEX_SHA_RE.fullmatch(str(git.get("head"))) is None
        or not isinstance(git.get("branch"), str)
        or not str(git.get("branch")).strip()
    ):
        raise QualificationError("external live Git identity is invalid")
    parsed_times: dict[str, datetime] = {}
    for label, timestamp in (
        ("capture", captured_at),
        ("listener start", listener.get("started_at")),
    ):
        try:
            parsed = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
        except ValueError as exc:
            raise QualificationError(
                f"external live {label} time is invalid"
            ) from exc
        if parsed.tzinfo is None:
            raise QualificationError(f"external live {label} time needs a timezone")
        parsed_times[label] = parsed.astimezone(timezone.utc)
    if parsed_times["listener start"] > parsed_times["capture"]:
        raise QualificationError("external live snapshot timestamps are not causal")
    _scan_sensitive_keys(value)
    return deepcopy(dict(value))


def external_snapshot_time(value: Mapping[str, Any]) -> datetime:
    raw = value.get("captured_at")
    try:
        parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError as exc:
        raise QualificationError("external live snapshot capture time is invalid") from exc
    if parsed.tzinfo is None:
        raise QualificationError("external live snapshot capture time needs a timezone")
    return parsed.astimezone(timezone.utc)


def require_fresh_external_snapshot(
    value: Mapping[str, Any], *, not_before: str | None = None
) -> None:
    captured = external_snapshot_time(value)
    now = datetime.now(timezone.utc)
    age = (now - captured).total_seconds()
    if age < -60 or age > MAX_SNAPSHOT_AGE_SECONDS:
        raise QualificationError("external live snapshot is stale or future-dated")
    if not_before is not None:
        try:
            boundary = datetime.fromisoformat(not_before.replace("Z", "+00:00"))
        except ValueError as exc:
            raise QualificationError("qualification checkpoint time is invalid") from exc
        if boundary.tzinfo is None or captured <= boundary.astimezone(timezone.utc):
            raise QualificationError("external live after-snapshot predates cleanup")


def load_external_snapshot(path: Path) -> dict[str, Any]:
    try:
        encoded = path.read_bytes()
    except OSError as exc:
        raise QualificationError("cannot read external live snapshot") from exc
    if len(encoded) > MAX_HTTP_BODY_BYTES:
        raise QualificationError("external live snapshot is too large")
    try:
        value = json.loads(encoded.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise QualificationError("external live snapshot is invalid JSON") from exc
    if not isinstance(value, dict):
        raise QualificationError("external live snapshot must be an object")
    return validate_external_live_snapshot(value)


def _without_capture_times(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: deepcopy(nested)
            for key, nested in value.items()
            if str(key) != "captured_at"
        }
    return deepcopy(value)


def compare_external_live_snapshots(before: Mapping[str, Any], after: Mapping[str, Any]) -> None:
    checked_before = validate_external_live_snapshot(before)
    checked_after = validate_external_live_snapshot(after)
    if not _json_exact_equal(
        _without_capture_times(checked_before), _without_capture_times(checked_after)
    ):
        raise QualificationError("external live listener/health/Git snapshot changed")


def atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    _scan_sensitive_keys(payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    encoded = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    try:
        temporary.write_text(encoded, encoding="utf-8")
        os.replace(temporary, path)
    except OSError as exc:
        raise QualificationError("cannot write qualification evidence") from exc


def _read_evidence(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise QualificationError("qualification evidence is missing or invalid") from exc
    if (
        not isinstance(value, dict)
        or not _is_exact_int(
            value.get("schema_version"), expected=EVIDENCE_SCHEMA_VERSION
        )
        or value.get("qualification") != QUALIFICATION_ID
    ):
        raise QualificationError("qualification evidence schema is unsupported")
    _scan_sensitive_keys(value)
    return value


def _inputs(config: PrepareConfig) -> dict[str, Any]:
    context_key = build_context_key(config.git_address, config.run_id)
    return {
        "repo_root": str(config.repo_root),
        "expected_sha": config.expected_sha,
        "ref": config.ref,
        "manifest_path": config.manifest_path,
        "git_address": config.git_address,
        "expected_child_python": str(config.expected_child_python),
        "expected_runtime_root": str(config.expected_runtime_root),
        "expected_managed_root": str(config.expected_managed_root),
        "ownership_marker": str(config.ownership_marker),
        "canonical_repository": canonical_repository_key(config.git_address),
        "run_id": config.run_id,
        "git_context_key": context_key,
        "idempotency_key": f"staging-qualification:{config.run_id}:{config.expected_sha}",
        "staging_url": config.staging_url,
        "managed_db": str(config.managed_db),
        "evidence_path": str(config.evidence_path),
        "protected_roots": [str(path) for path in config.protected_roots],
        "live_baseline_path": str(config.live_baseline_path),
    }


def _config_from_evidence(evidence: Mapping[str, Any], timeout_seconds: float) -> PrepareConfig:
    inputs = evidence.get("inputs")
    if not isinstance(inputs, Mapping):
        raise QualificationError("evidence inputs are missing")
    try:
        config = PrepareConfig(
            repo_root=Path(str(inputs["repo_root"])),
            expected_sha=str(inputs["expected_sha"]),
            ref=str(inputs["ref"]),
            manifest_path=str(inputs["manifest_path"]),
            git_address=str(inputs["git_address"]),
            expected_child_python=Path(str(inputs["expected_child_python"])),
            expected_runtime_root=Path(str(inputs["expected_runtime_root"])),
            expected_managed_root=Path(str(inputs["expected_managed_root"])),
            ownership_marker=Path(str(inputs["ownership_marker"])),
            run_id=str(inputs["run_id"]),
            staging_url=str(inputs["staging_url"]),
            managed_db=Path(str(inputs["managed_db"])),
            evidence_path=Path(str(inputs["evidence_path"])),
            protected_roots=tuple(Path(str(item)) for item in inputs["protected_roots"]),
            timeout_seconds=timeout_seconds,
            live_baseline_path=Path(str(inputs["live_baseline_path"])),
        )
    except (KeyError, TypeError) as exc:
        raise QualificationError("evidence inputs are incomplete") from exc
    return validate_config(config)


def _start_request(inputs: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "repository_id": "main",
        "ref": inputs["ref"],
        "manifest_path": inputs["manifest_path"],
        "idempotency_key": inputs["idempotency_key"],
    }


def _request_intent(path: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "method": "POST",
        "path": path,
        "payload": deepcopy(dict(payload)),
    }


def _require_exact_request_intent(
    evidence: Mapping[str, Any],
    key: str,
    expected: Mapping[str, Any],
) -> dict[str, Any]:
    actual = evidence.get(key)
    if not isinstance(actual, Mapping) or not _json_exact_equal(actual, expected):
        raise QualificationError(f"saved {key} does not match the exact request")
    return deepcopy(dict(actual))


_PREPARE_BASE_KEYS = {
    "schema_version",
    "qualification",
    "status",
    "created_at",
    "updated_at",
    "inputs",
    "git",
    "ownership_marker",
    "pre_restart_host",
    "external_live_baseline",
    "checks",
}
_PREPARE_STAGE_KEYS = {
    "initialized": set(),
    "host_observed": {"pre_restart_listener_pid"},
    "project_intent_recorded": {
        "pre_restart_listener_pid",
        "project_request_intent",
    },
    "project_provisioned": {
        "pre_restart_listener_pid",
        "project_request_intent",
        "project",
    },
    "start_intent_recorded": {
        "pre_restart_listener_pid",
        "project_request_intent",
        "project",
        "start_request_intent",
    },
    "start_accepted": {
        "pre_restart_listener_pid",
        "project_request_intent",
        "project",
        "start_request_intent",
        "start_http_status",
        "start_response",
    },
}


def _validate_prepare_checkpoint_shape(evidence: Mapping[str, Any]) -> str:
    status = evidence.get("status")
    stage_keys = _PREPARE_STAGE_KEYS.get(status)
    if stage_keys is None:
        raise QualificationError("prepare checkpoint status is not resumable")
    if set(evidence) != _PREPARE_BASE_KEYS | stage_keys:
        raise QualificationError(f"{status} prepare checkpoint shape is not exact")
    return str(status)


def _write_prepare_checkpoint(path: Path, evidence: Mapping[str, Any]) -> None:
    _validate_prepare_checkpoint_shape(evidence)
    atomic_write_json(path, evidence)


def _record_exact_proof(evidence: dict[str, Any], key: str, value: Any) -> None:
    if key in evidence:
        if not _json_exact_equal(evidence[key], value):
            raise QualificationError(f"saved {key} differs from the accepted proof")
        return
    evidence[key] = deepcopy(value)


def _assert_project_response(
    status: int, payload: Mapping[str, Any], inputs: Mapping[str, Any]
) -> str:
    project = payload.get("project")
    phone = payload.get("project_phone")
    if (
        not _is_exact_int(status, expected=200)
        or not isinstance(project, Mapping)
        or project.get("git_context_key") != inputs["git_context_key"]
        or not isinstance(phone, str)
        or re.fullmatch(r"9[0-9]{3}", phone) is None
        or project.get("project_phone") != phone
        or project.get("project_id") != phone
        or not isinstance(payload.get("created"), bool)
    ):
        raise QualificationError("project manager returned a different project context")
    return phone


def _project_from_checkpoint(
    evidence: Mapping[str, Any], inputs: Mapping[str, Any]
) -> str:
    project = evidence.get("project")
    if (
        not isinstance(project, Mapping)
        or set(project)
        != {"http_status", "project_phone", "git_context_key", "created"}
        or not _is_exact_int(project.get("http_status"), expected=200)
        or project.get("git_context_key") != inputs["git_context_key"]
        or not isinstance(project.get("project_phone"), str)
        or re.fullmatch(r"9[0-9]{3}", str(project.get("project_phone"))) is None
        or not isinstance(project.get("created"), bool)
    ):
        raise QualificationError("saved project proof is missing or inconsistent")
    return str(project["project_phone"])


def _assert_start_response(
    status: int,
    payload: Mapping[str, Any],
    *,
    config: PrepareConfig,
    project_id: str,
    allow_deduplicated: bool,
    expected_manifest_sha256: str,
) -> None:
    deduplicated = payload.get("deduplicated")
    if not isinstance(deduplicated, bool):
        raise QualificationError("start response lacks an exact deduplication flag")
    expected_status = 200 if deduplicated else 201
    if not _is_exact_int(status, expected=expected_status):
        raise QualificationError("start response HTTP/deduplication status mismatch")
    if deduplicated and not allow_deduplicated:
        raise QualificationError("fresh unique project unexpectedly deduplicated start")
    identity = payload.get("identity")
    assignments = payload.get("initial_assignment_ids")
    if (
        payload.get("status") != "active"
        or payload.get("phase") != "ACTIVATE"
        or payload.get("execution_mode") != "parallel"
        or not isinstance(identity, Mapping)
        or identity.get("project_id") != project_id
        or identity.get("repository_id") != "main"
        or identity.get("commit") != config.expected_sha
        or identity.get("manifest_path") != config.manifest_path
        or identity.get("manifest_sha256") != expected_manifest_sha256
        or payload.get("workspace_source_commit") != config.expected_sha
        or not isinstance(assignments, list)
        or len(assignments) != EXPECTED_CHILD_COUNT
        or len(set(assignments)) != EXPECTED_CHILD_COUNT
    ):
        raise QualificationError("start response is not pinned to expected four-child source")


def _start_from_checkpoint(
    evidence: Mapping[str, Any],
    *,
    config: PrepareConfig,
    project_id: str,
    expected_manifest_sha256: str,
) -> dict[str, Any]:
    status = evidence.get("start_http_status")
    payload = evidence.get("start_response")
    if not _is_exact_int(status) or not isinstance(payload, Mapping):
        raise QualificationError("saved start proof is missing or inconsistent")
    _assert_start_response(
        status,
        payload,
        config=config,
        project_id=project_id,
        allow_deduplicated=True,
        expected_manifest_sha256=expected_manifest_sha256,
    )
    return deepcopy(dict(payload))


def wait_for_healthy(
    reader: ManagedStateReader,
    *,
    project_id: str,
    start_response: Mapping[str, Any],
    config: PrepareConfig,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    deadline = time.monotonic() + config.timeout_seconds
    last_error = "managed state not present"
    while time.monotonic() < deadline:
        bundle = reader.read(project_id, str(start_response["sprint_id"]))
        if bundle is not None:
            state = bundle.get("state")
            if isinstance(state, Mapping):
                processes = state.get("processes")
                if isinstance(processes, list) and any(
                    isinstance(item, Mapping)
                    and item.get("state") in {"FAILED", "STOPPED"}
                    for item in processes
                ):
                    raise QualificationError("managed child became terminal before proof")
            try:
                summary = validate_runtime_snapshot(
                    bundle,
                    expected_sha=config.expected_sha,
                    expected_ref=config.ref,
                    project_id=project_id,
                    start_response=start_response,
                    expected_child_python=config.expected_child_python,
                    expected_runtime_root=config.expected_runtime_root,
                    expected_managed_root=config.expected_managed_root,
                    expected_service_root=config.repo_root,
                    expected_protected_roots=config.protected_roots,
                    expected_canonical_remote=canonical_repository_key(
                        config.git_address
                    ),
                )
                health = verify_child_health(summary)
                os_ownership = verify_child_os_ownership(summary, health)
                confirmation = reader.read(project_id, str(start_response["sprint_id"]))
                if confirmation is None or not _json_exact_equal(confirmation, bundle):
                    last_error = "managed state changed during health/OS ownership proof"
                    time.sleep(0.5)
                    continue
                return summary, health, os_ownership
            except QualificationError as exc:
                last_error = str(exc)
        time.sleep(0.5)
    raise QualificationError(f"timed out waiting for four healthy children: {last_error}")


def _wait_for_changed_listener(port: int, before: int, timeout: float) -> int:
    deadline = time.monotonic() + timeout
    last_error = "listener unavailable"
    while time.monotonic() < deadline:
        try:
            current = staging_listener_pid(port)
            if current != before:
                return current
            last_error = "listener PID is still the pre-restart owner"
        except QualificationError as exc:
            last_error = str(exc)
        time.sleep(0.5)
    raise QualificationError(f"staging host restart was not observed: {last_error}")


def _require_same_staging_owner(
    config: PrepareConfig,
    marker_proof: Mapping[str, Any],
    host_proof: Mapping[str, Any],
) -> None:
    if validate_ownership_marker(config) != marker_proof:
        raise QualificationError("staging ownership marker changed before HTTP POST")
    if validate_host_listener_ownership(config, marker_proof) != host_proof:
        raise QualificationError("staging listener ownership changed before HTTP POST")


def _staging_client(config: PrepareConfig) -> LoopbackJsonClient:
    return LoopbackJsonClient(
        config.staging_url,
        timeout=config.timeout_seconds,
    )


def prepare(config: PrepareConfig) -> dict[str, Any]:
    config = validate_config(config)
    git_proof = validate_git_source(config)
    if str(config.repo_root) not in sys.path:
        sys.path.insert(0, str(config.repo_root))
    marker_proof = validate_ownership_marker(config)
    host_proof = validate_host_listener_ownership(config, marker_proof)
    inputs = _inputs(config)
    new_evidence = not config.evidence_path.exists()
    if new_evidence:
        live_baseline = load_external_snapshot(config.live_baseline_path)
        require_fresh_external_snapshot(live_baseline)
        evidence: dict[str, Any] = {
            "schema_version": EVIDENCE_SCHEMA_VERSION,
            "qualification": QUALIFICATION_ID,
            "status": "initialized",
            "created_at": utc_now(),
            "updated_at": utc_now(),
            "inputs": inputs,
            "git": git_proof,
            "ownership_marker": marker_proof,
            "pre_restart_host": host_proof,
            "checks": [
                "local HEAD/ref/origin advertisement equal expected SHA",
                "all HTTP targets constrained to 127.0.0.1",
                "managed SQLite opened read-only",
                "protected live roots excluded from all output paths",
            ],
        }
        evidence["external_live_baseline"] = live_baseline
        _write_prepare_checkpoint(config.evidence_path, evidence)
    else:
        evidence = _read_evidence(config.evidence_path)
        _validate_prepare_checkpoint_shape(evidence)
        if not _json_exact_equal(evidence.get("inputs"), inputs):
            raise QualificationError("existing evidence belongs to different inputs")
        if (
            evidence.get("git") != git_proof
            or evidence.get("ownership_marker") != marker_proof
            or evidence.get("pre_restart_host") != host_proof
        ):
            raise QualificationError("source or staging host ownership changed during prepare")
    _, staging_port = validate_loopback_url(config.staging_url)
    listener_before = evidence.get("pre_restart_listener_pid")
    if listener_before is None:
        if evidence.get("status") != "initialized":
            raise QualificationError("pre-restart listener proof is missing")
        listener_before = int(host_proof["pid"])
        evidence["pre_restart_listener_pid"] = listener_before
        evidence["status"] = "host_observed"
        evidence["updated_at"] = utc_now()
        _write_prepare_checkpoint(config.evidence_path, evidence)
    elif (
        not _is_exact_int(listener_before, minimum=1)
        or listener_before != host_proof["pid"]
    ):
        raise QualificationError("pre-restart listener evidence differs from ownership proof")
    elif evidence.get("status") == "initialized":
        evidence["status"] = "host_observed"
        evidence["updated_at"] = utc_now()
        _write_prepare_checkpoint(config.evidence_path, evidence)

    client: LoopbackJsonClient | None = None

    def issue_saved_intent(intent: Mapping[str, Any]) -> tuple[int, dict[str, Any]]:
        nonlocal client
        if client is None:
            client = _staging_client(config)
        return client.request(
            str(intent["method"]),
            str(intent["path"]),
            deepcopy(intent["payload"]),
        )

    project_body = {
        "git_address": config.git_address,
        "git_context_key": inputs["git_context_key"],
        "project_name": f"UMSE staging qualification {config.run_id}",
    }
    expected_project_intent = _request_intent(
        "/project-manager/0001",
        project_body,
    )
    prepare_status = evidence.get("status")
    project_intent_is_new = False
    if prepare_status == "host_observed":
        if any(
            key in evidence
            for key in (
                "project_request_intent",
                "project",
                "start_request_intent",
                "start_http_status",
                "start_response",
            )
        ):
            raise QualificationError("host checkpoint contains later request proof")
        evidence["project_request_intent"] = expected_project_intent
        evidence["status"] = "project_intent_recorded"
        evidence["updated_at"] = utc_now()
        _write_prepare_checkpoint(config.evidence_path, evidence)
        project_intent_is_new = True
    elif prepare_status not in {
        "project_intent_recorded",
        "project_provisioned",
        "start_intent_recorded",
        "start_accepted",
    }:
        raise QualificationError("prepare project checkpoint is invalid")
    project_intent = _require_exact_request_intent(
        evidence,
        "project_request_intent",
        expected_project_intent,
    )
    prepare_status = evidence.get("status")
    if prepare_status == "project_intent_recorded":
        if any(
            key in evidence
            for key in (
                "project",
                "start_request_intent",
                "start_http_status",
                "start_response",
            )
        ):
            raise QualificationError("project intent checkpoint contains later proof")
        _require_same_staging_owner(config, marker_proof, host_proof)
        project_status, project_response = issue_saved_intent(project_intent)
        project_id = _assert_project_response(
            project_status,
            project_response,
            inputs,
        )
        if project_intent_is_new and project_response["created"] is not True:
            raise QualificationError("unique project context already exists")
        _record_exact_proof(evidence, "project", {
            "http_status": project_status,
            "project_phone": project_id,
            "git_context_key": inputs["git_context_key"],
            "created": project_response["created"],
        })
        evidence["status"] = "project_provisioned"
        evidence["updated_at"] = utc_now()
        _write_prepare_checkpoint(config.evidence_path, evidence)
    else:
        project_id = _project_from_checkpoint(evidence, inputs)

    start_path = (
        f"/api/v1/projects/{urllib.parse.quote(project_id, safe='')}"
        "/sprints/start-from-git"
    )
    expected_start_intent = _request_intent(
        start_path,
        _start_request(inputs),
    )
    prepare_status = evidence.get("status")
    start_intent_is_new = False
    if prepare_status == "project_provisioned":
        if any(
            key in evidence
            for key in ("start_request_intent", "start_http_status", "start_response")
        ):
            raise QualificationError("project checkpoint contains later start proof")
        evidence["start_request_intent"] = expected_start_intent
        evidence["status"] = "start_intent_recorded"
        evidence["updated_at"] = utc_now()
        _write_prepare_checkpoint(config.evidence_path, evidence)
        start_intent_is_new = True
    elif prepare_status not in {"start_intent_recorded", "start_accepted"}:
        raise QualificationError("prepare start checkpoint is invalid")
    start_intent = _require_exact_request_intent(
        evidence,
        "start_request_intent",
        expected_start_intent,
    )
    prepare_status = evidence.get("status")
    if prepare_status == "start_intent_recorded":
        if "start_http_status" in evidence or "start_response" in evidence:
            raise QualificationError("start intent checkpoint contains accepted proof")
        _require_same_staging_owner(config, marker_proof, host_proof)
        start_status, start_response = issue_saved_intent(start_intent)
        _assert_start_response(
            start_status,
            start_response,
            config=config,
            project_id=project_id,
            allow_deduplicated=not start_intent_is_new,
            expected_manifest_sha256=str(git_proof["manifest_sha256"]),
        )
        _record_exact_proof(evidence, "start_http_status", start_status)
        _record_exact_proof(evidence, "start_response", start_response)
        evidence["status"] = "start_accepted"
        evidence["updated_at"] = utc_now()
        _write_prepare_checkpoint(config.evidence_path, evidence)
    else:
        start_response = _start_from_checkpoint(
            evidence,
            config=config,
            project_id=project_id,
            expected_manifest_sha256=str(git_proof["manifest_sha256"]),
        )
    reader = ManagedStateReader(config.managed_db)
    summary, health, child_os = wait_for_healthy(
        reader,
        project_id=project_id,
        start_response=start_response,
        config=config,
    )
    if listener_before in {
        item["leader_pid"] for item in health
    } | {item["worker_pid"] for item in health}:
        raise QualificationError("staging host listener aliases a child process")
    checkpoint_host = validate_host_listener_ownership(config, marker_proof)
    listener_checkpoint = int(checkpoint_host["pid"])
    if listener_checkpoint != listener_before or checkpoint_host != host_proof:
        raise QualificationError("staging host changed during prepare")
    evidence["pre_restart"] = {
        "observed_at": utc_now(),
        "listener_pid": listener_checkpoint,
        "host_ownership": checkpoint_host,
        "runtime": _evidence_summary(summary),
        "child_health": health,
        "child_os_ownership": child_os,
    }
    evidence["status"] = "awaiting_external_restart"
    evidence["restart_protocol"] = {
        "performed_by_runner": False,
        "required_action": (
            "Restart only the staging host with the normal staging launcher, "
            "without stopping child processes; then run verify-after-restart."
        ),
        "pre_restart_listener_pid": listener_before,
    }
    evidence["updated_at"] = utc_now()
    atomic_write_json(config.evidence_path, evidence)
    return evidence


def verify_after_restart(
    evidence_path: Path,
    *,
    timeout_seconds: float,
) -> dict[str, Any]:
    evidence_path = _resolved(evidence_path)
    evidence = _read_evidence(evidence_path)
    if evidence.get("status") != "awaiting_external_restart":
        raise QualificationError("evidence is not awaiting an external restart")
    config = _config_from_evidence(evidence, timeout_seconds)
    if config.evidence_path != evidence_path:
        raise QualificationError("evidence self-path does not match invocation")
    if str(config.repo_root) not in sys.path:
        sys.path.insert(0, str(config.repo_root))
    if evidence.get("git") != validate_git_source(config):
        raise QualificationError("Git source proof changed after prepare")
    marker_proof = validate_ownership_marker(config)
    if evidence.get("ownership_marker") != marker_proof:
        raise QualificationError("staging ownership marker changed after prepare")
    listener_before = evidence.get("pre_restart_listener_pid")
    if not isinstance(listener_before, int) or listener_before <= 0:
        raise QualificationError("pre-restart listener proof is missing")
    _, staging_port = validate_loopback_url(config.staging_url)
    listener_after = _wait_for_changed_listener(
        staging_port, listener_before, config.timeout_seconds
    )
    host_after = validate_host_listener_ownership(config, marker_proof)
    if int(host_after["pid"]) != listener_after:
        raise QualificationError("post-restart listener ownership PID differs")
    host_before = evidence.get("pre_restart_host")
    if not isinstance(host_before, Mapping):
        raise QualificationError("pre-restart host ownership proof is missing")
    for key in ("executable_path", "cwd", "venv_ancestry", "command_sha256"):
        if host_after.get(key) != host_before.get(key):
            raise QualificationError(f"post-restart host ownership changed {key}")
    if host_after.get("birth_token_sha256") == host_before.get("birth_token_sha256"):
        raise QualificationError("post-restart host birth identity did not change")
    project = evidence.get("project")
    start_response = evidence.get("start_response")
    pre_restart = evidence.get("pre_restart")
    if (
        not isinstance(project, Mapping)
        or not isinstance(start_response, Mapping)
        or not isinstance(pre_restart, Mapping)
        or not isinstance(pre_restart.get("runtime"), Mapping)
    ):
        raise QualificationError("pre-restart evidence is incomplete")
    project_id = str(project.get("project_phone") or "")
    reader = ManagedStateReader(config.managed_db)
    summary, health, child_os = wait_for_healthy(
        reader,
        project_id=project_id,
        start_response=start_response,
        config=config,
    )
    after_summary = _evidence_summary(summary)
    validate_adoption(
        pre_restart["runtime"],
        after_summary,
        host_pid_before=listener_before,
        host_pid_after=listener_after,
    )
    before_health = pre_restart.get("child_health")
    if before_health != health:
        raise QualificationError("child health identities changed across host restart")
    if pre_restart.get("child_os_ownership") != child_os:
        raise QualificationError("child OS/Job ownership changed across host restart")
    inputs = evidence["inputs"]
    client = _staging_client(config)
    start_path = f"/api/v1/projects/{urllib.parse.quote(project_id, safe='')}/sprints/start-from-git"
    _require_same_staging_owner(config, marker_proof, host_after)
    replay_status, replay = client.request("POST", start_path, _start_request(inputs))
    _assert_start_response(
        replay_status,
        replay,
        config=config,
        project_id=project_id,
        allow_deduplicated=True,
        expected_manifest_sha256=str(evidence["git"]["manifest_sha256"]),
    )
    if replay_status != 200 or replay.get("deduplicated") is not True:
        raise QualificationError("start replay was not idempotently deduplicated")
    for key in (
        "sprint_id",
        "identity",
        "workspace_source_commit",
        "initial_assignment_ids",
        "execution_mode",
    ):
        if replay.get(key) != start_response.get(key):
            raise QualificationError(f"idempotent replay changed {key}")
    final_summary, final_health, final_child_os = wait_for_healthy(
        reader,
        project_id=project_id,
        start_response=start_response,
        config=config,
    )
    final_evidence_summary = _evidence_summary(final_summary)
    validate_adoption(
        pre_restart["runtime"],
        final_evidence_summary,
        host_pid_before=listener_before,
        host_pid_after=listener_after,
    )
    if health != final_health:
        raise QualificationError("idempotent replay changed child health identities")
    if child_os != final_child_os:
        raise QualificationError("idempotent replay changed child OS/Job ownership")
    final_host = validate_host_listener_ownership(config, marker_proof)
    if final_host != host_after:
        raise QualificationError("staging host changed during post-restart verification")
    evidence["post_restart"] = {
        "observed_at": utc_now(),
        "listener_pid": listener_after,
        "host_ownership": final_host,
        "runtime": final_evidence_summary,
        "child_health": final_health,
        "child_os_ownership": final_child_os,
    }
    evidence["replay_response"] = deepcopy(replay)
    evidence["restart_protocol"]["post_restart_listener_pid"] = listener_after
    evidence["restart_protocol"]["listener_pid_changed"] = True
    evidence["checks"].extend(
        [
            "four HEALTHY children retained exact PID/process/workspace/runtime/port identity",
            "durable process and lease owner rows remain one-to-one",
            "staging host listener PID changed through external restart",
            "same source-bound start request replayed with HTTP 200 deduplicated=true",
        ]
    )
    evidence["status"] = "verified_awaiting_cleanup"
    evidence["updated_at"] = utc_now()
    atomic_write_json(evidence_path, evidence)
    return evidence


def _cleanup_resources(
    evidence: Mapping[str, Any],
) -> tuple[str, str, dict[str, Mapping[str, Any]], dict[str, Mapping[str, Any]]]:
    project = evidence.get("project")
    start_response = evidence.get("start_response")
    post_restart = evidence.get("post_restart")
    if (
        not isinstance(project, Mapping)
        or not isinstance(start_response, Mapping)
        or not isinstance(post_restart, Mapping)
        or not isinstance(post_restart.get("runtime"), Mapping)
    ):
        raise QualificationError("verified evidence lacks cleanup identity")
    project_id = str(project.get("project_phone") or "")
    sprint_id = str(start_response.get("sprint_id") or "")
    runtime = post_restart["runtime"]
    processes = runtime.get("processes")
    leases = runtime.get("leases")
    if not isinstance(processes, list) or not isinstance(leases, list):
        raise QualificationError("verified evidence lacks cleanup resources")
    process_by_id = {
        str(item.get("process_id")): item
        for item in processes
        if isinstance(item, Mapping) and item.get("process_id")
    }
    lease_by_id = {
        str(item.get("lease_id")): item
        for item in leases
        if isinstance(item, Mapping) and item.get("lease_id")
    }
    if (
        not project_id
        or not sprint_id
        or len(process_by_id) != EXPECTED_CHILD_COUNT
        or len(lease_by_id) != EXPECTED_CHILD_COUNT
    ):
        raise QualificationError("cleanup identity is not exactly four owned children")
    if {
        str(item.get("process_id")) for item in lease_by_id.values()
    } != set(process_by_id):
        raise QualificationError("cleanup process/lease identity is not one-to-one")
    return project_id, sprint_id, process_by_id, lease_by_id


def _validate_cleanup_runtime_config(
    config: PrepareConfig, runtime_config: Mapping[str, Any]
) -> dict[str, Any]:
    expected_paths = {
        "service_root": config.repo_root,
        "runtime_root": config.expected_runtime_root,
        "process_runtime_root": config.expected_runtime_root / "processes",
        "log_root": config.expected_runtime_root / "logs",
        "pid_root": config.expected_runtime_root / "pids",
        "lease_root": config.expected_runtime_root / "leases",
        "prompt_root": config.ownership_marker.parent / "prompt",
        "managed_root": config.expected_managed_root,
    }
    for name, expected in expected_paths.items():
        if _path_key(runtime_config.get(name)) != _path_key(str(expected)):
            raise QualificationError(f"cleanup runtime config changed {name}")
    expected_database = (
        config.expected_runtime_root / "leases" / "managed-import.sqlite3"
    )
    if _path_key(str(config.managed_db)) != _path_key(str(expected_database)):
        raise QualificationError("cleanup database is outside the exact staging root")
    if (
        runtime_config.get("http_host") != "127.0.0.1"
        or not _is_exact_int(runtime_config.get("http_port"), expected=STAGING_HTTP_PORT)
        or not _is_exact_int(
            runtime_config.get("child_port_start"), expected=CHILD_PORT_START
        )
        or not _is_exact_int(
            runtime_config.get("child_port_end"), expected=CHILD_PORT_END
        )
        or runtime_config.get("instance_id") != STAGING_INSTANCE_ID
        or runtime_config.get("disable_telegram") is not True
        or runtime_config.get("disable_tunnel") is not True
    ):
        raise QualificationError("cleanup runtime boundary is not exact staging")
    actual_protected = [
        _path_key(str(item)) for item in runtime_config.get("protected_roots", [])
    ]
    expected_protected = [_path_key(str(item)) for item in config.protected_roots]
    if actual_protected != expected_protected:
        raise QualificationError("cleanup protected roots changed")
    return deepcopy(dict(runtime_config))


def _reprove_cleanup_checkpoint(
    config: PrepareConfig,
    evidence: Mapping[str, Any],
    bundle: Mapping[str, Any],
    *,
    project_id: str,
) -> dict[str, Any]:
    """Re-authenticate the exact verified children before any cleanup mutation."""

    start_response = evidence.get("start_response")
    post_restart = evidence.get("post_restart")
    if (
        not isinstance(start_response, Mapping)
        or not isinstance(post_restart, Mapping)
        or not isinstance(post_restart.get("runtime"), Mapping)
        or not isinstance(post_restart.get("child_health"), list)
        or not isinstance(post_restart.get("child_os_ownership"), list)
    ):
        raise QualificationError("verified evidence lacks the cleanup checkpoint proof")

    summary = validate_runtime_snapshot(
        bundle,
        expected_sha=config.expected_sha,
        expected_ref=config.ref,
        project_id=project_id,
        start_response=start_response,
        expected_child_python=config.expected_child_python,
        expected_runtime_root=config.expected_runtime_root,
        expected_managed_root=config.expected_managed_root,
        expected_service_root=config.repo_root,
        expected_protected_roots=config.protected_roots,
        expected_canonical_remote=canonical_repository_key(config.git_address),
    )
    runtime_proof = _evidence_summary(summary)
    if not _json_exact_equal(runtime_proof, post_restart["runtime"]):
        raise QualificationError(
            "managed runtime changed after the verified post-restart checkpoint"
        )

    health = verify_child_health(summary)
    if not _json_exact_equal(health, post_restart["child_health"]):
        raise QualificationError(
            "child health identity changed after the verified post-restart checkpoint"
        )

    child_os = verify_child_os_ownership(summary, health)
    if not _json_exact_equal(child_os, post_restart["child_os_ownership"]):
        raise QualificationError(
            "child OS/Job ownership changed after the verified post-restart checkpoint"
        )

    state = bundle.get("state")
    runtime_config = state.get("runtime_config") if isinstance(state, Mapping) else None
    if not isinstance(runtime_config, Mapping):
        raise QualificationError("cleanup runtime config is missing")
    return {
        "runtime_config": _validate_cleanup_runtime_config(config, runtime_config),
        "runtime": deepcopy(runtime_proof),
        "child_health": deepcopy(health),
        "child_os_ownership": deepcopy(child_os),
    }


def _record_pre_cleanup_checkpoint(
    evidence_path: Path,
    evidence: dict[str, Any],
    proof: Mapping[str, Any],
) -> dict[str, Any]:
    """Durably record the exact authenticated state before writable cleanup."""

    required_proof = {
        "runtime_config",
        "runtime",
        "child_health",
        "child_os_ownership",
    }
    if set(proof) != required_proof:
        raise QualificationError("cleanup checkpoint proof shape is invalid")
    checkpoint_values = {
        "staging_host_listener": {
            "port": STAGING_HTTP_PORT,
            "owners": {},
        },
        **{key: deepcopy(proof[key]) for key in sorted(required_proof)},
    }
    existing = evidence.get("pre_cleanup")
    if existing is not None:
        if (
            not isinstance(existing, Mapping)
            or set(existing) != {"observed_at", *checkpoint_values}
            or not isinstance(existing.get("observed_at"), str)
            or not existing["observed_at"].strip()
            or any(
                not _json_exact_equal(existing.get(key), value)
                for key, value in checkpoint_values.items()
            )
        ):
            raise QualificationError("stored pre-cleanup checkpoint differs")
        return deepcopy(dict(existing))

    observed_at = utc_now()
    checkpoint = {"observed_at": observed_at, **checkpoint_values}
    evidence["pre_cleanup"] = checkpoint
    evidence["updated_at"] = observed_at
    atomic_write_json(evidence_path, evidence)
    return deepcopy(checkpoint)


def _validate_cleanup_snapshot(
    bundle: Mapping[str, Any],
    *,
    process_ids: set[str],
    lease_ids: set[str],
) -> dict[str, Any]:
    state = bundle.get("state")
    if not isinstance(state, Mapping):
        raise QualificationError("cleanup runtime snapshot is missing")
    processes = _objects_by(state.get("processes"), "process_id", "cleanup processes")
    leases = _objects_by(state.get("port_leases"), "lease_id", "cleanup leases")
    owners = _owner_rows_by(
        bundle.get("owner_processes"), "process_id", "cleanup owner processes"
    )
    owner_leases = _owner_rows_by(
        bundle.get("owner_leases"), "lease_id", "cleanup owner leases"
    )
    if (
        set(processes) != process_ids
        or set(owners) != process_ids
        or set(leases) != lease_ids
        or set(owner_leases) != lease_ids
    ):
        raise QualificationError("cleanup durable resource set changed")
    for key, process in processes.items():
        owner = owners[key]
        if (
            process.get("state") != "STOPPED"
            or not _json_exact_equal(owner["json"], process)
            or any(
                not _scalar_exact_equal(
                    owner["relational"].get(column), process.get(column)
                )
                for column in (
                    "process_id",
                    "assignment_id",
                    "port_lease_id",
                    "pid",
                    "state",
                )
            )
        ):
            raise QualificationError(
                "cleanup did not terminalize every process with relational parity"
            )
    for key, lease in leases.items():
        owner = owner_leases[key]
        if (
            lease.get("status") != "released"
            or not _json_exact_equal(owner["json"], lease)
            or any(
                not _scalar_exact_equal(
                    owner["relational"].get(column), lease.get(column)
                )
                for column in (
                    "lease_id",
                    "instance_id",
                    "network_namespace_id",
                    "assignment_id",
                    "process_id",
                    "host",
                    "port",
                    "status",
                )
            )
        ):
            raise QualificationError(
                "cleanup did not release every lease with relational parity"
            )
    if bundle.get("live_assignment_processes") or bundle.get("live_assignment_leases"):
        raise QualificationError("cleanup left live process or lease ownership")
    return {
        "processes": [
            {"process_id": key, "state": "STOPPED"} for key in sorted(process_ids)
        ],
        "leases": [
            {"lease_id": key, "status": "released"} for key in sorted(lease_ids)
        ],
    }


def cleanup(evidence_path: Path) -> dict[str, Any]:
    evidence_path = _resolved(evidence_path)
    evidence = _read_evidence(evidence_path)
    if evidence.get("status") != "verified_awaiting_cleanup":
        raise QualificationError("evidence is not awaiting authenticated cleanup")
    config = _config_from_evidence(evidence, 120.0)
    if config.evidence_path != evidence_path:
        raise QualificationError("evidence self-path does not match invocation")
    if evidence.get("git") != validate_git_source(config):
        raise QualificationError("Git source proof changed before cleanup")
    if evidence.get("ownership_marker") != validate_ownership_marker(config):
        raise QualificationError("staging ownership marker changed before cleanup")
    project_id, sprint_id, processes, leases = _cleanup_resources(evidence)

    if _windows_listener_snapshot().get(STAGING_HTTP_PORT):
        raise QualificationError("stop the staging host before authenticated cleanup")
    reader = ManagedStateReader(config.managed_db)
    before = reader.read(project_id, sprint_id)
    if before is None:
        raise QualificationError("cleanup cannot load the managed sprint")
    pre_cleanup_proof = _reprove_cleanup_checkpoint(
        config,
        evidence,
        before,
        project_id=project_id,
    )

    # Health and Windows Job reproof can be slow.  Re-read the durable state and
    # recheck the staging host listener immediately before exposing writable
    # cleanup objects.  Any drift fails with no store/supervisor construction.
    after_reproof = reader.read(project_id, sprint_id)
    if after_reproof is None or not _json_exact_equal(after_reproof, before):
        raise QualificationError("managed runtime changed during cleanup reproof")
    if _windows_listener_snapshot().get(STAGING_HTTP_PORT):
        raise QualificationError("staging host listener reappeared before cleanup")
    _record_pre_cleanup_checkpoint(evidence_path, evidence, pre_cleanup_proof)
    exact_runtime_config = pre_cleanup_proof["runtime_config"]

    if str(config.repo_root) not in sys.path:
        sys.path.insert(0, str(config.repo_root))
    from nginx_qa.managed_import import (  # pylint: disable=import-outside-toplevel
        ManagedImportStore,
        ManagedPortReservationRegistry,
    )
    from nginx_qa.process_supervisor import (  # pylint: disable=import-outside-toplevel
        ManagedProcessSupervisor,
        ManagedProcessSupervisorError,
    )

    store = ManagedImportStore(config.managed_db)
    reservations = ManagedPortReservationRegistry()
    supervisor = ManagedProcessSupervisor(
        exact_runtime_config,
        store,
        reservations,
        health_timeout_seconds=10.0,
        stop_timeout_seconds=10.0,
        poll_interval_seconds=0.1,
    )
    results: list[dict[str, Any]] = []
    try:
        for process_id in sorted(processes):
            try:
                result = supervisor.stop(project_id, sprint_id, process_id)
            except ManagedProcessSupervisorError as exc:
                raise QualificationError(
                    f"authenticated cleanup failed for {process_id}: {exc.code}"
                ) from exc
            results.append(
                {
                    "process_id": result.process_id,
                    "state": result.state,
                    "action": result.action,
                }
            )
    finally:
        supervisor.close()
        reservations.close_all()

    after = reader.read(project_id, sprint_id)
    if after is None:
        raise QualificationError("cleanup runtime snapshot disappeared")
    cleanup_summary = _validate_cleanup_snapshot(
        after,
        process_ids=set(processes),
        lease_ids=set(leases),
    )
    owned_ports = {int(item.get("port")) for item in leases.values()}
    listeners_after_cleanup = _windows_listener_snapshot()
    if any(
        listeners_after_cleanup.get(port)
        for port in range(CHILD_PORT_START, CHILD_PORT_END + 1)
    ):
        raise QualificationError("cleanup left a listener in the staging child range")
    completed_at = utc_now()
    evidence["cleanup"] = {
        "completed_at": completed_at,
        "results": results,
        "durable_state": cleanup_summary,
        "released_ports": sorted(owned_ports),
        "staging_host_stopped": True,
    }
    evidence["checks"].extend(
        [
            "authenticated cleanup stopped all four Windows Jobs",
            "all durable processes are STOPPED and leases released",
            "staging host and managed child ports have no listeners",
        ]
    )
    evidence["status"] = "cleaned_awaiting_live_after"
    evidence["updated_at"] = completed_at
    atomic_write_json(evidence_path, evidence)
    return evidence


def validate_live_after_path(config: PrepareConfig, live_after_path: Path) -> Path:
    resolved_live_after = _resolved(live_after_path)
    evidence_root = (config.ownership_marker.parent / "evidence").resolve(strict=False)
    if (
        resolved_live_after.parent.resolve(strict=False) != evidence_root
        or resolved_live_after.suffix.casefold() != ".json"
        or not _is_safe_path_component(resolved_live_after.name)
    ):
        raise QualificationError(
            "live after-snapshot must be a JSON file directly inside the owned evidence root"
        )
    for label, protected_file in (
        ("evidence", config.evidence_path),
        ("managed database", config.managed_db),
        ("expected child Python", config.expected_child_python),
        ("live baseline", config.live_baseline_path),
    ):
        if os.path.normcase(str(resolved_live_after)) == os.path.normcase(
            str(protected_file)
        ):
            raise QualificationError(f"live after-snapshot must not replace {label}")
    if any(
        _paths_overlap(resolved_live_after, root) for root in config.protected_roots
    ):
        raise QualificationError(
            "external live snapshot file must be outside protected live roots"
        )
    return resolved_live_after


def finalize(evidence_path: Path, *, live_after_path: Path) -> dict[str, Any]:
    evidence_path = _resolved(evidence_path)
    evidence = _read_evidence(evidence_path)
    if evidence.get("status") != "cleaned_awaiting_live_after":
        raise QualificationError("evidence is not awaiting the final live snapshot")
    config = _config_from_evidence(evidence, 120.0)
    if config.evidence_path != evidence_path:
        raise QualificationError("evidence self-path does not match invocation")
    if evidence.get("git") != validate_git_source(config):
        raise QualificationError("Git source proof changed before finalization")
    if evidence.get("ownership_marker") != validate_ownership_marker(config):
        raise QualificationError("staging ownership marker changed before finalization")
    baseline = evidence.get("external_live_baseline")
    cleanup_proof = evidence.get("cleanup")
    if not isinstance(baseline, Mapping) or not isinstance(cleanup_proof, Mapping):
        raise QualificationError("finalization proof is incomplete")
    completed_at = cleanup_proof.get("completed_at")
    if not isinstance(completed_at, str):
        raise QualificationError("cleanup completion time is missing")
    resolved_live_after = validate_live_after_path(config, live_after_path)
    live_after = load_external_snapshot(resolved_live_after)
    require_fresh_external_snapshot(live_after, not_before=completed_at)
    compare_external_live_snapshots(baseline, live_after)

    final_listeners = _windows_listener_snapshot()
    if any(
        final_listeners.get(port)
        for port in (STAGING_HTTP_PORT, *range(CHILD_PORT_START, CHILD_PORT_END + 1))
    ):
        raise QualificationError("staging listeners reappeared after cleanup")
    evidence["external_live_after"] = live_after
    evidence["checks"].append("external live PID/port/file/Git snapshot unchanged")
    evidence["status"] = "passed"
    evidence["updated_at"] = utc_now()
    atomic_write_json(evidence_path, evidence)
    return evidence


def _prepare_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    prepare_parser = subparsers.add_parser("prepare")
    prepare_parser.add_argument("--repo-root", required=True, type=Path)
    prepare_parser.add_argument("--expected-sha", required=True)
    prepare_parser.add_argument("--ref", required=True)
    prepare_parser.add_argument("--manifest-path", required=True)
    prepare_parser.add_argument("--git-address", required=True)
    prepare_parser.add_argument(
        "--expected-child-python",
        type=Path,
        default=os.environ.get("NGINX_QA_STAGING_CHILD_PYTHON"),
        help=(
            "absolute staging interpreter path; may also be supplied through "
            "NGINX_QA_STAGING_CHILD_PYTHON"
        ),
    )
    prepare_parser.add_argument("--expected-runtime-root", required=True, type=Path)
    prepare_parser.add_argument("--expected-managed-root", required=True, type=Path)
    prepare_parser.add_argument("--ownership-marker", required=True, type=Path)
    prepare_parser.add_argument("--run-id", required=True)
    prepare_parser.add_argument("--staging-url", default=DEFAULT_STAGING_URL)
    prepare_parser.add_argument("--managed-db", required=True, type=Path)
    prepare_parser.add_argument("--evidence", required=True, type=Path)
    prepare_parser.add_argument(
        "--protected-root", required=True, action="append", type=Path
    )
    prepare_parser.add_argument("--timeout-seconds", type=float, default=120.0)
    prepare_parser.add_argument("--live-baseline-json", required=True, type=Path)
    verify_parser = subparsers.add_parser("verify-after-restart")
    verify_parser.add_argument("--evidence", required=True, type=Path)
    verify_parser.add_argument("--timeout-seconds", type=float, default=120.0)
    cleanup_parser = subparsers.add_parser("cleanup")
    cleanup_parser.add_argument("--evidence", required=True, type=Path)
    finalize_parser = subparsers.add_parser("finalize")
    finalize_parser.add_argument("--evidence", required=True, type=Path)
    finalize_parser.add_argument("--live-after-json", required=True, type=Path)
    return parser


def main(arguments: Sequence[str] | None = None) -> int:
    parsed = _prepare_parser().parse_args(arguments)
    try:
        if parsed.command == "prepare":
            result = prepare(
                PrepareConfig(
                    repo_root=parsed.repo_root,
                    expected_sha=parsed.expected_sha,
                    ref=parsed.ref,
                    manifest_path=parsed.manifest_path,
                    git_address=parsed.git_address,
                    expected_child_python=(
                        parsed.expected_child_python
                        if parsed.expected_child_python is not None
                        else Path("")
                    ),
                    expected_runtime_root=parsed.expected_runtime_root,
                    expected_managed_root=parsed.expected_managed_root,
                    ownership_marker=parsed.ownership_marker,
                    run_id=parsed.run_id,
                    staging_url=parsed.staging_url,
                    managed_db=parsed.managed_db,
                    evidence_path=parsed.evidence,
                    protected_roots=tuple(parsed.protected_root),
                    timeout_seconds=parsed.timeout_seconds,
                    live_baseline_path=parsed.live_baseline_json,
                )
            )
            print(
                json.dumps(
                    {
                        "status": result["status"],
                        "evidence": str(parsed.evidence.resolve(strict=False)),
                        "next": "restart staging externally, then run verify-after-restart",
                    },
                    sort_keys=True,
                )
            )
        elif parsed.command == "verify-after-restart":
            result = verify_after_restart(
                parsed.evidence,
                timeout_seconds=parsed.timeout_seconds,
            )
            print(
                json.dumps(
                    {
                        "status": result["status"],
                        "evidence": str(parsed.evidence.resolve(strict=False)),
                        "next": "stop the staging host, then run cleanup",
                    },
                    sort_keys=True,
                )
            )
        elif parsed.command == "cleanup":
            result = cleanup(parsed.evidence)
            print(
                json.dumps(
                    {
                        "status": result["status"],
                        "evidence": str(parsed.evidence.resolve(strict=False)),
                        "next": "capture live after-snapshot, then run finalize",
                    },
                    sort_keys=True,
                )
            )
        else:
            result = finalize(
                parsed.evidence,
                live_after_path=parsed.live_after_json,
            )
            print(
                json.dumps(
                    {
                        "status": result["status"],
                        "evidence": str(parsed.evidence.resolve(strict=False)),
                    },
                    sort_keys=True,
                )
            )
        return 0
    except QualificationError as exc:
        print(f"staging qualification failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
