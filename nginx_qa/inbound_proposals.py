"""Pure helpers for the versioned inbound pending-proposal API.

The HTTP adapter and durable pending-sprints file remain owned by ``main``.
This module deliberately contains no source adapters and no activation calls.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import os
import re
import stat
import subprocess
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping
from uuid import uuid4

from nginx_qa.managed_import import managed_schema_errors, parse_strict_json_object
from nginx_qa.sprint_types import canonical_json_bytes


INBOUND_PRODUCER_REGISTRY_ENV = "NGINX_QA_INBOUND_PRODUCER_REGISTRY"
CREATE_SCHEMA = "inbound-pending-proposal-create-v1.schema.json"
ACTION_SCHEMA = "inbound-pending-proposal-action-v1.schema.json"

CORRELATION_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
BEARER_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_-]{43,}\Z")
_SECRET_LITERAL = re.compile(
    r"(?:"
    r"\b(?:Bearer|Basic)\s+\S+|"
    r"-----BEGIN(?: [A-Z0-9]+)? PRIVATE KEY-----|"
    r"\b(?:sk-(?:proj-)?|ghp_|github_pat_|glpat-)[A-Za-z0-9_-]{8,}|"
    r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}|"
    r"[A-Za-z][A-Za-z0-9+.-]*://[^\s/@]+@"
    r")",
    re.IGNORECASE,
)
_URL_LITERAL = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://[^\s<>\"']+")
_CREDENTIAL_PARAMETER_NAME = re.compile(
    r"(?:^|[-_.])(?:authorization|access[-_]?token|api[-_]?key|apikey|"
    r"bearer(?:[-_]?token)?|credentials?|password|passwd|secret|token|"
    r"pending[-_]?token|producer[-_]?token)(?:$|[-_.])",
    re.IGNORECASE,
)
_CREDENTIAL_ENV_NAME = re.compile(
    r"(?:^|_)(?:AUTH|COOKIE|CREDENTIALS?|PASSWORD|PASSWD|PRIVATE_KEY|"
    r"SECRETS?|TOKENS?|API_KEY)(?:_|$)",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class ProducerPrincipal:
    producer_id: str
    token_sha256: str
    project_ids: frozenset[str]
    actions: frozenset[str]


class InboundProposalError(Exception):
    """Stable, value-free error carried from storage/auth to the HTTP edge."""

    def __init__(
        self,
        code: str,
        http_status: int,
        correlation_id: str,
        message: str,
        **details: Any,
    ) -> None:
        super().__init__(code)
        self.code = code
        self.http_status = http_status
        self.correlation_id = correlation_id
        self.message = message
        self.details = details

    @property
    def envelope(self) -> dict[str, Any]:
        detail: dict[str, Any] = {
            "error": self.code,
            "message": self.message,
            "correlation_id": self.correlation_id,
            "retryable": self.code == "PROPOSAL_SERVICE_UNAVAILABLE",
        }
        detail.update(self.details)
        return {"detail": detail}


def new_correlation_id() -> str:
    return f"corr-{uuid4().hex}"


def correlation_id_from_values(values: Iterable[str]) -> tuple[str, bool]:
    supplied = list(values)
    if not supplied:
        return new_correlation_id(), True
    if len(supplied) != 1 or CORRELATION_ID_PATTERN.fullmatch(supplied[0]) is None:
        return new_correlation_id(), False
    return supplied[0], True


def schema_issues(payload: Any, filename: str) -> list[dict[str, str]]:
    raw = managed_schema_errors(
        payload,
        filename,
        issue_code="PROPOSAL_SCHEMA_INVALID",
    )
    issues: list[dict[str, str]] = []
    for issue in raw:
        normalized = {
            "code": "PROPOSAL_SCHEMA_INVALID",
            "message": "Value does not satisfy the declared proposal schema",
        }
        field = str(issue.get("path") or "").strip()
        if field:
            normalized["field"] = field
        issues.append(normalized)
    return issues


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def create_request_fingerprint(
    canonical_project_id: str,
    payload: Mapping[str, Any],
) -> str:
    return canonical_sha256(
        [
            payload.get("schema_version"),
            canonical_project_id,
            payload.get("proposal_id"),
            payload.get("source"),
            payload.get("summary"),
            payload.get("candidate"),
        ]
    )


def action_request_fingerprint(
    canonical_project_id: str,
    pending_sprint_id: str,
    payload: Mapping[str, Any],
) -> str:
    return canonical_sha256(
        [
            payload.get("schema_version"),
            canonical_project_id,
            pending_sprint_id,
            payload.get("action"),
            payload.get("expected_revision"),
            payload.get("comment"),
        ]
    )


def configured_secret_values() -> tuple[str, ...]:
    values: set[str] = set()
    for name, raw_value in os.environ.items():
        if not _CREDENTIAL_ENV_NAME.search(name) or not raw_value:
            continue
        if len(raw_value) >= 4:
            values.add(raw_value)
        try:
            decoded = json.loads(raw_value)
        except (TypeError, json.JSONDecodeError):
            continue
        stack = [decoded]
        while stack:
            candidate = stack.pop()
            if isinstance(candidate, str) and len(candidate) >= 4:
                values.add(candidate)
            elif isinstance(candidate, Mapping):
                stack.extend(candidate.values())
            elif isinstance(candidate, list):
                stack.extend(candidate)
    return tuple(values)


def contains_recognized_secret(
    value: Any,
    *,
    configured_values: Iterable[str] = (),
    recognize_shapes: bool = True,
) -> bool:
    protected_values = tuple(item for item in configured_values if item)
    stack = [value]
    while stack:
        candidate = stack.pop()
        if isinstance(candidate, str):
            if recognize_shapes and (
                _SECRET_LITERAL.search(candidate)
                or _contains_credential_url(candidate)
            ):
                return True
            if any(secret in candidate for secret in protected_values):
                return True
        if isinstance(candidate, Mapping):
            stack.extend(candidate.keys())
            stack.extend(candidate.values())
        elif isinstance(candidate, (list, tuple)):
            stack.extend(candidate)
    return False


def _contains_credential_url(value: str) -> bool:
    for raw_url in _URL_LITERAL.findall(value):
        try:
            parsed = urllib.parse.urlsplit(raw_url.rstrip(".,);]"))
        except ValueError:
            continue
        components = [parsed.query]
        if parsed.fragment:
            components.append(parsed.fragment.partition("?")[2] or parsed.fragment)
        for component in components:
            for name, _ in urllib.parse.parse_qsl(
                component,
                keep_blank_values=True,
                strict_parsing=False,
            ):
                if _CREDENTIAL_PARAMETER_NAME.search(name):
                    return True
    return False


def is_canonical_bearer_token(token: str) -> bool:
    if BEARER_TOKEN_PATTERN.fullmatch(token) is None:
        return False
    try:
        encoded = token.encode("ascii", errors="strict")
        decoded = base64.b64decode(
            encoded + b"=" * (-len(encoded) % 4),
            altchars=b"-_",
            validate=True,
        )
    except (UnicodeEncodeError, binascii.Error, ValueError):
        return False
    if len(decoded) < 32:
        return False
    canonical = base64.urlsafe_b64encode(decoded).rstrip(b"=")
    return hmac.compare_digest(canonical, encoded)


def _windows_registry_acl_is_protected(acl: Any) -> bool:
    if not isinstance(acl, Mapping):
        return False
    current_sid = str(acl.get("current_sid") or "")
    raw_items = acl.get("items")
    items = raw_items if isinstance(raw_items, list) else [raw_items]
    if not current_sid or not items or not all(
        isinstance(item, Mapping) for item in items
    ):
        return False
    system_sid = "S-1-5-18"
    administrators_sid = "S-1-5-32-544"
    trusted_installer_sid = (
        "S-1-5-80-956008885-3418522649-1831038044-"
        "1853292631-2271478464"
    )
    file_allowed = {current_sid, system_sid}
    ancestor_allowed = {
        current_sid,
        system_sid,
        administrators_sid,
        trusted_installer_sid,
    }
    file_write_mask = 2 | 4 | 16 | 256 | 65536 | 262144 | 524288
    replacement_mask = 64 | 65536 | 262144 | 524288
    for index, item in enumerate(items):
        owner = str(item.get("owner") or "")
        allowed = file_allowed if index == 0 else ancestor_allowed
        if owner not in allowed:
            return False
        raw_access = item.get("access")
        access = raw_access if isinstance(raw_access, list) else [raw_access]
        permission_mask = file_write_mask if index == 0 else replacement_mask
        for rule in access:
            if not isinstance(rule, Mapping):
                return False
            if str(rule.get("type") or "").casefold() != "allow":
                continue
            if "inheritonly" in str(rule.get("propagation") or "").casefold():
                continue
            try:
                rights = int(rule.get("rights") or 0)
            except (TypeError, ValueError):
                return False
            sid = str(rule.get("sid") or "")
            if rights & permission_mask and sid not in allowed:
                return False
    return True


def _registry_acl_is_protected(path: Path) -> bool:
    if os.name != "nt":
        try:
            current_uid = os.geteuid()
            cursor = path
            while True:
                metadata = cursor.stat()
                if metadata.st_uid not in {0, current_uid}:
                    return False
                writable_by_others = bool(
                    metadata.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
                )
                sticky_directory = bool(
                    cursor.is_dir() and metadata.st_mode & stat.S_ISVTX
                )
                if writable_by_others and not sticky_directory:
                    return False
                if cursor.parent == cursor:
                    break
                cursor = cursor.parent
            return True
        except OSError:
            return False

    script = r"""
$ErrorActionPreference = 'Stop'
$currentSid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$cursor = Get-Item -LiteralPath $args[0] -Force
$items = @()
while ($null -ne $cursor) {
  $acl = Get-Acl -LiteralPath $cursor.FullName
  $access = @($acl.Access | ForEach-Object {
    [pscustomobject]@{
      sid = $_.IdentityReference.Translate([System.Security.Principal.SecurityIdentifier]).Value
      rights = [int64]$_.FileSystemRights
      type = $_.AccessControlType.ToString()
      propagation = $_.PropagationFlags.ToString()
    }
  })
  $items += [pscustomobject]@{
    path = $cursor.FullName
    is_container = [bool]$cursor.PSIsContainer
    owner = $acl.GetOwner([System.Security.Principal.SecurityIdentifier]).Value
    access = $access
  }
  $cursor = $cursor.Parent
}
[pscustomobject]@{current_sid = $currentSid; items = $items} |
  ConvertTo-Json -Depth 6 -Compress
"""
    creation_flags = int(getattr(subprocess, "CREATE_NO_WINDOW", 0))
    try:
        completed = subprocess.run(
            [
                "powershell.exe",
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                script,
                str(path),
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
            creationflags=creation_flags,
        )
        acl = json.loads(completed.stdout)
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError, TypeError):
        return False
    return _windows_registry_acl_is_protected(acl)


def _assert_registry_path_safe(raw_path: str) -> Path:
    path = Path(raw_path)
    if not path.is_absolute():
        raise RuntimeError("Inbound producer registry path must be absolute")
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise RuntimeError("Inbound producer registry is unavailable") from exc
    if not resolved.is_file():
        raise RuntimeError("Inbound producer registry is not a file")

    current = Path(path.anchor)
    for component in path.parts[1:]:
        current = current / component
        try:
            metadata = current.lstat()
        except OSError as exc:
            raise RuntimeError("Inbound producer registry is unavailable") from exc
        attributes = int(getattr(metadata, "st_file_attributes", 0))
        reparse_flag = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0))
        if current.is_symlink() or (reparse_flag and attributes & reparse_flag):
            raise RuntimeError("Inbound producer registry path is indirect")
    return resolved


def load_producer_registry(
    raw_path: str,
    *,
    known_project_ids: set[str] | None = None,
    require_protected_acl: bool = True,
) -> tuple[ProducerPrincipal, ...]:
    path = _assert_registry_path_safe(raw_path)
    if require_protected_acl and not _registry_acl_is_protected(path):
        raise RuntimeError("Inbound producer registry ACL is unsafe")
    try:
        blob = path.read_bytes()
    except OSError as exc:
        raise RuntimeError("Inbound producer registry is unavailable") from exc
    if len(blob) > 1024 * 1024:
        raise RuntimeError("Inbound producer registry is too large")
    try:
        payload = parse_strict_json_object(blob)
    except ValueError as exc:
        raise RuntimeError("Inbound producer registry JSON is invalid") from exc
    if schema_issues(payload, "inbound-producer-registry-v1.schema.json"):
        raise RuntimeError("Inbound producer registry schema is invalid")

    principals: list[ProducerPrincipal] = []
    seen_ids: set[str] = set()
    seen_digests: set[str] = set()
    for raw in payload["producers"]:
        producer_id = str(raw["producer_id"])
        digest = str(raw["token_sha256"])
        projects = frozenset(str(item) for item in raw["project_ids"])
        if producer_id in seen_ids or digest in seen_digests:
            raise RuntimeError("Inbound producer registry contains duplicates")
        if known_project_ids is not None and not projects.issubset(known_project_ids):
            raise RuntimeError("Inbound producer registry contains an unknown project")
        seen_ids.add(producer_id)
        seen_digests.add(digest)
        principals.append(
            ProducerPrincipal(
                producer_id=producer_id,
                token_sha256=digest,
                project_ids=projects,
                actions=frozenset(str(item) for item in raw["actions"]),
            )
        )
    return tuple(principals)


def configured_producer_registry(
    *,
    known_project_ids: set[str] | None = None,
) -> tuple[ProducerPrincipal, ...]:
    raw_path = str(os.environ.get(INBOUND_PRODUCER_REGISTRY_ENV) or "").strip()
    if not raw_path:
        raise RuntimeError("Inbound producer registry is not configured")
    return load_producer_registry(
        raw_path,
        known_project_ids=known_project_ids,
        require_protected_acl=True,
    )


def authenticate_bearer(
    authorization_values: Iterable[str],
    principals: Iterable[ProducerPrincipal],
) -> ProducerPrincipal | None:
    values = list(authorization_values)
    if len(values) != 1:
        return None
    raw = values[0]
    scheme, separator, token = raw.partition(" ")
    if separator != " " or scheme.casefold() != "bearer" or not token:
        return None
    if token.strip() != token or not is_canonical_bearer_token(token):
        return None
    try:
        encoded = token.encode("ascii", errors="strict")
    except UnicodeEncodeError:
        return None
    digest = hashlib.sha256(encoded).hexdigest()
    matched: ProducerPrincipal | None = None
    for principal in principals:
        # Do not stop early; comparison work is independent of registry order.
        if hmac.compare_digest(principal.token_sha256, digest):
            matched = principal
    return matched


__all__ = [
    "ACTION_SCHEMA",
    "CREATE_SCHEMA",
    "INBOUND_PRODUCER_REGISTRY_ENV",
    "InboundProposalError",
    "ProducerPrincipal",
    "action_request_fingerprint",
    "authenticate_bearer",
    "configured_producer_registry",
    "configured_secret_values",
    "contains_recognized_secret",
    "correlation_id_from_values",
    "create_request_fingerprint",
    "is_canonical_bearer_token",
    "load_producer_registry",
    "new_correlation_id",
    "schema_issues",
]
