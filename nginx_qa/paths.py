"""Configured directory defaults and native filesystem boundary checks.

Lexical identities remain owned by the existing sprint_types path validators.
The existing managed_import normalizer owns the logical-directory/env mapping.
"""

from __future__ import annotations

import configparser
import os
import stat
from collections.abc import Mapping
from pathlib import Path

from .sprint_types import absolute_path_key, windows_absolute_path_key


def native_path(value: str | os.PathLike[str]) -> Path:
    """Validate host syntax without probing a potentially protected filesystem."""
    raw = os.fspath(value)
    key = absolute_path_key(raw)
    if key is None or (os.name == "nt") == key.startswith("/"):
        raise ValueError("path must be canonical and absolute for this host")
    return Path(raw)


def resolve_native_path(
    value: str | os.PathLike[str], *, must_exist: bool = False
) -> Path:
    """Reject relative/foreign syntax and every symlink, junction or reparse ancestor.

    Missing tails are permitted for callers which create their own directories.
    This read-only check does not replace mutation-time ownership/locking checks.
    """
    path = native_path(value)
    key = absolute_path_key(str(path))
    for component in reversed((path, *path.parents)):
        try:
            metadata = component.lstat()
        except FileNotFoundError:
            continue
        if (
            stat.S_ISLNK(metadata.st_mode)
            or getattr(metadata, "st_file_attributes", 0) & 0x400
        ):
            raise ValueError("path contains a symlink, junction or reparse point")
        if component != path and not stat.S_ISDIR(metadata.st_mode):
            raise NotADirectoryError(str(component))
    resolved = path.resolve(strict=must_exist)
    if absolute_path_key(str(resolved)) != key:
        raise ValueError("path contains a filesystem alias")
    return resolved


def normalize_configured_path(value: str) -> str:
    """Validate foreign snapshots lexically; check native paths against the host."""
    key = absolute_path_key(value)
    if key is None:
        raise ValueError("managed path must be a canonical absolute DOS or POSIX path")
    native = (os.name == "nt") != key.startswith("/")
    if native:
        return resolve_native_path(value).as_posix()
    return value.replace("\\", "/")


def protected_root_values(raw: str) -> list[str]:
    """Read JSON (portable), or the legacy host path-list/Windows ';' form."""
    import json

    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError:
        decoded = None
        if raw.lstrip().startswith(("[", "{")):
            raise ValueError("protected roots contain malformed JSON") from None
    if decoded is None:
        if ";" in raw:
            decoded = raw.split(";")
        elif windows_absolute_path_key(raw) is not None:
            decoded = [raw]
        else:
            decoded = raw.split(os.pathsep)
    if (
        not isinstance(decoded, list)
        or not decoded
        or any(not isinstance(value, str) or not value.strip() for value in decoded)
    ):
        raise ValueError("protected roots must be a non-empty string list")
    return decoded


def default_prompt_root(*, platform: str | None = None) -> str:
    """Platform defaults live in one shipped properties file, never in consumers."""
    config = configparser.ConfigParser(interpolation=None)
    with (
        Path(__file__)
        .with_name("path-defaults.properties")
        .open(encoding="utf-8") as source
    ):
        config.read_file(source)
    return config["windows" if (platform or os.name) == "nt" else "posix"][
        "prompt_root"
    ]


def configured_prompt_root(environment: Mapping[str, str] | None = None) -> Path:
    source = os.environ if environment is None else environment
    value = source.get("NGINX_QA_SEQUENTIAL_PROMPT_ROOT", default_prompt_root())
    # Expansion applies only to this legacy convenience default, never managed roots.
    return native_path(os.path.expanduser(os.path.expandvars(value)))


def sequential_prompt_templates() -> tuple[str, str]:
    root = configured_prompt_root()
    return str(root / "{repository}"), str(
        root / "{repository}_{agent_phone}-latest.prompt"
    )
