"""Bounded source admission, independent of model-generated instructions."""
from __future__ import annotations
import re

PROFILES = {
    "docs-only-v1": {"paths": 32, "file": 1024**2, "tree": 32*1024**2,
                     "bundle": 32*1024**2, "objects": 8192, "history": 32*1024**2, "files": 4096},
    "java-maven-v1": {"paths": 128, "file": 8*1024**2, "tree": 128*1024**2,
                      "bundle": 64*1024**2, "objects": 32768, "history": 256*1024**2, "files": 8192},
}

def execution_path(value, profile_id="docs-only-v1"):
    if profile_id not in PROFILES:
        raise ValueError("CLIENT_PROJECT_PROFILE_FORBIDDEN")
    if (not isinstance(value, str) or len(value) > 240
            or not re.fullmatch(r"[A-Za-z0-9_./-]+", value)
            or any(p in ("", ".", "..") or p.startswith(".") for p in value.split("/"))):
        raise ValueError("WSL_DOCUMENT_PATH_INVALID")
    doc = value.startswith("docs/") and value.endswith(".md")
    code = (value == "pom.xml" or
            re.fullmatch(r"src/(main|test)/java/[A-Za-z0-9_./-]+\.java", value) or
            re.fullmatch(r"src/main/resources/templates/[A-Za-z0-9_./-]+\.html", value) or
            re.fullmatch(r"src/main/resources/static/[A-Za-z0-9_./-]+\.(js|css)", value) or
            re.fullmatch(r"src/test/resources/[A-Za-z0-9_./-]+\.(json|xml|txt)", value))
    if not doc and not (profile_id == "java-maven-v1" and code):
        raise ValueError("WSL_DOCUMENT_PATH_INVALID")
    return value

def execution_paths(paths, profile_id="docs-only-v1"):
    limits = PROFILES.get(profile_id)
    if limits is None:
        raise ValueError("CLIENT_PROJECT_PROFILE_FORBIDDEN")
    if (not isinstance(paths, list) or not 1 <= len(paths) <= limits["paths"]
            or any(not isinstance(p, str) for p in paths) or len(set(paths)) != len(paths)):
        raise ValueError("WSL_DOCUMENT_ALLOWLIST_REQUIRED")
    for path in paths:
        execution_path(path, profile_id)
    return paths
