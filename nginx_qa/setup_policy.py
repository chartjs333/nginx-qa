"""Private operator grants, read afresh for every wizard request.

The file is server configuration, never supplied by a browser or the advisor.
An authenticated session identifies an actor; it does not itself grant a project.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Any

from .setup_diagnostics import ACTION_IDS, canonical_json_bytes, parse_strict_json_with_limits


@dataclass(frozen=True)
class SetupGrant:
    actor_id: str
    project_id: str
    actions: frozenset[str]
    generation: int
    version: str

    def require(self, action: str) -> None:
        if action not in self.actions:
            raise PermissionError("SETUP_ACTION_FORBIDDEN")


class SetupPolicy:
    def __init__(self, path: Path | None):
        self.path = path

    def read(self) -> dict[str, Any]:
        if self.path is None or not self.path.is_file():
            return {"schema_version": 1, "grants": [], "selftest_profiles": {}}
        if self.path.is_symlink() or (hasattr(self.path, "is_junction") and self.path.is_junction()):
            raise ValueError("SETUP_POLICY_INVALID")
        raw = self.path.read_bytes()
        if len(raw) > 262144:
            raise ValueError("SETUP_POLICY_INVALID")
        data = parse_strict_json_with_limits(raw)
        if not isinstance(data, dict) or data.get("schema_version") != 1 or not isinstance(data.get("grants"), list):
            raise ValueError("SETUP_POLICY_INVALID")
        for grant in data["grants"]:
            if (not isinstance(grant, dict) or set(grant) - {"actor_id", "role", "projects", "actions"}
                    or ("actor_id" in grant) == ("role" in grant)
                    or ("role" in grant and grant["role"] != "paired_operator")
                    or not isinstance(grant.get("projects"), list)
                    or not isinstance(grant.get("actions"), list)
                    or set(grant["actions"]) - (set(ACTION_IDS) | {"read", "advise"})):
                raise ValueError("SETUP_POLICY_INVALID")
        return data

    def grant(self, actor_id: str, project_id: str, *, data=None) -> SetupGrant:
        data = self.read() if data is None else data
        version = hashlib.sha256(canonical_json_bytes(data)).hexdigest()
        actions: set[str] = set()
        for row in data["grants"]:
            matches = row.get("actor_id") == actor_id or (
                row.get("role") == "paired_operator" and actor_id.startswith("operator-"))
            if matches and project_id in row["projects"]:
                actions.update(row["actions"])
        result = SetupGrant(actor_id, project_id, frozenset(actions), int(version[:12], 16), version)
        result.require("read")
        return result

    def profile(self, profile_id: str, *, data=None) -> dict[str, Any]:
        profiles = (self.read() if data is None else data).get("selftest_profiles", {})
        profile = profiles.get(profile_id) if isinstance(profiles, dict) else None
        if not isinstance(profile, dict):
            raise ValueError("SELFTEST_PROFILE_NOT_CONFIGURED")
        # Immutable published manifest; the engine validates the same bytes from Git.
        required = {"target_project_id", "repository_id", "ref", "commit", "manifest_path", "manifest"}
        if set(profile) != required or not isinstance(profile["manifest"], dict):
            raise ValueError("SELFTEST_PROFILE_INVALID")
        import re
        if (not re.fullmatch(r"[0-9a-f]{40}", str(profile["commit"]))
                or not re.fullmatch(r"refs/(heads|tags)/[A-Za-z0-9_./-]+", str(profile["ref"]))):
            raise ValueError("SELFTEST_PROFILE_INVALID")
        return json.loads(json.dumps(profile))
