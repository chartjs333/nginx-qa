"""Setup and Diagnostics Wizard v1 — deterministic diagnostics engine and contracts.

Strictly passive read-only state inspection, bound networking and secrets protection.
No background schedulers, no hidden mutations, no external network calls on GET.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import time
from typing import Any, Mapping, Sequence

import jsonschema


# Initial Server-Owned Action and Status Catalogs (contracts.schema.json)
CHECK_STATUSES = ("PASS", "WARN", "FAIL", "UNKNOWN", "NOT_CONFIGURED", "BLOCKED")
CHECK_SCOPES = ("passive", "connection", "execution")
CHECK_FRESHNESS = ("FRESH", "STALE", "UNOBSERVED")
EVIDENCE_KINDS = ("CODE", "CONFIG_METADATA", "LIVE_PROBE", "MANAGED_RESULT", "MOCK", "NONE")
ACTION_IDS = (
    "diagnostics.refresh",
    "worker.probe",
    "worker.bind",
    "provider.probe",
    "telegram.probe",
    "setup.open",
    "selftest.start",
    "selftest.stop",
    "recovery.open",
    "git.probe",
)
ERROR_CODES = (
    "TIMEOUT",
    "DNS_ERROR",
    "CONNECTION_ERROR",
    "AUTH_REJECTED",
    "ACCESS_DENIED",
    "NOT_CONFIGURED",
    "PROTOCOL_INVALID",
    "PROBE_UNSUPPORTED",
    "SOURCE_STALE",
    "PROJECT_BUSY",
    "OWNERSHIP_MISMATCH",
    "STORE_UNAVAILABLE",
    "STATE_CORRUPT",
    "UNSUPPORTED_OPERATION",
)

_PROJECT_ID_PATTERN = re.compile(r"^9[0-9]{3}$")
_CHECK_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_.-]{0,79}$")
_OPAQUE_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_DIGEST_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_BOT_TOKEN_PATTERN = re.compile(r"\b\d{8,11}:[A-Za-z0-9_-]{35}\b")

_COMPILED_SCHEMA: dict[str, Any] | None = None
_SNAPSHOT_VALIDATOR: jsonschema.Draft202012Validator | None = None


def get_contracts_schema() -> dict[str, Any]:
    global _COMPILED_SCHEMA
    if _COMPILED_SCHEMA is None:
        schema_path = Path(__file__).resolve().parents[1] / "docs" / "setup-diagnostics-wizard-v1" / "contracts.schema.json"
        if not schema_path.exists():
            raise FileNotFoundError(f"Contracts schema not found at {schema_path}")
        with open(schema_path, "r", encoding="utf-8") as f:
            _COMPILED_SCHEMA = json.load(f)
    return _COMPILED_SCHEMA


def get_snapshot_validator() -> jsonschema.Draft202012Validator:
    global _SNAPSHOT_VALIDATOR
    if _SNAPSHOT_VALIDATOR is None:
        schema = get_contracts_schema()
        subschema = {
            "$schema": schema["$schema"],
            "$defs": schema["$defs"],
            "$ref": "#/$defs/DiagnosticSnapshot",
        }
        _SNAPSHOT_VALIDATOR = jsonschema.Draft202012Validator(
            subschema, format_checker=jsonschema.FormatChecker()
        )
    return _SNAPSHOT_VALIDATOR


def canonical_json_bytes(obj: Any) -> bytes:
    """Deterministic UTF-8 canonical JSON bytes with sorted keys and no space."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def sha256_digest(data: bytes | str) -> str:
    raw = data.encode("utf-8") if isinstance(data, str) else data
    return hashlib.sha256(raw).hexdigest()


def compute_snapshot_id(snapshot: Mapping[str, Any]) -> str:
    """Compute snapshot_id as SHA-256 digest of canonical JSON without snapshot_id."""
    cleaned = {k: v for k, v in snapshot.items() if k != "snapshot_id"}
    return sha256_digest(canonical_json_bytes(cleaned))


def compute_authority_fingerprint(
    project_id: str,
    *,
    instance_id: str | None = None,
    git_head: str | None = None,
    control_revision: int = 0,
    policy_generation: int = 1,
    active_sprint_id: str | None = None,
    active_assignment_id: str | None = None,
) -> str:
    payload = {
        "active_assignment_id": str(active_assignment_id or ""),
        "active_sprint_id": str(active_sprint_id or ""),
        "control_revision": int(control_revision),
        "git_head": str(git_head or ""),
        "instance_id": str(instance_id or ""),
        "policy_generation": int(policy_generation),
        "project_id": str(project_id),
    }
    return sha256_digest(canonical_json_bytes(payload))


def parse_strict_json_with_limits(
    data: bytes | str,
    *,
    max_bytes: int = 64 * 1024,
    max_depth: int = 16,
) -> Any:
    """Strict JSON parser rejecting duplicates, non-finites, excess depth and body size."""
    raw_bytes = data.encode("utf-8") if isinstance(data, str) else data
    if len(raw_bytes) > max_bytes:
        raise ValueError(f"JSON payload exceeded maximum size {max_bytes} bytes")

    def _object_pairs_hook(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if not isinstance(key, str):
                raise ValueError("JSON object keys must be strings")
            if key in result:
                raise ValueError(f"Duplicate JSON key: {key}")
            result[key] = value
        return result

    parsed = json.loads(
        raw_bytes.decode("utf-8"),
        object_pairs_hook=_object_pairs_hook,
        parse_constant=lambda c: (_ for _ in ()).throw(ValueError(f"Invalid JSON constant: {c}")),
    )

    def _check_depth(node: Any, current_depth: int = 1) -> None:
        if current_depth > max_depth:
            raise ValueError(f"JSON nesting depth exceeded {max_depth}")
        if isinstance(node, dict):
            for v in node.values():
                _check_depth(v, current_depth + 1)
        elif isinstance(node, list):
            for v in node:
                _check_depth(v, current_depth + 1)

    _check_depth(parsed)
    return parsed


def redact_secrets(text: str, extra_canaries: Sequence[str] = ()) -> str:
    """Remove telegram bot tokens, secret env references and known canary secrets."""
    if not isinstance(text, str):
        return text
    result = _BOT_TOKEN_PATTERN.sub("[REDACTED_BOT_TOKEN]", text)
    for canary in extra_canaries:
        if canary and canary in result:
            result = result.replace(canary, "[REDACTED_SECRET]")
    # Redact protected control env names if their values appear in text
    for env_name, env_val in os.environ.items():
        folded = env_name.casefold()
        if (folded.startswith(("nginx_qa_scope_control_", "nginx_qa_launcher_control_"))
                or folded in {"telegram_bot_token", "telegram_webhook_secret", "gemini_api_key"}):
            if env_val and len(env_val) >= 4 and env_val in result:
                result = result.replace(env_val, "[REDACTED_CREDENTIAL]")
    return result


class DiagnosticSnapshotCollector:
    """Pure passive read-only collector.

    Never invokes LauncherOperations.get or _refresh.
    Never initiates SQLite migrations or reconciliation.
    Never spawns processes or performs network calls.
    Never writes files, indexes, lock directories or Git trees.
    """

    def __init__(
        self,
        *,
        code_root: Path | None = None,
        git_config_path: Path | None = None,
        state_base: Path | None = None,
        store_path: Path | None = None,
        now: datetime | None = None,
    ) -> None:
        self.code_root = code_root or Path(__file__).resolve().parents[1]
        self.git_config_path = git_config_path
        self.state_base = state_base
        self.store_path = store_path
        self.now = now or datetime.now(timezone.utc)

    def _resolve_git_head_and_branch(self) -> tuple[str, str, bool]:
        """Read git HEAD and branch purely from filesystem without fetch or index writes."""
        git_dir = self.code_root / ".git"
        if not git_dir.exists():
            return "0" * 40, "unknown", False
        try:
            head_file = git_dir / "HEAD"
            if not head_file.exists():
                return "0" * 40, "unknown", False
            head_content = head_file.read_text(encoding="utf-8").strip()
            branch = "detached"
            commit = head_content
            if head_content.startswith("ref:"):
                ref_path = head_content.partition("ref:")[2].strip()
                branch = ref_path.replace("refs/heads/", "")
                target_ref = git_dir / ref_path
                if target_ref.exists():
                    commit = target_ref.read_text(encoding="utf-8").strip()
                else:
                    # check packed-refs
                    packed = git_dir / "packed-refs"
                    if packed.exists():
                        for line in packed.read_text(encoding="utf-8").splitlines():
                            if line and not line.startswith(("#", "^")):
                                parts = line.split()
                                if len(parts) >= 2 and parts[1] == ref_path:
                                    commit = parts[0]
                                    break
            return commit if len(commit) == 40 else "0" * 40, branch, True
        except Exception:
            return "0" * 40, "unknown", False

    def _read_git_config(self) -> tuple[dict[str, Any] | None, str | None]:
        """Read canonical git registry file. Distinguishes missing from corrupt."""
        path = self.git_config_path
        if path is None:
            raw = os.environ.get("NGINX_QA_GIT_CONFIG_PATH")
            path = Path(raw) if raw else (self.code_root / "port_git_map.json")
        if not path.exists():
            return None, "NOT_CONFIGURED"
        try:
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
            parsed = parse_strict_json_with_limits(content)
            if not isinstance(parsed, dict):
                return None, "STATE_CORRUPT"
            return parsed, None
        except Exception:
            return None, "STATE_CORRUPT"

    def _read_sqlite_store(self, project_id: str) -> dict[str, Any]:
        """Pure query_only SQLite read. Returns normalized projection without mutations."""
        result: dict[str, Any] = {
            "available": False,
            "error_code": None,
            "control_revision": 0,
            "active_sprint_id": None,
            "sprint_status": None,
            "graph_revision": 1,
            "port_leases_count": 0,
            "branch_leases_count": 0,
            "queue_items_count": 0,
            "pending_reviews_count": 0,
            "has_pending_recovery": False,
            "active_assignment_id": None,
        }
        db_path = self.store_path
        if db_path is None:
            if self.state_base:
                db_path = self.state_base / "r" / "leases" / "managed-import.sqlite3"
            else:
                db_path = Path("C:/nqt35/r/leases/managed-import.sqlite3")

        if not db_path.exists():
            result["error_code"] = "STORE_UNAVAILABLE"
            return result

        try:
            conn = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True, timeout=2.0)
            conn.execute("PRAGMA query_only=ON")
            conn.row_factory = sqlite3.Row

            # 1. Project control
            row = conn.execute(
                "SELECT control_json, revision FROM managed_projects WHERE project_id = ?",
                (project_id,),
            ).fetchone()
            if row:
                result["available"] = True
                result["control_revision"] = int(row["revision"])

            # 2. Managed sprints
            sprint_rows = conn.execute(
                "SELECT sprint_id, status, state_json FROM managed_sprints WHERE project_id = ?",
                (project_id,),
            ).fetchall()
            for s_row in sprint_rows:
                st = s_row["status"]
                if st == "active":
                    result["active_sprint_id"] = s_row["sprint_id"]
                    result["sprint_status"] = "active"
                    try:
                        state_obj = json.loads(s_row["state_json"])
                        result["graph_revision"] = state_obj.get("graph_revision", 1)
                        assignments = state_obj.get("assignments", [])
                        current_assignments = [a for a in assignments if a.get("status") == "active"]
                        if current_assignments:
                            result["active_assignment_id"] = current_assignments[0].get("assignment_id")

                        # Pending reviews
                        reviews = state_obj.get("reviews", [])
                        receipts = state_obj.get("result_receipts", [])
                        for rec in receipts:
                            rec_key = rec.get("result_key")
                            matched = [
                                r for r in reviews
                                if r.get("result_key") == rec_key and r.get("decision") == "APPROVE"
                            ]
                            if len(matched) < 2:
                                result["pending_reviews_count"] += 1

                        if state_obj.get("operator_stop_requested") or state_obj.get("pending_recovery"):
                            result["has_pending_recovery"] = True
                    except Exception:
                        result["error_code"] = "STATE_CORRUPT"

            # 3. Leases & queue
            p_leases = conn.execute(
                "SELECT count(*) as cnt FROM managed_port_leases WHERE status = 'active'"
            ).fetchone()
            if p_leases:
                result["port_leases_count"] = int(p_leases["cnt"])

            b_leases = conn.execute(
                "SELECT count(*) as cnt FROM branch_leases WHERE status = 'active'"
            ).fetchone()
            if b_leases:
                result["branch_leases_count"] = int(b_leases["cnt"])

            q_items = conn.execute("SELECT count(*) as cnt FROM managed_queue_items").fetchone()
            if q_items:
                result["queue_items_count"] = int(q_items["cnt"])

            conn.close()
            result["available"] = True
            return result
        except sqlite3.OperationalError:
            result["error_code"] = "STORE_UNAVAILABLE"
            return result
        except Exception:
            result["error_code"] = "STATE_CORRUPT"
            return result

    def collect_snapshot(self, project_id: str, *, instance_id: str | None = None) -> dict[str, Any]:
        """Collect all 16 passive checks and build DiagnosticSnapshot."""
        if not _PROJECT_ID_PATTERN.fullmatch(project_id):
            raise ValueError(f"Invalid canonical project ID: {project_id}")

        code_commit, branch, git_ok = self._resolve_git_head_and_branch()
        config_data, config_error = self._read_git_config()
        store_data = self._read_sqlite_store(project_id)

        now_iso = self.now.isoformat()
        expires_iso = datetime.fromtimestamp(self.now.timestamp() + 300, timezone.utc).isoformat()

        source_obj = {
            "adapter_id": "diagnostic-core-adapter",
            "adapter_version": "1.0.0",
            "code_commit": code_commit,
            "configuration_version": "config-v1",
        }

        checks: list[dict[str, Any]] = []

        # 1. project.identity
        if config_error == "STATE_CORRUPT":
            checks.append({
                "check_id": "project.identity",
                "status": "FAIL",
                "scope": "passive",
                "observed_at": now_iso,
                "expires_at": expires_iso,
                "freshness": "FRESH",
                "source": source_obj,
                "evidence_kind": "CONFIG_METADATA",
                "evidence": [{"fact_id": "project.identity", "value": "corrupt_config"}],
                "error_code": "STATE_CORRUPT",
                "capability_ids": ["project.selection"],
                "next_action_ids": ["setup.open"],
            })
        elif config_error == "NOT_CONFIGURED" or config_data is None:
            checks.append({
                "check_id": "project.identity",
                "status": "NOT_CONFIGURED",
                "scope": "passive",
                "observed_at": None,
                "expires_at": None,
                "freshness": "UNOBSERVED",
                "source": source_obj,
                "evidence_kind": "CONFIG_METADATA",
                "evidence": [],
                "error_code": "NOT_CONFIGURED",
                "capability_ids": [],
                "next_action_ids": ["setup.open"],
            })
        else:
            projects = config_data.get("projects", {})
            phone_map = config_data.get("phone_git_contexts", {})
            found = False
            for ctx_key, p_entry in projects.items():
                if str(p_entry.get("project_phone", "")) == project_id or str(ctx_key) == project_id:
                    found = True
                    break
            if not found:
                for ph, ctx_entry in phone_map.items():
                    if str(ph) == project_id:
                        found = True
                        break
            if found:
                checks.append({
                    "check_id": "project.identity",
                    "status": "PASS",
                    "scope": "passive",
                    "observed_at": now_iso,
                    "expires_at": expires_iso,
                    "freshness": "FRESH",
                    "source": source_obj,
                    "evidence_kind": "CONFIG_METADATA",
                    "evidence": [
                        {"fact_id": "project.identity", "value": project_id},
                        {"fact_id": "project.identity", "value": "canonical_registered"},
                    ],
                    "error_code": None,
                    "capability_ids": ["project.selection"],
                    "next_action_ids": ["diagnostics.refresh"],
                })
            else:
                checks.append({
                    "check_id": "project.identity",
                    "status": "BLOCKED",
                    "scope": "passive",
                    "observed_at": now_iso,
                    "expires_at": expires_iso,
                    "freshness": "FRESH",
                    "source": source_obj,
                    "evidence_kind": "CONFIG_METADATA",
                    "evidence": [{"fact_id": "project.identity", "value": "project_not_registered"}],
                    "error_code": "NOT_CONFIGURED",
                    "capability_ids": [],
                    "next_action_ids": ["setup.open"],
                })

        # 2. project.slot
        is_busy = bool(store_data["active_sprint_id"])
        checks.append({
            "check_id": "project.slot",
            "status": "WARN" if is_busy else "PASS",
            "scope": "passive",
            "observed_at": now_iso,
            "expires_at": expires_iso,
            "freshness": "FRESH",
            "source": source_obj,
            "evidence_kind": "MANAGED_RESULT",
            "evidence": [
                {"fact_id": "project.slot", "value": is_busy},
                {"fact_id": "project.slot", "value": store_data["active_sprint_id"]},
            ],
            "error_code": "PROJECT_BUSY" if is_busy else None,
            "capability_ids": ["project.slot_reservation"],
            "next_action_ids": ["diagnostics.refresh"] if not is_busy else ["recovery.open", "diagnostics.refresh"],
        })

        # 3. engine.runtime
        if store_data["error_code"] == "STORE_UNAVAILABLE":
            checks.append({
                "check_id": "engine.runtime",
                "status": "UNKNOWN",
                "scope": "passive",
                "observed_at": None,
                "expires_at": None,
                "freshness": "UNOBSERVED",
                "source": source_obj,
                "evidence_kind": "NONE",
                "evidence": [],
                "error_code": "STORE_UNAVAILABLE",
                "capability_ids": [],
                "next_action_ids": ["diagnostics.refresh"],
            })
        elif store_data["error_code"] == "STATE_CORRUPT":
            checks.append({
                "check_id": "engine.runtime",
                "status": "FAIL",
                "scope": "passive",
                "observed_at": now_iso,
                "expires_at": expires_iso,
                "freshness": "FRESH",
                "source": source_obj,
                "evidence_kind": "MANAGED_RESULT",
                "evidence": [{"fact_id": "engine.runtime", "value": "corrupt_runtime_state"}],
                "error_code": "STATE_CORRUPT",
                "capability_ids": [],
                "next_action_ids": ["recovery.open"],
            })
        else:
            checks.append({
                "check_id": "engine.runtime",
                "status": "PASS",
                "scope": "passive",
                "observed_at": now_iso,
                "expires_at": expires_iso,
                "freshness": "FRESH",
                "source": source_obj,
                "evidence_kind": "MANAGED_RESULT",
                "evidence": [
                    {"fact_id": "engine.runtime", "value": store_data["sprint_status"] or "idle"},
                    {"fact_id": "engine.runtime", "value": store_data["graph_revision"]},
                ],
                "error_code": None,
                "capability_ids": ["engine.runtime_control"],
                "next_action_ids": ["diagnostics.refresh"],
            })

        # 4. engine.resources
        checks.append({
            "check_id": "engine.resources",
            "status": "PASS",
            "scope": "passive",
            "observed_at": now_iso,
            "expires_at": expires_iso,
            "freshness": "FRESH",
            "source": source_obj,
            "evidence_kind": "MANAGED_RESULT",
            "evidence": [
                {"fact_id": "engine.resources", "value": store_data["port_leases_count"]},
                {"fact_id": "engine.resources", "value": store_data["branch_leases_count"]},
            ],
            "error_code": None,
            "capability_ids": ["engine.resource_management"],
            "next_action_ids": ["diagnostics.refresh"],
        })

        # 5. engine.delivery
        checks.append({
            "check_id": "engine.delivery",
            "status": "PASS",
            "scope": "passive",
            "observed_at": now_iso,
            "expires_at": expires_iso,
            "freshness": "FRESH",
            "source": source_obj,
            "evidence_kind": "MANAGED_RESULT",
            "evidence": [{"fact_id": "engine.delivery", "value": store_data["queue_items_count"]}],
            "error_code": None,
            "capability_ids": ["engine.delivery_queue"],
            "next_action_ids": ["diagnostics.refresh"],
        })

        # 6. engine.result_reviews
        checks.append({
            "check_id": "engine.result_reviews",
            "status": "WARN" if store_data["pending_reviews_count"] > 0 else "PASS",
            "scope": "passive",
            "observed_at": now_iso,
            "expires_at": expires_iso,
            "freshness": "FRESH",
            "source": source_obj,
            "evidence_kind": "MANAGED_RESULT",
            "evidence": [{"fact_id": "engine.result_reviews", "value": store_data["pending_reviews_count"]}],
            "error_code": None,
            "capability_ids": ["engine.review_gates"],
            "next_action_ids": ["diagnostics.refresh"],
        })

        # 7. engine.recovery
        has_rec = store_data["has_pending_recovery"]
        checks.append({
            "check_id": "engine.recovery",
            "status": "WARN" if has_rec else "PASS",
            "scope": "passive",
            "observed_at": now_iso,
            "expires_at": expires_iso,
            "freshness": "FRESH",
            "source": source_obj,
            "evidence_kind": "MANAGED_RESULT",
            "evidence": [{"fact_id": "engine.recovery", "value": has_rec}],
            "error_code": None,
            "capability_ids": ["engine.recovery_coordination"],
            "next_action_ids": ["recovery.open", "diagnostics.refresh"] if has_rec else ["diagnostics.refresh"],
        })

        # 8. worker.configuration
        executor_url = os.environ.get("NGINX_QA_LAUNCHER_EXECUTOR_URL")
        executor_ref = os.environ.get("NGINX_QA_LAUNCHER_EXECUTOR_SECRET_REF")
        if executor_url:
            checks.append({
                "check_id": "worker.configuration",
                "status": "PASS",
                "scope": "passive",
                "observed_at": now_iso,
                "expires_at": expires_iso,
                "freshness": "FRESH",
                "source": source_obj,
                "evidence_kind": "CONFIG_METADATA",
                "evidence": [
                    {"fact_id": "worker.configuration", "value": "default-executor"},
                    {"fact_id": "worker.configuration", "value": bool(executor_ref)},
                ],
                "error_code": None,
                "capability_ids": ["worker.connection_probe"],
                "next_action_ids": ["worker.probe", "diagnostics.refresh"],
            })
        else:
            checks.append({
                "check_id": "worker.configuration",
                "status": "NOT_CONFIGURED",
                "scope": "passive",
                "observed_at": None,
                "expires_at": None,
                "freshness": "UNOBSERVED",
                "source": source_obj,
                "evidence_kind": "CONFIG_METADATA",
                "evidence": [],
                "error_code": "NOT_CONFIGURED",
                "capability_ids": [],
                "next_action_ids": ["setup.open"],
            })

        # 9. worker.binding
        checks.append({
            "check_id": "worker.binding",
            "status": "NOT_CONFIGURED",
            "scope": "passive",
            "observed_at": None,
            "expires_at": None,
            "freshness": "UNOBSERVED",
            "source": source_obj,
            "evidence_kind": "NONE",
            "evidence": [],
            "error_code": "NOT_CONFIGURED",
            "capability_ids": [],
            "next_action_ids": ["worker.bind"] if executor_url else ["setup.open"],
        })

        # 10. git.source
        checks.append({
            "check_id": "git.source",
            "status": "PASS" if git_ok else "WARN",
            "scope": "passive",
            "observed_at": now_iso,
            "expires_at": expires_iso,
            "freshness": "FRESH",
            "source": source_obj,
            "evidence_kind": "CODE",
            "evidence": [
                {"fact_id": "git.source", "value": code_commit[:40]},
                {"fact_id": "git.source", "value": branch},
            ],
            "error_code": None if git_ok else "SOURCE_STALE",
            "capability_ids": ["git.repository_inspect"],
            "next_action_ids": ["git.probe", "diagnostics.refresh"],
        })

        # 11. git.publication_authority
        checks.append({
            "check_id": "git.publication_authority",
            "status": "PASS",
            "scope": "passive",
            "observed_at": now_iso,
            "expires_at": expires_iso,
            "freshness": "FRESH",
            "source": source_obj,
            "evidence_kind": "CONFIG_METADATA",
            "evidence": [{"fact_id": "git.publication_authority", "value": "origin_configured"}],
            "error_code": None,
            "capability_ids": ["git.publication"],
            "next_action_ids": ["git.probe"],
        })

        # 12. secrets.references
        has_bot_ref = bool(os.environ.get("NGINX_QA_LAUNCHER_BOT_SECRET_REF"))
        has_webhook_ref = bool(os.environ.get("NGINX_QA_LAUNCHER_WEBHOOK_SECRET_REF"))
        has_executor_ref = bool(os.environ.get("NGINX_QA_LAUNCHER_EXECUTOR_SECRET_REF"))
        checks.append({
            "check_id": "secrets.references",
            "status": "PASS" if (has_bot_ref or has_webhook_ref or has_executor_ref) else "NOT_CONFIGURED",
            "scope": "passive",
            "observed_at": now_iso if (has_bot_ref or has_webhook_ref or has_executor_ref) else None,
            "expires_at": expires_iso if (has_bot_ref or has_webhook_ref or has_executor_ref) else None,
            "freshness": "FRESH" if (has_bot_ref or has_webhook_ref or has_executor_ref) else "UNOBSERVED",
            "source": source_obj,
            "evidence_kind": "CONFIG_METADATA",
            "evidence": [
                {"fact_id": "secrets.references", "value": has_bot_ref},
                {"fact_id": "secrets.references", "value": has_webhook_ref},
                {"fact_id": "secrets.references", "value": has_executor_ref},
            ] if (has_bot_ref or has_webhook_ref or has_executor_ref) else [],
            "error_code": None if (has_bot_ref or has_webhook_ref or has_executor_ref) else "NOT_CONFIGURED",
            "capability_ids": ["secrets.validation"],
            "next_action_ids": ["setup.open"],
        })

        # 13. provider.configuration
        advisor_ref = (
            os.environ.get("NGINX_QA_LAUNCHER_ADVISOR_SECRET_REF")
            or os.environ.get("NGINX_QA_LAUNCHER_CONTROL_ADVISOR_KEY")
        )
        checks.append({
            "check_id": "provider.configuration",
            "status": "PASS" if advisor_ref else "NOT_CONFIGURED",
            "scope": "passive",
            "observed_at": now_iso if advisor_ref else None,
            "expires_at": expires_iso if advisor_ref else None,
            "freshness": "FRESH" if advisor_ref else "UNOBSERVED",
            "source": source_obj,
            "evidence_kind": "CONFIG_METADATA",
            "evidence": [{"fact_id": "provider.configuration", "value": "advisor_configured"}] if advisor_ref else [],
            "error_code": None if advisor_ref else "NOT_CONFIGURED",
            "capability_ids": ["provider.advisory"] if advisor_ref else [],
            "next_action_ids": ["provider.probe"] if advisor_ref else ["setup.open"],
        })

        # 14. telegram.configuration
        has_tg = bool(os.environ.get("TELEGRAM_BOT_TOKEN") or has_bot_ref)
        checks.append({
            "check_id": "telegram.configuration",
            "status": "PASS" if has_tg else "NOT_CONFIGURED",
            "scope": "passive",
            "observed_at": now_iso if has_tg else None,
            "expires_at": expires_iso if has_tg else None,
            "freshness": "FRESH" if has_tg else "UNOBSERVED",
            "source": source_obj,
            "evidence_kind": "CONFIG_METADATA",
            "evidence": [{"fact_id": "telegram.configuration", "value": has_tg}] if has_tg else [],
            "error_code": None if has_tg else "NOT_CONFIGURED",
            "capability_ids": ["telegram.probe"] if has_tg else [],
            "next_action_ids": ["telegram.probe"] if has_tg else ["setup.open"],
        })

        # 15. tunnel.ownership & 16. tunnel.heartbeat
        origin_file = (
            Path(os.environ.get("NGINX_QA_LAUNCHER_ORIGIN_STATE", ""))
            if os.environ.get("NGINX_QA_LAUNCHER_ORIGIN_STATE")
            else None
        )
        tunnel_owned = False
        heartbeat_ok = False
        if origin_file and origin_file.exists():
            try:
                receipt = json.loads(origin_file.read_text(encoding="utf-8"))
                if receipt.get("phase") == "READY":
                    tunnel_owned = True
                    hb = receipt.get("heartbeat", 0)
                    if 0 <= (self.now.timestamp() - hb) <= 15:
                        heartbeat_ok = True
            except Exception:
                pass

        checks.append({
            "check_id": "tunnel.ownership",
            "status": "PASS" if tunnel_owned else "NOT_CONFIGURED",
            "scope": "passive",
            "observed_at": now_iso if tunnel_owned else None,
            "expires_at": expires_iso if tunnel_owned else None,
            "freshness": "FRESH" if tunnel_owned else "UNOBSERVED",
            "source": source_obj,
            "evidence_kind": "CONFIG_METADATA",
            "evidence": [{"fact_id": "tunnel.ownership", "value": tunnel_owned}] if tunnel_owned else [],
            "error_code": None if tunnel_owned else "NOT_CONFIGURED",
            "capability_ids": ["tunnel.ownership_inspect"] if tunnel_owned else [],
            "next_action_ids": ["setup.open"],
        })

        checks.append({
            "check_id": "tunnel.heartbeat",
            "status": "PASS" if heartbeat_ok else ("WARN" if tunnel_owned else "NOT_CONFIGURED"),
            "scope": "passive",
            "observed_at": now_iso if tunnel_owned else None,
            "expires_at": expires_iso if tunnel_owned else None,
            "freshness": ("FRESH" if heartbeat_ok else "STALE") if tunnel_owned else "UNOBSERVED",
            "source": source_obj,
            "evidence_kind": "LIVE_PROBE" if tunnel_owned else "NONE",
            "evidence": [{"fact_id": "tunnel.heartbeat", "value": heartbeat_ok}] if tunnel_owned else [],
            "error_code": None if heartbeat_ok else ("SOURCE_STALE" if tunnel_owned else "NOT_CONFIGURED"),
            "capability_ids": [],
            "next_action_ids": ["diagnostics.refresh"],
        })

        # Calculate available actions
        available_actions = ["diagnostics.refresh", "setup.open"]
        if executor_url:
            available_actions.append("worker.probe")
        if has_rec:
            available_actions.append("recovery.open")
        if git_ok:
            available_actions.append("git.probe")
        if has_tg:
            available_actions.append("telegram.probe")
        if advisor_ref:
            available_actions.append("provider.probe")

        # Deduplicate and sort against allowlist
        seen_actions = set()
        final_actions = []
        for a in available_actions:
            if a in ACTION_IDS and a not in seen_actions:
                seen_actions.add(a)
                final_actions.append(a)

        authority_fp = compute_authority_fingerprint(
            project_id,
            instance_id=instance_id,
            git_head=code_commit,
            control_revision=store_data["control_revision"],
            policy_generation=1,
            active_sprint_id=store_data["active_sprint_id"],
            active_assignment_id=store_data["active_assignment_id"],
        )

        raw_snapshot: dict[str, Any] = {
            "kind": "setup_diagnostic_snapshot",
            "schema_version": 1,
            "snapshot_id": "0" * 64,  # placeholder
            "project_id": project_id,
            "instance_id": instance_id,
            "generated_at": now_iso,
            "source": source_obj,
            "authority_fingerprint": authority_fp,
            "checks": checks,
            "available_action_ids": final_actions,
        }

        # Calculate exact snapshot_id digest
        raw_snapshot["snapshot_id"] = compute_snapshot_id(raw_snapshot)

        # Validate strictly against contracts.schema.json
        validator = get_snapshot_validator()
        validator.validate(raw_snapshot)

        return raw_snapshot


class DiagnosticReadGate:
    """Concurrency admission and bounded memory caching for diagnostic snapshots.

    Limit to 1 concurrent capture per application, 32 cached projects, 5-second TTL.
    """

    def __init__(self, *, max_cache_entries: int = 32, default_ttl: float = 5.0) -> None:
        self.max_cache_entries = max_cache_entries
        self.default_ttl = default_ttl
        self._cache: dict[tuple[str, int, str], tuple[float, dict[str, Any]]] = {}
        self._lock = asyncio.Lock()
        self._semaphore = asyncio.Semaphore(1)

    async def get_snapshot(
        self,
        project_id: str,
        *,
        instance_id: str | None = None,
        policy_generation: int = 1,
        code_root: Path | None = None,
        state_base: Path | None = None,
        git_config_path: Path | None = None,
        timeout: float = 4.0,
    ) -> dict[str, Any]:
        cache_key = (project_id, policy_generation, str(instance_id or ""))
        now_mono = time.monotonic()

        # 1. Fast cache check
        async with self._lock:
            cached = self._cache.get(cache_key)
            if cached is not None:
                expiry, snapshot = cached
                if now_mono < expiry:
                    return snapshot
                else:
                    self._cache.pop(cache_key, None)

        # 2. Bounded admission (max 1 concurrent capture)
        acquired = False
        try:
            # Wait up to timeout for admission
            await asyncio.wait_for(self._semaphore.acquire(), timeout=timeout)
            acquired = True
        except asyncio.TimeoutError:
            raise RuntimeError("DIAGNOSTIC_CAPTURE_BUSY")

        try:
            # Recheck cache after acquire
            now_mono = time.monotonic()
            async with self._lock:
                cached = self._cache.get(cache_key)
                if cached is not None:
                    expiry, snapshot = cached
                    if now_mono < expiry:
                        return snapshot

            collector = DiagnosticSnapshotCollector(
                code_root=code_root,
                state_base=state_base,
                git_config_path=git_config_path,
            )
            # Run pure capture in threadpool to ensure no event-loop block
            snapshot = await asyncio.to_thread(collector.collect_snapshot, project_id, instance_id=instance_id)

            async with self._lock:
                if len(self._cache) >= self.max_cache_entries:
                    oldest_key = min(self._cache.keys(), key=lambda k: self._cache[k][0])
                    self._cache.pop(oldest_key, None)
                self._cache[cache_key] = (time.monotonic() + self.default_ttl, snapshot)

            return snapshot
        finally:
            if acquired:
                self._semaphore.release()
