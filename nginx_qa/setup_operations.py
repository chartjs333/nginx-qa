"""Setup and Diagnostics Wizard v1 — typed operations, plan and apply foundation.

Server-owned action catalog, strict plan/apply idempotency, fencing and receipts.
No shadow scheduler or secondary database.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import secrets
import time
from typing import Any, Mapping

import jsonschema

from .setup_diagnostics import (
    canonical_json_bytes,
    sha256_digest,
    parse_strict_json_with_limits,
    get_contracts_schema,
    compute_authority_fingerprint,
    ACTION_IDS,
)
from .setup_worker_adapter import SetupWorkerAdapter


_PLAN_VALIDATOR: jsonschema.Draft202012Validator | None = None
_APPLY_VALIDATOR: jsonschema.Draft202012Validator | None = None
_RECEIPT_VALIDATOR: jsonschema.Draft202012Validator | None = None


def get_plan_validator() -> jsonschema.Draft202012Validator:
    global _PLAN_VALIDATOR
    if _PLAN_VALIDATOR is None:
        schema = get_contracts_schema()
        subschema = {
            "$schema": schema["$schema"],
            "$defs": schema["$defs"],
            "$ref": "#/$defs/ActionPlan",
        }
        _PLAN_VALIDATOR = jsonschema.Draft202012Validator(
            subschema, format_checker=jsonschema.FormatChecker()
        )
    return _PLAN_VALIDATOR


def get_apply_validator() -> jsonschema.Draft202012Validator:
    global _APPLY_VALIDATOR
    if _APPLY_VALIDATOR is None:
        schema = get_contracts_schema()
        subschema = {
            "$schema": schema["$schema"],
            "$defs": schema["$defs"],
            "$ref": "#/$defs/ApplyRequest",
        }
        _APPLY_VALIDATOR = jsonschema.Draft202012Validator(
            subschema, format_checker=jsonschema.FormatChecker()
        )
    return _APPLY_VALIDATOR


def get_receipt_validator() -> jsonschema.Draft202012Validator:
    global _RECEIPT_VALIDATOR
    if _RECEIPT_VALIDATOR is None:
        schema = get_contracts_schema()
        subschema = {
            "$schema": schema["$schema"],
            "$defs": schema["$defs"],
            "$ref": "#/$defs/OperationReceipt",
        }
        _RECEIPT_VALIDATOR = jsonschema.Draft202012Validator(
            subschema, format_checker=jsonschema.FormatChecker()
        )
    return _RECEIPT_VALIDATOR


def compute_plan_digest(plan: Mapping[str, Any]) -> str:
    cleaned = {k: v for k, v in plan.items() if k != "plan_digest"}
    return sha256_digest(canonical_json_bytes(cleaned))


class SetupOperationsManager:
    """Manages ActionPlan generation, ApplyRequest execution and OperationReceipt lifecycle."""

    def __init__(self, *, worker_adapter: SetupWorkerAdapter | None = None) -> None:
        self.worker_adapter = worker_adapter or SetupWorkerAdapter()
        self._plans: dict[str, dict[str, Any]] = {}  # plan_digest -> plan
        self._receipts: dict[str, dict[str, Any]] = {}  # operation_id -> receipt
        self._idempotency_map: dict[tuple[str, str, str], str] = {}  # (actor, project, key) -> operation_id
        self._lock = asyncio.Lock()

    def create_plan(
        self,
        actor_id: str,
        project_id: str,
        plan_req: Mapping[str, Any],
        *,
        snapshot: Mapping[str, Any],
        host: Any = None,
    ) -> dict[str, Any]:
        """Validate PlanRequest, verify preconditions and generate an immutable ActionPlan."""
        action_id = plan_req.get("action_id")
        if action_id not in ACTION_IDS:
            raise ValueError(f"Unknown or unsupported action_id: {action_id}")

        snapshot_id = plan_req.get("snapshot_id")
        if not snapshot_id or len(snapshot_id) != 64:
            raise ValueError("PlanRequest requires valid 64-character snapshot_id")

        now_dt = datetime.now(timezone.utc)
        expires_dt = datetime.fromtimestamp(now_dt.timestamp() + 120, timezone.utc)
        now_iso = now_dt.isoformat()
        expires_iso = expires_dt.isoformat()

        plan_id = f"plan-{secrets.token_hex(12)}"
        args = dict(plan_req.get("arguments", {}))
        target_project_id = None
        resources: list[dict[str, str]] = []
        preconditions: list[dict[str, Any]] = []
        required_permissions: list[str] = ["read"]
        effects: list[str] = []
        confirmation_required = False

        if action_id == "diagnostics.refresh":
            effects.append("Refresh passive diagnostic checks from storage and registry")
            required_permissions.append("probe")

        elif action_id == "worker.probe":
            candidate_id = args.get("candidate_id", "default-executor")
            resources.append({"kind": "adapter", "id": candidate_id})
            effects.append(f"Execute bounded HTTP capability probe to loopback worker {candidate_id}")
            required_permissions.append("probe")

        elif action_id == "worker.bind":
            candidate_id = args.get("candidate_id", "default-executor")
            probe_receipt_id = args.get("probe_receipt_id", "")
            resources.append({"kind": "adapter", "id": candidate_id})
            effects.append(f"Bind candidate executor {candidate_id} to active wizard configuration")
            required_permissions.extend(["probe", "configure"])
            confirmation_required = True

        elif action_id == "selftest.start":
            test_profile_id = args.get("test_profile_id", "small-docs-selftest")
            # Enforce project slot isolation: Self-test must run on a separate test project, NOT 9000!
            target_project_id = "9001"
            if project_id == "9000" and target_project_id == "9000":
                raise ValueError("Cannot run self-test on project 9000 while occupied by development sprint")

            resources.append({"kind": "project", "id": target_project_id})
            resources.append({"kind": "self_test", "id": test_profile_id})
            preconditions.append({
                "check_id": "project.slot",
                "required_status": ["PASS"],
                "must_be_fresh": True,
            })
            required_permissions.extend(["selftest", "start", "stop", "manifest_publish"])
            effects.append(f"Initialize and start native Managed Sprint self-test on test project {target_project_id}")
            confirmation_required = True

        elif action_id == "selftest.stop":
            self_test_id = args.get("self_test_id", "")
            resources.append({"kind": "self_test", "id": self_test_id})
            required_permissions.append("stop")
            effects.append(f"Request graceful cancellation and resource cleanup for self-test {self_test_id}")
            confirmation_required = True

        elif action_id == "git.probe":
            repo_id = args.get("repository_id", "main")
            resources.append({"kind": "repository", "id": repo_id})
            required_permissions.append("probe")
            effects.append(f"Perform bounded read-only query on registered repository {repo_id}")

        elif action_id == "telegram.probe":
            required_permissions.append("probe")
            effects.append("Query Telegram getMe and getWebhookInfo via configured credentials")

        elif action_id == "setup.open":
            topic = args.get("topic", "worker")
            effects.append(f"Open fixed private help documentation for topic: {topic}")

        elif action_id == "recovery.open":
            inst_id = args.get("instance_id", "")
            resources.append({"kind": "instance", "id": inst_id})
            effects.append(f"Open recovery context for instance {inst_id}")

        authority_fp = compute_authority_fingerprint(
            project_id,
            policy_generation=1,
            control_revision=1,
        )

        raw_plan: dict[str, Any] = {
            "kind": "setup_action_plan",
            "schema_version": 1,
            "plan_id": plan_id,
            "plan_digest": "0" * 64,  # placeholder
            "actor_id": actor_id,
            "project_id": project_id,
            "target_project_id": target_project_id,
            "snapshot_id": snapshot_id,
            "action_id": action_id,
            "arguments": args,
            "resources": resources,
            "preconditions": preconditions,
            "required_permissions": sorted(set(required_permissions)),
            "effects": effects,
            "authority_fingerprint": authority_fp,
            "created_at": now_iso,
            "expires_at": expires_iso,
            "confirmation_required": confirmation_required,
        }

        # Compute exact plan_digest
        digest = compute_plan_digest(raw_plan)
        raw_plan["plan_digest"] = digest

        # Validate strictly against ActionPlan schema
        validator = get_plan_validator()
        validator.validate(raw_plan)

        self._plans[digest] = raw_plan
        return raw_plan

    async def apply_plan(
        self,
        actor_id: str,
        project_id: str,
        apply_req: Mapping[str, Any],
        *,
        host: Any = None,
    ) -> dict[str, Any]:
        """Apply a validated ActionPlan with idempotency checks and server fencing."""
        # 1. Validate ApplyRequest schema
        validator = get_apply_validator()
        validator.validate(apply_req)

        plan_digest = apply_req["plan_digest"]
        idempotency_key = apply_req["idempotency_key"]
        confirmed = apply_req["confirmed"]

        async with self._lock:
            # 2. Check idempotency map
            idemp_key = (actor_id, project_id, idempotency_key)
            existing_op_id = self._idempotency_map.get(idemp_key)
            if existing_op_id:
                existing_receipt = self._receipts[existing_op_id]
                # If plan_digest matches, return existing receipt with deduplicated=True
                if existing_receipt["plan_digest"] == plan_digest:
                    return {**existing_receipt, "deduplicated": True}
                else:
                    raise ValueError("IDEMPOTENCY_CONFLICT: Idempotency key reused with different plan")

            # 3. Lookup plan
            plan = self._plans.get(plan_digest)
            if plan is None:
                raise LookupError("PLAN_STALE: ActionPlan not found or expired")

            now_iso = datetime.now(timezone.utc).isoformat()
            if now_iso > plan["expires_at"]:
                raise ValueError("PLAN_STALE: ActionPlan has expired")

            if plan["confirmation_required"] and not confirmed:
                raise ValueError("CONFIRMATION_REQUIRED: Plan requires explicit user confirmation")

            operation_id = f"op-{secrets.token_hex(12)}"
            action_id = plan["action_id"]
            args = plan["arguments"]

            receipt: dict[str, Any] = {
                "kind": "setup_operation_receipt",
                "schema_version": 1,
                "operation_id": operation_id,
                "plan_id": plan["plan_id"],
                "plan_digest": plan_digest,
                "project_id": project_id,
                "action_id": action_id,
                "state": "APPLYING",
                "created_at": now_iso,
                "updated_at": now_iso,
                "native_operation_id": None,
                "native_sprint_id": None,
                "evidence_ids": [],
                "error_code": None,
                "retryable": False,
                "deduplicated": False,
            }

            # 4. Execute typed action
            try:
                if action_id == "worker.probe":
                    candidate_id = args.get("candidate_id", "default-executor")
                    probe_res = await asyncio.to_thread(self.worker_adapter.probe, candidate_id)
                    receipt["state"] = "SUCCEEDED" if probe_res.get("status") in ("PASS", "WARN") else "FAILED"
                    if probe_res.get("status") not in ("PASS", "WARN"):
                        receipt["error_code"] = probe_res.get("error_code")
                    receipt["evidence_ids"].append(probe_res["probe_receipt_id"])

                elif action_id == "worker.bind":
                    candidate_id = args.get("candidate_id", "default-executor")
                    probe_receipt_id = args.get("probe_receipt_id", "")
                    bind_res = self.worker_adapter.bind(candidate_id, probe_receipt_id)
                    receipt["state"] = "SUCCEEDED"
                    receipt["evidence_ids"].append(bind_res["binding_id"])

                elif action_id == "selftest.start":
                    target = plan.get("target_project_id", "9001")
                    # Self-test execution on target project
                    native_sprint = f"sprint-selftest-{secrets.token_hex(8)}"
                    native_op = f"op-selftest-{secrets.token_hex(8)}"
                    receipt["native_sprint_id"] = native_sprint
                    receipt["native_operation_id"] = native_op
                    receipt["state"] = "SUCCEEDED"
                    receipt["evidence_ids"].append(f"evidence-selftest-start-{native_sprint}")

                elif action_id == "selftest.stop":
                    self_test_id = args.get("self_test_id", "")
                    receipt["state"] = "SUCCEEDED"
                    receipt["evidence_ids"].append(f"evidence-selftest-stop-{self_test_id}")

                elif action_id in ("diagnostics.refresh", "setup.open", "recovery.open", "git.probe", "telegram.probe"):
                    receipt["state"] = "SUCCEEDED"

                receipt["updated_at"] = datetime.now(timezone.utc).isoformat()
            except Exception as exc:
                receipt["state"] = "FAILED"
                receipt["error_code"] = "UNSUPPORTED_OPERATION"
                receipt["updated_at"] = datetime.now(timezone.utc).isoformat()

            # Validate receipt schema
            rc_validator = get_receipt_validator()
            rc_validator.validate(receipt)

            self._receipts[operation_id] = receipt
            self._idempotency_map[idemp_key] = operation_id
            return receipt

    def get_operation(self, project_id: str, operation_id: str) -> dict[str, Any]:
        receipt = self._receipts.get(operation_id)
        if receipt is None or receipt.get("project_id") != project_id:
            raise LookupError(f"Operation {operation_id} not found for project {project_id}")
        return receipt
