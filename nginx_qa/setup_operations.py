"""Durable operator intents for typed setup actions; execution stays in the engine."""
from __future__ import annotations

import asyncio
from contextlib import nullcontext
from copy import deepcopy
from datetime import datetime, timezone, timedelta
import hashlib
import json
import os
from pathlib import Path
import secrets
from typing import Any, Mapping

import jsonschema
from .setup_diagnostics import canonical_json_bytes, sha256_digest, get_contracts_schema
from .setup_worker_adapter import SetupWorkerAdapter
from .git_provider import ManagedFileLock


def _validator(name):
    schema = get_contracts_schema()
    return jsonschema.Draft202012Validator(
        {"$schema": schema["$schema"], "$defs": schema["$defs"], "$ref": "#/$defs/" + name},
        format_checker=jsonschema.FormatChecker())


def get_plan_validator():
    return _validator("ActionPlan")


def get_apply_validator():
    return _validator("ApplyRequest")


def get_receipt_validator():
    return _validator("OperationReceipt")


def compute_plan_digest(plan):
    return sha256_digest(canonical_json_bytes({k: v for k, v in plan.items() if k != "plan_digest"}))


def _now():
    return datetime.now(timezone.utc).isoformat()


class SetupApplyNotStarted(ValueError):
    """A durable rejected receipt fences even a delayed same-key request."""
    def __init__(self, plan, receipt):
        super().__init__("PLAN_STALE")
        self.plan_id = plan["plan_id"]
        self.plan_digest = plan["plan_digest"]
        self.receipt = deepcopy(receipt)


class SetupOperationsManager:
    """One durable, owner-bound intent per idempotency key.

    Metadata files contain plans and receipts only. Native sprint state, leases,
    queues, review decisions and graph transitions remain in the canonical DB.
    Interrupted APPLYING intents are reconciled, never blindly executed twice.
    """
    def __init__(self, *, worker_adapter=None, durable_store_dir=None,
                 authorize=None, authority=None, action_handlers=None,
                 profile_validator=None, status_reader=None, file_lock=None, binding_authority=None):
        self.worker_adapter = worker_adapter or SetupWorkerAdapter()
        self.durable_store_dir = Path(durable_store_dir) if durable_store_dir else None
        self.authorize, self.authority = authorize, authority
        self.action_handlers = action_handlers or {}
        self.profile_validator, self.status_reader = profile_validator, status_reader
        self.file_lock = file_lock or ManagedFileLock
        self.binding_authority = binding_authority or authority
        self._lock = asyncio.Lock()
        self._state = {"schema_version": 1, "plans": {}, "operations": {}, "idempotency": {}}

    def _locked(self):
        if self.durable_store_dir is None or self.file_lock is None:
            raise ValueError("DURABLE_STORE_NOT_CONFIGURED")
        self.durable_store_dir.mkdir(parents=True, exist_ok=True)
        for path in (self.durable_store_dir, *self.durable_store_dir.parents):
            if path.is_symlink() or int(getattr(path.lstat(), "st_file_attributes", 0)) & 0x400:
                raise ValueError("STATE_PATH_INVALID")
        return self.file_lock(self.durable_store_dir / "operations.lock")

    def _load(self):
        path = self.durable_store_dir / "operations.json"
        if not path.exists():
            self._state = {"schema_version": 1, "plans": {}, "operations": {}, "idempotency": {}}
            return
        if path.is_symlink() or path.stat().st_size > 16 * 1024 * 1024:
            raise ValueError("STATE_CORRUPT")
        from .setup_diagnostics import parse_strict_json_with_limits
        # This ledger is bounded separately from small browser payloads.
        data = json.loads(path.read_text(encoding="utf-8"))
        if (not isinstance(data, dict) or set(data) != {"schema_version", "plans", "operations", "idempotency"}
                or data["schema_version"] != 1 or any(not isinstance(data[k], dict) for k in ("plans", "operations", "idempotency"))):
            raise ValueError("STATE_CORRUPT")
        for digest, plan in data["plans"].items():
            get_plan_validator().validate(plan)
            if digest != compute_plan_digest(plan) or digest != plan["plan_digest"]:
                raise ValueError("STATE_CORRUPT")
        for opid, record in data["operations"].items():
            get_receipt_validator().validate(record["receipt"])
            if record["receipt"]["operation_id"] != opid or not record.get("actor_id"):
                raise ValueError("STATE_CORRUPT")
        self._state = data

    def _save(self):
        path = self.durable_store_dir / "operations.json"
        raw = canonical_json_bytes(self._state)
        if len(raw) > 16 * 1024 * 1024:
            raise ValueError("OPERATION_HISTORY_LIMIT")
        temp = path.with_name("operations-" + secrets.token_hex(8) + ".tmp")
        try:
            with temp.open("xb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp, path)
        finally:
            if temp.exists():
                temp.unlink()

    def _authorize(self, actor, project, action):
        if self.authorize is None:
            raise PermissionError("SETUP_POLICY_NOT_CONFIGURED")
        self.authorize(actor, project, action)

    def _authority(self, actor, project):
        if self.authority is None:
            raise PermissionError("AUTHORITY_UNAVAILABLE")
        return self.authority(actor, project)

    def create_plan(self, actor_id, project_id, plan_req, *, snapshot, host=None,
                    authority_fingerprint_override=None):
        _validator("PlanRequest").validate(plan_req)
        action = plan_req["action_id"]
        self._authorize(actor_id, project_id, action)
        if (snapshot.get("project_id") != project_id or snapshot.get("snapshot_id") != plan_req["snapshot_id"]
                or action not in snapshot.get("available_action_ids", [])):
            raise ValueError("SNAPSHOT_MISMATCH")
        fingerprint = self._authority(actor_id, project_id)
        if snapshot["authority_fingerprint"] != fingerprint:
            raise ValueError("PLAN_STALE")
        args = deepcopy(plan_req["arguments"])
        target = None
        resources = []
        preconditions = []
        permissions = ["read"]
        if action.endswith(".probe") or action == "diagnostics.refresh":
            permissions.append("probe")
        if action == "worker.bind":
            permissions.extend(["probe", "configure"])
        if action == "selftest.start":
            if self.profile_validator is None:
                raise ValueError("SELFTEST_NOT_CONFIGURED")
            profile = self.profile_validator(actor_id, project_id, args["test_profile_id"])
            if isinstance(profile, tuple):
                profile = profile[0]
            target = profile["target_project_id"]
            resources.append({"kind": "project", "id": target})
            resources.append({"kind": "self_test", "id": args["test_profile_id"]})
            permissions.extend(["selftest", "start"])
            preconditions.append({"check_id": "worker.binding", "required_status": ["PASS"], "must_be_fresh": True})
        elif action == "selftest.stop":
            permissions.append("stop")
            resources.append({"kind": "self_test", "id": args["self_test_id"]})
        elif action == "git.probe":
            resources.append({"kind": "repository", "id": args["repository_id"]})
        elif action == "recovery.open":
            resources.append({"kind": "instance", "id": args["instance_id"]})
        if action.startswith("worker."):
            resources.append({"kind": "adapter", "id": args["candidate_id"]})
        now = datetime.now(timezone.utc)
        plan = {
            "kind": "setup_action_plan", "schema_version": 1,
            "plan_id": "plan-" + secrets.token_hex(12), "plan_digest": "0" * 64,
            "actor_id": actor_id, "project_id": project_id, "target_project_id": target,
            "snapshot_id": snapshot["snapshot_id"], "action_id": action, "arguments": args,
            "resources": resources, "preconditions": preconditions, "required_permissions": permissions,
            "effects": [self._effect(action)], "authority_fingerprint": fingerprint,
            "created_at": now.isoformat(), "expires_at": (now + timedelta(seconds=120)).isoformat(),
            "confirmation_required": action in {"worker.bind", "selftest.start", "selftest.stop"}}
        plan["plan_digest"] = compute_plan_digest(plan)
        get_plan_validator().validate(plan)
        with self._locked():
            self._load()
            # Expired unapplied plans are disposable; operation history is retained.
            self._state["plans"] = {k: p for k, p in self._state["plans"].items()
                                    if p["expires_at"] > now.isoformat()
                                    or any(o["receipt"]["plan_digest"] == k for o in self._state["operations"].values())}
            if len(self._state["plans"]) >= 4096:
                raise ValueError("PLAN_LIMIT")
            self._state["plans"][plan["plan_digest"]] = plan
            self._save()
        return deepcopy(plan)

    @staticmethod
    def _effect(action):
        return {
            "diagnostics.refresh": "Capture current passive diagnostic facts.",
            "worker.probe": "Authenticate and inspect the configured loopback client.",
            "worker.bind": "Persist this operator's verified project client binding.",
            "provider.probe": "Check the configured provider using its bounded health adapter.",
            "git.probe": "Read remote refs using the registered repository credential.",
            "telegram.probe": "Read getMe and getWebhookInfo using the configured bot.",
            "setup.open": "Resolve private setup guidance for the selected topic.",
            "recovery.open": "Read an existing native instance recovery context.",
            "selftest.start": "Prepare and activate the pinned test manifest in its separate registered project.",
            "selftest.stop": "Fence and cancel the owned native self-test and wait for its client stop acknowledgement.",
        }[action]

    async def apply_plan(self, actor_id, project_id, apply_req, *, host=None, plan_id=None,
                         current_authority_fingerprint=None):
        get_apply_validator().validate(apply_req)
        # A cancelled HTTP waiter must not release the execution lock early.
        async with self._lock:
            task = asyncio.create_task(asyncio.to_thread(self._apply_sync, actor_id, project_id, dict(apply_req), plan_id))
            try:
                return await asyncio.shield(task)
            except asyncio.CancelledError:
                try:
                    await asyncio.shield(task)
                finally:
                    raise

    def _reject_stale_apply(self, actor, project, plan, idem):
        # Authority can change back while an earlier HTTP request is queued.
        # Reserve the exact key before declaring nonexecution, so that queued
        # request cannot later run after this rejection has reached the browser.
        opid, now = "op-" + secrets.token_hex(12), _now()
        receipt = {"kind": "setup_operation_receipt", "schema_version": 1, "operation_id": opid,
            "plan_id": plan["plan_id"], "plan_digest": plan["plan_digest"], "project_id": project,
            "action_id": plan["action_id"], "state": "BLOCKED", "created_at": now, "updated_at": now,
            "native_operation_id": None, "native_sprint_id": None, "evidence_ids": [],
            "error_code": "PLAN_STALE", "retryable": False, "deduplicated": False}
        get_receipt_validator().validate(receipt)
        self._state["operations"][opid] = {"actor_id": actor, "receipt": receipt}
        self._state["idempotency"][idem] = opid
        self._save()
        raise SetupApplyNotStarted(plan, receipt)

    def _apply_sync(self, actor, project, request, path_plan_id):
        with self._locked():
            self._load()
            digest = request["plan_digest"]
            plan = self._state["plans"].get(digest)
            if plan is None:
                raise LookupError("PLAN_NOT_FOUND")
            if (plan["actor_id"] != actor or plan["project_id"] != project
                    or (path_plan_id is not None and plan["plan_id"] != path_plan_id)):
                raise PermissionError("PLAN_OWNER_MISMATCH")
            self._authorize(actor, project, plan["action_id"])
            idem = hashlib.sha256(canonical_json_bytes([actor, project, request["idempotency_key"]])).hexdigest()
            existing = self._state["idempotency"].get(idem)
            if existing:
                record = self._state["operations"][existing]
                if record["receipt"]["plan_digest"] != digest:
                    raise ValueError("IDEMPOTENCY_CONFLICT")
                return {**self._public_record(record, actor, project), "deduplicated": True}
            binding_version = self.binding_authority(actor, project)
            fingerprint = self._authority(actor, project)
            if plan["authority_fingerprint"] != fingerprint:
                self._reject_stale_apply(actor, project, plan, idem)
            if datetime.fromisoformat(plan["expires_at"]) <= datetime.now(timezone.utc):
                self._reject_stale_apply(actor, project, plan, idem)
            if plan["confirmation_required"] and not request["confirmed"]:
                raise ValueError("CONFIRMATION_REQUIRED")
            opid, now = "op-" + secrets.token_hex(12), _now()
            receipt = {"kind": "setup_operation_receipt", "schema_version": 1, "operation_id": opid,
                "plan_id": plan["plan_id"], "plan_digest": digest, "project_id": project,
                "action_id": plan["action_id"], "state": "APPLYING", "created_at": now, "updated_at": now,
                "native_operation_id": None, "native_sprint_id": None, "evidence_ids": [],
                "error_code": None, "retryable": False, "deduplicated": False}
            record = {"actor_id": actor, "receipt": receipt}
            self._state["operations"][opid] = record
            self._state["idempotency"][idem] = opid
            self._save()  # intent and deduplication are durable before any external effect
            try:
                action, args = plan["action_id"], plan["arguments"]
                scope = {"actor_id": actor, "project_id": project,
                         "authority_fingerprint": binding_version}
                if action == "worker.probe":
                    result = self.worker_adapter.probe(args["candidate_id"], **scope)
                    receipt.update(state="SUCCEEDED" if result["status"] == "PASS" else "FAILED",
                                   error_code=result.get("error_code"), evidence_ids=[result["probe_receipt_id"]])
                elif action == "worker.bind":
                    result = self.worker_adapter.bind(args["candidate_id"], args["probe_receipt_id"], **scope)
                    receipt.update(state="SUCCEEDED", evidence_ids=[result["binding_id"]])
                else:
                    handler = self.action_handlers.get(action)
                    if handler is None:
                        receipt.update(state="BLOCKED", error_code="ACTION_NOT_CONFIGURED")
                    else:
                        fields = handler(**scope, arguments=deepcopy(args), operation_id=opid)
                        if not isinstance(fields, dict) or set(fields) - {
                            "state", "error_code", "retryable", "evidence_ids", "native_operation_id", "native_sprint_id"}:
                            raise ValueError("INVALID_ACTION_RECEIPT")
                        receipt.update(fields)
            except Exception as exc:
                # Do not expose exception text, credentials or transport payloads.
                receipt.update(state="UNKNOWN", error_code="OPERATION_REQUIRES_RECONCILIATION", retryable=False)
                if isinstance(exc, (PermissionError, ValueError, LookupError)):
                    receipt.update(state="BLOCKED", error_code="ACTION_PRECONDITION_FAILED")
            receipt["updated_at"] = _now()
            get_receipt_validator().validate(receipt)
            self._save()
            return deepcopy(receipt)

    def _public_record(self, record, actor, project):
        receipt = deepcopy(record["receipt"])
        if record["actor_id"] != actor or receipt["project_id"] != project:
            raise PermissionError("OPERATION_OWNER_MISMATCH")
        self._authorize(actor, project, "read")
        if self.status_reader and receipt["action_id"] == "selftest.start":
            try:
                receipt.update(self.status_reader(actor, project, receipt["native_operation_id"] or receipt["operation_id"]))
            except LookupError:
                if receipt["state"] == "APPLYING":
                    receipt.update(state="UNKNOWN", error_code="OPERATION_REQUIRES_RECONCILIATION", retryable=False)
        elif receipt["state"] == "APPLYING":
            receipt.update(state="UNKNOWN", error_code="OPERATION_REQUIRES_RECONCILIATION", retryable=False)
        get_receipt_validator().validate(receipt)
        return receipt

    def inspect_apply(self, actor_id, project_id, request, *, plan_id):
        """Read the original server plan and exact-key receipt, never apply.

        Absence of a receipt is not proof that an HTTP attempt cannot arrive
        later. The client must retain the same key, even when this read returns
        null. Atomic ledger replacement lets this reader avoid a write/lock.
        """
        _validator("ApplyInspectionRequest").validate(request)
        self._authorize(actor_id, project_id, "read")
        if self.durable_store_dir is None:
            raise LookupError("PLAN_NOT_FOUND")
        reader = SetupOperationsManager(worker_adapter=self.worker_adapter,
            durable_store_dir=self.durable_store_dir)
        reader._load()
        plan = reader._state["plans"].get(request["plan_digest"])
        if plan is None:
            raise LookupError("PLAN_NOT_FOUND")
        if (plan["actor_id"] != actor_id or plan["project_id"] != project_id
                or plan["plan_id"] != plan_id):
            raise PermissionError("PLAN_OWNER_MISMATCH")
        idem = hashlib.sha256(canonical_json_bytes([actor_id, project_id, request["idempotency_key"]])).hexdigest()
        opid = reader._state["idempotency"].get(idem)
        receipt = None
        if opid:
            record = reader._state["operations"][opid]
            if record["actor_id"] != actor_id or record["receipt"]["project_id"] != project_id:
                raise PermissionError("OPERATION_OWNER_MISMATCH")
            if record["receipt"]["plan_digest"] != plan["plan_digest"]:
                raise ValueError("IDEMPOTENCY_CONFLICT")
            # Return recorded evidence only. In particular, do not call a status
            # reader, reconcile native state, construct a worker or write a lock.
            receipt = deepcopy(record["receipt"])
        retry_permitted = True
        try:
            self._authorize(actor_id, project_id, plan["action_id"])
        except PermissionError:
            retry_permitted = False
        result = {"kind": "setup_apply_inspection", "schema_version": 1,
                  "plan": deepcopy(plan), "receipt": receipt, "retry_permitted": retry_permitted}
        _validator("ApplyInspectionResponse").validate(result)
        return result

    def get_operation(self, project_id, operation_id, *, actor_id):
        if self.durable_store_dir is None:
            raise LookupError("OPERATION_NOT_FOUND")
        # Atomic ledger replacement permits a passive reader without creating a lock file.
        reader = SetupOperationsManager(worker_adapter=self.worker_adapter,
            durable_store_dir=self.durable_store_dir, authorize=self.authorize, status_reader=self.status_reader)
        reader._load()
        record = reader._state["operations"].get(operation_id)
        if record is None:
            raise LookupError("OPERATION_NOT_FOUND")
        return reader._public_record(record, actor_id, project_id)

    def list_operations(self, project_id, *, actor_id, limit=20):
        self._authorize(actor_id, project_id, "read")
        if self.durable_store_dir is None:
            return []
        reader = SetupOperationsManager(worker_adapter=self.worker_adapter, durable_store_dir=self.durable_store_dir)
        reader._load()
        receipts = [deepcopy(record["receipt"]) for record in reader._state["operations"].values()
                    if record["actor_id"] == actor_id and record["receipt"]["project_id"] == project_id]
        # Recorded evidence only; individual GET refreshes native status passively.
        return sorted(receipts, key=lambda value: value["created_at"], reverse=True)[:min(max(limit, 1), 20)]
