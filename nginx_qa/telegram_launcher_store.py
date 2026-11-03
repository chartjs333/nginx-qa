"""Launcher ingress receipts in the existing managed control database.

This adapter reserves a project and records transport/provision evidence. It
does not create assignments, schedule graph nodes or maintain a second engine.
"""
from __future__ import annotations

import hashlib
import json
import os
from copy import deepcopy
from datetime import datetime, timezone, timedelta
from typing import Any, Mapping
from uuid import uuid4

from nginx_qa.managed_import import TransactionalSprintImporter
from nginx_qa.sprint_types import canonical_json_sha256
from nginx_qa.telegram_launcher import Principal, fail


class LauncherOperations:
    def __init__(self, importer: TransactionalSprintImporter):
        importer.validate_mutation_roots()
        self.importer = importer
        self.store = importer.store

    @staticmethod
    def keys(bot_id: str, update_id: int, message_id: int, principal: Principal) -> tuple[str, str]:
        return (hashlib.sha256(f"{bot_id}:update:{update_id}".encode()).hexdigest(),
                hashlib.sha256(f"{bot_id}:message:{principal.chat_id}:{message_id}".encode()).hexdigest())

    @staticmethod
    def _decode(row) -> dict[str, Any]:
        try:
            record = json.loads(row["record_json"])
            if (record["operation_id"] != row["operation_id"] or record["project_id"] != row["project_id"]
                or record["dedupe_key"] != row["dedupe_key"] or record["message_key"] != row["message_key"]
                or record["payload_sha256"] != canonical_json_sha256(record["envelope"])):
                raise ValueError
            LauncherOperations._validate_client_receipts(record)
            return record
        except (ValueError, KeyError, TypeError, AttributeError):
            fail("OPERATION_CORRUPT", "Launcher operation requires operator recovery.", 409)

    @staticmethod
    def _validate_client_receipts(record, attempt=None):
        from nginx_qa.telegram_launcher_execution import descriptor, validate_receipt, ExecutorUnavailable
        try:
            ready = record.get("executor_ready")
            if ready is not None:
                validate_receipt(ready, descriptor(record), "READY")
            for key, status in (("executor_receipt", "ACCEPTED"), ("executor_stop_receipt", "STOPPED")):
                receipt = record.get(key)
                if receipt is None:
                    continue
                if not isinstance(receipt, dict):
                    raise ValueError
                expected = descriptor(record)
                if key == "executor_receipt":
                    ids = receipt.get("initial_assignment_ids")
                    if not isinstance(ids, list) or not ids or any(not isinstance(item, str) for item in ids):
                        raise ValueError
                    if attempt is not None and (attempt["status"] != "SUCCEEDED" or ids != attempt["response"]["initial_assignment_ids"]):
                        raise ValueError
                    expected["initial_assignment_ids"] = ids
                validate_receipt(receipt, expected, status)
                if not ready or receipt["client_session_id"] != ready["client_session_id"]:
                    raise ValueError
        except (ExecutorUnavailable, KeyError, TypeError, ValueError):
            fail("OPERATION_CORRUPT", "Stored client evidence requires operator recovery.", 409)

    @staticmethod
    def _owner(record: Mapping[str, Any], principal: Principal) -> None:
        if (record["user_id"], record["chat_id"], record["project_id"]) != (
                principal.user_id, principal.chat_id, principal.project_id):
            fail("ACCESS_DENIED", "Operation belongs to another Telegram user or project.", 403)

    def _replay(self, connection, principal, keys, digest, transport):
        rows = connection.execute(
            "SELECT * FROM telegram_launcher_operations WHERE dedupe_key = ? OR message_key = ?", keys).fetchall()
        if not rows:
            return None
        if len(rows) != 1:
            fail("IDEMPOTENCY_CONFLICT", "Telegram delivery identities conflict.", 409)
        record = self._decode(rows[0])
        self._owner(record, principal)
        if ((record["dedupe_key"], record["message_key"]) != keys or record["payload_sha256"] != digest
                or record["transport"] != transport):
            fail("IDEMPOTENCY_CONFLICT", "This Telegram delivery identity already has a different payload.", 409)
        return record

    def lookup(self, principal: Principal, keys: tuple[str, str], envelope: Mapping[str, Any], transport: Mapping[str, Any]):
        if not self.store.database_path.exists():
            return None
        with self.store._transaction() as connection:
            return self._replay(connection, principal, keys, canonical_json_sha256(envelope), transport)

    def reserve(self, principal: Principal, keys: tuple[str, str], envelope: Mapping[str, Any],
                transport: Mapping[str, Any], repository_binding: Mapping[str, Any]) -> dict[str, Any]:
        digest = canonical_json_sha256(envelope)
        try:
            quota = int(os.getenv("NGINX_QA_LAUNCHER_PREPARED_LIMIT", "100"))
            ttl = int(os.getenv("NGINX_QA_LAUNCHER_PREPARED_TTL_SECONDS", "86400"))
            if not 1 <= quota <= 100 or not 300 <= ttl <= 604800:
                raise ValueError
        except ValueError:
            fail("LIMIT_CONFIG_INVALID", "Prepared limit must be 1..100 and TTL 300..604800 seconds.", 503)
        with self.store._transaction() as connection:
            replay = self._replay(connection, principal, keys, digest, transport)
            if replay is not None:
                return replay
            control, revision = self.store._load_control(connection, principal.project_id)
            self.store._validate_control_snapshot(connection, principal.project_id, control)
            held = connection.execute("SELECT * FROM telegram_launcher_operations WHERE project_id = ? AND holds_slot = 1",
                                      (principal.project_id,)).fetchone()
            if held is not None:
                self._refresh(connection, held, control)
            self.store.assert_launcher_slot_available(connection, principal.project_id)
            indexed = self.store._indexed_runtime_or_error(connection, control, principal.project_id,
                correlation_id="launcher-reserve", phase="VALIDATE")
            if indexed is not None and indexed[0]["status"] == "active":
                fail("PROJECT_BUSY", "This project already has an active managed sprint.", 409)
            if any(r.get("status") not in {"SUCCEEDED", "FAILED"} for r in control["start_idempotency_records"]):
                fail("PROJECT_BUSY", "This project already has a managed import in progress.", 409)
            # Bounded prepared/blocked receipts across distinct projects. History
            # is retained; retirement is an explicit later lifecycle operation.
            if connection.execute("SELECT COUNT(*) FROM telegram_launcher_operations WHERE holds_slot = 1").fetchone()[0] >= quota:
                fail("PREPARED_QUOTA_EXCEEDED", "The pending instance quota is full.", 409)
            now = datetime.now(timezone.utc).isoformat()
            record = {"operation_id": "launch-" + uuid4().hex, "project_id": principal.project_id,
                "user_id": principal.user_id, "chat_id": principal.chat_id,
                "dedupe_key": keys[0], "message_key": keys[1], "transport": dict(transport),
                "payload_sha256": digest, "envelope": deepcopy(dict(envelope)),
                "repository_binding": deepcopy(dict(repository_binding)),
                "created_at": now, "updated_at": now, "lifecycle": "Created",
                "prepare_expires_at": (datetime.fromisoformat(now) + timedelta(seconds=ttl)).isoformat(),
                "blocked_reason": "PUBLICATION_REQUIRED", "provision_intent": None,
                "published": False, "instance_id": None, "sprint_id": None}
            # Ensure the canonical project control exists in this same commit.
            self.store._write_control(connection, control, revision)
            connection.execute("INSERT INTO telegram_launcher_operations VALUES (?, ?, ?, ?, 1, ?)",
                (record["operation_id"], principal.project_id, *keys, json.dumps(record, ensure_ascii=False)))
            return record

    def _refresh(self, connection, row, control):
        """Project launcher state from engine receipts, not requested UI labels."""
        record = self._decode(row)
        attempt = self.store.launcher_attempt(control, record["operation_id"])
        self._validate_client_receipts(record, attempt)
        holds_slot = row["holds_slot"]
        if attempt:
            record["current_import_key"] = attempt["idempotency_key"]
            record["attempt_id"] = attempt["attempt_id"]
            record["sprint_id"] = attempt["sprint_id"]
            if attempt["status"] == "FAILED" and record["lifecycle"] != "Stopped":
                record.update(lifecycle="Failed", blocked_reason=attempt["error"]["detail"]["error"])
            elif attempt["status"] == "SUCCEEDED":
                runtime_row = connection.execute("SELECT * FROM managed_sprints WHERE project_id = ? AND sprint_id = ?",
                                                (record["project_id"], attempt["sprint_id"])).fetchone()
                state = json.loads(runtime_row["state_json"])
                if self.store._runtime_row_issues(control, runtime_row, state, require_indexed=False):
                    fail("OPERATION_CORRUPT", "Runtime evidence requires operator recovery.", 409)
                record["engine_activated"] = True
                if state["status"] == "active":
                    accepted = record.get("executor_receipt", {}).get("status") == "ACCEPTED"
                    record.update(lifecycle="Running" if accepted else "Prepared",
                                  blocked_reason=None if accepted else "EXECUTOR_UNAVAILABLE")
                    if state.get("operator_stop_requested"):
                        record["blocked_reason"] = "STOP_PENDING"
                elif state["status"] in {"completed", "failed", "blocked", "stopped"}:
                    from nginx_qa.managed_cancellation import assert_no_live_resources
                    assert_no_live_resources(connection, state)
                    record.update(lifecycle={"completed": "Completed", "stopped": "Stopped"}.get(state["status"], "Failed"),
                                  blocked_reason=None if state["status"] in {"completed", "stopped"} else "ENGINE_TERMINAL_FAILURE")
                    holds_slot = 0
        if record != self._decode(row) or holds_slot != row["holds_slot"]:
            connection.execute("UPDATE telegram_launcher_operations SET holds_slot = ?, record_json = ? WHERE operation_id = ?",
                               (holds_slot, json.dumps(record, ensure_ascii=False), record["operation_id"]))
        return record

    def get(self, operation_id: str, principal: Principal) -> dict[str, Any]:
        with self.store._transaction() as connection:
            row = connection.execute("SELECT * FROM telegram_launcher_operations WHERE operation_id = ?", (operation_id,)).fetchone()
            if row is None:
                fail("OPERATION_NOT_FOUND", "Launcher operation was not found.", 404)
            record = self._decode(row)
            self._owner(record, principal)
            control, _ = self.store._load_control(connection, principal.project_id)
            self.store._validate_control_snapshot(connection, principal.project_id, control)
            return self._refresh(connection, row, control)

    def request_launch(self, operation_id, principal):
        if "start" not in principal.actions:
            fail("ACCESS_DENIED", "Start permission is required.", 403)
        return self.update(operation_id, principal, launch_requested=True)

    def accept_executor(self, operation_id, principal, receipt):
        from nginx_qa.telegram_launcher_execution import descriptor, validate_receipt
        # Record a real ACK even if the client completed before its HTTP reply.
        with self.store._transaction() as connection:
            row = connection.execute("SELECT * FROM telegram_launcher_operations WHERE operation_id = ?", (operation_id,)).fetchone()
            if row is None:
                fail("OPERATION_NOT_FOUND", "Launcher operation was not found.", 404)
            record = self._decode(row)
            self._owner(record, principal)
            control, _ = self.store._load_control(connection, principal.project_id)
            self.store._validate_control_snapshot(connection, principal.project_id, control)
            attempt = self.store.launcher_attempt(control, operation_id)
            if attempt is None or attempt["status"] != "SUCCEEDED":
                fail("OPERATION_STATE_CONFLICT", "Executor acceptance requires engine activation.", 409)
            expected = {**descriptor(record), "initial_assignment_ids": attempt["response"]["initial_assignment_ids"]}
            validated = validate_receipt(receipt, expected, "ACCEPTED")
            if validated["client_session_id"] != record.get("executor_ready", {}).get("client_session_id"):
                fail("EXECUTOR_RECEIPT_CONFLICT", "Executor session differs from readiness.", 409)
            if record.get("executor_receipt") not in (None, validated):
                fail("EXECUTOR_RECEIPT_CONFLICT", "Executor acceptance is immutable.", 409)
            record["executor_receipt"] = validated
            connection.execute("UPDATE telegram_launcher_operations SET record_json = ? WHERE operation_id = ?",
                               (json.dumps(record, ensure_ascii=False), operation_id))
            updated = connection.execute("SELECT * FROM telegram_launcher_operations WHERE operation_id = ?", (operation_id,)).fetchone()
            return self._refresh(connection, updated, control)

    def update(self, operation_id: str, principal: Principal, **changes) -> dict[str, Any]:
        if set(changes) - {"provision_intent", "published", "blocked_reason", "launch_requested", "executor_ready", "stop_requested", "executor_stop_receipt"}:
            raise ValueError("not a provision update")
        with self.store._transaction() as connection:
            row = connection.execute("SELECT * FROM telegram_launcher_operations WHERE operation_id = ?", (operation_id,)).fetchone()
            if row is None:
                fail("OPERATION_NOT_FOUND", "Launcher operation was not found.", 404)
            record = self._decode(row)
            self._owner(record, principal)
            if not row["holds_slot"]:
                fail("OPERATION_STATE_CONFLICT", "Operation no longer owns the project reservation.", 409)
            if record["published"] and changes == {"blocked_reason": "PUBLICATION_UNAVAILABLE"}:
                return record
            intent = changes.get("provision_intent")
            if intent is not None and record["provision_intent"] not in (None, intent):
                fail("MANIFEST_INTENT_CONFLICT", "The original manifest intent is immutable.", 409)
            if changes.get("published") is False and record["published"]:
                fail("OPERATION_STATE_CONFLICT", "Published evidence is immutable.", 409)
            for receipt_key in ("executor_receipt", "executor_ready", "executor_stop_receipt"):
                if receipt_key in changes and record.get(receipt_key) not in (None, changes[receipt_key]):
                    fail("EXECUTOR_RECEIPT_CONFLICT", "Executor acknowledgement is immutable.", 409)
            record.update(deepcopy(changes))
            self._validate_client_receipts(record)
            record["updated_at"] = datetime.now(timezone.utc).isoformat()
            connection.execute("UPDATE telegram_launcher_operations SET record_json = ? WHERE operation_id = ?",
                               (json.dumps(record, ensure_ascii=False), operation_id))
            return record

    def preparation_request(self, operation_id: str, principal: Principal):
        from nginx_qa.sprint_types import StartSprintFromGitRequest
        record = self.get(operation_id, principal)
        if not record["published"] or not isinstance(record["provision_intent"], dict):
            fail("PUBLICATION_REQUIRED", "Publish the immutable manifest before preparation.", 409)
        intent = record["provision_intent"]
        return StartSprintFromGitRequest(repository_id=record["repository_binding"]["repository_id"],
            ref=intent["ref"], manifest_path=intent["manifest_path"], idempotency_key=record.get("current_import_key", operation_id))

    def prepare(self, operation_id: str, principal: Principal) -> dict[str, Any]:
        if "prepare" not in principal.actions:
            fail("ACCESS_DENIED", "Preparation permission is required.", 403)
        current = self.get(operation_id, principal)
        if current.get("engine_activated") or current["lifecycle"] == "Stopped":
            return current
        request = self.preparation_request(operation_id, principal)
        self.importer.prepare(principal.project_id, request, operation_id=operation_id)
        return self.get(operation_id, principal)

    def cancel_prepared(self, operation_id: str, principal: Principal) -> dict[str, Any]:
        if "stop" not in principal.actions:
            fail("ACCESS_DENIED", "Stop permission is required.", 403)
        self.get(operation_id, principal)
        return self.store.cancel_prepared(principal.project_id, operation_id, clock=self.importer.clock)

    @staticmethod
    def public(record: Mapping[str, Any], *, deduplicated: bool = False) -> dict[str, Any]:
        return {"ok": True, "operation_id": record["operation_id"], "project_id": record["project_id"],
                "status": record["lifecycle"], "blocked_reason": record["blocked_reason"],
                "autonomous_mode": record["envelope"]["autonomous_mode"],
                "retryable": record["blocked_reason"] is not None,
                "instance_id": record["instance_id"], "sprint_id": record["sprint_id"],
                "deduplicated": deduplicated, "activated": record.get("engine_activated", False)}
