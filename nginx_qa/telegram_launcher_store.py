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
            return record
        except (ValueError, KeyError, TypeError):
            fail("OPERATION_CORRUPT", "Launcher operation requires operator recovery.", 409)

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

    def get(self, operation_id: str, principal: Principal) -> dict[str, Any]:
        with self.store._transaction() as connection:
            row = connection.execute("SELECT * FROM telegram_launcher_operations WHERE operation_id = ?", (operation_id,)).fetchone()
            if row is None:
                fail("OPERATION_NOT_FOUND", "Launcher operation was not found.", 404)
            record = self._decode(row)
            self._owner(record, principal)
            control, _ = self.store._load_control(connection, principal.project_id)
            self.store._validate_control_snapshot(connection, principal.project_id, control)
            attempt = self.store._record(control, operation_id)
            if attempt and attempt.get("status") == "FAILED" and record["lifecycle"] != "Stopped":
                record.update(lifecycle="Failed", blocked_reason=attempt["error"]["detail"]["error"],
                              sprint_id=attempt.get("sprint_id"), attempt_id=attempt["attempt_id"])
            return record

    def update(self, operation_id: str, principal: Principal, **changes) -> dict[str, Any]:
        if set(changes) - {"provision_intent", "published", "blocked_reason"}:
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
            record.update(deepcopy(changes))
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
            ref=intent["ref"], manifest_path=intent["manifest_path"], idempotency_key=operation_id)

    def prepare(self, operation_id: str, principal: Principal) -> dict[str, Any]:
        if "prepare" not in principal.actions:
            fail("ACCESS_DENIED", "Preparation permission is required.", 403)
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
                "deduplicated": deduplicated, "activated": False}
