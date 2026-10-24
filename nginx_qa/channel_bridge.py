"""Durable managed-assignment channel mailbox. No queue claims or graph approvals.

Extension tables share the managed database. Admission and graph result submission
use the same sprint lock; runtime JSON and frozen schemas are not extended.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from collections.abc import Callable
from contextlib import closing, contextmanager
from typing import Any

from .channel_hub_adapter import HubClient
from .channel_models import (
    BINDING,
    TERMINAL,
    Binding,
    BridgeError,
    CreateRequest,
    Event,
    canonical,
    strict_json,
    verify_signature,
)
from .legacy_scope_control import LegacyScopeControlError, verify_role_token

PROFILES = {
    "neurogeneticist": {
        "specialization": "Neurogenetics",
        "requested_capabilities": ["request_data", "request_observation"],
    },
    "clinician": {
        "specialization": "Clinical research",
        "requested_capabilities": [
            "send_message",
            "request_data",
            "request_live_session",
        ],
    },
    "bioinformatician": {
        "specialization": "Bioinformatics",
        "requested_capabilities": ["request_data", "send_message"],
    },
}


def assignment(runtime, state: dict[str, Any], binding: Binding) -> dict[str, Any]:
    if (
        state.get("identity", {}).get("project_id") != binding.project_id
        or state.get("sprint_id") != binding.sprint_id
        or state.get("status") != "active"
    ):
        raise BridgeError("CHANNEL_ASSIGNMENT_INACTIVE", 409)
    matches = []
    for candidate in runtime._state_bindings(binding.project_id, state):
        record = candidate.record
        agent_id = (
            record.get("agent_id")
            or record.get("reviewer_id")
            or record.get("coordinator_id")
        )
        if (
            record.get("assignment_id") != binding.assignment_id
            or agent_id != binding.agent_id
            or record.get("status") != "active"
            or not runtime._binding_is_live(candidate)
        ):
            continue
        if candidate.kind == "assignment" and binding.assignment_id not in state.get(
            "active_assignment_ids", []
        ):
            continue
        matches.append(
            {
                **record,
                "assignment_kind": candidate.kind,
                "agent_id": agent_id,
                "agent_phone": record.get("agent_phone")
                or record.get("reviewer_phone")
                or record.get("coordinator_phone"),
            }
        )
    if len(matches) != 1:
        raise BridgeError("CHANNEL_ASSIGNMENT_INACTIVE", 409)
    return matches[0]


def pending_external_inputs(
    database, project_id: str, sprint_id: str, assignment_id: str
) -> bool:
    """Called while the ordinary continuity sprint lock is held; disabled is inert."""
    with closing(sqlite3.connect(database, timeout=30)) as db:
        if not db.execute(
            "SELECT 1 FROM sqlite_master WHERE name='channel_requests'"
        ).fetchone():
            return False
        return (
            db.execute(
                "SELECT 1 FROM channel_requests WHERE project_id=? AND sprint_id=? AND assignment_id=? AND blocking=1 AND acknowledged=0 LIMIT 1",
                (project_id, sprint_id, assignment_id),
            ).fetchone()
            is not None
        )


class ChannelBridge:
    def __init__(
        self,
        runtime,
        hub: HubClient,
        key: bytes,
        policy: Callable[[], dict],
        clock=time.time,
    ):
        if len(key) < 32:
            raise ValueError("32-byte transport key required")
        self.runtime, self.hub, self.key, self.policy, self.clock = (
            runtime,
            hub,
            key,
            policy,
            clock,
        )
        self.database = runtime.store.database_path
        with self.db() as db:
            db.executescript("""
            CREATE TABLE IF NOT EXISTS channel_requests(
                id TEXT PRIMARY KEY, project_id TEXT NOT NULL, sprint_id TEXT NOT NULL,
                assignment_id TEXT NOT NULL, agent_id TEXT NOT NULL, dedupe TEXT NOT NULL,
                body TEXT NOT NULL, origin TEXT NOT NULL, fingerprint TEXT NOT NULL, status TEXT NOT NULL,
                blocking INTEGER NOT NULL, acknowledged INTEGER NOT NULL DEFAULT 0,
                attempts INTEGER NOT NULL DEFAULT 0, due REAL NOT NULL DEFAULT 0,
                sent INTEGER NOT NULL DEFAULT 0, cancel_pending INTEGER NOT NULL DEFAULT 0,
                error TEXT, UNIQUE(project_id,dedupe));
            CREATE INDEX IF NOT EXISTS channel_assignment ON channel_requests(assignment_id);
            CREATE TABLE IF NOT EXISTS channel_events(
                id TEXT PRIMARY KEY, request_id TEXT NOT NULL, fingerprint TEXT NOT NULL,
                body TEXT NOT NULL, received_at REAL NOT NULL, disposition TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS channel_audit(
                sequence INTEGER PRIMARY KEY, at REAL NOT NULL, request_id TEXT,
                action TEXT NOT NULL, detail TEXT NOT NULL);
            """)

    @contextmanager
    def db(self):
        db = sqlite3.connect(self.database, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA synchronous=FULL")
        try:
            with db:
                yield db
        finally:
            db.close()

    def snapshot(self, binding: Binding):
        try:
            state = self.runtime.scope_snapshot(binding.project_id, binding.sprint_id)
            return assignment(self.runtime, state, binding)
        except (KeyError, RuntimeError):
            raise BridgeError("CHANNEL_ASSIGNMENT_INACTIVE", 409) from None

    def authorize_agent(self, binding: Binding, token: str):
        record = self.snapshot(binding)
        try:
            verify_role_token(record["agent_phone"], token)
        except LegacyScopeControlError as exc:
            raise BridgeError(exc.code, exc.status_code) from None
        self.entitlement(binding)
        return record

    def entitlement(self, binding: Binding) -> dict:
        policy = self.policy()
        item = (
            policy.get("projects", {})
            .get(binding.project_id, {})
            .get("agents", {})
            .get(binding.agent_id)
        )
        if (
            not isinstance(item, dict)
            or item.get("revoked", False)
            or item.get("expires_at", 0) <= self.clock()
        ):
            raise BridgeError("CHANNEL_AGENT_NOT_ENTITLED")
        return item

    def allowed(self, request: CreateRequest):
        item = self.entitlement(request)
        actions = item.get("channels", {}).get(request.channel_id, [])
        project = self.policy().get("projects", {}).get(request.project_id, {})
        if request.operation not in actions or request.operation not in project.get(
            "operations", []
        ):
            raise BridgeError("CHANNEL_OPERATION_DENIED")

    def introspect(self, binding: Binding, bearer: str) -> dict:
        policy = self.policy()
        expected = policy.get("introspection_token_sha256", "")
        import hmac

        if (
            len(bearer) < 32
            or not expected
            or not hmac.compare_digest(
                hashlib.sha256(bearer.encode()).hexdigest(), expected
            )
        ):
            raise BridgeError("CHANNEL_INTROSPECTION_DENIED")
        if binding.project_id not in policy.get("introspection_projects", []):
            raise BridgeError("CHANNEL_INTROSPECTION_DENIED")
        result = {**binding.model_dump(), "active": False, "schema_version": 1}
        try:
            self.snapshot(binding)
            item = self.entitlement(binding)
        except BridgeError:
            return result
        project = policy["projects"][binding.project_id]
        result.update(
            active=True,
            channels={
                k: sorted(set(v) & set(project.get("operations", [])))
                for k, v in item.get("channels", {}).items()
            },
            profile=PROFILES.get(item.get("profile", ""), {}),
            required_assurance_levels=project.get("required_assurance_levels", {}),
            max_auth_age_seconds=project.get("max_auth_age_seconds", 300),
        )
        return result

    def discover(self, binding: Binding, token: str):
        self.authorize_agent(binding, token)
        remote = self.hub.call("discover", binding.model_dump())
        item = self.entitlement(binding)
        operations = self.policy()["projects"][binding.project_id].get("operations", [])
        channels = []
        for value in remote.get("channels", []):
            if value.get("project_id") != binding.project_id:
                raise BridgeError("CHANNEL_HUB_SCOPE_MISMATCH", 502)
            allowed = set(item.get("channels", {}).get(value["channel_id"], [])) & set(
                operations
            )
            value["capabilities"] = sorted(allowed & set(value["capabilities"]))
            if value["capabilities"]:
                channels.append(value)
        return {
            "schema_version": 1,
            "channels": channels,
            "profile": PROFILES.get(item.get("profile", ""), {}),
            "authorization": "discovery_is_not_a_grant",
        }

    def create(self, request: CreateRequest, token: str):
        self.authorize_agent(request, token)
        self.allowed(request)
        if (
            len(
                canonical(
                    {
                        "schema_version": "1.1",
                        "action": "submit",
                        "payload": request.wire(),
                    }
                )
            )
            > 65536
        ):
            raise BridgeError("CHANNEL_BODY_TOO_LARGE", 413)
        fingerprint = hashlib.sha256(canonical(request.model_dump())).hexdigest()
        with self.runtime._sprint_lock(request.sprint_id):
            record = self.authorize_agent(request, token)
            self.allowed(request)
            origin = {
                k: record.get(k)
                for k in (
                    "assignment_kind",
                    "node_id",
                    "source_assignment_id",
                    "context_id",
                    "occurrence_id",
                    "graph_revision",
                    "workspace_id",
                    "initial_head_commit",
                )
            }
            with self.db() as db:
                old = db.execute(
                    "SELECT * FROM channel_requests WHERE id=? OR (project_id=? AND dedupe=?)",
                    (request.request_id, request.project_id, request.idempotency_key),
                ).fetchall()
                if old:
                    if len(old) != 1 or old[0]["fingerprint"] != fingerprint:
                        raise BridgeError("CHANNEL_IDEMPOTENCY_CONFLICT", 409)
                    return self.view(old[0])
                if (
                    abs(request.created_at - self.clock()) > 60
                    or request.deadline <= self.clock()
                ):
                    raise BridgeError("CHANNEL_REQUEST_EXPIRED", 422)
                outstanding = db.execute(
                    "SELECT count(*) FROM channel_requests WHERE project_id=? AND assignment_id=? AND acknowledged=0",
                    (request.project_id, request.assignment_id),
                ).fetchone()[0]
                if outstanding >= 100:
                    raise BridgeError("CHANNEL_ASSIGNMENT_QUOTA", 429)
                # Remote calls are deliberately outside the durable transaction. Intake rechecks
                # Hub permissions, and local permission is checked again on every dispatch.
                db.execute(
                    "INSERT INTO channel_requests(id,project_id,sprint_id,assignment_id,agent_id,dedupe,body,origin,fingerprint,status,blocking) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        request.request_id,
                        request.project_id,
                        request.sprint_id,
                        request.assignment_id,
                        request.agent_id,
                        request.idempotency_key,
                        canonical(request.model_dump()).decode(),
                        canonical(origin).decode(),
                        fingerprint,
                        "pending",
                        int(request.blocking),
                    ),
                )
                self.audit(
                    db, request.request_id, "created", {"blocking": request.blocking}
                )
                return self.view(
                    db.execute(
                        "SELECT * FROM channel_requests WHERE id=?",
                        (request.request_id,),
                    ).fetchone()
                )

    def audit(self, db, ident, action, detail):
        db.execute(
            "INSERT INTO channel_audit(at,request_id,action,detail) VALUES(?,?,?,?)",
            (self.clock(), ident, action, canonical(detail).decode()),
        )

    def view(self, row):
        body = json.loads(row["body"])
        return {
            "schema_version": 1,
            "request": body,
            "origin": json.loads(row["origin"]),
            "status": row["status"],
            "error": row["error"],
            "dependency": (
                "RESOLVED"
                if row["acknowledged"]
                else "EXTERNAL_INPUT_AVAILABLE"
                if row["status"] in TERMINAL
                else "WAITING_FOR_EXTERNAL_INPUT"
            )
            if row["blocking"]
            else "NONBLOCKING",
            "acknowledged": bool(row["acknowledged"]),
            "cancel_pending": bool(row["cancel_pending"]),
        }

    def owned_row(self, db, binding, ident):
        row = db.execute(
            "SELECT * FROM channel_requests WHERE id=?", (ident,)
        ).fetchone()
        if row is None or any(row[k] != getattr(binding, k) for k in BINDING):
            raise BridgeError("CHANNEL_REQUEST_NOT_FOUND", 404)
        return row

    def status(self, binding: Binding, token: str, ident: str):
        self.authorize_agent(binding, token)
        with self.db() as db:
            row = self.owned_row(db, binding, ident)
            result = self.view(row)
            result["events"] = [
                {
                    "event": json.loads(e["body"]),
                    "disposition": e["disposition"],
                    "trust": "untrusted_external_data",
                    "received_at": e["received_at"],
                }
                for e in db.execute(
                    "SELECT * FROM channel_events WHERE request_id=? ORDER BY rowid DESC LIMIT 100",
                    (ident,),
                )
            ]
            return result

    def mailbox(self, binding: Binding, token: str, after: str = ""):
        self.authorize_agent(binding, token)
        with self.db() as db:
            rows = db.execute(
                "SELECT * FROM channel_requests WHERE project_id=? AND sprint_id=? AND assignment_id=? AND agent_id=? AND id>? ORDER BY id LIMIT 100",
                (*[getattr(binding, k) for k in BINDING], after),
            ).fetchall()
            return {
                "requests": [self.view(r) for r in rows],
                "next_cursor": rows[-1]["id"] if len(rows) == 100 else None,
            }

    def change(self, binding: Binding, token: str, ident: str, action: str):
        with self.runtime._sprint_lock(binding.sprint_id):
            self.authorize_agent(binding, token)
            with self.db() as db:
                row = self.owned_row(db, binding, ident)
                if action == "ack":
                    if row["status"] not in TERMINAL:
                        raise BridgeError("CHANNEL_RESPONSE_NOT_READY", 409)
                    db.execute(
                        "UPDATE channel_requests SET acknowledged=1 WHERE id=?",
                        (ident,),
                    )
                elif action == "cancel" and row["status"] not in TERMINAL:
                    db.execute(
                        "UPDATE channel_requests SET status='cancelled',cancel_pending=1,due=0 WHERE id=?",
                        (ident,),
                    )
                self.audit(db, ident, action, {})
                return self.view(
                    db.execute(
                        "SELECT * FROM channel_requests WHERE id=?", (ident,)
                    ).fetchone()
                )

    def callback(self, body: bytes, timestamp: str, digest: str):
        verify_signature(self.key, timestamp, body, digest, self.clock())
        event = Event.model_validate(strict_json(body))
        fingerprint = hashlib.sha256(canonical(event.model_dump())).hexdigest()
        with self.db() as db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute(
                "SELECT fingerprint FROM channel_events WHERE id=?", (event.event_id,)
            ).fetchone()
            if old:
                if old[0] != fingerprint:
                    raise BridgeError("CHANNEL_EVENT_CONFLICT", 409)
                return {"accepted": True, "duplicate": True}
            row = db.execute(
                "SELECT * FROM channel_requests WHERE id=?", (event.request_id,)
            ).fetchone()
            if row is None:
                raise BridgeError("CHANNEL_REQUEST_NOT_FOUND", 404)
            request = CreateRequest.model_validate_json(row["body"])
            metadata = event.source_metadata.model_dump()
            if (
                event.correlation_id != request.correlation_id
                or event.channel_id != request.channel_id
                or any(metadata.get(k) != getattr(request, k) for k in BINDING)
                or any(
                    metadata.get(k) != getattr(request, k)
                    for k in ("experiment_id", "checkpoint_id")
                )
                or metadata.get("sample_id") != request.payload.sample_id
            ):
                raise BridgeError("CHANNEL_EVENT_SCOPE_MISMATCH", 409)
            disposition = "applied"
            # Deadlines/cancellation and already terminal outcomes never resurrect. Store late
            # receipts for audit without releasing another assignment or completing any graph node.
            try:
                self.snapshot(request)
                self.allowed(request)
            except BridgeError:
                disposition = "quarantined_inactive"
            if request.deadline <= self.clock() and row["status"] not in TERMINAL:
                db.execute(
                    "UPDATE channel_requests SET status='expired',cancel_pending=1 WHERE id=?",
                    (request.request_id,),
                )
                disposition = "late"
            elif row["status"] in TERMINAL:
                disposition = "late"
            if disposition == "applied":
                ranks = {
                    "pending": 0,
                    "accepted": 1,
                    "queued": 1,
                    "human_unavailable": 2,
                    "delivered": 3,
                }
                if event.event_type in TERMINAL or ranks.get(
                    event.event_type, 0
                ) >= ranks.get(row["status"], 0):
                    db.execute(
                        "UPDATE channel_requests SET status=?,sent=1 WHERE id=?",
                        (event.event_type, event.request_id),
                    )
            db.execute(
                "INSERT INTO channel_events VALUES(?,?,?,?,?,?)",
                (
                    event.event_id,
                    event.request_id,
                    fingerprint,
                    canonical(event.model_dump()).decode(),
                    self.clock(),
                    disposition,
                ),
            )
            self.audit(
                db,
                event.request_id,
                "callback",
                {"event_id": event.event_id, "disposition": disposition},
            )
        return {"accepted": True, "duplicate": False, "disposition": disposition}

    def tick(self):
        # Network calls hold neither the SQLite writer nor sprint lock: Hub introspection
        # can safely call back into nginx-qa. Concurrent ticks rely on remote idempotency.
        with self.db() as db:
            rows = db.execute(
                "SELECT * FROM channel_requests WHERE (status NOT IN ('response_received','rejected','failed','expired','cancelled') OR cancel_pending=1) AND due<=? ORDER BY due,id LIMIT 100",
                (self.clock(),),
            ).fetchall()
        for row in rows:
            request = CreateRequest.model_validate_json(row["body"])
            terminal = None
            cancel = bool(row["cancel_pending"])
            try:
                self.snapshot(request)
                self.allowed(request)
                if request.deadline <= self.clock():
                    terminal, cancel = "expired", True
            except BridgeError:
                terminal, cancel = "rejected", True
            if terminal:
                with self.db() as db:
                    db.execute(
                        "UPDATE channel_requests SET status=?,cancel_pending=1 WHERE id=? AND status NOT IN ('response_received','rejected','failed','expired','cancelled')",
                        (terminal, request.request_id),
                    )
            try:
                if cancel:
                    self.hub.call("cancel", request.wire())
                    with self.db() as db:
                        db.execute(
                            "UPDATE channel_requests SET cancel_pending=0,error=NULL WHERE id=?",
                            (request.request_id,),
                        )
                elif not row["sent"]:
                    self.hub.call("submit", request.wire())
                    with self.db() as db:
                        db.execute(
                            "UPDATE channel_requests SET sent=1,error=NULL,due=?,status=CASE WHEN status='pending' THEN 'accepted' ELSE status END WHERE id=?",
                            (self.clock() + 5, request.request_id),
                        )
                else:
                    remote = self.hub.call("status", request.wire())
                    # Status is only a hint. The signed event inbox is authoritative for replies.
                    if remote.get("status") in (
                        "rejected",
                        "failed",
                        "cancelled",
                        "expired",
                    ):
                        with self.db() as db:
                            db.execute(
                                "UPDATE channel_requests SET status=? WHERE id=? AND status NOT IN ('response_received','rejected','failed','expired','cancelled')",
                                (remote["status"], request.request_id),
                            )
                    with self.db() as db:
                        db.execute(
                            "UPDATE channel_requests SET due=?,error=NULL WHERE id=?",
                            (self.clock() + 5, request.request_id),
                        )
            except BridgeError as exc:
                if exc.code == "CHANNEL_HUB_CONTRACT_UNAVAILABLE":
                    # A missing remote route is not an authorization decision. Keep the
                    # immutable outbox/cancel intent until recovery or normal expiry.
                    self._retry(row, exc.code)
                    continue
                with self.db() as db:
                    db.execute(
                        "UPDATE channel_requests SET status=CASE WHEN status IN ('cancelled','expired','response_received') THEN status ELSE 'rejected' END,error=?,due=? WHERE id=?",
                        (exc.code, self.clock() + 30, request.request_id),
                    )
                    self.audit(db, request.request_id, "denied", {"error": exc.code})
            except OSError:
                self._retry(row, "CHANNEL_HUB_UNAVAILABLE")

    def _retry(self, row, error):
        with self.db() as db:
            db.execute(
                "UPDATE channel_requests SET attempts=attempts+1,due=?,error=? WHERE id=?",
                (
                    self.clock() + min(60, 2 ** min(row["attempts"] + 1, 6)),
                    error,
                    row["id"],
                ),
            )
