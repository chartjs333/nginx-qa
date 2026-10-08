"""Notification lifecycle, separate from authoritative execution transactions."""
from __future__ import annotations

import asyncio
from copy import deepcopy

from . import scope_workflow as flow
from . import scope_runtime_adapters as adapters
from .legacy_scope_control import canonical_json_sha256
from .decision_notifications import NotificationStore, NotificationWorker


IDENTITY_FIELDS = (
    "assignment_id", "agent_id", "agent_phone", "node_id", "phase", "occurrence",
    "occurrence_id", "source_assignment_id",
)


def notification_snapshot(project_id, sprint_id, state, request, *, project_name="", sprint_name=""):
    """Return a binding only for a genuine, currently bound human decision.

    The outbox preserves the FIRST binding for a request. A later execution
    revision cannot silently refresh an already emailed authorization context.
    No scope material, credential or runtime environment is needed by SMTP.
    """
    if request.get("status") != "pending" or request.get("authorization_provenance"):
        return None
    try:
        assignment = adapters.assignment_identity(state, request["assignment_id"])
    except ValueError:
        return None
    if assignment.get("status") != "active":
        return None
    if (assignment.get("scope_handoff_intent") or {}).get("status") == "publishing":
        return None
    kind = adapters.runtime_kind(state)
    if kind != "managed_workspace_v1" and state.get("mode") != "parallel" and state.get("current_assignment_id") != assignment.get("assignment_id"):
        return None
    identity = {key: deepcopy(assignment.get(key)) for key in IDENTITY_FIELDS}
    if identity != request.get("identity") or request["base_scope_revision"] != flow.scope_revision(state):
        return None
    immutable = {key: deepcopy(value) for key, value in request.items() if key not in {"status", "decision", "notification"}}
    return {
        "runtime_type": kind, "project_id": str(project_id), "sprint_id": str(sprint_id),
        "request_id": request["request_id"], "request_sha256": request["request_sha256"],
        "content_sha256": canonical_json_sha256(immutable),
        "identity_sha256": canonical_json_sha256(identity),
        "assignment_id": request["assignment_id"],
        "base_scope_revision": flow.scope_revision(state),
        "execution_revision": flow.execution_revision(state),
        "project_name": project_name or str(project_id), "sprint_name": sprint_name or str(sprint_id),
        "node_id": str(identity.get("node_id") or ""),
        "role_id": str(identity.get("agent_phone") or identity.get("agent_id") or ""),
        # The store redacts complete secret values BEFORE truncating display.
        "summary": str((request.get("proposal") or {}).get("instructions") or ""),
        "reason": str(request.get("reason") or ""),
    }


class DecisionNotificationService:
    """A recoverable sidecar outbox, never a second semantic decision ledger."""

    def __init__(self, config, scan, current, provider=None):
        self.config = config
        self.store = NotificationStore(config.database_path,
            link_ttl_seconds=config.link_ttl_seconds, lease_seconds=config.lease_seconds,
            max_attempts=config.max_attempts,
            credentials=tuple(v for v in (config.smtp_username, config.smtp_password) if v)) if config.enabled else None
        self.worker = NotificationWorker(config, self.store, provider=provider) if config.enabled else None
        self._scan, self._current = scan, current
        self._task = None
        self._stop = asyncio.Event()
        self._tick_lock = asyncio.Lock()
        self.last_error_code = None

    async def start(self, *, background=True):
        if not self.config.enabled or not self.config.pending_decisions:
            return
        try:
            await asyncio.to_thread(self.store.initialize)
        except Exception:
            # SMTP/outbox failure never makes an execution graph unavailable.
            self.last_error_code = "NOTIFICATION_STORE_UNAVAILABLE"
        if background:
            self._task = asyncio.create_task(self._loop(), name="human-decision-email")

    async def reconcile_once(self):
        if not self.store or not self.config.pending_decisions:
            return 0
        try:
            await asyncio.to_thread(self.store.initialize)
            snapshots = await self._scan()
            for snapshot in snapshots:
                await asyncio.to_thread(self.store.enqueue, snapshot)
            self.last_error_code = None
            return len(snapshots)
        except Exception:
            # Never persist raw provider/credential-bearing exception messages.
            self.last_error_code = "NOTIFICATION_RECONCILIATION_FAILED"
            return 0

    async def tick_once(self):
        if not self.worker or not self.config.pending_decisions:
            return None
        async with self._tick_lock:
            await self.reconcile_once()
            try:
                return await asyncio.to_thread(self.worker.run_once, self._current)
            except Exception:
                self.last_error_code = "NOTIFICATION_DELIVERY_UNAVAILABLE"
                return None

    async def _loop(self):
        while not self._stop.is_set():
            await self.tick_once()
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.config.poll_interval_seconds)
            except TimeoutError:
                pass

    async def stop(self):
        self._stop.set()
        if self._task:
            # SMTP has a bounded timeout; do not cancel a thread mid-delivery and
            # pretend it stopped. The durable lease handles an actual process crash.
            await self._task
            self._task = None
