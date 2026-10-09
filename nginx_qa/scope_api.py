"""Human scope workflow HTTP adapter. No execution commands in read routes."""
from __future__ import annotations

import asyncio
from copy import deepcopy
import hashlib
import hmac
from pathlib import Path
import secrets
import time

from fastapi import HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse

from . import scope_workflow as flow
from . import scope_runtime_adapters as adapters
from .execution_observability import project_execution
from .decision_notifications import EmailConfig, NotificationError, binding_matches, is_expired, known_secret_values
from .decision_notification_runtime import DecisionNotificationService, notification_snapshot
from .legacy_scope_control import (
    ADMIN_TOKEN_HEADER, ROLE_TOKEN_HEADER, LegacyScopeControlError,
    assignment_binding, require_role_tokens_configured, verify_admin_token,
    verify_role_token, canonical_json_sha256, active_amendment,
)


def register_scope_api(host):
    app = host.app
    sessions: dict[str, dict] = {}
    cookie_name = "nginx_qa_operator"

    def error(exc):
        if isinstance(exc, (flow.ScopeDecisionError, LegacyScopeControlError, NotificationError)):
            raise HTTPException(exc.status_code, {"error": exc.code}) from exc
        raise exc

    def same_origin(request):
        origin = request.headers.get("origin")
        if origin and origin != str(request.base_url).rstrip("/"):
            raise HTTPException(403, {"error": "OPERATOR_ORIGIN_FORBIDDEN"})

    def session(request):
        now = time.monotonic()
        for key, value in list(sessions.items()):
            if value["expires"] < now:
                sessions.pop(key, None)
        return sessions.get(request.cookies.get(cookie_name, ""))

    def operator(request):
        same_origin(request)
        token = request.headers.get(ADMIN_TOKEN_HEADER)
        if token:
            verify_admin_token(token)
            return "credentialed_operator"
        current = session(request)
        if not current or not current["authenticated"]:
            raise HTTPException(401, {"error": "OPERATOR_SESSION_REQUIRED"})
        if not hmac.compare_digest(current["csrf_token"], request.headers.get("X-Nginx-QA-CSRF", "")):
            raise HTTPException(403, {"error": "OPERATOR_CSRF_REQUIRED"})
        return current["actor"]

    async def body(request):
        same_origin(request)
        data = await request.body()
        if len(data) > 256 * 1024:
            raise HTTPException(413, {"error": "SCOPE_REQUEST_TOO_LARGE"})
        # Reject known credentials BEFORE any durable event or exception detail.
        service = getattr(app.state, "decision_notifications", None)
        notification_credentials = tuple(value for value in
            (getattr(getattr(service, "config", None), "smtp_username", ""),
             getattr(getattr(service, "config", None), "smtp_password", "")) if value)
        credential_values = (*known_secret_values(), *notification_credentials)
        for value in credential_values:
            if value.encode() in data:
                raise HTTPException(400, {"error": "CREDENTIAL_IN_REQUEST_FORBIDDEN"})
        try:
            parsed = host.parse_strict_json_object(data)
        except ValueError as exc:
            raise HTTPException(400, {"error": "SCOPE_REQUEST_JSON_INVALID"}) from exc
        def strings(value):
            if isinstance(value, str):
                yield value
            elif isinstance(value, dict):
                for key, item in value.items():
                    yield str(key)
                    yield from strings(item)
            elif isinstance(value, list):
                for item in value:
                    yield from strings(item)
        decoded_strings = list(strings(parsed))
        for value in credential_values:
            if any(value in item for item in decoded_strings):
                raise HTTPException(400, {"error": "CREDENTIAL_IN_REQUEST_FORBIDDEN"})
        return parsed

    @app.get("/api/v1/operator/session")
    async def operator_session(request: Request):
        same_origin(request)
        current = session(request)
        new_cookie = None
        if current is None:
            if len(sessions) >= 128:
                raise HTTPException(429, {"error": "OPERATOR_SESSION_LIMIT"})
            new_cookie = secrets.token_hex(32)
            current = {"authenticated": False, "csrf_token": secrets.token_hex(32),
                       "pairing_code": secrets.token_hex(5).upper(),
                       "expires": time.monotonic() + 600, "actor": "operator-" + secrets.token_hex(8)}
            sessions[new_cookie] = current
        response = JSONResponse({"authenticated": current["authenticated"],
                                 "csrf_token": current["csrf_token"] if current["authenticated"] else None,
                                 "pairing_code": current["pairing_code"] if not current["authenticated"] else None})
        response.headers["Cache-Control"] = "no-store"
        if new_cookie:
            response.set_cookie(cookie_name, new_cookie, httponly=True, samesite="strict",
                                secure=request.url.scheme == "https", max_age=3600, path="/")
        return response

    @app.post("/api/v1/operator/session/authorize")
    async def authorize_operator_session(request: Request):
        try:
            # Pairing code is NOT a credential: only this admin-authenticated
            # request can approve it; browser possession of HttpOnly cookie is
            # separately required. The administrative secret stays in DPAPI.
            same_origin(request)
            verify_admin_token(request.headers.get(ADMIN_TOKEN_HEADER))
            data = await body(request)
            session(request)  # expire old entries
            found = next((v for v in sessions.values() if v["pairing_code"] == data.get("pairing_code") and not v["authenticated"]), None)
            if not found:
                raise HTTPException(409, {"error": "OPERATOR_PAIRING_STALE"})
            found.update(authenticated=True, expires=time.monotonic() + 3600)
            return {"authorized": True, "credentials_returned": False}
        except (flow.ScopeDecisionError, LegacyScopeControlError) as exc:
            error(exc)

    @app.delete("/api/v1/operator/session")
    async def logout(request: Request):
        operator(request)
        sessions.pop(request.cookies.get(cookie_name, ""), None)
        response = JSONResponse({"authenticated": False})
        response.delete_cookie(cookie_name, path="/")
        return response

    def project_entry(project_id):
        with host.git_config_file_lock():
            config = host.read_git_config_file()
            _, context_key, entry, _ = host.project_for_group_api(config, project_id)
            return deepcopy(entry), context_key

    def managed(project_id, sprint_id):
        # Never instantiate a runtime from a GET: construction/reconciliation
        # may change state. Lifespan owns the configured runtime.
        runtime = host.managed_continuity_runtime
        if runtime is None:
            return None
        states = runtime.store.runtime_states_for_project(project_id)
        return runtime if any(str(s.get("sprint_id")) == sprint_id for s in states) else None

    def verify_source(entry, raw_source):
        # Executors identify an immutable Git document; the orchestrator resolves
        # its hash. A supplied hash is an optional additional expected-value gate.
        candidate = deepcopy(raw_source)
        expected_hash = candidate.get("sha256") if isinstance(candidate, dict) else None
        if isinstance(candidate, dict) and "sha256" not in candidate:
            candidate["sha256"] = "0" * 64
        source = flow.validate_source(candidate)
        remote = host.canonical_remote_from_address(str(entry.get("git_address") or ""))
        if not remote or source["repository_key"].casefold() != remote.casefold():
            raise LegacyScopeControlError("SCOPE_REPOSITORY_MISMATCH", "Source must belong to this project")
        repo, _ = host.legacy_scope_source_repository(str(entry.get("git_address") or ""), remote)
        commit = host.legacy_scope_resolve_commit(repo, source["commit"])
        if commit != source["commit"]:
            raise LegacyScopeControlError("SCOPE_SOURCE_COMMIT_MISMATCH", "Exact commit required")
        blob = host.legacy_scope_git_command(repo, "show", f"{commit}:{source['path']}")
        actual_hash = hashlib.sha256(blob).hexdigest()
        if len(blob) > 4 * 1024 * 1024 or (expected_hash is not None and not hmac.compare_digest(actual_hash, expected_hash)):
            raise LegacyScopeControlError("SCOPE_SOURCE_HASH_MISMATCH", "Source content does not match")
        source["sha256"] = actual_hash
        return source

    def identity_for(state, assignment_id):
        item = adapters.assignment_identity(state, assignment_id)
        if item.get("status") != "active":
            raise LegacyScopeControlError("SCOPE_ASSIGNMENT_NOT_ACTIVE", "An active assignment is required")
        if (item.get("scope_handoff_intent") or {}).get("status") == "publishing":
            raise LegacyScopeControlError("SCOPE_HANDOFF_IN_PROGRESS", "Finish or retry the already authorized ordinary handoff before changing scope")
        if adapters.runtime_kind(state) != "managed_workspace_v1":
            if state.get("mode") == "parallel":
                if item.get("delivery_status") != "delivered" or not item.get("issued_task"):
                    raise LegacyScopeControlError("SCOPE_ASSIGNMENT_NOT_DELIVERED", "Only a recorded ordinary delivery can be amended")
            else:
                host.legacy_scope_require_assignment_delivery(state, assignment_id)
            if state.get("mode") != "parallel" and state.get("current_assignment_id") != assignment_id:
                raise LegacyScopeControlError("SCOPE_ASSIGNMENT_NOT_CURRENT", "Assignment changed")
        return {key: deepcopy(item.get(key)) for key in (
            "assignment_id", "agent_id", "agent_phone", "node_id", "phase", "occurrence",
            "occurrence_id", "source_assignment_id",
        )}

    def operation(state, agents, entry, action, payload, request_id, now, role_token, actor, notification=None):
        if action == "create":
            assignment_id = str(payload.get("assignment_id") or "")
        else:
            record = next((r for r in (state.get("scope_workflow") or {}).get("requests", []) if r.get("request_id") == request_id), None)
            if record is None:
                raise flow.ScopeDecisionError("SCOPE_REQUEST_NOT_FOUND", status_code=404)
            assignment_id = record["assignment_id"]
        ledger = state.get("scope_workflow") or {}
        if action == "decide" and any(item.get("idempotency_key") == payload.get("idempotency_key") for item in [*ledger.get("decisions", []), *ledger.get("attempts", [])]):
            # Exact historical replay precedes current delivery/CAS. The pure
            # ledger still rejects reuse of a key for different request bytes.
            return flow.decide_request(state, request_id, payload, record["identity"], now,
                lambda *_: (_ for _ in ()).throw(RuntimeError("Replay cannot apply")), actor=actor)
        if notification is not None:
            # Email and ordinary UI use exactly this same authoritative lock,
            # ledger and CAS. No email-only approval state is written.
            current_request = next((r for r in flow.request_list(state)["requests"] if r["request_id"] == request_id), None)
            current_binding = notification_snapshot(notification["project_id"], notification["sprint_id"], state, current_request) if current_request else None
            binding_error = None
            if is_expired(notification):
                binding_error = flow.ScopeDecisionError("NOTIFICATION_LINK_EXPIRED", status_code=410)
            elif current_binding is None or not binding_matches(notification, current_binding):
                binding_error = flow.ScopeDecisionError("NOTIFICATION_STALE")
            if binding_error is not None:
                if action == "decide":
                    def fail_binding():
                        raise binding_error
                    return flow.decide_request(state, request_id, payload, record["identity"], now,
                        lambda *_: (_ for _ in ()).throw(RuntimeError("Failed binding cannot apply")),
                        actor=actor, precondition=fail_binding)
                raise binding_error
            if action == "validate" and "proposal" in payload and payload["proposal"] != record["proposal"]:
                raise flow.ScopeDecisionError("NOTIFICATION_EDIT_FORBIDDEN", status_code=400)
        if action == "create":
            prior = next((item for item in ledger.get("requests", []) if item.get("idempotency_key") == payload.get("idempotency_key")), None)
            if prior:
                verify_role_token(prior["identity"]["agent_phone"], role_token)
                payload = deepcopy(payload)
                # Replay uses the already verified immutable source, without
                # requiring the Git checkout to remain available after handoff.
                # All supplied reference fields remain part of the digest.
                payload["source"].setdefault("sha256", prior["source"]["sha256"])
                return flow.create_request(state, payload, prior["identity"], now)
        identity = identity_for(state, assignment_id)
        runtime_agents = adapters.runtime_agents(state, agents, adapters.assignment_identity(state, assignment_id))

        def effective_view(candidate, proposal):
            bound = assignment_binding(candidate, assignment_id)
            normalized, actors = adapters.normalized_scope_snapshot(candidate, agents, assignment_id)
            node_agents = {str(n.get("id")): str(n.get("agent_id")) for n in (normalized.get("workflow") or {}).get("nodes", [])}
            by_id = {a.get("id"): a for a in actors}
            amendment = active_amendment(candidate.get("scope_control") or {}) or {}
            def core(actor_id, rule=None):
                actor = (rule or {}).get("effective") or by_id.get(actor_id, {})
                return {"profile": deepcopy(actor.get("profile")), "tasks": deepcopy(actor.get("tasks") or [])}
            return {
                "assignment": deepcopy(bound["effective_core"]) if bound else core(identity["agent_id"]),
                "nodes": {node: core(node_agents.get(node, node), (amendment.get("node_overrides") or {}).get(node)) for node in proposal["node_ids"]},
                "reviewers": {reviewer: core(reviewer, (amendment.get("reviewer_overrides") or {}).get(reviewer)) for reviewer in proposal["reviewer_ids"]},
            }

        def apply(candidate, proposal, source, authorization):
            verify_source(entry, source)
            before_view = effective_view(candidate, proposal)
            normalized, targets = adapters.normalized_scope_snapshot(candidate, agents, assignment_id)
            nodes = {str(node.get("id")): str(node.get("agent_id")) for node in (normalized.get("workflow") or {}).get("nodes", [])}
            target_ids = {nodes.get(node_id, node_id) for node_id in proposal["node_ids"]} | set(proposal["reviewer_ids"])
            phones = [str(a.get("phone") or "") for a in targets if a.get("id") in target_ids]
            require_role_tokens_configured(phones)
            amendment_id = "scope-human-" + canonical_json_sha256({"authorization": authorization, "proposal": proposal, "source": source})[:32]
            changed, receipt = adapters.apply_scope_revision(candidate, runtime_agents,
                amendment_id=amendment_id, proposal=proposal, source=source, authorization=authorization,
                expected_scope_revision=flow.scope_revision(candidate), applied_at=now,
                assignment_id=assignment_id)
            if adapters.runtime_kind(candidate) != "managed_workspace_v1":
                adapters.advance_revision(changed)
                changed["updated_at"] = now
            receipt["before_effective_sha256"] = canonical_json_sha256(before_view)
            receipt["after_effective_sha256"] = canonical_json_sha256(effective_view(changed, proposal))
            return changed, receipt

        def preview(candidate, proposal, source, authorization):
            before = effective_view(candidate, proposal)
            changed, _ = apply(candidate, proposal, source, authorization)
            return before, effective_view(changed, proposal)

        if action == "create":
            verify_role_token(identity["agent_phone"], role_token)
            payload = deepcopy(payload)
            payload["source"] = verify_source(entry, payload.get("source"))
            proposal = flow.validate_proposal(payload.get("proposal"))
            preview(state, proposal, payload["source"], {"request_id": "validation-only"})
            return flow.create_request(state, payload, identity, now)
        if action == "validate":
            return flow.validate_request(state, request_id, payload, identity, now, preview)
        return flow.decide_request(state, request_id, payload, identity, now, apply, actor=actor)

    def legacy_mutation(project_id, sprint_id, action, payload, request_id, role_token, actor, notification=None):
        with host.git_config_file_lock():
            with host.agents_file_lock():
                config = host.read_git_config_file()
                key, context, entry, _ = host.project_for_group_api(config, project_id)
                history = host.read_sprint_history_file().get("projects", {}).get(context, {})
                if history.get("current_sprint_id") != sprint_id:
                    raise flow.ScopeDecisionError("SCOPE_SPRINT_CONFLICT")
                state = deepcopy(entry.get(host.PROJECT_AGENT_ASSIGNMENT_KEY) or {})
                agents = host.full_agents_for_project(host.read_agents_file(), context, host.phone_git_contexts_from_config(config))
                changed, response = operation(state, agents, entry, action, payload, request_id, host.utc_now(), role_token, actor, notification)
                if changed != state:
                    entry[host.PROJECT_AGENT_ASSIGNMENT_KEY] = changed
                    config[host.PROJECTS_KEY][key] = entry
                    host.write_git_config_file(config)
                return response

    async def mutate(project_id, sprint_id, action, request, request_id="", notification=None):
        try:
            actor = operator(request) if action != "create" else "executor"
            payload = await body(request)
            if notification is not None and action == "decide" and payload.get("action") not in {"approve", "reject"}:
                raise flow.ScopeDecisionError("NOTIFICATION_ACTION_FORBIDDEN", status_code=400)
            if action in {"create", "decide"}:
                payload = host.validate_legacy_scope_schema(payload, "scope-change-request-v1.schema.json" if action == "create" else "scope-human-decision-v1.schema.json")
            token = request.headers.get(ROLE_TOKEN_HEADER, "")
            async with host.group_task_submission_lock:
                runtime = await asyncio.to_thread(managed, project_id, sprint_id)
                if runtime:
                    entry, _ = await asyncio.to_thread(project_entry, project_id)
                    def update(state):
                        return operation(state, [], entry, action, payload, request_id, host.utc_now(), token, actor, notification)
                    _, response = await asyncio.to_thread(runtime.scope_mutate, project_id, sprint_id, update)
                else:
                    response = await asyncio.to_thread(legacy_mutation, project_id, sprint_id, action, payload, request_id, token, actor, notification)
            # Reconciliation is performed by the separate notification worker.
            # A delivery/store failure must never turn a committed decision into
            # a reported execution failure; restart recovers the durable request.
            if response.get("error"):
                return JSONResponse(status_code=response["status_code"], content={"detail": response})
            return response
        except (flow.ScopeDecisionError, LegacyScopeControlError) as exc:
            error(exc)

    base = "/api/v1/projects/{project_id}/sprints/{sprint_id}"

    async def managed_effective(project_id, assignment_id, request, *, acknowledge=False):
        runtime = host.managed_continuity_runtime
        states = await asyncio.to_thread(runtime.store.runtime_states_for_project, project_id) if runtime else []
        found = next((s for s in states if any(a.get("assignment_id") == assignment_id for a in adapters.assignment_records(s))), None)
        if found is None:
            # Parallel assignments have no single 'current' pointer. They are
            # addressed by the actual ordinary queue delivery identity.
            def legacy_parallel():
                with host.git_config_file_lock():
                    config = host.read_git_config_file()
                    _, _, entry, _ = host.project_for_group_api(config, project_id)
                    candidate = entry.get(host.PROJECT_AGENT_ASSIGNMENT_KEY) or {}
                    return deepcopy(candidate) if candidate.get("mode") == "parallel" else None
            found = await asyncio.to_thread(legacy_parallel)
            if found is None:
                return None
            runtime = None
        try:
            identity = adapters.assignment_identity(found, assignment_id)
            verify_role_token(identity.get("agent_phone"), request.headers.get(ROLE_TOKEN_HEADER))
            if not acknowledge:
                result = adapters.effective_scope_snapshot(found, assignment_id)
                result.update(project_id=project_id, assignment_id=assignment_id,
                    acknowledged=not result["requires_scope_ack"],
                    precedence={"authoritative": "effective", "historical": "issued",
                                "rule": result["instruction_precedence"]})
                return result
            data = await body(request)
            if set(data) != {"schema_version", "scope_context"} or data["schema_version"] != 1:
                raise flow.ScopeDecisionError("SCOPE_ACK_INVALID", status_code=400)
            def ack(state):
                # Role/identity revalidated inside the same execution lock.
                record = adapters.assignment_identity(state, assignment_id)
                verify_role_token(record.get("agent_phone"), request.headers.get(ROLE_TOKEN_HEADER))
                return adapters.acknowledge_scope(state, assignment_id, data["scope_context"], host.utc_now())
            if runtime:
                _, result = await asyncio.to_thread(runtime.scope_mutate, project_id, found["sprint_id"], ack)
            else:
                def parallel_ack():
                    with host.git_config_file_lock():
                        config = host.read_git_config_file()
                        key, _, entry, _ = host.project_for_group_api(config, project_id)
                        current = deepcopy(entry.get(host.PROJECT_AGENT_ASSIGNMENT_KEY) or {})
                        identity_for(current, assignment_id)
                        changed, response = ack(current)
                        if changed != current:
                            entry[host.PROJECT_AGENT_ASSIGNMENT_KEY] = changed
                            config[host.PROJECTS_KEY][key] = entry
                            host.write_git_config_file(config)
                        return response
                async with host.group_task_submission_lock:
                    result = await asyncio.to_thread(parallel_ack)
            return result
        except (flow.ScopeDecisionError, LegacyScopeControlError) as exc:
            error(exc)

    host.scope_api_managed_effective = managed_effective

    @app.get("/api/v1/projects/{project_id}/assignments/{assignment_id}/scope-request-context")
    async def scope_request_context(project_id: str, assignment_id: str, request: Request):
        try:
            runtime = host.managed_continuity_runtime
            states = await asyncio.to_thread(runtime.store.runtime_states_for_project, project_id) if runtime else []
            state = next((s for s in states if any(a.get("assignment_id") == assignment_id for a in adapters.assignment_records(s))), None)
            if state is not None:
                sprint_id = state["sprint_id"]
            else:
                _, _, history = await host.project_sprint_history_snapshot(project_id)
                snap = await host.run_group_write_transaction(host.sequential_runtime_project_snapshot_transaction, project_id)
                state, sprint_id = snap["assignment"], (history or {}).get("current_sprint_id")
            identity = identity_for(state, assignment_id)
            verify_role_token(identity.get("agent_phone"), request.headers.get(ROLE_TOKEN_HEADER))
            if not sprint_id:
                raise flow.ScopeDecisionError("SCOPE_SPRINT_NOT_BOUND")
            return {"project_id": project_id, "sprint_id": sprint_id, "identity": identity,
                    "expected_execution_revision": flow.execution_revision(state),
                    "expected_scope_revision": flow.scope_revision(state),
                    "endpoint": f"/api/v1/projects/{project_id}/sprints/{sprint_id}/scope-requests",
                    "schema": "scope-change-request-v1.schema.json", "mutated": False,
                    "authorization": "Role token creates requests only; an operator records the decision."}
        except (flow.ScopeDecisionError, LegacyScopeControlError) as exc:
            error(exc)

    @app.post(base + "/scope-requests")
    async def create_scope_request(project_id: str, sprint_id: str, request: Request):
        return await mutate(project_id, sprint_id, "create", request)

    @app.post(base + "/scope-requests/{request_id}/validate")
    async def validate_scope_request(project_id: str, sprint_id: str, request_id: str, request: Request):
        return await mutate(project_id, sprint_id, "validate", request, request_id)

    @app.post(base + "/scope-requests/{request_id}/decisions")
    async def decide_scope_request(project_id: str, sprint_id: str, request_id: str, request: Request):
        return await mutate(project_id, sprint_id, "decide", request, request_id)

    async def read_snapshot(project_id, sprint_id):
        runtime = await asyncio.to_thread(managed, project_id, sprint_id)
        if runtime:
            state = await asyncio.to_thread(runtime.scope_snapshot, project_id, sprint_id)
            return {"project_id": project_id, "sprint_id": sprint_id, "execution": state}, state
        _, _, history = await host.project_sprint_history_snapshot(project_id)
        if history and history.get("current_sprint_id") == sprint_id:
            snapshot = await host.project_state_json(project_id, include_patch_content=False)
            snapshot["sprint_id"] = sprint_id
            return snapshot, snapshot["execution"]
        record = next((r for r in (history or {}).get("sprints", []) if r.get("id") == sprint_id), None)
        if record is None:
            raise HTTPException(404, {"error": "SPRINT_NOT_FOUND"})
        snapshot = deepcopy(record.get("final_state") or record.get("snapshot") or record.get("state") or {})
        if not snapshot:
            raise HTTPException(409, {"error": "HISTORICAL_SNAPSHOT_UNAVAILABLE"})
        snapshot.update(project_id=project_id, sprint_id=sprint_id)
        return snapshot, snapshot.get("execution", snapshot)

    def scan_notification_snapshots():
        """Read authoritative stores; notification worker is the only caller."""
        result = []
        with host.git_config_file_lock():
            config = host.read_git_config_file()
            histories = host.read_sprint_history_file().get("projects", {})
            for key, entry in config.get(host.PROJECTS_KEY, {}).items():
                project_id = str(entry.get("project_phone") or "")
                sprint_id = str(histories.get(key, {}).get("current_sprint_id") or "")
                state = entry.get(host.PROJECT_AGENT_ASSIGNMENT_KEY) or {}
                if project_id and sprint_id:
                    for record in flow.request_list(state)["requests"]:
                        snapshot = notification_snapshot(project_id, sprint_id, state, record,
                            project_name=entry.get("project_name") or project_id)
                        if snapshot:
                            result.append(snapshot)
        runtime = host.managed_continuity_runtime
        if runtime is not None:
            # active_runtime_states() is a startup reconciliation command, NOT
            # a read. Use the already initialized store's project snapshot API.
            projects = {str(entry.get("project_phone")) for entry in config.get(host.PROJECTS_KEY, {}).values() if entry.get("project_phone")}
            for project_id in projects:
                for state in runtime.store.runtime_states_for_project(project_id):
                    if state.get("status") != "active":
                        continue
                    sprint_id = str(state.get("sprint_id") or "")
                    if not sprint_id:
                        continue
                    for record in flow.request_list(state)["requests"]:
                        snapshot = notification_snapshot(project_id, sprint_id, state, record)
                        if snapshot:
                            result.append(snapshot)
        return result

    async def scan_notifications():
        return await asyncio.to_thread(scan_notification_snapshots)

    def notification_current(record):
        # Only a delivery eligibility hint; the POST binding is checked again
        # atomically inside operation(), never trusted from this earlier read.
        return any(binding_matches(record, snapshot) for snapshot in scan_notification_snapshots())

    def configure_notifications(config=None, provider=None):
        configuration_error = None
        if config is None:
            try:
                config = EmailConfig.from_environment()
            except (NotificationError, OSError, ValueError, TypeError):
                # Optional delivery configuration cannot take execution offline.
                # Fail closed for mail, and expose a static operator health code.
                config = EmailConfig()
                configuration_error = "EMAIL_CONFIG_INVALID"
        service = DecisionNotificationService(config,
            scan_notifications, notification_current, provider=provider)
        service.last_error_code = configuration_error
        app.state.decision_notifications = service
        return service

    app.state.configure_decision_notifications = configure_notifications
    app.state.decision_notifications = None

    def notification_service():
        service = app.state.decision_notifications
        if service is None or service.store is None:
            raise HTTPException(503, {"error": "NOTIFICATIONS_UNAVAILABLE"})
        return service

    async def notification_record(notification_id):
        try:
            record = await asyncio.to_thread(notification_service().store.get, notification_id)
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(503, {"error": "NOTIFICATION_STORE_UNAVAILABLE"}) from None
        if record is None:
            raise HTTPException(404, {"error": "NOTIFICATION_NOT_FOUND"})
        return record

    async def bound_notification_view(record):
        try:
            _, state = await read_snapshot(record["project_id"], record["sprint_id"])
        except (HTTPException, KeyError):
            return {"notification": record, "current": False, "stale_reason": "NOTIFICATION_STALE",
                "request": None, "execution_revision": None, "scope_revision": None}
        view = flow.request_list(state)
        original = next((item for item in view["requests"] if item["request_id"] == record["request_id"]), None)
        snapshot = notification_snapshot(record["project_id"], record["sprint_id"], state, original) if original else None
        current = snapshot is not None and binding_matches(record, snapshot)
        reason = "NOTIFICATION_LINK_EXPIRED" if is_expired(record) else None if current else "NOTIFICATION_STALE"
        if original:
            immutable = {key: value for key, value in original.items() if key not in {"status", "decision", "notification"}}
            if canonical_json_sha256(immutable) != record["binding"]["content_sha256"]:
                original = None  # Never present modified bytes as the emailed proposal.
        return {"notification": record, "current": reason is None, "stale_reason": reason,
            "request": original, "execution_revision": view["execution_revision"], "scope_revision": view["scope_revision"]}

    notification_base = "/api/v1/decision-notifications/{notification_id}"

    @app.get(notification_base)
    async def get_decision_notification(notification_id: str):
        # No claim, initialization, access event, retry, validation or decision.
        record = await notification_record(notification_id)
        return JSONResponse(await bound_notification_view(record), headers={"Cache-Control": "no-store", "Referrer-Policy": "no-referrer"})

    @app.post(notification_base + "/validate")
    async def validate_decision_notification(notification_id: str, request: Request):
        record = await notification_record(notification_id)
        return await mutate(record["project_id"], record["sprint_id"], "validate", request, record["request_id"], notification=record)

    @app.post(notification_base + "/decisions")
    async def decide_notification(notification_id: str, request: Request):
        record = await notification_record(notification_id)
        return await mutate(record["project_id"], record["sprint_id"], "decide", request, record["request_id"], notification=record)

    @app.post(notification_base + "/retry")
    async def retry_notification(notification_id: str, request: Request):
        try:
            operator(request)
            payload = await body(request)
            if set(payload) != {"idempotency_key"}:
                raise NotificationError("NOTIFICATION_RETRY_INVALID")
            record = await notification_record(notification_id)
            view = await bound_notification_view(record)
            if not view["current"]:
                raise NotificationError(view["stale_reason"], 409)
            return await asyncio.to_thread(notification_service().store.retry, notification_id,
                idempotency_key=payload["idempotency_key"])
        except (NotificationError, LegacyScopeControlError) as exc:
            error(exc)

    async def notification_metadata(project_id, sprint_id, state):
        service = app.state.decision_notifications
        def unavailable(code):
            return {item["request_id"]: {"status": "unavailable", "error_code": code}
                for item in flow.request_list(state)["requests"] if item["status"] == "pending"}, []
        if service is None:
            return {}, []
        if service.store is None:
            return unavailable(service.last_error_code) if service.last_error_code else ({}, [])
        try:
            records = await asyncio.to_thread(service.store.list_for, adapters.runtime_kind(state), project_id, sprint_id)
            events = []
            for record in records:
                events.extend(await asyncio.to_thread(service.store.events, record["notification_id"]))
            return {record["request_id"]: record for record in records}, events
        except Exception:
            # Delivery metadata is not execution health or a reviewer verdict.
            return unavailable("NOTIFICATION_STORE_UNAVAILABLE")

    @app.get(base + "/scope-requests")
    async def scope_requests(project_id: str, sprint_id: str):
        _, state = await read_snapshot(project_id, sprint_id)
        result = flow.request_list(state)
        metadata, _ = await notification_metadata(project_id, sprint_id, state)
        for record in result["requests"]:
            if record["request_id"] in metadata:
                record["notification"] = metadata[record["request_id"]]
        return result

    @app.get(base + "/observability")
    async def observability(project_id: str, sprint_id: str, cursor: str | None = None,
                            at_revision: int | None = None, at_checkpoint: str | None = None):
        snapshot, state = await read_snapshot(project_id, sprint_id)
        try:
            view = project_execution(snapshot, scope_requests=flow.request_list(state)["requests"],
                checkpoints=(state.get("execution_audit") or {}).get("checkpoints", []),
                cursor=cursor, at_revision=at_revision, at_checkpoint=at_checkpoint)
            if at_revision is None and at_checkpoint is None:
                metadata, events = await notification_metadata(project_id, sprint_id, state)
                for record in [*view["scope_requests"], *view["pending_decisions"]]:
                    if record["request_id"] in metadata:
                        record["notification"] = metadata[record["request_id"]]
                if metadata:
                    view["notification_events"] = events
                    # Notification metadata never enters immutable checkpoints.
                    # Its own cursor makes delivery-only changes visible to UI.
                    view["cursor"] = canonical_json_sha256({"execution": view["cursor"], "notifications": metadata, "events": events})
                    view["unchanged"] = cursor == view["cursor"]
            return view
        except ValueError as exc:
            raise HTTPException(409, {"error": "HISTORICAL_SNAPSHOT_UNAVAILABLE"}) from exc

    @app.get("/api/v1/execution-catalog")
    async def execution_catalog():
        config = await host.read_git_config()
        history = await asyncio.to_thread(host.read_sprint_history_file)
        projects = []
        for key, entry in config.get(host.PROJECTS_KEY, {}).items():
            project_id = str(entry.get("project_phone") or "")
            if not project_id:
                continue
            archive = history.get("projects", {}).get(key, {})
            sprints = [{"sprint_id": r.get("id"), "title": r.get("title"), "status": r.get("status")} for r in archive.get("sprints", [])]
            runtime = host.managed_continuity_runtime
            if runtime:
                states = await asyncio.to_thread(runtime.store.runtime_states_for_project, project_id)
                sprints += [{"sprint_id": s.get("sprint_id"), "title": s.get("sprint_id"), "status": s.get("status")} for s in states]
            projects.append({"project_id": project_id, "name": entry.get("project_name") or project_id, "sprints": sprints})
        return {"projects": projects}

    assets = Path(__file__).parent / "static"

    @app.get("/execution")
    async def execution_page():
        return FileResponse(assets / "execution.html", headers={"Cache-Control": "no-store", "Content-Security-Policy": "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'"})

    @app.get("/execution/assets/{name}")
    async def execution_asset(name: str):
        if name not in {"execution.js", "execution.css"}:
            raise HTTPException(404)
        return FileResponse(assets / name, headers={"Cache-Control": "no-cache"})
