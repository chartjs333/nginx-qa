"""Private setup routes: exact issued snapshots, current grants, typed operations."""
from __future__ import annotations
import asyncio
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import sqlite3
from typing import Any

import jsonschema
from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse, FileResponse
from .legacy_scope_control import ADMIN_TOKEN_HEADER, verify_admin_token
from .setup_diagnostics import (DiagnosticSnapshotCollector, DiagnosticReadGate,
    parse_strict_json_with_limits, canonical_json_bytes, ACTION_IDS, _PROJECT_ID_PATTERN, _OPAQUE_ID_PATTERN,
    open_readonly_snapshot, get_contracts_schema, DiagnosticSourceUnavailable)
from .setup_operations import SetupOperationsManager, SetupApplyNotStarted
from .setup_worker_adapter import SetupWorkerAdapter
from .setup_advisor import SetupAdvisor, AdvisorBusyError
from .setup_policy import SetupPolicy
from .setup_native import NativeSelfTest


def register_setup_diagnostics_api(host: Any) -> None:
    app = host.app
    gate = DiagnosticReadGate(max_cache_entries=32, default_ttl=5.0)
    state_raw = getattr(host, "state_base", None) or os.environ.get("NGINX_QA_STAGING_STATE_BASE")
    state = Path(state_raw) if state_raw and Path(state_raw).is_absolute() else None
    policy_raw = getattr(host, "setup_policy_path", None) or os.environ.get("NGINX_QA_SETUP_POLICY_PATH")
    policy = SetupPolicy(Path(policy_raw) if policy_raw else (state / "setup-policy.json" if state else None))
    def registry_path():
        # staging_host_app validates and rebinds legacy mutable paths after
        # importing main (and registering routes). Resolve the trusted host
        # binding when handling a request, never capture the checkout fallback.
        return Path(getattr(host, "git_config_path", "port_git_map.json"))
    code_root = Path(getattr(host, "service_root", None) or Path(__file__).resolve().parents[1])
    try:
        runtime_config = host.load_managed_runtime_config()
        store_path = Path(runtime_config["lease_root"]) / "managed-import.sqlite3"
    except (AttributeError, ValueError, KeyError, TypeError):
        store_path = None
    if getattr(host, "setup_store_path", None) is not None:
        store_path = Path(host.setup_store_path)  # injection for isolated hosts/tests
    ledger = state / "setup" if state else None
    worker = SetupWorkerAdapter(durable_store_dir=ledger / "worker" if ledger else None)
    advisor = SetupAdvisor(state_dir=ledger / "advisor" if ledger else None)
    assets = Path(__file__).parent / "static"

    def registry():
        git_path = registry_path()
        if not git_path.exists():
            return {}
        data = parse_strict_json_with_limits(git_path.read_bytes())
        if not isinstance(data, dict):
            raise ValueError("REGISTRY_INVALID")
        return data

    def entries():
        from .git_provider import canonical_remote_from_address
        result = {}
        data = registry()
        if not isinstance(data.get("projects", {}), dict):
            raise ValueError("REGISTRY_INVALID")
        for key, row in data.get("projects", {}).items():
            if isinstance(row, dict):
                phone = str(row.get("project_phone") or "")
                if _PROJECT_ID_PATTERN.fullmatch(phone):
                    remote = canonical_remote_from_address(row.get("git_address", ""))
                    context_key = row.get("git_context_key", str(key))
                    if (not remote or not isinstance(context_key, str)
                            or context_key.split("#", 1)[0] != remote or phone in result):
                        raise ValueError("REGISTRY_INVALID")
                    result[phone] = {**row, "context_key": context_key, "git_context_key": context_key}
        # Phone aliases do not create a second canonical project or bypass grants.
        return result

    def project(project_id):
        if not _PROJECT_ID_PATTERN.fullmatch(project_id):
            raise ValueError("INVALID_PROJECT_ID")
        row = entries().get(project_id)
        if row is None:
            raise LookupError("PROJECT_NOT_FOUND")
        return row

    def authorize(actor, project_id, action):
        project(project_id)
        policy.grant(actor, project_id).require(action)
        if (action in {"worker.bind", "selftest.start"} and actor.startswith("operator-")
                and not actor.startswith("operator-named-")):
            raise PermissionError("NAMED_OPERATOR_REQUIRED")

    def binding_fingerprint(actor, project_id):
        grant = policy.grant(actor, project_id)
        return hashlib.sha256(canonical_json_bytes({
            "policy": grant.version, "project": project(project_id), "actor": actor,
            "executor": worker.candidate_configuration("default-executor")})).hexdigest()

    def context(actor, project_id, instance_id=None):
        row = project(project_id)
        grant = policy.grant(actor, project_id)
        allowed = set(ACTION_IDS) & grant.actions
        if actor.startswith("operator-") and not actor.startswith("operator-named-"):
            allowed -= {"worker.bind", "selftest.start"}
        repository_root = row.get("repository_root")
        if repository_root and not Path(repository_root).is_absolute():
            repository_root = None
        try:
            binding = worker.active_binding(actor_id=actor, project_id=project_id,
                                            authority_fingerprint=binding_fingerprint(actor, project_id))
        except (LookupError, ValueError, PermissionError):
            binding = None
        return dict(instance_id=instance_id, code_root=code_root, git_config_path=registry_path(),
            store_path=store_path, state_base=state, repository_root=Path(repository_root) if repository_root else None,
            project_metadata=row, binding_metadata=binding, actor_id=actor,
            policy_generation=grant.generation, authority_version=grant.version,
            authorized_action_ids=sorted(allowed))

    def current_fingerprint(actor, project_id):
        args = context(actor, project_id)
        identity = {key: args.pop(key) for key in ("instance_id", "actor_id", "policy_generation",
                                                    "authority_version", "authorized_action_ids")}
        return DiagnosticSnapshotCollector(**args).collect_snapshot(project_id, **identity)["authority_fingerprint"]

    native = NativeSelfTest(host, policy, registry, worker, store_path, binding_authority=binding_fingerprint)

    def refresh(**scope):
        current_fingerprint(scope["actor_id"], scope["project_id"])
        return {"state": "SUCCEEDED", "evidence_ids": []}

    def guidance(**scope):
        return {"state": "SUCCEEDED", "evidence_ids": ["setup-help-" + scope["arguments"]["topic"]]}

    def native_start(**scope):
        return native.receipt_fields(native.start(**scope))

    def recovery(**scope):
        instance = scope["arguments"]["instance_id"]
        # Recovery is a passive native context link, never a hidden restart.
        if store_path is None or not store_path.is_file():
            raise LookupError("INSTANCE_NOT_FOUND")
        with closing(open_readonly_snapshot(store_path)) as connection:
            row = connection.execute("SELECT record_json FROM telegram_launcher_operations WHERE operation_id = ? AND project_id = ?",
                                     (instance, scope["project_id"])).fetchone()
        if row is None:
            raise LookupError("INSTANCE_NOT_FOUND")
        return {"state": "SUCCEEDED", "evidence_ids": [instance]}

    def git_probe(**scope):
        _, _, repos = native._project(scope["project_id"])
        repo_id = scope["arguments"]["repository_id"]
        if repo_id not in repos:
            raise ValueError("REPOSITORY_NOT_REGISTERED")
        importer = host.managed_sprint_importer_factory(host.load_managed_runtime_config(), repos)
        importer.validate_mutation_roots()
        repo = importer.git_provider.ensure_mirror(repo_id, fetch=True)
        # Remote readability is not proof of push authority.
        return {"state": "SUCCEEDED", "evidence_ids": [
            "git-read-" + hashlib.sha256(repo.canonical_remote.encode()).hexdigest()[:24]]}

    def provider_probe(**scope):
        # Credentials alone are not a provider round-trip. Unconfigured adapters fail explicitly.
        handler = getattr(host, "setup_provider_probe", None)
        if handler is None:
            return {"state": "BLOCKED", "error_code": "PROVIDER_PROBE_NOT_CONFIGURED"}
        return handler(scope["arguments"]["provider_id"])

    def telegram_probe(**scope):
        import httpx
        from .control_secrets import trusted_control_resolver
        ref = os.environ.get("NGINX_QA_LAUNCHER_BOT_SECRET_REF")
        if not ref:
            return {"state": "BLOCKED", "error_code": "NOT_CONFIGURED"}
        token = trusted_control_resolver(ref)
        if not isinstance(token, str) or not token:
            raise ValueError("NOT_CONFIGURED")
        async def bounded_probe():
            async with asyncio.timeout(10):
                async with httpx.AsyncClient(timeout=4, trust_env=False, follow_redirects=False) as client:
                    for method in ("getMe", "getWebhookInfo"):
                        async with client.stream("GET", "https://api.telegram.org/bot" + token + "/" + method,
                                                 headers={"Accept-Encoding": "identity"}) as response:
                            if response.headers.get("content-encoding", "identity") != "identity":
                                raise ValueError("PROBE_COMPRESSED_RESPONSE")
                            raw = bytearray()
                            async for chunk in response.aiter_raw():
                                raw.extend(chunk)
                                if len(raw) > 16384:
                                    raise ValueError("PROBE_RESPONSE_TOO_LARGE")
                            if response.status_code != 200 or json.loads(raw).get("ok") is not True:
                                raise ValueError("TELEGRAM_PROBE_FAILED")
        asyncio.run(bounded_probe())
        return {"state": "SUCCEEDED", "evidence_ids": ["telegram-getme-webhook"]}

    manager = SetupOperationsManager(worker_adapter=worker, durable_store_dir=ledger,
        authorize=authorize, authority=current_fingerprint, binding_authority=binding_fingerprint,
        profile_validator=native.validate_profile, status_reader=native.status,
        file_lock=getattr(host, "interprocess_file_lock", None),
        action_handlers={"diagnostics.refresh": refresh, "setup.open": guidance,
            "selftest.start": native_start, "selftest.stop": native.stop, "git.probe": git_probe,
            "provider.probe": provider_probe, "telegram.probe": telegram_probe, "recovery.open": recovery})
    host.setup_operations_manager, host.setup_advisor = manager, advisor
    host.setup_read_gate, host.setup_native = gate, native

    def authenticated(request):
        origin = request.headers.get("origin")
        if origin and origin != str(request.base_url).rstrip("/"):
            raise HTTPException(403, {"error": "OPERATOR_ORIGIN_FORBIDDEN"})
        authenticate = getattr(host, "scope_api_operator", None)
        if callable(authenticate):
            return authenticate(request)
        token = request.headers.get(ADMIN_TOKEN_HEADER)
        if token:
            try:
                verify_admin_token(token)
                return "credentialed_operator"
            except Exception:
                pass
        raise HTTPException(401, {"error": "OPERATOR_SESSION_REQUIRED"})

    def handle(exc):
        if isinstance(exc, HTTPException):
            raise exc
        if isinstance(exc, SetupApplyNotStarted):
            raise HTTPException(409, {"error": "PLAN_STALE", "apply_outcome": "NOT_STARTED",
                "plan_id": exc.plan_id, "plan_digest": exc.plan_digest, "receipt": exc.receipt}) from None
        if isinstance(exc, PermissionError):
            raise HTTPException(403, {"error": "SETUP_ACCESS_DENIED"}) from None
        if isinstance(exc, LookupError):
            raise HTTPException(409, {"error": "SETUP_CONTEXT_STALE"}) from None
        if isinstance(exc, AdvisorBusyError):
            raise HTTPException(429, {"error": "ADVISOR_BUSY"}) from None
        if isinstance(exc, DiagnosticSourceUnavailable):
            raise HTTPException(429 if exc.error_code == "SOURCE_BUSY" else 409, {"error": exc.error_code}) from None
        if isinstance(exc, (ValueError, jsonschema.ValidationError)):
            code = str(exc).split(":")[0]
            if not code.isupper() or len(code) > 80:
                code = "INVALID_SETUP_REQUEST"
            raise HTTPException(409 if code in {"PLAN_STALE", "IDEMPOTENCY_CONFLICT"} else 400, {"error": code}) from None
        if isinstance(exc, RuntimeError):
            raise HTTPException(429, {"error": "DIAGNOSTIC_CAPTURE_UNAVAILABLE"}) from None
        raise exc

    async def body(request):
        raw = bytearray()
        async for chunk in request.stream():
            raw.extend(chunk)
            if len(raw) > 65536:
                raise HTTPException(413, {"error": "SETUP_REQUEST_TOO_LARGE"})
        data = parse_strict_json_with_limits(bytes(raw))
        if not isinstance(data, dict):
            raise ValueError("INVALID_SETUP_REQUEST")
        return data

    def response(data):
        return JSONResponse(data, headers={"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"})

    def issued(actor, project_id, snapshot_id):
        if not isinstance(snapshot_id, str) or len(snapshot_id) != 64:
            raise ValueError("INVALID_SNAPSHOT_ID")
        args = context(actor, project_id)
        return gate.get_issued_snapshot(snapshot_id, project_id, actor_id=actor,
            policy_generation=args["policy_generation"], authority_version=args["authority_version"])

    from .task_wizard import register_task_wizard
    register_task_wizard(host, state, registry, authenticated)

    @app.get("/setup")
    async def setup_page():
        return FileResponse(assets / "setup.html", headers={"Cache-Control": "no-store",
            "Content-Security-Policy": "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'",
            "X-Content-Type-Options": "nosniff"})

    @app.get("/setup/assets/{name}")
    async def setup_asset(name: str):
        if name not in {"setup.js", "setup.css"}:
            raise HTTPException(404)
        return FileResponse(assets / name, media_type="application/javascript" if name.endswith(".js") else "text/css")

    @app.get("/api/v1/setup/projects")
    async def projects(request: Request):
        try:
            actor = authenticated(request)
            allowed = []
            for phone, entry in entries().items():
                try:
                    policy.grant(actor, phone)
                except PermissionError:
                    continue
                allowed.append({"project_id": phone, "canonical_phone": phone, "context_key": entry["context_key"],
                                "name": str(entry.get("project_name") or "Project " + phone)})
            return response({"schema_version": 1, "kind": "setup_canonical_projects", "projects": sorted(allowed, key=lambda p: p["project_id"])})
        except Exception as exc:
            handle(exc)

    @app.get("/api/v1/projects/{project_id}/setup/snapshot")
    async def snapshot(project_id: str, request: Request, instance_id: str | None = None):
        try:
            actor = authenticated(request)
            if instance_id is not None and not _OPAQUE_ID_PATTERN.fullmatch(instance_id):
                raise ValueError("INVALID_INSTANCE_ID")
            value = await gate.get_snapshot(project_id, **context(actor, project_id, instance_id))
            authorize(actor, project_id, "read")
            return response(value)
        except Exception as exc:
            handle(exc)

    @app.post("/api/v1/projects/{project_id}/setup/plans")
    async def plan(project_id: str, request: Request):
        try:
            actor = authenticated(request)
            data = await body(request)
            schema = get_contracts_schema()
            jsonschema.Draft202012Validator({"$ref": "#/$defs/PlanRequest", "$defs": schema["$defs"]}).validate(data)
            snapshot = issued(actor, project_id, data.get("snapshot_id"))
            value = await asyncio.to_thread(manager.create_plan, actor, project_id, data, snapshot=snapshot)
            authorize(actor, project_id, data["action_id"])
            return response(value)
        except Exception as exc:
            handle(exc)

    @app.post("/api/v1/projects/{project_id}/setup/plans/{plan_id}/apply")
    async def apply(project_id: str, plan_id: str, request: Request):
        try:
            actor = authenticated(request)
            authorize(actor, project_id, "read")
            value = await manager.apply_plan(actor, project_id, await body(request), plan_id=plan_id)
            authorize(actor, project_id, "read")
            gate.invalidate(project_id, actor_id=actor)
            return response(value)
        except SetupApplyNotStarted as exc:
            # The negative receipt is durable, but disclosure still requires
            # current read authority, just like a successful Apply response.
            try:
                authorize(actor, project_id, "read")
            except Exception as denied:
                handle(denied)
            handle(exc)
        except Exception as exc:
            handle(exc)

    @app.post("/api/v1/projects/{project_id}/setup/plans/{plan_id}/inspect")
    async def inspect_apply(project_id: str, plan_id: str, request: Request):
        try:
            actor = authenticated(request)
            authorize(actor, project_id, "read")
            value = await asyncio.to_thread(manager.inspect_apply, actor, project_id,
                await body(request), plan_id=plan_id)
            authorize(actor, project_id, "read")
            return response(value)
        except Exception as exc:
            handle(exc)

    @app.get("/api/v1/projects/{project_id}/setup/operations")
    async def operation_list(project_id: str, request: Request):
        try:
            actor = authenticated(request)
            authorize(actor, project_id, "read")
            values = await asyncio.to_thread(manager.list_operations, project_id, actor_id=actor)
            authorize(actor, project_id, "read")
            return response({"schema_version": 1, "kind": "setup_operation_list", "projection": "recorded", "operations": values})
        except Exception as exc:
            handle(exc)

    @app.get("/api/v1/projects/{project_id}/setup/operations/{operation_id}")
    async def operation(project_id: str, operation_id: str, request: Request):
        try:
            actor = authenticated(request)
            authorize(actor, project_id, "read")
            return response(await asyncio.to_thread(manager.get_operation, project_id, operation_id, actor_id=actor))
        except Exception as exc:
            handle(exc)

    @app.post("/api/v1/projects/{project_id}/setup/advice")
    async def advice(project_id: str, request: Request):
        try:
            actor = authenticated(request)
            authorize(actor, project_id, "advise")
            data = await body(request)
            schema = get_contracts_schema()
            jsonschema.Draft202012Validator({"$ref": "#/$defs/AdviceRequest", "$defs": schema["$defs"]}).validate(data)
            snapshot = issued(actor, project_id, data.get("snapshot_id"))
            result = await advisor.get_advice(snapshot, data, actor_id=actor, project_id=project_id)
            authorize(actor, project_id, "advise")
            return response(result)
        except Exception as exc:
            handle(exc)
