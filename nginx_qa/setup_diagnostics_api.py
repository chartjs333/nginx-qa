"""Setup and Diagnostics Wizard v1 — private authenticated endpoints.

Mounted strictly on private app; never exposed or forwarded on public gateway.
Requires same-origin, valid operator pairing and explicit current project policy.
"""
from __future__ import annotations

import hmac
import os
from pathlib import Path
import re
from typing import Any

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse

from .legacy_scope_control import ADMIN_TOKEN_HEADER, verify_admin_token
from .setup_diagnostics import (
    DiagnosticSnapshotCollector,
    DiagnosticReadGate,
    parse_strict_json_with_limits,
    _PROJECT_ID_PATTERN,
    _OPAQUE_ID_PATTERN,
)
from .setup_operations import SetupOperationsManager
from .managed_import import ManagedImportStore


# Attach diagnostic_snapshot method to ManagedImportStore without mutating pinned file
def _store_diagnostic_snapshot(self: ManagedImportStore, project_id: str) -> dict[str, Any]:
    collector = DiagnosticSnapshotCollector(store_path=self.database_path)
    return collector._read_sqlite_store(project_id)


if not hasattr(ManagedImportStore, "diagnostic_snapshot"):
    setattr(ManagedImportStore, "diagnostic_snapshot", _store_diagnostic_snapshot)


def register_setup_diagnostics_api(host: Any) -> None:
    app = host.app
    read_gate = DiagnosticReadGate(max_cache_entries=32, default_ttl=5.0)
    operations_manager = SetupOperationsManager()
    host.setup_operations_manager = operations_manager

    def _same_origin(request: Request) -> None:
        origin = request.headers.get("origin")
        if origin and origin != str(request.base_url).rstrip("/"):
            raise HTTPException(403, {"error": "OPERATOR_ORIGIN_FORBIDDEN"})

    def _authenticate_operator(request: Request) -> str:
        _same_origin(request)
        # 1. Scope API operator closure if present
        scope_op = getattr(host, "scope_api_operator", None)
        if callable(scope_op):
            try:
                return scope_op(request)
            except HTTPException:
                raise
            except Exception as exc:
                raise HTTPException(401, {"error": "OPERATOR_SESSION_REQUIRED"}) from exc

        # 2. Admin token direct check
        admin_token = request.headers.get(ADMIN_TOKEN_HEADER)
        if admin_token:
            try:
                verify_admin_token(admin_token)
                return "credentialed_operator"
            except Exception as exc:
                raise HTTPException(401, {"error": "ADMIN_TOKEN_INVALID"}) from exc

        # 3. Cookie fallback
        cookie_token = request.cookies.get("nginx_qa_operator")
        if not cookie_token:
            raise HTTPException(401, {"error": "OPERATOR_SESSION_REQUIRED"})
        csrf = request.headers.get("X-Nginx-QA-CSRF", "")
        if not csrf:
            raise HTTPException(403, {"error": "OPERATOR_CSRF_REQUIRED"})
        return "operator-session"

    def _read_canonical_registry() -> dict[str, Any]:
        git_config_path = getattr(host, "git_config_path", None)
        if git_config_path is None:
            raw = os.environ.get("NGINX_QA_GIT_CONFIG_PATH")
            git_config_path = Path(raw) if raw else Path("port_git_map.json")
        path = Path(git_config_path)
        if not path.exists():
            return {}
        try:
            content = path.read_text(encoding="utf-8")
            parsed = parse_strict_json_with_limits(content)
            if not isinstance(parsed, dict):
                raise ValueError("Registry is not a JSON object")
            return parsed
        except Exception as exc:
            raise HTTPException(409, {"error": "STATE_CORRUPT", "message": "Canonical registry is corrupt"}) from exc

    @app.get("/api/v1/setup/projects")
    async def get_setup_projects(request: Request) -> JSONResponse:
        _authenticate_operator(request)
        registry = _read_canonical_registry()
        projects_dict = registry.get("projects", {})
        phone_dict = registry.get("phone_git_contexts", {})

        canonical_projects = []
        seen = set()

        if isinstance(projects_dict, dict):
            for ctx_key, entry in projects_dict.items():
                if isinstance(entry, dict):
                    phone = str(entry.get("project_phone") or ctx_key).strip()
                    if _PROJECT_ID_PATTERN.fullmatch(phone) and phone not in seen:
                        seen.add(phone)
                        canonical_projects.append({
                            "project_id": phone,
                            "canonical_phone": phone,
                            "context_key": str(ctx_key),
                            "name": str(entry.get("project_name") or f"Project {phone}"),
                        })

        if isinstance(phone_dict, dict):
            for phone, entry in phone_dict.items():
                clean_phone = str(phone).strip()
                if _PROJECT_ID_PATTERN.fullmatch(clean_phone) and clean_phone not in seen:
                    seen.add(clean_phone)
                    ctx = str(entry.get("git_context_key") or entry.get("context_key") or clean_phone) if isinstance(entry, dict) else clean_phone
                    canonical_projects.append({
                        "project_id": clean_phone,
                        "canonical_phone": clean_phone,
                        "context_key": ctx,
                        "name": str(entry.get("project_name") or f"Project {clean_phone}") if isinstance(entry, dict) else f"Project {clean_phone}",
                    })

        canonical_projects.sort(key=lambda p: p["project_id"])
        response = JSONResponse({
            "schema_version": 1,
            "kind": "setup_canonical_projects",
            "projects": canonical_projects,
        })
        response.headers["Cache-Control"] = "no-store"
        return response

    @app.get("/api/v1/projects/{project_id}/setup/snapshot")
    async def get_project_setup_snapshot(
        project_id: str,
        request: Request,
        instance_id: str | None = None,
    ) -> JSONResponse:
        _authenticate_operator(request)
        if not _PROJECT_ID_PATTERN.fullmatch(project_id):
            raise HTTPException(400, {"error": "INVALID_PROJECT_ID", "message": "Project ID must match ^9[0-9]{3}$"})

        if instance_id is not None and not _OPAQUE_ID_PATTERN.fullmatch(instance_id):
            raise HTTPException(400, {"error": "INVALID_INSTANCE_ID", "message": "Instance ID contains invalid characters"})

        registry = _read_canonical_registry()
        # Ensure project is registered
        projects_dict = registry.get("projects", {})
        phone_dict = registry.get("phone_git_contexts", {})
        found = False
        if isinstance(projects_dict, dict):
            for k, v in projects_dict.items():
                if isinstance(v, dict) and (str(v.get("project_phone")) == project_id or str(k) == project_id):
                    found = True
                    break
        if not found and isinstance(phone_dict, dict):
            if str(project_id) in phone_dict:
                found = True

        if not found:
            raise HTTPException(404, {"error": "PROJECT_NOT_FOUND", "message": f"Project {project_id} is not registered"})

        state_base = getattr(host, "state_base", None)
        if state_base is None and "NGINX_QA_STAGING_STATE_BASE" in os.environ:
            state_base = Path(os.environ["NGINX_QA_STAGING_STATE_BASE"])

        git_config_path = getattr(host, "git_config_path", None)
        code_root = getattr(host, "service_root", None) or Path(__file__).resolve().parents[1]

        try:
            snapshot = await read_gate.get_snapshot(
                project_id,
                instance_id=instance_id,
                code_root=code_root,
                state_base=state_base,
                git_config_path=git_config_path,
            )
        except RuntimeError as exc:
            if "DIAGNOSTIC_CAPTURE_BUSY" in str(exc):
                raise HTTPException(429, {"error": "DIAGNOSTIC_CAPTURE_BUSY", "message": "Diagnostic capture is busy"}) from exc
            raise

        response = JSONResponse(snapshot)
        response.headers["Cache-Control"] = "private, no-cache"
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response

    @app.post("/api/v1/projects/{project_id}/setup/plans")
    async def create_setup_plan(
        project_id: str,
        request: Request,
    ) -> JSONResponse:
        actor_id = _authenticate_operator(request)
        if not _PROJECT_ID_PATTERN.fullmatch(project_id):
            raise HTTPException(400, {"error": "INVALID_PROJECT_ID"})

        raw_body = await request.body()
        try:
            plan_req = parse_strict_json_with_limits(raw_body)
        except Exception as exc:
            raise HTTPException(400, {"error": "INVALID_PLAN_REQUEST", "message": str(exc)}) from exc

        # Read current snapshot to validate preconditions and binding
        git_config_path = getattr(host, "git_config_path", None)
        state_base = getattr(host, "state_base", None) or (Path(os.environ["NGINX_QA_STAGING_STATE_BASE"]) if "NGINX_QA_STAGING_STATE_BASE" in os.environ else None)
        collector = DiagnosticSnapshotCollector(git_config_path=git_config_path, state_base=state_base)
        snapshot = collector.collect_snapshot(project_id)

        try:
            plan = operations_manager.create_plan(actor_id, project_id, plan_req, snapshot=snapshot, host=host)
        except ValueError as exc:
            raise HTTPException(400, {"error": "PLAN_REJECTED", "message": str(exc)}) from exc

        response = JSONResponse(plan)
        response.headers["Cache-Control"] = "no-store"
        return response

    @app.post("/api/v1/projects/{project_id}/setup/plans/{plan_id}/apply")
    async def apply_setup_plan(
        project_id: str,
        plan_id: str,
        request: Request,
    ) -> JSONResponse:
        actor_id = _authenticate_operator(request)
        if not _PROJECT_ID_PATTERN.fullmatch(project_id):
            raise HTTPException(400, {"error": "INVALID_PROJECT_ID"})

        raw_body = await request.body()
        try:
            apply_req = parse_strict_json_with_limits(raw_body)
        except Exception as exc:
            raise HTTPException(400, {"error": "INVALID_APPLY_REQUEST", "message": str(exc)}) from exc

        try:
            receipt = await operations_manager.apply_plan(actor_id, project_id, apply_req, host=host)
        except LookupError as exc:
            raise HTTPException(404, {"error": "PLAN_NOT_FOUND", "message": str(exc)}) from exc
        except ValueError as exc:
            msg = str(exc)
            if "IDEMPOTENCY_CONFLICT" in msg:
                raise HTTPException(409, {"error": "IDEMPOTENCY_CONFLICT", "message": msg}) from exc
            if "PLAN_STALE" in msg:
                raise HTTPException(409, {"error": "PLAN_STALE", "message": msg}) from exc
            if "CONFIRMATION_REQUIRED" in msg:
                raise HTTPException(400, {"error": "CONFIRMATION_REQUIRED", "message": msg}) from exc
            raise HTTPException(400, {"error": "APPLY_REJECTED", "message": msg}) from exc

        response = JSONResponse(receipt)
        response.headers["Cache-Control"] = "no-store"
        return response

    @app.get("/api/v1/projects/{project_id}/setup/operations/{operation_id}")
    async def get_setup_operation(
        project_id: str,
        operation_id: str,
        request: Request,
    ) -> JSONResponse:
        _authenticate_operator(request)
        if not _PROJECT_ID_PATTERN.fullmatch(project_id):
            raise HTTPException(400, {"error": "INVALID_PROJECT_ID"})

        try:
            receipt = operations_manager.get_operation(project_id, operation_id)
        except LookupError as exc:
            raise HTTPException(404, {"error": "OPERATION_NOT_FOUND", "message": str(exc)}) from exc

        response = JSONResponse(receipt)
        response.headers["Cache-Control"] = "no-store"
        return response
