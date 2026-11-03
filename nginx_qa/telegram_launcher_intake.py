"""Authenticated JSON intake and immutable Git provisioning, without activation."""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import urllib.error
import urllib.request
from typing import Any, Mapping

from fastapi import HTTPException

from nginx_qa.git_provider import ManagedGitError, canonical_remote_from_address
from nginx_qa.inbound_proposals import configured_secret_values, contains_recognized_secret
from nginx_qa.managed_import import ManagedImportError, managed_schema_errors, parse_strict_json_object
from nginx_qa.sprint_types import canonical_json_bytes, canonical_json_sha256, git_ref_format_valid, managed_graph_semantic_issues, relative_git_path_valid
from nginx_qa.telegram_launcher import MAX_UPDATE_BYTES, authorize_message, fail, load_policy
from nginx_qa.telegram_launcher_store import LauncherOperations
from nginx_qa.workspace_manager import WorkspaceError

KIND = "telegram_managed_sprint_v1"
DOCUMENT_NAME = "managed-sprint.json"


def launcher_text_selected(text: Any) -> bool:
    """Peek only at top-level namespace; leave unrelated legacy syntax alone."""
    if not isinstance(text, str):
        return False
    stripped = text.strip()
    if stripped.startswith("```"):
        match = re.fullmatch(r"```(?:json)?\s*\n([\s\S]*?)\n```", stripped)
        if not match:
            return False
        stripped = match.group(1)
    if not stripped.startswith("{"):
        return False
    try:
        pairs = json.loads(stripped, object_pairs_hook=lambda items: items)
        # Preserve duplicate top-level keys during selection; strict parsing
        # later rejects any ambiguous launcher envelope before side effects.
        return any(k == "kind" and isinstance(v, str) and v.startswith("telegram_managed_sprint_") for k, v in pairs)
    except (ValueError, TypeError, RecursionError):
        return False


def launcher_message_selected(message: Mapping[str, Any]) -> bool:
    document, caption = message.get("document"), message.get("caption")
    return (launcher_text_selected(message.get("text")) or launcher_text_selected(caption)
            or (isinstance(document, dict) and isinstance(document.get("file_name"), str)
                and document["file_name"].casefold() == DOCUMENT_NAME)
            or (isinstance(caption, str) and caption.strip().startswith("/sprint_import")))


def bounded_object(raw: bytes) -> dict[str, Any]:
    if len(raw) > MAX_UPDATE_BYTES:
        fail("PAYLOAD_TOO_LARGE", "Sprint JSON exceeds 1 MiB.", 413)
    try:
        value = parse_strict_json_object(raw)
    except ValueError:
        fail("INVALID_JSON", "Sprint must be strict UTF-8 JSON.")
    stack, count = [(value, 0)], 0
    while stack:
        item, depth = stack.pop()
        count += 1
        if depth > 64 or count > 32768:
            fail("PAYLOAD_COMPLEXITY_LIMIT", "Sprint exceeds depth or structural item limits.", 413)
        if isinstance(item, dict):
            stack.extend((v, depth + 1) for v in item.values())
        elif isinstance(item, list):
            stack.extend((v, depth + 1) for v in item)
    return value


def text_object(text: str) -> dict[str, Any] | None:
    stripped = text.strip()
    if stripped.startswith("```"):
        match = re.fullmatch(r"```(?:json)?\s*\n([\s\S]*?)\n```", stripped)
        if not match:
            fail("INVALID_JSON", "Use one complete JSON document or one JSON code fence.")
        stripped = match.group(1)
    if not stripped.startswith("{"):
        return None
    return bounded_object(stripped.encode("utf-8"))


def validate_envelope(envelope: Mapping[str, Any]) -> None:
    if envelope.get("kind") != KIND or type(envelope.get("schema_version")) is not int or envelope["schema_version"] != 1:
        fail("SCHEMA_UNSUPPORTED", "Supported launcher kind/version is telegram_managed_sprint_v1/1.")
    manifest = envelope.get("manifest")
    if isinstance(manifest, dict):
        for field, limit in (("nodes", 256), ("files", 1024)):
            if isinstance(manifest.get(field), list) and len(manifest[field]) > limit:
                fail("PAYLOAD_COMPLEXITY_LIMIT", "Sprint exceeds node or checksum limits.", 413)
    if managed_schema_errors(envelope, "telegram-managed-sprint-launcher-v1.schema.json"):
        fail("ENVELOPE_SCHEMA_INVALID", "JSON does not match the installed launcher and managed manifest schemas.")
    if managed_graph_semantic_issues(manifest):
        fail("MANAGED_GRAPH_INVALID", "Check transitions, unique roles/phones, reviewers and reachable terminals.")
    if contains_recognized_secret(envelope, configured_values=configured_secret_values()):
        fail("SECRET_IN_INPUT", "Remove credentials from the sprint JSON.")
    forbidden = {"runtime_root", "managed_root", "lease_root", "prompt_root", "protected_roots",
                 "operator_policy", "telegram_bot_token", "credentials", "upstream_url"}
    stack = [manifest]
    while stack:
        item = stack.pop()
        if isinstance(item, dict):
            if forbidden.intersection(k.lower() for k in item):
                fail("OPERATOR_SETTINGS_FORBIDDEN", "Uploaded JSON cannot supply runtime roots or operator settings.")
            stack.extend(item.values())
        elif isinstance(item, list):
            stack.extend(item)
    if any(isinstance(n.get("workspace"), dict) and "process" in n["workspace"] for n in manifest["nodes"]):
        fail("PROCESS_TEMPLATE_REQUIRED", "Uploaded process commands are disabled in launcher v1.")
    root_git = manifest["git"]
    if not root_git["expected_source_commit"] or not git_ref_format_valid(root_git["source_ref"]):
        fail("SOURCE_COMMIT_REQUIRED", "Use a pinned source commit and valid ref from /sprint_prompt.")
    branches = [root_git["assigned_branch"], *(r["git_branch"] for r in manifest["execution"]["reviewers"])]
    writes = []
    for node in manifest["nodes"]:
        if node.get("type", "task") == "terminal":
            continue
        workspace = node.get("workspace", {})
        branch = workspace.get("git", {}).get("assigned_branch", root_git["assigned_branch"])
        branches.append(branch)
        if workspace.get("access") == "write":
            writes.append(branch.casefold())
    if any(not git_ref_format_valid(b, branch=True) or not b.startswith(("agent/", "review/")) for b in branches):
        fail("GIT_BRANCH_POLICY_INVALID", "Launcher branches must be valid topic refs under agent/ or review/.")
    if len(writes) != len(set(writes)):
        fail("GIT_BRANCH_POLICY_INVALID", "Write nodes need distinct assigned branches.")
    paths = [entry["path"] for entry in manifest["files"]]
    if any(not relative_git_path_valid(p) for p in paths) or len(paths) != len({p.casefold() for p in paths}):
        fail("CHECKSUM_PATH_INVALID", "Checksum paths must be safe and unique.")


def repository_binding(spec):
    return {"repository_id": spec.repository_id, "canonical_remote": spec.canonical_remote,
            "transport_url": spec.transport_url, "credential_reference": spec.credential_reference}


def validate_source(importer, envelope, git_address, project_policy):
    importer.validate_mutation_roots()
    provider = importer.git_provider
    repository = provider.ensure_mirror(project_policy["repository_id"], fetch=True)
    manifest = envelope["manifest"]
    canonical = canonical_remote_from_address(git_address)
    if canonical != repository.canonical_remote or canonical_remote_from_address(manifest.get("git_address", "")) != canonical:
        fail("REPOSITORY_IDENTITY_MISMATCH", "Use the registered project's repository.", 409)
    if manifest["git"]["source_ref"] != project_policy["source_ref"]:
        fail("SOURCE_REF_FORBIDDEN", "Source ref differs from the operator project policy.", 403)
    commit = provider.resolve_commit(repository, project_policy["source_ref"])
    if commit != manifest["git"]["expected_source_commit"]:
        fail("SOURCE_COMMIT_MISMATCH", "Source changed; generate a fresh prompt and JSON.", 409)
    budget = 64 * 1024 * 1024
    for artifact in manifest["files"]:
        raw = provider.read_blob(repository, commit, artifact["path"], max_bytes=min(budget, 16 * 1024 * 1024))
        budget -= len(raw)
        if hashlib.sha256(raw).hexdigest() != artifact["sha256"]:
            fail("MANIFEST_CHECKSUM_MISMATCH", "A checksum does not match the pinned Git blob.", 409)
        if budget <= 0:
            fail("SOURCE_BYTE_LIMIT", "Checksum source data exceeds 64 MiB.", 413)
    provider.pin_commit(repository, commit, owner_id="launcher-source:" + canonical_json_sha256(manifest))
    return repository, repository_binding(repository.spec)


def download_document(host, token: str, document: Mapping[str, Any]) -> bytes:
    name = document.get("file_name")
    file_id, unique_id = document.get("file_id"), document.get("file_unique_id")
    if not isinstance(name, str) or not name.lower().endswith(".json") or len(name) > 256:
        fail("DOCUMENT_INVALID", "Attach a .json document.")
    if any(not isinstance(v, str) or not 1 <= len(v) <= 256 for v in (file_id, unique_id)):
        fail("DOCUMENT_INVALID", "Telegram document identity is missing.")
    size = document.get("file_size")
    if size is not None and (type(size) is not int or size < 0 or size > MAX_UPDATE_BYTES):
        fail("PAYLOAD_TOO_LARGE", "Sprint JSON exceeds 1 MiB.", 413)
    try:
        metadata = host.telegram_api_json(token, "getFile", {"file_id": file_id})
        path = metadata["result"]["file_path"]
        if (not isinstance(path, str) or not re.fullmatch(r"[A-Za-z0-9_./-]{1,256}", path)
                or path.startswith("/") or any(p in {"", ".", ".."} for p in path.split("/"))):
            raise ValueError
        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *args, **kwargs):
                return None
        opener = urllib.request.build_opener(NoRedirect)
        with opener.open(f"https://api.telegram.org/file/bot{token}/{path}", timeout=30) as response:
            raw = response.read(MAX_UPDATE_BYTES + 1)
    except (HTTPException, KeyError, TypeError, ValueError, urllib.error.URLError, TimeoutError):
        fail("TELEGRAM_UNAVAILABLE", "Cannot download this Telegram JSON document.", 503)
    if len(raw) > MAX_UPDATE_BYTES:
        fail("PAYLOAD_TOO_LARGE", "Downloaded sprint JSON exceeds 1 MiB.", 413)
    return raw


def reauthorize(message, principal, original_policy, action="prepare"):
    policy = load_policy()
    latest = authorize_message(message, principal.project_id, action, policy)
    if latest != principal or policy["projects"][principal.project_id] != original_policy["projects"][principal.project_id]:
        fail("ACCESS_DENIED", "Operator permissions or project policy changed during the request.", 403)
    return latest


def provision_and_prepare(operations, operation_id, principal, project_policy):
    from nginx_qa.telegram_launcher_execution import current_principal
    importer = operations.importer
    importer.validate_mutation_roots()
    digest = hashlib.sha256(("launcher-action:" + operation_id).encode()).hexdigest()
    with importer.git_provider.request_operation_lock(digest, timeout=30):
        principal = current_principal(principal, "prepare", project_policy)
        record = operations.get(operation_id, principal)
        if record["lifecycle"] in {"Stopped", "Completed"} or record.get("stop_requested"):
            return record
        if record["envelope"]["manifest"]["git"]["source_ref"] != project_policy["source_ref"]:
            fail("SOURCE_REF_FORBIDDEN", "Source policy changed.", 403)
        spec = importer.git_provider.resolve_repository(project_policy["repository_id"])
        if repository_binding(spec) != record["repository_binding"]:
            fail("REPOSITORY_BINDING_CHANGED", "Restore the original repository binding.", 409)
        if not record["published"]:
            if "manifest_publish" not in principal.actions:
                return record
            repository = importer.git_provider.ensure_mirror(project_policy["repository_id"], fetch=True)
            intent = record["provision_intent"]
            if intent is None:
                intent = importer.git_provider.create_launcher_manifest_commit(repository, operation_id=operation_id,
                    source_commit=record["envelope"]["manifest"]["git"]["expected_source_commit"],
                    manifest_blob=canonical_json_bytes(record["envelope"]["manifest"]), created_at=record["created_at"])
                record = operations.update(operation_id, principal, provision_intent=intent)
            principal = current_principal(principal, "manifest_publish", project_policy)
            try:
                importer.git_provider.publish_launcher_manifest(repository, intent)
            except ManagedGitError:
                return operations.update(operation_id, principal, blocked_reason="PUBLICATION_UNAVAILABLE")
            record = operations.update(operation_id, principal, published=True, blocked_reason="PREPARATION_REQUIRED")
        principal = current_principal(principal, "prepare", project_policy)
        record = operations.prepare(operation_id, principal)
        current_principal(principal, "prepare", project_policy)
        return record


async def continue_operation(operations, record, principal, project_policy, host):
    """Resume the same durable operation after delivery, callback or restart."""
    from nginx_qa.telegram_launcher_execution import current_principal, launch, stop
    principal = current_principal(principal, "stop" if record.get("stop_requested") else "prepare", project_policy)
    record = await asyncio.to_thread(operations.get, record["operation_id"], principal)
    if record["lifecycle"] in {"Stopped", "Completed"}:
        return record
    if record.get("stop_requested"):
        return await stop(operations, record["operation_id"], principal, project_policy, host)
    record = await asyncio.to_thread(provision_and_prepare, operations, record["operation_id"], principal, project_policy)
    principal = current_principal(principal, "prepare", project_policy)
    if (record["envelope"]["autonomous_mode"] or record.get("launch_requested")) and record["lifecycle"] not in {"Running", "Completed", "Stopped", "Failed"}:
        try:
            record = await launch(operations, record["operation_id"], principal, project_policy, host)
        except HTTPException as exc:
            if exc.status_code != 403:
                raise
            principal = current_principal(principal, "prepare", project_policy)
            record = await asyncio.to_thread(operations.update, record["operation_id"], principal, blocked_reason="START_PERMISSION_REQUIRED")
    return record


def pending_project_operations(host, config, project_id, project_policy):
    """Synchronous filesystem/SQLite work, kept off the ASGI event loop."""
    _, _, project, context = host.project_for_group_api(config, project_id)
    registry = host.managed_repository_registry_for_project(project, context)
    importer = host.managed_sprint_importer_factory(host.load_managed_runtime_config(), registry)
    if not importer.store.database_path.exists():
        return None, []
    operations = LauncherOperations(importer)
    spec = importer.git_provider.resolve_repository(project_policy["repository_id"])
    if canonical_remote_from_address(str(project.get("git_address") or context.get("git_address") or "")) != spec.canonical_remote:
        return None, []
    with importer.store._transaction() as connection:
        records = [operations._decode(row) for row in connection.execute(
            "SELECT * FROM telegram_launcher_operations WHERE project_id = ? AND holds_slot = 1", (project_id,))]
    return operations, records


async def reconcile_operations(host, *, stop_event=None):
    """One bounded, failure-isolated transport batch on the ASGI event loop."""
    from nginx_qa.telegram_launcher import ENABLED_ENV, Principal
    from nginx_qa.telegram_launcher_execution import current_principal
    if os.getenv(ENABLED_ENV, "").lower() not in {"1", "true"}:
        return
    policy = load_policy()
    config = await host.read_git_config()
    slots = asyncio.Semaphore(4)
    def stopping():
        return stop_event is not None and stop_event.is_set()
    async def recover_project(project_id, project_policy):
        async with slots:
            if stopping():
                return
            try:
                operations, records = await asyncio.to_thread(pending_project_operations, host, config, project_id, project_policy)
                for record in records:
                    if stopping():
                        return
                    try:
                        principal = Principal(record["user_id"], record["chat_id"], project_id, frozenset())
                        principal = current_principal(principal, "stop" if record.get("stop_requested") else "prepare", project_policy)
                        await continue_operation(operations, record, principal, project_policy, host)
                    except (HTTPException, ManagedImportError, ManagedGitError, RuntimeError, ValueError, OSError):
                        continue
            except (HTTPException, ManagedImportError, ManagedGitError, WorkspaceError, KeyError, TypeError, ValueError, RuntimeError, OSError):
                # One blocked/corrupt project cannot starve other projects.
                return
    # A corrupt row/SQLite failure can raise outside the expected domain errors.
    # gather's default early exception propagation would orphan still-running
    # siblings, falsely ending the single-flight/shutdown barrier. Never cancel
    # thread-backed mutations: collect every child before returning to the host.
    results = await asyncio.gather(*(recover_project(project_id, project_policy)
        for project_id, project_policy in policy["projects"].items()), return_exceptions=True)
    return {"failed_projects": sum(isinstance(result, BaseException) for result in results)}


async def maybe_handle_upload(update, message, host):
    if not launcher_message_selected(message):
        return None
    text, document = message.get("text"), message.get("document")
    candidate = text_object(text) if isinstance(text, str) else None
    policy = None
    caption_project = None
    if isinstance(document, dict):
        caption = message.get("caption")
        if text:
            fail("AMBIGUOUS_PAYLOAD", "Send one launcher JSON document without additional text.")
        if caption:
            match = re.fullmatch(r"/sprint_import\s+(9[0-9]{3})", caption.strip()) if isinstance(caption, str) else None
            if match is None:
                fail("AMBIGUOUS_PAYLOAD", "Use caption /sprint_import <project phone> or reserved filename managed-sprint.json.")
            caption_project = match.group(1)
        policy = load_policy()
        allowed = False
        for project_id in ([caption_project] if caption_project else policy["projects"]):
            try:
                download_principal = authorize_message(message, project_id, "prepare", policy)
                allowed = True
                break
            except HTTPException as exc:
                if exc.status_code != 403:
                    raise
        if not allowed:
            fail("ACCESS_DENIED", "No project preparation permission for this Telegram user.", 403)
        raw = await asyncio.to_thread(download_document, host, os.getenv("TELEGRAM_BOT_TOKEN", ""), document)
        reauthorize(message, download_principal, policy)
        candidate = bounded_object(raw)
    if candidate is None or not isinstance(candidate.get("kind"), str) or not candidate["kind"].startswith("telegram_managed_sprint_"):
        fail("LAUNCHER_ENVELOPE_REQUIRED", "Explicit launcher uploads require a managed launcher envelope.")
    if caption_project is not None and candidate.get("project_id") != caption_project:
        fail("PROJECT_IDENTITY_MISMATCH", "Caption project differs from the envelope project.", 403)
    policy = policy or load_policy()
    principal = authorize_message(message, str(candidate.get("project_id", "")), "prepare", policy)
    validate_envelope(candidate)
    if (type(update.get("update_id")) is not int or update["update_id"] < 0
            or type(message.get("message_id")) is not int or message["message_id"] < 1):
        fail("DELIVERY_IDENTITY_INVALID", "Telegram update and message identities are required.")
    bot_id = os.getenv("TELEGRAM_BOT_TOKEN", "").partition(":")[0]
    if not re.fullmatch(r"[1-9][0-9]{0,19}", bot_id):
        fail("TELEGRAM_UNAVAILABLE", "Configure a valid Telegram bot identity.", 503)
    reauthorize(message, principal, policy)
    config = await host.read_git_config()
    try:
        _, _, project, context = host.project_for_group_api(config, principal.project_id)
        registry = host.managed_repository_registry_for_project(project, context)
        importer = host.managed_sprint_importer_factory(host.load_managed_runtime_config(), registry)
        operations = LauncherOperations(importer)
        keys = operations.keys(bot_id, update["update_id"], message["message_id"], principal)
        transport = {"document_id": document.get("file_id") if isinstance(document, dict) else None,
                     "document_unique_id": document.get("file_unique_id") if isinstance(document, dict) else None}
        record = await asyncio.to_thread(operations.lookup, principal, keys, candidate, transport)
        deduplicated = record is not None
        project_policy = policy["projects"][principal.project_id]
        if candidate["manifest"]["git"]["source_ref"] != project_policy["source_ref"]:
            fail("SOURCE_REF_FORBIDDEN", "Source ref is no longer allowed by project policy.", 403)
        if record is None:
            repository, binding = await asyncio.to_thread(validate_source, importer, candidate,
                str(project.get("git_address") or context.get("git_address") or ""), project_policy)
            reauthorize(message, principal, policy)
            record = await asyncio.to_thread(operations.reserve, principal, keys, candidate, transport, binding)
        else:
            current_spec = importer.git_provider.resolve_repository(project_policy["repository_id"])
            if (repository_binding(current_spec) != record["repository_binding"]
                    or canonical_remote_from_address(str(project.get("git_address") or context.get("git_address") or "")) != current_spec.canonical_remote):
                fail("REPOSITORY_BINDING_CHANGED", "Restore the original repository binding to resume this operation.", 409)
            repository = None
        reauthorize(message, principal, policy)
        record = await continue_operation(operations, record, principal, project_policy, host)
        reauthorize(message, principal, policy)
        return operations.public(record, deduplicated=deduplicated)
    except WorkspaceError:
        fail("RUNTIME_ROOT_NOT_ISOLATED", "Managed roots must be isolated before JSON intake.", 409)
    except ManagedImportError as exc:
        fail("PROJECT_BUSY" if exc.code == "PROJECT_BUSY" else "MANAGED_STATE_CONFLICT",
             "Managed project state prevents this intake; inspect the operator status.", 409)
    except ManagedGitError:
        fail("GIT_CONTEXT_INVALID", "Authorized Git context could not be verified.", 409)
    except (KeyError, TypeError, ValueError, RuntimeError):
        fail("MANAGED_STATE_CONFLICT", "Launcher configuration or durable state requires operator inspection.", 409)
