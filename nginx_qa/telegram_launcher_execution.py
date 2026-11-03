"""Authorized launch adapter for an existing managed agent-client service.

This module does not run a model, choose graph successors or schedule agents.
The engine publishes assignments; an operator-configured existing client service
acknowledges that it will consume the normal identity/result/review APIs.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

from fastapi import HTTPException

from nginx_qa.managed_import import parse_strict_json_object
from nginx_qa.telegram_launcher import Principal, fail, load_policy


PROTOCOL = "nginx-qa-managed-client-v1"


class ExecutorUnavailable(RuntimeError):
    pass


def current_principal(principal: Principal, action: str, expected_project=None):
    """Reauthorize an already authenticated Telegram/Mini App identity."""
    policy = load_policy()
    project = policy["projects"].get(principal.project_id)
    if project is None or (expected_project is not None and project != expected_project):
        fail("ACCESS_DENIED", "The project policy changed.", 403)
    for grant in policy["grants"]:
        if (principal.user_id, principal.chat_id, principal.project_id) == (
            grant["user_id"], grant["chat_id"], grant["project_id"]
        ) and action in grant["actions"]:
            return Principal(principal.user_id, principal.chat_id, principal.project_id, frozenset(grant["actions"]))
    fail("ACCESS_DENIED", "Current operator permission is required.", 403)


def descriptor(record):
    manifest = record["envelope"]["manifest"]
    phones = {node["agent"]["phone"] for node in manifest["nodes"] if node.get("type", "task") != "terminal"}
    phones.update(reviewer["phone"] for reviewer in manifest["execution"]["reviewers"])
    return {"protocol": PROTOCOL, "operation_id": record["operation_id"],
            "project_id": record["project_id"], "sprint_id": record["sprint_id"],
            "agent_phones": sorted(phones)}


def validate_receipt(receipt, expected, status):
    if (not isinstance(receipt, dict)
        or set(receipt) != set(expected) | {"status", "client_session_id"}
        or any(receipt.get(key) != value for key, value in expected.items())
        or receipt.get("status") != status
        or not isinstance(receipt.get("client_session_id"), str)
        or not 1 <= len(receipt["client_session_id"]) <= 128
        or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-" for c in receipt["client_session_id"])):
        raise ExecutorUnavailable("Invalid scoped client acknowledgement")
    return dict(receipt)


class ExistingClientHTTPAdapter:
    """Optional bridge to an existing client service on operator-owned loopback."""
    def __init__(self, base_url, credential_reference, secret_resolver):
        try:
            parsed = urllib.parse.urlsplit(base_url)
            port = parsed.port
        except (ValueError, TypeError):
            raise ExecutorUnavailable("Invalid existing client endpoint") from None
        if (parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "::1"}
            or parsed.username or parsed.password or parsed.query or parsed.fragment
            or parsed.path not in {"", "/"} or not port
            or not isinstance(credential_reference, str) or not credential_reference):
            raise ExecutorUnavailable("Configure a scoped loopback client endpoint and secret reference")
        self.base_url = base_url.rstrip("/")
        self.credential_reference = credential_reference
        self.secret_resolver = secret_resolver

    def _request(self, action, payload):
        try:
            token = self.secret_resolver(self.credential_reference)
            if not isinstance(token, str) or not token or "\r" in token or "\n" in token:
                raise ValueError
            class NoRedirect(urllib.request.HTTPRedirectHandler):
                def redirect_request(self, *args, **kwargs):
                    return None
            request = urllib.request.Request(self.base_url + "/managed-client/" + action,
                data=json.dumps(payload, ensure_ascii=False).encode("utf-8"), method="POST",
                headers={"Content-Type": "application/json", "Authorization": "Bearer " + token})
            with urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect).open(request, timeout=10) as response:
                raw = response.read(32769)
            if len(raw) > 32768:
                raise ValueError
            return parse_strict_json_object(raw)
        except (ValueError, TypeError, LookupError, OSError, RuntimeError, urllib.error.URLError):
            raise ExecutorUnavailable("Existing client service is unavailable") from None

    def prepare(self, instance):
        return validate_receipt(self._request("prepare", instance), instance, "READY")

    def start(self, instance, assignment_ids, ready):
        expected = {**instance, "initial_assignment_ids": assignment_ids}
        result = self._request("start", {**expected, "client_session_id": ready["client_session_id"]})
        receipt = validate_receipt(result, expected, "ACCEPTED")
        if receipt["client_session_id"] != ready["client_session_id"]:
            raise ExecutorUnavailable("Client session changed during launch")
        return receipt

    def stop(self, instance, ready):
        receipt = validate_receipt(self._request("stop", {**instance, "client_session_id": ready["client_session_id"]}), instance, "STOPPED")
        if receipt["client_session_id"] != ready["client_session_id"]:
            raise ExecutorUnavailable("Client stop acknowledgement changed session")
        return receipt


def executor_for(host):
    factory = getattr(host, "managed_launcher_executor_factory", None)
    if callable(factory):
        return factory()
    url = os.getenv("NGINX_QA_LAUNCHER_EXECUTOR_URL", "")
    reference = os.getenv("NGINX_QA_LAUNCHER_EXECUTOR_SECRET_REF", "")
    from nginx_qa.control_secrets import trusted_control_resolver
    resolver = trusted_control_resolver
    if not url or not reference or not callable(resolver):
        raise ExecutorUnavailable("No existing managed client adapter is configured")
    return ExistingClientHTTPAdapter(url, reference, resolver)


def launch_sync(operations, operation_id, principal, project_policy, executor_factory):
    """One cross-process launch transaction around existing engine idempotency."""
    principal = current_principal(principal, "start", project_policy)
    operations.importer.validate_mutation_roots()
    digest = hashlib.sha256(("launcher-action:" + operation_id).encode()).hexdigest()
    with operations.importer.git_provider.request_operation_lock(digest, timeout=30):
        principal = current_principal(principal, "start", project_policy)
        record = operations.get(operation_id, principal)
        if record["lifecycle"] in {"Running", "Completed", "Stopped"} or record.get("stop_requested"):
            return record
        if record["lifecycle"] == "Failed":
            return record
        if not record.get("instance_id"):
            fail("PREPARATION_REQUIRED", "Prepare this instance before Start.", 409)
        operations.request_launch(operation_id, principal)
        instance = descriptor(record)
        try:
            executor = executor_factory()
            if executor is None:
                raise ExecutorUnavailable("No executor")
            ready = validate_receipt(executor.prepare(instance), instance, "READY")
            principal = current_principal(principal, "start", project_policy)
            if record.get("executor_ready") not in (None, ready):
                raise ExecutorUnavailable("Client did not preserve its original operation receipt")
            operations.update(operation_id, principal, executor_ready=ready)
            request = operations.preparation_request(operation_id, principal)
            result = operations.importer.activate_prepared(principal.project_id, request, operation_id=operation_id)
            principal = current_principal(principal, "start", project_policy)
            assignments = result.response["initial_assignment_ids"]
            expected = {**instance, "initial_assignment_ids": assignments}
            receipt = validate_receipt(executor.start(instance, assignments, ready), expected, "ACCEPTED")
            if receipt["client_session_id"] != ready["client_session_id"]:
                raise ExecutorUnavailable("Client acknowledgement changed session")
            # Preserve the actual external ACK before checking permission to
            # disclose it. Revocation cannot erase evidence of a call in flight.
            result = operations.accept_executor(operation_id, principal, receipt)
            current_principal(principal, "start", project_policy)
            return result
        except ExecutorUnavailable:
            operations.update(operation_id, principal, blocked_reason="EXECUTOR_UNAVAILABLE")
            return operations.get(operation_id, principal)


async def launch(operations, operation_id, principal, project_policy, host):
    current_principal(principal, "start", project_policy)
    result = await asyncio.to_thread(launch_sync, operations, operation_id, principal, project_policy,
                                     lambda: executor_for(host))
    current_principal(principal, "start", project_policy)
    return result


def stop_sync(operations, operation_id, principal, project_policy, executor_factory, runtime):
    from nginx_qa.managed_cancellation import begin_cancel, finalize_cancel
    principal = current_principal(principal, "stop", project_policy)
    operations.importer.validate_mutation_roots()
    digest = hashlib.sha256(("launcher-action:" + operation_id).encode()).hexdigest()
    with operations.importer.git_provider.request_operation_lock(digest, timeout=30):
        principal = current_principal(principal, "stop", project_policy)
        record = operations.get(operation_id, principal)
        if record["lifecycle"] in {"Stopped", "Completed"} or (record["lifecycle"] == "Failed" and record.get("engine_activated")):
            return record
        if not record.get("engine_activated"):
            return operations.cancel_prepared(operation_id, principal)
        operations.update(operation_id, principal, stop_requested=True, blocked_reason="STOP_PENDING")
        # Serialize the durable fence with ordinary graph result/review/recovery.
        # Release the lock before asking the external client to quiesce: it may
        # have an in-flight API request that must return before its stop ACK.
        with runtime._sprint_lock(record["sprint_id"]):
            state = begin_cancel(operations.store, principal.project_id, record["sprint_id"], operation_id,
                                 requested_at=datetime.now(timezone.utc).isoformat())
        if state["status"] != "active":
            return operations.get(operation_id, principal)
        try:
            receipt = record.get("executor_stop_receipt")
            if receipt is None:
                ready = record.get("executor_ready")
                if not ready:
                    raise ExecutorUnavailable("No original client session")
                principal = current_principal(principal, "stop", project_policy)
                executor = executor_factory()
                if executor is None:
                    raise ExecutorUnavailable("No existing managed client adapter")
                receipt = validate_receipt(executor.stop(descriptor(record), ready), descriptor(record), "STOPPED")
                if receipt["client_session_id"] != ready["client_session_id"]:
                    raise ExecutorUnavailable("Client stop acknowledgement changed session")
                operations.update(operation_id, principal, executor_stop_receipt=receipt)
            receipt = validate_receipt(receipt, descriptor(record), "STOPPED")
            if receipt["client_session_id"] != record.get("executor_ready", {}).get("client_session_id"):
                raise ExecutorUnavailable("Stored stop acknowledgement changed session")
            # Revocation does not erase a real ACK, but prevents new cleanup.
            principal = current_principal(principal, "stop", project_policy)
            with runtime._sprint_lock(record["sprint_id"]):
                state = operations.store.runtime_state(principal.project_id, record["sprint_id"])
                for assignment_id in {p["assignment_id"] for p in state["processes"]}:
                    runtime._stop_assignment_processes(principal.project_id, record["sprint_id"], assignment_id)
                finalize_cancel(operations.store, principal.project_id, record["sprint_id"], operation_id,
                                stopped_at=datetime.now(timezone.utc).isoformat())
        except ExecutorUnavailable:
            # The fence and reservation survive; retry the same client session.
            operations.update(operation_id, principal, blocked_reason="STOP_PENDING")
        return operations.get(operation_id, principal)


async def stop(operations, operation_id, principal, project_policy, host):
    from nginx_qa.managed_continuity import ManagedContinuityRuntime
    runtime = ManagedContinuityRuntime(operations.importer, process_supervisor=getattr(host, "managed_process_supervisor", None))
    result = await asyncio.to_thread(stop_sync, operations, operation_id, principal, project_policy,
                                     lambda: executor_for(host), runtime)
    current_principal(principal, "stop", project_policy)
    return result
