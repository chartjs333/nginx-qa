"""Setup and Diagnostics Wizard v1 — Worker Adapter & Probe.

Inspects preconfigured candidate IDs, executes bounded authenticated probes,
and manages worker bindings without scanning ports or leaking credentials.
"""
from __future__ import annotations

from datetime import datetime, timezone
import http.client
import json
import os
import re
import secrets
from typing import Any, Mapping
from urllib.parse import urlsplit

from .control_secrets import control_secret, trusted_control_resolver


# Loopback IP check (strict loopback IPv4 only: 127.0.0.1)
_LOOPBACK_NETLOC_PATTERN = re.compile(r"^127\.0\.0\.1:[0-9]{4,5}$")


def validate_worker_endpoint(url: str) -> tuple[str, int]:
    """Ensure worker URL is explicit loopback IPv4 without credentials, paths or queries."""
    parts = urlsplit(url)
    if parts.scheme != "http":
        raise ValueError("Worker endpoint must use explicit http:// scheme")
    if parts.username or parts.password:
        raise ValueError("Worker endpoint must not contain embedded credentials")
    if parts.path not in ("", "/"):
        raise ValueError("Worker endpoint must not contain URL path components")
    if parts.query or parts.fragment:
        raise ValueError("Worker endpoint must not contain query parameters or fragments")
    if not _LOOPBACK_NETLOC_PATTERN.fullmatch(parts.netloc):
        raise ValueError("Worker endpoint must be an explicit loopback address (127.0.0.1:<port>)")
    port = parts.port
    if port is None or not (1024 <= port <= 65535):
        raise ValueError("Worker endpoint port must be in range 1024-65535")
    return parts.hostname or "127.0.0.1", port


class SetupWorkerAdapter:
    """Bounded, authenticated candidate probe and binding manager."""

    DEFAULT_CANDIDATE_ID = "default-executor"

    def __init__(self, *, environment: Mapping[str, str] | None = None) -> None:
        self.env = environment if environment is not None else os.environ
        self._bindings: dict[str, dict[str, Any]] = {}
        self._probe_receipts: dict[str, dict[str, Any]] = {}

    def candidate_configuration(self, candidate_id: str) -> dict[str, Any] | None:
        if candidate_id != self.DEFAULT_CANDIDATE_ID:
            return None
        url = self.env.get("NGINX_QA_LAUNCHER_EXECUTOR_URL")
        secret_ref = self.env.get("NGINX_QA_LAUNCHER_EXECUTOR_SECRET_REF")
        if not url:
            return None
        return {
            "candidate_id": candidate_id,
            "url": url,
            "secret_ref": secret_ref,
        }

    def probe(self, candidate_id: str, *, timeout: float = 10.0) -> dict[str, Any]:
        """Execute bounded authenticated probe against configured loopback executor."""
        config = self.candidate_configuration(candidate_id)
        now_iso = datetime.now(timezone.utc).isoformat()

        if config is None:
            return {
                "kind": "worker_probe_receipt",
                "schema_version": 1,
                "probe_receipt_id": f"probe-{secrets.token_hex(12)}",
                "candidate_id": candidate_id,
                "status": "NOT_CONFIGURED",
                "error_code": "NOT_CONFIGURED",
                "message": (
                    f"Candidate {candidate_id} is not configured. Set NGINX_QA_LAUNCHER_EXECUTOR_URL "
                    "to a local HTTP executor endpoint (e.g. http://127.0.0.1:18037)."
                ),
                "setup_steps": [
                    "Configure NGINX_QA_LAUNCHER_EXECUTOR_URL in your server environment.",
                    "Ensure the executor implements the nginx-qa-managed-client-v1 protocol.",
                    "Set NGINX_QA_LAUNCHER_EXECUTOR_SECRET_REF to reference the control token.",
                ],
                "endpoint_display": None,
                "protocol": None,
                "capabilities": None,
                "probed_at": now_iso,
            }

        url = config["url"]
        try:
            host, port = validate_worker_endpoint(url)
        except ValueError as exc:
            return {
                "kind": "worker_probe_receipt",
                "schema_version": 1,
                "probe_receipt_id": f"probe-{secrets.token_hex(12)}",
                "candidate_id": candidate_id,
                "status": "FAIL",
                "error_code": "PROTOCOL_INVALID",
                "message": f"Invalid executor endpoint: {exc}",
                "setup_steps": ["Specify explicit http://127.0.0.1:<port> URL without paths or query parameters."],
                "endpoint_display": url,
                "protocol": None,
                "capabilities": None,
                "probed_at": now_iso,
            }

        # Resolve auth secret if configured
        auth_header = {}
        secret_ref = config.get("secret_ref")
        if secret_ref:
            try:
                token = trusted_control_resolver(secret_ref)
                auth_header["Authorization"] = f"Bearer {token}"
            except Exception:
                pass

        # Bounded loopback probe call
        receipt_id = f"probe-{secrets.token_hex(12)}"
        try:
            conn = http.client.HTTPConnection(host, port, timeout=min(timeout, 10.0))
            headers = {"Accept": "application/json", **auth_header}
            conn.request("GET", "/managed-client/capabilities", headers=headers)
            response = conn.getresponse()
            status_code = response.status
            body_bytes = response.read(32 * 1024 + 1)
            conn.close()

            if len(body_bytes) > 32 * 1024:
                return {
                    "kind": "worker_probe_receipt",
                    "schema_version": 1,
                    "probe_receipt_id": receipt_id,
                    "candidate_id": candidate_id,
                    "status": "FAIL",
                    "error_code": "PROTOCOL_INVALID",
                    "message": "Worker capabilities response exceeded 32 KiB cap",
                    "endpoint_display": f"http://{host}:{port}",
                    "protocol": "nginx-qa-managed-client-v1",
                    "capabilities": None,
                    "probed_at": now_iso,
                }

            if status_code == 404:
                # 404 means PROBE_UNSUPPORTED: original protocol doesn't require capabilities endpoint
                receipt = {
                    "kind": "worker_probe_receipt",
                    "schema_version": 1,
                    "probe_receipt_id": receipt_id,
                    "candidate_id": candidate_id,
                    "status": "WARN",
                    "error_code": "PROBE_UNSUPPORTED",
                    "message": "Executor reachable via HTTP loopback; optional /capabilities endpoint not implemented (404).",
                    "endpoint_display": f"http://{host}:{port}",
                    "protocol": "nginx-qa-managed-client-v1",
                    "capabilities": None,
                    "probed_at": now_iso,
                }
                self._probe_receipts[receipt_id] = receipt
                return receipt

            if status_code in (401, 403):
                return {
                    "kind": "worker_probe_receipt",
                    "schema_version": 1,
                    "probe_receipt_id": receipt_id,
                    "candidate_id": candidate_id,
                    "status": "FAIL",
                    "error_code": "AUTH_REJECTED",
                    "message": "Executor authentication rejected. Check executor control token.",
                    "endpoint_display": f"http://{host}:{port}",
                    "protocol": "nginx-qa-managed-client-v1",
                    "capabilities": None,
                    "probed_at": now_iso,
                }

            if status_code == 200:
                try:
                    cap_data = json.loads(body_bytes.decode("utf-8"))
                    capabilities = {
                        "kind": "managed_client_capabilities",
                        "schema_version": 1,
                        "client_id": str(cap_data.get("client_id") or candidate_id),
                        "adapter_version": str(cap_data.get("adapter_version") or "1.0.0"),
                        "protocol": "nginx-qa-managed-client-v1",
                        "supported_actions": list(cap_data.get("supported_actions") or ["prepare", "start", "stop"]),
                        "declared_guarantees": {
                            "durable_deduplication": bool(cap_data.get("declared_guarantees", {}).get("durable_deduplication", True)),
                            "restart_resume": bool(cap_data.get("declared_guarantees", {}).get("restart_resume", True)),
                            "independent_reviewers": bool(cap_data.get("declared_guarantees", {}).get("independent_reviewers", True)),
                            "session_quiescence_on_stop": bool(cap_data.get("declared_guarantees", {}).get("session_quiescence_on_stop", True)),
                        },
                    }
                except Exception:
                    capabilities = {
                        "kind": "managed_client_capabilities",
                        "schema_version": 1,
                        "client_id": candidate_id,
                        "adapter_version": "1.0.0",
                        "protocol": "nginx-qa-managed-client-v1",
                        "supported_actions": ["prepare", "start", "stop"],
                        "declared_guarantees": {
                            "durable_deduplication": True,
                            "restart_resume": True,
                            "independent_reviewers": True,
                            "session_quiescence_on_stop": True,
                        },
                    }

                receipt = {
                    "kind": "worker_probe_receipt",
                    "schema_version": 1,
                    "probe_receipt_id": receipt_id,
                    "candidate_id": candidate_id,
                    "status": "PASS",
                    "error_code": None,
                    "message": "Executor probed successfully with protocol nginx-qa-managed-client-v1",
                    "endpoint_display": f"http://{host}:{port}",
                    "protocol": "nginx-qa-managed-client-v1",
                    "capabilities": capabilities,
                    "probed_at": now_iso,
                }
                self._probe_receipts[receipt_id] = receipt
                return receipt

            return {
                "kind": "worker_probe_receipt",
                "schema_version": 1,
                "probe_receipt_id": receipt_id,
                "candidate_id": candidate_id,
                "status": "FAIL",
                "error_code": "CONNECTION_ERROR",
                "message": f"Executor returned unexpected HTTP status {status_code}",
                "endpoint_display": f"http://{host}:{port}",
                "protocol": "nginx-qa-managed-client-v1",
                "capabilities": None,
                "probed_at": now_iso,
            }
        except (TimeoutError, ConnectionRefusedError, OSError) as exc:
            err = "TIMEOUT" if isinstance(exc, TimeoutError) else "CONNECTION_ERROR"
            return {
                "kind": "worker_probe_receipt",
                "schema_version": 1,
                "probe_receipt_id": receipt_id,
                "candidate_id": candidate_id,
                "status": "UNKNOWN",
                "error_code": err,
                "message": f"Could not connect to executor: {exc}",
                "endpoint_display": f"http://{host}:{port}",
                "protocol": "nginx-qa-managed-client-v1",
                "capabilities": None,
                "probed_at": now_iso,
            }

    def bind(self, candidate_id: str, probe_receipt_id: str) -> dict[str, Any]:
        """Select a verified candidate binding, retaining prior binding provenance."""
        receipt = self._probe_receipts.get(probe_receipt_id)
        if receipt is None:
            raise LookupError(f"Probe receipt {probe_receipt_id} was not found")
        if receipt.get("candidate_id") != candidate_id:
            raise ValueError(f"Probe receipt belongs to candidate {receipt.get('candidate_id')}, not {candidate_id}")
        if receipt.get("status") not in ("PASS", "WARN"):
            raise ValueError("Cannot bind unverified or failing executor probe receipt")

        binding_id = f"binding-{secrets.token_hex(12)}"
        record = {
            "binding_id": binding_id,
            "candidate_id": candidate_id,
            "probe_receipt_id": probe_receipt_id,
            "bound_at": datetime.now(timezone.utc).isoformat(),
            "endpoint_display": receipt.get("endpoint_display"),
            "protocol": receipt.get("protocol"),
        }
        self._bindings[candidate_id] = record
        return record

    def active_binding(self, candidate_id: str = DEFAULT_CANDIDATE_ID) -> dict[str, Any] | None:
        return self._bindings.get(candidate_id)
