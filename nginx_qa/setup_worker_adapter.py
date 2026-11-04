"""Authenticated worker probes and durable owner-bound binding provenance."""
from __future__ import annotations
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import secrets
import socket
import threading
import time
from typing import Any, Mapping
from urllib.parse import urlsplit
import jsonschema
from .control_secrets import trusted_control_resolver
from .telegram_launcher_execution import ExistingClientHTTPAdapter, ExecutorUnavailable

PROTOCOL = "nginx-qa-managed-client-v1"
_SECRET_REF = re.compile(r"env:NGINX_QA_LAUNCHER_CONTROL_[A-Z0-9_]+")
_SCOPE_FIELDS = ("actor_id", "project_id", "authority_fingerprint")

def validate_worker_endpoint(url: str) -> tuple[str, int]:
    parts = urlsplit(url)
    if (parts.scheme != "http" or parts.username or parts.password
            or parts.path not in ("", "/") or parts.query or parts.fragment
            or not re.fullmatch(r"127\.0\.0\.1:[0-9]{4,5}", parts.netloc)):
        raise ValueError("Worker endpoint must be explicit http://127.0.0.1:<port>")
    if parts.port is None or not 1024 <= parts.port <= 65535:
        raise ValueError("Worker endpoint port must be in range 1024-65535")
    return "127.0.0.1", parts.port

def strict_object(data: bytes) -> dict:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate JSON property")
            result[key] = value
        return result
    value = json.loads(data.decode("utf-8"), object_pairs_hook=pairs,
                       parse_constant=lambda value: (_ for _ in ()).throw(ValueError("nonfinite JSON")))
    if not isinstance(value, dict):
        raise ValueError("JSON object required")
    return value

def atomic_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + secrets.token_hex(6) + ".tmp")
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            json.dump(data, stream, ensure_ascii=False, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)

def bounded_json_request(base_url: str, route: str, *, method="GET", payload=None,
                         headers=None, timeout=10, cancel_event=None, max_bytes=32768):
    """No proxy/redirect; one deadline includes slow headers and drip-fed bodies."""
    host, port = validate_worker_endpoint(base_url)
    budget = min(max(float(timeout), 0.05), 30.0)
    connection = http.client.HTTPConnection(host, port, timeout=budget)
    finished, expired = threading.Event(), threading.Event()
    active_socket = [None]
    deadline = time.monotonic() + budget
    def guard():
        while not finished.wait(0.02):
            if time.monotonic() >= deadline or (cancel_event and cancel_event.is_set()):
                expired.set()
                sock = active_socket[0] or connection.sock
                if sock is not None:
                    try:
                        sock.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                connection.close()
                return
    if cancel_event and cancel_event.is_set():
        raise TimeoutError("cancelled")
    watcher = threading.Thread(target=guard, daemon=True)
    watcher.start()
    try:
        request_headers = {"Accept": "application/json", **dict(headers or {})}
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        if body is not None:
            request_headers["Content-Type"] = "application/json"
        connection.request(method, route, body=body, headers=request_headers)
        active_socket[0] = connection.sock
        response = connection.getresponse()
        raw = response.read(max_bytes + 1)
        if expired.is_set():
            raise TimeoutError("deadline or cancellation")
        if len(raw) > max_bytes:
            raise ValueError("response too large")
        return response.status, strict_object(raw) if response.status == 200 else None
    except (OSError, http.client.HTTPException) as exc:
        if expired.is_set():
            raise TimeoutError("deadline or cancellation") from exc
        raise
    finally:
        finished.set()
        connection.close()
        watcher.join(timeout=0.1)

class BoundedExistingClientHTTPAdapter(ExistingClientHTTPAdapter):
    """Keep native exact receipt validation, bound the entire HTTP exchange."""
    def _request(self, action, payload):
        try:
            token = self.secret_resolver(self.credential_reference)
            if not isinstance(token, str) or len(token) < 16 or "\r" in token or "\n" in token:
                raise ValueError("credential unavailable")
            status, response = bounded_json_request(self.base_url, "/managed-client/" + action,
                method="POST", payload=payload, headers={"Authorization": "Bearer " + token}, timeout=10)
            if status != 200:
                raise ValueError("unsuccessful response")
            return response
        except (ValueError, TypeError, LookupError, OSError, RuntimeError, http.client.HTTPException):
            raise ExecutorUnavailable("Existing client service is unavailable") from None


class SetupWorkerAdapter:
    DEFAULT_CANDIDATE_ID = "default-executor"
    def __init__(self, *, environment: Mapping[str, str] | None = None,
                 durable_store_dir=None, secret_resolver=None,
                 probe_ttl_seconds=300, clock=time.time):
        self.env = environment if environment is not None else os.environ
        self.resolver = secret_resolver or trusted_control_resolver
        self.clock, self.ttl = clock, min(max(probe_ttl_seconds, 1), 900)
        self._lock = threading.RLock()
        self.path = Path(durable_store_dir) / "worker-bindings.json" if durable_store_dir else None
        self._state = {"probes": {}, "bindings": {}, "history": []}
        if self.path and self.path.exists():
            self._state = strict_object(self.path.read_bytes())
            if set(self._state) != {"probes", "bindings", "history"}:
                raise ValueError("WORKER_STORE_INVALID")
        schema = json.loads((Path(__file__).resolve().parents[1] /
                            "docs/setup-diagnostics-wizard-v1/contracts.schema.json").read_text(encoding="utf-8"))
        self.validator = jsonschema.Draft202012Validator(
            {"$ref": "#/$defs/WorkerCapabilities", "$defs": schema["$defs"]})

    def _save(self):
        if self.path:
            atomic_json(self.path, self._state)

    def candidate_configuration(self, candidate_id):
        url = self.env.get("NGINX_QA_LAUNCHER_EXECUTOR_URL")
        if candidate_id != self.DEFAULT_CANDIDATE_ID or not url:
            return None
        return {"candidate_id": candidate_id, "url": url,
                "secret_ref": self.env.get("NGINX_QA_LAUNCHER_EXECUTOR_SECRET_REF", "")}

    def _token(self, config):
        reference = config["secret_ref"]
        if not isinstance(reference, str) or not _SECRET_REF.fullmatch(reference):
            raise ValueError("AUTH_NOT_CONFIGURED")
        token = self.resolver(reference)
        if not isinstance(token, str) or len(token) < 16 or "\r" in token or "\n" in token:
            raise ValueError("AUTH_NOT_CONFIGURED")
        return token

    def _fingerprint(self, config):
        data = {**config, "credential_digest": hashlib.sha256(self._token(config).encode()).hexdigest()}
        return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()

    @staticmethod
    def _scope(actor_id, project_id, authority_fingerprint):
        if not all(isinstance(v, str) and v for v in (actor_id, project_id, authority_fingerprint)):
            raise ValueError("WORKER_AUTHORITY_REQUIRED")
        return dict(zip(_SCOPE_FIELDS, (actor_id, project_id, authority_fingerprint)))

    def probe(self, candidate_id, *, timeout=10.0, actor_id="", project_id="",
              authority_fingerprint="", cancel_event=None):
        receipt = {"kind": "worker_probe_receipt", "schema_version": 1,
            "probe_receipt_id": "probe-" + secrets.token_hex(12), "candidate_id": candidate_id,
            "status": "FAIL", "error_code": None, "message": "", "endpoint_display": None,
            "protocol": None, "capabilities": None,
            "probed_at": datetime.fromtimestamp(self.clock(), timezone.utc).isoformat()}
        config = self.candidate_configuration(candidate_id)
        if config is None:
            receipt.update(status="NOT_CONFIGURED", error_code="NOT_CONFIGURED",
                message="No configured managed worker candidate.",
                setup_steps=["Configure a managed-client endpoint and protected control credential reference."])
            return receipt
        try:
            host, port = validate_worker_endpoint(config["url"])
            receipt["endpoint_display"] = f"http://{host}:{port}"
            token, fingerprint = self._token(config), self._fingerprint(config)
        except (ValueError, LookupError, RuntimeError):
            receipt.update(error_code="PROTOCOL_INVALID" if receipt["endpoint_display"] is None else "AUTH_NOT_CONFIGURED",
                message="Worker endpoint or protected credential configuration is invalid.")
            return receipt
        try:
            status, caps = bounded_json_request(config["url"], "/managed-client/capabilities",
                headers={"Authorization": "Bearer " + token}, timeout=timeout, cancel_event=cancel_event)
            if status in (401, 403):
                receipt.update(error_code="AUTH_REJECTED", message="Worker rejected authentication.")
            elif status == 404:
                receipt.update(status="WARN", error_code="PROBE_UNSUPPORTED", protocol=PROTOCOL,
                    message="Worker capabilities unavailable; binding is unavailable.")
            elif status != 200:
                receipt.update(error_code="CONNECTION_ERROR", message="Worker returned an unsuccessful response.")
            else:
                self.validator.validate(caps)
                receipt.update(capabilities=caps, protocol=caps["protocol"])
                if set(caps["supported_actions"]) != {"prepare", "start", "stop"} or not all(caps["declared_guarantees"].values()):
                    receipt.update(error_code="PROTOCOL_UNSUPPORTED", message="Required execution guarantees are unavailable.")
                else:
                    receipt.update(status="PASS", message="Authenticated worker capabilities validated.")
        except (ValueError, UnicodeError, jsonschema.ValidationError):
            receipt.update(error_code="PROTOCOL_INVALID", message="Worker response violates the capabilities contract.")
        except (TimeoutError, OSError, http.client.HTTPException):
            receipt.update(status="UNKNOWN", error_code="CONNECTION_ERROR",
                message="Worker request failed or exceeded its bounded deadline.")
        if receipt["status"] == "PASS":
            with self._lock:
                self._state["probes"][receipt["probe_receipt_id"]] = {
                    "receipt": deepcopy(receipt),
                    "scope": dict(zip(_SCOPE_FIELDS, (actor_id, project_id, authority_fingerprint))),
                    "fingerprint": fingerprint, "expires_at": self.clock() + self.ttl}
                self._save()
        return receipt

    def _verify_proof(self, proof, scope, config, *, check_age=True):
        if proof["scope"] != scope:
            raise ValueError("WORKER_AUTHORITY_CHANGED")
        if check_age and self.clock() >= proof["expires_at"]:
            raise ValueError("WORKER_PROBE_EXPIRED")
        if config is None or proof["fingerprint"] != self._fingerprint(config):
            raise ValueError("WORKER_CONFIGURATION_CHANGED")

    def bind(self, candidate_id, probe_receipt_id, *, actor_id="", project_id="", authority_fingerprint=""):
        scope = self._scope(actor_id, project_id, authority_fingerprint)
        with self._lock:
            proof = self._state["probes"].get(probe_receipt_id)
            if proof is None:
                raise LookupError("WORKER_PROBE_NOT_FOUND")
            if proof["receipt"]["candidate_id"] != candidate_id or proof["receipt"]["status"] != "PASS":
                raise ValueError("WORKER_PROBE_INVALID")
            self._verify_proof(proof, scope, self.candidate_configuration(candidate_id))
            key = project_id + ":" + candidate_id
            old = self._state["bindings"].get(key)
            if old and old["probe_receipt_id"] == probe_receipt_id:
                return deepcopy(old)
            record = {"binding_id": "binding-" + secrets.token_hex(12), "candidate_id": candidate_id,
                "probe_receipt_id": probe_receipt_id,
                "bound_at": datetime.fromtimestamp(self.clock(), timezone.utc).isoformat(),
                "endpoint_display": proof["receipt"]["endpoint_display"], "protocol": PROTOCOL,
                "credential_reference": self.candidate_configuration(candidate_id)["secret_ref"],
                "configuration_fingerprint": proof["fingerprint"],
                **scope, "previous_binding_id": old["binding_id"] if old else None}
            if old:
                self._state["history"].append(old)
            self._state["bindings"][key] = record
            self._save()
            return deepcopy(record)

    def active_binding(self, candidate_id=DEFAULT_CANDIDATE_ID, *,
                       actor_id="", project_id="", authority_fingerprint=""):
        scope = self._scope(actor_id, project_id, authority_fingerprint)
        with self._lock:
            record = self._state["bindings"].get(project_id + ":" + candidate_id)
            if not record:
                return None
            try:
                self._verify_proof(self._state["probes"][record["probe_receipt_id"]],
                                   scope, self.candidate_configuration(candidate_id), check_age=False)
            except (ValueError, LookupError, RuntimeError):
                return None
            return deepcopy(record)

    def native_executor(self, candidate_id=DEFAULT_CANDIDATE_ID, **scope):
        if self.active_binding(candidate_id, **scope) is None:
            raise ValueError("WORKER_BINDING_NOT_CURRENT")
        config = self.candidate_configuration(candidate_id)
        return BoundedExistingClientHTTPAdapter(config["url"], credential_reference=config["secret_ref"],
                                         secret_resolver=self.resolver)

    def original_executor(self, binding_id, *, actor_id, project_id):
        """Resolve original owned transport for Stop; current grants are checked upstream."""
        with self._lock:
            records = [*self._state["bindings"].values(), *self._state["history"]]
            binding = next((r for r in records if r["binding_id"] == binding_id), None)
            if not binding or (binding["actor_id"], binding["project_id"]) != (actor_id, project_id):
                raise PermissionError("ORIGINAL_EXECUTOR_OWNER_MISMATCH")
            proof = self._state["probes"].get(binding["probe_receipt_id"])
            config = self.candidate_configuration(binding["candidate_id"])
            if not proof or config is None or proof["fingerprint"] != self._fingerprint(config):
                raise ValueError("ORIGINAL_EXECUTOR_CONFIGURATION_CHANGED")
            return BoundedExistingClientHTTPAdapter(config["url"], credential_reference=config["secret_ref"],
                                                    secret_resolver=self.resolver)
