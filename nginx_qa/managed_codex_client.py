"""Thin authenticated Codex client for the native Managed Sprint Engine.

The engine alone assigns work and advances graphs. This service only remembers
transport receipts, owned CLI process attempts and immutable submission payloads.
v1 deliberately admits operator-configured documentation-only test projects.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
import hashlib
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import tempfile
import threading
import time

import jsonschema

from .control_secrets import sanitized_child_environment, trusted_control_resolver
from .legacy_scope_control import ROLE_TOKEN_HEADER
from .process_supervisor import _WindowsJob, _spawn_suspended_in_windows_job
from .sprint_types import SprintProvenance
from .setup_worker_adapter import (
    PROTOCOL, atomic_json, bounded_json_request, strict_object, validate_worker_endpoint,
)

_DESCRIPTOR = {"protocol", "operation_id", "project_id", "sprint_id", "agent_phones"}
_SAFE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_SHA = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})")
_CAPS = {
    "kind": "managed_client_capabilities", "schema_version": 1,
    "client_id": "codex-cli-native-managed-v1", "adapter_version": "1.0.0",
    "protocol": PROTOCOL, "supported_actions": ["prepare", "start", "stop"],
    "declared_guarantees": {
        "durable_deduplication": True, "restart_resume": True,
        "independent_reviewers": True, "session_quiescence_on_stop": True,
    },
}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def contained(path, parent):
    resolved, anchor = Path(path).resolve(strict=True), Path(parent).resolve(strict=True)
    if resolved == anchor or anchor not in resolved.parents:
        raise ValueError("WORKSPACE_OUTSIDE_ALLOWED_ROOT")
    # Resolve rejects a junction escaping the configured tree; reject every
    # reparse component too, so a configured directory is never an alias.
    current = Path(path).absolute()
    while current != anchor and current != current.parent:
        if current.is_symlink() or (hasattr(current, "is_junction") and current.is_junction()):
            raise ValueError("WORKSPACE_ALIAS_FORBIDDEN")
        current = current.parent
    return resolved


class NativeAPI:
    def __init__(self, url, role_refs, resolver):
        validate_worker_endpoint(url)
        self.url, self.refs, self.resolver = url, role_refs, resolver

    def request(self, phone, route, payload=None, *, allow_missing=False):
        headers = {}
        if phone in self.refs:
            reference = self.refs[phone]
            if not re.fullmatch(r"env:NGINX_QA_SCOPE_CONTROL_[A-Z0-9_]+", reference):
                raise ValueError("ROLE_REFERENCE_INVALID")
            token = os.environ.get(reference[4:], "")
            if len(token) < 16 or "\r" in token or "\n" in token:
                raise ValueError("ROLE_CREDENTIAL_UNAVAILABLE")
            headers[ROLE_TOKEN_HEADER] = token
        status, response = bounded_json_request(self.url, route,
            method="GET" if payload is None else "POST", payload=payload,
            headers=headers, max_bytes=2 * 1024 * 1024, timeout=10)
        if allow_missing and status == 404:
            return {}
        if status != 200 or not isinstance(response, dict):
            raise ValueError("NATIVE_API_REJECTED")
        return response

    def identity(self, project, phone):
        return self.request(phone, f"/api/v1/projects/{project}/agents/{phone}/whoami", {}, allow_missing=True)


class CodexRunner:
    """Windows Job ownership, finite resources and a fresh CLI context per role."""
    def __init__(self, node_path, codex_script, *, timeout=600):
        self.node = str(Path(node_path).resolve(strict=True))
        self.script = str(Path(codex_script).resolve(strict=True))
        self.timeout = min(max(timeout, 30), 1800)
        self.environment = sanitized_child_environment()
        for key in list(self.environment):
            if key.upper() in {"OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GEMINI_API_KEY"}:
                self.environment.pop(key)
        self._slots = threading.BoundedSemaphore(2)

    def ready(self):
        if os.name != "nt":
            raise ValueError("WINDOWS_JOB_OWNERSHIP_REQUIRED")
        check = subprocess.run([self.node, self.script, "login", "status"],
            capture_output=True, timeout=10, env=self.environment,
            creationflags=subprocess.CREATE_NO_WINDOW)
        if check.returncode or b"Logged in" not in check.stdout + check.stderr:
            raise ValueError("CODEX_LOGIN_REQUIRED")

    def quiesce(self, job_name):
        job = _WindowsJob.open(job_name)
        if job is None:
            return
        try:
            job.terminate()
            deadline = time.monotonic() + 5
            while job.active_process_ids():
                if time.monotonic() >= deadline:
                    raise ValueError("CLIENT_QUIESCENCE_UNPROVEN")
                time.sleep(0.05)
        finally:
            job.close()

    def run(self, *, cwd, prompt, schema, directory, job_name, stop):
        while not self._slots.acquire(timeout=0.1):
            if stop.is_set():
                raise ValueError("CLIENT_STOPPING")
        job, child = None, None
        try:
            if stop.is_set():
                raise ValueError("CLIENT_STOPPING")
            directory.mkdir(parents=True, exist_ok=True)
            prompt_path, schema_path = directory / "prompt.txt", directory / "output-schema.json"
            output_path, events_path = directory / "result.json", directory / "events.jsonl"
            prompt_path.write_text(prompt, encoding="utf-8")
            atomic_json(schema_path, schema)
            output_path.unlink(missing_ok=True)
            job = _WindowsJob.create(job_name, {
                "memory_bytes": 2147483648, "process_count": 24, "cpu_percent": 50})
            argv = [self.node, self.script, "exec", "--ignore-user-config", "--ignore-rules",
                "--ephemeral", "--json", "-c", 'approval_policy="never"',
                "-c", "sandbox_workspace_write.network_access=false",
                "-c", 'web_search="disabled"', "--disable", "multi_agent",
                "--disable", "apps", "--disable", "browser_use", "--disable", "computer_use",
                "--disable", "in_app_browser", "-s", "read-only" if schema["title"] == "review" else "workspace-write",
                "-C", str(cwd), "--output-schema", str(schema_path), "-o", str(output_path),
                "Read the local task instructions in " + str(prompt_path) + " and perform them exactly. "
                "Return only the required structured result. Do not access credentials or call nginx-qa APIs."]
            with events_path.open("wb") as log:
                child = _spawn_suspended_in_windows_job(job, argv, cwd=str(cwd),
                    environment=self.environment, stdout_stream=log, stderr_stream=log,
                    inherit_recovery_job_handle=False)
                _WindowsJob.resume(child)
                deadline = time.monotonic() + self.timeout
                while child.poll() is None:
                    if stop.wait(0.05) or time.monotonic() >= deadline or events_path.stat().st_size > 4 * 1024 * 1024:
                        raise ValueError("CLI_CANCELLED_OR_RESOURCE_LIMIT")
                if child.returncode != 0:
                    raise ValueError("CLI_EXECUTION_FAILED")
            # Descendants must be gone before a result is eligible for submit.
            job.terminate()
            deadline = time.monotonic() + 5
            while job.active_process_ids():
                if time.monotonic() > deadline:
                    raise ValueError("CLIENT_QUIESCENCE_UNPROVEN")
                time.sleep(0.05)
            if not output_path.exists() or output_path.stat().st_size > 65536:
                raise ValueError("CLI_RESULT_INVALID")
            result = strict_object(output_path.read_bytes())
            jsonschema.Draft202012Validator(schema).validate(result)
            return result
        finally:
            if job:
                job.terminate()
                job.close()
            if child:
                child.wait(timeout=5)
                child.close()
            self._slots.release()


class ManagedCodexClient:
    def __init__(self, config, *, runner=None, api=None, resolver=trusted_control_resolver):
        self.config = deepcopy(config)
        self.root = Path(config["state_dir"]).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.lockfile = (self.root / "service.lock").open("a+b")
        if os.name == "nt":
            import msvcrt
            self.lockfile.seek(0)
            if not self.lockfile.read(1):
                self.lockfile.write(b"1")
                self.lockfile.flush()
            self.lockfile.seek(0)
            msvcrt.locking(self.lockfile.fileno(), msvcrt.LK_NBLCK, 1)
        self.state_path = self.root / "client-receipts.json"
        self.state = strict_object(self.state_path.read_bytes()) if self.state_path.exists() else {"sessions": {}}
        self.lock, self.threads, self.stops = threading.RLock(), {}, {}
        self.thread_context = threading.local()
        self.resolver = resolver
        self.runner = runner or CodexRunner(config["node_path"], config["codex_script"],
                                            timeout=config.get("role_timeout_seconds", 600))
        self.api = api or NativeAPI(config["engine_url"], config.get("role_secret_refs", {}), resolver)
        self.validators = {}
        for kind, name in (("identity", "managed-current-identity-response-v1"),
                           ("assignment", "managed-assignment-result-response-v1"),
                           ("review", "managed-review-decision-response-v1")):
            path = Path(__file__).resolve().parents[1] / "schemas" / (name + ".schema.json")
            self.validators[kind] = jsonschema.Draft202012Validator(json.loads(path.read_text(encoding="utf-8")))
        self.projects = {}
        for project, profile in config["projects"].items():
            if not project.isdigit() or profile["profile_id"] != "docs-only-v1":
                raise ValueError("CLIENT_PROJECT_PROFILE_FORBIDDEN")
            raw = Path(profile["manifest_path"]).read_bytes()
            manifest_sha256 = hashlib.sha256(raw).hexdigest()
            if manifest_sha256 != profile["manifest_sha256"]:
                raise ValueError("CLIENT_MANIFEST_CHANGED")
            if (not isinstance(profile.get("repository_id"), str) or not profile["repository_id"]
                    or not _SHA.fullmatch(str(profile.get("manifest_commit", "")))
                    or not isinstance(profile.get("git_manifest_path"), str)
                    or not profile["git_manifest_path"] or profile["git_manifest_path"].startswith("/")
                    or "\\" in profile["git_manifest_path"] or ":" in profile["git_manifest_path"]
                    or any(part in {"", ".", ".."} for part in profile["git_manifest_path"].split("/"))):
                raise ValueError("CLIENT_MANIFEST_PROVENANCE_REQUIRED")
            expected_sprint = SprintProvenance(project_id=project,
                repository_id=profile["repository_id"], commit=profile["manifest_commit"],
                manifest_path=profile["git_manifest_path"], manifest_sha256=manifest_sha256).stable_sprint_id()
            manifest = strict_object(raw)
            nodes = [node for node in manifest["nodes"] if node.get("type", "task") != "terminal"]
            phones = sorted({node["agent"]["phone"] for node in nodes} |
                            {reviewer["phone"] for reviewer in manifest["execution"]["reviewers"]})
            if len(phones) > 12 or len(manifest["execution"]["reviewers"]) != 2:
                raise ValueError("CLIENT_ROLE_LIMIT")
            allowed = profile.get("allowed_document_paths", [])
            if not allowed or any(not p.startswith("docs/") or ".." in Path(p).parts or "\\" in p for p in allowed):
                raise ValueError("CLIENT_DOCUMENT_ALLOWLIST_REQUIRED")
            self.projects[project] = {**profile, "manifest": manifest, "phones": phones,
                                      "expected_sprint_id": expected_sprint}
        # Reclaim only named jobs recorded before our own process launches.
        for session in self.state["sessions"].values():
            for attempt in session.get("assignments", {}).values():
                if attempt.get("phase") == "EXECUTING":
                    self.runner.quiesce(attempt["job_name"])
                    attempt["phase"] = "INTERRUPTED"
        self._save()

    def _save(self):
        atomic_json(self.state_path, self.state)

    def authenticate(self, header):
        expected = self.resolver(self.config["control_secret_ref"])
        if not isinstance(expected, str) or len(expected) < 16:
            return False
        return hmac.compare_digest(header or "", "Bearer " + expected)

    def _descriptor(self, payload, extra=(), *, require_admission=True):
        if set(payload) != _DESCRIPTOR | set(extra):
            raise ValueError("CLIENT_DESCRIPTOR_INVALID")
        descriptor = {key: deepcopy(payload[key]) for key in _DESCRIPTOR}
        if (descriptor["protocol"] != PROTOCOL
                or not _SAFE.fullmatch(str(descriptor["operation_id"]))
                or not re.fullmatch(r"msv1-[0-9a-f]{64}", str(descriptor["sprint_id"]))):
            raise ValueError("CLIENT_DESCRIPTOR_INVALID")
        project = self.projects.get(descriptor["project_id"])
        if require_admission and (project is None or descriptor["agent_phones"] != project["phones"]):
            raise ValueError("CLIENT_PROJECT_NOT_ALLOWED")
        if require_admission and descriptor["sprint_id"] != project["expected_sprint_id"]:
            raise ValueError("CLIENT_SPRINT_PROVENANCE_MISMATCH")
        return descriptor

    def _session(self, descriptor):
        session_id = "client-" + digest(descriptor)[:40]
        session = self.state["sessions"].get(session_id)
        if session and session["descriptor"] != descriptor:
            raise ValueError("CLIENT_SESSION_CONFLICT")
        return session_id, session

    def prepare(self, payload):
        descriptor = self._descriptor(payload)
        self.runner.ready()
        with self.lock:
            session_id, session = self._session(descriptor)
            if session is None:
                active = [s for s in self.state["sessions"].values() if s["phase"] not in ("STOPPED", "TERMINAL")]
                if len(active) >= 2 or any(s["descriptor"]["project_id"] == descriptor["project_id"] for s in active):
                    raise ValueError("CLIENT_PROJECT_BUSY")
                session = {"descriptor": descriptor, "phase": "READY", "assignments": {},
                           "config_fingerprint": digest(self.config)}
                self.state["sessions"][session_id] = session
                self._save()
            if session["phase"] in ("STOPPING", "STOPPED"):
                raise ValueError("CLIENT_SESSION_STOPPED")
            if session["config_fingerprint"] != digest(self.config):
                raise ValueError("CLIENT_CONFIGURATION_CHANGED")
            return {**descriptor, "status": "READY", "client_session_id": session_id}

    def start(self, payload):
        descriptor = self._descriptor(payload, ("initial_assignment_ids", "client_session_id"))
        ids = payload["initial_assignment_ids"]
        if not isinstance(ids, list) or not ids or len(ids) > 12 or not all(isinstance(i, str) and _SAFE.fullmatch(i) for i in ids) or len(set(ids)) != len(ids):
            raise ValueError("CLIENT_INITIAL_ASSIGNMENTS_INVALID")
        with self.lock:
            session_id, session = self._session(descriptor)
            if session is None or payload["client_session_id"] != session_id:
                raise ValueError("CLIENT_SESSION_UNKNOWN")
            if session["config_fingerprint"] != digest(self.config):
                raise ValueError("CLIENT_CONFIGURATION_CHANGED")
            if session["phase"] in ("STOPPING", "STOPPED", "TERMINAL"):
                raise ValueError("CLIENT_SESSION_STOPPED")
            if session.get("initial_assignment_ids") not in (None, ids):
                raise ValueError("CLIENT_ASSIGNMENTS_CHANGED")
            if session["phase"] == "READY":
                live = []
                for phone in descriptor["agent_phones"]:
                    identity = self.api.identity(descriptor["project_id"], phone)
                    if identity.get("managed") is True and identity.get("sprint_id") == descriptor["sprint_id"] and identity.get("project_id") == descriptor["project_id"]:
                        self.validators["identity"].validate(identity)
                        live.append(identity["assignment"]["assignment_id"])
                if not set(ids).issubset(live):
                    raise ValueError("CLIENT_NATIVE_ASSIGNMENTS_UNPROVEN")
                session.update(phase="ACCEPTED", initial_assignment_ids=list(ids))
                self._save()
            self._ensure_thread(session_id)
            return {**descriptor, "initial_assignment_ids": ids, "status": "ACCEPTED",
                    "client_session_id": session_id}

    def _ensure_thread(self, session_id):
        if session_id in self.threads and self.threads[session_id].is_alive():
            return
        self.stops[session_id] = threading.Event()
        thread = threading.Thread(target=self._loop, args=(session_id,), daemon=True)
        self.threads[session_id] = thread
        thread.start()

    def resume(self):
        with self.lock:
            for session_id, session in self.state["sessions"].items():
                if session["phase"] == "ACCEPTED":
                    if session["config_fingerprint"] != digest(self.config):
                        session["last_error"] = "CLIENT_CONFIGURATION_CHANGED"
                    else:
                        self._ensure_thread(session_id)
            self._save()

    def stop(self, payload):
        # Current operator authentication may stop an exact previously owned
        # session even after admission policy changes; it cannot create work.
        descriptor = self._descriptor(payload, ("client_session_id",), require_admission=False)
        with self.lock:
            session_id, session = self._session(descriptor)
            if session is None or session_id != payload["client_session_id"]:
                raise ValueError("CLIENT_SESSION_UNKNOWN")
            session["phase"] = "STOPPING"
            self._save()
            event = self.stops.get(session_id)
            if event:
                event.set()
            thread = self.threads.get(session_id)
        if thread:
            thread.join(timeout=15)
            if thread.is_alive():
                raise ValueError("CLIENT_QUIESCENCE_UNPROVEN")
        for attempt in session["assignments"].values():
            if attempt.get("job_name"):
                self.runner.quiesce(attempt["job_name"])
        with self.lock:
            session["phase"] = "STOPPED"
            self._save()
        return {**descriptor, "status": "STOPPED", "client_session_id": session_id}

    def _git(self, root, *args):
        argv = [shutil.which("git") or "git", "--no-replace-objects", "-c", "core.hooksPath=" + str(self.root / "empty-hooks"),
                "-C", str(root), *args]
        environment = {**sanitized_child_environment(), "GIT_TERMINAL_PROMPT": "0", "GCM_INTERACTIVE": "never"}
        if os.name != "nt":
            completed = subprocess.run(argv, capture_output=True, timeout=60, env=environment)
            raw, code = completed.stdout, completed.returncode
        else:
            # No inherited Job handle: a service crash kills Git and helpers.
            # These short fixed-argv calls cannot outlive their service owner.
            job = _WindowsJob.create("Local\\nq-client-git-" + secrets.token_hex(12),
                {"memory_bytes": 536870912, "process_count": 12, "cpu_percent": 50})
            child = None
            try:
                with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
                    child = _spawn_suspended_in_windows_job(job, argv, cwd=str(root), environment=environment,
                        stdout_stream=stdout, stderr_stream=stderr, inherit_recovery_job_handle=False)
                    _WindowsJob.resume(child)
                    deadline = time.monotonic() + 60
                    while child.poll() is None:
                        stop = getattr(self.thread_context, "stop", None)
                        if ((stop and stop.is_set()) or time.monotonic() > deadline
                                or os.fstat(stdout.fileno()).st_size + os.fstat(stderr.fileno()).st_size > 1048576):
                            raise ValueError("CLIENT_GIT_CANCELLED_OR_LIMIT")
                        time.sleep(0.02)
                    stdout.seek(0)
                    raw, code = stdout.read(1048577), child.returncode
            finally:
                job.terminate()
                if child:
                    child.wait(timeout=5)
                    child.close()
                job.close()
        if code or len(raw) > 1048576:
            raise ValueError("CLIENT_GIT_CHECK_FAILED")
        return raw.decode("utf-8").rstrip("\r\n")

    def _workspace(self, identity, project):
        workspace = identity["workspace"]
        root = contained(workspace["expected_root"], project["workspace_root"])
        if Path(workspace["actual_git_toplevel"]).resolve() != root or Path(self._git(root, "rev-parse", "--show-toplevel")).resolve() != root:
            raise ValueError("CLIENT_WORKSPACE_IDENTITY_MISMATCH")
        branch = workspace["assigned_branch"]
        if branch is None or not branch.startswith(project["branch_prefix"]) or self._git(root, "branch", "--show-current") != branch:
            raise ValueError("CLIENT_BRANCH_IDENTITY_MISMATCH")
        if self._git(root, "remote", "get-url", "origin") != project["repository_url"]:
            raise ValueError("CLIENT_REPOSITORY_MISMATCH")
        return root

    def _changed_docs(self, root, project):
        raw = self._git(root, "status", "--porcelain", "--untracked-files=all", "--ignored=matching")
        paths = []
        for line in raw.splitlines():
            # Reject renames, quoted paths, deletions and all non-allowlisted files.
            path = line[3:]
            if (line[:2] == "!!" or "R" in line[:2] or "D" in line[:2] or path not in project["allowed_document_paths"]
                    or not contained(root / path, root).is_file()):
                raise ValueError("CLIENT_DOCS_SCOPE_VIOLATION")
            paths.append(path)
        return paths

    def _committed_docs(self, root, project, initial_head):
        """Check the full committed delta; a clean index is not a scope proof."""
        if not isinstance(initial_head, str) or not _SHA.fullmatch(initial_head):
            raise ValueError("CLIENT_INITIAL_HEAD_INVALID")
        self._git(root, "merge-base", "--is-ancestor", initial_head, "HEAD")
        history = self._git(root, "rev-list", "--parents", "--max-count=33", initial_head + "..HEAD").splitlines()
        if len(history) > 32 or any(len(line.split()) != 2 for line in history):
            raise ValueError("CLIENT_COMMITTED_HISTORY_UNSUPPORTED")
        ranges = [(initial_head, "HEAD"), *((line.split()[1], line.split()[0]) for line in history)]
        for before, after in ranges:
            raw = self._git(root, "diff", "--raw", "--no-abbrev", "--no-renames",
                            "--no-ext-diff", "--no-textconv", "-z", before, after, "--")
            self._validate_doc_delta(root, project, raw)

    @staticmethod
    def _validate_doc_delta(root, project, raw):
        entries = raw.split("\0")
        if entries[-1] != "" or len(entries) % 2 != 1:
            raise ValueError("CLIENT_COMMITTED_SCOPE_VIOLATION")
        for header, path in zip(entries[0:-1:2], entries[1:-1:2]):
            match = re.fullmatch(r":([0-7]{6}) ([0-7]{6}) [0-9a-f]{40,64} [0-9a-f]{40,64} ([AM])", header)
            if (not match or match[2] not in {"100644", "100755"}
                    or match[1] not in {"000000", "100644", "100755"}
                    or path not in project["allowed_document_paths"]
                    or not contained(root / path, root).is_file()):
                raise ValueError("CLIENT_COMMITTED_SCOPE_VIOLATION")

    def _submission_fence(self, session_id):
        session = self.state["sessions"][session_id]
        if self.stops[session_id].is_set() or session["phase"] != "ACCEPTED":
            raise ValueError("CLIENT_STOPPING")
        if session["config_fingerprint"] != digest(self.config):
            raise ValueError("CLIENT_CONFIGURATION_CHANGED")
        self._descriptor(session["descriptor"])
        return session

    def _replay_pending(self, session_id):
        """Recover exact request/ACK loss before discovering newly issued roles."""
        session = self._submission_fence(session_id)
        descriptor = session["descriptor"]
        for assignment_id, attempt in list(session["assignments"].items()):
            if attempt.get("phase") != "SUBMITTING":
                continue
            with self.lock:
                self._submission_fence(session_id)
                payload = attempt.get("payload")
                phone, kind = attempt.get("phone"), attempt.get("kind")
                if (phone not in descriptor["agent_phones"] or kind not in {"assignment", "review"}
                        or not isinstance(payload, dict) or payload.get("assignment_id") != assignment_id):
                    raise ValueError("CLIENT_RETAINED_SUBMISSION_INVALID")
                # Native result/review replay reauthorizes the role and exact
                # scope. Current identity may already have advanced after ACK loss.
                route = f"/api/v1/projects/{descriptor['project_id']}/agents/{phone}/whoami"
                receipt = self.api.request(phone, route, deepcopy(payload))
                self._validate_receipt(kind, payload, receipt)
                self._submission_fence(session_id)
                attempt.update(phase="SUBMITTED", native_receipt=receipt)
                session.pop("last_error", None)
                self._save()

    def _loop(self, session_id):
        session = self.state["sessions"][session_id]
        descriptor = session["descriptor"]
        stop = self.stops[session_id]
        self.thread_context.stop = stop
        while not stop.is_set():
            progressed = False
            try:
                self._replay_pending(session_id)
                for phone in descriptor["agent_phones"]:
                    if stop.is_set():
                        return
                    identity = self.api.identity(descriptor["project_id"], phone)
                    if identity.get("managed") is not True or identity.get("sprint_id") != descriptor["sprint_id"] or identity.get("project_id") != descriptor["project_id"]:
                        continue
                    self.validators["identity"].validate(identity)
                    assignment = identity["assignment"]
                    assignment_id = assignment["assignment_id"]
                    previous = session["assignments"].get(assignment_id)
                    if previous and previous.get("phase") == "SUBMITTED":
                        continue
                    self._execute(session_id, phone, identity)
                    progressed = True
                view = self.api.request("", f"/api/v1/projects/{descriptor['project_id']}/sprints/{descriptor['sprint_id']}/observability")
                if view.get("execution", {}).get("status") in {"completed", "cancelled", "stopped", "failed", "blocked"}:
                    with self.lock:
                        session["phase"] = "TERMINAL"
                        session["native_terminal_status"] = view["execution"]["status"]
                        self._save()
                    return
            except Exception as exc:
                with self.lock:
                    # Do not leak model output, credentials or HTTP response bodies.
                    session["last_error"] = str(exc) if re.fullmatch(r"[A-Z0-9_]{1,80}", str(exc)) else type(exc).__name__
                    self._save()
                # A failed/ambiguous execution is retained for operator diagnosis;
                # do not silently rerun agents forever.
                return
            if not progressed:
                stop.wait(2)

    def _execute(self, session_id, phone, identity):
        session = self.state["sessions"][session_id]
        descriptor, stop = session["descriptor"], self.stops[session_id]
        project = self.projects[descriptor["project_id"]]
        assignment, kind = identity["assignment"], identity["assignment_kind"]
        assignment_id = assignment["assignment_id"]
        if not _SAFE.fullmatch(assignment_id) or kind not in {"assignment", "review"}:
            raise ValueError("CLIENT_ROLE_REQUIRES_OPERATOR")
        route = f"/api/v1/projects/{descriptor['project_id']}/agents/{phone}/whoami"
        previous = session["assignments"].get(assignment_id)
        if previous and previous.get("payload"):
            self._replay_pending(session_id)
            return
        nodes = project["manifest"]["nodes"]
        if kind == "assignment":
            node = next((n for n in nodes if n.get("id") == assignment.get("node_id")), None)
            if not node or node["agent"]["phone"] != phone:
                raise ValueError("CLIENT_TASK_NOT_ALLOWLISTED")
            root = self._workspace(identity, project)
            if previous is None and self._git(root, "rev-parse", "HEAD") != assignment["initial_head_commit"]:
                raise ValueError("CLIENT_INITIAL_HEAD_MISMATCH")
            self._committed_docs(root, project, assignment["initial_head_commit"])
            self._changed_docs(root, project)
            task = {"agent": node["agent"], "tasks": node["tasks"],
                    "allowed_outcomes": assignment["allowed_outcomes"]}
            fields = {"status": {"enum": assignment["allowed_outcomes"]},
                      "summary": {"type": "string", "minLength": 1, "maxLength": 16000}}
        else:
            source = session["assignments"].get(assignment["source_assignment_id"])
            if not source or source.get("phase") != "SUBMITTED":
                raise ValueError("CLIENT_REVIEW_SOURCE_UNAVAILABLE")
            root = contained(source["workspace_root"], project["workspace_root"])
            if self._git(root, "rev-parse", "HEAD") != assignment["result_commit"] or self._git(root, "status", "--porcelain"):
                raise ValueError("CLIENT_REVIEW_COMMIT_CHANGED")
            reviewer = next((r for r in project["manifest"]["execution"]["reviewers"] if r["phone"] == phone), None)
            if reviewer is None:
                raise ValueError("CLIENT_REVIEWER_NOT_ALLOWLISTED")
            task = {"reviewer": reviewer, "source_task": source["task"],
                    "from_commit": source["payload"]["from_commit"], "result_commit": assignment["result_commit"],
                    "result_summary": source["payload"]["result"]}
            fields = {"status": {"enum": ["APPROVE", "REJECT"]},
                      "summary": {"type": "string", "minLength": 1, "maxLength": 16000}}
        scope = identity.get("effective_scope")
        if scope:
            task["effective_scope"] = scope
            if scope.get("requires_scope_ack"):
                self.api.request(phone, f"/api/v1/projects/{descriptor['project_id']}/assignments/{assignment_id}/effective-scope/ack",
                    {"schema_version": 1, "scope_context": scope["scope_context"]})
        elif assignment.get("graph_revision", 1) != 1:
            raise ValueError("CLIENT_UPDATED_TASK_SCOPE_REQUIRED")
        schema = {"type": "object", "title": "review" if kind == "review" else "author",
                  "properties": fields, "required": ["status", "summary"], "additionalProperties": False}
        prompt = ("You are executing exactly one native nginx-qa assignment. The native engine owns the graph.\n"
            "Do not create agents, change branches, commit, push, start servers, change credentials, or submit HTTP results.\n"
            "Do not edit any file except the following operator-approved documentation paths: "
            + json.dumps(project["allowed_document_paths"]) + ".\n"
            + ("You are an independent reviewer. Read the exact source diff and verify requirements. "
               "Do not modify files. Decide APPROVE or REJECT based on evidence; never assume success.\n"
               if kind == "review" else "Implement and verify the assigned documentation task in this workspace.\n")
            + "Effective scope supersedes any conflicting original task. Report actual checks and limitations.\n"
            + json.dumps(task, ensure_ascii=False, indent=2))
        if len(prompt.encode()) > 256000:
            raise ValueError("CLIENT_PROMPT_LIMIT")
        job_name = "Local\\nq-codex-" + digest([session_id, assignment_id])[:40]
        attempt = {"phase": "EXECUTING", "phone": phone, "kind": kind, "job_name": job_name,
                   "workspace_root": str(root), "task": task}
        with self.lock:
            session["assignments"][assignment_id] = attempt
            self._save()
        output = self.runner.run(cwd=root, prompt=prompt, schema=schema,
            directory=self.root / session_id / assignment_id, job_name=job_name, stop=stop)
        if stop.is_set():
            return
        current = self.api.identity(descriptor["project_id"], phone)
        if (current.get("assignment") != identity["assignment"]
                or current.get("sprint_id") != identity["sprint_id"]
                or (current.get("effective_scope") or {}).get("scope_context") != (scope or {}).get("scope_context")):
            raise ValueError("CLIENT_ASSIGNMENT_AUTHORITY_CHANGED")
        if kind == "review":
            if self._git(root, "rev-parse", "HEAD") != assignment["result_commit"] or self._git(root, "status", "--porcelain"):
                raise ValueError("CLIENT_REVIEW_COMMIT_CHANGED")
            payload = {"assignment_id": assignment_id, "status": output["status"], "feedback": output["summary"]}
        else:
            self._workspace(identity, project)
            self._committed_docs(root, project, assignment["initial_head_commit"])
            changed = self._changed_docs(root, project)
            if changed:
                self._git(root, "add", "--", *changed)
                self._git(root, "commit", "-m", "docs: complete managed setup self-test")
            commit = self._git(root, "rev-parse", "HEAD")
            if not _SHA.fullmatch(commit) or self._git(root, "status", "--porcelain"):
                raise ValueError("CLIENT_COMMIT_INVALID")
            self._committed_docs(root, project, assignment["initial_head_commit"])
            self._submission_fence(session_id)
            branch = identity["workspace"]["assigned_branch"]
            self._git(root, "push", "origin", "HEAD:refs/heads/" + branch)
            payload = {"assignment_id": assignment_id, "status": output["status"], "result": output["summary"],
                       "from_commit": assignment["initial_head_commit"], "git_commit": commit, "git_branch": branch}
        if scope:
            payload["scope_context"] = scope["scope_context"]
        with self.lock:
            attempt.update(phase="SUBMITTING", payload=payload)
            self._save()
        if stop.is_set():
            return
        self._replay_pending(session_id)

    def _validate_receipt(self, kind, payload, receipt):
        self.validators[kind].validate(receipt)
        if receipt["assignment_id"] != payload["assignment_id"]:
            raise ValueError("CLIENT_NATIVE_RECEIPT_MISMATCH")
        if kind == "assignment" and (receipt["result_commit"] != payload["git_commit"] or receipt["outcome"] != payload["status"]):
            raise ValueError("CLIENT_NATIVE_RECEIPT_MISMATCH")
        if kind == "review" and receipt["decision"] != payload["status"]:
            raise ValueError("CLIENT_NATIVE_RECEIPT_MISMATCH")


def serve(config):
    client = ManagedCodexClient(config)
    host, port = validate_worker_endpoint(config["listen_url"])
    slots = threading.BoundedSemaphore(8)
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass
        def handle_one_request(self):
            if not slots.acquire(blocking=False):
                self.close_connection = True
                return
            self.connection.settimeout(5)
            try:
                super().handle_one_request()
            finally:
                slots.release()
        def reply(self, status, body):
            raw = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
        def authorized(self):
            if not client.authenticate(self.headers.get("Authorization")):
                self.reply(401, {"error": "AUTH_REQUIRED"})
                return False
            return True
        def do_GET(self):
            if not self.authorized():
                return
            if self.path == "/managed-client/capabilities":
                self.reply(200, _CAPS)
            else:
                self.reply(404, {"error": "NOT_FOUND"})
        def do_POST(self):
            if not self.authorized():
                return
            action = self.path.removeprefix("/managed-client/")
            if self.path != "/managed-client/" + action or action not in {"prepare", "start", "stop"}:
                self.reply(404, {"error": "NOT_FOUND"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 1 <= length <= 32768 or self.headers.get("Transfer-Encoding"):
                    raise ValueError("CLIENT_REQUEST_INVALID")
                payload = strict_object(self.rfile.read(length))
                self.reply(200, getattr(client, action)(payload))
            except Exception as exc:
                code = str(exc) if re.fullmatch(r"[A-Z0-9_]{1,80}", str(exc)) else "CLIENT_REQUEST_FAILED"
                self.reply(409, {"error": code})
    server = ThreadingHTTPServer((host, port), Handler)
    client.resume()
    try:
        server.serve_forever()
    finally:
        for session in list(client.state["sessions"].values()):
            if session["phase"] not in ("STOPPED", "TERMINAL"):
                sid, _ = client._session(session["descriptor"])
                client.stop({**session["descriptor"], "client_session_id": sid})
        server.server_close()
        client.lockfile.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Operator-owned local configuration JSON")
    args = parser.parse_args()
    serve(strict_object(Path(args.config).read_bytes()))

if __name__ == "__main__":
    main()
