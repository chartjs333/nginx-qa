"""Regression tests; fixture clients are explicitly mocked, never live qualification."""
from copy import deepcopy
import hashlib
import http.server
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import MagicMock, patch

from nginx_qa.setup_worker_adapter import SetupWorkerAdapter, bounded_json_request
from nginx_qa.managed_codex_client import ManagedCodexClient, CodexRunner, _CAPS, PROTOCOL, diagnose, main
from nginx_qa.process_supervisor import _WindowsJob, _spawn_suspended_in_windows_job
from nginx_qa.sprint_types import SprintProvenance

TOKEN = "fixture-control-token-0123456789"
SCOPE = dict(actor_id="operator-a", project_id="9000", authority_fingerprint="policy-v1")


class CapabilityFixture:
    def __init__(self, raw=None, status=200, slow=False):
        self.raw = json.dumps(_CAPS).encode() if raw is None else raw
        self.status, self.slow, self.requests = status, slow, []
        owner = self
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                owner.requests.append(self.headers.get("Authorization"))
                if self.headers.get("Authorization") != "Bearer " + TOKEN:
                    self.send_response(401)
                    self.end_headers()
                    return
                self.send_response(owner.status)
                self.send_header("Content-Length", str(len(owner.raw)))
                self.end_headers()
                try:
                    if owner.slow:
                        for char in owner.raw:
                            self.wfile.write(bytes([char]))
                            self.wfile.flush()
                            time.sleep(0.025)
                    else:
                        self.wfile.write(owner.raw)
                except OSError:
                    pass
            def log_message(self, *args):
                pass
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.server.shutdown()
        self.server.server_close()

    @property
    def env(self):
        return {"NGINX_QA_LAUNCHER_EXECUTOR_URL": f"http://127.0.0.1:{self.server.server_port}",
                "NGINX_QA_LAUNCHER_EXECUTOR_SECRET_REF": "env:NGINX_QA_LAUNCHER_CONTROL_TEST"}


class WorkerProbeCorrections(unittest.TestCase):
    def test_missing_secret_fails_before_network(self):
        with CapabilityFixture() as fixture:
            adapter = SetupWorkerAdapter(environment=fixture.env, secret_resolver=lambda ref: "")
            self.assertEqual(adapter.probe("default-executor")["error_code"], "AUTH_NOT_CONFIGURED")
            self.assertEqual(fixture.requests, [])

    def test_malformed_200_never_fabricates_guarantees(self):
        for raw in (b"{}", b"null", b"[]", b'{"protocol":"wrong"}', b'{"kind":"a","kind":"b"}', b"x" * 32769):
            with self.subTest(raw=raw[:40]), CapabilityFixture(raw=raw) as fixture:
                adapter = SetupWorkerAdapter(environment=fixture.env, secret_resolver=lambda ref: TOKEN)
                receipt = adapter.probe("default-executor", **SCOPE)
                self.assertEqual(receipt["status"], "FAIL")
                self.assertEqual(receipt["error_code"], "PROTOCOL_INVALID")
                with self.assertRaises(LookupError):
                    adapter.bind("default-executor", receipt["probe_receipt_id"], **SCOPE)

    def test_capabilities_exact_and_false_guarantees_rejected(self):
        caps = deepcopy(_CAPS)
        caps["declared_guarantees"]["restart_resume"] = False
        with CapabilityFixture(raw=json.dumps(caps).encode()) as fixture:
            receipt = SetupWorkerAdapter(environment=fixture.env, secret_resolver=lambda ref: TOKEN).probe("default-executor", **SCOPE)
            self.assertEqual(receipt["status"], "FAIL")
            self.assertFalse(receipt["capabilities"]["declared_guarantees"]["restart_resume"])

    def test_persisted_binding_scope_expiry_rotation_and_previous_provenance(self):
        with tempfile.TemporaryDirectory() as temporary, CapabilityFixture() as fixture:
            clock = [1000]
            options = dict(environment=fixture.env, secret_resolver=lambda ref: TOKEN,
                           durable_store_dir=temporary, clock=lambda: clock[0], probe_ttl_seconds=5)
            adapter = SetupWorkerAdapter(**options)
            receipt = adapter.probe("default-executor", **SCOPE)
            self.assertEqual(receipt["capabilities"], _CAPS)
            with self.assertRaisesRegex(ValueError, "AUTHORITY_CHANGED"):
                adapter.bind("default-executor", receipt["probe_receipt_id"], **{**SCOPE, "actor_id": "other"})
            binding = adapter.bind("default-executor", receipt["probe_receipt_id"], **SCOPE)
            self.assertEqual(adapter.bind("default-executor", receipt["probe_receipt_id"], **SCOPE), binding)
            clock[0] += 6
            reopened = SetupWorkerAdapter(**options)
            self.assertEqual(reopened.active_binding(**SCOPE), binding)
            with self.assertRaisesRegex(ValueError, "PROBE_EXPIRED"):
                reopened.bind("default-executor", receipt["probe_receipt_id"], **SCOPE)
            current = reopened.probe("default-executor", **SCOPE)
            newer = reopened.bind("default-executor", current["probe_receipt_id"], **SCOPE)
            self.assertEqual(newer["previous_binding_id"], binding["binding_id"])
            reopened.resolver = lambda ref: "rotated-control-token-0123456789"
            self.assertIsNone(reopened.active_binding(**SCOPE))

    def test_unsupported_probe_not_bindable_and_endpoint_redacted(self):
        with CapabilityFixture(status=404) as fixture:
            adapter = SetupWorkerAdapter(environment=fixture.env, secret_resolver=lambda ref: TOKEN)
            result = adapter.probe("default-executor", **SCOPE)
            self.assertEqual(result["status"], "WARN")
            with self.assertRaises(LookupError):
                adapter.bind("default-executor", result["probe_receipt_id"], **SCOPE)
        result = SetupWorkerAdapter(environment={"NGINX_QA_LAUNCHER_EXECUTOR_URL":
             "http://secret:password@example.com:9000"}).probe("default-executor")
        self.assertNotIn("password", json.dumps(result))
        self.assertIsNone(result["endpoint_display"])

    def test_original_stop_transport_survives_policy_narrowing_but_not_owner_or_transport_change(self):
        with CapabilityFixture() as fixture:
            adapter = SetupWorkerAdapter(environment=fixture.env, secret_resolver=lambda ref: TOKEN)
            proof = adapter.probe('default-executor', **SCOPE)
            binding = adapter.bind('default-executor', proof['probe_receipt_id'], **SCOPE)
            changed_scope = {**SCOPE, 'authority_fingerprint':'f'*64}
            self.assertIsNone(adapter.active_binding(**changed_scope))
            self.assertIsNotNone(adapter.original_executor(binding['binding_id'], actor_id=SCOPE['actor_id'], project_id=SCOPE['project_id']))
            with self.assertRaises(PermissionError):
                adapter.original_executor(binding['binding_id'], actor_id='another-owner', project_id=SCOPE['project_id'])
            adapter.resolver = lambda ref: 'rotated-control-token-0123456789'
            with self.assertRaisesRegex(ValueError, 'CONFIGURATION_CHANGED'):
                adapter.original_executor(binding['binding_id'], actor_id=SCOPE['actor_id'], project_id=SCOPE['project_id'])

    def test_total_deadline_and_cancellation_bound_drip_feed(self):
        with CapabilityFixture(slow=True) as fixture:
            started = time.monotonic()
            with self.assertRaises((TimeoutError, OSError)):
                bounded_json_request(fixture.env["NGINX_QA_LAUNCHER_EXECUTOR_URL"],
                    "/managed-client/capabilities", headers={"Authorization": "Bearer " + TOKEN}, timeout=0.12)
            self.assertLess(time.monotonic() - started, 0.8)
        event = threading.Event()
        event.set()
        with self.assertRaises(TimeoutError):
            bounded_json_request("http://127.0.0.1:19001", "/", cancel_event=event)


class FixtureRunner:
    def __init__(self):
        self.calls, self.quiesced = [], []

    def ready(self):
        pass

    def quiesce(self, name):
        self.quiesced.append(name)

    def run(self, **kwargs):
        self.calls.append(kwargs)
        if kwargs["schema"]["title"] == "author":
            path = kwargs["cwd"] / "docs" / "selftest.md"
            path.parent.mkdir(exist_ok=True)
            path.write_text("Verified native-client fixture documentation.\n", encoding="utf-8")
            return {"status": "DONE", "summary": "Fixture author wrote documentation."}
        return {"status": "APPROVE", "summary": "Independent fixture review."}


class CodexSandboxPolicy(unittest.TestCase):
    def test_each_role_has_explicit_network_deny_and_fixed_sandbox(self):
        runner = object.__new__(CodexRunner)
        runner.node, runner.script, runner.timeout = "node-fixture", "codex-fixture", 30
        runner.environment, runner._slots = {}, threading.BoundedSemaphore(1)
        runner.ready = MagicMock()
        for title, mode in (("author", "workspace-write"), ("review", "read-only")):
            with self.subTest(role=title), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                captured = []
                def spawn(job, argv, **kwargs):
                    captured.extend(argv)
                    (root / "result.json").write_text('{"status":"DONE"}')
                    return MagicMock(returncode=0, poll=lambda: 0, wait=lambda **kwargs: 0)
                job = MagicMock(active_process_ids=lambda: frozenset())
                with patch("nginx_qa.managed_codex_client._WindowsJob.create", return_value=job), \
                     patch("nginx_qa.managed_codex_client._WindowsJob.resume"), \
                     patch("nginx_qa.managed_codex_client._spawn_suspended_in_windows_job", side_effect=spawn):
                    runner.run(cwd=root, prompt="Fixture only", schema={"title": title, "type": "object"},
                               directory=root, job_name="fixture", stop=threading.Event())
                self.assertIn("sandbox_workspace_write.network_access=false", captured)
                self.assertEqual(captured[captured.index("-s") + 1], mode)
                self.assertIn('approval_policy="never"', captured)
                self.assertIn("--ignore-user-config", captured)
                self.assertIn("--strict-config", captured)
                self.assertIn('windows.sandbox="mxc"', captured)
                self.assertNotIn('windows.sandbox="unelevated"', captured)
        self.assertEqual(runner.ready.call_count, 2)

    def fixture_runner(self, backend="mxc"):
        runner = object.__new__(CodexRunner)
        runner.node, runner.script = "node-fixture", "codex-fixture"
        runner.sandbox_backend = backend
        runner.environment = {}
        runner.sandbox_diagnostic = {"state": "NOT_CHECKED"}
        runner._bounded_local = MagicMock(return_value=subprocess.CompletedProcess([], 0, b"codex-cli 0.162.0", b""))
        return runner

    def test_child_context_overrides_and_credentials_are_not_inherited(self):
        source = {"PATH": "expected", "SystemRoot": "C:/Windows", "CODEX_PERMISSION_PROFILE": "danger-full-access",
                  "codex_internal_originator_override": "parent", "CODEX_HOME": "elsewhere",
                  "CODEX_THREAD_ID": "parent-thread", "OPENAI_BASE_URL": "https://untrusted.invalid",
                  "OPENAI_API_KEY": "secret", "ANTHROPIC_API_KEY": "secret", "GOOGLE_API_KEY": "secret",
                  "GEMINI_API_KEY": "secret", "NGINX_QA_SCOPE_CONTROL_A": "secret",
                  "NGINX_QA_LAUNCHER_CONTROL_A": "secret", "TELEGRAM_BOT_TOKEN": "secret"}
        self.assertEqual(CodexRunner.child_environment(source), {"PATH": "expected", "SystemRoot": "C:/Windows"})

    def test_unsupported_host_is_blocked_without_any_child_or_fallback(self):
        runner = self.fixture_runner()
        with patch.object(runner, "_mxc_os_supported", return_value=False), \
             patch.object(runner, "_prove_native_boundary") as probe:
            with self.assertRaisesRegex(ValueError, "CODEX_SANDBOX_UNAVAILABLE"):
                runner.ready()
        runner._bounded_local.assert_not_called()
        probe.assert_not_called()
        self.assertEqual(runner.sandbox_diagnostic["reason"], "HOST_MXC_UNSUPPORTED")

    def test_elevated_or_weaker_mode_never_provisions_or_requests_uac(self):
        for backend, reason in (("elevated", "ELEVATED_NONINTERACTIVE_ADMISSION_UNSUPPORTED"),
                                ("unelevated", "SANDBOX_BACKEND_UNSUPPORTED")):
            runner = self.fixture_runner(backend)
            with self.assertRaisesRegex(ValueError, "CODEX_SANDBOX_UNAVAILABLE"):
                runner.ready()
            runner._bounded_local.assert_not_called()
            self.assertEqual(runner.sandbox_diagnostic["reason"], reason)

    def test_strict_cli_version_required_before_native_canary(self):
        runner = self.fixture_runner()
        runner._bounded_local.return_value.stdout = b"codex-cli 0.161.0"
        with patch.object(runner, "_mxc_os_supported", return_value=True), \
             patch.object(runner, "_prove_native_boundary") as probe:
            with self.assertRaisesRegex(ValueError, "CODEX_SANDBOX_UNAVAILABLE"):
                runner.ready()
        probe.assert_not_called()
        self.assertEqual(runner.sandbox_diagnostic["reason"], "CLI_STRICT_MXC_UNSUPPORTED")

    def test_canary_and_existing_login_both_required_for_ready(self):
        runner = self.fixture_runner()
        runner._bounded_local.side_effect = [subprocess.CompletedProcess([], 0, b"codex-cli 0.162.0", b""),
                                             subprocess.CompletedProcess([], 0, b"", b"Logged in using ChatGPT")]
        with patch.object(runner, "_mxc_os_supported", return_value=True), \
             patch.object(runner, "_prove_native_boundary") as probe:
            runner.ready()
        probe.assert_called_once()
        self.assertEqual(runner.sandbox_diagnostic["state"], "PASS")
        runner._bounded_local.side_effect = [subprocess.CompletedProcess([], 0, b"codex-cli 0.162.0", b""),
                                             subprocess.CompletedProcess([], 1, b"", b"Not logged in")]
        with patch.object(runner, "_mxc_os_supported", return_value=True), \
             patch.object(runner, "_prove_native_boundary"):
            with self.assertRaisesRegex(ValueError, "CODEX_LOGIN_REQUIRED"):
                runner.ready()
        self.assertEqual(runner.sandbox_diagnostic["state"], "BLOCKED")

    def test_direct_or_resumed_run_cannot_bypass_preflight(self):
        runner = self.fixture_runner()
        runner.ready = MagicMock(side_effect=ValueError("CODEX_SANDBOX_UNAVAILABLE"))
        with patch("nginx_qa.managed_codex_client._WindowsJob.create") as job:
            with self.assertRaisesRegex(ValueError, "CODEX_SANDBOX_UNAVAILABLE"):
                runner.run(cwd=Path.cwd(), prompt="No model", schema={}, directory=Path.cwd(),
                           job_name="must-not-launch", stop=threading.Event())
        job.assert_not_called()

    @unittest.skipUnless(os.name == "nt", "Windows owned preflight cancellation")
    def test_stop_terminates_an_owned_preflight_child_promptly(self):
        runner = self.fixture_runner()
        runner.environment = CodexRunner.child_environment()
        # Use the real bounded helper, not fixture_runner's protocol mock.
        stop = threading.Event()
        timer = threading.Timer(0.15, stop.set)
        timer.start()
        started = time.monotonic()
        try:
            with self.assertRaisesRegex(ValueError, "CLIENT_STOPPING"):
                CodexRunner._bounded_local(runner,
                    [sys.executable, "-c", "import time; time.sleep(20)"], Path.cwd(), stop=stop)
        finally:
            timer.cancel()
        self.assertLess(time.monotonic() - started, 3)

    def test_no_attempt_or_model_claim_never_counts_as_boundary_evidence(self):
        good = {"nonce": "nonce", "executed": True, "read_ok": True, "doc_write_ok": True,
                "socket_attempted": True, "socket_denied": True, "git_write_attempted": True, "git_write_denied": True}
        arguments = dict(role="author", nonce="nonce", before=b"git", after=b"git", connections=[])
        CodexRunner.validate_boundary_evidence(good, **arguments)
        for evidence in ({}, {"attempted": True, "network_denied": True, "git_write_denied": True},
                         {**good, "socket_attempted": False}, {**good, "git_write_attempted": False},
                         {**good, "nonce": "stale"}, {**good, "doc_write_ok": False}):
            with self.assertRaisesRegex(ValueError, "BOUNDARY_UNPROVEN"):
                CodexRunner.validate_boundary_evidence(evidence, **arguments)
        for changed in ({"connections": [True]}, {"after": b"modified"}, {"role": "review"}):
            with self.assertRaisesRegex(ValueError, "BOUNDARY_UNPROVEN"):
                CodexRunner.validate_boundary_evidence(good, **{**arguments, **changed})

    def test_actual_unsandboxed_native_socket_and_git_write_fail_boundary_probe(self):
        # No model: run only our static canary in its own temporary Git fixture.
        # A nominal flag must not turn the deliberately absent sandbox into PASS.
        runner = self.fixture_runner()
        def local(argv, cwd, **kwargs):
            command = argv[argv.index("--") + 1:] if argv[0] == runner.node else argv
            return subprocess.run(command, cwd=cwd, capture_output=True, timeout=8)
        runner._bounded_local = local
        with self.assertRaisesRegex(ValueError, "BOUNDARY_UNPROVEN"):
            runner._prove_native_boundary()

    def test_check_is_sanitized_and_does_not_construct_service_or_resume(self):
        runner = self.fixture_runner()
        runner.ready = MagicMock(side_effect=lambda: runner._sandbox_unavailable("HOST_MXC_UNSUPPORTED"))
        config = {"node_path": "node", "codex_script": "script", "state_dir": "must-not-create"}
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "config.json"
            path.write_text(json.dumps(config))
            output = io.StringIO()
            with patch("nginx_qa.managed_codex_client.CodexRunner", return_value=runner), \
                 patch("nginx_qa.managed_codex_client.ManagedCodexClient") as client, \
                 patch("nginx_qa.managed_codex_client.serve") as server, patch("sys.stdout", output):
                self.assertEqual(main(["--config", str(path), "--diagnose"]), 2)
            client.assert_not_called()
            server.assert_not_called()
            self.assertEqual(json.loads(output.getvalue())["reason"], "HOST_MXC_UNSUPPORTED")
        with patch("nginx_qa.managed_codex_client.CodexRunner", side_effect=ValueError("private-token-secret")):
            result = diagnose(config)
        self.assertNotIn("private-token-secret", json.dumps(result))


@unittest.skipUnless(os.name == "nt", "Windows owned job contract")
class OwnedProcessQuiescence(unittest.TestCase):
    def test_local_fixture_job_is_stopped_with_no_remaining_descendants(self):
        name = "Local\\nq-client-fixture-" + str(os.getpid())
        job = _WindowsJob.create(name, {"memory_bytes": 268435456, "process_count": 4, "cpu_percent": 20})
        child = None
        try:
            with tempfile.TemporaryFile() as output:
                child = _spawn_suspended_in_windows_job(job,
                    [sys.executable, "-c", "import time; time.sleep(20)"], cwd=os.getcwd(),
                    environment=dict(os.environ), stdout_stream=output, stderr_stream=output,
                    inherit_recovery_job_handle=False)
                _WindowsJob.resume(child)
                self.assertIn(child.pid, job.active_process_ids())
                runner = object.__new__(CodexRunner)
                runner.quiesce(name)
                self.assertEqual(job.active_process_ids(), frozenset())
                self.assertIsNotNone(child.wait(timeout=5))
        finally:
            job.terminate()
            job.close()
            if child:
                child.close()


class FixtureNative:
    def __init__(self, identity):
        self.author = identity
        self.submissions = []
        self.reviewed = []

    def identity(self, project, phone):
        if phone == "7101":
            return {} if self.submissions else deepcopy(self.author)
        if self.submissions and phone in {"7191", "7192"} and phone not in self.reviewed:
            return {"managed": True, "project_id": project, "sprint_id": self.author["sprint_id"],
                "assignment_kind": "review", "assignment": {
                    "assignment_id": "review-" + phone, "source_assignment_id": "assignment-one",
                    "result_commit": self.submissions[0]["git_commit"], "reviewer_phone": phone}}
        return {}

    def request(self, phone, route, payload=None):
        if route.endswith("/observability"):
            return {"execution": {"status": "completed" if len(self.reviewed) == 2 else "active"}}
        if phone == "7101":
            if self.submissions:
                if payload != self.submissions[0]:
                    raise ValueError("NATIVE_FIXTURE_IDEMPOTENCY_CONFLICT")
                return {"status": "ALREADY_ACCEPTED", "assignment_id": payload["assignment_id"],
                        "outcome": payload["status"], "result_commit": payload["git_commit"],
                        "result_key": "result-" + "2" * 64, "deduplicated": True}
            self.submissions.append(payload)
            return {"status": "REVIEWS_PENDING", "assignment_id": payload["assignment_id"],
                    "outcome": payload["status"], "result_commit": payload["git_commit"],
                    "result_key": "result-" + "2" * 64, "deduplicated": False}
        self.reviewed.append(phone)
        return {"status": "REVIEW_ACCEPTED", "assignment_id": payload["assignment_id"],
                "source_assignment_id": "assignment-one", "result_key": "result-" + "2" * 64,
                "decision": payload["status"], "deduplicated": False}


class ManagedClientProtocolCorrections(unittest.TestCase):
    def git(self, *args):
        return subprocess.check_output(["git", *args], stderr=subprocess.DEVNULL).decode().strip()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.managed = root / "managed"
        self.workspace = self.managed / "author"
        self.workspace.mkdir(parents=True)
        self.remote = root / "remote.git"
        self.git("init", "--bare", str(self.remote))
        self.git("init", str(self.workspace))
        self.git("-C", str(self.workspace), "config", "user.name", "Fixture")
        self.git("-C", str(self.workspace), "config", "user.email", "fixture@example.invalid")
        (self.workspace / "README.md").write_text("Fixture repository.\n")
        self.git("-C", str(self.workspace), "add", ".")
        self.git("-C", str(self.workspace), "commit", "-m", "initial")
        self.git("-C", str(self.workspace), "checkout", "-b", "agent/selftest-author")
        self.git("-C", str(self.workspace), "remote", "add", "origin", str(self.remote))
        commit = self.git("-C", str(self.workspace), "rev-parse", "HEAD")
        manifest = {"nodes": [
            {"id": "author", "agent": {"id": "author", "phone": "7101"},
             "tasks": [{"message": "Write docs/selftest.md."}]},
            {"id": "coordinator", "agent": {"id": "coord", "phone": "7102"},
             "tasks": [{"message": "Recover only if needed."}]},
            {"id": "done", "type": "terminal"}],
            "execution": {"reviewers": [{"id": "r1", "phone": "7191"}, {"id": "r2", "phone": "7192"}]}}
        path = root / "manifest.json"
        path.write_text(json.dumps(manifest))
        self.config = {"state_dir": str(root / "client"), "engine_url": "http://127.0.0.1:19002",
            "control_secret_ref": "env:NGINX_QA_LAUNCHER_CONTROL_TEST",
            "projects": {"9010": {"profile_id": "docs-only-v1", "manifest_path": str(path),
                "manifest_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "repository_id": "fixture-repo", "manifest_commit": commit,
                "git_manifest_path": "orchestration/docs-selftest.json",
                "workspace_root": str(self.managed), "branch_prefix": "agent/selftest-",
                "repository_url": str(self.remote), "allowed_document_paths": ["docs/selftest.md"]}}}
        expected_sprint = SprintProvenance("9010", "fixture-repo", commit,
            "orchestration/docs-selftest.json", hashlib.sha256(path.read_bytes()).hexdigest()).stable_sprint_id()
        self.descriptor = {"protocol": PROTOCOL, "operation_id": "operation-test", "project_id": "9010",
             "sprint_id": expected_sprint, "agent_phones": ["7101", "7102", "7191", "7192"]}
        identity = {"managed": True, "project_id": "9010", "sprint_id": self.descriptor["sprint_id"],
            "assignment_kind": "assignment", "assignment": {"assignment_id": "assignment-one",
                "node_id": "author", "source_commit": commit, "initial_head_commit": commit,
                "workspace_id": "workspace-one", "allowed_outcomes": ["DONE"]},
            "workspace": {"expected_root": str(self.workspace), "actual_git_toplevel": str(self.workspace),
                "workspace_id": "workspace-one", "assigned_branch": "agent/selftest-author", "source_commit": commit, "initial_head_commit": commit}}
        self.api, self.runner = FixtureNative(identity), FixtureRunner()
        self.client = ManagedCodexClient(self.config, runner=self.runner, api=self.api, resolver=lambda ref: TOKEN)

    def tearDown(self):
        for session in list(self.client.state["sessions"].values()):
            sid, _ = self.client._session(session["descriptor"])
            self.client.stop({**session["descriptor"], "client_session_id": sid})
        self.client.lockfile.close()
        self.temp.cleanup()

    def test_exact_native_receipts_real_git_and_distinct_review_process_attempts(self):
        ready = self.client.prepare(self.descriptor)
        self.assertEqual(set(ready), set(self.descriptor) | {"status", "client_session_id"})
        self.assertEqual(ready, self.client.prepare(self.descriptor))
        start = {**self.descriptor, "client_session_id": ready["client_session_id"],
                 "initial_assignment_ids": ["assignment-one"]}
        accepted = self.client.start(start)
        self.assertEqual(accepted["status"], "ACCEPTED")
        self.assertEqual(set(accepted), set(start) | {"status"})
        self.client.threads[ready["client_session_id"]].join(10)
        self.assertEqual(self.client.state["sessions"][ready["client_session_id"]]["phase"], "TERMINAL",
            self.client.state)
        self.assertEqual(len(self.api.submissions), 1)
        self.assertEqual(self.api.reviewed, ["7191", "7192"])
        self.assertEqual(len({call["job_name"] for call in self.runner.calls}), 3)
        self.assertEqual([c["schema"]["title"] for c in self.runner.calls], ["author", "review", "review"])
        self.assertEqual(self.git("--git-dir", str(self.remote), "rev-parse", "refs/heads/agent/selftest-author"),
                         self.api.submissions[0]["git_commit"])
        stopped = self.client.stop({**self.descriptor, "client_session_id": ready["client_session_id"]})
        self.assertEqual(stopped["status"], "STOPPED")
        self.assertEqual(set(stopped), set(ready))

    def test_no_arbitrary_projects_phones_endpoints_and_unknown_initial_assignment(self):
        for change in ({"project_id": "9000"}, {"agent_phones": ["9999"]}, {"engine_url": "http://evil"}):
            with self.assertRaises(ValueError):
                self.client.prepare({**self.descriptor, **change})
        ready = self.client.prepare(self.descriptor)
        with self.assertRaisesRegex(ValueError, "NATIVE_ASSIGNMENTS_UNPROVEN"):
            self.client.start({**self.descriptor, "client_session_id": ready["client_session_id"],
                               "initial_assignment_ids": ["assignment-not-live"]})
        self.assertEqual(self.runner.calls, [])

    def retained_author_after_failed_publication(self):
        ready = self.client.prepare(self.descriptor)
        sid = ready["client_session_id"]
        with patch.object(self.client, "_publish_commit", side_effect=TimeoutError("fixture publication timeout")):
            self.client.start({**self.descriptor, "client_session_id": sid,
                               "initial_assignment_ids": ["assignment-one"]})
            self.client.threads[sid].join(10)
        attempt = self.client.state["sessions"][sid]["assignments"]["assignment-one"]
        self.assertEqual(attempt["phase"], "OUTPUT_READY")
        self.assertEqual(attempt["validated_commit"], self.git("-C", str(self.workspace), "rev-parse", "HEAD"))
        self.assertEqual(len(self.runner.calls), 1)
        self.assertEqual(self.api.submissions, [])
        return sid

    def test_publication_timeout_restart_reuses_validated_author_then_two_reviews(self):
        sid = self.retained_author_after_failed_publication()
        output = deepcopy(self.client.state["sessions"][sid]["assignments"]["assignment-one"]["validated_output"])
        self.client.lockfile.close()
        self.client = ManagedCodexClient(self.config, runner=self.runner, api=self.api, resolver=lambda ref: TOKEN)
        self.client.resume()
        self.client.threads[sid].join(10)
        self.assertEqual(self.client.state["sessions"][sid]["phase"], "TERMINAL", self.client.state)
        self.assertEqual([call["schema"]["title"] for call in self.runner.calls], ["author", "review", "review"])
        self.assertEqual(self.api.submissions[0]["result"], output["summary"])
        self.assertEqual(self.git("--git-dir", str(self.remote), "rev-parse", "refs/heads/agent/selftest-author"),
                         self.api.submissions[0]["git_commit"])

    def test_retained_output_rejects_dirty_files_commit_drift_and_changed_authority(self):
        sid = self.retained_author_after_failed_publication()
        document = self.workspace / "docs/selftest.md"
        document.write_text("Changed after validated author finished.\n")
        with self.assertRaisesRegex(ValueError, "CLIENT_VALIDATED_OUTPUT_CHANGED"):
            self.client._execute(sid, "7101", deepcopy(self.api.author))
        self.git("-C", str(self.workspace), "add", "docs/selftest.md")
        self.git("-C", str(self.workspace), "commit", "-m", "fixture unassigned drift")
        with self.assertRaisesRegex(ValueError, "CLIENT_VALIDATED_OUTPUT_CHANGED"):
            self.client._execute(sid, "7101", deepcopy(self.api.author))
        self.api.author["assignment"]["allowed_outcomes"] = ["DONE", "BLOCKED_EXTERNAL"]
        with self.assertRaisesRegex(ValueError, "CLIENT_ASSIGNMENT_AUTHORITY_CHANGED"):
            self.client._execute(sid, "7101", deepcopy(self.api.author))
        self.assertEqual(len(self.runner.calls), 1)
        self.assertEqual(self.api.submissions, [])

    def test_configured_pushurl_must_match_operator_repository_before_execution(self):
        self.git("-C", str(self.workspace), "remote", "set-url", "--push", "origin", "https://example.invalid/unapproved.git")
        ready = self.client.prepare(self.descriptor)
        sid = ready["client_session_id"]
        self.client.start({**self.descriptor, "client_session_id": sid,
                           "initial_assignment_ids": ["assignment-one"]})
        self.client.threads[sid].join(10)
        self.assertEqual(self.client.state["sessions"][sid]["last_error"], "CLIENT_REPOSITORY_MISMATCH")
        self.assertEqual(self.runner.calls, [])
        self.assertEqual(self.api.submissions, [])

    def read_only_coordinator_fixture(self):
        self.git("-C", str(self.workspace), "checkout", "--detach")
        project = self.client.projects["9010"]
        project["manifest"]["nodes"][0]["workspace"] = {"access": "read"}
        project["manifest"]["coordinator"] = {"node_id": "author"}
        self.api.author["workspace"]["assigned_branch"] = None
        self.api.author["assignment"]["allowed_outcomes"] = ["RESUME", "BLOCKED_EXTERNAL", "STOP", "NEED_DECISION"]
        def report(**kwargs):
            self.runner.calls.append(kwargs)
            self.assertEqual(kwargs["schema"]["title"], "review", "Read assignments must use the enforced read-only boundary")
            outcomes = kwargs["schema"]["properties"]["status"]["enum"]
            if "BLOCKED_EXTERNAL" in outcomes:
                self.assertEqual(outcomes, ["BLOCKED_EXTERNAL"], "No autonomous operator decision or repeat loop")
                return {"status": "BLOCKED_EXTERNAL", "summary": "Exact missing input: Excel Manager change instruction."}
            return {"status": "APPROVE", "summary": "Read-only fixture result has exact source commit and question."}
        self.runner.run = report

    def start_fixture(self):
        ready = self.client.prepare(self.descriptor)
        sid = ready["client_session_id"]
        self.client.start({**self.descriptor, "client_session_id": sid,
                           "initial_assignment_ids": ["assignment-one"]})
        self.client.threads[sid].join(30)
        self.assertFalse(self.client.threads[sid].is_alive())
        return self.client.state["sessions"][sid]

    def test_read_only_coordinator_reports_exact_source_without_commit_or_publication(self):
        self.read_only_coordinator_fixture()
        head = self.git("-C", str(self.workspace), "rev-parse", "HEAD")
        with patch.object(self.client, "_publish_commit") as publish:
            session = self.start_fixture()
        self.assertEqual(session["phase"], "TERMINAL", session)
        publish.assert_not_called()
        payload = self.api.submissions[0]
        self.assertEqual((payload["from_commit"], payload["git_commit"], payload["git_branch"]), (head, head, None))
        self.assertEqual(payload["status"], "BLOCKED_EXTERNAL")
        self.assertIn("Exact missing input", payload["result"])
        self.assertEqual(self.api.reviewed, ["7191", "7192"])
        self.assertEqual(self.git("-C", str(self.workspace), "status", "--porcelain"), "")
        self.assertEqual(self.git("--git-dir", str(self.remote), "for-each-ref"), "")

    def test_read_only_native_identity_cannot_allow_branch_or_dirty_source(self):
        self.read_only_coordinator_fixture()
        project = self.client.projects["9010"]
        identity = deepcopy(self.api.author)
        identity["workspace"]["assigned_branch"] = "agent/selftest-author"
        with self.assertRaisesRegex(ValueError, "CLIENT_READ_ONLY_WORKSPACE_CHANGED"):
            self.client._workspace(identity, project, read_only=True)
        identity = deepcopy(self.api.author)
        identity["assignment"]["initial_head_commit"] = "f" * 40
        with self.assertRaisesRegex(ValueError, "CLIENT_READ_ONLY_WORKSPACE_CHANGED"):
            self.client._workspace(identity, project, read_only=True)
        (self.workspace / "README.md").write_text("Unapproved change.\n")
        with patch.object(self.client, "_publish_commit") as publish:
            session = self.start_fixture()
        self.assertEqual(session["last_error"], "CLIENT_READ_ONLY_WORKSPACE_CHANGED")
        publish.assert_not_called()
        self.assertEqual(self.runner.calls, [])
        self.assertEqual(self.api.submissions, [])

    def test_read_only_output_cannot_resume_or_change_source(self):
        self.read_only_coordinator_fixture()
        def forbidden_resume(**kwargs):
            return {"status": "RESUME", "summary": "Invented decision must be rejected."}
        self.runner.run = forbidden_resume
        with patch.object(self.client, "_publish_commit") as publish:
            session = self.start_fixture()
        self.assertEqual(session["last_error"], "ValidationError")
        publish.assert_not_called()
        self.assertEqual(self.api.submissions, [])

    def test_read_only_source_is_rechecked_after_model_returns(self):
        self.read_only_coordinator_fixture()
        def compromised_runner(**kwargs):
            (self.workspace / "README.md").write_text("Unexpected mutation outside the read-only runner.\n")
            return {"status": "BLOCKED_EXTERNAL", "summary": "Question."}
        self.runner.run = compromised_runner
        with patch.object(self.client, "_publish_commit") as publish:
            session = self.start_fixture()
        self.assertEqual(session["last_error"], "CLIENT_READ_ONLY_WORKSPACE_CHANGED")
        publish.assert_not_called()
        self.assertEqual(self.api.submissions, [])

    def test_status_is_read_only_exact_descriptor_and_sanitized(self):
        absent = self.client.status(self.descriptor)
        self.assertEqual(absent['phase'], 'NOT_PREPARED')
        self.assertFalse(self.client.state['sessions'])
        ready = self.client.prepare(self.descriptor)
        session = self.client.state['sessions'][ready['client_session_id']]
        session['last_error'] = 'private model output and token=secret'
        session['assignments']['assignment-one'] = {'phase':'EXECUTING', 'payload':{'result':'private'}, 'job_name':'private'}
        before = deepcopy(self.client.state)
        result = self.client.status(self.descriptor)
        self.assertEqual(result['phase'], 'READY')
        self.assertFalse(result['thread_alive'])
        self.assertEqual(result['last_error'], 'CLIENT_EXECUTION_FAILED')
        self.assertEqual(result['assignments'], [{'assignment_id':'assignment-one','phase':'EXECUTING'}])
        self.assertNotIn('private', json.dumps(result))
        self.assertEqual(self.client.state, before)
        self.assertEqual(self.runner.calls, [])
        with self.assertRaises(ValueError):
            self.client.status({**self.descriptor, 'sprint_id':'msv1-'+'f'*64})
        with self.assertRaises(ValueError):
            self.client.status({**self.descriptor, 'payload':{}})

    def test_explicit_execution_retry_quiesces_old_job_and_records_fresh_generation(self):
        normal_run = self.runner.run
        self.runner.run = MagicMock(side_effect=ValueError('FIXTURE_EXECUTION_FAILED'))
        session = self.start_fixture()
        old = deepcopy(session['assignments']['assignment-one'])
        self.assertEqual(session['last_error'], 'FIXTURE_EXECUTION_FAILED')
        self.runner.run = normal_run
        with patch.object(self.runner, 'quiesce', wraps=self.runner.quiesce) as quiesce:
            session = self.start_fixture()
        new = session['assignments']['assignment-one']
        self.assertEqual(session['phase'], 'TERMINAL', session)
        self.assertNotEqual(new['job_name'], old['job_name'])
        self.assertEqual(new['execution_generation'], old['execution_generation'] + 1)
        self.assertEqual(new['previous_attempts'][-1]['job_name'], old['job_name'])
        quiesce.assert_any_call(old['job_name'])
        self.assertEqual(len(self.api.submissions), 1)
        self.assertEqual(self.api.submissions[0]['assignment_id'], 'assignment-one')

    def ignored_report_fixture(self):
        (self.workspace / '.gitignore').write_text('docs/\n')
        self.git('-C',str(self.workspace),'add','.gitignore')
        self.git('-C',str(self.workspace),'commit','-m','Fixture ignored report directory')
        head=self.git('-C',str(self.workspace),'rev-parse','HEAD')
        for record in (self.api.author['assignment'],self.api.author['workspace']):
            record['source_commit']=record['initial_head_commit']=head

    def test_exact_approved_ignored_report_is_committed_but_no_directory_is_forced(self):
        self.ignored_report_fixture()
        session=self.start_fixture()
        self.assertEqual(session['phase'],'TERMINAL',session)
        self.assertEqual(len(self.api.submissions),1)
        self.assertIn('docs/selftest.md',self.git('-C',str(self.workspace),'ls-files'))
        self.assertEqual(self.git('-C',str(self.workspace),'status','--porcelain'), '')

    def test_unapproved_ignored_sibling_still_prevents_publication(self):
        self.ignored_report_fixture()
        normal_run=self.runner.run
        def write_sibling(**kwargs):
            result=normal_run(**kwargs)
            (self.workspace/'docs/private.txt').write_text('Unapproved ignored content')
            return result
        self.runner.run=write_sibling
        with patch.object(self.client,'_publish_commit') as publish:
            session=self.start_fixture()
        self.assertEqual(session['last_error'],'CLIENT_DOCS_SCOPE_VIOLATION')
        publish.assert_not_called()
        self.assertEqual(self.api.submissions,[])

    def test_nul_status_keeps_leading_column_and_exact_path(self):
        project=self.client.projects['9010']
        (self.workspace/'docs').mkdir()
        (self.workspace/'docs/selftest.md').write_text('Allowed content')
        for status in (' M','M ','MM','??','!!'):
            with patch.object(self.client,'_git',return_value=status+' docs/selftest.md\0'):
                self.assertEqual(self.client._changed_docs(self.workspace,project),['docs/selftest.md'])
        for raw in (' M docs/selftest.md', 'R  docs/selftest.md\0README.md\0', '?? docs/other\nfile.md\0'):
            with patch.object(self.client,'_git',return_value=raw), self.assertRaisesRegex(ValueError,'CLIENT_DOCS_SCOPE_VIOLATION'):
                self.client._changed_docs(self.workspace,project)

    def test_wrong_sprint_same_project_and_phones_is_rejected_before_ready(self):
        wrong = {**self.descriptor, "sprint_id": "msv1-" + "f" * 64}
        with self.assertRaisesRegex(ValueError, "SPRINT_PROVENANCE_MISMATCH"):
            self.client.prepare(wrong)
        ready = self.client.prepare(self.descriptor)
        with self.assertRaisesRegex(ValueError, "SPRINT_PROVENANCE_MISMATCH"):
            self.client.start({**wrong, "client_session_id": ready["client_session_id"],
                               "initial_assignment_ids": ["assignment-one"]})
        self.assertEqual(len(self.client.state["sessions"]), 1)
        self.assertEqual(self.runner.calls, [])

    def test_cli_committing_unexpected_file_cannot_push_or_submit(self):
        def unsafe_commit(**kwargs):
            self.runner.calls.append(kwargs)
            (self.workspace / "unexpected.py").write_text("print('must never be published')\n")
            self.git("-C", str(self.workspace), "add", "unexpected.py")
            self.git("-C", str(self.workspace), "commit", "-m", "unauthorized fixture commit")
            return {"status": "DONE", "summary": "Fixture tries clean-index scope bypass"}
        self.runner.run = unsafe_commit
        ready = self.client.prepare(self.descriptor)
        self.client.start({**self.descriptor, "client_session_id": ready["client_session_id"],
                           "initial_assignment_ids": ["assignment-one"]})
        self.client.threads[ready["client_session_id"]].join(10)
        self.assertEqual(self.api.submissions, [])
        remote_refs = subprocess.run(["git", "--git-dir", str(self.remote), "show-ref", "--heads"], capture_output=True)
        self.assertEqual(remote_refs.returncode, 1)
        self.assertEqual(remote_refs.stdout, b"")
        self.assertEqual(self.client.state["sessions"][ready["client_session_id"]]["last_error"],
                         "CLIENT_COMMITTED_SCOPE_VIOLATION")

    def test_first_role_refuses_head_drift_even_when_worktree_is_clean(self):
        (self.workspace / "README.md").write_text("Unassigned preexisting commit\n")
        self.git("-C", str(self.workspace), "add", "README.md")
        self.git("-C", str(self.workspace), "commit", "-m", "unassigned drift")
        ready = self.client.prepare(self.descriptor)
        self.client.start({**self.descriptor, "client_session_id": ready["client_session_id"],
                           "initial_assignment_ids": ["assignment-one"]})
        self.client.threads[ready["client_session_id"]].join(10)
        self.assertEqual(self.runner.calls, [])
        self.assertEqual(self.api.submissions, [])
        self.assertEqual(self.client.state["sessions"][ready["client_session_id"]]["last_error"],
                         "CLIENT_INITIAL_HEAD_MISMATCH")

    def test_cli_ignored_unexpected_file_cannot_push_or_submit(self):
        (self.workspace / ".gitignore").write_text("forbidden.tmp\n")
        self.git("-C", str(self.workspace), "add", ".gitignore")
        self.git("-C", str(self.workspace), "commit", "-m", "fixture baseline ignored pattern")
        initial = self.git("-C", str(self.workspace), "rev-parse", "HEAD")
        self.api.author["assignment"]["initial_head_commit"] = initial
        self.api.author["workspace"]["initial_head_commit"] = initial
        original_run = self.runner.run
        def write_ignored(**kwargs):
            result = original_run(**kwargs)
            (self.workspace / "forbidden.tmp").write_text("ignored does not mean allowed")
            return result
        self.runner.run = write_ignored
        ready = self.client.prepare(self.descriptor)
        self.client.start({**self.descriptor, "client_session_id": ready["client_session_id"],
                           "initial_assignment_ids": ["assignment-one"]})
        self.client.threads[ready["client_session_id"]].join(10)
        self.assertEqual(self.api.submissions, [])
        self.assertEqual(len(self.runner.calls), 1)
        self.assertEqual(self.client.state["sessions"][ready["client_session_id"]]["last_error"],
                         "CLIENT_DOCS_SCOPE_VIOLATION")
        remote_refs = subprocess.run(["git", "--git-dir", str(self.remote), "show-ref", "--heads"], capture_output=True)
        self.assertEqual(remote_refs.returncode, 1)
        self.assertEqual(remote_refs.stdout, b"")

    def test_author_lost_ack_replays_on_restart_before_review_without_rerun(self):
        original_request = self.api.request
        dropped = []
        def request(phone, route, payload=None):
            result = original_request(phone, route, payload)
            if phone == "7101" and not dropped:
                dropped.append(deepcopy(payload))
                raise TimeoutError("fixture lost acknowledgement after native accept")
            return result
        self.api.request = request
        ready = self.client.prepare(self.descriptor)
        sid = ready["client_session_id"]
        self.client.start({**self.descriptor, "client_session_id": sid,
                           "initial_assignment_ids": ["assignment-one"]})
        self.client.threads[sid].join(10)
        self.assertEqual(self.client.state["sessions"][sid]["assignments"]["assignment-one"]["phase"], "SUBMITTING")
        self.assertEqual(self.api.identity("9010", "7101"), {})
        self.client.stops[sid].set()
        with self.assertRaisesRegex(ValueError, "CLIENT_STOPPING"):
            self.client._replay_pending(sid)
        self.client.stops[sid].clear()
        self.client.config["role_timeout_seconds"] = 42
        with self.assertRaisesRegex(ValueError, "CLIENT_CONFIGURATION_CHANGED"):
            self.client._replay_pending(sid)
        self.client.config.pop("role_timeout_seconds")
        self.client.lockfile.close()
        self.client = ManagedCodexClient(self.config, runner=self.runner, api=self.api, resolver=lambda ref: TOKEN)
        self.client.resume()
        self.client.threads[sid].join(10)
        session = self.client.state["sessions"][sid]
        self.assertEqual(session["phase"], "TERMINAL", session)
        self.assertTrue(session["assignments"]["assignment-one"]["native_receipt"]["deduplicated"])
        self.assertEqual(session["assignments"]["assignment-one"]["payload"], dropped[0])
        self.assertEqual(len(self.api.submissions), 1)
        self.assertEqual(self.api.reviewed, ["7191", "7192"])
        self.assertEqual([c["schema"]["title"] for c in self.runner.calls], ["author", "review", "review"])

    def test_native_quiescent_blocked_releases_client_admission(self):
        original = self.api.request
        def request(phone, route, payload=None):
            if route.endswith("/observability"):
                return {"execution": {"status": "blocked"}}
            return original(phone, route, payload)
        self.api.request = request
        ready = self.client.prepare(self.descriptor)
        sid = ready["client_session_id"]
        self.client.start({**self.descriptor, "client_session_id": sid,
                           "initial_assignment_ids": ["assignment-one"]})
        self.client.threads[sid].join(10)
        self.assertEqual(self.client.state["sessions"][sid]["phase"], "TERMINAL")
        self.assertEqual(self.client.state["sessions"][sid]["native_terminal_status"], "blocked")
        admitted = self.client.prepare({**self.descriptor, "operation_id": "operation-next"})
        self.assertEqual(admitted["status"], "READY")

    def test_restart_retains_receipts_and_reclaims_only_recorded_owned_jobs(self):
        ready = self.client.prepare(self.descriptor)
        sid = ready["client_session_id"]
        self.client.state["sessions"][sid]["assignments"]["a"] = {
            "phase": "EXECUTING", "job_name": "Local\\fixture-owned"}
        self.client._save()
        self.client.lockfile.close()
        self.client = ManagedCodexClient(self.config, runner=self.runner, api=self.api, resolver=lambda ref: TOKEN)
        self.assertEqual(self.client.prepare(self.descriptor), ready)
        self.assertEqual(self.runner.quiesced, ["Local\\fixture-owned"])
        self.assertEqual(self.client.state["sessions"][sid]["assignments"]["a"]["phase"], "INTERRUPTED")

    def test_scope_escape_has_no_result_or_git_push(self):
        (self.workspace / "unexpected.txt").write_text("must not be committed")
        ready = self.client.prepare(self.descriptor)
        self.client.start({**self.descriptor, "client_session_id": ready["client_session_id"],
                           "initial_assignment_ids": ["assignment-one"]})
        self.client.threads[ready["client_session_id"]].join(5)
        self.assertEqual(self.api.submissions, [])
        self.assertEqual(self.runner.calls, [])
        self.assertEqual(self.client.state["sessions"][ready["client_session_id"]]["last_error"],
                         "CLIENT_DOCS_SCOPE_VIOLATION")

    def test_changed_configuration_blocks_restart_but_keeps_owned_stop(self):
        ready = self.client.prepare(self.descriptor)
        self.client.config["role_timeout_seconds"] = 900
        self.client.projects.clear()
        self.client.resume()
        sid = ready["client_session_id"]
        self.assertNotIn(sid, self.client.threads)
        stopped = self.client.stop({**self.descriptor, "client_session_id": sid})
        self.assertEqual(stopped["status"], "STOPPED")

    def test_resumed_branch_uses_initial_head_not_source_commit(self):
        (self.workspace / "README.md").write_text("Previously accepted branch state.\n")
        self.git("-C", str(self.workspace), "add", "README.md")
        self.git("-C", str(self.workspace), "commit", "-m", "prior accepted state")
        initial = self.git("-C", str(self.workspace), "rev-parse", "HEAD")
        self.api.author["assignment"]["initial_head_commit"] = initial
        self.api.author["workspace"]["initial_head_commit"] = initial
        ready = self.client.prepare(self.descriptor)
        self.client.start({**self.descriptor, "client_session_id": ready["client_session_id"],
                           "initial_assignment_ids": ["assignment-one"]})
        self.client.threads[ready["client_session_id"]].join(10)
        self.assertEqual(self.api.submissions[0]["from_commit"], initial)
        self.assertNotEqual(initial, self.api.author["assignment"]["source_commit"])

    def test_real_native_runtime_identity_result_and_review_contract(self):
        # Existing native engine fixture uses a temporary SQLite store; only its
        # repository verification is mocked. No live host or sprint is touched.
        from tests.test_managed_continuity import ManagedContinuityTests
        native = ManagedContinuityTests(methodName="test_current_identity_projects_workspace_without_scope_control")
        native.setUp()
        try:
            identity = native.runtime.current_identity("project-id", "2861")
            self.client.validators["identity"].validate(identity)
            request = native.result_request()
            result = native.submit_result()
            self.client._validate_receipt("assignment", request, result.response)
            for phone in ("2891", "2892"):
                review_identity = native.runtime.current_identity("project-id", phone)
                self.client.validators["identity"].validate(review_identity)
                review = {"assignment_id": review_identity["assignment"]["assignment_id"],
                          "status": "APPROVE", "feedback": "Native contract fixture verification"}
                response = native.runtime.submit_if_managed("project-id", phone,
                    json.dumps(review).encode(), "worker-contract-fixture")
                self.client._validate_receipt("review", review, response.response)
        finally:
            native.tearDown()


if __name__ == "__main__":
    unittest.main()
