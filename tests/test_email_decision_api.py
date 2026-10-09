"""Isolated HTTP acceptance for email entry into the authoritative scope ledger.

No network listener, SMTP connection, installed service, or real credential is
used. Existing legacy fixtures redirect every runtime and prompt path to a fresh
temporary directory. All identifiers and credentials below are synthetic.
"""
import asyncio
from copy import deepcopy
import json
import hashlib
import os
from pathlib import Path
import secrets
import subprocess
import sys
import threading
import unittest
from urllib.parse import urlsplit
from unittest.mock import patch

import main
from nginx_qa.decision_notifications import EmailConfig, SmtpEmailProvider
from nginx_qa.scope_runtime_adapters import runtime_kind
from tests.test_scope_workflow import HumanScopeHTTPTests


class RecordingEmailProvider:
    """Recording transport; a failure must never propagate to execution."""

    def __init__(self):
        self.messages = []
        self.notifications = []
        self.fail = False
        self.config = None

    def send(self, notification):
        if self.fail:
            raise OSError("synthetic mail transport unavailable")
        self.notifications.append(deepcopy(notification))
        message = SmtpEmailProvider(self.config).message(notification)
        # Inspect both the serialized MIME envelope and decoded human-readable
        # body, so base64 transfer encoding cannot conceal a credential leak.
        self.messages.append(message.as_string() + "\n" + message.get_content())


async def request_http(target, *, method="GET", payload=None, headers=None):
    """Call ASGI directly, including HTML and response cookies; no TCP socket."""
    parsed = urlsplit(target)
    received = False
    messages = []

    async def receive():
        nonlocal received
        if received:
            return {"type": "http.disconnect"}
        received = True
        return {"type": "http.request", "more_body": False,
                "body": json.dumps(payload).encode() if payload is not None else b""}

    async def send(message):
        messages.append(message)

    request_headers = [(b"host", b"testserver:19087")]
    request_headers += [(str(key).lower().encode(), str(value).encode())
                        for key, value in (headers or {}).items()]
    scope = {"type": "http", "asgi": {"version": "3.0"},
             "http_version": "1.1", "scheme": "http", "method": method,
             "path": parsed.path, "raw_path": parsed.path.encode(),
             "query_string": parsed.query.encode(), "root_path": "",
             "headers": request_headers, "server": ("testserver", 19087),
             "client": ("127.0.0.1", 39087)}
    await main.app(scope, receive, send)
    response = next(value for value in messages if value["type"] == "http.response.start")
    body = b"".join(value.get("body", b"") for value in messages
                    if value["type"] == "http.response.body")
    response_headers = {key.decode(): value.decode() for key, value in response["headers"]}
    decoded = json.loads(body) if "application/json" in response_headers.get("content-type", "") else body.decode()
    return response["status"], decoded, response_headers


class EmailDecisionHTTPTests(HumanScopeHTTPTests):
    PROJECT_PHONE = "9109"
    PROJECT_CONTEXT = "github.com/example/borealis-email"
    GIT_ADDRESS = "https://github.com/example/borealis-email.git"
    COORDINATOR_PHONE = "6100"
    FORMAL_PHONE = "6101"
    REVIEWER_ONE_PHONE = "6191"
    REVIEWER_TWO_PHONE = "6192"
    AMENDMENT_ID = "BOREALIS-EMAIL-TEST"

    def _base_manifest(self):
        manifest = super()._base_manifest()
        manifest["sprint"] = {"id": "borealis-email", "title": "Borealis scope decisions"}
        manifest["nodes"][0]["tasks"][0]["message"] = "Original bounded Borealis task"
        return manifest

    def setUp(self):
        super().setUp()
        self.provider = RecordingEmailProvider()
        self.service = None
        self.old_service = getattr(main.app.state, "decision_notifications", None)
        self.old_managed_runtime = main.managed_continuity_runtime
        main.managed_continuity_runtime = None

    async def asyncTearDown(self):
        if self.service is not None:
            await self.service.stop()

    def tearDown(self):
        main.app.state.decision_notifications = self.old_service
        main.managed_continuity_runtime = self.old_managed_runtime
        super().tearDown()

    @property
    def admin_headers(self):
        return {"X-Nginx-QA-Scope-Control-Token": self.admin_token}

    def execution_invariants(self):
        state = self._stored_assignment()
        fields = ("assignments", "reviews", "workflow", "current_assignment_id",
                  "current_node_id", "last_transition", "pending_transition",
                  "required_approvals", "status")
        return {**{field: deepcopy(state.get(field)) for field in fields},
                "queues": self._queue_snapshot()}

    def persisted_bytes(self):
        return {str(path.relative_to(self.temp_path)): path.read_bytes()
                for path in self.temp_path.rglob("*")
                if path.is_file() and self.repo_path not in path.parents}

    def replace_isolated_state(self, mutate):
        self.assertIn(self.temp_path, main.git_config_path.parents)
        config = main.read_git_config_file()
        state = config[main.PROJECTS_KEY][self.PROJECT_CONTEXT][main.PROJECT_AGENT_ASSIGNMENT_KEY]
        mutate(state)
        main.write_git_config_file(config)

    async def pair_browser(self):
        code, anonymous, headers = await request_http("/api/v1/operator/session")
        self.assertEqual(code, 200, anonymous)
        cookie = headers["set-cookie"].split(";", 1)[0]
        code, authorized, _ = await request_http("/api/v1/operator/session/authorize",
            method="POST", payload={"pairing_code": anonymous["pairing_code"]},
            headers=self.admin_headers)
        self.assertEqual(code, 200, authorized)
        code, session, _ = await request_http("/api/v1/operator/session", headers={"Cookie": cookie})
        self.assertEqual(code, 200, session)
        self.assertTrue(session["authenticated"])
        self.assertNotIn(self.admin_token, json.dumps(session))
        return {"Cookie": cookie, "X-Nginx-QA-CSRF": session["csrf_token"],
                "Origin": "http://testserver:19087"}

    async def configure_mail(self, *, enabled=True):
        self.config = EmailConfig(enabled=enabled, pending_decisions=enabled,
            database_path=self.temp_path / "notifications" / "outbox.sqlite3",
            public_base_url="https://borealis.example.test",
            destination="operator@example.test", sender="nginx-qa@example.test",
            smtp_host="smtp.example.test", link_ttl_seconds=60,
            smtp_username="synthetic-smtp-user", smtp_password=secrets.token_hex(32))
        self.provider.config = self.config
        self.service = main.app.state.configure_decision_notifications(self.config, self.provider)
        await self.service.start(background=False)

    async def pending_mail(self, *, deliver=True):
        await self.configure_mail()
        aid, payload, requested = await self._request_human_scope()
        self.aid, self.request_payload, self.requested = aid, payload, requested
        self.request_url = self.workflow_url + "/scope-requests/" + requested["request_id"]
        self.version = {"expected_execution_revision": payload["expected_execution_revision"],
                        "expected_scope_revision": payload["expected_scope_revision"]}
        self.initial_invariants = self.execution_invariants()
        await self.service.reconcile_once()
        if deliver:
            await self.service.tick_once()
        notices = self.service.store.list_for(runtime_kind(self._stored_assignment()),
            self.PROJECT_PHONE, self.sprint_id, requested["request_id"])
        self.assertEqual(len(notices), 1, notices)
        self.notification = notices[0]
        self.notice_url = "/api/v1/decision-notifications/" + self.notification["notification_id"]
        self.page_url = "/execution?notification=" + self.notification["notification_id"]
        self.assertEqual(self.execution_invariants(), self.initial_invariants)
        return self.notification

    async def validate_email(self, headers=None):
        code, validation, _ = await request_http(self.notice_url + "/validate", method="POST",
            payload=self.version, headers=headers or self.admin_headers)
        self.assertEqual(code, 200, validation)
        self.assertIn("diff", validation)
        self.assertIn("Do not deploy", validation["diff"])
        return validation

    async def decide_email(self, action, key, *, validation=None, headers=None):
        payload = {**self.version, "action": action, "idempotency_key": key}
        if validation is not None:
            payload["validation_id"] = validation["validation_id"]
        response = await request_http(self.notice_url + "/decisions", method="POST",
            payload=payload, headers=headers or self.admin_headers)
        return payload, response

    async def test_email_delivered_page_operator_pairing_approve_and_ack_keep_graph(self):
        notice = await self.pending_mail()
        self.assertEqual(notice["status"], "delivered")
        self.assertEqual(len(self.provider.messages), 1)
        self.assertIn(self.page_url, self.provider.messages[0])
        paired = await self.pair_browser()
        before = self.persisted_bytes()
        code, page, _ = await request_http(self.page_url)
        self.assertEqual(code, 200)
        self.assertIn("Pending", page)
        code, card, _ = await request_http(self.notice_url)
        self.assertEqual(code, 200, card)
        self.assertTrue(card["current"])
        self.assertEqual(card["request"]["request_id"], self.requested["request_id"])
        self.assertEqual(self.persisted_bytes(), before)
        validation = await self.validate_email(paired)
        _, (code, result, _) = await self.decide_email("approve", "email-approve", validation=validation, headers=paired)
        self.assertEqual(code, 200, result)
        self.assertEqual(result["scope_revision"], 1)
        self.assertFalse(result["graph_advanced"])
        self.assertEqual(self.execution_invariants(), self.initial_invariants)
        self.assertEqual(len(self._stored_assignment()["scope_workflow"]["decisions"]), 1)
        before_ack = self.execution_invariants()
        await self._scope_and_ack(self.aid, self.COORDINATOR_PHONE)
        self.assertEqual(self.execution_invariants(), before_ack)

    async def test_email_reject_is_same_authoritative_decision_without_scope_apply(self):
        await self.pending_mail()
        _, (code, rejected, _) = await self.decide_email("reject", "email-reject")
        self.assertEqual(code, 200, rejected)
        self.assertEqual(rejected["status"], "rejected")
        self.assertEqual(rejected["scope_revision"], 0)
        self.assertNotIn("scope_control", self._stored_assignment())
        self.assertEqual(self.execution_invariants(), self.initial_invariants)
        code, listing, _ = await request_http(self.workflow_url + "/scope-requests")
        self.assertEqual(code, 200, listing)
        self.assertEqual(listing["requests"][0]["status"], "rejected")
        await self.service.tick_once()
        self.assertEqual(len(self.provider.messages), 1)

    async def test_email_scanner_repeated_get_head_and_unauthorized_post_are_read_only(self):
        await self.pending_mail()
        before = self.persisted_bytes()
        for _ in range(3):
            for path in (self.page_url, self.notice_url):
                code, _, _ = await request_http(path, headers={"User-Agent": "SyntheticEmailLinkScanner/1"})
                self.assertEqual(code, 200)
                code, _, _ = await request_http(path, method="HEAD")
                self.assertIn(code, {200, 405})
        code, _, _ = await request_http(self.notice_url + "/decisions", method="POST",
            payload={**self.version, "action": "reject", "idempotency_key": "scanner"})
        self.assertEqual(code, 401)
        self.assertEqual(self.persisted_bytes(), before)
        self.assertEqual(self.execution_invariants(), self.initial_invariants)
        paired = await self.pair_browser()
        before = self.persisted_bytes()
        for bad_headers in ({"Cookie": paired["Cookie"]},
                            {**paired, "Origin": "https://untrusted.example.test"}):
            code, _, _ = await request_http(self.notice_url + "/validate", method="POST",
                payload=self.version, headers=bad_headers)
            self.assertEqual(code, 403)
        self.assertEqual(self.persisted_bytes(), before)

    async def test_email_local_ui_race_one_winner_and_exact_retry_no_second_revision(self):
        await self.pending_mail()
        validation = await self.validate_email()
        email_payload = {**self.version, "action": "approve", "validation_id": validation["validation_id"], "idempotency_key": "race-email"}
        local_payload = {**self.version, "action": "reject", "idempotency_key": "race-local"}
        requests = [(self.notice_url + "/decisions", email_payload),
                    (self.request_url + "/decisions", local_payload)]
        results = await asyncio.gather(*(request_http(path, method="POST", payload=body,
            headers=self.admin_headers) for path, body in requests))
        self.assertEqual(sorted(item[0] for item in results), [200, 409], results)
        state = self._stored_assignment()
        self.assertEqual(len(state["scope_workflow"]["decisions"]), 1)
        self.assertEqual(len(state["scope_workflow"]["attempts"]), 1)
        self.assertEqual(self.execution_invariants(), self.initial_invariants)
        winner = next(index for index, response in enumerate(results) if response[0] == 200)
        path, body = requests[winner]
        before = self.persisted_bytes()
        code, retry, _ = await request_http(path, method="POST", payload=body, headers=self.admin_headers)
        self.assertEqual(code, 200, retry)
        self.assertTrue(retry["deduplicated"])
        self.assertEqual(self.persisted_bytes(), before)
        code, _, _ = await request_http(path, method="POST",
            payload={**body, "validation_id": validation["validation_id"],
                     "action": "reject" if body["action"] == "approve" else "approve"},
            headers=self.admin_headers)
        self.assertEqual(code, 409)
        self.assertEqual(self.persisted_bytes(), before)

    async def test_email_already_decided_local_page_is_stale_and_cannot_reapply(self):
        await self.pending_mail()
        code, _, _ = await request_http(self.request_url + "/decisions", method="POST",
            payload={**self.version, "action": "reject", "idempotency_key": "local-first"},
            headers=self.admin_headers)
        self.assertEqual(code, 200)
        before = self.persisted_bytes()
        code, card, _ = await request_http(self.notice_url)
        self.assertEqual(code, 200, card)
        self.assertFalse(card["current"])
        self.assertTrue(card["stale_reason"])
        self.assertEqual(self.persisted_bytes(), before)
        _, (code, _, _) = await self.decide_email("reject", "late-email")
        self.assertEqual(code, 409)
        self.assertEqual(len(self._stored_assignment()["scope_workflow"]["decisions"]), 1)
        self.assertEqual(len(self._stored_assignment()["scope_workflow"]["attempts"]), 1)
        self.assertEqual(self.execution_invariants(), self.initial_invariants)

    async def test_email_expired_link_read_only_and_normal_ui_still_operates(self):
        notice = await self.pending_mail()
        before = self.persisted_bytes()
        with patch("nginx_qa.decision_notifications.time.time", return_value=notice["expires_at"] + 1):
            code, card, _ = await request_http(self.notice_url)
            self.assertEqual(code, 200, card)
            self.assertFalse(card["current"])
            self.assertTrue(card["stale_reason"])
            self.assertEqual(self.persisted_bytes(), before)
            _, (code, _, _) = await self.decide_email("reject", "expired-email")
            self.assertIn(code, {409, 410})
        code, result, _ = await request_http(self.request_url + "/decisions", method="POST",
            payload={**self.version, "action": "reject", "idempotency_key": "normal-ui-after-expiry"},
            headers=self.admin_headers)
        self.assertEqual(code, 200, result)
        self.assertEqual(result["status"], "rejected")
        self.assertEqual(self.execution_invariants(), self.initial_invariants)

    async def test_email_revision_assignment_and_request_content_drift_are_stale(self):
        await self.pending_mail()
        baseline = deepcopy(self._stored_assignment())
        mutations = {
            "scope": lambda state: state.update(scope_control={"effective_revision": 9}),
            "execution": lambda state: state.update(revision=state["revision"] + 1),
            "assignment": lambda state: state.update(current_assignment_id="new-isolated-assignment"),
            "request-content": lambda state: state["scope_workflow"]["requests"][0]["proposal"].update(instructions="External isolated fixture drift"),
        }
        for label, mutate in mutations.items():
            with self.subTest(drift=label):
                self.replace_isolated_state(lambda state: (state.clear(), state.update(deepcopy(baseline)), mutate(state)))
                before = self.persisted_bytes()
                code, card, _ = await request_http(self.notice_url)
                self.assertEqual(code, 200, card)
                self.assertFalse(card["current"])
                self.assertTrue(card["stale_reason"])
                self.assertEqual(self.persisted_bytes(), before)
                code, _, _ = await request_http(self.notice_url + "/validate", method="POST",
                    payload=self.version, headers=self.admin_headers)
                self.assertEqual(code, 409)
                self.assertEqual(self.persisted_bytes(), before)
                self.assertEqual(self._stored_assignment()["scope_workflow"]["decisions"], [])

    async def test_email_delivery_failure_retry_is_separate_durable_metadata(self):
        await self.pending_mail(deliver=False)
        self.provider.fail = True
        semantic_before = main.git_config_path.read_bytes()
        await self.service.tick_once()
        failed = self.service.store.get(self.notification["notification_id"])
        self.assertEqual(failed["status"], "failed")
        self.assertEqual(main.git_config_path.read_bytes(), semantic_before)
        self.assertEqual(self.execution_invariants(), self.initial_invariants)
        code, _, _ = await request_http(self.notice_url + "/retry", method="POST",
            payload={"idempotency_key": "retry-notice"})
        self.assertEqual(code, 401)
        code, retried, _ = await request_http(self.notice_url + "/retry", method="POST",
            payload={"idempotency_key": "retry-notice"}, headers=self.admin_headers)
        self.assertEqual(code, 200, retried)
        self.provider.fail = False
        await self.service.tick_once()
        delivered = self.service.store.get(self.notification["notification_id"])
        self.assertEqual(delivered["status"], "delivered")
        self.assertEqual(delivered["request_id"], failed["request_id"])
        self.assertEqual(delivered["binding"], failed["binding"])
        self.assertEqual(len(self.provider.messages), 1)
        before = self.persisted_bytes()
        code, duplicate, _ = await request_http(self.notice_url + "/retry", method="POST",
            payload={"idempotency_key": "retry-notice"}, headers=self.admin_headers)
        self.assertEqual(code, 200, duplicate)
        self.assertEqual(self.persisted_bytes(), before)
        self.assertEqual(main.git_config_path.read_bytes(), semantic_before)

    async def test_email_two_paired_browsers_approve_concurrently_one_revision(self):
        await self.pending_mail()
        browser_a, browser_b = await self.pair_browser(), await self.pair_browser()
        validation = await self.validate_email(browser_a)
        async def submit(headers, key):
            return await self.decide_email("approve", key, validation=validation, headers=headers)
        outcomes = await asyncio.gather(submit(browser_a, "browser-a"), submit(browser_b, "browser-b"))
        self.assertEqual(sorted(result[1][0] for result in outcomes), [200, 409], outcomes)
        state = self._stored_assignment()
        self.assertEqual(state["scope_control"]["effective_revision"], 1)
        self.assertEqual(len(state["scope_workflow"]["decisions"]), 1)
        self.assertEqual(len(state["scope_workflow"]["attempts"]), 1)
        self.assertEqual(self.execution_invariants(), self.initial_invariants)

    async def test_email_binding_is_checked_inside_execution_lock_after_request_arrives(self):
        await self.pending_mail()
        validation = await self.validate_email()
        record_read = threading.Event()
        original_get = self.service.store.get

        def observed_get(notification_id):
            value = original_get(notification_id)
            record_read.set()
            return value

        await main.group_task_submission_lock.acquire()
        try:
            with patch.object(self.service.store, "get", side_effect=observed_get):
                in_flight = asyncio.create_task(self.decide_email("approve", "waiting-for-lock", validation=validation))
                for _ in range(200):
                    if record_read.is_set():
                        break
                    await asyncio.sleep(0.005)
                self.assertTrue(record_read.is_set(), "Request never reached notification lookup")
                self.assertFalse(in_flight.done())
                # No revision bump: CAS alone would not detect this. A guard
                # outside the lock could approve content no longer in the mail.
                self.replace_isolated_state(lambda state:
                    state["scope_workflow"]["requests"][0].update(reason="Changed after request lookup"))
        finally:
            main.group_task_submission_lock.release()
        _, (code, refused, _) = await in_flight
        self.assertEqual(code, 409, refused)
        self.assertIn("NOTIFICATION_STALE", json.dumps(refused))
        state = self._stored_assignment()
        self.assertNotIn("scope_control", state)
        self.assertEqual(state["scope_workflow"]["decisions"], [])
        self.assertEqual(len(state["scope_workflow"]["attempts"]), 1)
        self.assertEqual(self.execution_invariants(), self.initial_invariants)

    async def test_email_expired_failed_attempt_replay_and_accepted_retry_are_idempotent(self):
        notice = await self.pending_mail()
        validation = await self.validate_email()
        with patch("nginx_qa.decision_notifications.time.time", return_value=notice["expires_at"] + 1):
            refused_payload, (code, refused, _) = await self.decide_email("approve", "expired-failed", validation=validation)
            self.assertEqual(code, 410, refused)
            before = self.persisted_bytes()
            code, replay, _ = await request_http(self.notice_url + "/decisions", method="POST",
                payload=refused_payload, headers=self.admin_headers)
            self.assertEqual(code, 410, replay)
            self.assertTrue(replay["detail"]["deduplicated"])
            self.assertEqual(self.persisted_bytes(), before)
        # A current ordinary UI action may still decide the same request.
        accepted_payload = {**refused_payload, "idempotency_key": "normal-ui-accepted"}
        code, accepted, _ = await request_http(self.request_url + "/decisions", method="POST",
            payload=accepted_payload, headers=self.admin_headers)
        self.assertEqual(code, 200, accepted)
        before = self.persisted_bytes()
        with patch("nginx_qa.decision_notifications.time.time", return_value=notice["expires_at"] + 2):
            code, replay, _ = await request_http(self.notice_url + "/decisions", method="POST",
                payload=accepted_payload, headers=self.admin_headers)
            self.assertEqual(code, 200, replay)
            self.assertTrue(replay["deduplicated"])
            self.assertEqual(replay["receipt"], accepted["receipt"])
            self.assertEqual(self.persisted_bytes(), before)
        self.assertEqual(self.execution_invariants(), self.initial_invariants)

    async def test_email_disabled_service_never_creates_database_or_sends(self):
        await self.configure_mail(enabled=False)
        await self._request_human_scope()
        before = self.persisted_bytes()
        await self.service.reconcile_once()
        await self.service.tick_once()
        self.assertFalse(self.config.database_path.exists())
        self.assertEqual(self.provider.messages, [])
        self.assertEqual(self.persisted_bytes(), before)

    def fresh_process_summary(self):
        script = """
import hashlib, json, sys
from pathlib import Path
from nginx_qa.decision_notifications import NotificationStore
from nginx_qa.scope_workflow import request_list
path, database, context, assignment_key, notification_id = sys.argv[1:]
data = Path(path).read_bytes()
config = json.loads(data)
state = config['projects'][context][assignment_key]
notice = NotificationStore(Path(database)).get(notification_id)
listing = request_list(state)
print(json.dumps({'state_sha256': hashlib.sha256(data).hexdigest(),
 'request_status': listing['requests'][0]['status'],
 'request_id': listing['requests'][0]['request_id'],
 'notification_status': notice['status'], 'notification_binding': notice['binding'],
 'scope_revision': listing['scope_revision'],
 'decision_count': len(state.get('scope_workflow',{}).get('decisions',[])),
 'notification_events': NotificationStore(Path(database)).events(notification_id)}))
"""
        completed = subprocess.run([sys.executable, "-c", script,
            str(main.git_config_path), str(self.config.database_path), self.PROJECT_CONTEXT,
            main.PROJECT_AGENT_ASSIGNMENT_KEY, self.notification["notification_id"]],
            cwd=str(Path(__file__).resolve().parents[1]), capture_output=True,
            text=True, encoding="utf-8", timeout=30, check=True)
        return json.loads(completed.stdout)

    def fresh_process_http(self, *, decision=None):
        """A new interpreter imports the actual server adapter, with no lifespan
        launcher/listener. Every path is the same isolated persisted fixture.
        Synthetic auth is inherited through the child environment, never argv.
        """
        script = """
import asyncio, hashlib, json, os, sys
from pathlib import Path
import main
from nginx_qa.decision_notifications import EmailConfig
from tests.test_email_decision_api import request_http

async def run():
    fixture = json.loads(sys.stdin.read())
    root = Path(fixture['root'])
    for name, filename in {
        'git_config_path': 'port_git_map.json', 'agents_path': 'agents.json',
        'history_path': 'conversation_log.jsonl', 'sprint_history_path': 'project_sprints.json',
        'pending_sprints_path': 'pending_project_sprints.json',
    }.items():
        setattr(main, name, root / filename)
    main.managed_continuity_runtime = None
    main.project_state_patch_cache = {}
    service = main.app.state.configure_decision_notifications(EmailConfig(
        enabled=True, pending_decisions=True, database_path=Path(fixture['database']),
        public_base_url='https://borealis.example.test', destination='operator@example.test',
        sender='nginx-qa@example.test', smtp_host='smtp.example.test'))
    await service.start(background=False)
    try:
        path = '/api/v1/decision-notifications/' + fixture['notification_id']
        code, card, _ = await request_http(path)
        _, session, _ = await request_http('/api/v1/operator/session')
        result = {'get_status': code, 'current': card['current'],
                  'request_status': card['request']['status'],
                  'new_process_operator_authenticated': session['authenticated']}
        if fixture.get('decision'):
            status, replay, _ = await request_http(path + '/decisions', method='POST',
                payload=fixture['decision'], headers={
                    'X-Nginx-QA-Scope-Control-Token': os.environ['NGINX_QA_SCOPE_CONTROL_ADMIN_TOKEN']})
            result.update(replay_status=status, deduplicated=replay.get('deduplicated'),
                          receipt=replay.get('receipt'), graph_advanced=replay.get('graph_advanced'))
        result['state_sha256'] = hashlib.sha256(main.git_config_path.read_bytes()).hexdigest()
        print(json.dumps(result))
    finally:
        await service.stop()

asyncio.run(run())
"""
        # Do not propagate unrelated credentials/configuration into the test
        # child; its only nginx-qa credential is our randomly generated fixture.
        environment = {key: value for key, value in os.environ.items()
                       if not key.startswith("NGINX_QA_")}
        environment["NGINX_QA_SCOPE_CONTROL_ADMIN_TOKEN"] = self.admin_token
        completed = subprocess.run([sys.executable, "-c", script],
            input=json.dumps({"root": str(self.temp_path), "database": str(self.config.database_path),
                              "notification_id": self.notification["notification_id"], "decision": decision}),
            env=environment, cwd=str(Path(__file__).resolve().parents[1]),
            capture_output=True, text=True, encoding="utf-8", timeout=30, check=True)
        return json.loads(completed.stdout)

    async def restart_mail_service(self):
        await self.service.stop()
        self.service = main.app.state.configure_decision_notifications(self.config, self.provider)
        await self.service.start(background=False)

    async def test_email_new_process_pending_sent_and_decided_preserve_history(self):
        await self.pending_mail(deliver=False)
        pending = await asyncio.to_thread(self.fresh_process_summary)
        self.assertEqual(pending["request_status"], "pending")
        self.assertEqual(pending["decision_count"], 0)
        self.assertEqual(pending["notification_binding"], self.notification["binding"])
        self.assertEqual(pending["state_sha256"], hashlib.sha256(main.git_config_path.read_bytes()).hexdigest())
        pending_http = await asyncio.to_thread(self.fresh_process_http)
        self.assertEqual(pending_http["get_status"], 200)
        self.assertTrue(pending_http["current"])
        self.assertEqual(pending_http["request_status"], "pending")
        self.assertFalse(pending_http["new_process_operator_authenticated"])
        self.assertEqual(pending_http["state_sha256"], pending["state_sha256"])
        await self.restart_mail_service()
        await self.service.tick_once()
        sent = await asyncio.to_thread(self.fresh_process_summary)
        self.assertEqual(sent["notification_status"], "delivered")
        self.assertEqual(sent["request_status"], "pending")
        self.assertEqual(sent["request_id"], pending["request_id"])
        self.assertEqual(sent["state_sha256"], pending["state_sha256"])
        self.assertEqual(sent["notification_events"][:len(pending["notification_events"])], pending["notification_events"])
        sent_http = await asyncio.to_thread(self.fresh_process_http)
        self.assertTrue(sent_http["current"])
        self.assertEqual(sent_http["state_sha256"], sent["state_sha256"])
        await self.restart_mail_service()
        await self.service.tick_once()
        self.assertEqual(len(self.provider.messages), 1)
        validation = await self.validate_email()
        decision, (code, accepted, _) = await self.decide_email("approve", "restart-approve", validation=validation)
        self.assertEqual(code, 200, accepted)
        accepted_summary = await asyncio.to_thread(self.fresh_process_summary)
        self.assertEqual(accepted_summary["request_status"], "applied")
        self.assertEqual(accepted_summary["scope_revision"], 1)
        self.assertEqual(accepted_summary["decision_count"], 1)
        before_child = self.persisted_bytes()
        notification_before_child = self.service.store.get(self.notification["notification_id"])
        events_before_child = self.service.store.events(self.notification["notification_id"])
        accepted_http = await asyncio.to_thread(self.fresh_process_http, decision=decision)
        self.assertEqual(accepted_http["get_status"], 200)
        self.assertFalse(accepted_http["current"])
        self.assertFalse(accepted_http["new_process_operator_authenticated"])
        self.assertEqual(accepted_http["replay_status"], 200)
        self.assertTrue(accepted_http["deduplicated"])
        self.assertEqual(accepted_http["receipt"], accepted["receipt"])
        self.assertFalse(accepted_http["graph_advanced"])
        # Startup may update SQLite's schema/change-counter header; the GET and
        # exact retry must preserve all execution files and logical outbox rows.
        database_relative = str(self.config.database_path.relative_to(self.temp_path))
        execution_files = lambda files: {key: value for key, value in files.items() if key != database_relative}
        self.assertEqual(execution_files(self.persisted_bytes()), execution_files(before_child))
        self.assertEqual(self.service.store.get(self.notification["notification_id"]), notification_before_child)
        self.assertEqual(self.service.store.events(self.notification["notification_id"]), events_before_child)
        await self.restart_mail_service()
        before = self.persisted_bytes()
        code, card, _ = await request_http(self.notice_url)
        self.assertEqual(code, 200, card)
        self.assertFalse(card["current"])
        code, retry, _ = await request_http(self.notice_url + "/decisions", method="POST",
            payload=decision, headers=self.admin_headers)
        self.assertEqual(code, 200, retry)
        self.assertTrue(retry["deduplicated"])
        self.assertEqual(self.persisted_bytes(), before)
        self.assertEqual(await asyncio.to_thread(self.fresh_process_summary), accepted_summary)
        self.assertEqual(self.execution_invariants(), self.initial_invariants)

    async def test_email_only_new_human_pending_not_prior_authorization_or_regular_work(self):
        await self.configure_mail()
        os.environ["NGINX_QA_SCOPE_CONTROL_REPOSITORY_MAP"] = json.dumps({self.PROJECT_CONTEXT: str(self.repo_path)})
        await self._import_sprint()
        await self._claim("continuity-coordinator")
        await self.service.tick_once()
        self.assertEqual(self.provider.messages, [])
        state = self._stored_assignment()
        aid = state["current_assignment_id"]
        payload = {"assignment_id": aid, "idempotency_key": "already-authorized",
            "expected_execution_revision": state["revision"], "expected_scope_revision": 0,
            "reason": "Apply existing permission, do not ask for new consent",
            "proposal": {"instructions": "Previously approved Borealis proof only",
                         "node_ids": ["continuity-coordinator", "formal-linkage"],
                         "reviewer_ids": ["reviewer-one", "reviewer-two"],
                         "retained_restrictions": ["No deployment"]},
            "source": {"repository_key": self.PROJECT_CONTEXT, "commit": self.base_commit,
                       "path": self.MANIFEST_PATH, "sha256": self.base_manifest_sha256},
            "authorization_provenance": {"kind": "existing_human_authorization", "reference": "Prior isolated human permission"}}
        code, requested, _ = await request_http(
            f"/api/v1/projects/{self.PROJECT_PHONE}/sprints/{self.sprint_id}/scope-requests",
            method="POST", payload=payload,
            headers={"X-Nginx-QA-Scope-Token": self.role_tokens[self.COORDINATOR_PHONE]})
        self.assertEqual(code, 200, requested)
        self.assertEqual(requested["status"], "approved_pending_application")
        before = main.git_config_path.read_bytes()
        await self.service.tick_once()
        self.assertEqual(self.provider.messages, [])
        self.assertEqual(main.git_config_path.read_bytes(), before)
        self.assertEqual(self.service.store.list_for(runtime_kind(self._stored_assignment()), self.PROJECT_PHONE, self.sprint_id), [])

    async def test_email_credentials_never_in_mail_url_outbox_or_error_evidence(self):
        await self.pending_mail()
        secrets_used = (self.admin_token, *self.role_tokens.values(), self.config.smtp_password,
                        self.config.smtp_username)
        mail = "\n".join(self.provider.messages)
        durable = b"\n".join(self.persisted_bytes().values())
        code, card, _ = await request_http(self.notice_url)
        self.assertEqual(code, 200, card)
        for secret in secrets_used:
            self.assertNotIn(secret, mail)
            self.assertNotIn(secret.encode(), durable)
            self.assertNotIn(secret, json.dumps(card))
        self.assertRegex(self.notification["notification_id"], r"^[a-zA-Z0-9_-]+$")
        self.assertNotIn("bearer", mail.lower())
        before = self.persisted_bytes()
        # Transporting any actual configured credential in a semantic request
        # must fail before it reaches either audit ledger, even JSON-escaped.
        with patch.dict(os.environ, {"NGINX_QA_SMTP_PASSWORD": self.config.smtp_password}):
            for secret in (self.admin_token, self.role_tokens[self.COORDINATOR_PHONE], self.config.smtp_password):
                code, response, _ = await request_http(self.notice_url + "/decisions", method="POST",
                    payload={**self.version, "action": "reject", "idempotency_key": "credential-refused", "reason": secret},
                    headers=self.admin_headers)
                self.assertEqual(code, 400, response)
                self.assertNotIn(secret, json.dumps(response))
        self.assertEqual(self.persisted_bytes(), before)


def load_tests(loader, tests, pattern):
    # Do not silently rerun the inherited legacy/HTTP suites for every subclass.
    return unittest.TestSuite(EmailDecisionHTTPTests(name)
        for name in loader.getTestCaseNames(EmailDecisionHTTPTests)
        if name.startswith("test_email_"))


if __name__ == "__main__":
    unittest.main()
