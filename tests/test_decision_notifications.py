"""Isolated outbox/config/SMTP tests: no real credentials or network calls."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import smtplib
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

from nginx_qa.decision_notifications import (
    BINDING_FIELDS, EmailConfig, NotificationError, NotificationStore,
    NotificationWorker, SmtpEmailProvider, binding_matches, is_expired, safe_text,
)


def snapshot(**changes):
    return {"runtime_type": "legacy_sequential_conditional_graph", "project_id": "project-orion",
        "sprint_id": "sprint-orion", "request_id": "request-one", "request_sha256": "a" * 64,
        "content_sha256": "b" * 64, "identity_sha256": "c" * 64, "assignment_id": "assignment-one",
        "base_scope_revision": 2, "execution_revision": 7, "project_name": "Orion",
        "sprint_name": "Safe scope expansion", "node_id": "coordinator", "role_id": "coordinator-role",
        "summary": "Allow an additional isolated proof", "reason": "New prerequisite requires human permission", **changes}


class ConfigTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="nginxqa-email-config-")
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "notifications.sqlite3"
        self.raw = {"schema_version": 1, "notifications": {"email": {
            "enabled": True, "pending_decisions": True, "destination": "operator@example.test",
            "sender": "notifications@example.test", "database_path": str(self.path),
            "public_base_url": "https://qa.example.test", "smtp": {"host": "smtp.example.test", "port": 587, "security": "starttls"}}}}

    def test_disabled_default_no_files_or_credentials_required(self):
        self.assertFalse(EmailConfig.from_environment({}).enabled)
        self.assertFalse(EmailConfig.from_dict({"schema_version": 1, "notifications": {"email": {"enabled": False}}}, env={}).enabled)
        self.assertFalse(self.path.exists())

    def test_explicit_external_config_credentials_process_only_redacted_repr(self):
        config_path = Path(self.temp.name) / "operator-notifications.json"
        config_path.write_text(json.dumps(self.raw), encoding="utf-8")
        env = {"NGINX_QA_NOTIFICATION_CONFIG": str(config_path), "NGINX_QA_SMTP_USERNAME": "synthetic-smtp-user",
            "NGINX_QA_SMTP_PASSWORD": "synthetic-smtp-password"}
        config = EmailConfig.from_environment(env)
        self.assertEqual(config.database_path, self.path)
        self.assertEqual(config.smtp_password, env["NGINX_QA_SMTP_PASSWORD"])
        self.assertNotIn(env["NGINX_QA_SMTP_PASSWORD"], repr(config))
        self.assertNotIn(env["NGINX_QA_SMTP_USERNAME"], repr(config))
        self.assertNotIn("synthetic", config_path.read_text(encoding="utf-8"))
        self.assertFalse(self.path.exists())

    def test_rejects_inline_credentials_unknownkeys_tls_and_header_injection(self):
        changes = [({"password": "synthetic"}, "EMAIL_CONFIG_INVALID"),
            ({"destination": "victim@example.test\r\nBcc: other@example.test"}, "EMAIL_CONFIG_INVALID"),
            ({"smtp": {"host": "smtp.example.test", "port": 25, "security": "none"}}, "EMAIL_CONFIG_TLS_REQUIRED"),
            ({"smtp": {"host": "smtp.example.test", "port": 587, "security": "starttls", "password": "synthetic"}}, "EMAIL_CONFIG_INVALID"),
            ({"database_path": "relative.sqlite3"}, "EMAIL_CONFIG_DATABASE_INVALID"),
            ({"database_path": str(Path(__file__).resolve().parents[1] / "outbox.sqlite3")}, "EMAIL_CONFIG_DATABASE_INVALID")]
        for change, code in changes:
            with self.subTest(change=change):
                raw = deepcopy(self.raw)
                raw["notifications"]["email"].update(change)
                with self.assertRaises(NotificationError) as raised:
                    EmailConfig.from_dict(raw, env={})
                self.assertEqual(str(raised.exception), code)

    def test_https_origin_only_loopback_test_opt_in(self):
        for url in ("http://qa.example.test", "https://user:password@qa.example.test", "https://qa.example.test/?secret=x", "https://qa.example.test/#secret", "https://qa.example.test/path", "http://127.0.0.1:19047", "https://qa.example.test:bad", "https://[broken"):
            with self.subTest(url=url):
                raw = deepcopy(self.raw)
                raw["notifications"]["email"]["public_base_url"] = url
                with self.assertRaises(NotificationError):
                    EmailConfig.from_dict(raw, env={})
        for url in ("http://127.0.0.1:19047", "http://[::1]:19047", "http://localhost:19047"):
            raw = deepcopy(self.raw)
            raw["notifications"]["email"].update(public_base_url=url, allow_loopback_http=True)
            self.assertTrue(EmailConfig.from_dict(raw, env={}).enabled)
        raw["notifications"]["email"]["public_base_url"] = "http://192.0.2.10:19047"
        with self.assertRaises(NotificationError):
            EmailConfig.from_dict(raw, env={})

    def test_disabled_config_still_forbids_credentials_and_no_secret_in_link_origin(self):
        with self.assertRaises(NotificationError):
            EmailConfig.from_dict({"schema_version": 1, "notifications": {"email": {
                "enabled": False, "smtp": {"password": "synthetic-password"}}}}, env={})
        raw = deepcopy(self.raw)
        raw["notifications"]["email"]["public_base_url"] = "https://synthetic-secret.example.test"
        with self.assertRaisesRegex(NotificationError, "EMAIL_CONFIG_URL_INVALID"):
            EmailConfig.from_dict(raw, env={"NGINX_QA_SCOPE_CONTROL_ADMIN_TOKEN": "synthetic-secret"})

    def test_schema_matches_complete_and_disabled_config(self):
        import jsonschema
        schema = json.loads((Path(__file__).resolve().parents[1] / "schemas" / "decision-notification-config-v1.schema.json").read_text(encoding="utf-8"))
        validator = jsonschema.Draft202012Validator(schema)
        validator.validate(self.raw)
        validator.validate({"schema_version": 1, "notifications": {"email": {"enabled": False}}})
        bad = deepcopy(self.raw)
        bad["notifications"]["email"]["smtp"]["password"] = "forbidden"
        self.assertTrue(list(validator.iter_errors(bad)))


class OutboxTests(unittest.TestCase):
    def test_overlapping_secrets_redacted_as_original_spans_without_partial_leaks(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(safe_text("operator-long-private-password", credentials=("operator", "operator-long-private-password")), "[REDACTED]")
            self.assertEqual(safe_text("before abcdefgh after", credentials=("abcde", "defgh")), "before [REDACTED] after")
            self.assertEqual(safe_text("abcabcabc", credentials=("abcabc", "bcab")), "[REDACTED]")
            self.assertEqual(safe_text("safe " + "x" * 390 + "operator-long-private-password", limit=400,
                credentials=("operator", "operator-long-private-password")), ("safe " + "x" * 390 + "[REDACTED]")[:400])
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="nginxqa-email-outbox-")
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "outbox.sqlite3"
        self.store = NotificationStore(self.path, link_ttl_seconds=3600, lease_seconds=120)
        self.store.initialize()
        self.config = EmailConfig(enabled=True, pending_decisions=True, database_path=self.path,
            destination="operator@example.test", sender="notifications@example.test", public_base_url="https://qa.example.test")

    def test_enqueue_concurrent_deduplication_and_immutable_first_binding(self):
        barrier = threading.Barrier(8)
        def enqueue(_):
            barrier.wait(timeout=10)
            return self.store.enqueue(snapshot(), now=100)
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(enqueue, range(8)))
        self.assertEqual(len({r["notification_id"] for r in results}), 1)
        record = results[0]
        self.assertEqual(len(self.store.events(record["notification_id"])), 2)
        changed = self.store.enqueue(snapshot(execution_revision=999, summary="changed"), now=200)
        self.assertEqual(changed, record)
        self.assertEqual(record["binding"], {key: snapshot()[key] for key in BINDING_FIELDS})

    def test_get_list_events_are_readonly_and_missing_database_not_created(self):
        record = self.store.enqueue(snapshot(), now=100)
        before = self.path.read_bytes(), self.path.stat().st_mtime_ns, sorted(p.name for p in self.path.parent.iterdir())
        for _ in range(4):
            self.assertEqual(self.store.get(record["notification_id"]), record)
            self.assertEqual(self.store.list_for("legacy_sequential_conditional_graph", "project-orion", "sprint-orion"), [record])
            self.store.events(record["notification_id"])
        self.assertEqual(before, (self.path.read_bytes(), self.path.stat().st_mtime_ns, sorted(p.name for p in self.path.parent.iterdir())))
        missing = NotificationStore(self.path.parent / "not-created" / "outbox.sqlite3")
        self.assertIsNone(missing.get("id"))
        self.assertEqual(missing.list_for("kind", "project", "sprint"), [])
        self.assertEqual(missing.events("id"), [])
        self.assertFalse(missing.path.parent.exists())

    def test_only_one_worker_claim_and_fencing_old_lease(self):
        record = self.store.enqueue(snapshot(), now=100)
        with ThreadPoolExecutor(max_workers=8) as pool:
            claimed = list(pool.map(lambda _: self.store.claim(now=101), range(8)))
        active = [value for value in claimed if value is not None]
        self.assertEqual(len(active), 1)
        first, lease = active[0]
        self.assertEqual(first["attempts"], 1)
        restarted = NotificationStore(self.path)
        recovered, fresh_lease = restarted.claim(now=222)
        self.assertEqual(recovered["notification_id"], record["notification_id"])
        self.assertEqual(recovered["attempts"], 2)
        self.assertFalse(self.store.finish(record["notification_id"], lease, status="delivered", now=223))
        self.assertTrue(restarted.finish(record["notification_id"], fresh_lease, status="delivered", now=223))
        kinds = [e["kind"] for e in self.store.events(record["notification_id"])]
        self.assertEqual(kinds.count("lease_recovered"), 1)

    def test_failed_retry_exact_idempotency_no_new_identity_or_rebound(self):
        record = self.store.enqueue(snapshot(), now=100)
        _, lease = self.store.claim(now=101)
        self.store.finish(record["notification_id"], lease, status="failed", error_code="SMTP_DELIVERY_FAILED", now=102)
        retried = self.store.retry(record["notification_id"], idempotency_key="retry-one", now=103)
        self.assertFalse(retried["deduplicated"])
        events = self.store.events(record["notification_id"])
        duplicate = self.store.retry(record["notification_id"], idempotency_key="retry-one", now=104)
        self.assertTrue(duplicate["deduplicated"])
        self.assertEqual(self.store.events(record["notification_id"]), events)
        self.assertEqual(retried["binding"], record["binding"])
        self.assertEqual(retried["expires_at"], record["expires_at"])
        with self.assertRaisesRegex(NotificationError, "NOTIFICATION_RETRY_CONFLICT"):
            self.store.retry(record["notification_id"], idempotency_key="another", now=104)
        with self.assertRaisesRegex(NotificationError, "NOTIFICATION_LINK_EXPIRED"):
            self.store.retry(record["notification_id"], idempotency_key="after-expiry", now=4000)

    def test_notification_event_table_append_only(self):
        record = self.store.enqueue(snapshot(), now=100)
        with closing(sqlite3.connect(self.path)) as connection:
            for query in ("DELETE FROM notification_events", "UPDATE notification_events SET kind='tampered'"):
                with self.assertRaises(sqlite3.DatabaseError):
                    connection.execute(query)
        self.assertEqual(len(self.store.events(record["notification_id"])), 2)

    def test_binding_checks_every_field_and_expiry(self):
        record = self.store.enqueue(snapshot(), now=100)
        self.assertTrue(binding_matches(record, snapshot()))
        for key in BINDING_FIELDS:
            changed = snapshot()
            changed[key] = changed[key] + 1 if isinstance(changed[key], int) else changed[key] + "different"
            self.assertFalse(binding_matches(record, changed), key)
        self.assertFalse(is_expired(record, 3699))
        self.assertTrue(is_expired(record, 3700))

    def test_restart_pending_sent_failed_and_audit_read_by_new_process(self):
        pending = self.store.enqueue(snapshot(request_id="pending"), now=100)
        sent = self.store.enqueue(snapshot(request_id="sent"), now=99)
        record, lease = self.store.claim(now=101)
        self.assertEqual(record["notification_id"], sent["notification_id"])
        self.store.finish(sent["notification_id"], lease, status="delivered", now=102)
        script = "from nginx_qa.decision_notifications import NotificationStore;import json,sys;s=NotificationStore(sys.argv[1]);print(json.dumps([s.get(sys.argv[2]),s.get(sys.argv[3]),s.events(sys.argv[3])]))"
        child = subprocess.run([sys.executable, "-c", script, str(self.path), pending["notification_id"], sent["notification_id"]],
            cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True, check=True)
        observed = json.loads(child.stdout)
        self.assertEqual(observed[0]["status"], "pending")
        self.assertEqual(observed[1]["status"], "delivered")
        self.assertEqual(observed[1]["binding"], sent["binding"])
        self.assertEqual(observed[2], self.store.events(sent["notification_id"]))

    def test_secret_redaction_before_persistence_and_email_no_raw_error_audit(self):
        synthetic = "unit-test-admin-token-" + "f" * 64
        password = "unit-test-smtp-password"
        with patch.dict(os.environ, {"NGINX_QA_SCOPE_CONTROL_ADMIN_TOKEN": synthetic, "NGINX_QA_SMTP_PASSWORD": password}):
            record = self.store.enqueue(snapshot(summary="approve " + synthetic, reason="reason " + password), now=100)
            self.assertNotIn(synthetic, json.dumps(record))
            self.assertNotIn(password, json.dumps(record))
            provider = SmtpEmailProvider(replace(self.config, smtp_password=password, smtp_username="unit-smtp-user"))
            mail = provider.message(record).as_string()
            self.assertNotIn(synthetic, mail)
            self.assertNotIn(password, mail)
            _, lease = self.store.claim(now=101)
            self.store.finish(record["notification_id"], lease, status="failed", error_code=password, now=102)
            self.assertNotIn(password, json.dumps(self.store.events(record["notification_id"])))
            self.assertNotIn(password.encode(), self.path.read_bytes())
            self.assertNotIn(synthetic.encode(), self.path.read_bytes())
            with self.assertRaisesRegex(NotificationError, "CREDENTIAL_IN_NOTIFICATION_FORBIDDEN"):
                self.store.enqueue(snapshot(assignment_id=synthetic), now=105)

    def test_redaction_happens_before_display_truncation_and_masks_unknown_bearer(self):
        synthetic = "unit-test-synthetic-secret" + "9" * 64
        with patch.dict(os.environ, {"NGINX_QA_SCOPE_CONTROL_ADMIN_TOKEN": synthetic}):
            record = self.store.enqueue(snapshot(summary="x" * 500 + synthetic,
                reason="Bearer never-configured-opaque-credential"), now=100)
        self.assertNotIn("unit-test", record["summary"])
        self.assertNotIn("never-configured", record["reason"])
        self.assertLessEqual(len(record["summary"]), 512)

    def test_worker_failure_stale_expiry_and_disabled_do_not_touch_input(self):
        class BadProvider:
            def send(self, record):
                raise RuntimeError("synthetic-secret-provider-detail")
        source = snapshot()
        before = deepcopy(source)
        record = self.store.enqueue(source, now=100)
        result = NotificationWorker(self.config, self.store, BadProvider()).run_once(lambda _: True, now=101)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["error_code"], "DELIVERY_FAILED")
        self.assertEqual(source, before)
        self.assertNotIn("synthetic-secret", self.path.read_bytes().decode("utf-8", errors="ignore"))
        self.store.retry(record["notification_id"], idempotency_key="retry", now=102)
        result = NotificationWorker(self.config, self.store, BadProvider()).run_once(lambda _: False, now=103)
        self.assertEqual(result["error_code"], "SNAPSHOT_STALE")
        expired = self.store.enqueue(snapshot(request_id="expired"), now=100)
        result = NotificationWorker(self.config, self.store, BadProvider()).run_once(lambda _: True, now=4000)
        self.assertEqual(result["notification_id"], expired["notification_id"])
        self.assertEqual(result["error_code"], "LINK_EXPIRED")
        before_bytes = self.path.read_bytes()
        self.assertEqual(NotificationWorker(EmailConfig(), self.store, BadProvider()).run_once(lambda _: True), {"status": "disabled"})
        self.assertEqual(self.path.read_bytes(), before_bytes)


class SmtpTests(unittest.TestCase):
    def setUp(self):
        self.config = EmailConfig(enabled=True, pending_decisions=True, smtp_host="smtp.example.test", smtp_port=587,
            smtp_username="synthetic-smtp-user", smtp_password="synthetic-smtp-password", destination="operator@example.test",
            sender="notifications@example.test", public_base_url="https://qa.example.test")
        self.record = {**snapshot(), "notification_id": "d" * 32}
        self.calls = []
        outer = self
        class FakeSmtp:
            def __init__(self, *args, **kwargs):
                outer.calls.append("connect")
            def __enter__(self): return self
            def __exit__(self, *args): outer.calls.append("close")
            def ehlo(self): outer.calls.append("ehlo")
            def starttls(self, context):
                outer.assertTrue(context.check_hostname)
                outer.calls.append("tls")
            def login(self, username, password):
                outer.assertEqual((username, password), (outer.config.smtp_username, outer.config.smtp_password))
                outer.calls.append("login")
            def send_message(self, message):
                outer.message = message
                outer.calls.append("send")
                return {}
        self.fake = FakeSmtp

    def test_starttls_precedes_auth_and_mail_contains_only_public_link(self):
        with patch("nginx_qa.decision_notifications.smtplib.SMTP", self.fake):
            SmtpEmailProvider(self.config).send(self.record)
        self.assertEqual(self.calls, ["connect", "ehlo", "tls", "ehlo", "login", "send", "close"])
        body = self.message.get_content()
        self.assertIn("https://qa.example.test/execution?notification=" + "d" * 32, body)
        self.assertIn("Orion", body)
        self.assertNotIn("approve=", body)
        self.assertNotIn(self.config.smtp_password, self.message.as_string())
        self.assertNotIn(self.config.smtp_username, self.message.as_string())

    def test_implicit_ssl_provider_and_safe_auth_failure(self):
        with patch("nginx_qa.decision_notifications.smtplib.SMTP_SSL", self.fake):
            SmtpEmailProvider(replace(self.config, smtp_security="ssl")).send(self.record)
        self.assertNotIn("tls", self.calls)
        class Failing(self.fake):
            def login(self, username, password):
                raise smtplib.SMTPAuthenticationError(535, b"synthetic-smtp-password")
        with patch("nginx_qa.decision_notifications.smtplib.SMTP", Failing):
            with self.assertRaisesRegex(NotificationError, "^SMTP_AUTH_FAILED$") as raised:
                SmtpEmailProvider(self.config).send(self.record)
        self.assertNotIn("password", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
