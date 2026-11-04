"""Real private routes with issued snapshots and genuine operator pairing.

All state, grants and databases belong to a temporary isolated host. No live
server, credentials, Managed Sprint assignment or repository is accessed.
"""
from __future__ import annotations
from contextlib import ExitStack, closing
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient
from nginx_qa.legacy_scope_control import ADMIN_TOKEN_ENV, ADMIN_TOKEN_HEADER
from nginx_qa.scope_api import register_scope_api
from nginx_qa.setup_diagnostics_api import register_setup_diagnostics_api
from nginx_qa.setup_diagnostics import parse_strict_json_with_limits, get_snapshot_validator
from nginx_qa.setup_advisor import get_advice_response_validator
from nginx_qa.setup_operations import get_plan_validator, get_receipt_validator

TOKEN = "isolated-private-api-fixture-" + "z"*32

class TestSetupPrivateAPI(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.root = Path(self.stack.enter_context(tempfile.TemporaryDirectory(prefix="sdwc-private-api-")))
        env = {k:v for k,v in os.environ.items()
               if not k.startswith("NGINX_QA") and k not in {"TELEGRAM_BOT_TOKEN", "TELEGRAM_WEBHOOK_SECRET"}}
        env[ADMIN_TOKEN_ENV] = TOKEN
        self.stack.enter_context(patch.dict(os.environ, env, clear=True))
        self.registry = self.root/"registry.json"
        self.policy = self.root/"policy.json"
        self.state = self.root/"state"
        self.repo = self.root/"repo"; self.repo.mkdir()
        self.store = self.root/"managed-import.sqlite3"
        with closing(sqlite3.connect(self.store)) as connection:
            connection.execute("CREATE TABLE fixture_marker (value TEXT)")
            connection.execute("INSERT INTO fixture_marker VALUES ('isolated-test-only')")
            connection.commit()
        self.registry.write_text(json.dumps({"projects":{
            "github.com/fixture/a#main":{"project_phone":"9001","project_name":"Fixture A","git_address":"https://github.com/fixture/a.git","repository_root":str(self.repo)},
            "github.com/fixture/b#main":{"project_phone":"9002","project_name":"Fixture B","git_address":"https://github.com/fixture/b.git","repository_root":str(self.repo)},
        }}),encoding="utf-8")
        self.write_policy()
        self.host = SimpleNamespace(app=FastAPI(), state_base=self.state,
            setup_policy_path=self.policy, git_config_path=self.registry,
            service_root=self.repo, setup_store_path=self.store,
            parse_strict_json_object=parse_strict_json_with_limits)
        register_scope_api(self.host)
        register_setup_diagnostics_api(self.host)
        self.admin = self.stack.enter_context(TestClient(self.host.app, raise_server_exceptions=False))
        self.alice = self.paired_client()
        self.bob = self.paired_client()

    def write_policy(self, actions=None, projects=None):
        self.policy.write_text(json.dumps({"schema_version":1,"grants":[{
            "role":"paired_operator", "projects":projects or ["9001","9002"],
            "actions":actions if actions is not None else ["read","advise","diagnostics.refresh","setup.open"],
        }],"selftest_profiles":{}}),encoding="utf-8")

    def paired_client(self, operator_id=None):
        client = self.stack.enter_context(TestClient(self.host.app, raise_server_exceptions=False))
        session = client.get("/api/v1/operator/session")
        self.assertEqual(session.status_code,200)
        self.assertFalse(session.json()["authenticated"])
        self.assertIsNone(session.json()["csrf_token"])
        # Only the local privileged operator approves pairing. Browser receives
        # a scoped HttpOnly cookie and CSRF token, never the administrative token.
        paired = self.admin.post("/api/v1/operator/session/authorize",
            headers={ADMIN_TOKEN_HEADER:TOKEN}, json={"pairing_code":session.json()["pairing_code"],
                                                     **({"operator_id":operator_id} if operator_id else {})})
        self.assertEqual(paired.status_code,200,paired.text)
        confirmed = client.get("/api/v1/operator/session").json()
        self.assertTrue(confirmed["authenticated"])
        client.headers["X-Nginx-QA-CSRF"] = confirmed["csrf_token"]
        return client

    def snapshot(self, client=None, project="9001"):
        response = (client or self.alice).get(f"/api/v1/projects/{project}/setup/snapshot")
        self.assertEqual(response.status_code,200,response.text)
        get_snapshot_validator().validate(response.json())
        return response.json()

    def plan(self, client=None, project="9001", snapshot=None):
        snapshot = snapshot or self.snapshot(client,project)
        response = (client or self.alice).post(f"/api/v1/projects/{project}/setup/plans",json={
            "schema_version":1,"snapshot_id":snapshot["snapshot_id"],
            "action_id":"diagnostics.refresh","arguments":{}})
        self.assertEqual(response.status_code,200,response.text)
        get_plan_validator().validate(response.json())
        return response.json()

    def apply(self, plan, client=None, project="9001", path_plan=None, key="apply-fixture"):
        return (client or self.alice).post(
            f"/api/v1/projects/{project}/setup/plans/{path_plan or plan['plan_id']}/apply",json={
                "schema_version":1,"plan_digest":plan["plan_digest"],"confirmed":True,"idempotency_key":key})

    def tree(self):
        return {str(p.relative_to(self.root)):hashlib.sha256(p.read_bytes()).hexdigest()
                for p in self.root.rglob("*") if p.is_file()}

    def test_real_issued_snapshot_advice_plan_apply_and_durable_replay(self):
        snap = self.snapshot()
        advice = self.alice.post("/api/v1/projects/9001/setup/advice",json={
            "schema_version":1,"snapshot_id":snap["snapshot_id"],"question":"Готовность?",
            "idempotency_key":"advice-fixture"})
        self.assertEqual(advice.status_code,200,advice.text)
        get_advice_response_validator().validate(advice.json())
        self.assertEqual(advice.json()["snapshot_id"],snap["snapshot_id"])
        plan = self.plan(snapshot=snap)
        applied = self.apply(plan)
        self.assertEqual(applied.status_code,200,applied.text)
        receipt = applied.json(); get_receipt_validator().validate(receipt)
        self.assertEqual(receipt["state"],"SUCCEEDED")
        self.assertIsNone(receipt["native_sprint_id"])
        replay = self.apply(plan)
        self.assertEqual(replay.status_code,200,replay.text)
        self.assertTrue(replay.json()["deduplicated"])
        self.assertEqual(replay.json()["operation_id"],receipt["operation_id"])
        fetched = self.alice.get("/api/v1/projects/9001/setup/operations/"+receipt["operation_id"])
        self.assertEqual(fetched.status_code,200,fetched.text)
        self.assertEqual(fetched.json()["state"],"SUCCEEDED")
        self.assertTrue((self.state/"setup"/"operations.json").is_file())

    def test_named_operator_recovers_owned_receipt_after_new_host_pairing(self):
        alice = self.paired_client('alice')
        plan = self.plan(alice)
        result = self.apply(plan, alice)
        self.assertEqual(200, result.status_code, result.text)
        receipt = result.json()
        old_actor = alice.get('/api/v1/operator/session').json()['actor_id']
        # New closures have no old sessions or cookies. Only administrator can restore named ownership.
        self.host = SimpleNamespace(app=FastAPI(), state_base=self.state, setup_policy_path=self.policy,
            git_config_path=self.registry, service_root=self.repo, setup_store_path=self.store,
            parse_strict_json_object=parse_strict_json_with_limits)
        register_scope_api(self.host)
        register_setup_diagnostics_api(self.host)
        self.admin = self.stack.enter_context(TestClient(self.host.app, raise_server_exceptions=False))
        restored = self.paired_client('alice')
        self.assertEqual(old_actor, restored.get('/api/v1/operator/session').json()['actor_id'])
        route = '/api/v1/projects/9001/setup/operations/' + receipt['operation_id']
        self.assertEqual(200, restored.get(route).status_code)
        bob = self.paired_client('bob')
        self.assertEqual(403, bob.get(route).status_code)
        listing = '/api/v1/projects/9001/setup/operations'
        self.assertEqual([receipt['operation_id']], [r['operation_id'] for r in restored.get(listing).json()['operations']])
        self.assertEqual([], bob.get(listing).json()['operations'])
        replay = self.apply(plan, restored)
        self.assertEqual(200, replay.status_code, replay.text)
        self.assertTrue(replay.json()['deduplicated'])

    def test_get_is_readonly_even_missing_runtime_schema_and_operation(self):
        before = self.tree()
        self.assertEqual(self.alice.get("/api/v1/setup/projects").status_code,200)
        self.snapshot()
        self.alice.get("/api/v1/projects/9001/setup/operations/nonexistent")
        self.assertEqual(self.tree(),before)
        self.assertFalse(self.state.exists())

    def test_snapshot_cannot_cross_actor_project_or_be_invented(self):
        snap = self.snapshot()
        for client,project,sid in ((self.bob,"9001",snap["snapshot_id"]),
                                   (self.alice,"9002",snap["snapshot_id"]),
                                   (self.alice,"9001","f"*64)):
            with self.subTest(project=project,snapshot=sid[:8],client=id(client)):
                result = client.post(f"/api/v1/projects/{project}/setup/advice",json={
                    "schema_version":1,"snapshot_id":sid,"question":"Status?","idempotency_key":"scope-test"})
                self.assertTrue(400<=result.status_code<500,result.text)
                result = client.post(f"/api/v1/projects/{project}/setup/plans",json={
                    "schema_version":1,"snapshot_id":sid,"action_id":"diagnostics.refresh","arguments":{}})
                self.assertTrue(400<=result.status_code<500,result.text)
        self.assertFalse(self.state.exists())

    def test_plan_owner_project_and_path_fences(self):
        plan = self.plan()
        for client,project,path in ((self.bob,"9001",plan["plan_id"]),
                                   (self.alice,"9002",plan["plan_id"]),
                                   (self.alice,"9001","different-plan")):
            result = self.apply(plan,client=client,project=project,path_plan=path)
            self.assertEqual(result.status_code,403,result.text)
        ledger = json.loads((self.state/"setup"/"operations.json").read_text())
        self.assertEqual(ledger["operations"],{})

    def test_policy_revocation_invalidates_old_snapshot_and_plan(self):
        snap = self.snapshot()
        plan = self.plan(snapshot=snap)
        self.write_policy(actions=["read"])
        advice = self.alice.post("/api/v1/projects/9001/setup/advice",json={
            "schema_version":1,"snapshot_id":snap["snapshot_id"],"question":"Status?","idempotency_key":"revoke"})
        self.assertEqual(advice.status_code,403)
        applied = self.apply(plan)
        self.assertEqual(applied.status_code,403,applied.text)
        self.write_policy(actions=[])
        self.assertEqual(self.alice.get("/api/v1/projects/9001/setup/snapshot").status_code,403)
        self.assertEqual(self.alice.get("/api/v1/setup/projects").json()["projects"],[])

    def test_default_deny_and_fake_cookie_cannot_authenticate(self):
        self.policy.unlink()
        self.assertEqual(self.alice.get("/api/v1/setup/projects").json()["projects"],[])
        self.assertEqual(self.alice.get("/api/v1/projects/9001/setup/snapshot").status_code,403)
        stranger = self.stack.enter_context(TestClient(self.host.app,raise_server_exceptions=False))
        stranger.cookies.set("nginx_qa_operator","fabricated")
        stranger.headers["X-Nginx-QA-CSRF"]="fabricated"
        self.assertEqual(stranger.get("/api/v1/setup/projects").status_code,401)
        del self.host.scope_api_operator
        self.assertEqual(stranger.get("/api/v1/setup/projects").status_code,401)

    def test_csrf_origin_and_malformed_apply_are_client_errors(self):
        plan = self.plan()
        token = self.alice.headers.pop("X-Nginx-QA-CSRF")
        self.assertEqual(self.apply(plan).status_code,403)
        self.alice.headers["X-Nginx-QA-CSRF"]=token
        self.alice.headers["Origin"]="https://foreign.invalid"
        self.assertEqual(self.apply(plan).status_code,403)
        del self.alice.headers["Origin"]
        path=f"/api/v1/projects/9001/setup/plans/{plan['plan_id']}/apply"
        for payload in ({"schema_version":1,"plan_id":plan["plan_id"],"confirmation":True},
                        {"schema_version":1,"plan_digest":plan["plan_digest"],"confirmed":"yes","idempotency_key":"bad"},
                        [], {"unexpected":True}):
            result=self.alice.post(path,json=payload)
            self.assertTrue(400<=result.status_code<500,result.text)
        duplicate=self.alice.post(path,content='{"schema_version":1,"schema_version":1}')
        self.assertEqual(duplicate.status_code,400)

    def test_malformed_snapshot_identifier_is_client_error(self):
        for value in (None, [], {}, 42, True):
            for endpoint in ("plans","advice"):
                payload = {"schema_version":1,"snapshot_id":value}
                payload.update({"action_id":"diagnostics.refresh","arguments":{}} if endpoint=="plans"
                               else {"question":"Status?","idempotency_key":"bad-id"})
                result=self.alice.post(f"/api/v1/projects/9001/setup/{endpoint}",json=payload)
                self.assertTrue(400<=result.status_code<500,
                                f"{endpoint}, {type(value).__name__}: HTTP {result.status_code}")

if __name__=="__main__": unittest.main()
