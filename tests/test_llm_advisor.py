"""Advisor tests use explicit transport fixtures; none is a live model proof."""
from __future__ import annotations
import asyncio
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import unittest
import httpx
from nginx_qa.setup_advisor import (
    SetupAdvisor, ResponsesAdviceTransport, AdvisorBusyError,
    get_advice_request_validator, get_advice_response_validator,
)

def snapshot():
    now = datetime.now(timezone.utc)
    return {
        "snapshot_id": "c" * 64, "project_id": "9000",
        "generated_at": now.isoformat(), "authority_fingerprint": "d" * 64,
        "checks": [
            {"check_id": "project.slot", "status": "PASS", "scope": "passive",
             "freshness": "FRESH", "expires_at": (now + timedelta(seconds=300)).isoformat(),
             "error_code": None, "evidence": [{"fact_id": "secret", "value": "private-canary"}]},
            {"check_id": "worker.configuration", "status": "NOT_CONFIGURED",
             "scope": "passive", "freshness": "UNOBSERVED", "expires_at": None,
             "error_code": "NOT_CONFIGURED", "evidence": []},
        ],
        "available_action_ids": ["diagnostics.refresh", "setup.open", "worker.bind"],
    }

def request(snap=None, key="request-1"):
    return {"schema_version": 1, "snapshot_id": (snap or snapshot())["snapshot_id"],
            "question": "Как подключить исполнителя?", "idempotency_key": key}

def model_output(snap=None):
    snap = snap or snapshot()
    return {
        "kind": "setup_advice", "schema_version": 1, "snapshot_id": snap["snapshot_id"],
        "explanation": "Проверьте настройку исполнителя, это рекомендация.",
        "claims": [{"text": "Проверка настройки не выполнена.",
                    "check_ids": ["worker.configuration"], "certainty": "UNKNOWN"}],
        "proposals": [{"action_id": "setup.open", "arguments": {"topic": "worker"},
                       "check_ids": ["worker.configuration"], "reason": "Прочитать инструкцию."}],
        "uncertainty": "HIGH",
    }

class TestLLMAdvisor(unittest.IsolatedAsyncioTestCase):
    async def test_fallback_is_explicit_and_unknown_is_not_ready(self):
        snap = snapshot()
        snap["checks"][0].update(status="UNKNOWN", freshness="UNOBSERVED")
        advisor = SetupAdvisor(environment={})
        result = await advisor.get_advice(snap, request(snap))
        get_advice_response_validator().validate(result)
        self.assertIn("PROVIDER_NOT_CONFIGURED", result["explanation"])
        self.assertEqual(result["uncertainty"], "HIGH")
        self.assertTrue(all(c["certainty"] == "UNKNOWN" for c in result["claims"]))
        self.assertNotIn("свободен", json.dumps(result, ensure_ascii=False))
        self.assertNotIn("настроен через", json.dumps(result, ensure_ascii=False))
        codex = SetupAdvisor(environment={"NGINX_QA_SETUP_ADVISOR_PROVIDER": "codex"})
        result = await codex.get_advice(snap, request(snap))
        self.assertIn("CODEX_TOOL_ISOLATION_UNVERIFIED", result["explanation"])

    async def test_real_adapter_interface_invoked_once_and_cache_is_actor_scoped(self):
        calls = []
        async def transport(facts, question):
            calls.append((facts, question))
            return json.dumps(model_output())
        advisor = SetupAdvisor(environment={}, transport=transport)
        snap, req = snapshot(), request()
        first = await advisor.get_advice(snap, req, actor_id="alice")
        second = await advisor.get_advice(snap, req, actor_id="alice")
        self.assertEqual(first, second)
        self.assertEqual(len(calls), 1)
        self.assertIn("Ответ модели", first["explanation"])
        await advisor.get_advice(snap, req, actor_id="bob")
        self.assertEqual(len(calls), 2)
        with self.assertRaisesRegex(ValueError, "idempotency"):
            await advisor.get_advice(snap, dict(req, question="Different"), actor_id="alice")

    async def test_minimal_projection_and_question_redaction(self):
        seen = []
        async def transport(facts, question):
            seen.append((facts, question))
            return json.dumps(model_output())
        advisor = SetupAdvisor(environment={}, transport=transport)
        snap = snapshot()
        snap["checks"][0]["raw_config"] = {"secret": "private-canary"}
        req = request()
        req["question"] = r"Check https://secret.example/key at C:\private\canary; user@example.com 127.0.0.1 " + "x" * 40
        await advisor.get_advice(snap, req)
        packed = json.dumps(seen)
        for canary in ("private-canary", "secret.example", "user@example.com", "127.0.0.1", "x"*40, "authority_fingerprint", "evidence"):
            self.assertNotIn(canary, packed)
        self.assertNotIn("project_id", seen[0][0])

    async def test_semantics_fail_closed(self):
        snap = snapshot()
        cases = []
        bad = model_output(); bad["snapshot_id"] = "f"*64; cases.append(bad)
        bad = model_output(); bad["claims"][0]["check_ids"] = ["not.observed"]; cases.append(bad)
        bad = model_output(); bad["claims"][0]["certainty"] = "OBSERVED_FACT"; cases.append(bad)
        bad = model_output(); bad["claims"][0]["certainty"] = "INFERENCE"; cases.append(bad)
        bad = model_output(); bad["proposals"][0].update(action_id="worker.bind", arguments={"candidate_id":"invented","probe_receipt_id":"fake"}); cases.append(bad)
        bad = model_output(); bad["proposals"][0]["arguments"]["command"] = "whoami"; cases.append(bad)
        bad = model_output(); bad["extra"] = True; cases.append(bad)
        for index, bad in enumerate(cases):
            with self.subTest(index=index):
                async def transport(*args): return json.dumps(bad)
                result = await SetupAdvisor(transport=transport).get_advice(snap, request(key=str(index)))
                self.assertIn("PROVIDER_UNAVAILABLE_OR_INVALID", result["explanation"])
                get_advice_response_validator().validate(result)

    async def test_duplicate_keys_depth_size_and_secret_exception_are_rejected(self):
        samples = ['{"kind":"setup_advice","kind":"setup_advice"}',
                   '{"a":' * 17 + "0" + "}"*17, "x" * 8193]
        for raw in samples:
            async def transport(*args): return raw
            result = await SetupAdvisor(transport=transport).get_advice(snapshot(), request())
            self.assertIn("PROVIDER_UNAVAILABLE_OR_INVALID", result["explanation"])
        async def broken(*args): raise RuntimeError("sk-secret-canary")
        result = await SetupAdvisor(transport=broken).get_advice(snapshot(), request())
        self.assertNotIn("sk-secret-canary", json.dumps(result))

    async def test_stale_snapshot_never_calls_provider_and_output_rechecks_freshness(self):
        calls = []
        snap = snapshot()
        snap["generated_at"] = (datetime.now(timezone.utc)-timedelta(minutes=6)).isoformat()
        async def transport(*args): calls.append(True); return json.dumps(model_output())
        result = await SetupAdvisor(transport=transport).get_advice(snap, request(snap))
        self.assertFalse(calls)
        self.assertIn("SNAPSHOT_STALE", result["explanation"])
        snap = snapshot()
        async def expires(*args):
            snap["generated_at"] = (datetime.now(timezone.utc)-timedelta(minutes=6)).isoformat()
            return json.dumps(model_output())
        result = await SetupAdvisor(transport=expires).get_advice(snap, request(snap))
        self.assertIn("PROVIDER_UNAVAILABLE_OR_INVALID", result["explanation"])

    async def test_concurrency_and_cancellation_do_not_duplicate_inference(self):
        started = asyncio.Event()
        finish = asyncio.Event()
        calls = []
        async def transport(*args):
            calls.append(True); started.set(); await finish.wait()
            return json.dumps(model_output())
        advisor = SetupAdvisor(transport=transport)
        snap, req = snapshot(), request()
        task = asyncio.create_task(advisor.get_advice(snap, req))
        await started.wait()
        with self.assertRaises(AdvisorBusyError):
            await advisor.get_advice(snap, request(key="different"))
        with self.assertRaises(AdvisorBusyError):
            await advisor.get_advice(snap, req)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError): await task
        replay = await advisor.get_advice(snap, req)
        self.assertIn("REQUEST_CANCELLED", replay["explanation"])
        self.assertEqual(len(calls), 1)
        finish.set()
        result = await advisor.get_advice(snap, request(key="new"))
        self.assertIn("Ответ модели", result["explanation"])

    async def test_utf8_question_limit(self):
        with self.assertRaisesRegex(ValueError, "4 KiB"):
            await SetupAdvisor().get_advice(snapshot(), dict(request(), question="я" * 3000))

class TestResponsesTransport(unittest.IsolatedAsyncioTestCase):
    def adapter(self, handler):
        options = []
        def factory(**kwargs):
            options.append(kwargs)
            return httpx.AsyncClient(transport=httpx.MockTransport(handler), **kwargs)
        return ResponsesAdviceTransport(model="fixture-model",
            secret_ref="env:NGINX_QA_LAUNCHER_CONTROL_ADVISOR_KEY",
            resolver=lambda _: "fixture-secret-not-live", client_factory=factory), options

    async def test_http_request_has_no_tools_no_redirects_no_secret_context(self):
        requests = []
        def handler(req):
            body = json.loads(req.content); requests.append((req, body))
            return httpx.Response(200, json={"status":"completed", "output":[
                {"type":"message", "role":"assistant",
                 "content":[{"type":"output_text","text":json.dumps(model_output())}]}]})
        adapter, options = self.adapter(handler)
        raw = await adapter({"minimal":True}, "Вопрос")
        self.assertEqual(json.loads(raw)["kind"], "setup_advice")
        req, body = requests[0]
        self.assertEqual(str(req.url), "https://api.openai.com/v1/responses")
        self.assertEqual(body["tools"], [])
        self.assertEqual(body["tool_choice"], "none")
        self.assertFalse(body["store"])
        self.assertNotIn("fixture-secret-not-live", body["input"])
        self.assertEqual(req.headers["authorization"], "Bearer fixture-secret-not-live")
        self.assertFalse(options[0]["follow_redirects"])
        self.assertFalse(options[0]["trust_env"])

    async def test_http_tool_call_oversize_redirect_and_incomplete_are_rejected(self):
        samples = [
            httpx.Response(200,json={"status":"completed","output":[{"type":"function_call","name":"exec"}]}),
            httpx.Response(200,content=b"x"*32769),
            httpx.Response(302,headers={"Location":"http://127.0.0.1:18035"}),
            httpx.Response(200,json={"status":"incomplete","output":[]}),
        ]
        for sample in samples:
            adapter, _ = self.adapter(lambda _: sample)
            with self.assertRaises(ValueError): await adapter({}, "question")

if __name__ == "__main__": unittest.main()
