import json
import os
import secrets
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from fastapi import FastAPI
from nginx_qa.scope_api import register_scope_api


class OperatorSessionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.admin = secrets.token_hex(32)
        self.environment = patch.dict(os.environ, {"NGINX_QA_SCOPE_CONTROL_ADMIN_TOKEN": self.admin})
        self.environment.start()
        host = SimpleNamespace(app=FastAPI(), parse_strict_json_object=json.loads)
        register_scope_api(host)
        self.app = host.app

    def tearDown(self):
        self.environment.stop()

    async def request(self, method="GET", path="/api/v1/operator/session", payload=None, headers=None, raw_body=None):
        sent = False
        messages = []
        async def receive():
            nonlocal sent
            if sent:
                return {"type": "http.disconnect"}
            sent = True
            body = raw_body if raw_body is not None else json.dumps(payload).encode() if payload is not None else b""
            return {"type": "http.request", "body": body, "more_body": False}
        async def send(message):
            messages.append(message)
        scope = {"type": "http", "asgi": {"version": "3.0"}, "method": method, "path": path,
            "raw_path": path.encode(), "query_string": b"", "root_path": "", "scheme": "http", "http_version": "1.1",
            "headers": [(b"host", b"127.0.0.1:19225"), *((str(k).lower().encode(), str(v).encode()) for k, v in (headers or {}).items())],
            "server": ("127.0.0.1", 19225), "client": ("127.0.0.1", 35000)}
        await self.app(scope, receive, send)
        start = next(item for item in messages if item["type"] == "http.response.start")
        response = json.loads(b"".join(item.get("body", b"") for item in messages if item["type"] == "http.response.body"))
        return start["status"], response, {key.decode(): value.decode() for key, value in start["headers"]}

    async def test_pairing_needs_admin_cookie_csrf_origin_and_logout(self):
        code, anonymous, headers = await self.request()
        self.assertEqual(code, 200)
        self.assertFalse(anonymous["authenticated"])
        self.assertIsNone(anonymous["csrf_token"])
        self.assertIn("HttpOnly", headers["set-cookie"])
        self.assertIn("SameSite=strict", headers["set-cookie"])
        cookie = headers["set-cookie"].split(";", 1)[0]
        path = "/api/v1/operator/session/authorize"
        payload = {"pairing_code": anonymous["pairing_code"]}
        code, _, _ = await self.request("POST", path, payload)
        self.assertEqual(code, 403)
        code, _, _ = await self.request("POST", path, payload, {"X-Nginx-QA-Scope-Control-Token": self.admin, "Origin": "https://untrusted.invalid"})
        self.assertEqual(code, 403)
        code, authorized, _ = await self.request("POST", path, payload, {"X-Nginx-QA-Scope-Control-Token": self.admin})
        self.assertEqual(code, 200)
        self.assertFalse(authorized["credentials_returned"])
        code, current, _ = await self.request(headers={"Cookie": cookie})
        self.assertTrue(current["authenticated"])
        self.assertNotIn(self.admin, json.dumps(current))
        _, other, _ = await self.request()
        self.assertFalse(other["authenticated"])
        code, _, _ = await self.request("DELETE", headers={"Cookie": cookie})
        self.assertEqual(code, 403)
        code, _, _ = await self.request("DELETE", headers={"Cookie": cookie, "X-Nginx-QA-CSRF": current["csrf_token"]})
        self.assertEqual(code, 200)
        _, expired, _ = await self.request(headers={"Cookie": cookie})
        self.assertFalse(expired["authenticated"])

    async def test_credential_in_json_is_rejected_before_any_output(self):
        code, result, _ = await self.request("POST", "/api/v1/operator/session/authorize",
            {"pairing_code": self.admin}, {"X-Nginx-QA-Scope-Control-Token": self.admin})
        self.assertEqual(code, 400)
        self.assertEqual(result["detail"]["error"], "CREDENTIAL_IN_REQUEST_FORBIDDEN")
        self.assertNotIn(self.admin, json.dumps(result))

    async def test_escaped_json_credential_and_nested_values_are_rejected(self):
        encoded = "".join("\\u%04x" % ord(character) for character in self.admin)
        bodies = [('{"pairing_code":"' + encoded + '"}').encode(),
            ('{"pairing_code":"invalid","nested":[{"text":"' + encoded + '"}]}').encode()]
        for body in bodies:
            self.assertNotIn(self.admin.encode(), body)
            code, result, _ = await self.request("POST", "/api/v1/operator/session/authorize",
                headers={"X-Nginx-QA-Scope-Control-Token": self.admin}, raw_body=body)
            self.assertEqual(code, 400)
            self.assertEqual(result["detail"]["error"], "CREDENTIAL_IN_REQUEST_FORBIDDEN")
            self.assertNotIn(self.admin, json.dumps(result))


if __name__ == "__main__":
    unittest.main()
