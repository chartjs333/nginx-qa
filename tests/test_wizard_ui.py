"""Automated qualification suite for Setup and Diagnostics Wizard v1 (wizard-ui).

Acceptance cases:
- A02: Private authenticated wizard and public gateway denial
- A18: Injection, XSS and unsafe navigation defense
- A21: Reusable Russian private wizard UX and Plan & Apply integration
"""
from __future__ import annotations

import os
from pathlib import Path
import re
import json
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

import main as app_module
from nginx_qa.legacy_scope_control import ADMIN_TOKEN_HEADER, ADMIN_TOKEN_ENV
from nginx_qa.launcher_gateway import create_gateway


TEST_ADMIN_TOKEN = "e" * 32


class TestWizardUI(unittest.TestCase):
    def test_production_javascript_requests_and_retry_behavior(self):
        root = Path(__file__).resolve().parents[1]
        node = shutil.which("node")
        self.assertIsNotNone(node, "Node is required to verify the shipped UI behavior")
        run = subprocess.run([node, str(root / "tests/setup_ui_harness.cjs")],
                             cwd=root, capture_output=True, text=True, timeout=30)
        self.assertEqual(run.returncode, 0, run.stderr)
        result = json.loads(run.stdout)
        self.assertTrue(result["passed"])
        from nginx_qa.setup_advisor import _validator
        _validator("PlanRequest").validate(result["plan_request"])
        _validator("ApplyRequest").validate(result["apply_request"])

    @classmethod
    def setUpClass(cls):
        os.environ[ADMIN_TOKEN_ENV] = TEST_ADMIN_TOKEN
        cls.app = app_module.app
        cls.client = TestClient(cls.app, raise_server_exceptions=False)

    def test_a02_private_ui_and_assets(self):
        # 1. GET /setup returns HTML with security headers
        res = self.client.get("/setup")
        self.assertEqual(res.status_code, 200)
        self.assertIn("text/html", res.headers.get("content-type", ""))
        self.assertIn("default-src 'self'", res.headers.get("content-security-policy", ""))
        self.assertEqual(res.headers.get("x-content-type-options"), "nosniff")
        self.assertIn("Мастер настройки и диагностики", res.text)

        # 2. GET /setup/assets/setup.js
        res_js = self.client.get("/setup/assets/setup.js")
        self.assertEqual(res_js.status_code, 200)
        self.assertIn("javascript", res_js.headers.get("content-type", ""))

        # 3. GET /setup/assets/setup.css
        res_css = self.client.get("/setup/assets/setup.css")
        self.assertEqual(res_css.status_code, 200)
        self.assertIn("text/css", res_css.headers.get("content-type", ""))

        # 4. Unknown asset or traversal rejected with 404
        res_bad = self.client.get("/setup/assets/evil.exe")
        self.assertEqual(res_bad.status_code, 404)

        res_traversal = self.client.get("/setup/assets/../main.py")
        self.assertEqual(res_traversal.status_code, 404)

    def test_a02_public_gateway_denial(self):
        # Public gateway must never route /setup or /setup/assets
        gateway_app = create_gateway(app_module)
        gw_client = TestClient(gateway_app, raise_server_exceptions=False)

        # 1. Gateway offline / disabled -> 503
        with patch.dict(os.environ, {"NGINX_QA_LAUNCHER_GATEWAY_ENABLED": "0"}):
            r = gw_client.get("/setup")
            self.assertEqual(r.status_code, 503)

        # 2. Gateway enabled -> 404 for unmounted private wizard routes
        with patch.dict(
            os.environ,
            {
                "NGINX_QA_LAUNCHER_GATEWAY_ENABLED": "1",
                "NGINX_QA_LAUNCHER_GATEWAY_TARGET_URL": "http://127.0.0.1:18035",
            },
        ), patch("nginx_qa.launcher_gateway.gateway_origin", return_value="http://public.test"):
            r = gw_client.get("/setup", headers={"Host": "public.test"})
            self.assertIn(r.status_code, (404, 503))

            r_asset = gw_client.get("/setup/assets/setup.js", headers={"Host": "public.test"})
            self.assertIn(r_asset.status_code, (404, 503))

    def test_a18_xss_and_injection_immunity(self):
        static_dir = Path(__file__).resolve().parents[1] / "nginx_qa" / "static"
        html_content = (static_dir / "setup.html").read_text(encoding="utf-8")
        js_content = (static_dir / "setup.js").read_text(encoding="utf-8")

        # 1. No inline script in setup.html
        scripts = re.findall(r"<script(?![^>]*src=)[^>]*>(.*?)</script>", html_content, re.DOTALL | re.IGNORECASE)
        for s in scripts:
            self.assertEqual(s.strip(), "", "Found inline script in setup.html")

        # 2. No dangerous DOM sink in setup.js (innerHTML, outerHTML, document.write, eval)
        self.assertNotIn(".innerHTML", js_content)
        self.assertNotIn(".outerHTML", js_content)
        self.assertNotIn("document.write", js_content)
        self.assertNotIn("eval(", js_content)

        # 3. Verify textContent is used for rendering
        self.assertIn(".textContent", js_content)

    def test_a21_russian_reusable_wizard_ux(self):
        static_dir = Path(__file__).resolve().parents[1] / "nginx_qa" / "static"
        html_content = (static_dir / "setup.html").read_text(encoding="utf-8")

        # 1. Russian language declaration and labels
        self.assertIn('<html lang="ru">', html_content)
        self.assertIn("Мастер настройки и диагностики", html_content)
        self.assertIn("LLM-помощник", html_content)
        self.assertIn("Серверные операции", html_content)
        self.assertIn("Диагностический снимок", html_content)

        # 2. Plan & Apply modal elements
        self.assertIn('dialog id="plan-dialog"', html_content)
        self.assertIn('id="plan-apply-btn"', html_content)
        self.assertIn('id="plan-cancel-btn"', html_content)

        # 3. Accessibility attributes
        self.assertIn('role="status"', html_content)
        self.assertIn('role="alert"', html_content)
        self.assertIn('role="region"', html_content)
        self.assertIn('aria-live="polite"', html_content)
        self.assertIn('aria-label=', html_content)


if __name__ == "__main__":
    unittest.main()
