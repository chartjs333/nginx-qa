import contextlib
import io
import os
import unittest
import urllib.error
from unittest import mock

import configure_telegram


class ConfigureTelegramTests(unittest.TestCase):
    ENV_NAMES = (
        "TELEGRAM_BOT_TOKEN",
        "TELEGRAM_WEBHOOK_SECRET",
        "TELEGRAM_WEBHOOK_URL",
        "TELEGRAM_WEBHOOK_AUTO_REGISTER",
        "TELEGRAM_DROP_PENDING_UPDATES",
        "TELEGRAM_ALLOWED_CHAT_IDS",
        "TELEGRAM_ALLOWED_USER_IDS",
        "TELEGRAM_HISTORY_CHAT_ID",
        "TELEGRAM_HISTORY_MESSAGE_THREAD_ID",
        "TELEGRAM_WEBHOOK_REGISTER_RETRY_SECONDS",
        "TELEGRAM_WEBHOOK_IP_ADDRESS",
        "CLOUDFLARED_QUICK_TUNNEL",
    )

    def clean_environment(self, values: dict[str, str] | None = None):
        environment = {name: "" for name in self.ENV_NAMES}
        environment.update(values or {})
        return mock.patch.dict(os.environ, environment, clear=False)

    def test_missing_token_keeps_telegram_disabled_without_failure(self) -> None:
        output = io.StringIO()
        with self.clean_environment(), contextlib.redirect_stdout(output):
            configure_telegram.configure()
        self.assertIn("integration is disabled", output.getvalue())

    def test_auto_registration_requires_https_url_and_real_secret(self) -> None:
        with self.clean_environment(
            {
                "TELEGRAM_BOT_TOKEN": "123:new-token",
                "TELEGRAM_WEBHOOK_AUTO_REGISTER": "1",
                "TELEGRAM_WEBHOOK_URL": "http://localhost/webhook",
                "TELEGRAM_WEBHOOK_SECRET": "valid-secret",
            }
        ):
            with self.assertRaisesRegex(ValueError, "public HTTPS"):
                configure_telegram.configure()

    def test_registration_passes_secret_allowlist_and_drop_flag(self) -> None:
        with self.clean_environment(
            {
                "TELEGRAM_BOT_TOKEN": "123:new-token",
                "TELEGRAM_WEBHOOK_AUTO_REGISTER": "true",
                "TELEGRAM_WEBHOOK_URL": "https://example.com/api/v1/telegram/agents",
                "TELEGRAM_WEBHOOK_SECRET": "secure_webhook-secret",
                "TELEGRAM_DROP_PENDING_UPDATES": "1",
                "TELEGRAM_ALLOWED_CHAT_IDS": "99,-100123",
                "TELEGRAM_ALLOWED_USER_IDS": "42",
            }
        ), mock.patch.object(
            configure_telegram,
            "telegram_request",
            return_value={"ok": True, "result": True},
        ) as request:
            configure_telegram.configure()

        request.assert_called_once()
        token, method, data = request.call_args.args
        self.assertEqual(token, "123:new-token")
        self.assertEqual(method, "setWebhook")
        self.assertEqual(data["secret_token"], "secure_webhook-secret")
        self.assertEqual(data["drop_pending_updates"], "true")
        self.assertIn("message", data["allowed_updates"])

    def test_invalid_allowlist_is_rejected_before_network_call(self) -> None:
        with self.clean_environment(
            {
                "TELEGRAM_BOT_TOKEN": "123:new-token",
                "TELEGRAM_ALLOWED_CHAT_IDS": "not-a-number",
            }
        ):
            with self.assertRaisesRegex(ValueError, "numeric Telegram IDs"):
                configure_telegram.configure()

    def test_invalid_history_destination_is_rejected(self) -> None:
        with self.clean_environment(
            {
                "TELEGRAM_BOT_TOKEN": "123:new-token",
                "TELEGRAM_HISTORY_CHAT_ID": "channel-name",
            }
        ):
            with self.assertRaisesRegex(ValueError, "TELEGRAM_HISTORY_CHAT_ID"):
                configure_telegram.configure()

    def test_transient_webhook_dns_error_is_retried(self) -> None:
        with self.clean_environment(
            {
                "TELEGRAM_BOT_TOKEN": "123:new-token",
                "TELEGRAM_WEBHOOK_AUTO_REGISTER": "1",
                "TELEGRAM_WEBHOOK_URL": "https://fresh.trycloudflare.com/hook",
                "TELEGRAM_WEBHOOK_SECRET": "valid-secret",
                "CLOUDFLARED_QUICK_TUNNEL": "1",
            }
        ), mock.patch.object(
            configure_telegram,
            "telegram_request",
            side_effect=[
                RuntimeError(
                    "Telegram setWebhook failed with HTTP 400: "
                    "Bad Request: bad webhook: Failed to resolve host"
                ),
                {"ok": True, "result": True},
            ],
        ) as request, mock.patch.object(
            configure_telegram.socket,
            "getaddrinfo",
            return_value=[],
        ), mock.patch.object(configure_telegram.time, "sleep") as sleep:
            configure_telegram.configure()

        self.assertEqual(request.call_count, 2)
        sleep.assert_called_once_with(5.0)

    def test_quick_tunnel_ip_is_passed_to_telegram(self) -> None:
        with self.clean_environment(
            {
                "TELEGRAM_BOT_TOKEN": "123:new-token",
                "TELEGRAM_WEBHOOK_AUTO_REGISTER": "1",
                "TELEGRAM_WEBHOOK_URL": "https://fresh.trycloudflare.com/hook",
                "TELEGRAM_WEBHOOK_SECRET": "valid-secret",
                "CLOUDFLARED_QUICK_TUNNEL": "1",
            }
        ), mock.patch.object(
            configure_telegram.socket,
            "getaddrinfo",
            return_value=[
                (
                    configure_telegram.socket.AF_INET,
                    configure_telegram.socket.SOCK_STREAM,
                    6,
                    "",
                    ("104.16.230.132", 443),
                )
            ],
        ), mock.patch.object(
            configure_telegram,
            "telegram_request",
            return_value={"ok": True, "result": True},
        ) as request:
            configure_telegram.configure()

        self.assertEqual(request.call_args.args[2]["ip_address"], "104.16.230.132")

    def test_http_error_includes_telegram_description(self) -> None:
        error = urllib.error.HTTPError(
            "https://api.telegram.org/redacted/setWebhook",
            400,
            "Bad Request",
            {},
            io.BytesIO(
                b'{"ok":false,"description":"Bad Request: bad webhook: Failed to resolve host"}'
            ),
        )
        with mock.patch.object(configure_telegram.urllib.request, "urlopen", side_effect=error):
            with self.assertRaisesRegex(RuntimeError, "Failed to resolve host"):
                configure_telegram.telegram_request("redacted", "setWebhook", {"url": "https://x"})


if __name__ == "__main__":
    unittest.main()
