import contextlib
import io
import os
import unittest
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


if __name__ == "__main__":
    unittest.main()
