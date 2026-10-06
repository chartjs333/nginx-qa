"""Optional delivery failures must not prevent serving the execution API."""
import asyncio
import unittest
from unittest.mock import patch

import main
from nginx_qa.decision_notifications import NotificationError


class NotificationLifecycleTests(unittest.TestCase):
    def test_invalid_external_config_disables_only_email(self):
        previous = main.app.state.decision_notifications
        try:
            with patch("nginx_qa.scope_api.EmailConfig.from_environment",
                       side_effect=NotificationError("EMAIL_CONFIG_CREDENTIALS_INVALID")):
                service = main.app.state.configure_decision_notifications()
            self.assertFalse(service.config.enabled)
            self.assertIsNone(service.store)
            self.assertEqual(service.last_error_code, "EMAIL_CONFIG_INVALID")
            asyncio.run(service.start(background=False))
            asyncio.run(service.stop())
        finally:
            main.app.state.decision_notifications = previous
