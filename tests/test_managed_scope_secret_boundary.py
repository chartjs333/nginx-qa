"""Managed children must not inherit or request scope-control credentials."""

import os
import secrets
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import main
from nginx_qa.process_supervisor import ManagedProcessSupervisor


class ManagedScopeSecretBoundaryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.protected = {
            name: secrets.token_hex(32)
            for name in (
                "NGINX_QA_SCOPE_CONTROL_ADMIN_TOKEN",
                "NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_2750",
                "NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_2751",
                "NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_2752",
                "NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_2753",
                "NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_2754",
                "NGINX_QA_SCOPE_CONTROL_FUTURE_CREDENTIAL",
            )
        }
        self.context = SimpleNamespace(
            process={
                "environment_redacted": {},
                "process_id": "isolated-process",
                "runtime_root": "isolated-runtime",
                "launch_nonce": "isolated-nonce",
            },
            lease={"host": "127.0.0.1", "port": 19391},
        )
        # Exercise the production environment builder without constructing a
        # supervisor, opening a runtime store, or spawning a child process.
        self.supervisor = SimpleNamespace(
            secret_resolver=main.managed_process_secret_resolver,
        )

    def test_scope_control_credentials_are_not_automatically_inherited(self) -> None:
        with patch.dict(os.environ, self.protected, clear=False):
            environment = ManagedProcessSupervisor._child_environment(
                self.supervisor, self.context
            )
        self.assertFalse(
            any(key.casefold().startswith("nginx_qa_scope_control_") for key in environment),
            "child inherited a protected environment name",
        )
        self.assertFalse(
            set(self.protected.values()).intersection(environment.values()),
            "child inherited a protected value through an alias",
        )

    def test_explicit_admin_and_role_aliases_fail_before_environment_lookup(self) -> None:
        for name in self.protected:
            for spelling in (name, name.lower(), name.swapcase()):
                with self.subTest(variable=spelling):
                    self.context.process["environment_redacted"] = {
                        "ORDINARY_CHILD_SECRET": {"secret_ref": "env:" + spelling}
                    }
                    resolver = Mock(wraps=main.managed_process_secret_resolver)
                    self.supervisor.secret_resolver = resolver
                    with patch.dict(os.environ, self.protected, clear=False):
                        with self.assertRaises(LookupError) as raised:
                            ManagedProcessSupervisor._child_environment(
                                self.supervisor, self.context
                            )
                    self.assertEqual(str(raised.exception), "managed process secret reference is protected")
                    self.assertEqual(resolver.call_count, 1)
                    with patch.object(main.os.environ, "get") as lookup:
                        with self.assertRaises(LookupError):
                            main.managed_process_secret_resolver("env:" + spelling)
                        lookup.assert_not_called()

    def test_ordinary_ephemeral_child_secret_still_resolves(self) -> None:
        value = secrets.token_hex(32)
        self.context.process["environment_redacted"] = {
            "ORDINARY_CHILD_SECRET": {"secret_ref": "env:NGINX_QA_TEST_CHILD_SECRET"}
        }
        with patch.dict(os.environ, {"NGINX_QA_TEST_CHILD_SECRET": value}, clear=False):
            environment = ManagedProcessSupervisor._child_environment(
                self.supervisor, self.context
            )
        self.assertTrue(
            environment["ORDINARY_CHILD_SECRET"] == value,
            "ordinary child secret resolution changed",
        )


if __name__ == "__main__":
    unittest.main()
