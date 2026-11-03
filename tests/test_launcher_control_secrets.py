import json
import os
import secrets
import subprocess
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import main
from nginx_qa import control_secrets
from nginx_qa.git_provider import _minimal_environment
from nginx_qa.process_supervisor import ManagedProcessSupervisor, ManagedProcessSupervisorError
from nginx_qa.telegram_launcher_execution import executor_for
from tests import test_managed_scope_secret_boundary as scope_tests


class LauncherSecretBoundaryTests(scope_tests.ManagedScopeSecretBoundaryTests):
    def setUp(self):
        super().setUp()
        self.protected.update({name:secrets.token_hex(32) for name in (
            'NGINX_QA_LAUNCHER_CONTROL_BOT_TOKEN','NGINX_QA_LAUNCHER_CONTROL_WEBHOOK_SECRET',
            'NGINX_QA_LAUNCHER_CONTROL_EXECUTOR_TOKEN','NGINX_QA_LAUNCHER_CONTROL_FUTURE_AUTHORITY',
            'TELEGRAM_BOT_TOKEN','TELEGRAM_WEBHOOK_SECRET')})

    def test_git_source_aliases_fail_before_lookup_and_destinations_are_protected(self):
        for name in self.protected:
            for spelling in (name,name.lower()):
                with self.subTest(name=spelling),patch.object(main.os.environ,'get') as lookup:
                    with self.assertRaises(LookupError):
                        main.managed_git_credential_resolver('env:'+spelling)
                    lookup.assert_not_called()
            with self.assertRaises(ValueError):
                _minimal_environment({name:'untrusted injected value'})
            self.context.process['environment_redacted']={name:'untrusted injected value'}
            with self.assertRaises(ManagedProcessSupervisorError):
                ManagedProcessSupervisor._child_environment(self.supervisor,self.context)

    def test_legacy_child_environment_really_omits_control_values(self):
        with patch.dict(os.environ,self.protected):
            result=subprocess.run([sys.executable,'-c','import os,json; print(json.dumps(dict(os.environ)))'],
                env=control_secrets.sanitized_child_environment(),stdout=subprocess.PIPE,check=True,text=True)
        child=json.loads(result.stdout)
        self.assertFalse(set(self.protected.values()) & set(child.values()))
        self.assertFalse(any(control_secrets.protected_environment_name(name) for name in child))

    def test_trusted_executor_uses_protected_reference_but_child_cannot(self):
        name='NGINX_QA_LAUNCHER_CONTROL_EXECUTOR_TOKEN'
        with patch.dict(os.environ,{name:'test-existing-client-secret','NGINX_QA_LAUNCHER_EXECUTOR_URL':'http://127.0.0.1:19002',
                'NGINX_QA_LAUNCHER_EXECUTOR_SECRET_REF':'env:'+name}):
            adapter=executor_for(SimpleNamespace())
            self.assertEqual('test-existing-client-secret',adapter.secret_resolver('env:'+name))
            with self.assertRaises(LookupError):
                main.managed_process_secret_resolver('env:'+name)
        with self.assertRaises(LookupError):
            control_secrets.trusted_control_resolver('env:ORDINARY_ALIAS')

    def test_public_auth_never_falls_back_to_legacy_raw_bot_token(self):
        with patch.dict(os.environ,{'TELEGRAM_BOT_TOKEN':'legacy-token'},clear=True):
            self.assertEqual('legacy-token',control_secrets.control_secret('bot'))
            self.assertEqual('',control_secrets.control_secret('bot',allow_legacy=False))
