import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from nginx_qa import launcher_tunnel as tunnel
from nginx_qa.launcher_origin import owned_origin
from nginx_qa.telegram_launcher_controls import gateway_origin
from tests.test_launcher_host import LauncherHostGuards
from tests.test_staging_isolation import run_powershell,assert_powershell_success,parse_environment_file,REPOSITORY_ROOT


class OwnedOriginTests(LauncherHostGuards):
    def receipt(self):
        proof=json.loads(self.marker.read_text())
        return {'schema_version':1,'initialization_id':proof['initialization_id'],'instance_id':'test-gateway',
            'upstream':'http://127.0.0.1:18037','phase':'READY','public_origin':'https://fixture-one.trycloudflare.com','heartbeat':1000}

    def test_origin_is_owned_fresh_and_reconnect_changes_binding(self):
        path=Path(self.env['NGINX_QA_LAUNCHER_ORIGIN_STATE']);record=self.receipt()
        with patch.dict(os.environ,self.env,clear=True):
            self.assertIsNone(owned_origin(now=1000))
            path.write_text(json.dumps(record));self.assertEqual(record['public_origin'],owned_origin(now=1005))
            self.assertIsNone(owned_origin(now=1016));self.assertIsNone(owned_origin(now=999))
            record['public_origin']='https://fixture-two.trycloudflare.com';path.write_text(json.dumps(record))
            with patch('nginx_qa.launcher_origin.time.time',return_value=1001):
                self.assertEqual(record['public_origin'],gateway_origin())
            record['phase']='FAILED';path.write_text(json.dumps(record));self.assertIsNone(owned_origin(now=1001))
            record['initialization_id']='other';path.write_text(json.dumps(record))
            with self.assertRaises(ValueError):owned_origin(now=1001)

    def test_no_foreign_upstream_or_receipt_path(self):
        path=Path(self.env['NGINX_QA_LAUNCHER_ORIGIN_STATE']);record=self.receipt()
        record['upstream']='http://127.0.0.1:18036';path.write_text(json.dumps(record))
        with patch.dict(os.environ,self.env,clear=True):
            with self.assertRaises(ValueError):owned_origin(now=1001)
        with patch.dict(os.environ,{**self.env,'NGINX_QA_LAUNCHER_ORIGIN_STATE':str(self.base/'foreign.json')},clear=True):
            with self.assertRaises(ValueError):owned_origin(now=1001)


class TunnelProtocolTests(unittest.TestCase):
    def test_webhook_updates_only_its_previous_origin_never_drops_messages(self):
        calls=[]
        def request(token,method,data):
            calls.append((method,data));return {'ok':True,'result':{'url':'https://old.trycloudflare.com/launcher/telegram/webhook'}}
        with patch.object(tunnel,'control_secret',side_effect=lambda purpose,**kw:'fixture-bot' if purpose=='bot' else 'fixture_webhook'):
            tunnel.webhook_registration('https://new.trycloudflare.com','https://old.trycloudflare.com',request=request)
            self.assertEqual('false',calls[-1][1]['drop_pending_updates'])
            self.assertEqual(['message','callback_query'],json.loads(calls[-1][1]['allowed_updates']))
            calls.clear()
            with self.assertRaises(ValueError):tunnel.webhook_registration('https://new.trycloudflare.com',None,request=request)
            self.assertEqual(['getWebhookInfo'],[call[0] for call in calls])

    def test_unknown_or_reused_pid_is_never_signalled(self):
        with patch.object(tunnel,'_pid_definitely_dead',return_value=False) as probe:
            with self.assertRaises(RuntimeError):tunnel.assert_prior_dead({'monitor':{'pid':4321}})
            probe.assert_called_once_with(4321)
        with patch.object(tunnel,'_pid_definitely_dead',return_value=True):tunnel.assert_prior_dead({'child':{'pid':4321}})

    def test_tunnel_child_has_no_control_or_project_credentials(self):
        with patch.dict(os.environ,{'SystemRoot':'C:/Windows','PATH':'fixture','NGINX_QA_LAUNCHER_CONTROL_BOT_TOKEN':'not-exported',
                'TUNNEL_URL':'http://wrong','GITHUB_TOKEN':'not-exported'},clear=True):
            self.assertEqual({'SYSTEMROOT':'C:/Windows','PATH':'fixture'},{key.upper():value for key,value in tunnel.child_environment().items()})

    @unittest.skipUnless(os.name=='nt','Windows Job integration')
    def test_native_job_stop_is_scoped_to_owned_monitor_and_child(self):
        with tempfile.TemporaryDirectory(prefix='launcher-tunnel-test-') as directory:
            root=Path(directory);exe=root/'cloudflared.exe';exe.write_bytes(b'fixture path only; subprocess replaced with real Python')
            config={'origin_state':root/'launcher-origin.json','initialization_id':'fixture-init',
                'ownership':{'instance_id':'fixture'},'public_port':19999}
            original_spawn=tunnel._spawn_suspended_in_windows_job;stop=threading.Event();errors=[]
            def spawn(job,command,**kwargs):
                self.assertFalse(kwargs['inherit_recovery_job_handle'])
                return original_spawn(job,[sys.executable,'-u','-c',
                    'import time; print("https://native-fixture.trycloudflare.com",flush=True); time.sleep(60)'],**kwargs)
            def run():
                try:tunnel.monitor(config,exe,startup_timeout=5,shutdown=stop)
                except BaseException as error:
                    import traceback
                    errors.append(traceback.format_exc())
            with patch.object(tunnel,'_spawn_suspended_in_windows_job',side_effect=spawn),patch.dict(os.environ,{'NGINX_QA_LAUNCHER_PUBLIC_ORIGIN':''}):
                worker=threading.Thread(target=run);worker.start()
                try:
                    deadline=time.monotonic()+10;receipt=None
                    while time.monotonic()<deadline and worker.is_alive():
                        if config['origin_state'].exists():
                            receipt=tunnel.read_receipt(config['origin_state'],config)
                            if receipt['phase']=='READY':break
                        time.sleep(.05)
                    self.assertFalse(errors);self.assertEqual('READY',receipt['phase'])
                    child=receipt['child'];self.assertFalse(tunnel._pid_definitely_dead(child['pid']))
                    with self.assertRaises(OSError):
                        with tunnel.exclusive_monitor(tunnel.paths(config)[2]):pass
                    tunnel.request_stop(config);worker.join(15)
                    self.assertFalse(worker.is_alive());self.assertFalse(errors)
                    self.assertTrue(tunnel._pid_definitely_dead(child['pid']))
                    self.assertEqual('STOPPED',tunnel.read_receipt(config['origin_state'],config)['phase'])
                    self.assertIsNotNone(tunnel.process_identity(os.getpid()))
                finally:stop.set();worker.join(20)

    def test_failed_restart_preserves_acknowledged_and_attempted_webhook_origins(self):
        a='https://a.trycloudflare.com';b='https://b.trycloudflare.com';c='https://c.trycloudflare.com'
        prior={'phase':'FAILED','public_origin':None,'registered_origin':a,'registration_origins':[a,b]}
        history=tunnel.webhook_history(prior)
        self.assertEqual(a,history['registered_origin']);self.assertEqual([a,b],history['registration_origins'])
        for actual in (a,b):
            calls=[]
            def request(token,method,data):
                calls.append(method);return {'ok':True,'result':{'url':actual+'/launcher/telegram/webhook'}}
            with patch.object(tunnel,'control_secret',return_value='fixture_secret'):
                tunnel.webhook_registration(c,history['registration_origins'],request=request)
            self.assertEqual(['getWebhookInfo','setWebhook'],calls)


class GatewayProfileTests(unittest.TestCase):
    def test_profile_frozen_values_and_gateway_port_guard(self):
        completed=run_powershell(r'''
            $values=Get-ExpectedStagingEnvironment -Profile "telegram-managed-sprint-launcher-v1-gateway"
            foreach ($entry in $values.GetEnumerator()) { [Environment]::SetEnvironmentVariable($entry.Key,$entry.Value,"Process") }
            Assert-ExactStagingEnvironment -ExpectedValues $values
            $rejected=$false
            [Environment]::SetEnvironmentVariable("NGINX_QA_GATEWAY_HOST","0.0.0.0","Process")
            try { Assert-ExactStagingEnvironment -ExpectedValues $values } catch { $rejected=$true }
            if (-not $rejected) { throw "Wildcard gateway was accepted" }
            $values | ConvertTo-Json -Compress
        ''')
        assert_powershell_success(self,completed)
        values=json.loads(completed.stdout.strip().splitlines()[-1])
        template=parse_environment_file(REPOSITORY_ROOT/'orchestration/sprints/telegram-managed-sprint-launcher-v1/gateway-18036.env.example')
        self.assertTrue(all(template[key]==value for key,value in values.items()))
        self.assertEqual(('18036','18037','18700-18749'),tuple(values[key] for key in ('NGINX_QA_HTTP_PORT','NGINX_QA_GATEWAY_PORT','NGINX_QA_CHILD_PORT_RANGE')))
        self.assertEqual(('true','true'),(values['NGINX_QA_TELEGRAM_LAUNCHER_ENABLED'],values['NGINX_QA_LAUNCHER_GATEWAY_ENABLED']))
