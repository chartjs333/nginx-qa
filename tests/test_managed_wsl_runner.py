"""Controller boundary regressions. These mocks are not live sandbox evidence."""
import base64
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import time
import unittest
from unittest.mock import patch,Mock

from nginx_qa.managed_wsl_runner import WslCodexRunner,document_path,regular_destination


class ControllerBoundaryTests(unittest.TestCase):
    def runner(self):
        r=object.__new__(WslCodexRunner)
        r._slots=threading.BoundedSemaphore(1);r._quarantined_job=None;r.timeout=60
        r.sandbox_diagnostic={'state':'NOT_CHECKED'};r.ready=Mock();r.distro='NginxQaWorker-test'
        return r

    def test_document_path_rejects_escape_and_non_docs(self):
        for p in ('../x','docs/../x.md','docs//x.md','docs/.x/../y.md','docs/x.txt','docs/x.md:stream','C:/docs/x.md','docs\\x.md','/docs/x.md','docs/a/./b.md'):
            with self.subTest(p=p),self.assertRaises(ValueError):document_path(p)
        self.assertEqual(document_path('docs/setup-selftest.md'),'docs/setup-selftest.md')

    def test_destination_rejects_hardlink(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);(root/'docs').mkdir();(root/'source').write_text('untouched')
            os.link(root/'source',root/'docs/x.md')
            with self.assertRaisesRegex(ValueError,'WSL_DESTINATION_INVALID'):regular_destination(root,'docs/x.md')
            self.assertEqual((root/'source').read_text(),'untouched')

    def test_quiescence_failure_retains_slot(self):
        r=self.runner();r.quiesce=Mock(side_effect=ValueError('CLIENT_QUIESCENCE_UNPROVEN'))
        with tempfile.TemporaryDirectory() as t,patch('nginx_qa.managed_wsl_runner.regular_destination',side_effect=ValueError('fixture-before-launch')):
            with self.assertRaisesRegex(ValueError,'QUIESCENCE'):
                r.run(cwd=t,prompt='',schema={'title':'author'},directory=Path(t)/'receipt',job_name='job',stop=threading.Event(),allowed_paths=['docs/a.md'])
        self.assertEqual(r._slots._value,0);self.assertEqual(r._quarantined_job,'job')

    def test_quarantine_clears_only_after_real_stop_receipt(self):
        r=self.runner();r._slots.acquire();r._quarantined_job='job'
        r._exchange=Mock(return_value={'state':'STOPPED','quiescent':True})
        r.quiesce('job')
        self.assertEqual(r._slots._value,1);self.assertIsNone(r._quarantined_job)

    def test_false_quiescence_never_clears_quarantine(self):
        r=self.runner();r._slots.acquire();r._quarantined_job='job'
        r._exchange=Mock(return_value={'state':'STOPPED','quiescent':False})
        with self.assertRaisesRegex(ValueError,'QUIESCENCE'):r.quiesce('job')
        self.assertEqual(r._slots._value,0);self.assertEqual(r._quarantined_job,'job')

    def test_restart_readiness_rejects_live_orphan_cgroup(self):
        r=self.runner();r._auth=Mock(return_value={});r._exchange=Mock(side_effect=ValueError('WSL_PREVIOUS_WORK_UNRETIRED'))
        with self.assertRaisesRegex(ValueError,'SANDBOX_UNAVAILABLE'):WslCodexRunner.ready(r)
        self.assertEqual(r.sandbox_diagnostic['state'],'BLOCKED')
        self.assertEqual(r.sandbox_diagnostic['reason'],'WSL_PREVIOUS_WORK_UNRETIRED')

    def test_expired_access_token_does_not_reach_cli(self):
        r=self.runner()
        with tempfile.TemporaryDirectory() as t:
            r.auth_path=Path(t)/'auth.json'
            payload=base64.urlsafe_b64encode(json.dumps({'exp':time.time()-1}).encode()).decode().rstrip('=')
            r.auth_path.write_text(json.dumps({'auth_mode':'chatgpt','tokens':{'access_token':'a.'+payload+'.z','refresh_token':'private-refresh'}}))
            with self.assertRaisesRegex(ValueError,'WSL_ACCESS_SNAPSHOT_EXPIRED'):r._auth()

    def test_refresh_authority_never_leaves_source(self):
        r=self.runner()
        with tempfile.TemporaryDirectory() as t:
            r.auth_path=Path(t)/'auth.json'
            payload=base64.urlsafe_b64encode(json.dumps({'exp':time.time()+10000}).encode()).decode().rstrip('=')
            original={'auth_mode':'chatgpt','tokens':{'access_token':'a.'+payload+'.z','refresh_token':'private-refresh'}}
            r.auth_path.write_text(json.dumps(original));before=r.auth_path.read_bytes()
            snapshot=r._auth();self.assertEqual(snapshot['tokens']['refresh_token'],'')
            self.assertEqual(r.auth_path.read_bytes(),before)

    def test_unsupported_login_fails_explicitly(self):
        r=self.runner()
        with tempfile.TemporaryDirectory() as t:
            r.auth_path=Path(t)/'auth.json';r.auth_path.write_text('{"auth_mode":"api","OPENAI_API_KEY":"fixture"}')
            with self.assertRaisesRegex(ValueError,'EXISTING_CHATGPT_LOGIN_REQUIRED'):r._auth()

    def test_no_allowlist_never_probes_or_runs(self):
        r=self.runner()
        with self.assertRaisesRegex(ValueError,'ALLOWLIST_REQUIRED'):
            r.run(cwd='.',prompt='',schema={},directory=Path('.'),job_name='job',stop=threading.Event())
        r.ready.assert_not_called()

    def test_stop_before_execution_never_probes(self):
        r=self.runner();stop=threading.Event();stop.set()
        with self.assertRaisesRegex(ValueError,'CLIENT_STOPPING'):
            r.run(cwd='.',prompt='',schema={},directory=Path('.'),job_name='job',stop=stop,allowed_paths=['docs/a.md'])
        r.ready.assert_not_called()

    def test_import_rejects_foreign_document(self):
        r=self.runner();r._auth=Mock(return_value={});r.quiesce=Mock()
        r._exchange=Mock(return_value={'state':'COMPLETED','quiescent':True,'job':r.job_id('job'),
            'output':{'status':'DONE'},'changes':{'docs/foreign.md':base64.b64encode(b'bad').decode()}})
        with tempfile.TemporaryDirectory() as t:
            root=Path(t)/'repo';root.mkdir()
            def git(*args):return subprocess.run(['git','-C',str(root),*args],capture_output=True,check=True)
            git('init');git('config','user.name','fixture');git('config','user.email','fixture@example.invalid')
            (root/'README.md').write_text('fixture');git('add','.');git('commit','-m','fixture')
            def local(argv,cwd,**kwargs):return subprocess.run(argv,capture_output=True,timeout=30)
            r._bounded_local=local
            with self.assertRaisesRegex(ValueError,'WSL_CHANGE_SET_INVALID'):
                r.run(cwd=root,prompt='fixture',schema={'title':'author','type':'object'},directory=Path(t)/'receipt',job_name='job',stop=threading.Event(),allowed_paths=['docs/allowed.md'])
            self.assertFalse((root/'docs/foreign.md').exists())
            self.assertEqual(git('status','--porcelain').stdout,b'')

    def test_small_head_cannot_hide_large_historical_blob(self):
        r=self.runner();r.quiesce=Mock();r._exchange=Mock()
        with tempfile.TemporaryDirectory() as t:
            root=Path(t)/'repo';root.mkdir()
            def git(*args):return subprocess.run(['git','-C',str(root),*args],capture_output=True,check=True)
            git('init');git('config','user.name','fixture');git('config','user.email','fixture@example.invalid')
            (root/'README.md').write_text('small current tree')
            (root/'historical.bin').write_bytes(b'0'*(2*1024*1024))
            git('add','.');git('commit','-m','historical compressible large object')
            git('rm','historical.bin');git('commit','-m','small HEAD')
            bundle=Path(t)/'compressed.bundle';git('bundle','create',str(bundle),'HEAD')
            self.assertLess(bundle.stat().st_size,32768)
            r._bounded_local=lambda argv,cwd,**kwargs:subprocess.run(argv,capture_output=True,timeout=30)
            with self.assertRaisesRegex(ValueError,'WSL_OBJECT_EXPANSION_LIMIT'):
                r.run(cwd=root,prompt='fixture',schema={'title':'author'},directory=Path(t)/'receipt',job_name='job',stop=threading.Event(),allowed_paths=['docs/allowed.md'])
            r._exchange.assert_not_called()
            self.assertEqual(git('status','--porcelain').stdout,b'')

    def test_truncated_object_headers_cannot_pass_expansion_fence(self):
        r=self.runner();r.quiesce=Mock();r._exchange=Mock()
        def local(argv,cwd,**kwargs):
            data=b'a'*40+b'\n' if 'rev-parse' in argv else (b'blob 1\n'*20000)[:131073]
            return subprocess.CompletedProcess(argv,0,data,b'')
        r._bounded_local=local
        with tempfile.TemporaryDirectory() as t:
            with self.assertRaisesRegex(ValueError,'WSL_GIT_EXPORT_OUTPUT_LIMIT'):
                r.run(cwd=t,prompt='fixture',schema={'title':'author'},directory=Path(t)/'receipt',job_name='job',stop=threading.Event(),allowed_paths=['docs/allowed.md'])
        r._exchange.assert_not_called()


if __name__=='__main__':unittest.main()
