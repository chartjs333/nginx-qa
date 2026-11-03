import asyncio
from contextlib import asynccontextmanager
import hashlib
import json
import os
from pathlib import Path
import platform
import socket
import sys
import tempfile
import threading
import unittest
import uuid
from unittest.mock import patch

from fastapi import FastAPI
from nginx_qa import launcher_host as host
from nginx_qa.launcher_gateway import create_gateway


class LauncherHostGuards(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory();self.addCleanup(self.temporary.cleanup)
        self.base=Path(self.temporary.name)
        self.root=self.base/'source';self.state=self.base/'state'
        self.root.mkdir();self.state.mkdir()
        for path in ('v/Scripts','r','p','m','legacy/runtime_state'):(self.state/path).mkdir(parents=True,exist_ok=True)
        self.python=self.state/'v/Scripts/python.exe';self.python.write_bytes(b'fixture interpreter path')
        base_python=Path(getattr(sys,'_base_executable',sys.executable))
        self.env={'NGINX_QA_SERVICE_ROOT':str(self.root),'NGINX_QA_STAGING_STATE_BASE':str(self.state),
            'NGINX_QA_PROTECTED_ROOTS':json.dumps([str(self.base/'protected')]),'NGINX_QA_STAGING_VENV_ROOT':str(self.state/'v'),
            'NGINX_QA_STAGING_BASE_PYTHON':str(base_python),'NGINX_QA_RUNTIME_ROOT':str(self.state/'r'),
            'NGINX_QA_PROMPT_ROOT':str(self.state/'p'),'NGINX_QA_MANAGED_ROOT':str(self.state/'m'),
            'NGINX_QA_INSTANCE_ID':'test-gateway','NGINX_QA_CHILD_PORT_RANGE':'19000-19049',
            'NGINX_QA_HTTP_HOST':'127.0.0.1','NGINX_QA_GATEWAY_HOST':'127.0.0.1','NGINX_QA_HTTP_PORT':'18036','NGINX_QA_GATEWAY_PORT':'18037',
            'NGINX_QA_LAUNCHER_ORIGIN_STATE':str(self.state/'legacy/runtime_state/launcher-origin.json')}
        self.owner={'instance_id':'test-gateway','service_root':str(self.root),'state_base':str(self.state),
            'venv_root':str(self.state/'v'),'runtime_root':str(self.state/'r'),'prompt_root':str(self.state/'p'),'managed_root':str(self.state/'m'),
            'base_python':str(base_python),'base_python_prefix':sys.base_prefix,'base_python_version':platform.python_version(),
            'base_python_sha256':hashlib.sha256(base_python.read_bytes()).hexdigest(),
            'http_host':'127.0.0.1','gateway_host':'127.0.0.1','http_port':18036,'gateway_port':18037,
            'child_port_range':'19000-19049','gateway_origin_state':self.env['NGINX_QA_LAUNCHER_ORIGIN_STATE'],
            'protected_roots':[str(self.base/'protected')],'approved_commit':'a'*40,'branch':'agent/fixture',
            'origin':'https://github.com/chartjs333/nginx-qa.git'}
        self.marker=self.state/'.nginx-qa-staging-owner.json';self.write_marker()

    def write_marker(self):
        self.marker.write_text(json.dumps({'schema_version':1,'initialization_id':str(uuid.uuid4()),'ownership':self.owner}))

    def validate(self):
        with patch.object(sys,'prefix',str(self.state/'v')),patch.object(sys,'executable',str(self.python)):
            return host.validate_gateway_configuration(self.env,code_root=self.root,verify_git=False)

    def test_exact_owned_paths_ports_and_interpreter_pass_without_writes(self):
        before=self.marker.read_bytes()
        result=self.validate()
        self.assertEqual((18036,18037),(result['private_port'],result['public_port']))
        self.assertEqual(before,self.marker.read_bytes())

    def test_drift_wildcard_overlap_tampered_owner_and_interpreter_fail(self):
        original=dict(self.env)
        for key,value in [('NGINX_QA_GATEWAY_PORT','18036'),('NGINX_QA_GATEWAY_PORT','19001'),('NGINX_QA_GATEWAY_HOST','0.0.0.0'),
                          ('NGINX_QA_LAUNCHER_ORIGIN_STATE',str(self.base/'foreign.json')),('NGINX_QA_INSTANCE_ID','other')]:
            with self.subTest(key=key,value=value):
                self.env={**original,key:value}
                with self.assertRaises(ValueError):self.validate()
        self.env=original
        self.owner['base_python_sha256']='0'*64;self.write_marker()
        with self.assertRaises(ValueError):self.validate()

    def test_occupied_public_port_closes_already_reserved_private_socket(self):
        public=socket.socket();public.bind(('127.0.0.1',0));public.listen();self.addCleanup(public.close)
        probe=socket.socket();probe.bind(('127.0.0.1',0));private_port=probe.getsockname()[1];probe.close()
        with self.assertRaises(OSError):host.reserve_listeners(private_port,public.getsockname()[1])
        released=socket.socket();self.addCleanup(released.close);released.bind(('127.0.0.1',private_port))


class LauncherHostLifecycle(unittest.IsolatedAsyncioTestCase):
    async def scenario(self,*,public_failure=False,private_failure=False,partial_failure=False,held_work=False,cancel_drain=False):
        events=[]
        @asynccontextmanager
        async def lifespan(app):
            events.append('engine-start')
            try:yield
            finally:events.append('engine-stop')
        private=FastAPI(lifespan=lifespan)
        public=create_gateway(object())
        stop=asyncio.Event();release=threading.Event();ready=asyncio.Event()
        servers=[]
        class Server:
            def __init__(self,config):
                self.public=config.app is public;self.started=False;self.should_exit=False;servers.append(self)
            async def serve(self,sockets):
                if self.public and partial_failure:raise RuntimeError('fixture startup failure')
                self.started=True
                if len(servers)==2 and all(item.started for item in servers):ready.set()
                if self.public and public_failure:
                    await asyncio.sleep(0.04);raise RuntimeError('fixture public listener failure')
                if not self.public and private_failure:
                    await asyncio.sleep(0.04);raise RuntimeError('fixture private listener failure')
                while not self.should_exit:await asyncio.sleep(0.005)
        work=None
        if held_work:
            work=asyncio.create_task(asyncio.to_thread(release.wait));public.state.launcher_work.add(work)
            work.add_done_callback(public.state.launcher_work.discard)
        sockets=host.reserve_listeners(0,0)
        task=asyncio.create_task(host.serve_pair(private,public,sockets,stop,server_factory=Server))
        try:
            if partial_failure or private_failure:
                with self.assertRaises(RuntimeError):await asyncio.wait_for(task,2)
            else:
                await asyncio.wait_for(ready.wait(),2);await asyncio.sleep(0.07)
                self.assertFalse(task.done(),'public failure must not stop the engine')
                self.assertEqual(['engine-start'],events)
                stop.set()
                if held_work:
                    await asyncio.sleep(0.04);self.assertFalse(task.done());self.assertEqual(['engine-start'],events)
                if cancel_drain:
                    task.cancel();await asyncio.sleep(.04)
                    self.assertFalse(task.done());self.assertFalse(work.cancelled())
                    self.assertEqual(['engine-start'],events);self.assertEqual(1,len(public.state.launcher_work))
                    task.cancel();await asyncio.sleep(.04);self.assertFalse(task.done())
                release.set()
                if cancel_drain:
                    with self.assertRaises(asyncio.CancelledError):await asyncio.wait_for(task,2)
                else:await asyncio.wait_for(task,2)
            self.assertEqual(['engine-start','engine-stop'],events)
            self.assertTrue(all(sock.fileno()==-1 for sock in sockets))
        finally:
            stop.set();release.set();await asyncio.gather(task,return_exceptions=True)
            if work:await work

    async def test_one_lifespan_and_real_work_drained_before_teardown(self):await self.scenario(held_work=True)
    async def test_public_failure_does_not_stop_engine(self):await self.scenario(public_failure=True)
    async def test_partial_startup_closes_both_listeners_and_one_lifespan(self):await self.scenario(partial_failure=True)
    async def test_cancellation_during_drain_waits_actual_worker_and_one_lifespan(self):await self.scenario(held_work=True,cancel_drain=True)
    async def test_private_failure_is_reported_after_cleanup(self):await self.scenario(private_failure=True)
