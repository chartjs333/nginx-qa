import asyncio
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException
from nginx_qa import launcher_auth as auth
from nginx_qa import launcher_gateway as gateway
from nginx_qa.managed_import import ManagedImportStore
from nginx_qa.telegram_launcher import Principal
from tests.test_launcher_gateway import ASGIClient, ORIGIN


class AuthClockAndQuotaTests(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory();self.addCleanup(self.temporary.cleanup)
        self.store=ManagedImportStore(Path(self.temporary.name)/'state.sqlite')
        self.record={'operation_id':'launch-fixture','project_id':'9002','user_id':'123456','chat_id':'123456','instance_id':'fixture-instance'}
        self.handle='tml:9002:'+'x'*24
        self.principal=Principal('123456','123456','9002',frozenset({'read'}))
        self.auth=auth.LauncherAuth(SimpleNamespace(store=self.store,get=lambda *args:dict(self.record)))
        self.permission=patch.object(auth,'current_principal',side_effect=lambda p,*args:p)
        self.permission.start();self.addCleanup(self.permission.stop)
        self.verified={'user_id':'123456','digest':'verified-fixture-digest','valid_until':time.time()+300}
        self.challenge=self.auth.challenge(self.record,self.handle,ORIGIN)

    def token(self):
        return self.auth.authenticate(self.handle,self.challenge['challenge'],self.challenge['browser'],self.verified,self.principal,ORIGIN,{})['exchange_token']

    def exchange(self,token):
        return self.auth.exchange(self.handle,self.challenge['challenge'],self.challenge['browser'],token,self.verified,self.principal,ORIGIN,{})

    def blocked_then_expired(self,operation):
        connection=self.store._connect();connection.execute('BEGIN IMMEDIATE')
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                future=pool.submit(operation)
                time.sleep(0.4)
                connection.commit()
                with self.assertRaises(HTTPException):future.result(timeout=5)
        finally:
            connection.close()

    def test_session_and_exchange_cannot_outlive_expiry_while_waiting_for_database(self):
        token=self.token();session=self.exchange(token)['session']
        with self.store._transaction() as connection:
            connection.execute('UPDATE launcher_auth_sessions SET expires=?',(time.time()+0.15,))
        self.blocked_then_expired(lambda:self.auth.session(self.handle,session,ORIGIN,{}))
        self.verified['digest']='second-fixture-digest'
        self.challenge=self.auth.challenge(self.record,self.handle,ORIGIN)
        token=self.token()
        with self.store._transaction() as connection:
            connection.execute('UPDATE launcher_auth_exchanges SET expires=?',(time.time()+0.15,))
        self.blocked_then_expired(lambda:self.exchange(token))

    def test_signed_identity_freshness_is_rechecked_after_database_wait(self):
        self.verified['valid_until']=time.time()+0.15
        self.blocked_then_expired(self.token)
        with self.store._transaction() as connection:
            self.assertEqual(0,connection.execute('SELECT COUNT(*) FROM launcher_auth_exchanges').fetchone()[0])

    def test_failed_browser_attempts_consume_durable_per_user_quota(self):
        codes=[]
        for _ in range(25):
            with self.assertRaises(HTTPException) as raised:
                self.auth.authenticate(self.handle,self.challenge['challenge'],'wrong-browser',self.verified,self.principal,ORIGIN,{},now=1000)
            codes.append(raised.exception.status_code)
        self.assertEqual([401]*20+[429]*5,codes)
        with self.store._transaction() as connection:
            self.assertEqual(20,connection.execute("SELECT count FROM launcher_auth_limits WHERE key='auth:123456:9002'").fetchone()[0])


class GatewayBoundaryAndDrainTests(unittest.IsolatedAsyncioTestCase):
    async def test_telegram_web_navigation_can_load_only_public_static_content(self):
        host=SimpleNamespace(read_git_config=AsyncMock())
        app=gateway.create_gateway(host);client=ASGIClient(app)
        with patch.dict('os.environ',{'NGINX_QA_LAUNCHER_GATEWAY_ENABLED':'true','NGINX_QA_TELEGRAM_LAUNCHER_ENABLED':'true','NGINX_QA_LAUNCHER_PUBLIC_ORIGIN':ORIGIN}):
            for path in ('/launcher/tml:9002:'+'x'*24,'/launcher/assets/app.js','/launcher/assets/app.css'):
                code,_,headers=await client.request(path,headers={'origin':'https://web.telegram.org','sec-fetch-site':'cross-site'})
                self.assertEqual(200,code)
                self.assertFalse(any(key==b'set-cookie' for key,_ in headers))
            self.assertEqual(403,(await client.request('/launcher/auth/challenge',{'handle':'tml:9002:'+'x'*24},headers={'origin':'https://web.telegram.org','sec-fetch-site':'cross-site'}))[0])
            self.assertEqual(421,(await client.request('/launcher/assets/app.js',headers={'host':'foreign.test'}))[0])
        host.read_git_config.assert_not_called()

    async def test_cancelled_request_leaves_actual_thread_tracked_until_shutdown_drain(self):
        started,release=threading.Event(),threading.Event()
        host=SimpleNamespace(read_git_config=AsyncMock(return_value={}))
        app=gateway.create_gateway(host);client=ASGIClient(app)
        def held(*args):
            started.set();release.wait(5)
            return None
        with patch.dict('os.environ',{'NGINX_QA_LAUNCHER_GATEWAY_ENABLED':'true','NGINX_QA_TELEGRAM_LAUNCHER_ENABLED':'true','NGINX_QA_LAUNCHER_PUBLIC_ORIGIN':ORIGIN}), \
             patch.object(gateway,'load_policy',return_value={'projects':{'9002':{}}}), \
             patch.object(gateway,'open_operations',side_effect=held):
            request=asyncio.create_task(client.request('/launcher/auth/challenge',{'handle':'tml:9002:'+'x'*24}))
            try:
                self.assertTrue(await asyncio.to_thread(started.wait,2))
                request.cancel();await asyncio.gather(request,return_exceptions=True)
                self.assertTrue(app.state.launcher_work)
                draining=asyncio.create_task(gateway.drain_gateway(app))
                await asyncio.sleep(0.03);self.assertFalse(draining.done())
                release.set();await asyncio.wait_for(draining,2)
                self.assertFalse(app.state.launcher_work)
            finally:
                release.set();await asyncio.gather(request,return_exceptions=True)
                await gateway.drain_gateway(app)

    async def test_websocket_routes_are_denied_without_legacy_or_identity_fallthrough(self):
        host=SimpleNamespace(read_git_config=AsyncMock())
        app=gateway.create_gateway(host);sent=[]
        async def receive():return {'type':'websocket.connect'}
        async def send(message):sent.append(message)
        await app({'type':'websocket','asgi':{'version':'3.0'},'scheme':'wss','path':'/launcher/instances/tml:9002:'+'x'*24+'/ws',
            'query_string':b'','headers':[(b'host',b'launcher.example.test'),(b'x-user-id',b'123456')],
            'client':('127.0.0.1',15000),'server':('launcher.example.test',443),'subprotocols':[]},receive,send)
        self.assertEqual('websocket.close',sent[-1]['type']);host.read_git_config.assert_not_called()
