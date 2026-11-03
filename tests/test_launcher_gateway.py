from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import time
import unittest
from http.cookies import SimpleCookie
from urllib.parse import urlencode
from unittest.mock import patch

from fastapi import HTTPException

from nginx_qa import launcher_auth as auth
from nginx_qa.launcher_gateway import create_gateway
from nginx_qa.launcher_transport_store import LauncherTransport
from nginx_qa.telegram_launcher import POLICY_ENV
from tests.test_telegram_launcher_intake import LauncherIntakeFixture
from tests import test_telegram_launcher_launch_modes as modes


TOKEN='456789:fixture-telegram-control-secret-123456'
ORIGIN='https://launcher.example.test'


def signed_data(*,user=123456,issued=None,query='fixture-query',extra=None,token=TOKEN):
    values={'auth_date':str(int(time.time()) if issued is None else issued),'query_id':query,
            'user':json.dumps({'id':user,'first_name':'Тест'},ensure_ascii=False,separators=(',',':'))}
    values.update(extra or {})
    canonical='\n'.join(key+'='+values[key] for key in sorted(values))
    secret=hmac.new(b'WebAppData',token.encode(),hashlib.sha256).digest()
    values['hash']=hmac.new(secret,canonical.encode(),hashlib.sha256).hexdigest()
    return urlencode(values)


class ASGIClient:
    def __init__(self,app):
        self.app=app
        self.cookies={}

    async def request(self,path,body=None,*,method=None,headers=None):
        raw=json.dumps(body).encode() if body is not None else b''
        selected={'host':'launcher.example.test','origin':ORIGIN}
        if body is not None:
            selected['content-type']='application/json'
        if self.cookies:
            selected['cookie']='; '.join(key+'='+value for key,value in self.cookies.items())
        selected.update(headers or {})
        received=False
        finished=asyncio.Event()
        messages=[]
        async def receive():
            nonlocal received
            if not received:
                received=True
                return {'type':'http.request','body':raw,'more_body':False}
            await finished.wait()
            return {'type':'http.disconnect'}
        async def send(message):
            messages.append(message)
            if message['type']=='http.response.body' and not message.get('more_body'):
                finished.set()
        scope={'type':'http','asgi':{'version':'3.0'},'http_version':'1.1','method':method or ('POST' if body is not None else 'GET'),
            'scheme':'https','path':path,'raw_path':path.encode(),'query_string':b'','root_path':'',
            'headers':[(key.encode(),value.encode()) for key,value in selected.items()],
            'client':('127.0.0.1',15000),'server':('launcher.example.test',443)}
        await asyncio.wait_for(self.app(scope,receive,send),120)
        start=next(message for message in messages if message['type']=='http.response.start')
        for key,value in start['headers']:
            if key.lower()==b'set-cookie':
                # This protocol fixture tests flags, not browser cookie policy.
                parsed=SimpleCookie();parsed.load(value.decode().replace('; Partitioned',''))
                for name,item in parsed.items():
                    if item['max-age']=='0': self.cookies.pop(name,None)
                    else:self.cookies[name]=item.value
        content=b''.join(message.get('body',b'') for message in messages if message['type']=='http.response.body')
        try:data=json.loads(content)
        except ValueError:data=content.decode()
        return start['status'],data,start['headers']


class TelegramHMACUnitTests(unittest.TestCase):
    def test_signature_field_is_authenticated_and_encoding_order_cannot_change_replay_key(self):
        raw=signed_data(issued=1000,extra={'signature':'signed-third-party-field'})
        verified=auth.verify_init_data(raw,TOKEN,now=1001)
        reordered='&'.join(reversed(raw.split('&'))).replace('+','%20')
        self.assertEqual(verified,auth.verify_init_data(reordered,TOKEN,now=1001))
        self.assertEqual('123456',verified['user_id'])
        with self.assertRaises(HTTPException):
            auth.verify_init_data(raw.replace('signed-third-party-field','changed'),TOKEN,now=1001)

    def test_rejects_duplicate_tampered_expired_future_wrong_bot_or_unsupported_context(self):
        good=signed_data(issued=1000)
        invalid=[good+'&user=duplicate',good.replace('auth_date=1000','auth_date=1001'),good+'&bad=%ZZ',
            signed_data(issued=699),signed_data(issued=1031),signed_data(issued=1000,extra={'chat_type':'group'}),
            signed_data(issued=1000,extra={'chat':'{}'}),signed_data(issued=1000,extra={'query_id':''})]
        for raw in invalid:
            with self.subTest(raw_length=len(raw)),self.assertRaises(HTTPException) as raised:
                auth.verify_init_data(raw,TOKEN,now=1000)
            self.assertEqual('TELEGRAM_AUTH_INVALID',raised.exception.detail['error'])
        with self.assertRaises(HTTPException):auth.verify_init_data(good,TOKEN+'other',now=1000)


class LauncherGatewayTests(LauncherIntakeFixture):
    allow_start=modes.LauncherLaunchModeTests.allow_start

    def setUp(self):
        super().setUp()
        self.guard_env=patch.dict(os.environ,{'NGINX_QA_LAUNCHER_PUBLIC_ORIGIN':ORIGIN,
            'NGINX_QA_LAUNCHER_GATEWAY_ENABLED':'true',
            'NGINX_QA_LAUNCHER_CONTROL_BOT_TOKEN':TOKEN,'NGINX_QA_LAUNCHER_CONTROL_WEBHOOK_SECRET':'test-webhook'})
        self.guard_env.start();self.addCleanup(self.guard_env.stop)
        self.client=modes.FakeExistingClient(self)
        self.allow_start(self.client)
        self.result=self.ingest()
        record=self.operations.get(self.result['operation_id'],self.principal)
        self.handle=LauncherTransport(self.operations).handle(record,'web')
        self.app=create_gateway(self.host)
        self.browser=ASGIClient(self.app)
        self.instance_path='/launcher/instances/'+self.handle

    def call(self,*args,**kwargs):
        return asyncio.run(self.browser.request(*args,**kwargs))

    def exchange_body(self,raw=None):
        raw=raw or signed_data()
        self.last_init_data=raw
        code,challenge,_=self.call('/launcher/auth/challenge',{'handle':self.handle})
        self.assertEqual(200,code)
        code,token,_=self.call('/launcher/auth/telegram',{'handle':self.handle,'challenge':challenge['challenge'],'init_data':raw})
        self.assertEqual(200,code)
        return {'handle':self.handle,'challenge':challenge['challenge'],'init_data':raw,'exchange_token':token['exchange_token']}

    def login(self):
        code,result,headers=self.call('/launcher/auth/exchange',self.exchange_body())
        self.assertEqual(200,code)
        return result['csrf_token'],headers

    def test_scanner_deny_default_login_reload_csrf_real_start_stop_logout(self):
        code,html,headers=self.call('/launcher/'+self.handle)
        self.assertEqual(200,code)
        self.assertFalse(any(key==b'set-cookie' for key,_ in headers))
        with self.imp.store._transaction() as connection:
            self.assertEqual(0,connection.execute('SELECT COUNT(*) FROM launcher_auth_challenges').fetchone()[0])
        for path in ('/','/health','/docs','/openapi.json','/api/v1/projects/9002','/files/secret','/launcher/../health','/launcher/assets/../secret','/launcher/instances/'+self.handle+'/events'):
            self.assertIn(self.call(path)[0],{404,405},path)
        self.assertEqual(401,self.call(self.instance_path+'/status',headers={'x-user-id':'123456','x-forwarded-for':'127.0.0.1'})[0])
        csrf,headers=self.login()
        cookies=';'.join(value.decode() for key,value in headers if key==b'set-cookie')
        for attribute in ('Secure','HttpOnly','SameSite=none','Path=/','Partitioned'):
            self.assertIn(attribute,cookies)
        code,status,headers=self.call(self.instance_path+'/status')
        self.assertEqual(200,code);self.assertEqual('Waiting',status['status'])
        self.assertEqual(b'no-store',dict(headers)[b'cache-control'])
        self.assert_no_activation()
        self.assertEqual(403,self.call(self.instance_path+'/start',{})[0])
        self.assertEqual(403,self.call(self.instance_path+'/start',{},headers={'origin':'https://evil.test','x-csrf-token':csrf})[0])
        self.assertEqual(421,self.call(self.instance_path+'/status',headers={'host':'evil.test'})[0])
        code,result,_=self.call(self.instance_path+'/start',{},headers={'x-csrf-token':csrf})
        self.assertEqual(200,code);self.assertEqual('Running',result['status']);self.assertEqual(1,len(self.client.starts))
        code,result,_=self.call(self.instance_path+'/stop',{},headers={'x-csrf-token':csrf})
        self.assertEqual(200,code);self.assertEqual('Stopped',result['status'])
        self.assertEqual(200,self.call(self.instance_path+'/logout',{},headers={'x-csrf-token':csrf})[0])
        self.assertEqual(401,self.call(self.instance_path+'/status')[0])
        challenge=self.call('/launcher/auth/challenge',{'handle':self.handle})[1]
        self.assertEqual(409,self.call('/launcher/auth/telegram',{'handle':self.handle,'challenge':challenge['challenge'],'init_data':self.last_init_data})[0])

    def test_exchange_is_hash_only_single_use_and_browser_user_instance_bound(self):
        body=self.exchange_body()
        attacker=ASGIClient(self.app)
        self.assertEqual(401,asyncio.run(attacker.request('/launcher/auth/exchange',body))[0])
        wrong={**body,'handle':'tml:'+self.project_id+':'+('x'*24)}
        self.assertEqual(401,self.call('/launcher/auth/exchange',wrong)[0])
        self.policy['grants'].append({**self.policy['grants'][0],'user_id':'999','chat_id':'999'})
        os.environ[POLICY_ENV]=json.dumps(self.policy)
        self.assertEqual(403,self.call('/launcher/auth/exchange',{**body,'init_data':signed_data(user=999)})[0])
        async def race():
            return await asyncio.gather(self.browser.request('/launcher/auth/exchange',body),self.browser.request('/launcher/auth/exchange',body))
        results=asyncio.run(race())
        self.assertEqual([200,401],sorted(result[0] for result in results))
        with self.imp.store._transaction() as connection:
            state='\n'.join(str(tuple(row)) for table in ('launcher_auth_challenges','launcher_auth_exchanges','launcher_auth_launches','launcher_auth_sessions') for row in connection.execute('SELECT * FROM '+table))
            self.assertEqual(1,connection.execute('SELECT COUNT(*) FROM launcher_auth_sessions').fetchone()[0])
        for secret in (body['challenge'],body['exchange_token'],body['init_data'],TOKEN,*self.browser.cookies.values()):
            self.assertNotIn(secret,state)

    def test_acl_revocation_expiry_scope_and_replay_encoding(self):
        body=self.exchange_body()
        self.assertEqual(200,self.call('/launcher/auth/exchange',body)[0])
        session=next(value for key,value in self.browser.cookies.items() if '-session-' in key)
        with self.assertRaises(HTTPException):
            auth.LauncherAuth(self.operations).session('tml:'+self.project_id+':'+('x'*24),session,ORIGIN,self.policy['projects'][self.project_id])
        self.policy['grants'][0]['actions'].remove('read');os.environ[POLICY_ENV]=json.dumps(self.policy)
        self.assertEqual(403,self.call(self.instance_path+'/status')[0])
        self.policy['grants'][0]['actions'].append('read');os.environ[POLICY_ENV]=json.dumps(self.policy)
        with self.imp.store._transaction() as connection:
            connection.execute('UPDATE launcher_auth_sessions SET expires=0')
        self.assertEqual(401,self.call(self.instance_path+'/status')[0])
        challenge=self.call('/launcher/auth/challenge',{'handle':self.handle})[1]
        reordered='&'.join(reversed(body['init_data'].split('&')))
        self.assertEqual(409,self.call('/launcher/auth/telegram',{'handle':self.handle,'challenge':challenge['challenge'],'init_data':reordered})[0])
        self.assert_no_activation()

    def test_exchange_expiry_origin_rotation_and_ttl_limit(self):
        body=self.exchange_body()
        with self.imp.store._transaction() as connection:
            connection.execute('UPDATE launcher_auth_exchanges SET expires=0')
        self.assertEqual(401,self.call('/launcher/auth/exchange',body)[0])
        with patch.dict(os.environ,{'NGINX_QA_LAUNCHER_EXCHANGE_TTL':'301'}):
            fresh=signed_data(query='fresh-query')
            challenge=self.call('/launcher/auth/challenge',{'handle':self.handle})[1]
            self.assertEqual(503,self.call('/launcher/auth/telegram',{'handle':self.handle,'challenge':challenge['challenge'],'init_data':fresh})[0])
        body=self.exchange_body(signed_data(query='third-query'))
        with patch.dict(os.environ,{'NGINX_QA_LAUNCHER_PUBLIC_ORIGIN':'https://replacement.example.test'}):
            self.assertEqual(403,self.call('/launcher/auth/exchange',body,headers={'host':'replacement.example.test','origin':'https://replacement.example.test'})[0])
        self.assert_no_activation()
