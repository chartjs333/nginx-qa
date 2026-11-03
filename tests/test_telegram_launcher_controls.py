from __future__ import annotations

import asyncio
import copy
import json
import os
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from fastapi import HTTPException

from nginx_qa import telegram_launcher as launcher
from nginx_qa import telegram_launcher_controls as controls
from nginx_qa.launcher_transport_store import LauncherTransport
from nginx_qa.telegram_launcher_store import LauncherOperations
from tests.test_telegram_launcher_intake import LauncherIntakeFixture
from tests import test_telegram_launcher_launch_modes as modes
from tests.test_telegram_launcher_launch_modes import FakeExistingClient
from tests.test_telegram_launcher_prompt import message, request


class LauncherControlsTests(LauncherIntakeFixture):
    allow_start=modes.LauncherLaunchModeTests.allow_start
    launch=modes.LauncherLaunchModeTests.launch
    stop=modes.LauncherLaunchModeTests.stop

    def setUp(self):
        super().setUp()
        self.transport=LauncherTransport(self.operations)
        self.host.telegram_api_json.return_value={'ok':True,'result':{'message_id':15,'chat':{'id':int(self.principal.chat_id)}}}
        self.env2=patch.dict(os.environ,{'NGINX_QA_LAUNCHER_PUBLIC_ORIGIN':'https://launcher.example.test'})
        self.env2.start()
        self.addCleanup(self.env2.stop)

    def callback(self,handle,*,sender=None,chat=None,**query):
        return {'update_id':101,'callback_query':{'id':'callback-101','data':handle,
            'from':sender or message('')['from'],
            'message':{'date':1,'message_id':15,'from':{'id':456789,'is_bot':True},'chat':chat or message('')['chat']},**query}}

    def dispatch(self,update):
        return asyncio.run(launcher.maybe_handle_prompt(request(update),self.host))

    def record(self,result):
        return self.operations.get(result['operation_id'],self.principal)

    def notices(self):
        with self.imp.store._transaction() as connection:
            return [dict(row) for row in connection.execute('SELECT * FROM launcher_notices ORDER BY id')]

    def test_three_buttons_status_and_double_start_use_actual_engine_once(self):
        client=FakeExistingClient(self)
        self.allow_start(client)
        result=self.ingest()
        markup=controls.keyboard(self.transport,self.record(result))
        buttons=[row[0] for row in markup['inline_keyboard']]
        self.assertEqual(['Запустить спринт','Открыть Web UI','Статус спринта'],[button['text'] for button in buttons])
        self.assertTrue(buttons[1]['web_app']['url'].startswith('https://launcher.example.test/launcher/tml:'))
        for button in (buttons[0],buttons[2]):
            self.assertLessEqual(len(button['callback_data'].encode('utf-8')),64)
        self.assertEqual('Waiting',self.dispatch(self.callback(buttons[2]['callback_data']))['status'])
        self.assert_no_activation()
        callback=self.callback(buttons[0]['callback_data'])
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(lambda _:self.dispatch(callback),range(2)))
        self.assertEqual(['Running','Running'],[item['status'] for item in results])
        self.assertEqual(1,len(client.starts))
        state=self.imp.store.active_runtime_state(self.project_id)
        self.assertEqual(1,len(state['assignments']))
        sent=[call.args[1] for call in self.host.telegram_api_json.call_args_list]
        self.assertEqual('answerCallbackQuery',sent[0])

    def test_owner_current_acl_private_chat_expiry_and_wrong_action(self):
        self.allow_start(FakeExistingClient(self))
        result=self.ingest()
        handle=self.transport.handle(self.record(result),'start')
        for update in (
            self.callback(handle,sender={'id':999,'is_bot':False}),
            self.callback(handle,chat={'id':-1,'type':'group'}),
            self.callback(handle,inline_message_id='unsupported'),
            self.callback(handle,business_connection_id='unsupported'),
        ):
            with self.assertRaises(HTTPException):
                self.dispatch(update)
        self.policy['grants'][0]['actions'].remove('start')
        os.environ[launcher.POLICY_ENV]=json.dumps(self.policy)
        with self.assertRaises(HTTPException) as raised:
            self.dispatch(self.callback(handle))
        self.assertEqual(403,raised.exception.status_code)
        self.assert_no_activation()
        with self.assertRaises(HTTPException):
            self.transport.resolve(handle,self.principal,action='web')
        with self.assertRaises(HTTPException) as raised:
            self.transport.resolve(handle,self.principal,now=time.time()+90000)
        self.assertEqual(410,raised.exception.status_code)
        old=handle
        with self.imp.store._transaction() as connection:
            connection.execute('UPDATE launcher_handles SET expires=0')
        self.dispatch({'update_id':102,'message':{**message('/sprint_status '+self.project_id),'message_id':102}})
        new=self.transport.handle(self.record(result),'start')
        self.assertNotEqual(old,new)
        with self.assertRaises(HTTPException):
            self.transport.resolve(old,self.principal)

    def test_outbox_atomic_dedup_leases_lost_ack_retry_and_retirement(self):
        self.allow_start()
        result=self.ingest()
        self.ingest()
        self.assertEqual(['Created','Waiting'],[json.loads(row['snapshot'])['lifecycle'] for row in self.notices()])
        count=len(self.notices())
        with self.assertRaises(RuntimeError):
            with self.imp.store._transaction() as connection:
                row=connection.execute('SELECT record_json FROM telegram_launcher_operations').fetchone()
                damaged=json.loads(row[0]); damaged['lifecycle']='Failed'
                connection.execute('UPDATE telegram_launcher_operations SET record_json=?',(json.dumps(damaged),))
                raise RuntimeError('transaction rollback fixture')
        self.assertEqual(count,len(self.notices()))
        first,lease=self.transport.claim(self.project_id,now=100)
        second,other=self.transport.claim(self.project_id,now=100)
        self.assertIsNone(self.transport.claim(self.project_id,now=101))
        recovered,newlease=LauncherTransport(LauncherOperations(self.importer())).claim(self.project_id,now=221)
        self.assertEqual(first['id'],recovered['id'])
        self.assertFalse(self.transport.finish(first,lease,delivered=True,now=222))
        self.assertTrue(self.transport.finish(recovered,newlease,delivered=True,message_id=15,now=222))
        self.host.telegram_api_json.side_effect=TimeoutError('secret=do-not-store')
        controls.deliver_one(self.operations,self.project_id,self.policy['projects'][self.project_id],self.host,now=222)
        row=self.notices()[1]
        self.assertEqual('pending',row['status'])
        self.assertEqual('DELIVERY_FAILED',row['error_code'])
        self.assertNotIn('do-not-store',json.dumps(self.notices()))
        self.host.telegram_api_json.side_effect=None
        controls.deliver_one(self.operations,self.project_id,self.policy['projects'][self.project_id],self.host,now=1000)
        self.assertEqual(['delivered','delivered'],[row['status'] for row in self.notices()])
        self.operations.cancel_prepared(result['operation_id'],self.principal)
        asyncio.run(controls.reconcile_notifications(self.host))
        self.assertEqual('Stopped',json.loads(self.notices()[-1]['snapshot'])['lifecycle'])
        self.assertEqual('delivered',self.notices()[-1]['status'])

    def test_notification_delivery_failure_is_bounded_and_never_launches(self):
        self.ingest()
        self.host.telegram_api_json.side_effect=TimeoutError('provider failed')
        for now in range(1000,10000,1000):
            controls.deliver_one(self.operations,self.project_id,self.policy['projects'][self.project_id],self.host,now=now)
        # First intent reached five attempts; second continues independently.
        self.assertEqual('failed',self.notices()[0]['status'])
        self.assertEqual(5,self.notices()[0]['attempts'])
        self.assert_no_activation()

    def test_auto_completion_status_and_terminal_notification_need_only_current_read(self):
        client=FakeExistingClient(self)
        self.allow_start(client)
        self.envelope['autonomous_mode']=True
        result=self.ingest()
        self.assertEqual('Running',result['status'])
        asyncio.run(controls.reconcile_notifications(self.host))
        self.assertTrue(all(row['status']=='delivered' for row in self.notices()))
        record=self.record(result)
        instance=__import__('nginx_qa.telegram_launcher_execution',fromlist=['descriptor']).descriptor(record)
        client.finish=True; client.starts.clear()
        client.start(instance,record['executor_receipt']['initial_assignment_ids'],record['executor_ready'])
        self.policy['grants'][0]['actions']=['read']
        os.environ[launcher.POLICY_ENV]=json.dumps(self.policy)
        asyncio.run(controls.reconcile_notifications(self.host))
        self.assertEqual('Completed',json.loads(self.notices()[-1]['snapshot'])['lifecycle'])
        self.assertEqual('delivered',self.notices()[-1]['status'])
        status=controls.instance_status(self.operations,result['operation_id'],self.principal,self.policy['projects'][self.project_id])
        self.assertEqual('Completed',status['status'])
        self.assertEqual(2,len(status['reviews']))
        self.assertTrue(all(review['decision']=='APPROVE' for review in status['reviews']))
        self.assertFalse(any(row['live'] for row in status['assignments']))
        self.assertEqual('NOT_REPORTED',status['checks_status'])
        serialized=json.dumps(status)
        self.assertNotIn(str(self.base),serialized)
        self.assertNotIn('result_summary_redacted',serialized)

    def test_revoked_read_suppresses_notice_and_status_cannot_activate(self):
        self.ingest()
        self.policy['grants'][0]['actions'].remove('read')
        os.environ[launcher.POLICY_ENV]=json.dumps(self.policy)
        asyncio.run(controls.reconcile_notifications(self.host))
        self.assertTrue(all(row['status']=='suppressed' for row in self.notices()))
        self.host.telegram_api_json.assert_not_called()
        self.assert_no_activation()

    def test_stale_generation_and_callback_rate_limit_fail_closed(self):
        result=self.ingest(); record=self.record(result)
        handle=self.transport.handle(record,'status')
        with patch.object(self.operations,'get',return_value={**record,'current_import_key':'new-attempt'}):
            with self.assertRaises(HTTPException) as raised:
                self.transport.resolve(handle,self.principal)
        self.assertEqual('HANDLE_STALE',raised.exception.detail['error'])
        for _ in range(20):
            self.transport.rate_limit(self.principal,now=100)
        with self.assertRaises(HTTPException) as raised:
            self.transport.rate_limit(self.principal,now=100)
        self.assertEqual(429,raised.exception.status_code)
        self.transport.rate_limit(self.principal,now=180)


class LauncherControlsPureTests(unittest.TestCase):
    def test_gateway_origin_rejects_insecure_or_redirect_urls(self):
        for value in ('http://example.test','https://user:pass@example.test','https://example.test/path','https://example.test?token=x','https://127.0.0.1','https://example.test:8443','https://bad.local','https://0x7f.0.0.1','https://foo.local.','https://foo.localhost.'):
            with self.subTest(value=value),patch.dict(os.environ,{'NGINX_QA_LAUNCHER_PUBLIC_ORIGIN':value}),self.assertRaises(HTTPException):
                controls.gateway_origin()
        with patch.dict(os.environ,{'NGINX_QA_LAUNCHER_PUBLIC_ORIGIN':''}):
            self.assertIsNone(controls.gateway_origin())

    def test_only_explicit_typed_reports_are_projected_without_raw_summary(self):
        commit='a'*40
        report={'launcher_report_v1':{'checks':[{'name':'integration','status':'PASS'}],'artifacts':['evidence/checks.json'],'pr_url':'https://github.com/example/repo/pull/5'}}
        state={'result_receipts':[{'result_commit':commit,'result_summary_redacted':json.dumps(report)}]}
        projected=controls.structured_reports(state,'https://github.com/example/repo')
        self.assertEqual('agent_report',projected[0]['source'])
        self.assertEqual(commit,projected[0]['artifacts'][0]['commit'])
        for raw in ('All done PASS C:/private/task.txt',json.dumps({'launcher_report_v1':{**report['launcher_report_v1'],'artifacts':['C:/private/task.txt']}}),json.dumps({'launcher_report_v1':{**report['launcher_report_v1'],'pr_url':'https://evil.test/pull/5'}})):
            state['result_receipts'][0]['result_summary_redacted']=raw
            self.assertEqual([],controls.structured_reports(state,'https://github.com/example/repo'))
