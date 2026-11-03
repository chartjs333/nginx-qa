"""Cross-component qualification with real engine APIs and disposable remotes."""
import asyncio
from contextlib import asynccontextmanager
import copy
import json
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from fastapi import FastAPI
from nginx_qa.git_provider import RepositorySpec
from nginx_qa.managed_import import TransactionalSprintImporter
from nginx_qa.managed_continuity import ManagedContinuityError,ManagedContinuityRuntime
from nginx_qa.sprint_types import canonical_json_bytes
from nginx_qa.telegram_launcher import Principal,POLICY_ENV
from nginx_qa.telegram_launcher_store import LauncherOperations
from nginx_qa import telegram_launcher_execution as execution
from nginx_qa import telegram_launcher_intake as intake
from nginx_qa.launcher_gateway import create_gateway
from nginx_qa.launcher_host import reserve_listeners,serve_pair
from tests import test_telegram_launcher_intake as intake_tests
from tests import test_telegram_launcher_launch_modes as modes


class LauncherQualificationTests(intake_tests.LauncherIntakeFixture):
    allow_start=modes.LauncherLaunchModeTests.allow_start
    launch=modes.LauncherLaunchModeTests.launch
    stop=modes.LauncherLaunchModeTests.stop

    def complete(self,runtime,project,identity,*,revision=None):
        assignment=identity['assignment'];workspace=identity['workspace'];root=Path(workspace['actual_git_toplevel'])
        if revision:
            (root/'review-fix.txt').write_text(revision,encoding='utf-8',newline='\n')
            self.git('add','review-fix.txt',cwd=root);self.git('commit','-m','fixture reviewer correction',cwd=root)
        head=self.git('rev-parse','HEAD',cwd=root)
        self.assertEqual('',self.git('-c','core.autocrlf=false','status','--porcelain','--untracked-files=all',cwd=root))
        self.git('push','origin','HEAD:refs/heads/'+workspace['assigned_branch'],cwd=root)
        response=runtime.submit_if_managed(project,'2864',canonical_json_bytes({
            'assignment_id':assignment['assignment_id'],'status':'DONE','result':'Fixture API result',
            'from_commit':assignment['initial_head_commit'],'git_commit':head,'git_branch':workspace['assigned_branch']}),'qa-fixture-result')
        self.assertEqual('REVIEWS_PENDING',response.response['status'])
        return head

    def test_launcher_reject_rework_and_two_new_reviews_complete_exact_new_result(self):
        client=modes.FakeExistingClient(self);self.allow_start(client)
        operation=self.ingest();self.launch(operation['operation_id'])
        initial=client.runtime.current_identity(self.project_id,'2864')
        first=self.complete(client.runtime,self.project_id,initial)
        stale=client.runtime.current_identity(self.project_id,'2892')
        review=client.runtime.current_identity(self.project_id,'2891')
        rejected=client.runtime.submit_if_managed(self.project_id,'2891',canonical_json_bytes({
            'assignment_id':review['assignment']['assignment_id'],'status':'REJECT','feedback':'Fixture requires a committed correction'}),'qa-reject')
        self.assertEqual('REWORK_ENQUEUED',rejected.response['status'])
        revised=client.runtime.current_identity(self.project_id,'2864')
        self.assertNotEqual(initial['assignment']['assignment_id'],revised['assignment']['assignment_id'])
        self.assertEqual(1,revised['assignment']['rework_cycle'])
        with self.assertRaises(ManagedContinuityError):
            client.runtime.submit_if_managed(self.project_id,'2892',canonical_json_bytes({
                'assignment_id':stale['assignment']['assignment_id'],'status':'APPROVE','feedback':'Stale approval must not advance'}),'qa-stale-review')
        final=self.complete(client.runtime,self.project_id,revised,revision='Actual committed correction after independent REJECT.\n')
        self.assertNotEqual(first,final)
        review_ids=[]
        for phone in ('2891','2892'):
            identity=client.runtime.current_identity(self.project_id,phone)
            review_ids.append(identity['assignment']['assignment_id'])
            self.assertEqual(final,identity['assignment']['result_commit'])
            accepted=client.runtime.submit_if_managed(self.project_id,phone,canonical_json_bytes({
                'assignment_id':identity['assignment']['assignment_id'],'status':'APPROVE','feedback':'Fixture independent corrected-head verification '+phone}),'qa-approve-'+phone)
            self.assertEqual('REVIEW_ACCEPTED',accepted.response['status'])
        self.assertEqual(2,len(set(review_ids)))
        state=self.imp.store.runtime_state(self.project_id,operation['sprint_id'])
        self.assertEqual('completed',state['status'])
        self.assertEqual('Completed',self.operations.get(operation['operation_id'],self.principal)['lifecycle'])
        approved=[row for row in state['reviews'] if row['decision']=='APPROVE']
        self.assertEqual(2,len(approved))

    def test_two_active_projects_stop_one_preserves_other_queues_history_workspace_and_result(self):
        first_client=modes.FakeExistingClient(self);self.allow_start(first_client)
        first=self.ingest();self.launch(first['operation_id'])
        first_identity=first_client.runtime.current_identity(self.project_id,'2864')
        second_remote=self.base/'independent-upstream.git'
        self.git('clone','--bare',str(self.source),str(second_remote))
        registry={'main':RepositorySpec('main','example.invalid/acme/independent',str(second_remote))}
        second_imp=TransactionalSprintImporter(self.runtime_config,registry,allow_local_transport=True,port_reservations=self.port_reservations)
        operations=LauncherOperations(second_imp)
        principal=Principal(self.principal.user_id,self.principal.chat_id,'9003',self.principal.actions)
        envelope=copy.deepcopy(self.envelope);envelope['project_id']='9003'
        envelope['manifest']['git_address']='https://example.invalid/acme/independent.git'
        project_policy=copy.deepcopy(self.policy['projects'][self.project_id])
        self.policy['projects']['9003']=project_policy
        self.policy['grants'].append({**self.policy['grants'][0],'project_id':'9003'})
        os.environ[POLICY_ENV]=json.dumps(self.policy)
        repository,binding=intake.validate_source(second_imp,envelope,envelope['manifest']['git_address'],project_policy)
        record=operations.reserve(principal,operations.keys('456789',2,2,principal),envelope,{},binding)
        intent=second_imp.git_provider.create_launcher_manifest_commit(repository,operation_id=record['operation_id'],
            source_commit=self.commit,manifest_blob=canonical_json_bytes(envelope['manifest']),created_at=record['created_at'])
        operations.update(record['operation_id'],principal,provision_intent=intent)
        second_imp.git_provider.publish_launcher_manifest(repository,intent)
        operations.update(record['operation_id'],principal,published=True)
        prepared=operations.prepare(record['operation_id'],principal)
        fixture=SimpleNamespace(importer=lambda:second_imp,assertEqual=self.assertEqual,git=self.git)
        second_client=modes.FakeExistingClient(fixture)
        self.host.managed_launcher_executor_factory=lambda:second_client
        running=asyncio.run(execution.launch(operations,record['operation_id'],principal,project_policy,self.host))
        self.assertEqual('Running',running['lifecycle'])
        second_identity=second_client.runtime.current_identity('9003','2864')
        self.assertNotEqual(first_identity['workspace']['actual_git_toplevel'],second_identity['workspace']['actual_git_toplevel'])
        self.assertNotEqual(first['instance_id'],prepared['instance_id'])
        before=second_imp.store.runtime_state('9003',prepared['sprint_id'])
        with second_imp.store._transaction() as connection:
            queues=[dict(row) for row in connection.execute("SELECT * FROM managed_queue_items WHERE json_extract(payload_json,'$.sprint_id')=? ORDER BY dedupe_key",(prepared['sprint_id'],))]
        self.assertTrue(queues)
        root=Path(second_identity['workspace']['actual_git_toplevel'])
        branch=self.git('branch','--show-current',cwd=root);head=self.git('rev-parse','HEAD',cwd=root)
        self.host.managed_launcher_executor_factory=lambda:first_client
        self.assertEqual('Stopped',self.stop(first['operation_id'])['lifecycle'])
        self.assertEqual(before,second_imp.store.runtime_state('9003',prepared['sprint_id']))
        with second_imp.store._transaction() as connection:
            self.assertEqual(queues,[dict(row) for row in connection.execute("SELECT * FROM managed_queue_items WHERE json_extract(payload_json,'$.sprint_id')=? ORDER BY dedupe_key",(prepared['sprint_id'],))])
        self.assertEqual((branch,head),(self.git('branch','--show-current',cwd=root),self.git('rev-parse','HEAD',cwd=root)))
        self.assertEqual(second_identity['assignment']['assignment_id'],second_client.runtime.current_identity('9003','2864')['assignment']['assignment_id'])
        final=self.complete(second_client.runtime,'9003',second_identity)
        for phone in ('2891','2892'):
            review=second_client.runtime.current_identity('9003',phone)
            second_client.runtime.submit_if_managed('9003',phone,canonical_json_bytes({
                'assignment_id':review['assignment']['assignment_id'],'status':'APPROVE','feedback':'Independent project continues '+phone}),'qa-independent-'+phone)
        self.assertEqual('completed',second_imp.store.runtime_state('9003',prepared['sprint_id'])['status'])
        self.assertEqual('Stopped',self.operations.get(first['operation_id'],self.principal)['lifecycle'])
        self.assertEqual(final,self.git('rev-parse','HEAD',cwd=root))


class ActualDualListenerTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_uvicorn_two_loopback_sockets_one_lifespan_and_no_private_route_on_public(self):
        events=[]
        @asynccontextmanager
        async def lifespan(app):
            events.append('start')
            try:yield
            finally:events.append('stop')
        private=FastAPI(lifespan=lifespan)
        @private.get('/private-probe')
        async def probe():return {'private':True}
        public=create_gateway(object())
        sockets=reserve_listeners(0,0);ports=[sock.getsockname()[1] for sock in sockets]
        stop=asyncio.Event()
        async def get(port,path):
            reader,writer=await asyncio.open_connection('127.0.0.1',port)
            try:
                writer.write(('GET '+path+' HTTP/1.1\r\nHost: gateway.example.test\r\nConnection: close\r\n\r\n').encode());await writer.drain()
                return await asyncio.wait_for(reader.read(),3)
            finally:writer.close();await writer.wait_closed()
        with patch.dict(os.environ,{'NGINX_QA_LAUNCHER_GATEWAY_ENABLED':'true','NGINX_QA_TELEGRAM_LAUNCHER_ENABLED':'true',
                'NGINX_QA_LAUNCHER_PUBLIC_ORIGIN':'https://gateway.example.test'}):
            task=asyncio.create_task(serve_pair(private,public,sockets,stop))
            try:
                for _ in range(100):
                    if events:break
                    await asyncio.sleep(.01)
                await asyncio.sleep(.1)
                self.assertIn(b'200 OK',await get(ports[0],'/private-probe'))
                shell=await get(ports[1],'/launcher/tml:9002:'+'A'*24)
                self.assertIn(b'200 OK',shell);self.assertIn(b'no-store',shell)
                self.assertIn(b'404 Not Found',await get(ports[1],'/private-probe'))
                self.assertIn(b'404 Not Found',await get(ports[1],'/openapi.json'))
                self.assertEqual(['start'],events)
            finally:
                stop.set();await asyncio.wait_for(task,5)
            self.assertEqual(['start','stop'],events)
            self.assertTrue(all(sock.fileno()==-1 for sock in sockets))
