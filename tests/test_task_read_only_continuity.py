"""Real temporary Git/native graph with deterministic, explicitly fixture model output."""
import hashlib
import json
import time
from pathlib import Path
from types import SimpleNamespace
import unittest

from nginx_qa.managed_codex_client import ManagedCodexClient, PROTOCOL
from nginx_qa.sprint_types import canonical_json_bytes
from nginx_qa.task_wizard import TaskWizard
from tests.test_managed_import import ManagedImportFixture
from tests.test_telegram_launcher_launch_modes import FakeExistingClient


class NativeReadOnlyContinuityTest(ManagedImportFixture):
    canonical_remote = 'github.com/acme/repository'

    def test_missing_input_traverses_read_only_report_to_native_operator_gate(self):
        imp = self.importer()
        wizard = TaskWizard(SimpleNamespace(), self.base/'wizard-state', lambda: {})
        wizard.importer = lambda project: imp
        project = {'allowed_actor_ids':['alice'], 'name':'Fixture',
            'repository_url':'https://github.com/acme/repository.git','repository_id':'main',
            'profile_id':'docs-only-v1','source_ref':'refs/heads/main','branch_prefix':'agent/task-',
            'phones':{'author':'2864','coordinator':'2860','reviewers':['2891','2892']},
            'allowed_paths':['docs/managed-task-result.md'],'source_checks':['service.py']}
        wizard.policy_path.parent.mkdir(parents=True)
        wizard.policy_path.write_text(json.dumps({'schema_version':1,'projects':{self.project_id:project}}))
        fixture_transport = FakeExistingClient(self)
        wizard._executor = lambda p: fixture_transport
        admissions=[]
        wizard._client=lambda p,route,payload=None: admissions.append(payload) if route=='admit' else {'protocol':'fixture'}
        preview=wizard.preview('alice',{'repository_url':project['repository_url'],'task':'Explain the missing input; do not guess.'})
        started=wizard.start('alice',preview['task_id'],preview['plan_digest'])
        admission=admissions[0]
        manifest_path=self.base/'client-manifest.json'
        manifest_path.write_bytes(canonical_json_bytes(admission['manifest']))
        identity=fixture_transport.runtime.current_identity(self.project_id,'2864')
        # The fixture is a real local remote. No live repository/model/host is used.
        config={'state_dir':str(self.base/'client-state'),'engine_url':'http://127.0.0.1:19998',
            'control_secret_ref':'env:NGINX_QA_LAUNCHER_CONTROL_FIXTURE',
            'projects':{self.project_id:{'profile_id':'docs-only-v1','manifest_path':str(manifest_path),
                'manifest_sha256':hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
                'repository_id':'main','manifest_commit':admission['manifest_commit'],
                'git_manifest_path':admission['git_manifest_path'],'workspace_root':str(Path(self.runtime_config['managed_root'])/'projects'/self.project_id),
                'branch_prefix':'agent/task-','repository_url':str(self.remote),'allowed_document_paths':project['allowed_paths']}}}
        fixture=self
        class API:
            def identity(self, project_id, phone):
                return fixture_transport.runtime.current_identity(project_id,phone) or {}
            def request(self, phone, route, payload=None):
                if route.endswith('/observability'):
                    fixture_transport.runtime.reconcile_all()
                    state=imp.store.runtime_state(fixture.project_id,identity['sprint_id'])
                    return {'execution':{'status':state['status']}}
                return fixture_transport.runtime.submit_if_managed(fixture.project_id,phone,
                    canonical_json_bytes(payload),'fixture-read-only').response
        calls=[]
        class Runner:
            sandbox_diagnostic={'state':'PASS'}
            def ready(self, **kwargs):pass
            def quiesce(self, job):pass
            def run(self, **kwargs):
                calls.append(kwargs)
                outcomes=kwargs['schema']['properties']['status']['enum']
                if 'APPROVE' in outcomes:return {'status':'APPROVE','summary':'Independent deterministic fixture evidence.'}
                if outcomes==['BLOCKED_EXTERNAL']:
                    fixture.assertEqual(kwargs['schema']['title'],'review')
                    fixture.assertIn('Exact missing instruction',kwargs['prompt'])
                    return {'status':'BLOCKED_EXTERNAL','summary':'Exact missing instruction: which source workbook is authoritative?'}
                return {'status':'NEED_DECISION','summary':'Exact missing instruction: which source workbook is authoritative?'}
        client=ManagedCodexClient(config,runner=Runner(),api=API(),resolver=lambda ref:'fixture-token-0123456789')
        try:
            descriptor={'protocol':PROTOCOL,'operation_id':started['operation_id'],'project_id':self.project_id,
                'sprint_id':identity['sprint_id'],'agent_phones':['2860','2864','2891','2892']}
            ready=client.prepare(descriptor);sid=ready['client_session_id']
            client.start({**descriptor,'client_session_id':sid,'initial_assignment_ids':[identity['assignment']['assignment_id']]})
            deadline=time.monotonic()+60
            while time.monotonic()<deadline:
                state=imp.store.runtime_state(self.project_id,identity['sprint_id'])
                if len(state['reviews'])==4:break
                time.sleep(.1)
            state=imp.store.runtime_state(self.project_id,identity['sprint_id'])
            self.assertEqual(state['status'],'active')
            self.assertEqual(len(state['reviews']),4)
            self.assertEqual(len(calls),6)
            coordinator=next(a for a in state['assignments'] if a['node_id']=='continuity')
            attempt=client.state['sessions'][sid]['assignments'][coordinator['assignment_id']]
            self.assertIsNone(attempt['payload']['git_branch'])
            self.assertEqual(attempt['payload']['git_commit'],coordinator['source_commit'])
            self.assertEqual(attempt['payload']['status'],'BLOCKED_EXTERNAL')
            context=next(c for c in state['coordinator_contexts'] if c.get('failed_assignment_id')==coordinator['assignment_id'])
            self.assertEqual(context['reason_code'],'BLOCKED_EXTERNAL')
            self.assertIsNone(context['assigned_branch'])
            self.assertEqual(context['result_commit'],coordinator['source_commit'])
            self.assertEqual(len(context['reviewer_feedback']),2)
            self.assertIn('Exact missing instruction',context['test_evidence_summary']['result_summary_redacted'])
            # Reporting the question is not an operator recovery decision.
            self.assertFalse(state.get('recovery_records'))
            self.assertTrue(any('Exact missing instruction' in s['text'] for s in wizard.get('alice',preview['task_id'])['summaries']))
        finally:
            for session in client.state['sessions'].values():
                sid,_=client._session(session['descriptor'])
                client.stop({**session['descriptor'],'client_session_id':sid})
            client.lockfile.close()


if __name__=='__main__':unittest.main()
