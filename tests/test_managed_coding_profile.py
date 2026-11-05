"""Admission/export policy tests. Fixture runners are not live model evidence."""
import ast
from copy import deepcopy
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import tempfile
import threading
import unittest
from unittest.mock import Mock

from nginx_qa.managed_execution_profiles import execution_path, execution_paths
from nginx_qa.managed_codex_client import ManagedCodexClient
from nginx_qa.sprint_types import canonical_json_bytes
from tests.test_managed_import import ManagedImportFixture


class CodingAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.allowed = ['src/main/java/example/Form.java', 'src/main/resources/templates/form.html', 'docs/task.md']
        self.config = {'state_dir': str(self.root/'state'), 'sandbox_backend': 'wsl2-bwrap',
            'engine_url': 'http://127.0.0.1:19999', 'projects': {},
            'project_templates': {'9020': {'profile_id': 'java-maven-v1', 'repository_id': 'main',
                'workspace_root': str(self.root/'managed'), 'branch_prefix': 'agent/',
                'repository_url': 'https://example.invalid/acme/repository.git',
                'allowed_document_paths': self.allowed}}}
        self.client = ManagedCodexClient(self.config, runner=Mock(), api=Mock())
        manifest = ManagedImportFixture.manifest_fixture('a'*64)
        manifest['git']['expected_source_commit'] = 'a'*40
        self.payload = {'project_id': '9020', 'manifest': manifest,
            'manifest_sha256': hashlib.sha256(canonical_json_bytes(manifest)).hexdigest(),
            'manifest_commit': 'b'*40, 'git_manifest_path': 'orchestration/tasks/one.json',
            'allowed_document_paths': self.allowed[:2]}

    def tearDown(self):
        self.client.lockfile.close()
        self.tmp.cleanup()

    def rehash(self, payload):
        payload['manifest_sha256'] = hashlib.sha256(canonical_json_bytes(payload['manifest'])).hexdigest()

    def test_durable_exact_pin_admission_and_restart(self):
        accepted = self.client.admit(self.payload)
        self.assertEqual(accepted['status'], 'ADMITTED')
        self.assertEqual(self.client.admit(self.payload), accepted)
        self.client.lockfile.close()
        self.client = ManagedCodexClient(self.config, runner=Mock(), api=Mock())
        self.assertEqual(self.client.admit(self.payload), accepted)
        self.assertEqual(self.client.projects['9020']['expected_sprint_id'], accepted['sprint_id'])

    def test_unconfigured_project_scope_root_and_role_escalation_denied(self):
        variants=[]
        p=deepcopy(self.payload);p['project_id']='9021';variants.append(p)
        p=deepcopy(self.payload);p['allowed_document_paths']=['src/main/java/example/Foreign.java'];variants.append(p)
        p=deepcopy(self.payload);p['workspace_root']='C:/';variants.append(p)
        p=deepcopy(self.payload);p['manifest']['git_address']='https://example.invalid/foreign/repo.git';self.rehash(p);variants.append(p)
        p=deepcopy(self.payload);p['manifest']['git']['assigned_branch']='main';self.rehash(p);variants.append(p)
        p=deepcopy(self.payload);p['manifest']['execution']['reviewers'][1]['phone']='2891';self.rehash(p);variants.append(p)
        for p in variants:
            with self.subTest(payload=p), self.assertRaises(ValueError):self.client.admit(p)
        self.assertFalse((self.root/'state/project-admissions.json').exists())

    def test_graph_hash_and_source_pin_are_required(self):
        p=deepcopy(self.payload);p['manifest']['nodes'][0]['transitions']['DONE']='missing';self.rehash(p)
        with self.assertRaisesRegex(ValueError,'MANIFEST_INVALID'):self.client.admit(p)
        p=deepcopy(self.payload);p['manifest_sha256']='0'*64
        with self.assertRaisesRegex(ValueError,'MANIFEST_CHANGED'):self.client.admit(p)
        p=deepcopy(self.payload);p['manifest']['git']['expected_source_commit']=None;self.rehash(p)
        with self.assertRaisesRegex(ValueError,'SOURCE_COMMIT_REQUIRED'):self.client.admit(p)

    def test_active_project_allows_only_exact_replay(self):
        accepted=self.client.admit(self.payload)
        self.client.state['sessions']['fixture']={'descriptor':{'project_id':'9020'},'phase':'READY'}
        self.assertEqual(self.client.admit(self.payload),accepted)
        p=deepcopy(self.payload);p['manifest_commit']='c'*40
        with self.assertRaisesRegex(ValueError,'PROJECT_BUSY'):self.client.admit(p)
        self.client.state['sessions']['fixture']['phase']='TERMINAL'
        self.assertNotEqual(self.client.admit(p)['sprint_id'],accepted['sprint_id'])

    def test_policy_change_cannot_reuse_admission(self):
        self.client.admit(self.payload)
        self.client.config['project_templates']['9020']['allowed_document_paths']=['docs/task.md']
        with self.assertRaisesRegex(ValueError,'POLICY_CHANGED'):self.client._load_admissions()

    def test_exact_paths_have_no_credentials_globs_or_escape(self):
        for p in self.allowed+['pom.xml','src/test/java/example/FormTest.java']:
            self.assertEqual(execution_path(p,'java-maven-v1'),p)
        for p in ['src/main/LITE/privateKey.pem','src/main/resources/application.properties',
                  '.git/config','src/main/java/../secret.java','src/main/java/*','src/main/java/.secret/F.java',
                  'src/main/java/x.java:stream','src/main/java\\x.java','src/test/resources/input.xlsx']:
            with self.subTest(path=p), self.assertRaises(ValueError):execution_path(p,'java-maven-v1')
        with self.assertRaises(ValueError):execution_path(self.allowed[0],'docs-only-v1')
        with self.assertRaises(ValueError):execution_paths(self.allowed*50,'java-maven-v1')


class SupervisorProjectionTests(unittest.TestCase):
    def functions(self):
        # Linux-only imports are omitted; execute the actual pure policy helpers.
        path=Path(__file__).resolve().parents[1]/'nginx_qa/wsl_worker_supervisor.py'
        tree=ast.parse(path.read_text(encoding='utf-8'))
        keep=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in {'safe_path','requirements','mounts'}]
        namespace={'Path':Path,'PurePosixPath':PurePosixPath,'re':re,'json':json,
                   'BIN':'/opt/nginx-qa-worker/bin','UID':1000,'DISABLED':()}
        exec(compile(ast.Module(body=keep,type_ignores=[]),str(path),'exec'),namespace)
        return namespace

    def test_author_exact_source_mount_review_readonly_and_sensitive_mask(self):
        f=self.functions()
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);(root/'work/src').mkdir(parents=True)
            (root/'work/src/privateKey.pem').write_text('fixture secret')
            author=f['mounts'](root,['src/main/java/example/F.java'],False,'java-maven-v1')
            review=f['mounts'](root,['src/main/java/example/F.java'],True,'java-maven-v1')
            self.assertIn('/work/.git',author)
            self.assertIn('/dev/null',author)
            self.assertIn('/work/src/privateKey.pem',author)
            self.assertIn('/work/src/main/java/example/F.java',author)
            self.assertNotIn('/work/src/main/java/example/F.java',review)
            self.assertIn('/maven-cache',review)
            req=f['requirements'](['src/main/java/example/F.java'],True,'java-maven-v1')
            self.assertIn('"/tmp"="write"',req)
            self.assertIn('"/work/.git"="deny"',req)
            self.assertIn('enabled=false',req)

    def test_controller_and_supervisor_admit_same_source_path_contract(self):
        safe=self.functions()['safe_path']
        for profile in ('docs-only-v1','java-maven-v1'):
            for p in ('docs/a.md','src/main/java/p/F.java','src/test/java/p/FTest.java','pom.xml',
                      'src/main/resources/templates/a.html','src/main/resources/static/a.js',
                      'src/main/resources/application.properties','../x','src/.git/x.java','docs/.hidden.md'):
                decisions=[]
                for validate in (execution_path,safe):
                    try:validate(p,profile);decisions.append(True)
                    except ValueError:decisions.append(False)
                self.assertEqual(decisions[0],decisions[1],(profile,p))

if __name__=='__main__':unittest.main()
