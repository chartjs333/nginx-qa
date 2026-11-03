"""Run existing isolated unittest suites and record source-bound QA evidence.

No server, live Telegram, model, tunnel or production project is contacted by
this runner. Test fixtures own their temporary remotes/state/listeners.
"""
from __future__ import annotations

import argparse
from datetime import datetime,timezone
import hashlib
from importlib import metadata
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))


DEFAULT_MODULES=(
    'test_telegram_launcher_contract','test_telegram_launcher_prompt','test_telegram_launcher_intake',
    'test_telegram_launcher_preparation','test_telegram_launcher_launch_modes','test_telegram_launcher_controls',
    'test_telegram_launcher_delivery_lifecycle','test_telegram_launcher_qualification',
    'test_launcher_auth_races','test_launcher_control_secrets','test_launcher_gateway','test_launcher_host',
    'test_launcher_tunnel','test_launcher_transport_migration','test_managed_import','test_managed_continuity',
    'test_managed_continuity_import_recovery','test_managed_continuity_merge_git','test_managed_workspaces',
    'test_process_supervisor','test_port_leases','test_branch_leases','test_git_provider','test_workspace_safety',
    'test_managed_scope_secret_boundary','test_scope_workflow','test_scope_revisions','test_scope_migration',
    'test_scope_runtime_adapters','test_scope_snapshot_memory','test_scope_probe_serialization','test_legacy_scope_control',
    'test_staging_isolation','test_staging_legacy_inventory','test_staging_cleanup_reproof','test_windows_launcher',
    'test_inbound_proposal_api','test_inbound_coordinator_contract','test_configure_telegram',
    'test_sequential_prompt_ui','test_sprint_type_contract','test_pending_sprints_ui','test_cycle_graph_ui',
    'test_project_manager','test_project_actor_import','test_queue_persistence','test_operator_session',
    'test_execution_projection_cache','test_execution_read_scheduling','test_execution_refresh','test_execution_observability',
    'test_auto_refresh_ui','test_checkpoint_copy_memory','test_prompt_archive_source_metadata','test_prompt_archive_auth',
    'test_prompt_archive_response_shape_contract','test_prompt_archive_incremental_security',
)


def git(*arguments):
    from nginx_qa.control_secrets import sanitized_child_environment
    return subprocess.run(['git','-C',str(ROOT),*arguments],check=True,stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,env=sanitized_child_environment()).stdout.decode('utf-8').strip()


def source_inventory():
    paths=set(git('ls-files','-co','--exclude-standard','-z').split('\0'))
    prefixes=('nginx_qa/','tests/','scripts/','static/','schemas/','prompts/')
    top={'main.py','configure_telegram.py','run_staging.ps1','staging_host_app.py','cloudflared_quick_tunnel.ps1','requirements.txt',
         'group_templates.json','.env.example','.env.staging.example',
         'docs/universal-managed-sprint-contract.md'}
    result={}
    for relative in sorted(paths):
        path=ROOT/relative
        launcher_document=(relative.startswith('docs/telegram-managed-sprint-launcher-v1/')
                           and '/evidence/' not in relative)
        if path.is_file() and (relative in top or relative.startswith(prefixes)
                              or launcher_document or relative.endswith('.env.example')):
            raw=path.read_bytes()
            if path.suffix.lower() in {'.py','.ps1','.js','.css','.html','.json','.md','.txt','.bat'}:
                raw=raw.replace(b'\r\n',b'\n')
            result[relative]=hashlib.sha256(raw).hexdigest()
    return result


def flatten(suite):
    for item in suite:
        if isinstance(item,unittest.TestSuite):yield from flatten(item)
        else:yield item


class EvidenceResult(unittest.TextTestResult):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs);self.records=[];self.current={};self.started={}

    def startTest(self,test):
        self.current[test.id()]={'test':test.id(),'status':'PASS'};self.started[test.id()]=time.monotonic()
        super().startTest(test)

    def addError(self,test,error):
        if test.id() not in self.current:
            self.records.append({'test':test.id(),'status':'ERROR','exception_type':error[0].__name__,'seconds':0})
        else:self.current[test.id()].update(status='ERROR',exception_type=error[0].__name__)
        super().addError(test,error)

    def addFailure(self,test,error):
        self.current[test.id()].update(status='FAIL',exception_type=error[0].__name__);super().addFailure(test,error)

    def addSkip(self,test,reason):
        if test.id() not in self.current:
            self.records.append({'test':test.id(),'status':'SKIP','reason':reason,'seconds':0})
        else:self.current[test.id()].update(status='SKIP',reason=reason)
        super().addSkip(test,reason)

    def addExpectedFailure(self,test,error):
        self.current[test.id()].update(status='EXPECTED_FAILURE',exception_type=error[0].__name__);super().addExpectedFailure(test,error)

    def addUnexpectedSuccess(self,test):
        self.current[test.id()].update(status='UNEXPECTED_SUCCESS');super().addUnexpectedSuccess(test)

    def addSubTest(self,test,subtest,error):
        if error is not None:self.current[test.id()].update(status='FAIL',exception_type=error[0].__name__)
        super().addSubTest(test,subtest,error)

    def stopTest(self,test):
        item=self.current.pop(test.id());item['seconds']=round(time.monotonic()-self.started.pop(test.id()),6)
        self.records.append(item);super().stopTest(test)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence',type=Path,required=True)
    parser.add_argument('--modules',nargs='+',default=list(DEFAULT_MODULES))
    parser.add_argument('--list',action='store_true')
    parser.add_argument('--failfast',action='store_true')
    args=parser.parse_args()
    os.chdir(ROOT)
    modules=['tests.'+name.removeprefix('tests.') for name in args.modules]
    cases={test.id():test for test in flatten(unittest.defaultTestLoader.loadTestsFromNames(modules))}
    if args.list:
        print(json.dumps({'modules':modules,'unique_test_ids':len(cases)},indent=2));return 0
    before=source_inventory();started=datetime.now(timezone.utc).isoformat();begin=time.monotonic()
    head=git('rev-parse','HEAD');branch=git('branch','--show-current');dirty=git('status','--porcelain','--untracked-files=all')
    result=unittest.TextTestRunner(verbosity=2,resultclass=EvidenceResult,failfast=args.failfast,stream=sys.stdout).run(unittest.TestSuite(cases.values()))
    after=source_inventory()
    packages={name:metadata.version(name) for name in ('fastapi','uvicorn','jsonschema')}
    record={'kind':'isolated_automated_qualification','started_utc':started,'finished_utc':datetime.now(timezone.utc).isoformat(),
        'status':'PASS' if result.wasSuccessful() and before==after else 'FAIL','seconds':round(time.monotonic()-begin,6),
        'source_commit':head,'branch':branch,'worktree_status_before':dirty,'source_files_sha256_lf':before,
        'source_unchanged_during_tests':before==after,'modules':modules,'tests_run':result.testsRun,
        'failures':len(result.failures),'errors':len(result.errors),'skipped':len(result.skipped),
        'python':sys.version,'python_executable':sys.executable,'platform':platform.platform(),'packages':packages,
        'results':result.records,'external_acceptance':{'real_telegram_mobile':'NOT_RUN','real_cloudflare':'NOT_RUN','external_model_roundtrip':'NOT_RUN'},
        'scope':'Local disposable fixtures; no production project aliases, no live agent/model or provider calls.'}
    args.evidence.parent.mkdir(parents=True,exist_ok=True)
    temporary=args.evidence.with_name(args.evidence.name+'.tmp')
    temporary.write_text(json.dumps(record,ensure_ascii=False,indent=2)+'\n',encoding='utf-8',newline='\n')
    os.replace(temporary,args.evidence)
    return 0 if record['status']=='PASS' else 1


if __name__=='__main__':raise SystemExit(main())
