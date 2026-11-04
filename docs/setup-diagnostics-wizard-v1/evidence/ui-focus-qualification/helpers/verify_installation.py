"""Source/state/readiness proof; invokes only explicit worker probe, never a sprint."""
import hashlib,json,sqlite3,subprocess,urllib.request
from pathlib import Path
from http_qualification import Client
ROOT=Path(__file__).resolve().parent
SOURCE=Path('D:/nq-sdw-launcher')
PRODUCT='c9193d7d6cab67f0b191a86f5e0e268bce074f9e'
BRANCH='codex/sdw-qualified-ui-focus-recovery-20261104'
def git(*args):return subprocess.check_output(['git','-C',str(SOURCE),*args]).decode().strip()
def rows(path):
 db=sqlite3.connect('file:'+str(path)+'?mode=ro',uri=True)
 try:return list(db.execute('SELECT sprint_id,status,state_json FROM managed_sprints ORDER BY sprint_id'))
 finally:db.close()
def main():
 assert git('rev-parse','HEAD')==PRODUCT and git('branch','--show-current')==BRANCH
 assert not git('status','--porcelain=v1','--untracked-files=all')
 assert git('ls-remote','--heads','origin','refs/heads/'+BRANCH).split()[0]==PRODUCT
 for ref,commit in [('codex/sdw-qualified-ui-recovery-20261104','fdafbf58db85555706b8ea04e11c1d4450b0f120'),('codex/sdw-qualified-host-continuation-20261104','a1841e420a517dfdb9ed2c5a4b44eaeb6b3286ca'),('codex/sdw-qualified-host-20261104','65662f69db52c27b6c0972ccec8031cfb68a02fc')]:
  assert git('ls-remote','--heads','origin','refs/heads/'+ref).split()[0]==commit
 backup=ROOT/'protected-pre-upgrade-backup'
 before=rows(backup/'managed-import.sqlite3');after=rows(Path('C:/nqt37/r/leases/managed-import.sqlite3'));assert before==after
 assert (backup/'client-receipts.json').read_bytes()==Path('C:/nqt37/wsl-client-r4/client-receipts.json').read_bytes()
 assert (backup/'environment.SECRET').read_bytes()==(SOURCE/'.env.staging').read_bytes()
 before_owner=json.loads((backup/'owner.json').read_bytes());owner=json.loads(Path('C:/nqt37/.nginx-qa-staging-owner.json').read_bytes())
 expected=json.loads(json.dumps(before_owner));expected['ownership'].update(approved_commit=PRODUCT,branch=BRANCH);assert owner==expected
 blobs=json.loads((ROOT/'client-runtime-blob-equality.json').read_bytes());assert blobs['identical'] and len(blobs['modules'])==16 and blobs['installed_source']==PRODUCT
 asset=urllib.request.build_opener(urllib.request.ProxyHandler({})).open('http://127.0.0.1:18038/setup/assets/setup.js',timeout=10).read()
 assert asset==(SOURCE/'nginx_qa/static/setup.js').read_bytes()
 client=Client();actor=client.pair();probe=client.action('worker.probe',{'candidate_id':'default-executor'},'sdwu-focus-qualified-worker-probe-c9193d-20261104');assert probe['receipt']['state']=='SUCCEEDED'
 snapshot=client.req('/api/v1/projects/9000/setup/snapshot');binding=next(c for c in snapshot['checks'] if c['check_id']=='worker.binding')
 assert binding['status']=='PASS', 'Binding requires explicit native wizard repair'
 report={'status':'PASS','tested_product_commit':PRODUCT,'installed_branch':BRANCH,'source_clean':True,'old_remote_refs_unchanged':True,'initialization_id':owner['initialization_id'],'environment_bytes_unchanged':True,'native_rows_byte_identical':[{'sprint_id':s,'status':t,'state_sha256':hashlib.sha256(v.encode()).hexdigest()} for s,t,v in after],'client_journal_sha256':hashlib.sha256((backup/'client-receipts.json').read_bytes()).hexdigest(),'actual_imported_modules':blobs,'served_setup_js_sha256':hashlib.sha256(asset).hexdigest(),'actor':actor,'actual_worker_probe':probe,'existing_binding':binding,'binding_rewrite_needed':False,'new_model_run':False}
 (ROOT/'installed-readiness.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8');print(json.dumps({'status':'PASS','product':PRODUCT,'worker_probe':probe['receipt']['state'],'binding':binding['status'],'native_rows_preserved':len(after)}))
if __name__=='__main__':main()
