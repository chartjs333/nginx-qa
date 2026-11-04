"""Actual restart of the exact owned host, with unchanged native/client history."""
import json,subprocess,sys
from pathlib import Path
from http_qualification import Client
from stop_wizard import quiescent
ROOT=Path(__file__).resolve().parent
PS='C:/Windows/System32/WindowsPowerShell/v1.0/powershell.exe'
def run(args):
 p=subprocess.run(args,capture_output=True,text=True,encoding='utf-8',timeout=180)
 if p.returncode:raise RuntimeError(p.stderr[-3000:]+p.stdout[-3000:])
 return p.stdout
before=quiescent()
browser=json.loads((ROOT/'browser-recovery-evidence.json').read_bytes());assert browser['status']=='PASS'
operation=next(c for c in browser['cases'] if c['name']=='exact receipt after reload and project switch')['operation_id']
client=Client();actor=client.pair();receipt=client.req('/api/v1/projects/9000/setup/operations/'+operation);assert receipt['state']=='SUCCEEDED'
old_pid=(ROOT/'host-launch-pid.txt').read_text().strip()
stop=run([sys.executable,'-X','utf8','-B',str(ROOT/'stop_wizard.py'),'--stop'])
assert quiescent()==before
start=run([PS,'-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',str(ROOT/'start_all.ps1')])
new_pid=(ROOT/'host-launch-pid.txt').read_text().strip();assert new_pid!=old_pid
client=Client();assert client.pair()==actor
restored=client.req('/api/v1/projects/9000/setup/operations/'+operation);assert restored==receipt
assert quiescent()==before
report={'status':'PASS','tested_product_commit':'c9193d7d6cab67f0b191a86f5e0e268bce074f9e','old_launcher_pid':int(old_pid),'new_launcher_pid':int(new_pid),'ownership_proof':'timestamped host-stop and host-launch receipts in this directory','same_named_actor':actor,'operation_id':operation,'receipt_byte_fields_identical':True,'native_and_client_state_before':before,'native_and_client_state_unchanged':True,'new_native_sprint':False,'stop_stdout':stop,'start_stdout':start}
(ROOT/'host-restart-evidence.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'status':'PASS','new_launcher_pid':int(new_pid),'receipt_restored':operation,'native_client_history_unchanged':True}))
