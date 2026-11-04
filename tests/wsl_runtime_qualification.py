"""Explicit live WSL lifecycle probes; no model, native API or production repo.

Run with --config pointing at an installed owned runner config and --output at
the operator evidence file. Creates only disposable private guest attempts.
"""
import argparse,hashlib,json,subprocess,sys,time,uuid
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from nginx_qa.managed_wsl_runner import WslCodexRunner

SETUP=r'''
import runpy,os,json,sys,time,subprocess
from pathlib import Path
m=runpy.run_path('/opt/nginx-qa-worker/supervisor.py')
job=sys.argv[1];mode=sys.argv[2]
admission=m['admit']()
attempt,lock=m['new_attempt'](job)
(attempt/'work/docs').mkdir(parents=True);(attempt/'work/.git').mkdir()
(attempt/'work/docs/probe.md').write_text('untouched')
(attempt/'config/requirements.toml').write_text(m['requirements'](['docs/probe.md'],False))
m['guard_mountpoints'](attempt/'work',['docs/probe.md'])
for p in (attempt/'work').rglob('*'):os.chown(p,1000,1000)
os.chown(attempt/'work',1000,1000)
print(json.dumps({'ready':True,'attempt':str(attempt)}),flush=True)
code="import os,time;from pathlib import Path; p=os.fork();\nif p==0:\n os.setsid(); q=os.fork();\n if q==0:\n  Path('docs/probe.md').write_text('daemon-'+str(os.getpid()));time.sleep(40)\n else:os._exit(0)\ntime.sleep(40)"
if mode=='pressure':
 code="import os,time,json,resource;from pathlib import Path\np=Path('docs/probe.md');r={'file_limit':resource.getrlimit(resource.RLIMIT_FSIZE)[1]}\ntry:p.write_bytes(b'x'*(r['file_limit']+1024))\nexcept OSError as e:r['file_errno']=e.errno\nr['file_size']=p.stat().st_size;r['forked']=0\nfor i in range(100):\n try:pid=os.fork()\n except OSError as e:r['fork_errno']=e.errno;break\n if pid==0:time.sleep(40);os._exit(0)\n r['forked']+=1\np.write_text(json.dumps(r))"
try:
 if mode=='orphan':
  groups=m['Groups'](attempt.name+'-pre-receipt')
  m['write_json'](attempt/'launch-intent.json',{'job':job,'generation':attempt.name})
  child=subprocess.Popen(['/bin/sleep','40'],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,preexec_fn=groups.attach)
  print(json.dumps({'orphan_created':True,'active_receipt_written':False,'pid':child.pid}),flush=True)
  lock.close();admission.close()
  # Retain only the WSL transport while deliberately relinquishing executor
  # ownership. Some WSL builds otherwise retire every child at transport exit.
  sys.stdin.buffer.read(1)
 else:
  if mode=='preparing':
   wait_deadline=time.monotonic()+12
   while not m['cancelled'](attempt):
    assert time.monotonic()<wait_deadline,'stop fence was not installed'
    time.sleep(.03)
  try:
   m['run_bounded'](attempt,['docs/probe.md'],False,['/usr/bin/python3','-c',code],timeout=2 if mode=='deadline' else 30,heartbeat=mode not in ('deadline','pressure'))
   error=None
  except Exception as exc:error=str(exc)
  print(json.dumps({'error':error,'marker':(attempt/'work/docs/probe.md').read_text(),'retired':(attempt/'retired.json').exists()}),flush=True)
finally:lock.close();admission.close()
'''

def main():
    p=argparse.ArgumentParser();p.add_argument('--config',required=True);p.add_argument('--output',required=True);a=p.parse_args()
    runner=WslCodexRunner(json.loads(Path(a.config).read_bytes()));wsl=runner.executable;distro=runner.distro
    def launch(name,mode):
        job=runner.job_id(name)
        child=subprocess.Popen([wsl,'-d',distro,'-u','root','--exec','/usr/bin/python3','-I','-c',SETUP,job,mode],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,creationflags=subprocess.CREATE_NO_WINDOW)
        ready=json.loads(child.stdout.readline());assert ready['ready']
        return child,ready,job
    def live(job):
        script='from pathlib import Path;import json,sys;p=[p for c in ("memory","pids","cpu") for p in (Path("/sys/fs/cgroup")/c).glob("nginx-qa-"+sys.argv[1]+"-*")];print(json.dumps({"pids":sorted(set(int(x) for d in p for x in (d/"cgroup.procs").read_text().split()))}))'
        data=subprocess.check_output([wsl,'-d',distro,'-u','root','--exec','/usr/bin/python3','-I','-c',script,job],timeout=10)
        return json.loads(data)['pids']
    def marker(path):
        script='from pathlib import Path;import sys;print((Path(sys.argv[1])/"work/docs/probe.md").read_text())'
        return subprocess.check_output([wsl,'-d',distro,'-u','root','--exec','/usr/bin/python3','-I','-c',script,path],timeout=10).decode().strip()
    checks=[]
    name='lifecycle-deadline-'+uuid.uuid4().hex
    child,ready,job=launch(name,'deadline');out,err=child.communicate(timeout=15)
    result=json.loads(out.splitlines()[-1]);assert result['error']=='WSL_DEADLINE_EXCEEDED' and result['marker'].startswith('daemon-') and result['retired'] and not live(job),(result,err)
    checks.append({'case':'deadline_double_fork_setsid','status':'PASS','evidence':result})
    name='lifecycle-eof-'+uuid.uuid4().hex
    child,ready,job=launch(name,'eof')
    deadline=time.monotonic()+8
    while not marker(ready['attempt']).startswith('daemon-'):
        assert time.monotonic()<deadline;child.stdin.write(b'\n');child.stdin.flush();time.sleep(.1)
    assert live(job)
    child.stdin.close();child.stdin=None;out,err=child.communicate(timeout=15)
    result=json.loads(out.splitlines()[-1]);assert result['error']=='WSL_CONTROLLER_EOF' and result['retired'] and not live(job),(result,err)
    checks.append({'case':'controller_pipe_eof','status':'PASS','evidence':result})
    name='lifecycle-early-stop-'+uuid.uuid4().hex
    child,ready,job=launch(name,'preparing');runner.quiesce(name);out,err=child.communicate(timeout=15)
    result=json.loads(out.splitlines()[-1]);assert result['error']=='WSL_JOB_CANCELLED' and result['marker']=='untouched' and not live(job),(result,err)
    checks.append({'case':'stop_during_preparation_prevents_future_spawn','status':'PASS','evidence':result})
    name='lifecycle-orphan-'+uuid.uuid4().hex
    child,ready,job=launch(name,'orphan')
    result=json.loads(child.stdout.readline());assert result['orphan_created'] and result['active_receipt_written'] is False and live(job)
    fresh=WslCodexRunner(json.loads(Path(a.config).read_bytes()))
    try:fresh.ready();raise AssertionError('orphan incorrectly admitted')
    except ValueError:assert fresh.sandbox_diagnostic['reason']=='WSL_PREVIOUS_WORK_UNRETIRED',fresh.sandbox_diagnostic
    runner.quiesce(name);assert not live(job);child.communicate(timeout=10)
    checks.append({'case':'restart_detects_orphan_without_postfork_receipt_and_pidfd_stop_retires','status':'PASS'})
    # A hard loss of the Linux supervisor itself must kill its PID namespace.
    # The fixture is trusted code, but it creates actual daemonized descendants.
    name='lifecycle-supervisor-loss-'+uuid.uuid4().hex
    child,ready,job=launch(name,'eof')
    deadline=time.monotonic()+8
    while not marker(ready['attempt']).startswith('daemon-'):
        assert time.monotonic()<deadline;child.stdin.write(b'\n');child.stdin.flush();time.sleep(.1)
    script='import os,signal,sys;from pathlib import Path;target=sys.argv[1];pids=[]\nfor d in Path("/proc").iterdir():\n if d.name.isdigit():\n  try: raw=(d/"cmdline").read_bytes().split(b"\\0")\n  except OSError: continue\n  if len(raw)>5 and raw[0].endswith(b"python3") and target.encode() in raw and b"eof" in raw:pids.append(int(d.name))\nassert len(pids)==1\nfd=os.pidfd_open(pids[0]);signal.pidfd_send_signal(fd,signal.SIGKILL);os.close(fd)'
    subprocess.run([wsl,'-d',distro,'-u','root','--exec','/usr/bin/python3','-I','-c',script,job],check=True,timeout=10)
    child.stdin.close();child.stdin=None;child.communicate(timeout=10)
    deadline=time.monotonic()+8
    while live(job):assert time.monotonic()<deadline;time.sleep(.1)
    runner.quiesce(name)
    checks.append({'case':'linux_supervisor_SIGKILL_pdeath_and_namespace_retirement','status':'PASS'})
    name='lifecycle-resource-pressure-'+uuid.uuid4().hex
    child,ready,job=launch(name,'pressure');out,err=child.communicate(timeout=15)
    result=json.loads(out.splitlines()[-1]);pressure=json.loads(result['marker'])
    assert result['error'] is None and result['retired'] and not live(job),(result,err)
    assert pressure['file_errno']==27 and pressure['file_size']==pressure['file_limit']==16*1024*1024,pressure
    assert pressure['fork_errno']==11 and 1<=pressure['forked']<64,pressure
    checks.append({'case':'actual_file_size_and_process_pressure_retired','status':'PASS','evidence':pressure})
    fresh.ready();assert fresh.sandbox_diagnostic['state']=='PASS'
    report={'status':'PASS','kind':'live_owned_WSL_boundary_and_lifecycle_no_model','checks':checks,'final_boundary':fresh.sandbox_diagnostic,'native_sprint_acceptance':False}
    Path(a.output).write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'status':'PASS','cases':len(checks),'output':a.output}))

if __name__=='__main__':main()
