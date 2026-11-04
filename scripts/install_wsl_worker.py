"""Install pinned worker files into an explicitly owned, already imported WSL2 guest.

No distro import, global WSL configuration, Docker action, credential creation,
model request, native sprint mutation or service startup is performed here.
"""
import argparse,hashlib,json,os,subprocess,sys,winreg
from pathlib import Path

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--runtime-root',required=True);p.add_argument('--cli-directory',required=True)
    p.add_argument('--bootstrap',action='store_true',help='Install guest packages/account and apply per-guest isolation settings')
    a=p.parse_args();root=Path(a.runtime_root).resolve(strict=True);source=Path(a.cli_directory).resolve(strict=True)
    owner=json.loads((root/'.nginx-qa-worker-owner.json').read_bytes())
    assert owner['kind']=='nginx-qa-isolated-worker-runtime'
    distro=owner['distro'];assert distro.startswith('NginxQaWorker-')
    registry=winreg.OpenKey(winreg.HKEY_CURRENT_USER,r'Software\Microsoft\Windows\CurrentVersion\Lxss')
    default=winreg.QueryValueEx(registry,'DefaultDistribution')[0];found=[];i=0
    while True:
        try:key=winreg.OpenKey(registry,winreg.EnumKey(registry,i));i+=1
        except OSError:break
        try:
            if winreg.QueryValueEx(key,'DistributionName')[0]==distro:
                found.append(winreg.QueryValueEx(key,'BasePath')[0])
        finally:winreg.CloseKey(key)
    assert len(found)==1 and Path(found[0].removeprefix('\\\\?\\')).resolve()==root/'distro','Owned distro registration mismatch'
    wsl=str(Path(os.environ['SystemRoot'])/'System32/wsl.exe')
    def execute(code,raw=b'',timeout=30):
        r=subprocess.run([wsl,'-d',distro,'-u','root','--exec','/usr/bin/python3','-I','-c',code],input=raw,capture_output=True,timeout=timeout)
        if r.returncode:raise RuntimeError('Owned WSL provisioning command failed; no secret output retained')
        return r.stdout
    execute('from pathlib import Path;assert not any((p/"cgroup.procs").read_text().strip() for c in ("memory","pids","cpu") for p in (Path("/sys/fs/cgroup")/c).glob("nginx-qa-*")),"Worker is active"')
    if a.bootstrap:
        script='''set -eu
apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y -qq bubblewrap ca-certificates python3 git util-linux procps
id nqworker >/dev/null 2>&1 || useradd --create-home --shell /bin/bash nqworker
test "$(id -u nqworker)" = 1000
cat > /etc/wsl.conf <<'EOF'
[automount]
enabled=false
mountFsTab=false
[interop]
enabled=false
appendWindowsPath=false
[user]
default=nqworker
[boot]
systemd=false
EOF
'''
        r=subprocess.run([wsl,'-d',distro,'-u','root','--exec','/bin/sh','-s'],input=script.encode(),capture_output=True,timeout=240)
        assert r.returncode==0,'Owned guest bootstrap failed'
        subprocess.run([wsl,'--terminate',distro],check=True,stdout=subprocess.DEVNULL,timeout=30)
    hashes={}
    for name in ('codex','codex-code-mode-host','rg','codex-resources/bwrap'):
        raw=(source/name).read_bytes();assert raw[:4]==b'\x7fELF','Expected Linux ELF resource'
        hashes[name]=hashlib.sha256(raw).hexdigest()
        code='import sys,os,hashlib;from pathlib import Path;p=Path('+repr('/opt/nginx-qa-worker/bin/'+name)+');p.parent.mkdir(parents=True,exist_ok=True);raw=sys.stdin.buffer.read();assert hashlib.sha256(raw).hexdigest()=='+repr(hashes[name])+';p.write_bytes(raw);os.chmod(p,0o755)'
        execute(code,raw,90)
    raw=(Path(__file__).resolve().parents[1]/'nginx_qa/wsl_worker_supervisor.py').read_bytes()
    supervisor_sha=hashlib.sha256(raw).hexdigest()
    execute('import sys,os;from pathlib import Path;p=Path("/opt/nginx-qa-worker/supervisor.py");p.write_bytes(sys.stdin.buffer.read());os.chmod(p,0o755)',raw)
    cfg={'sandbox_backend':'wsl2-bwrap','wsl_distribution':distro,
         'wsl_supervisor':'/opt/nginx-qa-worker/supervisor.py','wsl_supervisor_sha256':supervisor_sha,
         'wsl_cli_sha256':hashes['codex'],'wsl_runtime_hashes':hashes,'role_timeout_seconds':600}
    dest=root/'runner-config.json';dest.write_text(json.dumps(cfg,indent=2)+'\n',encoding='utf-8')
    assert winreg.QueryValueEx(registry,'DefaultDistribution')[0]==default
    winreg.CloseKey(registry)
    print(json.dumps({'configuration':str(dest),'distro':distro,'installed_hashes':hashes,'supervisor_sha256':supervisor_sha,'started':False}))

if __name__=='__main__':main()
