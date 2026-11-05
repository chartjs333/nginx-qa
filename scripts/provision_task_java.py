"""Install a verified JDK and build cache in the already owned, idle WSL guest."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess


ROOT = Path('D:/nq-sdw-worker-runtime')
WORK = Path('C:/nqt38/development')


def execute(code, raw=b'', timeout=120):
    result = subprocess.run(['wsl.exe', '-d', 'NginxQaWorker-20261104', '-u', 'root', '--exec',
                             '/usr/bin/python3', '-I', '-c', code], input=raw, capture_output=True, timeout=timeout)
    if result.returncode:
        # These commands never contain credentials. Keep diagnostics bounded.
        raise RuntimeError(result.stderr.decode('utf-8', 'replace')[-3000:])
    return result.stdout.decode('utf-8', 'replace')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--cache', action='store_true')
    args = parser.parse_args()
    owner = json.loads((ROOT / '.nginx-qa-worker-owner.json').read_bytes())
    assert owner['distro'] == 'NginxQaWorker-20261104' and owner['kind'] == 'nginx-qa-isolated-worker-runtime'
    # Installer validates the Windows registration and exact BasePath before this script is used.
    execute('from pathlib import Path;assert not any((p/"cgroup.procs").read_text().strip() for c in ("memory","pids","cpu") for p in (Path("/sys/fs/cgroup")/c).glob("nginx-qa-*")),"Worker active"')
    if not args.cache:
        metadata = json.loads((WORK / 'jdk8-source.json').read_text(encoding='utf-8-sig'))
        raw = (WORK / 'jdk8.tar.gz').read_bytes()
        assert hashlib.sha256(raw).hexdigest() == metadata['binary']['package']['checksum']
        print(execute('''import sys,io,tarfile,os
from pathlib import Path
root=Path('/opt/nginx-qa-worker')
target=root/'java'
if not target.exists():
    staging=root/'java-install'
    staging.mkdir(exist_ok=False)
    with tarfile.open(fileobj=io.BytesIO(sys.stdin.buffer.read()),mode='r:gz') as t:
        t.extractall(staging,filter='data')
    children=list(staging.iterdir())
    assert len(children)==1 and (children[0]/'bin/java').is_file()
    children[0].rename(target)
    staging.rmdir()
import subprocess
r=subprocess.run(['/bin/sh','-c','apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq maven'],capture_output=True,timeout=240)
assert r.returncode==0,r.stderr.decode()[-1000:]
(root/'maven-repository').mkdir(exist_ok=True)
r=subprocess.run([str(target/'bin/java'),'-version'],capture_output=True)
print(r.stderr.decode())
''', raw, timeout=300))
    else:
        raw = (WORK / 'maven-cache.tar').read_bytes()
        print(execute('''import sys,io,tarfile
from pathlib import Path
root=Path('/opt/nginx-qa-worker/maven-repository')
root.mkdir(exist_ok=True)
with tarfile.open(fileobj=io.BytesIO(sys.stdin.buffer.read()),mode='r:') as t:
    for member in t.getmembers():
        assert not member.issym() and not member.islnk()
        assert 'settings.xml' not in member.name and 'credentials' not in member.name.lower()
    t.extractall(root,filter='data')
print('Offline Maven cache installed')
''', raw, timeout=180))


if __name__ == '__main__':
    main()
