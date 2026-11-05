"""Root-owned Linux transport executor, never a scheduler. Install in the owned guest.

The host sends one bounded JSON request followed by heartbeat bytes. All model
processes live in a PID namespace and private cgroups. EOF/deadline kills that
namespace, including setsid/double-fork descendants, before returning a receipt.
"""
from __future__ import annotations
import base64
import configparser
import ctypes
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import resource
import select
import signal
import socket
import stat
import subprocess
import sys
import threading
import tempfile
import time
import uuid

ROOT = Path('/var/lib/nginx-qa-worker')
BIN = '/opt/nginx-qa-worker/bin'
CLI = BIN + '/codex'
UID = 1000
MAX_BUNDLE = 32 * 1024 * 1024
MAX_FILE = 1024 * 1024
MAX_WRITE = 16 * 1024 * 1024
MAX_REQUEST = 96 * 1024 * 1024
DISABLED = ('apps', 'plugins', 'remote_plugin', 'hooks', 'multi_agent', 'browser_use',
            'computer_use', 'in_app_browser', 'memories', 'shell_snapshot',
            'request_permissions_tool', 'skill_mcp_dependency_install')


def safe_path(value, profile_id='docs-only-v1'):
    if profile_id == 'java-maven-v1':
        if (not isinstance(value,str) or len(value)>240 or not re.fullmatch(r'[A-Za-z0-9_./-]+',value)
                or any(p in ('','.','..') or p.startswith('.') for p in value.split('/'))):
            raise ValueError('WSL_DOCUMENT_PATH_INVALID')
        if (value=='pom.xml' or value.startswith('docs/') and value.endswith('.md')
                or re.fullmatch(r'src/(main|test)/java/[A-Za-z0-9_./-]+\.java',value)
                or re.fullmatch(r'src/main/resources/templates/[A-Za-z0-9_./-]+\.html',value)
                or re.fullmatch(r'src/main/resources/static/[A-Za-z0-9_./-]+\.(js|css)',value)
                or re.fullmatch(r'src/test/resources/[A-Za-z0-9_./-]+\.(json|xml|txt)',value)):
            return value
        raise ValueError('WSL_DOCUMENT_PATH_INVALID')
    if profile_id != 'docs-only-v1':raise ValueError('CLIENT_PROJECT_PROFILE_FORBIDDEN')
    if (not isinstance(value, str) or len(value) > 240 or '\\' in value or ':' in value
            or not re.fullmatch(r'docs/[A-Za-z0-9_./-]+\.md', value)
            or any(p in ('', '.', '..') or p.startswith('.') for p in value.split('/'))):
        raise ValueError('WSL_DOCUMENT_PATH_INVALID')
    return value


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def write_json(path, value):
    temp = path.with_suffix('.tmp')
    temp.write_text(json.dumps(value), encoding='utf-8');os.chmod(temp, 0o600)
    temp.replace(path)


def command(args, *, attempt, cwd=None, timeout=15):
    generation=attempt.name+'-prep-'+uuid.uuid4().hex
    write_json(attempt/'preparation-intent.json',{'generation':generation,'job':attempt.name[:64]})
    groups=Groups(generation);proc=None;parent=os.getpid()
    def child():
        if os.getppid()!=parent:os._exit(125)
        groups.attach()
        if ctypes.CDLL(None).prctl(1,signal.SIGKILL,0,0,0)!=0 or os.getppid()!=parent:os._exit(125)
    try:
        with tempfile.TemporaryFile() as out,tempfile.TemporaryFile() as err:
            if cancelled(attempt):raise ValueError('WSL_JOB_CANCELLED')
            proc=subprocess.Popen(['/usr/bin/unshare','--pid','--fork','--kill-child',*args],cwd=cwd,
                env={'PATH':'/usr/bin:/bin','HOME':'/nonexistent','GIT_CONFIG_NOSYSTEM':'1',
                     'GIT_CONFIG_GLOBAL':'/dev/null','GIT_TERMINAL_PROMPT':'0'},
                stdin=subprocess.DEVNULL,stdout=out,stderr=err,preexec_fn=child)
            deadline=time.monotonic()+timeout
            while proc.poll() is None:
                if cancelled(attempt):raise ValueError('WSL_JOB_CANCELLED')
                if time.monotonic()>deadline or os.fstat(out.fileno()).st_size+os.fstat(err.fileno()).st_size>4*1024*1024:
                    raise ValueError('WSL_PREPARATION_LIMIT')
                time.sleep(.03)
            if proc.returncode:raise ValueError('WSL_LOCAL_COMMAND_FAILED')
            out.seek(0);return out.read(4*1024*1024+1)
    finally:
        if proc and proc.poll() is None:proc.kill()
        if proc:proc.wait(timeout=5)
        groups.retire()


def requirements(paths, review, profile_id='docs-only-v1'):
    fs = {':minimal':'read', BIN:'read', '/work':'read', '/task':'read', '/private':'deny'}
    if not review:
        # Codex's Linux mount builder expects writable roots to be directories.
        # The outer namespace is stricter: only the individual approved regular
        # files are writable bind mounts; their parent directories remain RO.
        fs.update({'/work/'+str(PurePosixPath(safe_path(p,profile_id)).parent):'write' for p in paths})
    if profile_id == 'java-maven-v1':
        fs.update({'/tmp':'write', '/maven-cache':'read', '/opt/nginx-qa-worker/java':'read'})
        # Source files and exact approved author writes remain subject to outer
        # bind mounts. The model cannot recover secrets from Git history.
        fs['/work/.git']='deny'
    lines = ['allowed_permission_profiles={"nq-managed"=true}',
        'default_permissions="nq-managed"', 'allowed_approval_policies=["never"]',
        'allowed_web_search_modes=["disabled"]','allow_browser_and_computer_use=false',
        'allow_login_shell=false','allow_remote_control=false','allow_managed_hooks_only=true',
        '[mcp_servers]', '[plugins]', '[features]']
    lines += [key+'=false' for key in DISABLED]
    lines += ['[permissions.nq-managed.filesystem]']
    lines += [json.dumps(k)+'='+json.dumps(v) for k,v in fs.items()]
    lines += ['[permissions.nq-managed.network]', 'enabled=false']
    return '\n'.join(lines)+'\n'


def mounts(attempt, paths, review, profile_id='docs-only-v1'):
    args = ['/usr/bin/bwrap', '--unshare-user','--unshare-pid','--unshare-ipc','--unshare-uts',
            '--die-with-parent','--new-session','--ro-bind','/usr','/usr']
    for name in ('bin','sbin','lib','lib64'):
        args += ['--symlink','usr/'+name,'/'+name]
    args += ['--proc','/proc','--dev','/dev','--tmpfs','/tmp','--dir','/home/nqworker',
        '--ro-bind',BIN,BIN,'--ro-bind',str(attempt/'work'),'/work']
    if not review:
        for p in paths:args += ['--bind',str(attempt/'work'/safe_path(p,profile_id)),'/work/'+p]
    if profile_id == 'java-maven-v1':
        args += ['--tmpfs','/work/.git', '--ro-bind','/opt/nginx-qa-worker/maven-repository','/maven-cache',
                 '--ro-bind','/opt/nginx-qa-worker/java','/opt/nginx-qa-worker/java']
        if Path('/etc/maven').is_dir():args += ['--ro-bind','/etc/maven','/etc/maven']
        # Debian's mvn and Java dependency links resolve through this root-owned
        # directory. Without it the actual sandbox cannot execute /usr/bin/mvn.
        if Path('/etc/alternatives').is_dir():args += ['--ro-bind','/etc/alternatives','/etc/alternatives']
        for p in (attempt/'work').rglob('*'):
            name=p.name.lower()
            if p.is_file() and (p.suffix.lower() in {'.pem','.key','.p12','.pfx','.jks','.keystore'}
                    or name.startswith('.env') or name in {'auth.json','credentials.json','settings.xml'}
                    or name.startswith('application') and p.suffix.lower() in {'.properties','.yml','.yaml'}):
                args += ['--ro-bind','/dev/null','/work/'+p.relative_to(attempt/'work').as_posix()]
    args += ['--bind',str(attempt/'private'),'/private',
        '--ro-bind',str(attempt/'task'),'/task','--ro-bind',str(attempt/'config'),'/etc/codex']
    for p in ('passwd','group','resolv.conf','nsswitch.conf','ssl'):
        args += ['--ro-bind','/etc/'+p,'/etc/'+p]
    return args+['--uid',str(UID),'--gid',str(UID),'--cap-drop','ALL','--chdir','/work',
        '--clearenv','--setenv','PATH','/usr/bin:/bin','--setenv','HOME','/home/nqworker',
        '--setenv','CODEX_HOME','/private/codex','--setenv','LANG','C.UTF-8']+(
        ['--setenv','JAVA_HOME','/opt/nginx-qa-worker/java'] if profile_id=='java-maven-v1' else [])+['--']


def guard_mountpoints(work, paths):
    # The pinned Codex bwrap builder mounts protection placeholders beneath
    # writable roots. Prepare empty directories before making parents RO.
    for parent in {work/str(PurePosixPath(p).parent) for p in paths}:
        for name in ('.git','.codex','.agents','.aws','.azure','.ssh','.config'):
            target=parent/name
            if not target.exists():target.mkdir()


class Groups:
    """cgroup v1 limits in the dedicated WSL guest; no host-wide changes."""
    def __init__(self, name):
        self.paths=[]
        for controller,values in (
            ('memory',{'memory.limit_in_bytes':str(2*1024**3),'memory.memsw.limit_in_bytes':str(2*1024**3)}),
            ('pids',{'pids.max':'64'}),
            ('cpu',{'cpu.cfs_period_us':'100000','cpu.cfs_quota_us':'50000'})):
            p=Path('/sys/fs/cgroup')/controller/('nginx-qa-'+name)
            p.mkdir();self.paths.append(p)
            for filename,value in values.items():
                if filename=='memory.memsw.limit_in_bytes' and not (p/filename).exists():continue
                (p/filename).write_text(value)

    def attach(self):
        for p in self.paths:(p/'cgroup.procs').write_text(str(os.getpid()))

    def pids(self):
        result=set()
        for p in self.paths:
            try:result.update(int(x) for x in (p/'cgroup.procs').read_text().split())
            except FileNotFoundError:pass
        return result

    def retire(self):
        deadline=time.monotonic()+8
        while True:
            pids=self.pids()
            if not pids:break
            kill_members(self.paths)
            if time.monotonic()>deadline:raise ValueError('WSL_QUIESCENCE_UNPROVEN')
            time.sleep(.03)
        for p in reversed(self.paths):
            try:p.rmdir()
            except FileNotFoundError:pass


def group_pids(paths):
    result=set()
    for path in paths:
        try:result.update(int(x) for x in (path/'cgroup.procs').read_text().split())
        except FileNotFoundError:pass
    return result


def kill_members(paths):
    # A PID from a stale cgroup snapshot is not ownership proof. Open a pidfd,
    # then recheck group membership and signal that stable kernel reference.
    for pid in group_pids(paths):
        try:fd=os.pidfd_open(pid,0)
        except ProcessLookupError:continue
        try:
            if pid in group_pids(paths):signal.pidfd_send_signal(fd,signal.SIGKILL)
        except ProcessLookupError:pass
        finally:os.close(fd)


def cancelled(attempt):
    return (ROOT/(attempt.name[:64]+'.cancel')).exists()


def run_bounded(attempt, paths, review, argv, *, timeout, heartbeat=True, profile_id='docs-only-v1'):
    if cancelled(attempt):raise ValueError('WSL_JOB_CANCELLED')
    # Intent precedes cgroup creation/fork, so recovery has an owned generation
    # even if the supervisor dies between fork and the PID receipt.
    write_json(attempt/'launch-intent.json',{'generation':attempt.name,'job':attempt.name[:64]})
    groups=Groups(attempt.name)
    output=attempt/'stdout';errors=attempt/'stderr'
    parent_pid=os.getpid()
    def child():
        if os.getppid()!=parent_pid:os._exit(125)
        groups.attach()
        # Bind-mounted documents live on disk, outside the memory cgroup.
        # Bound writes in-kernel before executing any nontrusted process.
        # Codex's private SQLite WAL exceeds the 1 MiB document contract.
        # The kernel ceiling bounds temporary writes; export still requires
        # each approved document to satisfy the stricter MAX_FILE contract.
        resource.setrlimit(resource.RLIMIT_FSIZE,(MAX_WRITE,MAX_WRITE))
        resource.setrlimit(resource.RLIMIT_CORE,(0,0))
        os.setgroups([]);os.setgid(UID);os.setuid(UID)
        if ctypes.CDLL(None).prctl(1,signal.SIGKILL,0,0,0)!=0:os._exit(125)
        if os.getppid()!=parent_pid:os._exit(125)
    proc=None
    try:
        with output.open('wb') as out,errors.open('wb') as err:
            if cancelled(attempt):raise ValueError('WSL_JOB_CANCELLED')
            proc=subprocess.Popen(mounts(attempt,paths,review,profile_id)+argv,stdin=subprocess.DEVNULL,
                stdout=out,stderr=err,preexec_fn=child,env={'PATH':'/usr/bin:/bin'})
            write_json(attempt/'active.json',{'pid':proc.pid,'start_ticks':Path(f'/proc/{proc.pid}/stat').read_text().split()[21],
                'cgroups':[str(p) for p in groups.paths],'namespace':'private-bwrap-pid','deadline_seconds':timeout})
            deadline=time.monotonic()+timeout;last=time.monotonic()
            while proc.poll() is None:
                if cancelled(attempt):raise ValueError('WSL_JOB_CANCELLED')
                if time.monotonic()>deadline:raise ValueError('WSL_DEADLINE_EXCEEDED')
                if output.stat().st_size+errors.stat().st_size>4*1024*1024:
                    raise ValueError('WSL_OUTPUT_LIMIT')
                if heartbeat:
                    if select.select([sys.stdin.buffer],[],[],.05)[0]:
                        data=os.read(sys.stdin.fileno(),4096)
                        if not data:raise ValueError('WSL_CONTROLLER_EOF')
                        if any(x!=10 for x in data):raise ValueError('WSL_HEARTBEAT_INVALID')
                        last=time.monotonic()
                    if time.monotonic()-last>8:raise ValueError('WSL_HEARTBEAT_EXPIRED')
                else:time.sleep(.03)
            code=proc.returncode
        groups.retire()
        return code,output.read_bytes(),errors.read_bytes()
    finally:
        if proc and proc.poll() is None:proc.kill()
        if proc:proc.wait(timeout=5)
        if groups.paths and groups.paths[0].exists():groups.retire()
        write_json(attempt/'retired.json',{'quiescent':True,'at':time.time()})
        (attempt/'active.json').unlink(missing_ok=True)


def new_attempt(job):
    if not re.fullmatch('[0-9a-f]{64}',job):raise ValueError('WSL_JOB_ID_INVALID')
    ROOT.mkdir(exist_ok=True,mode=0o711);os.chmod(ROOT,0o711)
    lock=(ROOT/(job+'.lock')).open('a+b');os.chmod(ROOT/(job+'.lock'),0o600)
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    if (ROOT/(job+'.cancel')).exists():
        lock.close();raise ValueError('WSL_JOB_CANCELLED')
    attempt=ROOT/(job+'-'+uuid.uuid4().hex)
    attempt.mkdir(mode=0o711)
    for part in ('private','private/codex','task','config'):
        (attempt/part).mkdir(mode=0o700);os.chown(attempt/part,UID,UID)
    os.chmod(attempt/'config',0o755)
    return attempt,lock


def verify_runtime(request):
    if os.getuid()!=0:raise ValueError('WSL_SUPERVISOR_REQUIRES_ROOT')
    if sha(CLI)!=request.get('cli_sha256'):raise ValueError('WSL_CLI_CHANGED')
    if sha(__file__)!=request.get('supervisor_sha256'):raise ValueError('WSL_SUPERVISOR_CHANGED')
    hashes=request.get('runtime_hashes',{})
    required={'codex','codex-code-mode-host','rg','codex-resources/bwrap'}
    if set(hashes)!=required:raise ValueError('WSL_RUNTIME_MANIFEST_REQUIRED')
    for name,expected in hashes.items():
        p=Path(BIN)/name;s=p.stat()
        if s.st_uid!=0 or s.st_mode&0o022 or sha(p)!=expected:raise ValueError('WSL_RUNTIME_CHANGED')
    settings=configparser.ConfigParser();settings.read('/etc/wsl.conf')
    # Empty mountpoint directories survive an earlier guest boot. The shared WSL
    # kernel may retain a binfmt entry for another distro: never disable that
    # global entry. The model namespace omits /init, host mounts and IPC sockets.
    if (settings.get('automount','enabled',fallback='true')!='false'
            or settings.get('interop','enabled',fallback='true')!='false'
            or any(p.is_dir() and any(p.iterdir()) for p in (Path('/mnt/c'),Path('/mnt/d')))):
        raise ValueError('WSL_HOST_EXPOSURE_PRESENT')
    if not Path('/sys/fs/cgroup/memory/memory.limit_in_bytes').exists():
        raise ValueError('WSL_CGROUPS_UNAVAILABLE')


CANARY = r'''import os,socket,json,sys
from pathlib import Path
r={'nonce':sys.argv[1],'uid':os.getuid(),'read':Path('README.md').read_text()==sys.argv[1]}
def write(p):
 try:Path(p).write_text('attempt');return True
 except OSError:return False
r['doc_write']=write('docs/probe.md')
r['git_write']=write('.git/config')
r['unapproved_write']=write('docs/unapproved.md')
r['outside_write']=write('/etc/passwd')
r['scratch_write']=write('/tmp/build-probe.txt')
for k,p in [('source_secret','/work/fixture.pem'),('git_secret','/work/.git/fixture-key')]:
 try:r[k]=Path(p).read_text()==sys.argv[1]
 except OSError:r[k]=False
for k,p in [('secret_read','/private/codex/secret'),('host_read','/mnt/c/Windows/win.ini'),('sibling_read','/var/lib/nginx-qa-worker/sibling-secret')]:
 try:Path(p).read_bytes();r[k]=True
 except OSError:r[k]=False
r['interop']=Path('/proc/sys/fs/binfmt_misc/WSLInterop').exists()
r['socket_attempted']=True
s=None
try:s=socket.socket();s.settimeout(.5);s.connect(('127.0.0.1',int(sys.argv[2])));s.sendall(sys.argv[1].encode());r['socket_allowed']=True
except OSError:r['socket_allowed']=False
finally:
 if s:s.close()
r['external_socket_attempted']=True
s=None
try:s=socket.socket();s.settimeout(.5);s.connect(('1.1.1.1',443));r['external_socket_allowed']=True
except OSError:r['external_socket_allowed']=False
finally:
 if s:s.close()
print(json.dumps(r))
'''


def check(request):
    profile_id=request.get('profile_id','docs-only-v1')
    if profile_id not in ('docs-only-v1','java-maven-v1'):raise ValueError('CLIENT_PROJECT_PROFILE_FORBIDDEN')
    if profile_id=='java-maven-v1':
        for p in (Path('/opt/nginx-qa-worker/java/bin/java'),Path('/opt/nginx-qa-worker/java/bin/javac'),Path('/usr/bin/mvn'),Path('/opt/nginx-qa-worker/maven-repository')):
            if not p.exists():raise ValueError('WSL_MAVEN_TOOLCHAIN_UNAVAILABLE')
            s=p.stat()
            if s.st_uid!=0 or s.st_mode&0o022:raise ValueError('WSL_MAVEN_TOOLCHAIN_UNTRUSTED')
    evidence=[]
    for review in (False,True):
        attempt,lock=new_attempt(hashlib.sha256((request['job']+str(review)).encode()).hexdigest())
        nonce=uuid.uuid4().hex
        try:
            (attempt/'work/docs').mkdir(parents=True);(attempt/'work/.git').mkdir()
            (attempt/'work/README.md').write_text(nonce)
            (attempt/'work/docs/probe.md').write_text('unchanged')
            (attempt/'work/.git/config').write_text('unchanged')
            if profile_id=='java-maven-v1':
                (attempt/'work/fixture.pem').write_text(nonce)
                (attempt/'work/.git/fixture-key').write_text(nonce)
            guard_mountpoints(attempt/'work',['docs/probe.md'])
            (attempt/'private/codex/secret').write_text(nonce)
            for p in (attempt/'work').rglob('*'):os.chown(p,UID,UID)
            os.chown(attempt/'work',UID,UID)
            (attempt/'config/requirements.toml').write_text(requirements(['docs/probe.md'],review,profile_id))
            listener=socket.socket();listener.bind(('127.0.0.1',0));listener.listen(4)
            # Control connects before running the real denied socket operation.
            control=socket.create_connection(listener.getsockname(),timeout=2)
            conn,_=listener.accept();conn.close();control.close()
            listener.setblocking(False)
            argv=[CLI,'sandbox','--include-managed-config','-P','nq-managed','-c','approval_policy="never"','-C','/work',
                  '--','/usr/bin/python3','-c',CANARY,nonce,str(listener.getsockname()[1])]
            code,out,err=run_bounded(attempt,['docs/probe.md'],review,argv,timeout=20,profile_id=profile_id)
            try:conn,_=listener.accept();conn.close();received=True
            except BlockingIOError:received=False
            finally:listener.close()
            if code:raise ValueError('WSL_BOUNDARY_COMMAND_FAILED')
            item=json.loads(out)
            if profile_id=='java-maven-v1' and (item['scratch_write'] is not True or item['source_secret'] or item['git_secret']):
                raise ValueError('WSL_CODING_BOUNDARY_UNPROVEN')
            if (item['nonce']!=nonce or item['uid']!=UID or item['read'] is not True
                    or item['doc_write'] is not (not review) or received
                    or any(item[k] for k in ('git_write','unapproved_write','outside_write','secret_read','host_read','sibling_read','interop','socket_allowed','external_socket_allowed'))
                    or item['socket_attempted'] is not True or item['external_socket_attempted'] is not True
                    or (attempt/'work/.git/config').read_text()!='unchanged'):
                raise ValueError('WSL_BOUNDARY_UNPROVEN')
            evidence.append({'role':'review' if review else 'author',**item,'quiescent':True,'loopback_control_reachable':True})
            if profile_id=='java-maven-v1':
                code,out,err=run_bounded(attempt,['docs/probe.md'],review,[CLI,'sandbox','--include-managed-config','-P','nq-managed',
                    '-c','approval_policy="never"','-C','/work','--','/usr/bin/mvn','-version'],timeout=20,profile_id=profile_id)
                if code or b'Apache Maven' not in out+err or b'Java version: 1.8' not in out+err:
                    raise ValueError('WSL_MAVEN_JDK8_REQUIRED')
            if not review:
                auth=request.get('auth')
                if not isinstance(auth,dict) or auth.get('auth_mode')!='chatgpt' or not auth.get('tokens'):
                    raise ValueError('WSL_EXISTING_CHATGPT_LOGIN_REQUIRED')
                p=attempt/'private/codex/auth.json';p.write_text(json.dumps(auth),encoding='utf-8')
                os.chmod(p,0o600);os.chown(p,UID,UID)
                code,out,err=run_bounded(attempt,['docs/probe.md'],True,[CLI,'login','status'],timeout=15)
                if code or b'Logged in' not in out+err:raise ValueError('WSL_EXISTING_LOGIN_UNAVAILABLE')
        finally:
            (attempt/'private/codex/auth.json').unlink(missing_ok=True)
            lock.close()
    return {'state':'PASS','backend':'wsl2-bwrap','profile_id':profile_id,'boundary':'native-canary-verified','existing_login':True,'evidence':evidence}


def run(request):
    profile_id=request.get('profile_id','docs-only-v1')
    if profile_id not in ('docs-only-v1','java-maven-v1'):raise ValueError('CLIENT_PROJECT_PROFILE_FORBIDDEN')
    max_file=MAX_FILE if profile_id=='docs-only-v1' else 8*1024*1024
    max_bundle=MAX_BUNDLE if profile_id=='docs-only-v1' else 64*1024*1024
    paths=request['allowed_paths']
    if not isinstance(paths,list) or not 1<=len(paths)<=(32 if profile_id=='docs-only-v1' else 128) or len(set(paths))!=len(paths):raise ValueError('WSL_PATHS_INVALID')
    for path in paths:safe_path(path,profile_id)
    review=request['schema'].get('title')=='review'
    if request['schema'].get('title') not in ('review','author'):raise ValueError('WSL_ROLE_INVALID')
    attempt,lock=new_attempt(request['job'])
    try:
        bundle=base64.b64decode(request['bundle'],validate=True)
        if len(bundle)>max_bundle:raise ValueError('WSL_BUNDLE_LIMIT')
        (attempt/'source.bundle').write_bytes(bundle)
        command(['git','-c','core.hooksPath=/nonexistent','clone','--no-checkout',str(attempt/'source.bundle'),str(attempt/'work')],attempt=attempt)
        if not re.fullmatch('[0-9a-f]{40}|[0-9a-f]{64}',request['head']):raise ValueError('WSL_HEAD_INVALID')
        command(['git','-c','core.hooksPath=/nonexistent','-C',str(attempt/'work'),'checkout','--detach',request['head']],attempt=attempt)
        entries=command(['git','-C',str(attempt/'work'),'ls-files','--stage','-z'],attempt=attempt).split(b'\0')
        if len(entries)>4097 or any(e and e.split()[0] not in (b'100644',b'100755') for e in entries):
            raise ValueError('WSL_SOURCE_NOT_REGULAR')
        before={}
        for p in paths:
            f=attempt/'work'/p
            if f.exists() and (not f.is_file() or f.is_symlink() or f.stat().st_size>max_file):raise ValueError('WSL_DOCUMENT_INVALID')
            before[p]=f.read_bytes() if f.exists() else None
            if not review:f.parent.mkdir(parents=True,exist_ok=True);f.touch(exist_ok=True)
        if not review:guard_mountpoints(attempt/'work',paths)
        for p in (attempt/'work').rglob('*'):
            if p.is_symlink():raise ValueError('WSL_SOURCE_ALIAS')
            os.chown(p,UID,UID)
        os.chown(attempt/'work',UID,UID)
        if len(request['prompt'].encode())>256000:raise ValueError('WSL_PROMPT_LIMIT')
        (attempt/'task/prompt.txt').write_text(request['prompt'],encoding='utf-8')
        (attempt/'task/schema.json').write_text(json.dumps(request['schema']),encoding='utf-8')
        if profile_id=='java-maven-v1':
            diff=request.get('source_diff','')
            if not isinstance(diff,str) or len(diff.encode())>2*1024*1024:raise ValueError('WSL_DIFF_LIMIT')
            (attempt/'task/source.diff').write_text(diff,encoding='utf-8')
        for p in (attempt/'task').iterdir():os.chown(p,UID,UID)
        auth=request['auth']
        if not isinstance(auth,dict) or auth.get('auth_mode')!='chatgpt' or not auth.get('tokens'):
            raise ValueError('WSL_EXISTING_CHATGPT_LOGIN_REQUIRED')
        auth_path=attempt/'private/codex/auth.json'
        auth_path.write_text(json.dumps(auth),encoding='utf-8');os.chmod(auth_path,0o600);os.chown(auth_path,UID,UID)
        (attempt/'config/requirements.toml').write_text(requirements(paths,review,profile_id))
        argv=[CLI,'exec','--ignore-user-config','--ignore-rules','--strict-config','--ephemeral','--json',
              '-c','approval_policy="never"','-c','default_permissions="nq-managed"','-c','web_search="disabled"',
              '-c','allow_login_shell=false','-C','/work','--output-schema','/task/schema.json','-o','/private/result.json']
        for key in DISABLED:argv += ['--disable',key]
        argv += ['Read /task/prompt.txt and perform exactly that one native assignment. Return only the required structured result.']
        code,out,err=run_bounded(attempt,paths,review,argv,timeout=min(max(int(request['timeout']),30),1800),profile_id=profile_id)
        if code:raise ValueError('WSL_MODEL_EXECUTION_FAILED')
        result_path=attempt/'private/result.json'
        if not result_path.is_file() or result_path.stat().st_size>65536:raise ValueError('WSL_MODEL_RESULT_INVALID')
        output=json.loads(result_path.read_bytes())
        changes={}
        changed_bytes=0
        for p in paths:
            f=attempt/'work'/p
            if not f.exists():
                if before[p] is not None:raise ValueError('WSL_DOCUMENT_DELETED')
                continue
            st=f.lstat()
            if not stat.S_ISREG(st.st_mode) or st.st_nlink!=1 or st.st_size>max_file:raise ValueError('WSL_DOCUMENT_INVALID')
            content=f.read_bytes()
            if before[p] is None and content==b'':continue
            if before[p]!=content:
                if review:raise ValueError('WSL_REVIEW_MUTATED')
                changed_bytes+=len(content)
                if changed_bytes>(20*1024*1024 if profile_id=='java-maven-v1' else 2*1024*1024):
                    raise ValueError('WSL_CHANGE_SET_LIMIT')
                changes[p]=base64.b64encode(content).decode()
        return {'state':'COMPLETED','output':output,'changes':changes,'quiescent':True,'job':request['job']}
    finally:
        # Only the fixed auth file in this freshly created owned attempt is removed.
        (attempt/'private/codex/auth.json').unlink(missing_ok=True)
        lock.close()


def stop(request):
    job=request['job']
    if not re.fullmatch('[0-9a-f]{64}',job):raise ValueError('WSL_JOB_ID_INVALID')
    ROOT.mkdir(exist_ok=True,mode=0o711)
    # Permanent generation fence precedes scanning, including a run that has not
    # reached clone/fork yet. The native client cannot silently rerun this job.
    write_json(ROOT/(job+'.cancel'),{'cancelled':True,'at':time.time()})
    lock=(ROOT/(job+'.lock')).open('a+b');deadline=time.monotonic()+12
    try:
        while True:
            paths=[p for c in ('memory','pids','cpu') for p in (Path('/sys/fs/cgroup')/c).glob('nginx-qa-'+job+'-*')]
            kill_members(paths)
            try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);break
            except BlockingIOError:
                if time.monotonic()>deadline:raise ValueError('WSL_QUIESCENCE_UNPROVEN')
                time.sleep(.03)
        # Holding the job lock plus the cancellation fence rules out future
        # launch. Derive cgroups even when no post-fork active.json was written.
        paths=[p for c in ('memory','pids','cpu') for p in (Path('/sys/fs/cgroup')/c).glob('nginx-qa-'+job+'-*')]
        if group_pids(paths):raise ValueError('WSL_QUIESCENCE_UNPROVEN')
        for p in paths:p.rmdir()
        for attempt in ROOT.glob(job+'-*'):
            if not attempt.is_dir():continue
            (attempt/'active.json').unlink(missing_ok=True)
            (attempt/'private/codex/auth.json').unlink(missing_ok=True)
            write_json(attempt/'retired.json',{'quiescent':True,'cancelled':True,'at':time.time()})
        return {'state':'STOPPED','job':job,'quiescent':True}
    finally:lock.close()


def admit():
    ROOT.mkdir(exist_ok=True,mode=0o711)
    lock=(ROOT/'execution.lock').open('a+b')
    try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:lock.close();raise ValueError('WSL_EXECUTOR_BUSY') from None
    paths=[p for c in ('memory','pids','cpu') for p in (Path('/sys/fs/cgroup')/c).glob('nginx-qa-*')]
    if group_pids(paths):
        lock.close();raise ValueError('WSL_PREVIOUS_WORK_UNRETIRED')
    # No live process and an exclusive executor lock prove empty leftovers safe.
    for p in paths:p.rmdir()
    return lock


def main():
    try:
        raw=sys.stdin.buffer.readline(MAX_REQUEST+1)
        if len(raw)>MAX_REQUEST or not raw.endswith(b'\n'):raise ValueError('WSL_REQUEST_LIMIT')
        request=json.loads(raw)
        verify_runtime(request)
        operation=request.get('operation')
        if operation in ('check','run'):
            admission=admit()
            try:response=check(request) if operation=='check' else run(request)
            finally:admission.close()
        elif operation=='stop':response=stop(request)
        else:raise ValueError('WSL_OPERATION_INVALID')
        print(json.dumps(response),flush=True)
        return 0
    except Exception as exc:
        error=str(exc) if re.fullmatch('[A-Z0-9_]{1,80}',str(exc)) else 'WSL_SUPERVISOR_FAILED'
        print(json.dumps({'state':'BLOCKED','error':error}),flush=True)
        return 2


if __name__=='__main__':raise SystemExit(main())
