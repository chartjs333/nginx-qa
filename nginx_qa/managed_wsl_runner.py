"""Windows controller adapter for the separately provisioned Linux sandbox.

Only the existing native client invokes this runner. No assignment, graph,
credential or Git publication endpoint is exposed by the Linux executor.
"""
from __future__ import annotations
import base64
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import stat
import subprocess
import tempfile
import threading
import time
import jsonschema

from .managed_codex_client import CodexRunner
from .setup_worker_adapter import strict_object, atomic_json

MAX_BUNDLE=32*1024*1024
MAX_FILE=1024*1024


def document_path(value):
    if (not isinstance(value,str) or len(value)>240 or not re.fullmatch(r'docs/[A-Za-z0-9_./-]+\.md',value)
            or any(p in ('','.','..') for p in value.split('/'))):
        raise ValueError('WSL_DOCUMENT_PATH_INVALID')
    return value


def regular_destination(root, relative):
    path=root/document_path(relative)
    current=root
    for part in Path(relative).parts:
        current=current/part
        if current.is_symlink() or (hasattr(current,'is_junction') and current.is_junction()):
            raise ValueError('WSL_DESTINATION_ALIAS')
    if path.exists():
        s=path.stat()
        if not stat.S_ISREG(s.st_mode) or s.st_nlink!=1 or s.st_size>MAX_FILE:
            raise ValueError('WSL_DESTINATION_INVALID')
    return path


class WslCodexRunner(CodexRunner):
    def __init__(self, config):
        if os.name!='nt':raise ValueError('WSL_WINDOWS_CONTROLLER_REQUIRED')
        self.distro=config.get('wsl_distribution','')
        if not re.fullmatch(r'NginxQaWorker-[A-Za-z0-9-]{1,50}',self.distro):
            raise ValueError('WSL_OWNED_DISTRO_REQUIRED')
        self.supervisor=config.get('wsl_supervisor','/opt/nginx-qa-worker/supervisor.py')
        if self.supervisor!='/opt/nginx-qa-worker/supervisor.py':raise ValueError('WSL_SUPERVISOR_PATH_INVALID')
        self.cli_sha=config.get('wsl_cli_sha256','');self.supervisor_sha=config.get('wsl_supervisor_sha256','')
        if not all(re.fullmatch('[0-9a-f]{64}',x) for x in (self.cli_sha,self.supervisor_sha)):
            raise ValueError('WSL_RUNTIME_HASHES_REQUIRED')
        self.runtime_hashes=config.get('wsl_runtime_hashes',{})
        if (set(self.runtime_hashes)!={'codex','codex-code-mode-host','rg','codex-resources/bwrap'}
                or any(not re.fullmatch('[0-9a-f]{64}',str(v)) for v in self.runtime_hashes.values())):
            raise ValueError('WSL_RUNTIME_MANIFEST_REQUIRED')
        self.auth_path=Path(config.get('wsl_auth_file',str(Path.home()/'.codex/auth.json'))).resolve(strict=True)
        self.executable=str(Path(os.environ.get('SystemRoot','C:/Windows'))/'System32/wsl.exe')
        self.timeout=min(max(config.get('role_timeout_seconds',600),30),1800)
        self.environment=self.child_environment()
        self._slots=threading.BoundedSemaphore(1)
        self._quarantined_job=None
        self.sandbox_diagnostic={'state':'NOT_CHECKED'}

    def _auth(self):
        if self.auth_path.stat().st_size>65536:raise ValueError('WSL_AUTH_INVALID')
        auth=strict_object(self.auth_path.read_bytes())
        if auth.get('auth_mode')!='chatgpt' or not isinstance(auth.get('tokens'),dict):
            raise ValueError('WSL_EXISTING_CHATGPT_LOGIN_REQUIRED')
        # The worker receives no refresh authority. Copying a live refresh token
        # into an independent guest could rotate it behind the Windows account.
        # Each invocation uses a fresh, short-lived access snapshot; expiry/401
        # must block, never silently refresh or overwrite the user's login.
        token=auth['tokens'].get('access_token','')
        try:
            part=token.split('.')[1]
            expiry=json.loads(base64.urlsafe_b64decode(part+'='*(-len(part)%4)))['exp']
            if not isinstance(expiry,(int,float)) or expiry<time.time()+self.timeout+120:raise ValueError()
        except (ValueError,KeyError,IndexError,TypeError):
            raise ValueError('WSL_ACCESS_SNAPSHOT_EXPIRED') from None
        snapshot=deepcopy(auth)
        snapshot['tokens']['refresh_token']=''
        return snapshot

    @staticmethod
    def job_id(name):return hashlib.sha256(name.encode()).hexdigest()

    def _exchange(self, operation, job, *, stop=None, **body):
        request={'operation':operation,'job':job,'cli_sha256':self.cli_sha,
                 'supervisor_sha256':self.supervisor_sha,'runtime_hashes':self.runtime_hashes,**body}
        raw=(json.dumps(request,separators=(',',':'))+'\n').encode()
        if len(raw)>48*1024*1024:raise ValueError('WSL_REQUEST_LIMIT')
        p=None;error=None;finished=threading.Event();writer_errors=[]
        with tempfile.TemporaryFile() as output,tempfile.TemporaryFile() as stderr:
            try:
                # Read, verify and execute the same immutable bytes, before any
                # supervisor code runs. Operator-controlled root files are trusted.
                loader=('import hashlib,os,sys;from pathlib import Path;'
                    'p=Path("/opt/nginx-qa-worker/supervisor.py");s=p.stat();raw=p.read_bytes();'
                    'assert s.st_uid==0 and not s.st_mode&18 and hashlib.sha256(raw).hexdigest()==sys.argv[1];'
                    'exec(compile(raw,str(p),"exec"),{"__name__":"__main__","__file__":str(p)})')
                p=subprocess.Popen([self.executable,'-d',self.distro,'-u','root','--exec',
                    '/usr/bin/python3','-I','-c',loader,self.supervisor_sha],stdin=subprocess.PIPE,
                    stdout=output,stderr=stderr,env=self.environment,
                    creationflags=subprocess.CREATE_NO_WINDOW,close_fds=True)
                def writer():
                    try:
                        p.stdin.write(raw);p.stdin.flush()
                        while not finished.wait(1):
                            if stop is not None and stop.is_set():break
                            p.stdin.write(b'\n');p.stdin.flush()
                    except (OSError,ValueError):writer_errors.append(True)
                    finally:
                        try:p.stdin.close()
                        except OSError:pass
                thread=threading.Thread(target=writer,daemon=True);thread.start()
                deadline=time.monotonic()+(self.timeout+20 if operation=='run' else 65)
                while p.poll() is None:
                    if stop is not None and stop.is_set():
                        error='CLIENT_STOPPING';finished.set()
                    if time.monotonic()>deadline or os.fstat(output.fileno()).st_size>4*1024*1024 or os.fstat(stderr.fileno()).st_size>131072:
                        error='WSL_TRANSPORT_LIMIT';finished.set();break
                    time.sleep(.05)
                finished.set();thread.join(timeout=2)
                if p.poll() is None:
                    # EOF gives the Linux watchdog time to retire the PID namespace.
                    try:p.wait(timeout=12)
                    except subprocess.TimeoutExpired:p.kill();p.wait(timeout=5);error='WSL_QUIESCENCE_UNPROVEN'
                output.seek(0);response=strict_object(output.read(4*1024*1024+1))
                if error:raise ValueError(error)
                if p.returncode or response.get('state')=='BLOCKED':
                    code=response.get('error','WSL_EXECUTION_FAILED')
                    raise ValueError(code if re.fullmatch('[A-Z0-9_]{1,80}',str(code)) else 'WSL_EXECUTION_FAILED')
                return response
            finally:
                finished.set()
                if p and p.poll() is None:p.kill();p.wait(timeout=5)

    def ready(self, *, stop=None):
        try:
            if self._quarantined_job is not None:self.quiesce(self._quarantined_job)
            result=self._exchange('check',secrets.token_hex(32),stop=stop,auth=self._auth())
            if result.get('state')!='PASS' or result.get('backend')!='wsl2-bwrap':raise ValueError('WSL_BOUNDARY_UNPROVEN')
            self.sandbox_diagnostic=result
        except Exception as exc:
            code=str(exc) if re.fullmatch('[A-Z0-9_]{1,80}',str(exc)) else 'WSL_PREFLIGHT_FAILED'
            self.sandbox_diagnostic={'state':'BLOCKED','error':'CODEX_SANDBOX_UNAVAILABLE','reason':code,
                'guidance':['Verify the separately owned distro and pinned supervisor/CLI, then repeat --check. No backend fallback is used.']}
            raise ValueError('CODEX_SANDBOX_UNAVAILABLE') from None

    def quiesce(self, job_name):
        result=self._exchange('stop',self.job_id(job_name))
        if result.get('state')!='STOPPED' or result.get('quiescent') is not True:
            raise ValueError('CLIENT_QUIESCENCE_UNPROVEN')
        if self._quarantined_job==job_name:
            self._quarantined_job=None
            self._slots.release()

    def run(self, *, cwd,prompt,schema,directory,job_name,stop,allowed_paths=None):
        if stop.is_set():raise ValueError('CLIENT_STOPPING')
        paths=list(allowed_paths or [])
        if not 1<=len(paths)<=32 or len(set(paths))!=len(paths):raise ValueError('WSL_DOCUMENT_ALLOWLIST_REQUIRED')
        for p in paths:document_path(p)
        self.ready(stop=stop)
        while not self._slots.acquire(timeout=.1):
            if stop.is_set():raise ValueError('CLIENT_STOPPING')
        try:
            root=Path(cwd).resolve(strict=True);directory.mkdir(parents=True,exist_ok=True)
            destinations={p:regular_destination(root,p) for p in paths}
            before={p:f.read_bytes() if f.exists() else None for p,f in destinations.items()}
            def git(*args):
                r=self._bounded_local([shutil.which('git') or 'git','--no-replace-objects','-c','core.hooksPath='+str(directory/'empty-hooks'),
                    '-C',str(root),*args],root,timeout=60,stop=stop)
                if r.returncode:raise ValueError('WSL_GIT_EXPORT_FAILED')
                # The inherited helper returns at most cap+1 bytes. A fast
                # command may finish between monitor ticks; never interpret a
                # truncated object listing as a complete expansion-size proof.
                if len(r.stdout)>131072 or len(r.stderr)>131072:
                    raise ValueError('WSL_GIT_EXPORT_OUTPUT_LIMIT')
                return r.stdout
            head=git('rev-parse','HEAD').decode().strip()
            # A small current tree and compressed bundle do not bound historical
            # object expansion. Inspect all stored object headers before export;
            # rejecting unrelated unreachable objects is intentionally conservative.
            sizes=git('cat-file','--batch-all-objects','--batch-check=%(objecttype) %(objectsize)').splitlines()
            object_total=0
            if len(sizes)>8192:raise ValueError('WSL_OBJECT_EXPANSION_LIMIT')
            for entry in sizes:
                kind,size=entry.split();size=int(size)
                if kind not in (b'blob',b'tree',b'commit',b'tag') or size>MAX_FILE:
                    raise ValueError('WSL_OBJECT_EXPANSION_LIMIT')
                object_total+=size
            if object_total>MAX_BUNDLE:raise ValueError('WSL_OBJECT_EXPANSION_LIMIT')
            # Reject nonregular tracked entries and excessive expansion before
            # exporting any object into the guest; model-owned Git is never imported.
            tree=git('ls-tree','-rlz','HEAD').split(b'\0')
            total=0
            for entry in filter(None,tree):
                meta,name=entry.split(b'\t',1);mode,kind,oid,size=meta.split()
                if mode not in (b'100644',b'100755') or kind!=b'blob' or int(size)>MAX_FILE:
                    raise ValueError('WSL_SOURCE_NOT_REGULAR_OR_BOUNDED')
                total+=int(size)
            if len(tree)>4097 or total>MAX_BUNDLE:raise ValueError('WSL_SOURCE_LIMIT')
            bundle_path=directory/'source.bundle'
            git('bundle','create',str(bundle_path),'HEAD')
            if bundle_path.stat().st_size>MAX_BUNDLE:raise ValueError('WSL_BUNDLE_LIMIT')
            atomic_json(directory/'wsl-attempt.json',{'job':self.job_id(job_name),'head':head,'distro':self.distro,
                'allowed_paths':paths,'role':schema['title'],'bundle_sha256':hashlib.sha256(bundle_path.read_bytes()).hexdigest()})
            result=self._exchange('run',self.job_id(job_name),stop=stop,
                bundle=base64.b64encode(bundle_path.read_bytes()).decode(),head=head,
                prompt=prompt,schema=schema,allowed_paths=paths,timeout=self.timeout,auth=self._auth())
            if result.get('state')!='COMPLETED' or result.get('quiescent') is not True or result.get('job')!=self.job_id(job_name):
                raise ValueError('WSL_RESULT_UNPROVEN')
            output=result['output'];jsonschema.Draft202012Validator(schema).validate(output)
            changes=result.get('changes')
            if not isinstance(changes,dict) or not set(changes).issubset(paths) or (schema['title']=='review' and changes):
                raise ValueError('WSL_CHANGE_SET_INVALID')
            decoded={p:base64.b64decode(v,validate=True) for p,v in changes.items()}
            if any(len(v)>MAX_FILE for v in decoded.values()):raise ValueError('WSL_DOCUMENT_LIMIT')
            if stop.is_set():raise ValueError('CLIENT_STOPPING')
            if git('rev-parse','HEAD').decode().strip()!=head:raise ValueError('WSL_SOURCE_CHANGED')
            for p,f in destinations.items():
                regular_destination(root,p)
                if (f.read_bytes() if f.exists() else None)!=before[p]:raise ValueError('WSL_DESTINATION_CHANGED')
            for p,content in decoded.items():
                f=destinations[p];f.parent.mkdir(parents=True,exist_ok=True);regular_destination(root,p)
                f.write_bytes(content)
            atomic_json(directory/'wsl-result.json',{'job':result['job'],'quiescent':True,'output':output,
                'changed_paths':sorted(changes)})
            return output
        finally:
            try:self.quiesce(job_name)
            except Exception:
                self._quarantined_job=job_name
                # Retain admission until actual retirement is established. On
                # service restart Linux admission rejects all live old cgroups.
                raise
            else:self._slots.release()
