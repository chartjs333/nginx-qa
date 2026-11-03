"""One engine lifespan with separate private and narrow public loopback listeners."""
from __future__ import annotations

import asyncio
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import signal
import socket
import subprocess
import sys
import uuid

from nginx_qa.control_secrets import sanitized_child_environment
from nginx_qa.managed_import import parse_strict_json_object
from nginx_qa.workspace_manager import ManagedWorkspaceManager


def _path(value):
    candidate=Path(value)
    if not candidate.is_absolute() or not candidate.exists():
        raise ValueError('Gateway roots must be existing absolute paths')
    absolute=Path(os.path.abspath(candidate))
    if os.path.normcase(str(absolute))!=os.path.normcase(str(absolute.resolve(strict=True))):
        raise ValueError('Gateway roots cannot contain aliases')
    for component in (absolute,*absolute.parents):
        info=component.lstat()
        if component.is_symlink() or int(getattr(info,'st_file_attributes',0))&0x400:
            raise ValueError('Gateway paths cannot contain reparse points')
    return absolute


def _same_path(first,second):
    return os.path.normcase(os.path.abspath(first))==os.path.normcase(os.path.abspath(second))


def validate_gateway_configuration(environment=None,*,code_root=None,verify_git=True):
    """No listeners or main import until exact owned paths/provenance pass."""
    env=os.environ if environment is None else environment
    root=_path(code_root or Path(__file__).resolve().parents[1])
    service=_path(env['NGINX_QA_SERVICE_ROOT'])
    state=_path(env['NGINX_QA_STAGING_STATE_BASE'])
    if service!=root or state==root or state in root.parents or root in state.parents:
        raise ValueError('Gateway service/state roots are not isolated')
    protected=json.loads(env['NGINX_QA_PROTECTED_ROOTS'])
    if not isinstance(protected,list) or not protected or any(not isinstance(path,str) or not Path(path).is_absolute() for path in protected):
        raise ValueError('Gateway protected roots must be explicit')
    ManagedWorkspaceManager.validate_isolated_root(str(state),protected_roots=tuple(protected)+(str(root),))
    marker=_path(state/'.nginx-qa-staging-owner.json')
    if not marker.is_file() or marker.stat().st_size>32768:
        raise ValueError('Gateway ownership marker is invalid')
    proof=parse_strict_json_object(marker.read_bytes())
    if proof.get('schema_version')!=1 or str(uuid.UUID(proof['initialization_id']))!=proof['initialization_id']:
        raise ValueError('Gateway initialization identity is invalid')
    owner=proof['ownership']
    if not isinstance(owner,dict):
        raise ValueError('Gateway ownership is invalid')
    fields={'service_root':'NGINX_QA_SERVICE_ROOT','state_base':'NGINX_QA_STAGING_STATE_BASE',
        'venv_root':'NGINX_QA_STAGING_VENV_ROOT','runtime_root':'NGINX_QA_RUNTIME_ROOT',
        'prompt_root':'NGINX_QA_PROMPT_ROOT','managed_root':'NGINX_QA_MANAGED_ROOT','base_python':'NGINX_QA_STAGING_BASE_PYTHON'}
    mutable=[]
    for field,setting in fields.items():
        actual=_path(env[setting])
        if not _same_path(owner[field],actual):
            raise ValueError('Gateway ownership path differs from environment')
        if field in {'runtime_root','prompt_root','managed_root','venv_root'}:
            if actual.parent!=state or actual in mutable:
                raise ValueError('Gateway mutable roots must be distinct direct children of its state base')
            mutable.append(actual)
    for field,setting in (('instance_id','NGINX_QA_INSTANCE_ID'),('child_port_range','NGINX_QA_CHILD_PORT_RANGE'),
                          ('http_host','NGINX_QA_HTTP_HOST'),('gateway_host','NGINX_QA_GATEWAY_HOST')):
        if owner[field]!=env[setting]:
            raise ValueError('Gateway ownership setting differs from environment')
    if owner['http_host']!='127.0.0.1' or owner['gateway_host']!='127.0.0.1':
        raise ValueError('Both gateway listeners must bind explicit loopback')
    private,public=int(env['NGINX_QA_HTTP_PORT']),int(env['NGINX_QA_GATEWAY_PORT'])
    start,end=map(int,env['NGINX_QA_CHILD_PORT_RANGE'].split('-'))
    if (not 1024<=private<=65535 or not 1024<=public<=65535 or private==public
        or not 1024<=start<=end<=65535 or start<=private<=end or start<=public<=end
        or owner['http_port']!=private or owner['gateway_port']!=public):
        raise ValueError('Gateway ports are not independently owned')
    origin_state=state/'legacy'/'runtime_state'/'launcher-origin.json'
    _path(origin_state.parent)
    if not _same_path(env['NGINX_QA_LAUNCHER_ORIGIN_STATE'],origin_state) or not _same_path(owner['gateway_origin_state'],origin_state):
        raise ValueError('Gateway origin state must be root-local and owned')
    if origin_state.exists():
        _path(origin_state)
    if [_path_key(value) for value in owner['protected_roots']]!=[_path_key(value) for value in protected]:
        raise ValueError('Gateway protected root ownership changed')
    if not _same_path(sys.prefix,owner['venv_root']) or not _same_path(sys.base_prefix,owner['base_python_prefix']):
        raise ValueError('Gateway must use the exact owned virtual environment')
    if (not _same_path(_path(sys.executable).parent.parent,owner['venv_root'])
        or not _same_path(getattr(sys,'_base_executable',sys.executable),owner['base_python'])):
        raise ValueError('Gateway interpreter executable differs from its ownership')
    if owner['base_python_version']!=platform.python_version() or hashlib.sha256(Path(env['NGINX_QA_STAGING_BASE_PYTHON']).read_bytes()).hexdigest()!=owner['base_python_sha256']:
        raise ValueError('Gateway interpreter provenance changed')
    if not re.fullmatch(r'[0-9a-f]{40}',owner['approved_commit']) or owner['origin']!='https://github.com/chartjs333/nginx-qa.git':
        raise ValueError('Gateway source provenance is invalid')
    if verify_git:
        def git(*args):
            return subprocess.run(['git','-C',str(root),*args],env=sanitized_child_environment(),
                stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,encoding='utf-8',check=True,timeout=30).stdout.strip()
        if (git('rev-parse','HEAD')!=owner['approved_commit'] or git('branch','--show-current')!=owner['branch']
            or git('remote','get-url','origin')!=owner['origin'] or git('status','--porcelain','--untracked-files=all')):
            raise ValueError('Gateway checkout differs from the approved clean source')
    return {'private_port':private,'public_port':public,'state_base':state,'origin_state':origin_state,
            'initialization_id':proof['initialization_id'],'ownership':owner}


def _path_key(value):
    return os.path.normcase(os.path.abspath(value))


def reserve_listeners(private_port,public_port):
    sockets=[]
    try:
        for port in (private_port,public_port):
            sock=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            sockets.append(sock)
            if hasattr(socket,'SO_EXCLUSIVEADDRUSE'):
                sock.setsockopt(socket.SOL_SOCKET,socket.SO_EXCLUSIVEADDRUSE,1)
            sock.bind(('127.0.0.1',port))
            sock.listen(128)
            sock.setblocking(False)
        return sockets
    except BaseException:
        for sock in sockets:sock.close()
        raise


async def serve_pair(private_app,public_app,sockets,stop_event,*,server_factory=None):
    # A request to cancel the host means graceful common shutdown. It must not
    # cancel to_thread wrappers or a lifespan's cleanup while real work exists.
    task=asyncio.create_task(_serve_pair(private_app,public_app,sockets,stop_event,server_factory=server_factory))
    cancelled=False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled=True
            stop_event.set()
    result=task.result()
    if cancelled:raise asyncio.CancelledError
    return result


async def _serve_pair(private_app,public_app,sockets,stop_event,*,server_factory=None):
    from nginx_qa.launcher_gateway import drain_gateway
    import uvicorn
    class OwnedServer(uvicorn.Server):
        @contextmanager
        def capture_signals(self):
            yield  # The single host owns signal handling for both listeners.
        def install_signal_handlers(self):
            pass  # Older supported uvicorn releases use this hook.
    factory=server_factory or OwnedServer
    servers=[factory(uvicorn.Config(app,host='127.0.0.1',port=sock.getsockname()[1],lifespan='off',
        access_log=False,proxy_headers=False,server_header=False,timeout_graceful_shutdown=None,
        limit_concurrency=64,ws='none' if index else 'auto'))
        for index,(app,sock) in enumerate(zip((private_app,public_app),sockets))]
    async def run(server,sock):
        try:
            await server.serve(sockets=[sock])
        except BaseException as error:
            # Convert a server's SystemExit into an observed task failure; it
            # cannot tear down the event loop and orphan unrelated engine work.
            if isinstance(error,asyncio.CancelledError):raise
            raise RuntimeError('Owned listener failed') from None
    tasks=[]
    try:
        async with private_app.router.lifespan_context(private_app):
            try:
                tasks=[asyncio.create_task(run(server,sock)) for server,sock in zip(servers,sockets)]
                deadline=asyncio.get_running_loop().time()+10
                while not all(server.started for server in servers):
                    if any(task.done() for task in tasks) or asyncio.get_running_loop().time()>=deadline:
                        raise RuntimeError('Both listeners must start before accepting the host as ready')
                    await asyncio.sleep(0.01)
                stopped=asyncio.create_task(stop_event.wait())
                # A failed public listener/tunnel does not stop the private API
                # or engine. Observe its failure now, but wait only for private
                # exit or the operator's common shutdown signal.
                tasks[1].add_done_callback(lambda task:task.exception() if not task.cancelled() else None)
                try:
                    await asyncio.wait((tasks[0],stopped),return_when=asyncio.FIRST_COMPLETED)
                    if tasks[0].done():tasks[0].result()
                finally:
                    stopped.cancel()
                    await asyncio.gather(stopped,return_exceptions=True)
            finally:
                for server in servers:server.should_exit=True
                if tasks:
                    await asyncio.gather(*tasks,return_exceptions=True)
                await drain_gateway(public_app)
    finally:
        for sock in sockets:sock.close()


async def main_async():
    config=validate_gateway_configuration()
    sockets=reserve_listeners(config['private_port'],config['public_port'])
    try:
        # The Python ownership gate runs BEFORE this imports legacy main.
        import staging_host_app
        from nginx_qa.launcher_gateway import create_gateway
        public=create_gateway(staging_host_app._main)
        stop_event=asyncio.Event()
        loop=asyncio.get_running_loop()
        previous={}
        for name in ('SIGINT','SIGTERM','SIGBREAK'):
            selected=getattr(signal,name,None)
            if selected is not None:
                previous[selected]=signal.getsignal(selected)
                signal.signal(selected,lambda *args:loop.call_soon_threadsafe(stop_event.set))
        try:
            await serve_pair(staging_host_app.app,public,sockets,stop_event)
        finally:
            for selected,handler in previous.items():signal.signal(selected,handler)
    finally:
        for sock in sockets:sock.close()


if __name__=='__main__':
    try:
        asyncio.run(main_async())
    except Exception:
        # Startup exceptions may include runtime paths/config; keep public logs
        # free of source bodies and credential values. Operator guards are local.
        print('Launcher host startup/shutdown failed; inspect owned configuration.',file=sys.stderr)
        raise SystemExit(1)
