"""Owned Quick Tunnel adapter for the existing launcher's public listener only.

The monitor retains the native child handle and an existing supervisor Job.
Stop is an owned request, never PID discovery/kill. The engine is independent.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import asdict
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import threading
import time
import uuid

from nginx_qa.control_secrets import control_secret
from nginx_qa.launcher_host import _path, validate_gateway_configuration
from nginx_qa.managed_import import parse_strict_json_object
from nginx_qa.process_supervisor import _atomic_write_json, _WindowsJob, process_identity, _pid_definitely_dead, _spawn_suspended_in_windows_job


def paths(config):
    origin=config['origin_state']
    return origin,origin.with_name('launcher-tunnel-stop.json'),origin.with_name('launcher-tunnel.lock')


def read_receipt(path,config):
    if not path.exists():return None
    _path(path)
    if path.stat().st_size>32768:raise ValueError('Tunnel receipt too large')
    value=parse_strict_json_object(path.read_bytes())
    if (value['schema_version']!=1 or value['initialization_id']!=config['initialization_id']
        or value['instance_id']!=config['ownership']['instance_id']
        or value['upstream']!=f"http://127.0.0.1:{config['public_port']}"):
        raise ValueError('Tunnel receipt belongs to another service')
    return value


def assert_prior_dead(receipt):
    if receipt is None:return
    # Unknown/reused PIDs are deliberately not killed and block automatic reuse.
    for field in ('monitor','child'):
        identity=receipt.get(field)
        if identity and not _pid_definitely_dead(identity['pid']):
            raise RuntimeError('Prior tunnel process death is not proven')


@contextmanager
def exclusive_monitor(path):
    if os.name!='nt':raise RuntimeError('Owned launcher tunnel currently requires Windows')
    import msvcrt
    if path.exists():_path(path)
    with path.open('a+b') as stream:
        if stream.tell()==0:stream.write(b'0');stream.flush()
        stream.seek(0)
        msvcrt.locking(stream.fileno(),msvcrt.LK_NBLCK,1)
        try:yield
        finally:
            stream.seek(0);msvcrt.locking(stream.fileno(),msvcrt.LK_UNLCK,1)


def request_stop(config):
    origin,stop_path,_=paths(config)
    receipt=read_receipt(origin,config)
    if receipt is None or receipt['phase'] in {'STOPPED','FAILED'}:return
    monitor=receipt.get('monitor')
    current=process_identity(monitor['pid']) if monitor else None
    if current is None or asdict(current)!=monitor:
        raise RuntimeError('Tunnel monitor identity is unproven; receipt preserved')
    _atomic_write_json(stop_path,{'initialization_id':config['initialization_id'],
        'generation':receipt['generation'],'monitor':monitor})


def stop_requested(path,receipt):
    if not path.exists():return False
    _path(path)
    if path.stat().st_size>32768:raise ValueError('Invalid tunnel stop request')
    value=parse_strict_json_object(path.read_bytes())
    return (value.get('initialization_id')==receipt['initialization_id']
        and value.get('generation')==receipt['generation'] and value.get('monitor')==receipt['monitor'])


def webhook_registration(origin,previous_origins,*,request=None):
    from configure_telegram import telegram_request
    call=request or telegram_request
    bot=control_secret('bot',allow_legacy=False)
    secret=control_secret('webhook',allow_legacy=False)
    if not bot or not re.fullmatch(r'[A-Za-z0-9_-]{1,256}',secret or ''):
        raise ValueError('Protected bot and webhook credentials are required')
    current=call(bot,'getWebhookInfo',{}).get('result',{}).get('url','')
    target=origin+'/launcher/telegram/webhook'
    if isinstance(previous_origins,str):previous_origins=[previous_origins]
    owned_previous={value+'/launcher/telegram/webhook' for value in (previous_origins or [])}
    if current and current not in owned_previous|{target}:
        raise ValueError('Existing webhook belongs to another integration')
    call(bot,'setWebhook',{'url':target,'secret_token':secret,'drop_pending_updates':'false',
        'allowed_updates':json.dumps(['message','callback_query'])})


def webhook_history(prior):
    prior=prior or {}
    acknowledged=prior.get('registered_origin')
    attempted=prior.get('registration_origins',[])
    if (not isinstance(attempted,list) or len(attempted)>64
        or any(not isinstance(value,str) or not re.fullmatch(r'https://[a-z0-9-]+\.trycloudflare\.com',value) for value in attempted)
        or (acknowledged is not None and acknowledged not in attempted)):
        raise ValueError('Invalid owned webhook history')
    return {'registered_origin':acknowledged,'registration_origins':list(attempted)}


def child_environment():
    # Tunnel needs no project/agent credentials or inherited tunnel configuration.
    return {key:value for key,value in os.environ.items()
            if key.upper() in {'SYSTEMROOT','WINDIR','PATH','TEMP','TMP','COMSPEC','PATHEXT'}}


def monitor(config,executable,*,register_webhook=False,startup_timeout=90,shutdown=None):
    if os.name!='nt':raise RuntimeError('Owned launcher tunnel currently requires Windows')
    if os.getenv('NGINX_QA_LAUNCHER_PUBLIC_ORIGIN'):
        raise ValueError('Use either a named public origin or the owned Quick Tunnel')
    executable=_path(executable)
    if not executable.is_file() or executable.name.lower()!='cloudflared.exe':
        raise ValueError('Provide the installed cloudflared.exe path')
    if not 5<=startup_timeout<=300:raise ValueError('Invalid tunnel startup timeout')
    origin,stop_path,lock=paths(config)
    with exclusive_monitor(lock):
        prior=read_receipt(origin,config);assert_prior_dead(prior)
        identity=process_identity(os.getpid())
        if identity is None:raise RuntimeError('Cannot prove monitor identity')
        generation=uuid.uuid4().hex
        upstream=f"http://127.0.0.1:{config['public_port']}"
        receipt={'schema_version':1,'initialization_id':config['initialization_id'],
            'instance_id':config['ownership']['instance_id'],'generation':generation,'upstream':upstream,
            'monitor':asdict(identity),'child':None,'phase':'STARTING','public_origin':None,
            'heartbeat':time.time(),'webhook':'PENDING' if register_webhook else 'NOT_REQUESTED'}
        receipt.update(webhook_history(prior))
        _atomic_write_json(origin,receipt)
        stop=shutdown or threading.Event()
        previous_handlers={}
        if threading.current_thread() is threading.main_thread():
            for name in ('SIGINT','SIGTERM','SIGBREAK'):
                selected=getattr(signal,name,None)
                if selected is not None:
                    previous_handlers[selected]=signal.getsignal(selected)
                    signal.signal(selected,lambda *args:stop.set())
        job=None;process=None;registration=None
        log_path=origin.with_name('launcher-tunnel-'+generation+'.log')
        config_path=origin.with_name('launcher-tunnel-'+generation+'.yaml')
        config_path.write_text('no-autoupdate: true\n',encoding='utf-8')
        try:
            job=_WindowsJob.create('Local\\nginx-qa-launcher-tunnel-'+generation,
                {'process_count':4,'memory_bytes':256*1024*1024,'cpu_percent':30})
            with log_path.open('xb') as log,ThreadPoolExecutor(max_workers=1,thread_name_prefix='launcher-webhook') as executor:
                command=[str(executable),'tunnel','--config',str(config_path),'--url',upstream,
                    '--no-autoupdate','--metrics','127.0.0.1:0']
                receipt['command']=command
                # Suspended child joins the existing supervisor Job before it
                # can connect. Crash closes the Job and kills its exact members.
                process=_spawn_suspended_in_windows_job(job,command,cwd=str(origin.parent),environment=child_environment(),
                    stdout_stream=log,stderr_stream=log,inherit_recovery_job_handle=False)
                child=process_identity(process.pid)
                if child is None or not job.contains_exact(child):raise RuntimeError('Cannot prove tunnel child identity')
                receipt['child']=asdict(child);_atomic_write_json(origin,receipt)
                job.resume(process)
                deadline=time.monotonic()+startup_timeout
                while not stop.is_set() and not stop_requested(stop_path,receipt):
                    if process.poll() is not None:raise RuntimeError('Tunnel exited')
                    if receipt['phase']=='STARTING':
                        with log_path.open('rb') as reader:
                            reader.seek(max(0,log_path.stat().st_size-1024*1024))
                            match=re.search(rb'https://[a-z0-9-]+\.trycloudflare\.com',reader.read())
                        if match:
                            receipt.update(phase='READY',public_origin=match.group().decode('ascii'))
                            if register_webhook:
                                if len(receipt['registration_origins'])>=64:
                                    raise RuntimeError('Inspect unresolved webhook registration history before another attempt')
                                # Persist intent before the external request; a
                                # lost acknowledgement cannot lose URL ownership.
                                receipt['registration_origins'].append(receipt['public_origin'])
                                _atomic_write_json(origin,receipt)
                                registration=executor.submit(webhook_registration,receipt['public_origin'],
                                    list(receipt['registration_origins']))
                        elif time.monotonic()>=deadline:raise RuntimeError('Tunnel URL deadline exceeded')
                    if registration is not None and registration.done():
                        try:
                            registration.result();receipt['webhook']='REGISTERED'
                            receipt['registered_origin']=receipt['public_origin']
                            receipt['registration_origins']=[receipt['public_origin']]
                        except Exception:receipt['webhook']='FAILED'
                        registration=None
                    receipt['heartbeat']=time.time();_atomic_write_json(origin,receipt)
                    stop.wait(1)
                receipt['phase']='STOPPING';_atomic_write_json(origin,receipt)
            # ThreadPoolExecutor waits for the actual bounded webhook call.
        except BaseException:
            receipt['phase']='FAILED';_atomic_write_json(origin,receipt)
            raise
        finally:
            try:
                if job is not None:job.terminate()
                if process is not None:
                    # Job termination is asynchronous. Do not race it with a
                    # second TerminateProcess; wait on the retained native handle.
                    process.wait(timeout=15)
                if job is not None and job.active_process_ids():
                    raise RuntimeError('Tunnel descendants have not quiesced')
                receipt['phase']='STOPPED' if receipt['phase']=='STOPPING' else 'FAILED'
                receipt['heartbeat']=time.time();_atomic_write_json(origin,receipt)
            finally:
                if process is not None:process.close()
                if job is not None:job.close()
                for selected,handler in previous_handlers.items():signal.signal(selected,handler)


def main():
    parser=argparse.ArgumentParser(description='Owned Launcher Quick Tunnel')
    parser.add_argument('action',choices=('start','stop'))
    parser.add_argument('--executable')
    parser.add_argument('--register-webhook',action='store_true')
    args=parser.parse_args()
    config=validate_gateway_configuration()
    if args.action=='stop':request_stop(config)
    else:monitor(config,args.executable,register_webhook=args.register_webhook)


if __name__=='__main__':
    try:main()
    except Exception:
        print('Owned tunnel action failed; inspect its local receipt. No unrelated process was stopped.')
        raise SystemExit(1)
