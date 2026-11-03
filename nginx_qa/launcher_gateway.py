"""Deny-default public Mini App boundary; the legacy app is never mounted here."""
from __future__ import annotations

import asyncio
import json
import os
import secrets
import time
from pathlib import Path
from urllib.parse import urlsplit

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response

from nginx_qa.control_secrets import control_secret
from nginx_qa.launcher_auth import LauncherAuth, cookie_suffix, csrf_token, verify_init_data
from nginx_qa.launcher_transport_store import LauncherTransport
from nginx_qa.managed_import import parse_strict_json_object
from nginx_qa.telegram_launcher import Principal, fail, load_policy, maybe_handle_prompt
from nginx_qa.telegram_launcher_controls import HANDLE, gateway_origin, instance_status, open_operations
from nginx_qa.telegram_launcher_execution import current_principal, launch, stop


STATIC=Path(__file__).resolve().parents[1]/'static'/'launcher'
SECURITY_HEADERS={
    'Cache-Control':'no-store','Referrer-Policy':'no-referrer','X-Content-Type-Options':'nosniff',
    'Content-Security-Policy':"default-src 'none'; script-src 'self' https://telegram.org; style-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'; object-src 'none'; frame-ancestors 'self' https://web.telegram.org https://*.telegram.org",
    'Permissions-Policy':'camera=(), microphone=(), geolocation=()',
}


async def strict_body(request,fields):
    if request.headers.get('content-type','').split(';')[0].strip().lower()!='application/json':
        fail('CONTENT_TYPE_INVALID','Use an application/json request.',415)
    raw=bytearray()
    async for chunk in request.stream():
        raw.extend(chunk)
        if len(raw)>32768:
            fail('PAYLOAD_TOO_LARGE','Authentication request is too large.',413)
    try:
        body=parse_strict_json_object(bytes(raw))
        if set(body)!=set(fields) or any(not isinstance(value,str) or not value or len(value)>16384 for value in body.values()):
            raise ValueError
        return body
    except (ValueError,UnicodeError):
        fail('INVALID_JSON','Use the documented authentication request fields.')


def _cookie_name(handle,kind):
    return '__Host-nqa-'+kind+'-'+cookie_suffix(handle)


def _set_cookie(response,handle,kind,value,ttl):
    # Telegram Web embeds the app cross-site. CHIPS isolates cookies by the
    # top-level site; SameSite=None supports the embedded context. Exact Origin,
    # signed identity, browser binding and CSRF remain mandatory on API writes.
    response.set_cookie(_cookie_name(handle,kind),value,max_age=ttl,secure=True,httponly=True,samesite='none',path='/')
    _partition_cookie(response,_cookie_name(handle,kind))


def _partition_cookie(response,name):
    # Python3.12 SimpleCookie has no Partitioned option; append the standard
    # attribute to this cookie only, including its matching deletion receipt.
    response.raw_headers=[(key,value+b'; Partitioned' if key==b'set-cookie' and value.startswith((name+'=').encode()) else value)
                          for key,value in response.raw_headers]


def _delete_cookie(response,handle,kind):
    name=_cookie_name(handle,kind)
    response.delete_cookie(name,path='/',secure=True,httponly=True,samesite='none')
    _partition_cookie(response,name)


def create_gateway(host):
    app=FastAPI(docs_url=None,redoc_url=None,openapi_url=None)
    app.state.launcher_work=set()

    async def owned(awaitable):
        task=asyncio.create_task(awaitable)
        app.state.launcher_work.add(task)
        def completed(done):
            app.state.launcher_work.discard(done)
            if not done.cancelled():
                done.exception()  # observe errors without logging credentials
        task.add_done_callback(completed)
        return await asyncio.shield(task)

    async def thread(function,*args,**kwargs):
        return await owned(asyncio.to_thread(function,*args,**kwargs))
    # No proxy addresses participate in authority or rate-limit identity.
    rate={'bucket':-1,'count':0}

    @app.middleware('http')
    async def public_boundary(request,call_next):
        try:
            if (os.getenv('NGINX_QA_LAUNCHER_GATEWAY_ENABLED','').lower() not in {'1','true'}
                or os.getenv('NGINX_QA_TELEGRAM_LAUNCHER_ENABLED','').lower() not in {'1','true'}):
                fail('GATEWAY_DISABLED','The protected gateway is disabled.',503)
            origin=gateway_origin()
            if origin is None:
                fail('GATEWAY_OFFLINE','The protected gateway is not configured.',503)
            counts={}
            for name,_ in request.scope.get('headers',[]):
                counts[name.lower()]=counts.get(name.lower(),0)+1
            if any(counts.get(name,0)>1 for name in (b'host',b'origin',b'cookie',b'content-type',b'x-csrf-token',b'x-telegram-bot-api-secret-token')):
                fail('REQUEST_INVALID','Ambiguous request headers.',400)
            if request.headers.get('host','').lower()!=urlsplit(origin).netloc.lower():
                fail('HOST_INVALID','This host is not the configured gateway.',421)
            # Every mutation, including login, is same-origin. Telegram webhook
            # is the single exception: it has its own mandatory secret check.
            public_static=(request.method in {'GET','HEAD'} and (
                request.url.path in {'/launcher/assets/app.js','/launcher/assets/app.css'}
                or (request.url.path.startswith('/launcher/') and HANDLE.fullmatch(request.url.path.removeprefix('/launcher/')) is not None)))
            if request.url.path!='/launcher/telegram/webhook' and not public_static:
                supplied=request.headers.get('origin')
                if (request.method not in {'GET','HEAD'} and supplied!=origin) or (supplied is not None and supplied!=origin):
                    fail('ORIGIN_INVALID','Use the protected Mini App origin.',403)
                if request.headers.get('sec-fetch-site') not in (None,'same-origin','none'):
                    fail('ORIGIN_INVALID','Cross-site requests are not allowed.',403)
            if request.url.path.startswith('/launcher/auth/'):
                bucket=int(time.monotonic())//60
                if rate['bucket']!=bucket:
                    rate.update(bucket=bucket,count=0)
                rate['count']+=1
                if rate['count']>120:
                    fail('RATE_LIMITED','Wait before trying to sign in again.',429)
            request.scope['headers']=[(key,value) for key,value in request.scope.get('headers',[])
                if not key.lower().startswith((b'x-forwarded-',b'x-auth-',b'x-user-',b'x-identity-'))
                and key.lower() not in {b'forwarded',b'x-real-ip',b'authorization'}]
            response=await call_next(request)
        except HTTPException as error:
            response=JSONResponse({'detail':error.detail},status_code=error.status_code)
        except Exception:
            response=JSONResponse({'detail':{'error':'GATEWAY_UNAVAILABLE','message':'The gateway request could not be completed.',
                'correlation_id':'gateway-'+secrets.token_hex(12),'retryable':True}},status_code=503)
        for key,value in SECURITY_HEADERS.items():
            response.headers[key]=value
        return response

    async def context(handle):
        match=HANDLE.fullmatch(handle)
        if match is None:
            fail('INSTANCE_NOT_FOUND','This Mini App instance is unavailable.',404)
        project_id=match.group(1)
        policy=load_policy()
        project_policy=policy['projects'].get(project_id)
        if project_policy is None:
            fail('ACCESS_DENIED','This project is unavailable.',403)
        operations=await thread(open_operations,host,await owned(host.read_git_config()),project_id,project_policy)
        if not operations.store.database_path.exists():
            fail('INSTANCE_NOT_FOUND','This Mini App instance is unavailable.',404)
        return operations,project_policy

    async def verified_context(body):
        token=control_secret('bot',allow_legacy=False)
        if not token:
            fail('AUTH_UNAVAILABLE','The trusted Telegram auth credential is not configured.',503)
        verified=verify_init_data(body['init_data'],token)
        operations,project_policy=await context(body['handle'])
        project_id=HANDLE.fullmatch(body['handle']).group(1)
        principal=Principal(verified['user_id'],verified['user_id'],project_id,frozenset())
        principal=current_principal(principal,'read',project_policy)
        return operations,project_policy,verified,principal

    async def session_context(request,handle,*,mutating=False):
        operations,project_policy=await context(handle)
        token=request.cookies.get(_cookie_name(handle,'session'),'')
        auth=LauncherAuth(operations)
        record,principal=await thread(auth.session,handle,token,gateway_origin(),project_policy,
            csrf=request.headers.get('x-csrf-token','') if mutating else None)
        return operations,project_policy,record,principal,token

    @app.get('/launcher/assets/app.js')
    async def javascript():
        return Response((STATIC/'app.js').read_text(encoding='utf-8'),media_type='application/javascript')

    @app.get('/launcher/assets/app.css')
    async def stylesheet():
        return Response((STATIC/'app.css').read_text(encoding='utf-8'),media_type='text/css')

    @app.get('/launcher/{handle}')
    async def shell(handle):
        if HANDLE.fullmatch(handle) is None:
            fail('INSTANCE_NOT_FOUND','Open the Mini App from the private bot chat.',404)
        # Link previews receive a static shell, never a token, cookie or state.
        return HTMLResponse((STATIC/'index.html').read_text(encoding='utf-8'))

    @app.post('/launcher/auth/challenge')
    async def challenge(request:Request):
        body=await strict_body(request,{'handle'})
        operations,project_policy=await context(body['handle'])
        def prepare():
            with operations.store._transaction() as connection:
                row=connection.execute('SELECT * FROM launcher_handles WHERE handle=?',(body['handle'],)).fetchone()
                if row is None or row['action']!='web':
                    fail('INSTANCE_NOT_FOUND','Open current Mini App controls in your private chat.',404)
                principal=Principal(row['user_id'],row['chat_id'],row['project_id'],frozenset())
            current_principal(principal,'read',project_policy)
            record,_=LauncherTransport(operations).resolve(body['handle'],principal,action='web')
            return LauncherAuth(operations).challenge(record,body['handle'],gateway_origin())
        result=await thread(prepare)
        response=JSONResponse({'challenge':result['challenge'],'expires_in':result['expires_in']})
        _set_cookie(response,body['handle'],'launch',result['browser'],result['expires_in'])
        return response

    @app.post('/launcher/auth/telegram')
    async def telegram_auth(request:Request):
        body=await strict_body(request,{'handle','challenge','init_data'})
        operations,project_policy,verified,principal=await verified_context(body)
        browser=request.cookies.get(_cookie_name(body['handle'],'launch'),'')
        if not browser:
            fail('BROWSER_SESSION_UNAVAILABLE','This client blocks protected cookies. Open the Mini App in a supported Telegram app.',409)
        result=await thread(LauncherAuth(operations).authenticate,body['handle'],body['challenge'],browser,
            verified,principal,gateway_origin(),project_policy)
        response=JSONResponse(result)
        _set_cookie(response,body['handle'],'launch',browser,result['expires_in'])
        return response

    @app.post('/launcher/auth/exchange')
    async def exchange(request:Request):
        body=await strict_body(request,{'handle','challenge','exchange_token','init_data'})
        operations,project_policy,verified,principal=await verified_context(body)
        browser=request.cookies.get(_cookie_name(body['handle'],'launch'),'')
        result=await thread(LauncherAuth(operations).exchange,body['handle'],body['challenge'],browser,body['exchange_token'],
            verified,principal,gateway_origin(),project_policy)
        response=JSONResponse({'ok':True,'csrf_token':result['csrf_token'],'expires_in':result['expires_in']})
        _set_cookie(response,body['handle'],'session',result['session'],result['expires_in'])
        _delete_cookie(response,body['handle'],'launch')
        return response

    @app.get('/launcher/instances/{handle}/status')
    async def status(request:Request,handle):
        operations,policy,record,principal,token=await session_context(request,handle)
        result=await thread(instance_status,operations,record['operation_id'],principal,policy)
        current_principal(principal,'read',policy)
        return {**result,'csrf_token':csrf_token(token)}

    @app.post('/launcher/instances/{handle}/{action}')
    async def action(request:Request,handle,action):
        if action not in {'start','stop','logout'}:
            fail('ACTION_NOT_FOUND','This operation is unavailable.',404)
        # Mutation bodies contain no route/identity/upstream overrides.
        await strict_body(request,set())
        operations,policy,record,principal,token=await session_context(request,handle,mutating=True)
        if action=='logout':
            await thread(LauncherAuth(operations).logout,token)
            response=JSONResponse({'ok':True})
            _delete_cookie(response,handle,'session')
            return response
        principal=current_principal(principal,action,policy)
        await owned((launch if action=='start' else stop)(operations,record['operation_id'],principal,policy,host))
        return await thread(instance_status,operations,record['operation_id'],principal,policy)

    @app.post('/launcher/telegram/webhook')
    async def webhook(request:Request):
        # Only explicit launcher commands/envelopes/callbacks. No legacy import
        # fallthrough that could expose the full admin app through this gateway.
        if not control_secret('webhook',allow_legacy=False) or not control_secret('bot',allow_legacy=False):
            fail('AUTH_UNAVAILABLE','Trusted Telegram credentials are not configured.',503)
        response=await owned(maybe_handle_prompt(request,host))
        if response is None:
            return {'ok':True,'ignored':True}
        return response

    return app


async def drain_gateway(app):
    """After closing ingress, drain real service work before main lifespan exits."""
    failed=0
    while app.state.launcher_work:
        results=await asyncio.gather(*tuple(app.state.launcher_work),return_exceptions=True)
        failed+=sum(isinstance(result,BaseException) for result in results)
    return {'failed_operations':failed}
