"""Owner-scoped Telegram controls and delivery, using existing engine services."""
from __future__ import annotations

import asyncio
import ipaddress
import json
import os
import re
import socket
from urllib.parse import urlsplit

from fastapi import HTTPException

from nginx_qa.launcher_transport_store import LauncherTransport
from nginx_qa.telegram_launcher import Principal, authorize_message, fail, load_policy
from nginx_qa.telegram_launcher_execution import current_principal, launch
from nginx_qa.telegram_launcher_store import LauncherOperations

HANDLE = re.compile(r'tml:(9[0-9]{3}):[A-Za-z0-9_-]{24}')
STATES = frozenset({'Created','Prepared','Waiting','Running','Completed','Failed','Stopped'})


def gateway_origin():
    """Operator-owned HTTPS origin, never supplied by a task/callback."""
    value = os.getenv('NGINX_QA_LAUNCHER_PUBLIC_ORIGIN', '')
    if not value:
        return None
    try:
        parsed = urlsplit(value)
        if (parsed.scheme != 'https' or not parsed.hostname or parsed.port not in (None,443)
            or parsed.username or parsed.password or parsed.path not in ('','/') or parsed.query or parsed.fragment
            or not re.fullmatch(r'[a-zA-Z0-9.-]+', parsed.hostname) or '.' not in parsed.hostname
            or parsed.hostname.endswith('.') or parsed.hostname.lower().endswith(('.localhost','.local'))):
            raise ValueError
        try:
            legacy_address=socket.inet_aton(parsed.hostname)
        except OSError:
            legacy_address=None
        if legacy_address is not None and (str(ipaddress.ip_address(legacy_address)) != parsed.hostname or not ipaddress.ip_address(legacy_address).is_global):
            raise ValueError
        try:
            if not ipaddress.ip_address(parsed.hostname).is_global:
                raise ValueError
        except ValueError as error:
            # Non-IP DNS names are allowed; IP-looking invalid names are not.
            if re.fullmatch(r'[0-9.]+', parsed.hostname):
                raise error
        return 'https://' + parsed.netloc.lower()
    except (ValueError,TypeError):
        fail('GATEWAY_CONFIG_INVALID', 'Configure the protected public HTTPS origin.', 503)


def open_operations(host, config, project_id, project_policy):
    from nginx_qa.git_provider import canonical_remote_from_address
    _, _, project, context = host.project_for_group_api(config, project_id)
    registry = host.managed_repository_registry_for_project(project, context)
    importer = host.managed_sprint_importer_factory(host.load_managed_runtime_config(), registry)
    operations = LauncherOperations(importer)
    spec = importer.git_provider.resolve_repository(project_policy['repository_id'])
    if canonical_remote_from_address(str(project.get('git_address') or context.get('git_address') or '')) != spec.canonical_remote:
        fail('REPOSITORY_BINDING_CHANGED', 'Project registry binding changed.', 409)
    return operations


def _safe_identifier(value, *, commit=False):
    pattern = r'[0-9a-f]{40,64}' if commit else r'[A-Za-z0-9][A-Za-z0-9._-]{0,127}'
    return value if isinstance(value,str) and re.fullmatch(pattern,value) else None


def structured_reports(state, repository):
    """Optional explicit agent reports, never infer a test PASS from graph DONE.

    Raw summaries are never sent. Claims remain labelled agent-reported; artifact
    paths are repository-relative and bound to the actual accepted result commit.
    """
    reports = []
    for result in state.get('result_receipts', [])[-20:]:
        raw = result.get('result_summary_redacted', '')
        if not isinstance(raw,str) or len(raw.encode('utf-8')) > 32768:
            continue
        try:
            from nginx_qa.managed_import import parse_strict_json_object
            item = parse_strict_json_object(raw.encode('utf-8'))
            if set(item) != {'launcher_report_v1'}:
                continue
            item = item['launcher_report_v1']
            if not isinstance(item,dict) or set(item) != {'checks','artifacts','pr_url'}:
                continue
            from nginx_qa.inbound_proposals import contains_recognized_secret, configured_secret_values
            if contains_recognized_secret(json.dumps(item),configured_values=configured_secret_values()):
                continue
            if not isinstance(item['checks'],list) or not isinstance(item['artifacts'],list) or len(item['checks']) > 32 or len(item['artifacts']) > 32:
                continue
            checks=[]
            for check in item['checks']:
                if not isinstance(check,dict) or set(check) != {'name','status'} or not _safe_identifier(check['name']) or check['status'] not in {'PASS','FAIL','NOT_RUN','BLOCKED'}:
                    raise ValueError
                checks.append(dict(check))
            from nginx_qa.sprint_types import relative_git_path_valid
            artifacts=[]
            for path in item['artifacts']:
                if not isinstance(path,str) or len(path)>240 or not relative_git_path_valid(path):
                    raise ValueError
                artifacts.append({'path':path,'commit':result['result_commit']})
            url=item['pr_url']
            expected_repo='https://'+repository.removeprefix('https://').removesuffix('.git')
            if url is not None and (not isinstance(url,str) or not re.fullmatch(re.escape(expected_repo) + r'/pull/[1-9][0-9]*',url)):
                raise ValueError
            reports.append({'source':'agent_report','commit':result['result_commit'],'checks':checks,'artifacts':artifacts,'pr_url':url})
        except (ValueError,KeyError,TypeError,UnicodeError):
            continue
    return reports


def instance_status(operations, operation_id, principal, project_policy):
    principal = current_principal(principal, 'read', project_policy)
    record = operations.get(operation_id,principal)
    state = operations.store.runtime_state(principal.project_id, record['sprint_id']) if record.get('sprint_id') else None
    state = state or {}
    active = set(state.get('active_assignment_ids',[])) if record['lifecycle']=='Running' and not record.get('stop_requested') else set()
    assignments=[]
    for assignment in state.get('assignments',[])[-40:]:
        assignments.append({key:_safe_identifier(assignment.get(key),commit=key=='result_commit') for key in ('assignment_id','node_id','agent_phone','status','result_commit')})
        assignments[-1]['live'] = assignment.get('assignment_id') in active
    reviews=[]
    for review in state.get('review_assignments',[])[-40:]:
        reviews.append({key:_safe_identifier(review.get(key),commit=key=='result_commit') for key in ('assignment_id','source_assignment_id','reviewer_phone','status','decision','result_commit')})
        reviews[-1]['live'] = record['lifecycle']=='Running' and not record.get('stop_requested') and review.get('status')=='active'
    reports=structured_reports(state,record['repository_binding']['canonical_remote'])
    public=operations.public(record)
    if public['blocked_reason'] is not None and not re.fullmatch(r'[A-Z][A-Z0-9_]{0,79}',str(public['blocked_reason'])):
        public['blocked_reason']='OPERATOR_INSPECTION_REQUIRED'
    public.update(assignments=assignments,reviews=reviews,assignment_count=len(state.get('assignments',[])),
                  review_count=len(state.get('review_assignments',[])),reports=reports,
                  checks_status='REPORTED' if reports else 'NOT_REPORTED')
    current_principal(principal,'read',project_policy)
    return public


def keyboard(transport, record):
    origin=gateway_origin()
    start=transport.handle(record,'start')
    web=transport.handle(record,'web')
    status=transport.handle(record,'status')
    return {'inline_keyboard':[
        [{'text':'Запустить спринт','callback_data':start}],
        [{'text':'Открыть Web UI',**({'web_app':{'url':origin+'/launcher/'+web}} if origin else {'callback_data':web})}],
        [{'text':'Статус спринта','callback_data':status}]]}


def status_text(status):
    lines=[f"Спринт {status['project_id']}: {status['status']}",
           'Режим: '+('автономный' if status['autonomous_mode'] else 'ручной'),
           f"Задания: {status['assignment_count']}; reviews: {status['review_count']}",
           'Проверки: '+status['checks_status']]
    if status.get('blocked_reason'):
        lines.append('Ожидание: '+status['blocked_reason'])
    for assignment in status['assignments'][-6:]:
        lines.append(f"{assignment['node_id']} · {assignment['agent_phone']} · {assignment['status']}"+
                     (' · выполняется' if assignment['live'] else '')+
                     (f" · {assignment['result_commit']}" if assignment['result_commit'] else ''))
    for review in status['reviews'][-4:]:
        lines.append(f"Reviewer {review['reviewer_phone']}: {review['decision'] or review['status']} · {review['result_commit']}")
    for report in status['reports'][-2:]:
        lines.append('Отчёт агента (не независимая проверка): '+report['commit'])
        lines.extend(check['name']+': '+check['status'] for check in report['checks'][:4])
        lines.extend('Артефакт: '+artifact['path'] for artifact in report['artifacts'][:2])
        if report['pr_url']:
            lines.append('PR (из отчёта): '+report['pr_url'])
    # Telegram limits count UTF-16 code units, not Python Unicode scalars.
    return '\n'.join(lines).encode('utf-16-le')[:6800].decode('utf-16-le',errors='ignore')


def _callback_message(query):
    message=query.get('message')
    if (not isinstance(message,dict) or query.get('inline_message_id') or message.get('business_connection_id')
        or query.get('business_connection_id') or type(message.get('date')) is not int or message['date']<=0
        or message.get('sender_business_bot') or message.get('is_automatic_forward')):
        fail('UNSUPPORTED_UPDATE','Open the bot in a personal private chat.',403)
    return {'from':query.get('from'),'chat':message.get('chat')}


async def handle_callback(update,host):
    query=update.get('callback_query')
    if not isinstance(query,dict) or not isinstance(query.get('data'),str) or not query['data'].startswith('tml:'):
        return None
    if type(update.get('update_id')) is not int or update['update_id']<0 or not isinstance(query.get('id'),str) or not 1<=len(query['id'])<=128:
        fail('CALLBACK_INVALID','Invalid callback identity.')
    match=HANDLE.fullmatch(query['data'])
    if match is None or len(query['data'].encode('utf-8'))>64:
        fail('CALLBACK_INVALID','Use the latest private-chat controls.')
    token=os.getenv('TELEGRAM_BOT_TOKEN','')
    # Answer before database/Git/executor work. This acknowledges receipt only.
    try:
        await asyncio.to_thread(host.telegram_api_json,token,'answerCallbackQuery',
                                {'callback_query_id':query['id'],'text':'Проверяю состояние…','cache_time':0})
    except Exception:
        pass
    project_id=match.group(1)
    message=_callback_message(query)
    policy=load_policy()
    principal=authorize_message(message,project_id,'read',policy)
    project_policy=policy['projects'][project_id]
    operations=await asyncio.to_thread(open_operations,host,await host.read_git_config(),project_id,project_policy)
    transport=LauncherTransport(operations)
    record,action=await asyncio.to_thread(transport.resolve,query['data'],principal)
    await asyncio.to_thread(transport.rate_limit,principal)
    if action=='start':
        principal=current_principal(principal,'start',project_policy)
        if record['lifecycle'] not in {'Running','Completed','Stopped'}:
            record=await launch(operations,record['operation_id'],principal,project_policy,host)
    status=await asyncio.to_thread(instance_status,operations,record['operation_id'],principal,project_policy)
    text=status_text(status)
    if action=='web':
        text='Откройте Web UI через кнопку нового сообщения в личном чате. Если gateway не настроен, требуется его настройка оператором.\n'+text
    current_principal(principal,'read',project_policy)
    record=await asyncio.to_thread(operations.get,record['operation_id'],principal)
    markup=await asyncio.to_thread(keyboard,transport,record)
    current_principal(principal,'read',project_policy)
    await asyncio.to_thread(host.telegram_api_json,token,'sendMessage',
        {'chat_id':principal.chat_id,'text':text,'reply_markup':json.dumps(markup,ensure_ascii=False)})
    return {'ok':True,'command':'launcher_callback','action':action,'instance_id':record['instance_id'],'status':status['status']}


async def handle_status_command(message,host):
    text=message.get('text')
    if not isinstance(text,str) or not text.startswith('/sprint_status'):
        return None
    match=re.fullmatch(r'/sprint_status\s+(9[0-9]{3})',text.strip())
    if match is None:
        fail('STATUS_REQUEST_INVALID','Use /sprint_status <project phone>.')
    if message.get('business_connection_id') or message.get('sender_business_bot'):
        fail('UNSUPPORTED_UPDATE','Open the bot in a personal private chat.',403)
    project_id=match.group(1)
    policy=load_policy()
    principal=authorize_message(message,project_id,'read',policy)
    project_policy=policy['projects'][project_id]
    operations=await asyncio.to_thread(open_operations,host,await host.read_git_config(),project_id,project_policy)
    def latest():
        if not operations.store.database_path.exists():
            fail('INSTANCE_NOT_FOUND','No instance is available for this user.',404)
        with operations.store._transaction() as connection:
            # Select this owner's latest instance, never another user's slot.
            for row in connection.execute('SELECT * FROM telegram_launcher_operations WHERE project_id=? ORDER BY rowid DESC', (project_id,)):
                record=operations._decode(row)
                if (record['user_id'],record['chat_id'])==(principal.user_id,principal.chat_id):
                    return record['operation_id']
        fail('INSTANCE_NOT_FOUND','No instance is available for this user.',404)
    operation_id=await asyncio.to_thread(latest)
    status=await asyncio.to_thread(instance_status,operations,operation_id,principal,project_policy)
    record=await asyncio.to_thread(operations.get,operation_id,principal)
    data={'chat_id':principal.chat_id,'text':status_text(status)}
    if record.get('instance_id'):
        data['reply_markup']=json.dumps(await asyncio.to_thread(keyboard,LauncherTransport(operations),record),ensure_ascii=False)
    current_principal(principal,'read',project_policy)
    await asyncio.to_thread(host.telegram_api_json,os.getenv('TELEGRAM_BOT_TOKEN',''),'sendMessage',data)
    return {'ok':True,'command':'sprint_status','instance_id':record['instance_id'],'status':status['status']}


def deliver_one(operations,project_id,project_policy,host,*,now=None):
    """One synchronous leased provider call. Delivery never activates work."""
    transport=LauncherTransport(operations)
    claimed=transport.claim(project_id,now=now)
    if claimed is None:
        return False
    notice,lease=claimed
    delivered=suppressed=False
    message_id=None
    try:
        snapshot=notice['snapshot']
        principal=Principal(snapshot['user_id'],snapshot['chat_id'],project_id,frozenset())
        principal=current_principal(principal,'read',project_policy)
        status=instance_status(operations,notice['operation_id'],principal,project_policy)
        record=operations.get(notice['operation_id'],principal)
        if snapshot['lifecycle'] not in STATES:
            raise ValueError
        data={'chat_id':principal.chat_id,'text':'Событие: '+snapshot['lifecycle']+'\n'+status_text(status)}
        if record.get('instance_id'):
            data['reply_markup']=json.dumps(keyboard(transport,record),ensure_ascii=False)
        current_principal(principal,'read',project_policy)
        response=host.telegram_api_json(os.getenv('TELEGRAM_BOT_TOKEN',''),'sendMessage',data)
        result=response.get('result') if isinstance(response,dict) and response.get('ok') is True else None
        if not isinstance(result,dict) or type(result.get('message_id')) is not int or result['message_id']<=0 or result.get('chat',{}).get('id')!=int(principal.chat_id):
            raise ValueError
        message_id=result['message_id']
        delivered=True
    except HTTPException as error:
        suppressed=error.status_code==403
    except Exception:
        pass  # only static error codes are persisted
    transport.finish(notice,lease,delivered=delivered,suppressed=suppressed,message_id=message_id,now=now)
    return True


async def reconcile_notifications(host,*,stop_event=None):
    if os.getenv('NGINX_QA_TELEGRAM_LAUNCHER_ENABLED','').lower() not in {'1','true'} or not os.getenv('TELEGRAM_BOT_TOKEN',''):
        return
    policy=load_policy()
    config=await host.read_git_config()
    slots=asyncio.Semaphore(4)
    async def project_delivery(project_id,project_policy):
        async with slots:
            if stop_event is not None and stop_event.is_set():
                return
            operations=await asyncio.to_thread(open_operations,host,config,project_id,project_policy)
            if not operations.store.database_path.exists():
                return
            def refresh_projection():
                with operations.store._transaction() as connection:
                    records=[operations._decode(row) for row in connection.execute(
                        'SELECT * FROM telegram_launcher_operations WHERE project_id=? AND holds_slot=1',(project_id,))]
                for record in records:
                    try:
                        principal=Principal(record['user_id'],record['chat_id'],project_id,frozenset())
                        current_principal(principal,'read',project_policy)
                        operations.get(record['operation_id'],principal)
                    except HTTPException:
                        continue
            # Observe terminal engine receipts even if prepare/start was revoked
            # and all earlier notifications have already been delivered.
            await asyncio.to_thread(refresh_projection)
            for _ in range(10):
                if stop_event is not None and stop_event.is_set():
                    break
                if not await asyncio.to_thread(deliver_one,operations,project_id,project_policy,host):
                    break
    results=await asyncio.gather(*(project_delivery(p,c) for p,c in policy['projects'].items()),return_exceptions=True)
    return {'failed_projects':sum(isinstance(result,BaseException) for result in results)}
