"""Single-use Telegram Mini App authentication, scoped to one engine instance."""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import secrets
import time
from urllib.parse import parse_qsl

from nginx_qa.managed_import import parse_strict_json_object
from nginx_qa.telegram_launcher import Principal, fail
from nginx_qa.telegram_launcher_execution import current_principal


SCHEMA = """
CREATE TABLE IF NOT EXISTS launcher_auth_challenges (
 challenge_hash TEXT PRIMARY KEY, browser_hash TEXT NOT NULL,
 binding_json TEXT NOT NULL, expires REAL NOT NULL, used INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS launcher_auth_launches (
 digest TEXT PRIMARY KEY, expires REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS launcher_auth_exchanges (
 token_hash TEXT PRIMARY KEY, challenge_hash TEXT NOT NULL UNIQUE,
 digest TEXT NOT NULL, expires REAL NOT NULL, consumed INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS launcher_auth_sessions (
 session_hash TEXT PRIMARY KEY, binding_json TEXT NOT NULL,
 csrf_hash TEXT NOT NULL, expires REAL NOT NULL, revoked INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS launcher_auth_limits (
 key TEXT PRIMARY KEY, bucket INTEGER NOT NULL, count INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS launcher_session_live ON launcher_auth_sessions(expires) WHERE revoked=0;
CREATE INDEX IF NOT EXISTS launcher_session_owner ON launcher_auth_sessions(
 json_extract(binding_json,'$.user_id'),json_extract(binding_json,'$.project_id'),expires) WHERE revoked=0;
"""


def digest(value):
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


def bounded_setting(name,default,minimum,maximum):
    try:
        value=int(os.getenv(name,str(default)))
        if not minimum<=value<=maximum:
            raise ValueError
        return value
    except (ValueError,TypeError):
        fail('AUTH_CONFIG_INVALID','Authentication lifetime configuration is invalid.',503)


def verify_init_data(raw,bot_token,*,now=None):
    """Telegram bot HMAC: every decoded field except hash, including signature.

    Replay identity uses authenticated canonical data, not URL encoding/order.
    https://core.telegram.org/bots/webapps#validating-data-received-via-the-mini-app
    """
    now=time.time() if now is None else now
    try:
        if not isinstance(raw,str) or not 1<=len(raw.encode('utf-8'))<=16384 or not bot_token:
            raise ValueError
        if re.search(r'%(?![0-9a-fA-F]{2})',raw):
            raise ValueError
        pairs=parse_qsl(raw,keep_blank_values=True,strict_parsing=True,encoding='utf-8',errors='strict',max_num_fields=32)
        values=dict(pairs)
        if len(values)!=len(pairs) or any(not re.fullmatch(r'[a-z_]{1,64}',key) or any(c in value for c in '\r\n\0') for key,value in pairs):
            raise ValueError
        supplied=values.pop('hash')
        if not re.fullmatch(r'[0-9a-f]{64}',supplied):
            raise ValueError
        canonical='\n'.join(key+'='+values[key] for key in sorted(values))
        key=hmac.new(b'WebAppData',bot_token.encode('utf-8'),hashlib.sha256).digest()
        expected=hmac.new(key,canonical.encode('utf-8'),hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected,supplied):
            raise ValueError
        if not re.fullmatch(r'[0-9]{1,12}',values['auth_date']):
            raise ValueError
        issued=int(values['auth_date'])
        if not now-300<=issued<=now+30:
            raise ValueError
        user=parse_strict_json_object(values['user'].encode('utf-8'))
        if type(user.get('id')) is not int or not 0<user['id']<2**52 or user.get('is_bot',False) is not False:
            raise ValueError
        if values.get('chat_type') not in (None,'private','sender') or 'chat' in values or 'receiver' in values:
            raise ValueError
        if not isinstance(values.get('query_id'),str) or not 1<=len(values['query_id'])<=256:
            raise ValueError
        return {'user_id':str(user['id']),'digest':digest(bot_token.partition(':')[0]+'\0'+expected),
                'valid_until':issued+300}
    except (ValueError,KeyError,TypeError,UnicodeError):
        fail('TELEGRAM_AUTH_INVALID','Open a fresh Mini App session from the private bot chat.',401)


def cookie_suffix(handle):
    return digest(handle)[:20]


def csrf_token(session):
    return digest('launcher-csrf\0'+session)


def binding(record,handle,origin):
    return {key:record[key] for key in ('operation_id','project_id','user_id','chat_id','instance_id')} | {
        'generation':record.get('current_import_key') or record['operation_id'],'handle':handle,'origin':origin}


class LauncherAuth:
    def __init__(self,operations):
        self.operations,self.store=operations,operations.store

    @staticmethod
    def _rate(connection,key,now,limit):
        bucket=int(now)//60
        row=connection.execute('SELECT * FROM launcher_auth_limits WHERE key=?',(key,)).fetchone()
        count=row['count']+1 if row and row['bucket']==bucket else 1
        if count>limit:
            fail('RATE_LIMITED','Wait before trying to sign in again.',429)
        connection.execute('INSERT OR REPLACE INTO launcher_auth_limits VALUES(?,?,?)',(key,bucket,count))

    def _current(self,stored,principal,origin,project_policy):
        principal=current_principal(principal,'read',project_policy)
        record=self.operations.get(stored['operation_id'],principal)
        if binding(record,stored['handle'],origin)!=stored:
            fail('AUTH_SCOPE_STALE','Open a fresh Mini App session for this instance.',403)
        return record

    def _charge(self,principal,clock):
        # Count rejected attempts in a separate committed quota transaction.
        with self.store._transaction() as connection:
            self._rate(connection,'auth:'+principal.user_id+':'+principal.project_id,clock(),20)

    def challenge(self,record,handle,origin,*,now=None):
        clock=time.time if now is None else lambda fixed=now:float(fixed)
        challenge,browser=secrets.token_urlsafe(32),secrets.token_urlsafe(32)
        stored=binding(record,handle,origin)
        with self.store._transaction() as connection:
            now=clock()
            self._rate(connection,'challenge-global',now,60)
            # Authentication capabilities expire independently of sprint history.
            connection.execute('DELETE FROM launcher_auth_exchanges WHERE expires<?',(now-3600,))
            connection.execute('DELETE FROM launcher_auth_challenges WHERE expires<?',(now-3600,))
            connection.execute('DELETE FROM launcher_auth_launches WHERE expires<?',(now,))
            connection.execute('DELETE FROM launcher_auth_sessions WHERE expires<?',(now-86400,))
            connection.execute('DELETE FROM launcher_auth_limits WHERE bucket<?',(int(now)//60-60,))
            if connection.execute('SELECT COUNT(*) FROM launcher_auth_challenges WHERE expires>?',(now,)).fetchone()[0]>=256:
                fail('AUTH_QUOTA_EXCEEDED','Too many pending sign-ins. Try again later.',429)
            connection.execute('INSERT INTO launcher_auth_challenges VALUES(?,?,?,?,0)',
                (digest(challenge),digest(browser),json.dumps(stored),now+120))
        return {'challenge':challenge,'browser':browser,'expires_in':120}

    def authenticate(self,handle,challenge,browser,verified,principal,origin,project_policy,*,now=None):
        clock=time.time if now is None else lambda fixed=now:float(fixed)
        self._charge(principal,clock)
        token=secrets.token_urlsafe(32)
        ttl=bounded_setting('NGINX_QA_LAUNCHER_EXCHANGE_TTL',60,5,300)
        # Read the immutable binding before current engine/ACL validation, then
        # recheck the exact challenge under the consume transaction below.
        with self.store._transaction() as connection:
            row=connection.execute('SELECT * FROM launcher_auth_challenges WHERE challenge_hash=?',(digest(challenge),)).fetchone()
        if row is None:
            fail('AUTH_CHALLENGE_INVALID','Open a fresh Mini App session.',401)
        stored=json.loads(row['binding_json'])
        self._current(stored,principal,origin,project_policy)
        with self.store._transaction() as connection:
            now=clock()
            current_principal(principal,'read',project_policy)
            row=connection.execute('SELECT * FROM launcher_auth_challenges WHERE challenge_hash=?',(digest(challenge),)).fetchone()
            if (verified['valid_until']<=now or row is None or row['used'] or row['expires']<=now or row['binding_json']!=json.dumps(stored)
                or not hmac.compare_digest(row['browser_hash'],digest(browser)) or stored['handle']!=handle
                or stored['user_id']!=verified['user_id'] or stored['user_id']!=principal.user_id):
                fail('AUTH_CHALLENGE_INVALID','Open a fresh Mini App session.',401)
            if connection.execute('SELECT 1 FROM launcher_auth_launches WHERE digest=?',(verified['digest'],)).fetchone():
                fail('AUTH_REPLAY','This Telegram launch has already been used. Reopen the Mini App.',409)
            connection.execute('INSERT INTO launcher_auth_launches VALUES(?,?)',(verified['digest'],verified['valid_until']+30))
            connection.execute('UPDATE launcher_auth_challenges SET used=1 WHERE challenge_hash=?',(digest(challenge),))
            connection.execute('INSERT INTO launcher_auth_exchanges VALUES(?,?,?,?,0)',(digest(token),digest(challenge),verified['digest'],now+ttl))
        return {'exchange_token':token,'expires_in':ttl}

    def exchange(self,handle,challenge,browser,token,verified,principal,origin,project_policy,*,now=None):
        clock=time.time if now is None else lambda fixed=now:float(fixed)
        self._charge(principal,clock)
        session=secrets.token_urlsafe(32)
        ttl=bounded_setting('NGINX_QA_LAUNCHER_SESSION_TTL',1800,60,3600)
        with self.store._transaction() as connection:
            row=connection.execute('SELECT * FROM launcher_auth_challenges WHERE challenge_hash=?',(digest(challenge),)).fetchone()
        if row is None:
            fail('TOKEN_INVALID','The login exchange is unavailable.',401)
        stored=json.loads(row['binding_json'])
        self._current(stored,principal,origin,project_policy)
        with self.store._transaction() as connection:
            now=clock()
            current_principal(principal,'read',project_policy)
            item=connection.execute('SELECT * FROM launcher_auth_exchanges WHERE token_hash=?',(digest(token),)).fetchone()
            row=connection.execute('SELECT * FROM launcher_auth_challenges WHERE challenge_hash=?',(digest(challenge),)).fetchone()
            if (verified['valid_until']<=now or item is None or item['consumed'] or item['expires']<=now or item['challenge_hash']!=digest(challenge)
                or item['digest']!=verified['digest'] or row is None or not row['used']
                or row['binding_json']!=json.dumps(stored) or not hmac.compare_digest(row['browser_hash'],digest(browser))
                or stored['handle']!=handle or stored['user_id']!=verified['user_id'] or stored['user_id']!=principal.user_id):
                fail('TOKEN_INVALID','The login exchange is expired, already used or belongs to another session.',401)
            if (connection.execute('SELECT COUNT(*) FROM launcher_auth_sessions WHERE revoked=0 AND expires>?',(now,)).fetchone()[0]>=512
                or connection.execute("SELECT COUNT(*) FROM launcher_auth_sessions WHERE revoked=0 AND expires>? AND json_extract(binding_json,'$.user_id')=? AND json_extract(binding_json,'$.project_id')=?",(now,principal.user_id,principal.project_id)).fetchone()[0]>=16):
                fail('SESSION_QUOTA_EXCEEDED','Close an existing session before opening another.',429)
            connection.execute('UPDATE launcher_auth_exchanges SET consumed=1 WHERE token_hash=?',(digest(token),))
            connection.execute('INSERT INTO launcher_auth_sessions VALUES(?,?,?,?,0)',
                (digest(session),json.dumps(stored),digest(csrf_token(session)),now+ttl))
        return {'session':session,'csrf_token':csrf_token(session),'expires_in':ttl}

    def session(self,handle,token,origin,project_policy,*,csrf=None,now=None):
        clock=time.time if now is None else lambda fixed=now:float(fixed)
        with self.store._transaction() as connection:
            row=connection.execute('SELECT * FROM launcher_auth_sessions WHERE session_hash=?',(digest(token),)).fetchone()
        if row is None or row['revoked'] or row['expires']<=clock():
            fail('SESSION_EXPIRED','Open a fresh Mini App session.',401)
        stored=json.loads(row['binding_json'])
        if stored['handle']!=handle or stored['origin']!=origin or (csrf is not None and not hmac.compare_digest(row['csrf_hash'],digest(csrf))):
            fail('ACCESS_DENIED','The session does not authorize this request.',403)
        principal=Principal(stored['user_id'],stored['chat_id'],stored['project_id'],frozenset())
        record=self._current(stored,principal,origin,project_policy)
        with self.store._transaction() as connection:
            latest=connection.execute('SELECT * FROM launcher_auth_sessions WHERE session_hash=?',(digest(token),)).fetchone()
            if latest is None or latest['revoked'] or latest['expires']<=clock() or latest['binding_json']!=row['binding_json']:
                fail('SESSION_EXPIRED','Open a fresh Mini App session.',401)
        return record,current_principal(principal,'read',project_policy)

    def logout(self,token):
        with self.store._transaction() as connection:
            connection.execute('UPDATE launcher_auth_sessions SET revoked=1 WHERE session_hash=?',(digest(token),))
