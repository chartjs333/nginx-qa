"""Telegram delivery receipts in the engine database; never execution state.

Lifecycle intents commit with their authoritative operation transition. Leases
and bounded retry follow the existing notification transport pattern. Email
human-decision bindings and managed assignment queues remain distinct.
"""
from __future__ import annotations

import json
import secrets
import time


SCHEMA = """
CREATE TABLE IF NOT EXISTS launcher_notices (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 operation_id TEXT NOT NULL, project_id TEXT NOT NULL,
 snapshot TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
 attempts INTEGER NOT NULL DEFAULT 0, next_attempt REAL NOT NULL DEFAULT 0,
 lease TEXT, lease_until REAL, error_code TEXT, message_id INTEGER
);
CREATE INDEX IF NOT EXISTS launcher_notice_due ON launcher_notices(project_id,status,next_attempt);
CREATE INDEX IF NOT EXISTS launcher_notice_operation ON launcher_notices(operation_id);
CREATE TABLE IF NOT EXISTS launcher_transport_migrations(version INTEGER PRIMARY KEY);
CREATE TABLE IF NOT EXISTS launcher_handles (
 handle TEXT PRIMARY KEY, operation_id TEXT NOT NULL, project_id TEXT NOT NULL,
 user_id TEXT NOT NULL, chat_id TEXT NOT NULL, instance_id TEXT NOT NULL,
 generation TEXT NOT NULL, action TEXT NOT NULL, expires REAL NOT NULL,
 UNIQUE(operation_id,action)
);
CREATE TABLE IF NOT EXISTS launcher_callback_limits (
 user_id TEXT NOT NULL, project_id TEXT NOT NULL, bucket INTEGER NOT NULL,
 count INTEGER NOT NULL, PRIMARY KEY(user_id,project_id)
);
CREATE TRIGGER IF NOT EXISTS launcher_notice_created AFTER INSERT ON telegram_launcher_operations
WHEN COALESCE(json_extract(NEW.record_json,'$.owner_kind'),'telegram') = 'telegram'
BEGIN
 INSERT INTO launcher_notices(operation_id,project_id,snapshot)
 VALUES(NEW.operation_id,NEW.project_id,json_object(
  'user_id',json_extract(NEW.record_json,'$.user_id'),
  'chat_id',json_extract(NEW.record_json,'$.chat_id'),
  'lifecycle',json_extract(NEW.record_json,'$.lifecycle'),
  'instance_id',json_extract(NEW.record_json,'$.instance_id')));
END;
CREATE TRIGGER IF NOT EXISTS launcher_notice_changed AFTER UPDATE OF record_json ON telegram_launcher_operations
WHEN json_extract(OLD.record_json,'$.lifecycle') != json_extract(NEW.record_json,'$.lifecycle')
 AND COALESCE(json_extract(NEW.record_json,'$.owner_kind'),'telegram') = 'telegram'
 AND json_extract(NEW.record_json,'$.lifecycle') IN ('Created','Waiting','Running','Completed','Failed','Stopped')
BEGIN
 INSERT INTO launcher_notices(operation_id,project_id,snapshot)
 VALUES(NEW.operation_id,NEW.project_id,json_object(
  'user_id',json_extract(NEW.record_json,'$.user_id'),
  'chat_id',json_extract(NEW.record_json,'$.chat_id'),
  'lifecycle',json_extract(NEW.record_json,'$.lifecycle'),
  'instance_id',json_extract(NEW.record_json,'$.instance_id')));
END;
"""

BACKFILL = """INSERT INTO launcher_notices(operation_id,project_id,snapshot)
 SELECT o.operation_id,o.project_id,json_object(
 'user_id',json_extract(o.record_json,'$.user_id'),
 'chat_id',json_extract(o.record_json,'$.chat_id'),
 'lifecycle',json_extract(o.record_json,'$.lifecycle'),
 'instance_id',json_extract(o.record_json,'$.instance_id'))
 FROM telegram_launcher_operations o WHERE COALESCE(json_extract(o.record_json,'$.owner_kind'),'telegram') = 'telegram' AND NOT EXISTS
 (SELECT 1 FROM launcher_notices n WHERE n.operation_id=o.operation_id);
"""


def initialize(connection):
    connection.executescript(SCHEMA)
    if not connection.execute('SELECT 1 FROM launcher_transport_migrations WHERE version=2').fetchone():
        try:
            connection.executescript('BEGIN IMMEDIATE; DROP TRIGGER IF EXISTS launcher_notice_created; '
                'DROP TRIGGER IF EXISTS launcher_notice_changed; ' + SCHEMA[SCHEMA.index('CREATE TRIGGER'):]
                + 'INSERT OR IGNORE INTO launcher_transport_migrations VALUES(2); COMMIT;')
        except BaseException:
            connection.rollback()
            raise
    if connection.execute('SELECT 1 FROM launcher_transport_migrations WHERE version=1').fetchone():
        return
    connection.execute('BEGIN IMMEDIATE')
    try:
        if not connection.execute('SELECT 1 FROM launcher_transport_migrations WHERE version=1').fetchone():
            connection.execute(BACKFILL)
            connection.execute('INSERT INTO launcher_transport_migrations VALUES(1)')
        connection.commit()
    except BaseException:
        connection.rollback()
        raise


class LauncherTransport:
    def __init__(self, operations):
        self.operations, self.store = operations, operations.store

    def handle(self, record, action, *, now=None):
        from nginx_qa.telegram_launcher import fail
        if record.get('owner_kind', 'telegram') != 'telegram':
            fail('ACCESS_DENIED', 'Private operator instances have no Telegram controls.', 403)
        if action not in {'start', 'web', 'status'} or not record.get('instance_id'):
            fail('INSTANCE_NOT_READY', 'Prepare the instance before opening its controls.', 409)
        now = time.time() if now is None else now
        binding = (record['operation_id'], record['project_id'], record['user_id'], record['chat_id'],
                   record['instance_id'], record.get('current_import_key') or record['operation_id'], action)
        with self.store._transaction() as connection:
            row = connection.execute('SELECT * FROM launcher_handles WHERE operation_id=? AND action=?',
                                     (record['operation_id'], action)).fetchone()
            if row and tuple(row[k] for k in ('operation_id','project_id','user_id','chat_id','instance_id','generation','action')) == binding and row['expires'] > now + 60:
                return row['handle']
            handle = 'tml:' + record['project_id'] + ':' + secrets.token_urlsafe(18)
            connection.execute('DELETE FROM launcher_handles WHERE operation_id=? AND action=?', (record['operation_id'], action))
            connection.execute('INSERT INTO launcher_handles VALUES(?,?,?,?,?,?,?,?,?)', (handle, *binding, now + 86400))
            return handle

    def resolve(self, handle, principal, *, action=None, now=None):
        from nginx_qa.telegram_launcher import fail
        now = time.time() if now is None else now
        if not self.store.database_path.exists():
            fail('HANDLE_EXPIRED', 'Open the latest private-chat controls.', 410)
        with self.store._transaction() as connection:
            row = connection.execute('SELECT * FROM launcher_handles WHERE handle=?', (handle,)).fetchone()
            if row is None or row['expires'] <= now:
                fail('HANDLE_EXPIRED', 'Open the latest private-chat controls.', 410)
            if (row['user_id'],row['chat_id'],row['project_id']) != (principal.user_id,principal.chat_id,principal.project_id) or (action and row['action'] != action):
                fail('ACCESS_DENIED', 'This control is not available to this user.', 403)
            binding = dict(row)
        record = self.operations.get(binding['operation_id'], principal)
        if (record.get('instance_id'), record.get('current_import_key') or record['operation_id']) != (binding['instance_id'], binding['generation']):
            fail('HANDLE_STALE', 'Use the controls for the current instance attempt.', 409)
        return record, binding['action']

    def rate_limit(self, principal, *, now=None):
        from nginx_qa.telegram_launcher import fail
        bucket = int(time.time() if now is None else now) // 60
        with self.store._transaction() as connection:
            row = connection.execute('SELECT * FROM launcher_callback_limits WHERE user_id=? AND project_id=?',
                                     (principal.user_id, principal.project_id)).fetchone()
            count = row['count'] + 1 if row and row['bucket'] == bucket else 1
            if count > 20:
                fail('RATE_LIMITED', 'Wait before using the controls again.', 429)
            connection.execute('INSERT OR REPLACE INTO launcher_callback_limits VALUES(?,?,?,?)',
                               (principal.user_id,principal.project_id,bucket,count))

    def claim(self, project_id, *, now=None):
        now = time.time() if now is None else now
        with self.store._transaction() as connection:
            connection.execute("UPDATE launcher_notices SET status=CASE WHEN attempts>=5 THEN 'failed' ELSE 'pending' END, lease=NULL,lease_until=NULL,error_code='LEASE_EXPIRED' WHERE project_id=? AND status='sending' AND lease_until<=?", (project_id,now))
            row = connection.execute("SELECT * FROM launcher_notices n WHERE project_id=? AND status='pending' AND next_attempt<=? "
                "AND EXISTS (SELECT 1 FROM telegram_launcher_operations o WHERE o.operation_id=n.operation_id "
                "AND COALESCE(json_extract(o.record_json,'$.owner_kind'),'telegram')='telegram') ORDER BY id LIMIT 1", (project_id,now)).fetchone()
            if row is None:
                return None
            lease = secrets.token_hex(16)
            connection.execute("UPDATE launcher_notices SET status='sending',attempts=attempts+1,lease=?,lease_until=? WHERE id=?", (lease,now+120,row['id']))
            return {**dict(row),'snapshot':json.loads(row['snapshot']),'attempts':row['attempts']+1},lease

    def finish(self, notice, lease, *, delivered=False, suppressed=False, message_id=None, now=None):
        now = time.time() if now is None else now
        status = 'delivered' if delivered else 'suppressed' if suppressed else 'failed' if notice['attempts'] >= 5 else 'pending'
        error = None if delivered else 'ACCESS_REVOKED' if suppressed else 'DELIVERY_FAILED'
        with self.store._transaction() as connection:
            result = connection.execute('UPDATE launcher_notices SET status=?,next_attempt=?,lease=NULL,lease_until=NULL,error_code=?,message_id=? WHERE id=? AND status=\'sending\' AND lease=?',
                (status,now+min(300,2**notice['attempts']),error,message_id,notice['id'],lease))
            return result.rowcount == 1
