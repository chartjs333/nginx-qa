import json
import sqlite3
import unittest

from nginx_qa import launcher_transport_store as transport


class LauncherTransportMigrationTests(unittest.TestCase):
    def test_private_ingress_never_generates_telegram_notices(self):
        connection = self.database()
        private = {'owner_kind':'setup_operator', 'actor_id':'alice', 'lifecycle':'Created'}
        connection.execute('INSERT INTO telegram_launcher_operations VALUES(?,?,?)',
                           ('private-before', '9002', json.dumps(private)))
        transport.initialize(connection)
        self.assertEqual(3, connection.execute('SELECT COUNT(*) FROM launcher_notices').fetchone()[0])
        connection.execute('INSERT INTO telegram_launcher_operations VALUES(?,?,?)',
                           ('private-after', '9002', json.dumps(private)))
        connection.execute("UPDATE telegram_launcher_operations SET record_json=json_set(record_json,'$.lifecycle','Running') WHERE operation_id='private-after'")
        self.assertEqual(3, connection.execute('SELECT COUNT(*) FROM launcher_notices').fetchone()[0])
        connection.execute("UPDATE telegram_launcher_operations SET record_json=json_set(record_json,'$.lifecycle','Stopped') WHERE operation_id='op-1'")
        self.assertEqual(4, connection.execute('SELECT COUNT(*) FROM launcher_notices').fetchone()[0])
        transport.initialize(connection)
        self.assertEqual(4, connection.execute('SELECT COUNT(*) FROM launcher_notices').fetchone()[0])

    def database(self):
        connection=sqlite3.connect(':memory:',isolation_level=None)
        self.addCleanup(connection.close)
        connection.executescript('CREATE TABLE telegram_launcher_operations(operation_id TEXT PRIMARY KEY,project_id TEXT,record_json TEXT);')
        for number in range(3):
            record={'user_id':'123','chat_id':'123','instance_id':'instance-'+str(number),
                    'lifecycle':'Completed','raw_private_task':'must-not-copy'}
            connection.execute('INSERT INTO telegram_launcher_operations VALUES(?,?,?)',('op-'+str(number),'9002',json.dumps(record)))
        return connection

    def test_upgrade_is_once_and_preserves_frozen_safe_history(self):
        connection=self.database()
        transport.initialize(connection)
        snapshots=connection.execute('SELECT snapshot FROM launcher_notices ORDER BY id').fetchall()
        self.assertEqual(3,len(snapshots))
        self.assertNotIn('must-not-copy',str(snapshots))
        connection.execute("UPDATE telegram_launcher_operations SET record_json=json_set(record_json,'$.lifecycle','Stopped') WHERE operation_id='op-1'")
        transport.initialize(connection)
        self.assertEqual(4,connection.execute('SELECT COUNT(*) FROM launcher_notices').fetchone()[0])
        self.assertEqual(snapshots,connection.execute('SELECT snapshot FROM launcher_notices ORDER BY id LIMIT 3').fetchall())
        plan=str(connection.execute('EXPLAIN QUERY PLAN SELECT 1 FROM launcher_notices WHERE operation_id=?',('op-1',)).fetchall())
        self.assertIn('launcher_notice_operation',plan)

    def test_failed_migration_marker_rolls_back_backfill_before_retry(self):
        connection=self.database()
        def authorizer(action,first,second,database,trigger):
            return sqlite3.SQLITE_DENY if action==sqlite3.SQLITE_INSERT and first=='launcher_transport_migrations' else sqlite3.SQLITE_OK
        connection.set_authorizer(authorizer)
        with self.assertRaises(sqlite3.DatabaseError):
            transport.initialize(connection)
        self.assertEqual(0,connection.execute('SELECT COUNT(*) FROM launcher_notices').fetchone()[0])
        connection.set_authorizer(None)
        transport.initialize(connection)
        self.assertEqual(3,connection.execute('SELECT COUNT(*) FROM launcher_notices').fetchone()[0])
