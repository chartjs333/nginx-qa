"""Private operator harness; keys are sent only by the recipient-bound helper."""
import hashlib
import http.cookiejar
import json
from pathlib import Path
import time
import urllib.error
import urllib.request
from authorize_wizard_browser import authorize

BASE = 'http://127.0.0.1:18038'
ROOT = Path(__file__).resolve().parent


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Client:
    def __init__(self):
        self.jar = http.cookiejar.CookieJar()
        self.op = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPCookieProcessor(self.jar), NoRedirect())
        self.headers = {}

    def req(self, path, payload=None, retry=0):
        assert path.startswith('/api/v1/') and not path.startswith('//')
        request = urllib.request.Request(BASE + path, data=None if payload is None else json.dumps(payload).encode(),
            headers={'Content-Type': 'application/json', 'Origin': BASE, **self.headers})
        try:
            # The provider has a bounded 240s cold-start budget. This does not
            # extend that budget; it allows its receipt to reach the harness.
            with self.op.open(request, timeout=260) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            if payload is None and exc.code == 429 and retry < 3:
                time.sleep(.2)
                return self.req(path, payload, retry + 1)
            raise RuntimeError(f'{path}: HTTP {exc.code}: {exc.read(2000).decode()}') from None

    def pair(self):
        session = self.req('/api/v1/operator/session')
        authorize(session['pairing_code'])
        session = self.req('/api/v1/operator/session')
        self.headers = {'X-Nginx-QA-CSRF': session['csrf_token']}
        return session['actor_id']

    def action(self, action, arguments, key, project='9000'):
        journal = ROOT / ('http-action-' + hashlib.sha256(key.encode()).hexdigest()[:20] + '.json')
        if journal.exists():
            retained = json.loads(journal.read_bytes())
            assert retained['action'] == action and retained['arguments'] == arguments and retained['project'] == project
            return {**retained, 'receipt': self.req(retained['apply_path'], retained['apply_body'])}
        path = '/api/v1/projects/' + project + '/setup/'
        snapshot = self.req(path + 'snapshot')
        plan = self.req(path + 'plans', {'schema_version': 1, 'snapshot_id': snapshot['snapshot_id'], 'action_id': action, 'arguments': arguments})
        body = {'schema_version': 1, 'plan_digest': plan['plan_digest'], 'idempotency_key': key, 'confirmed': True}
        retained = {'action': action, 'arguments': arguments, 'project': project, 'snapshot': snapshot, 'plan': plan,
                    'apply_path': path + 'plans/' + plan['plan_id'] + '/apply', 'apply_body': body}
        journal.write_text(json.dumps(retained, indent=2) + '\n', encoding='utf-8')
        return {**retained, 'receipt': self.req(retained['apply_path'], body)}
