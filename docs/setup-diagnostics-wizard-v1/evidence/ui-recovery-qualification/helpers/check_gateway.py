"""No-credential live fail-closed observation plus isolated enabled-route proof."""
import hashlib
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
import urllib.error
import urllib.request
from unittest.mock import patch

sys.path.insert(0, 'D:/nq-sdw-launcher')
from fastapi.testclient import TestClient
from nginx_qa.launcher_gateway import create_gateway

ROOT = Path(__file__).resolve().parent
CASES = [('/setup', 'GET'), ('/setup/assets/setup.js', 'GET'),
         ('/api/v1/projects/9000/setup/snapshot', 'GET'),
         ('/api/v1/projects/9000/setup/plans/plan-fixture/inspect', 'POST'),
         ('/api/v1/projects/9000/setup/plans/plan-fixture/inspect', 'GET')]


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


live = []
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
for path, method in CASES:
    request = urllib.request.Request('http://127.0.0.1:18037' + path, method=method,
        data=b'{}' if method == 'POST' else None, headers={'Content-Type': 'application/json'})
    try:
        with opener.open(request, timeout=5) as response:
            status, body = response.status, response.read(8192)
    except urllib.error.HTTPError as error:
        status, body = error.code, error.read(8192)
    data = json.loads(body)
    code = data.get('detail', {}).get('error') if isinstance(data.get('detail'), dict) else None
    assert status in (403, 404) or (status == 503 and code in ('GATEWAY_DISABLED', 'GATEWAY_OFFLINE')), (path, status, code)
    live.append({'path': path, 'method': method, 'status': status, 'error_code': code,
                 'body_sha256': hashlib.sha256(body).hexdigest(), 'credentials_sent': False})

# Real middleware/routes, isolated TestClient and fake configured origin. No
# running gateway configuration or foreign operator permission is changed.
isolated = []
with patch.dict(os.environ, {'NGINX_QA_LAUNCHER_GATEWAY_ENABLED': '1', 'NGINX_QA_TELEGRAM_LAUNCHER_ENABLED': '1'}), \
     patch('nginx_qa.launcher_gateway.gateway_origin', return_value='http://public.test'):
    client = TestClient(create_gateway(SimpleNamespace()), raise_server_exceptions=False)
    for path, method in CASES:
        response = client.request(method, path, headers={'Host': 'public.test', 'Origin': 'http://public.test'},
                                  content=b'{}' if method == 'POST' else None)
        assert response.status_code == 404, (path, response.status_code)
        isolated.append({'path': path, 'method': method, 'status': 404})
report = {'status': 'PASS', 'product': 'fdafbf58db85555706b8ea04e11c1d4450b0f120',
          'live_gateway': live, 'live_meaning': 'Existing gateway is fail-closed while offline; not a fresh live configured-route/TLS proof.',
          'isolated_enabled_gateway_route_checks': isolated, 'existing_services_reconfigured': False,
          'prior_cloudflare_24_check_provenance_unchanged': True}
(ROOT / 'gateway-denial.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
print(json.dumps({'status': 'PASS', 'live_statuses': [r['status'] for r in live], 'isolated_enabled_route_statuses': [r['status'] for r in isolated]}))
