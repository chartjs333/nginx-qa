"""Read the common tunnel's short-lived, owned public-origin receipt."""
from __future__ import annotations

import os
from pathlib import Path
import re
import time

from nginx_qa.managed_import import parse_strict_json_object


def owned_origin(*, now=None):
    setting=os.getenv('NGINX_QA_LAUNCHER_ORIGIN_STATE','')
    if not setting:
        return None
    from nginx_qa.launcher_host import _path, _same_path
    state=_path(os.environ['NGINX_QA_STAGING_STATE_BASE'])
    expected=state/'legacy'/'runtime_state'/'launcher-origin.json'
    if not _same_path(setting,expected):
        raise ValueError('Origin receipt is outside its owned state')
    _path(expected.parent)
    marker=_path(state/'.nginx-qa-staging-owner.json')
    if marker.stat().st_size>32768:
        raise ValueError('Invalid origin owner')
    proof=parse_strict_json_object(marker.read_bytes())
    owner=proof['ownership']
    if (proof['schema_version']!=1 or owner['instance_id']!=os.environ['NGINX_QA_INSTANCE_ID']
        or owner['gateway_port']!=int(os.environ['NGINX_QA_GATEWAY_PORT'])
        or not _same_path(owner['state_base'],state)
        or not _same_path(owner['gateway_origin_state'],expected)):
        raise ValueError('Origin ownership changed')
    if not expected.exists():
        return None
    _path(expected)
    if expected.stat().st_size>32768:
        raise ValueError('Invalid origin receipt')
    receipt=parse_strict_json_object(expected.read_bytes())
    if (receipt['schema_version']!=1 or receipt['initialization_id']!=proof['initialization_id']
        or receipt['instance_id']!=owner['instance_id']
        or receipt['upstream']!=f"http://127.0.0.1:{owner['gateway_port']}"):
        raise ValueError('Origin receipt belongs to another service')
    if receipt['phase']!='READY':
        return None
    stamp=receipt['heartbeat']
    current=time.time() if now is None else now
    if not isinstance(stamp,(float,int)) or isinstance(stamp,bool) or not 0<=current-stamp<=15:
        return None
    value=receipt['public_origin']
    if not isinstance(value,str) or not re.fullmatch(r'https://[a-z0-9-]+\.trycloudflare\.com',value):
        raise ValueError('Invalid quick tunnel origin')
    return value
