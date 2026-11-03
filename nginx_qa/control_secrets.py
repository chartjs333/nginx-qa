"""Trusted control credentials are never delegated as child/Git environment."""
from __future__ import annotations

import os
import re


def protected_environment_name(name):
    folded=str(name).casefold()
    return (folded.startswith(('nginx_qa_scope_control_','nginx_qa_launcher_control_'))
            or folded in {'telegram_bot_token','telegram_webhook_secret'})


def sanitized_child_environment(source=None):
    return {key:value for key,value in (os.environ if source is None else source).items()
            if not protected_environment_name(key)}


def trusted_control_resolver(reference):
    provider,separator,locator=reference.partition(':')
    if (provider!='env' or separator!=':' or not re.fullmatch(r'NGINX_QA_LAUNCHER_CONTROL_[A-Z0-9_]+',locator)):
        raise LookupError('launcher control reference must name the protected namespace')
    value=os.environ.get(locator)
    if not isinstance(value,str) or not value:
        raise LookupError('launcher control credential is unavailable')
    return value


def control_secret(purpose, *, allow_legacy=True):
    names={
        'bot':('NGINX_QA_LAUNCHER_BOT_SECRET_REF','NGINX_QA_LAUNCHER_CONTROL_BOT_TOKEN','TELEGRAM_BOT_TOKEN'),
        'webhook':('NGINX_QA_LAUNCHER_WEBHOOK_SECRET_REF','NGINX_QA_LAUNCHER_CONTROL_WEBHOOK_SECRET','TELEGRAM_WEBHOOK_SECRET'),
        'executor':('NGINX_QA_LAUNCHER_EXECUTOR_SECRET_REF','NGINX_QA_LAUNCHER_CONTROL_EXECUTOR_TOKEN',None),
    }
    if purpose not in names:
        raise LookupError('unknown launcher control purpose')
    setting,default,legacy=names[purpose]
    reference=os.environ.get(setting)
    if reference or os.environ.get(default):
        return trusted_control_resolver(reference or 'env:'+default)
    return os.environ.get(legacy,'') if allow_legacy and legacy else ''
