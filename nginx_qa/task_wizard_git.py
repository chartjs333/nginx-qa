"""Immutable input topic plumbing through the existing managed Git provider."""
from __future__ import annotations
import hashlib
import re
import tempfile
from pathlib import Path
from .git_provider import ManagedGitError
from .sprint_types import git_ref_format_valid


def remote_sources(provider, repository, *, contains=None):
    """Only the registered origin's fetched heads confer source authority.

    Local assignments, private pins, tags and arbitrary revision expressions
    are deliberately excluded, even when their objects exist in this mirror.
    """
    args = ('--git-dir', str(repository.mirror_path), 'for-each-ref', '--count=2001',
            '--format=%(refname)%00%(objectname)')
    if contains is not None:
        provider.assert_commit(repository, contains)
        args += ('--contains=' + contains,)
    raw = provider.runner.run_raw_stdout((*args, 'refs/remotes/origin/'), max_stdout_bytes=2 * 1024 * 1024)
    lines = raw.decode('utf-8').splitlines()
    if len(lines) > 2000:
        raise ValueError('TASK_SOURCE_LIMIT')
    rows = []
    for line in lines:
        ref, commit = line.split('\0', 1)
        branch = ref.removeprefix('refs/remotes/origin/')
        if ref == 'refs/remotes/origin/HEAD' or not ref.startswith('refs/remotes/origin/'):
            continue
        if not git_ref_format_valid(branch, branch=True):
            continue
        size = 40 if repository.object_format == 'sha1' else 64
        if not re.fullmatch('[0-9a-f]{' + str(size) + '}', commit):
            raise ValueError('TASK_SOURCE_INVALID')
        rows.append({'ref': 'refs/heads/' + branch, 'commit': commit})
    return rows


def select_source(provider, repository, selection):
    if (not isinstance(selection, dict) or set(selection) != {'kind', 'value'}
            or not isinstance(selection['value'], str)):
        raise ValueError('TASK_SOURCE_INVALID')
    kind, value = selection['kind'], selection['value']
    if kind == 'branch':
        if not value.startswith('refs/heads/') or not git_ref_format_valid(value):
            raise ValueError('TASK_SOURCE_INVALID')
        for row in remote_sources(provider, repository):
            if row['ref'] == value:
                provider.assert_commit(repository, row['commit'])
                return {**selection, 'commit': row['commit']}
        raise ValueError('TASK_SOURCE_NOT_REMOTE')
    if kind == 'commit':
        size = 40 if repository.object_format == 'sha1' else 64
        if not re.fullmatch('[0-9a-f]{' + str(size) + '}', value):
            raise ValueError('TASK_SOURCE_INVALID')
        try:
            if remote_sources(provider, repository, contains=value):
                return {**selection, 'commit': value}
        except ManagedGitError:
            pass
        raise ValueError('TASK_SOURCE_NOT_REMOTE')
    raise ValueError('TASK_SOURCE_INVALID')


def source_intent(provider, repository, task_id, commit):
    if not re.fullmatch(r'task-[0-9a-f]{32}', task_id):
        raise ValueError('TASK_ID_INVALID')
    provider.pin_commit(repository, commit, owner_id='task-source:' + task_id)
    return {'commit': commit, 'ref': 'refs/heads/agent/task-source/' + task_id}


def publish_source(provider, repository, task_id, intent):
    if (set(intent) != {'commit', 'ref'}
            or intent['ref'] != 'refs/heads/agent/task-source/' + task_id
            or provider.pinned_commit(repository, owner_id='task-source:' + task_id) != intent['commit']):
        raise ValueError('TASK_SOURCE_INTENT_CHANGED')
    provider.assert_commit(repository, intent['commit'])
    _publish_topic(provider, repository, intent)


def prepare_input_commit(provider, repository, *, task_id, source_commit, attachment, created_at):
    if not re.fullmatch(r'task-[0-9a-f]{32}', task_id) or not 0 < len(attachment) <= 2 * 1024 * 1024:
        raise ValueError('TASK_INPUT_INVALID')
    provider.assert_commit(repository, source_commit)
    provider._verify_bare_mirror(repository.spec, repository.mirror_path)
    args = ('--git-dir', str(repository.mirror_path))
    name = task_id + '.xlsx'
    entries = provider.runner.run_raw_stdout((*args, 'ls-tree', source_commit), max_stdout_bytes=65536).decode('utf-8')
    if any(line.rsplit('\t', 1)[-1] == name for line in entries.splitlines()):
        raise ValueError('TASK_INPUT_PATH_CONFLICT')
    with tempfile.TemporaryDirectory(prefix='task-input-', dir=repository.mirror_path.parent) as directory:
        path = Path(directory) / 'input.xlsx'
        provider._assert_internal_path(path)
        path.write_bytes(attachment)
        blob = provider.runner.run((*args, 'hash-object', '-w', '--no-filters', '--', str(path))).stdout.strip()
    tree = provider.runner.run((*args, 'mktree'), stdin_text=entries.rstrip('\n') + f'\n100644 blob {blob}\t{name}\n').stdout.strip()
    env = {k: v for k, v in {
        'GIT_AUTHOR_NAME': 'nginx-qa Task Wizard', 'GIT_COMMITTER_NAME': 'nginx-qa Task Wizard',
        'GIT_AUTHOR_EMAIL': 'task@nginx-qa.invalid', 'GIT_COMMITTER_EMAIL': 'task@nginx-qa.invalid',
        'GIT_AUTHOR_DATE': created_at, 'GIT_COMMITTER_DATE': created_at}.items()}
    commit = provider.runner.run((*args, 'commit-tree', tree, '-p', source_commit),
        stdin_text=f'Input for {task_id}\n', environment=env).stdout.strip()
    provider.pin_commit(repository, commit, owner_id='task-input:' + task_id)
    return {'commit': commit, 'ref': 'refs/heads/agent/task-input/' + task_id,
            'path': name, 'sha256': hashlib.sha256(attachment).hexdigest(), 'parent': source_commit}


def publish_input(provider, repository, task_id, intent):
    if (intent['ref'] != 'refs/heads/agent/task-input/' + task_id or intent['path'] != task_id + '.xlsx'
            or provider.pinned_commit(repository, owner_id='task-input:' + task_id) != intent['commit']):
        raise ValueError('TASK_INPUT_INTENT_CHANGED')
    raw = provider.read_blob(repository, intent['commit'], intent['path'], max_bytes=2 * 1024 * 1024)
    if hashlib.sha256(raw).hexdigest() != intent['sha256']:
        raise ValueError('TASK_INPUT_INTENT_CHANGED')
    args = ('--git-dir', str(repository.mirror_path))
    parent = provider.runner.run((*args, 'rev-list', '--parents', '-n', '1', intent['commit'])).stdout.split()
    if parent != [intent['commit'], intent['parent']]:
        raise ValueError('TASK_INPUT_INTENT_CHANGED')
    _publish_topic(provider, repository, intent)


def _publish_topic(provider, repository, intent):
    """Create only; never update an existing source or previous attempt."""
    provider._verify_bare_mirror(repository.spec, repository.mirror_path)
    args = ('--git-dir', str(repository.mirror_path))
    env = provider._credential_environment(repository.spec)
    def head():
        output = provider.runner.run((*args, 'ls-remote', '--refs', 'origin', intent['ref']), environment=env).stdout.split()
        if not output:
            return None
        if len(output) != 2 or output[1] != intent['ref']:
            raise ValueError('TASK_INPUT_REMOTE_INVALID')
        return output[0]
    current = head()
    if current == intent['commit']:
        return
    if current is not None:
        raise ValueError('TASK_INPUT_PUBLICATION_CONFLICT')
    try:
        provider.runner.run((*args, 'push', f"--force-with-lease={intent['ref']}:", 'origin',
                             f"{intent['commit']}:{intent['ref']}"), environment=env)
    except ManagedGitError:
        if head() != intent['commit']:
            raise
    if head() != intent['commit']:
        raise ValueError('TASK_INPUT_PUBLICATION_UNCONFIRMED')
