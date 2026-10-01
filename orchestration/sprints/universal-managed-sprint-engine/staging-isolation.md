# Staging isolation contract

## Live instance

Текущая работающая установка nginx-qa считается внешним live baseline.

Запрещено:

- изменять её source checkout;
- использовать её `.venv`;
- запускать тесты, меняющие live project state;
- читать/копировать live `runtime_state` как test fixture;
- останавливать или перезапускать live server;
- регистрировать staging Telegram webhook на live bot;
- использовать live port.

## Staging defaults

```text
root: D:\nginx-qa-staging\universal-managed-sprint-engine
host: 127.0.0.1
HTTP port: 18025
managed child ports: 18100-18199
```

Staging создаётся из Git в новом каталоге. Если каталог существует, имеет неизвестное
содержимое или dirty state, запуск fail closed; автоматическая очистка запрещена.

## Separate state

Staging использует отдельные:

- `.venv`;
- `.env.staging`;
- `runtime_state`;
- queue files;
- pending sprint state;
- project registry;
- prompt/archive path;
- logs;
- tunnel files;
- PID/process records;
- repository mirror/workspace root.

## Staging launcher

Нужен `run_staging.ps1` и/или `run_staging.bat`, который явно задаёт:

```text
NGINX_QA_HTTP_HOST=127.0.0.1
NGINX_QA_HTTP_PORT=18025
NGINX_QA_RUNTIME_ROOT=<stage>/runtime_state
NGINX_QA_PROMPT_ROOT=<stage>/prompt
NGINX_QA_MANAGED_ROOT=<stage>/managed
NGINX_QA_CHILD_PORT_RANGE=18100-18199
NGINX_QA_INSTANCE_ID=universal-managed-sprint-engine-staging
NGINX_QA_DISABLE_TELEGRAM=1
NGINX_QA_DISABLE_TUNNEL=1
```

Launcher должен отказаться от запуска, если порт занят или runtime root указывает
в live directory.

## Live health guard

Перед и после staging test suite выполнить read-only health request к live instance.
Любое изменение live runtime files/mtime, PID или port ownership — тестовый FAIL.

## Promotion

Promotion node формирует:

- commit/PR;
- migration notes;
- config defaults;
- backup/rollback plan;
- operator commands.

Он не выполняет live deployment.
