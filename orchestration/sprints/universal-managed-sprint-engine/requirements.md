# ТЗ: Universal Managed Workspace & Continuous Sprint Engine

**Проект:** `chartjs333/nginx-qa`  
**База разработки:** `agent/groups-cycles-graph-ui@3c42efc1701a0297d3563d7d31e66e532b072394`  
**Тип документа:** implementation-ready specification  
**Дата:** 2026-10-01

## 1. Цель

Добавить в nginx-qa новый, явно типизированный режим спринта, который управляет
Git-репозиториями, изолированными рабочими директориями, локальными процессами,
портами, ветками, handoff и recovery без ручного `git clone/worktree/switch`.

Новая логика не должна менять поведение уже работающих legacy-спринтов.

## 2. Обязательная обратная совместимость

Вводится необязательное верхнеуровневое поле:

```json
{"sprint_type": "managed_workspace_v1"}
```

Правила dispatch:

1. `sprint_type` отсутствует — выполнить **точно существующую legacy-логику**.
2. `sprint_type="legacy_v1"` — явный вызов существующей legacy-логики.
3. `sprint_type="managed_workspace_v1"` — новый managed workspace engine.
4. Неизвестное значение — `SPRINT_TYPE_UNSUPPORTED` до любых runtime mutations.
5. `execution.mode` (`sequential`/`parallel`) остаётся отдельным понятием и не
   подменяется `sprint_type`.
6. Старые sprint JSON, архивы, pending sprints, очереди, agents, endpoints и UI
   должны работать без миграции.
7. Экспорт/архив нового спринта сохраняет `sprint_type`; старый архив без поля
   читается как legacy.
8. UI показывает type badge, но отсутствие badge не меняет legacy workflow.

## 3. Изоляция уже работающего nginx-qa

Во время разработки и квалификации текущая работающая установка nginx-qa является
**live baseline** и не изменяется.

Новый код должен запускаться только из отдельной копии:

```text
staging root: D:\nginx-qa-staging\universal-managed-sprint-engine
staging host: 127.0.0.1
staging port: 18025
child process port pool: 18100-18199
```

Все значения должны быть конфигурируемыми environment variables; указанные значения
являются Windows defaults для данного спринта.

Жёсткие правила:

- не выполнять checkout/reset/clean/install в каталоге live instance;
- не использовать live `.venv`;
- не читать и не изменять live `runtime_state`;
- не копировать live queue/pending/agent state в staging;
- не регистрировать Telegram webhook из staging;
- не запускать staging tunnel по умолчанию;
- не занимать live port 8025/8026;
- не останавливать и не перезапускать live process;
- до promotion разрешены только read-only health checks live endpoint;
- staging имеет отдельные `.env`, `.venv`, logs, runtime state, prompt archive,
  PID/lease files и workspace root;
- promotion в live не автоматическая: итоговый node формирует release plan и требует
  отдельного operator action вне этого sprint.

## 4. Managed repository cache

Для каждого repository nginx-qa создаёт managed bare mirror:

```text
<managed_root>/repositories/<repository_key>.git
```

Требования:

- canonical repository identity не зависит от пользовательского checkout path;
- fetch выполняется в mirror с process timeout и captured stdout/stderr;
- credentials передаются через credential references, не sprint JSON;
- mirror не используется как рабочее дерево;
- одна блокировка fetch на repository;
- несколько assignments могут читать один mirror;
- mirror update не изменяет уже pinned source commit активного assignment.

## 5. Managed workspace

Каждый write-assignment получает отдельный workspace:

```text
<managed_root>/projects/<project_id>/sprints/<sprint_id>/nodes/<node_id>/<assignment_id>/
```

Перед изменяющей Git-операцией проверяются:

```text
expected workspace root
actual git toplevel
actual git dir
repository remote
source commit
assigned branch
working tree state
```

Обязательный invariant:

```text
actual_git_toplevel == expected_workspace_root
```

При несовпадении — `WORKSPACE_ROOT_MISMATCH`, без `reset`, `clean`, `checkout` или
других изменяющих действий.

Запрещено автоматически выполнять:

```text
git reset --hard
git clean -fd
git checkout -f
git restore .
force push
```

Dirty workspace сохраняется и направляется в Coordinator как `WORKSPACE_DIRTY`.

## 6. Ветки и ownership

Manifest задаёт:

```json
{
  "git": {
    "source_ref": "refs/heads/main",
    "expected_source_commit": null,
    "assigned_branch": "agent/example",
    "existing_branch_policy": "resume"
  }
}
```

Поддерживаемые политики:

- `create`;
- `resume`;
- `reject_if_exists`;
- `require_exact_head`.

Одна write-ветка не может одновременно принадлежать двум активным assignments.
Read-only workspaces могут использовать один commit параллельно.

Diverged branch не reset-ится; результат — `BRANCH_DIVERGED` → Coordinator.

## 7. Параллельные процессы

`managed_workspace_v1` должен поддерживать несколько одновременных процессов.

Для каждого process assignment выделяются:

- отдельный workspace;
- отдельный runtime root;
- уникальный localhost port lease;
- PID/process-group record;
- stdout/stderr logs;
- health endpoint;
- resource limits;
- owner assignment ID.

Port allocator:

- configurable range;
- durable lease;
- проверка фактического bind;
- recovery после restart;
- release только после подтверждённого process exit;
- один port не выдаётся двум живым process records.

В тестах обязательно одновременно запустить минимум 4 managed workspaces/processes
без пересечения файлов, runtime state и портов.

## 8. Process supervisor

Добавить универсальный supervisor:

```text
PREPARED → STARTING → HEALTHY → STOPPING → STOPPED
                     ↘ FAILED
```

Требования:

- process стартует только в managed workspace;
- host по умолчанию `127.0.0.1`;
- command/env/cwd записываются без секретов;
- process group завершается целиком;
- restart policy задаётся manifest/policy;
- после restart nginx-qa восстанавливает process records и проверяет реальные PID/ports;
- orphan process не получает новый assignment автоматически;
- live nginx-qa process не может быть выбран supervisor как managed child.

## 9. Импорт спринта из Git

Новый API:

```http
POST /api/v1/projects/{project_id}/sprints/start-from-git
```

Request:

```json
{
  "repository_id": "main",
  "ref": "refs/heads/sprint-definition",
  "manifest_path": "orchestration/sprint.json",
  "idempotency_key": "..."
}
```

Система:

1. fetch mirror;
2. resolve ref → immutable commit;
3. read manifest from exact commit;
4. validate JSON/schema/type;
5. perform graph/workspace/checksum preflight;
6. prepare runtime state;
7. atomically activate sprint;
8. create first assignment.

Sprint identity:

```text
project_id + repository_id + commit + manifest_path + manifest_sha256
```

## 10. Transactional import

Стадии:

```text
VALIDATE → PREPARE → ACTIVATE
```

До `ACTIVATE` нельзя менять текущих agents, queues, execution state или active sprint.

Недопустимое состояние:

```text
status=active
workflow=null
current_node_id=null
allowed_outcomes=[]
```

должно отклоняться invariant checker и никогда не сохраняться.

## 11. Preflight

До запуска проверяются:

- sprint type;
- schema version;
- start node;
- transitions и targets;
- terminal reachability;
- task/agent/node uniqueness;
- reviewers;
- coordinator routing;
- rework limits;
- source ref/commit;
- manifest/checksum files;
- branch ownership;
- workspace root policy;
- port range;
- runtime-root isolation;
- live/staging port conflict;
- secrets absence.

Ошибка preflight не создаёт assignment.

## 12. Continuity Coordinator

Универсальный Coordinator получает:

- current graph revision;
- failed/blocked assignment;
- branch/source/result commits;
- diff summary;
- workspace status;
- process/port records;
- test/evidence summary;
- normalized error;
- reviewer feedback.

Он может:

- исправить sprint-owned manifest/checksum/future transitions;
- повторить idempotent handoff;
- направить node на rework;
- изменить future plan;
- создать recovery record;
- продолжить тот же node.

Он не может:

- переписать accepted result;
- изменить completed-node source/result commit;
- пропустить mandatory reviews;
- выдавать себя за независимого reviewer;
- force-push;
- выполнять destructive Git recovery;
- менять project source вне разрешённого ownership.

## 13. Durable handoff

Result key:

```text
assignment_id + outcome + result_commit
```

Повтор того же result возвращает `ALREADY_ACCEPTED`.

Transition journal:

```text
RESULT_RECEIVED
RESULT_VALIDATED
REVIEWS_PENDING
REVIEWS_ACCEPTED
TRANSITION_COMMITTED
NEXT_ASSIGNMENT_ENQUEUED
```

После crash переход восстанавливается idempotently.

## 14. Hot repair

Новый API:

```http
POST /api/v1/sprints/{sprint_id}/repair
```

Разрешено менять только future nodes, prompts, profiles, reviewer metadata,
coordinator routing и checksum metadata.

Запрещено менять completed node, accepted result/reviews, assignment ID и source/result
commit завершённой работы.

## 15. Конфигурация путей и портов

Добавить environment/config values:

```text
NGINX_QA_HTTP_HOST
NGINX_QA_HTTP_PORT
NGINX_QA_RUNTIME_ROOT
NGINX_QA_PROMPT_ROOT
NGINX_QA_MANAGED_ROOT
NGINX_QA_CHILD_PORT_RANGE
NGINX_QA_INSTANCE_ID
NGINX_QA_DISABLE_TELEGRAM
NGINX_QA_DISABLE_TUNNEL
```

Legacy defaults остаются прежними:

```text
host=0.0.0.0
port=8025
runtime_root=runtime_state
```

Новые staging launcher/tests обязаны задавать отдельные значения явно.

## 16. Предлагаемая модульная структура

Не расширять монолит `main.py` всей новой логикой. Вынести, например:

```text
nginx_qa/
  sprint_types.py
  sprint_preflight.py
  sprint_runtime.py
  workspace_manager.py
  git_provider.py
  branch_leases.py
  port_leases.py
  process_supervisor.py
  continuity.py
  runtime_config.py
```

`main.py` сохраняет совместимый FastAPI entry point и подключает сервисы.

## 17. Тесты

Добавить отдельные test families:

```text
tests/test_sprint_types.py
tests/test_managed_workspaces.py
tests/test_workspace_safety.py
tests/test_branch_leases.py
tests/test_port_leases.py
tests/test_process_supervisor.py
tests/test_transactional_sprint_import.py
tests/test_idempotent_handoff.py
tests/test_continuity_coordinator.py
tests/test_staging_isolation.py
tests/test_legacy_sprint_regression.py
```

Обязательные сценарии:

1. старый JSON без `sprint_type` даёт прежний результат;
2. `legacy_v1` эквивалентен отсутствующему field;
3. unknown type отвергается до mutation;
4. managed workspace не использует parent `.git`;
5. drive root/home/install root отвергаются;
6. dirty workspace не очищается;
7. branch divergence не reset-ится;
8. четыре процесса получают разные directories/ports;
9. restart восстанавливает leases;
10. checksum mismatch обнаруживается preflight;
11. invalid graph не создаёт assignment;
12. duplicate result idempotent;
13. crash transition восстанавливается;
14. STOP маршрутизируется Coordinator;
15. hot repair не переписывает completed history;
16. staging process работает на `18025`;
17. live service остаётся доступным и неизменённым;
18. staging не читает live runtime files;
19. legacy pending/Telegram/direct import tests проходят;
20. Windows launcher и Linux command path покрыты.

## 18. Promotion gate

До promotion должны быть:

- full unit/integration suite;
- clean staging restart;
- 4-process concurrency test;
- legacy regression;
- source-bound evidence package;
- два reviewer approvals;
- explicit operator-approved deployment plan.

Sprint не перезапускает live instance и не копирует staging runtime state в live.

## 19. Definition of Done

Feature считается готовой, если:

- новый `managed_workspace_v1` работает в staging;
- legacy behavior byte/semantic-compatible по тестам;
- active live instance не был остановлен или изменён;
- process/workspace/port isolation доказана тестами;
- transactional import и idempotent handoff проходят crash tests;
- documentation/API/examples обновлены;
- release qualification выдала PASS;
- promotion остаётся отдельной явной операцией.
