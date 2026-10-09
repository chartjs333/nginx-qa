# Инструкция агенту: декларативные группы проекта

Groups API создаёт и изменяет проектные группы из серверного реестра шаблонов `group_templates.json`. Идентификатор проекта в URL — только канонический `project_phone`, полученный от Project Manager `0001`.

Базовый URL далее обозначен как `<QA_BASE_URL>`. Отправляй JSON с заголовком `Content-Type: application/json`.

## 1. Получить канонический номер проекта

Сначала зарегистрируй или найди проект:

```http
POST <QA_BASE_URL>/project-manager/0001
Content-Type: application/json

{
  "git_address": "https://github.com/owner/repository.git"
}
```

Возьми верхнеуровневый `project_phone` из ответа. Не используй вместо него Git-адрес, `git_context_key`, порт сервера, номер агента или зарезервированный номер `0001`.

Если у проекта уже есть группы, этот же ответ содержит их количество в `group_count`, декларации в `project.groups` и полные записи относящихся к проекту агентов в `agents`. Поле `cycle_count` показывает число сохранённых циклов разработки. Системный Project Manager `0001` в `agents` проекта не включается.

## 2. Просмотреть доступные шаблоны

```http
GET <QA_BASE_URL>/api/v1/group-templates
```

Ответ содержит декларативные `group_templates`. У шаблона есть `template_id`, допустимые роли в `agent_templates`, внутренние связи и правило итогового отчёта. Значение для `task_template.agents` выбирай только из `agent_templates[].role` выбранного шаблона.

Пример сокращённого ответа:

```json
{
  "schema_version": 1,
  "group_templates": {
    "backend_dev_team_v1": {
      "template_id": "backend_dev_team_v1",
      "name": "Backend Development Team",
      "description": "Команда для backend/API задач",
      "agent_templates": [
        {"role": "system_analyst", "spec": "analytic_v2", "is_entrypoint": true},
        {"role": "backend_developer", "spec": "programmer_backend_v1", "is_entrypoint": false},
        {"role": "qa_engineer", "spec": "qa_tester_v1", "is_entrypoint": false}
      ]
    }
  }
}
```

Реестр управляется сервером. Клиент не присылает произвольные `profile`, `phone`, `agent_id` или локальный путь к prompt.

Чтобы получить один шаблон вместе с относящимися к нему agent specs:

```http
GET <QA_BASE_URL>/api/v1/group-templates/{template_id}
```

В этом же серверном реестре раздел `group_topologies` задаёт связи между шаблонами групп и `customer_reporting`: кто передаёт работу другой группе, по какому событию и кто собирает итоговый отчёт заказчику. Когда в проекте присутствует ровно по одному неархивному экземпляру требуемых шаблонов, система материализует эти декларации в реальные `group_id`, `agent_id`, номера и endpoint’ы. Они возвращаются в `project.group_relationships` и `project.customer_reporting`; завершённая группа остаётся в topology, чтобы сработали правила отчёта по завершению. `paused`/`completed`-получатель при этом не принимает новые задачи. Если экземпляра ещё нет либо один шаблон развёрнут несколько раз и выбор неоднозначен, topology остаётся со статусом `waiting_for_groups` или `pending_binding` и произвольная связь не создаётся.

## 3. Создать группу

```http
POST <QA_BASE_URL>/api/v1/projects/{project_phone}/groups
Content-Type: application/json

{
  "group_key": "auth-module-team",
  "template_id": "backend_dev_team_v1",
  "group_name": "Auth Module Team",
  "task_template": {
    "id": "auth-delivery-v1",
    "agents": [
      "system_analyst",
      "backend_developer",
      "qa_engineer"
    ]
  },
  "custom_connections": []
}
```

`group_key` — стабильный idempotency key группы внутри проекта. `template_id` выбирает серверный group template. `task_template.id` — опциональная provenance-метка задачи или набора работ, а `task_template.agents` — обязательный непустой список уникальных ролей. Порядок ролей не создаёт новую группу и не меняет identity. Сервер выбирает спецификации агентов из реестра, назначает агентам отдельные номера и связывает их с проектом.

Для совместимости также принимается `enabled_roles` вместо `task_template.agents`. Используй каноническое поле `task_template.agents` в новых клиентах.

`custom_connections: []` означает «не переопределять связи» и оставляет граф из выбранного шаблона. Поле `connections: []` имеет другое значение: это явный пустой граф, допустимый только когда все выбранные роли остаются достижимыми (обычно для группы из одного entrypoint). Непустые `custom_connections` и `connections` задают override, который должен ссылаться только на выбранные роли и поддерживаемые очереди. В развёрнутой группе должен быть ровно один entrypoint, а каждая выбранная роль должна быть достижима от него.

Номер проекта используется как `conversation_phone`. Отдельный `agent_phone` используется как адрес получателя. Не подменяй один номер другим.

### Идемпотентность

Повтор того же `group_key` с тем же нормализованным запросом для того же проекта возвращает ту же группу, те же `group_id`, `agent_id` и `agent_phone`. Он не создаёт дубликаты агентов, связей или групп.

Если существует группа с той же identity, но запрос меняет шаблон, task template, описание, имя, набор ролей или граф, сервер возвращает `409`. Для намеренного изменения используй `PUT` существующей группы.

Пример ответа:

```json
{
  "created": true,
  "group": {
    "group_id": "group-backend-dev-team-v1-0123456789ab",
    "project_id": "9000",
    "project_phone": "9000",
    "group_name": "Auth Module Team",
    "template_id": "backend_dev_team_v1",
    "status": "active",
    "revision": 1,
    "agents": [
      {
        "role": "system_analyst",
        "spec": "analytic_v2",
        "agent_id": "agent-...",
        "agent_phone": "4000",
        "is_entrypoint": true
      }
    ],
    "connections": []
  }
}
```

Используй фактические идентификаторы и endpoint'ы из ответа, не вычисляй их самостоятельно.

## 4. Прочитать группы

Список групп проекта:

```http
GET <QA_BASE_URL>/api/v1/projects/{project_phone}/groups
```

По умолчанию список включает архивные записи для аудита. Чтобы получить только неархивные группы, добавь `?include_archived=false`.

Одна группа:

```http
GET <QA_BASE_URL>/api/v1/groups/{group_id}
```

`project_id` в Groups API равен каноническому `project_phone`.

## 5. Изменить группу

```http
PUT <QA_BASE_URL>/api/v1/groups/{group_id}
Content-Type: application/json

{
  "expected_revision": 1,
  "group_name": "Auth Module Delivery Team",
  "task_template": {
    "id": "auth-delivery-v1",
    "agents": [
      "system_analyst",
      "backend_developer",
      "qa_engineer"
    ]
  }
}
```

`PUT` выполняет reconciliation существующей группы. Совпадающие role/spec не получают новых записей или телефонов. Нельзя ссылаться на роль, отсутствующую в выбранном шаблоне, дублировать роли или оставлять связь с исключённым агентом. Значения `paused`, `completed` и `archived` прекращают приём и выдачу новых задач этой группе; вернуть `paused`-группу в работу можно явным `PUT` со статусом `active`.

`expected_revision` обязана совпадать с текущей `revision`. При конфликте ревизий заново прочитай группу и повтори осознанное изменение. Помимо `group_name` и состава ролей разрешены `connections`/`custom_connections`, `reporting_rule` и `status`.

## 6. Удалить группу

```http
DELETE <QA_BASE_URL>/api/v1/groups/{group_id}
```

Удаление выполняется как soft archive: группа и аудит сохраняются, но её `status` становится `archived`, а новые задачи отклоняются. Повторное удаление не должно создавать новую группу или дублировать архивную операцию.

## 7. Поставить внешнюю задачу группе

```http
POST <QA_BASE_URL>/api/v1/groups/{group_id}/tasks
Content-Type: application/json

{
  "message": "Добавить проверку срока действия token: просроченный token отклоняется, валидный продолжает работать.",
  "cycle_title": "Auth token expiration",
  "task_template": {
    "id": "auth-token-exp-v1",
    "agents": ["system_analyst", "backend_developer", "qa_engineer"]
  },
  "request_id": "customer-request-1842"
}
```

`message` обязателен. `cycle_title`, `task_template.id` и `request_id` опциональны, но для повторяемых интеграций передавай стабильный `request_id`. Опциональный `task_template.agents` перечисляет роли из versioned group template, которые требуются задаче. Перед постановкой задачи сервер reconciles недостающие роли в группе; неизвестная шаблону роль отклоняется.

Внешняя задача доставляется единственному entrypoint-агенту группы и открывает цикл разработки. Сохрани возвращённые `cycle_id`, `task_id` и `task_node_id`: они нужны для последующих передач, артефактов и аудита. Если клиент явно передаёт существующий `cycle_id`, система добавляет root-задачу в этот активный цикл; цикл другого проекта или завершённый цикл отклоняется. Этот endpoint не позволяет выбрать произвольного внутреннего получателя. Повтор с тем же `request_id` и тем же содержимым возвращает те же идентификаторы и не ставит вторую копию задачи; другое содержимое с тем же `request_id` является конфликтом. Если процесс сервера перезапустился после записи истории, но до доставки находившейся только в памяти задачи, повтор восстанавливает её в очередь. Уже доставленная задача повторно не ставится.

Сокращённый ответ:

```json
{
  "status": "queued",
  "cycle_id": "cycle-20260804-a1b2c3d4e5f6",
  "task_id": "customer-request-1842",
  "task_node_id": "cycle-task-...",
  "queue_item_id": "...",
  "to_agent_id": "agent-system-analyst-..."
}
```

## 8. Передать задачу по декларативной связи

Используй только `connection_id`, присутствующий в `group.connections`:

```http
POST <QA_BASE_URL>/api/v1/groups/{group_id}/connections/{connection_id}/tasks
Content-Type: application/json

{
  "message": "Реализовать согласованную спецификацию из аналитического отчёта.",
  "from_agent_id": "agent-system-analyst-...",
  "cycle_id": "cycle-20260804-a1b2c3d4e5f6",
  "parent_task_node_id": "cycle-task-...",
  "request_id": "auth-spec-approved-7"
}
```

`from_agent_id` должен совпадать с отправителем сохранённой связи. Для продолжения существующего цикла обязательно передай его `cycle_id` и один из идентификаторов родительской задачи: предпочтительно `parent_task_node_id`, либо `parent_task_id`. Родитель должен существовать в этом цикле. Вызов без `cycle_id` оставлен для совместимости, но открывает отдельный цикл и не продолжает прежнюю историю. Сервер определяет получателя, очередь и телефоны из сохранённой связи. Нельзя передавать собственные `from_phone`, `to_phone`, `to_agent_id`, `queue` или подменять направление связи. Не отправляй внутренние задачи напрямую в `/worker/all`, `/tester/all` или `/consultant/all`: такой обход не подтверждает принадлежность декларативному графу.

## 9. Получить задачи конкретного агента

```http
GET <QA_BASE_URL>/api/v1/groups/{group_id}/agents/{agent_id}/tasks
```

Разрешён только агент, входящий в эту группу. Ответ забирает следующую адресованную ему задачу. `404` означает, что задачи сейчас нет либо указанный ресурс не существует; не опрашивай endpoint чаще интервала, заданного профилем агента.

Ответ содержит сквозные `cycle_id`, `task_id`, `task_node_id`, `parent_task_id` и `parent_task_node_id`. Передавай эти значения дальше без самостоятельного вычисления.

## 10. История цикла разработки

Список циклов проекта:

```http
GET <QA_BASE_URL>/api/v1/projects/{project_phone}/cycles
```

Опциональные query-параметры: `cycle_status=queued|in_progress|completed`, `group_id` и `limit`. Пустой проект возвращает `200`, `cycle_count: 0` и `cycles: []`.

Полная хронологическая лента одного цикла:

```http
GET <QA_BASE_URL>/api/v1/cycles/{cycle_id}/history
```

Система сама создаёт события `CYCLE_STARTED`, `GROUP_DEPLOYED`, `MESSAGE_QUEUED`, `TASK_STARTED` и `HANDOFF_TRIGGERED` при вызовах task/connection/poll endpoint'ов. Каждое событие получает серверные `event_id`, `sequence` и `timestamp`, содержит снимки отправителя, получателя, групп, связи, очереди и lineage задачи. Cycle history является append-only: cycle-события нельзя удалить через History API. Агент не присылает серверные поля вручную.

Фактический граф прохождения задач и коммуникаций:

```http
GET <QA_BASE_URL>/api/v1/cycles/{cycle_id}/graph
```

Ответ содержит task nodes, agent nodes, `task_lineage` и агрегированные `communications`. Граф строится по зафиксированным событиям, а не по текущему шаблону группы, поэтому последующее обновление группы не переписывает прошлое.

Артефакт, итоговый отчёт группы и решение о закрытии регистрируются через единый endpoint:

```http
POST <QA_BASE_URL>/api/v1/cycles/{cycle_id}/events
Content-Type: application/json

{
  "event_type": "ARTIFACT_CREATED",
  "group_id": "group-backend-...",
  "from_agent_id": "agent-backend-developer-...",
  "task_node_id": "cycle-task-...",
  "artifact": {
    "type": "git_commit",
    "ref": "abc123",
    "path": "src/auth.py"
  },
  "request_id": "auth-artifact-1"
}
```

Для отчёта используй `event_type: "GROUP_REPORT_SUBMITTED"` и объект `report`; для принятого заказчиком результата — `event_type: "CYCLE_COMPLETED"`, непустой объект `decision` и при необходимости `message`. Только Project Manager (`agent-project-manager`) закрывает цикл; его можно не указывать явно. Для `ARTIFACT_CREATED` и `GROUP_REPORT_SUBMITTED` обязательны реальные `group_id` и `from_agent_id` участника цикла.

Передавай стабильный `request_id` каждому lifecycle-событию. Точный повтор возвращает существующее событие, а попытка использовать тот же ключ с другим содержимым возвращает `409`. `CYCLE_COMPLETED` удаляет ещё не выданные сообщения этого цикла из рабочих очередей, фиксирует для них `MESSAGE_REMOVED` и возвращает `cancelled_queue_task_count`; после этого новые задачи, handoff'ы и lifecycle-события цикла отклоняются, но history и graph остаются доступны.

## Ошибки

- `400` — неверное JSON-тело, пустой или повторяющийся `task_template.agents`, неизвестная роль, неподдерживаемая связь или попытка передать server-owned routing fields.
- `404` — проект, шаблон, группа, связь, агент или цикл не найдены; для polling также может означать пустую очередь.
- `409` — idempotency conflict, неверная связь цикла/проекта/родительской задачи, завершённый цикл, устаревшая revision, проект ещё не имеет канонического телефона либо изменение оставляет некорректный граф.

Не исправляй `404` или `409` созданием произвольных агентов через `/agents`.

## Локальный контракт

Groups API создаёт локальные записи агентов и ставит сообщения в очереди, но не запускает процессы агентов. Ответ создания группы и `GET /api/v1/groups/{group_id}` возвращают локальному вызывающему полные профили и параметры участников в `agents`/`agent_profiles`. Используй эти данные, чтобы запустить нужных агентов и обращаться к их group-endpoint’ам.
