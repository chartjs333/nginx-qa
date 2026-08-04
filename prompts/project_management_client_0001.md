# Инструкция агенту: регистрация текущего проекта через Project Manager `0001`

Project Manager находит проект по Git-адресу и возвращает его постоянный номер и полные записи агентов этого проекта. Если Git ещё не зарегистрирован, менеджер создаёт чистый проект, резервирует для него свободный номер из диапазона `9000–9999` и не создаёт агентов. Менеджер не клонирует репозиторий и не обращается к Git remote по сети.

Существующий проект никогда не создаётся повторно. Для него ответ всегда содержит `created: false` и ранее назначенный `project_phone`. Если старый проект ещё не имеет номера, менеджер назначает его один раз; следующие запросы возвращают тот же номер.

## Определение текущего проекта

Регистрируй проект, в котором находится текущая рабочая директория агента.

Не используй Git-адрес из примеров как фактическое значение `git_address`. Не придумывай адрес и не подставляй адрес другого проекта.

Перед запросом:

1. Определи корень Git-репозитория текущей рабочей директории:

```bash
git rev-parse --show-toplevel
```

2. Получи адрес `origin` именно этого репозитория:

```bash
git remote get-url origin
```

3. Передай полученный безопасный адрес без изменений в поле `git_address`.

Если текущая директория не находится в Git-репозитории или remote `origin` отсутствует, не отправляй запрос. Сообщи пользователю, что Git-адрес текущего проекта определить невозможно.

Если URL содержит пароль, токен или HTTP(S)-учётные данные, не отправляй и не показывай их. Сообщи пользователю, что `origin` необходимо заменить безопасным URL. Обычный SSH-пользователь в форме `git@host:owner/repo.git` допустим, но никогда не помещай токен на место этого пользователя.

## Основной запрос

Используй базовый URL запущенного QA-сервиса. Для локального экземпляра по умолчанию это `http://localhost:8025`; для удалённого сервера или запуска через `run_8026.bat` замени host/port.

Отправь строго `POST` на фиксированный путь:

```text
<QA_BASE_URL>/project-manager/0001
```

Используй только доверенный `QA_BASE_URL` во внутренней сети или за настроенным reverse proxy. Сам endpoint не выполняет аутентификацию и не должен публиковаться напрямую в открытый интернет.

Номер `0001` является строкой с ведущими нулями и зарезервирован для Project Manager. Не подставляй вместо него номер проекта или агента.

Обязательные заголовок и JSON-тело:

```http
POST /project-manager/0001 HTTP/1.1
Host: localhost:8025
Content-Type: application/json

{
  "git_address": "<адрес из git remote get-url origin>"
}
```

Не передавай желаемый номер проекта в запросе. Project Manager сам выбирает свободный `project_phone` из диапазона `9000–9999` и сохраняет привязку к Git context.

### Пример для Bash

```bash
git_root="$(git rev-parse --show-toplevel)" || exit 1
git_address="$(git -C "$git_root" remote get-url origin)" || exit 1
payload="$(python -c 'import json, sys; print(json.dumps({"git_address": sys.argv[1]}))' "$git_address")" || exit 1

curl -X POST "http://localhost:8025/project-manager/0001" \
  -H "Content-Type: application/json" \
  --data "$payload"
```

### Пример для PowerShell

```powershell
$gitRoot = git rev-parse --show-toplevel
if ($LASTEXITCODE -ne 0) {
    throw "Текущая директория не находится в Git-репозитории"
}

$gitAddress = git -C $gitRoot remote get-url origin
if ($LASTEXITCODE -ne 0) {
    throw "У текущего Git-репозитория отсутствует remote origin"
}

$payload = @{
    git_address = $gitAddress
} | ConvertTo-Json -Compress

$response = Invoke-RestMethod `
    -Method Post `
    -Uri "http://localhost:8025/project-manager/0001" `
    -ContentType "application/json" `
    -Body $payload

$response.project_phone
```

## Номер проекта

- `project_manager_phone` всегда равен строке `"0001"`; это адрес самого Project Manager.
- `project_phone` — постоянный номер найденного или созданного проекта из диапазона `9000–9999`.
- `phone_assigned: true` означает, что номер был назначен именно во время текущего запроса.
- `phone_assigned: false` означает, что проект уже имел номер и менеджер вернул существующую привязку.
- Номер считается частью проекта: повторные запросы не должны менять его или резервировать дополнительные номера.

Смысл флагов:

| Состояние до запроса | `created` | `phone_assigned` | Результат |
|---|---:|---:|---|
| Git ещё не зарегистрирован | `true` | `true` | Создан проект и назначен новый `project_phone` |
| Проект уже имеет номер | `false` | `false` | Возвращён ранее назначенный `project_phone` |
| Legacy-проект существует без номера | `false` | `true` | Проект не пересоздан; номер назначен один раз |
| Повторный запрос legacy-проекта | `false` | `false` | Возвращён тот же номер |

`created` сообщает только о создании проекта. Назначение номера существующему legacy-проекту не меняет `created` на `true`.

## Успешный ответ

Успешный resolve-or-create возвращает HTTP `200`.

Пример первого ответа для нового проекта:

```json
{
  "project_manager_phone": "0001",
  "project_phone": "9000",
  "phone_assigned": true,
  "created": true,
  "project": {
    "project_name": "current-project",
    "git_address": "https://github.com/owner/current-project.git",
    "git_context_key": "github.com/owner/current-project",
    "project_phone": "9000",
    "project_id": "9000",
    "groups": [],
    "group_relationships": [],
    "customer_reporting": {},
    "phones": ["9000"],
    "ports": ["8025"]
  },
  "agent_count": 0,
  "agents": [],
  "group_count": 0,
  "cycle_count": 0
}
```

Повторный запрос с эквивалентным Git-адресом возвращает тот же `project_phone`, но уже с `created: false` и `phone_assigned: false`. Он не создаёт второй проект и не занимает новый номер.

Вложенное `project.project_phone` совпадает с верхнеуровневым `project_phone`. В `project.phones` перечислены все известные номера Git context; массив может содержать дополнительные исторические привязки. Для новых phone-bound запросов используй канонический номер из верхнеуровневого поля `project_phone`.

Для существующего проекта `agents` содержит полные локальные записи его агентов. В ответ входят `id`, `name`, `phone`, `profile`, `parameters`, `template_source`, `status` и другие сохранённые нормализованные поля. Вызывающий агент должен использовать `profile` и `parameters`, чтобы понять роли, настройки и рабочие endpoint’ы команды проекта.

`project.groups` содержит сохранённые экземпляры групп проекта: состав ролей, назначенные `agent_id`/`agent_phone`, внутренние queue-связи, reporting rule, состояние и revision. `project.group_relationships` содержит материализованные связи между группами, а `project.customer_reporting` — правила отчёта заказчику. Верхнеуровневый `group_count` равен числу сохранённых групп, включая архивные.

Верхнеуровневый `cycle_count` показывает число зафиксированных циклов разработки проекта. Их сводки, полную историю и граф получай через Cycles API, описанный ниже.

Пример одной записи:

```json
{
  "id": "agent-example-programmer",
  "name": "Example Programmer",
  "phone": "9101",
  "profile": "Полная инструкция агента...",
  "parameters": {
    "git_context_key": "github.com/owner/current-project",
    "project_phone": "9000"
  },
  "template_source": "prompts/programmer.md",
  "status": "active"
}
```

## Нормализация Git-адреса

Варианты HTTPS/SSH, стандартные порты, завершающий `/` и суффикс `.git` приводятся к одному ключу репозитория. Регистр host всегда игнорируется; регистр пути игнорируется для GitHub, но сохраняется для других серверов, где `Org/Repo` и `org/repo` могут быть разными репозиториями.

Поле `git_address` в ответе может сохранять безопасную форму первого зарегистрированного адреса. Для сравнения проектов используй `git_context_key`, а для адресации очередей — `project_phone`.

## Использование `project_phone`

После успешного ответа возьми `project_phone` без преобразований и подставляй его вместо `{phone}` во все endpoint’ы, привязанные к проекту:

```text
GET  <QA_BASE_URL>/work/{phone}
POST <QA_BASE_URL>/work/{phone}
GET  <QA_BASE_URL>/test/{phone}
POST <QA_BASE_URL>/test/{phone}
GET  <QA_BASE_URL>/work-design/{phone}
POST <QA_BASE_URL>/work-design/{phone}
GET  <QA_BASE_URL>/test-design/{phone}
POST <QA_BASE_URL>/test-design/{phone}
```

Например, при `"project_phone": "9000"` используй `/work/9000` и `/test/9000`.

Не используй `0001` в phone-bound URL проекта: это зарезервированный номер менеджера. Не выбирай новый номер самостоятельно и не заменяй уже возвращённый `project_phone` другим свободным номером.

## Если у одного Git несколько проектов

При неоднозначности сервер вернёт HTTP `409` и массив `detail.candidates`. До выбора кандидата новый проект и новый номер не создаются.

Выбери нужный проект на основании текущего контекста. Если выбор неоднозначен, покажи кандидатов пользователю и попроси выбрать.

Без изменений скопируй выбранный `git_context_key` из массива и повтори запрос:

```json
{
  "git_address": "<адрес из git remote get-url origin>",
  "git_context_key": "<точный ключ из detail.candidates>"
}
```

Опциональное поле `project_name` можно передать при первом создании нового проекта. Для уже существующего проекта оно ничего не переименовывает.

Не придумывай новый `git_context_key`, когда для Git уже есть кандидаты. Неизвестный или опечатанный ключ вернёт `409` и не создаст лишний проект или номер.

## Ошибки

- `400` — отсутствует JSON-тело, неверен `Content-Type`, нет `git_address`, адрес имеет неверный формат либо содержит пароль или HTTP(S)-credentials.
- `409` — нужно выбрать проект из `detail.candidates`, переданный `git_context_key` отсутствует среди кандидатов, в диапазоне `9000–9999` нет свободного project phone либо сохранённый номер конфликтует с другим Git context (`detail.error: "project_phone_conflict"`).
- `404` — использован другой endpoint, например `/project-manager/0002`.

После `400` исправь запрос. После неоднозначного `409` повтори его с одним из предложенных `git_context_key`.

Не создавай проект, номер или агента обходными запросами.

## После регистрации: Groups API

Для управления группами передавай полученный `project_phone` как `{project_id}` без изменений:

```text
GET  <QA_BASE_URL>/api/v1/group-templates
GET  <QA_BASE_URL>/api/v1/projects/{project_id}/groups
POST <QA_BASE_URL>/api/v1/projects/{project_id}/groups
GET  <QA_BASE_URL>/api/v1/projects/{project_id}/cycles
```

Каждая внешняя задача группы открывает или продолжает цикл разработки и возвращает `cycle_id`. Полную ленту и фактический граф взаимодействий получай через `GET /api/v1/cycles/{cycle_id}/history` и `GET /api/v1/cycles/{cycle_id}/graph`. Артефакты, отчёты групп и решение заказчика о завершении регистрируются через `POST /api/v1/cycles/{cycle_id}/events`.

Полный контракт создания, обновления, очередей групп и истории циклов описан в `prompts/group_management_api.md`. В URL Groups/Cycles API нельзя использовать Git-адрес или `git_context_key`: для project-scoped URL допустим только канонический номер проекта.
