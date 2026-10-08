# Асинхронные human decisions по email

Base: `8581390511838efb15a69cb2fadac89b12483b3b`.
Релиз устанавливается только по итоговому exact SHA из delivery report, не по tip
ветки `codex/async-human-decision-email`. Эта итерация **не разрешает deployment**.

## Что реализовано

`Pending decision → notification outbox → SMTP → /execution?notification=ID →
operator pairing → Preview → Approve / Reject → существующий scope workflow`.

Механизм общий для legacy sequential, legacy parallel и managed runtime. Имена
проектов, номера ролей и узлы не зашиты в production code. Только новый, всё ещё
актуальный `pending` human scope request требует письма. Обычные задания,
review/ACK/transition, progress и remediation не создают уведомлений.
`approved_pending_application` с ранее выданным разрешением не запрашивает новое
согласие и не отправляет human-decision email.

Письмо содержит короткие Project/Sprint/Node/Role/требование/причину и ссылку.
Статус `delivered` означает **SMTP acceptance**, не доказательство получения
или прочтения. Полные инструкции, restrictions, immutable source и exact diff
показывает штатная карточка `/execution`.

Обычные Graph, Current assignment, Pending decisions, Timeline, scope revisions,
ACK и historical snapshots сохранены. Добавлены delivery status, Retry notification
и отдельно помеченный notification audit, не изображающий переходы графа.
Существующий обычный Edit не расширен; в email-bound режиме только Approve/Reject.

## Полномочия и безопасность

Ссылка содержит случайный 128-битный публичный идентификатор, **не capability**.
Он не предоставляет admin/API-доступ и не заменяет operator authentication.
Не нужно пересылать или вводить bearer token в браузере. Используется существующее
pairing: браузер получает HttpOnly SameSite=Strict cookie и показывает код; локальный
доверенный операторский клиент подтверждает код через существующий
`POST /api/v1/operator/session/authorize`. Admin token остаётся только в локальном
защищённом клиенте. Browser POST требует действующую session, CSRF и same-origin.
После server restart операторские in-memory sessions нужно привязать снова.

GET страницы, GET notification, GET observability, polling и scanner/HEAD **не**
создают request, validation, decision, ACK, claim/dequeue, transition, retry или
notification audit. Explicit Preview — отдельный авторизованный POST; он сохраняет
обычную v2 validation, но не принимает решение. Approve/Reject требуют отдельного
подтверждения. Они не завершают assignment и не продвигают execution graph.

Первое notification binding неизменно: runtime/project/sprint/request,
request SHA-256, полный content SHA-256, assignment identity SHA-256,
assignment ID, base scope revision и execution revision. Retry не перебиндит
письмо к новому контексту. Изменение binding или expiry запрещает новую mutation;
пользователь должен открыть актуальное Pending decision в обычном `/execution`.
Срок ссылки по умолчанию 1 час, разрешённый диапазон 60 секунд–24 часа.

Binding повторно проверяется **внутри** того же authoritative aggregate lock, что
обычный UI: legacy file lock либо managed transaction/sprint lock. Используются
существующие validation/CAS/apply и один ledger. Один конкурент становится winner;
проигравший получает 409 (expiry: 410) и одну append-only conflict attempt.
Exact retry ранее принятого или отклонённого запроса возвращает тот же receipt,
даже после expiry/смены assignment; он никогда не применяет scope снова.
Конфликтующее повторное использование idempotency key запрещено.

Известные process credentials отклоняются до записи request/decision payload;
notification display redacts полные значения **до** сокращения текста. В почте,
URL, outbox и audit нет admin/role tokens, SMTP password, cookie/CSRF secrets или
распечатки environment. Текст исключений SMTP не сохраняется: только статические
error codes. Новые GET сохраняют существующую read-only модель доступа nginx-qa;
opaque ID не является механизмом конфиденциальности данных.

## Configuration / provider contract

По умолчанию email выключен и SQLite не создаётся. Укажите только путь
`NGINX_QA_NOTIFICATION_CONFIG` к внешнему JSON; не используйте `.env`, Git,
project prompts, runtime conversations или evidence для credentials.

Пример **без секретов**, адреса/пути исключительно placeholders:

```json
{
  "schema_version": 1,
  "notifications": {
    "email": {
      "enabled": true,
      "pending_decisions": true,
      "destination": "operator@example.test",
      "sender": "nginx-qa@example.test",
      "public_base_url": "https://qa.example.test",
      "database_path": "C:\\ProgramData\\nginx-qa\\notifications\\decisions.sqlite3",
      "smtp": {"host": "smtp.example.test", "port": 587, "security": "starttls"},
      "link_ttl_seconds": 3600,
      "poll_interval_seconds": 30,
      "lease_seconds": 120,
      "max_attempts": 5
    }
  }
}
```

Schema: `schemas/decision-notification-config-v1.schema.json`. Runtime проверяет
absolute external DB path, отсутствие inline credentials/неизвестных полей,
адреса без header injection, HTTPS origin без path/query/userinfo и обязательный
SMTP TLS (`starttls` либо `ssl`). Loopback HTTP допускается только явным
`allow_loopback_http: true` для isolated qualification; не для удалённого телефона.

`NGINX_QA_SMTP_USERNAME` и `NGINX_QA_SMTP_PASSWORD` передаются **только окружением
дочернего серверного процесса**. На Windows предназначен тот же одобренный local
credential pattern: отдельное DPAPI CurrentUser хранилище вне репозитория с ACL
только для service user/SYSTEM, загрузка локальным launcher непосредственно в
child environment. SMTP credential provisioning и правки существующего live
launcher выполняются отдельно при согласованном deployment; эта разработка
не создаёт credentials и не изменяет нынешнее шеститокенное хранилище.
Не передавать значения аргументами команд, через URL/JSON, чат, user/system-wide
Windows variables или log. Для relay без auth можно не задавать обе переменные;
задавать только одну запрещено. Сертификат SMTP проверяется системным trust store.

`NotificationProvider.send(notification)` — generic interface; реализован только
`SmtpEmailProvider`. Telegram/Slack/webhook и новый approval workflow не добавлены.

Неправильная email config отключает только email (`EMAIL_CONFIG_INVALID`);
execution API остаётся доступен. Ошибка открытия outbox/доставки также не превращает
спринт в blocked/NO_GO/reject. У Pending decision отображается delivery failure
или unavailable. Отсутствие configured provider не означает успешную доставку.

## API additions

| Method / path | Контракт |
| --- | --- |
| `GET /api/v1/decision-notifications/{id}` | Read-only notification, immutable исходный request при совпадении content hash, `current`, `stale_reason`, revisions. `request=null`, если исходные bytes больше не совпадают. |
| `POST .../{id}/validate` | Обычные expected execution/scope revisions; optional proposal должен точно совпасть с исходным. Штатная validation/exact diff. |
| `POST .../{id}/decisions` | Обычная `scope-human-decision-v1` schema; только `approve`/`reject`, idempotency key, exact revisions, validation ID для approve. Один v2 ledger. |
| `POST .../{id}/retry` | `{ "idempotency_key": "unique-operator-retry" }`; operator auth/CSRF, то же notification/request, только failure до expiry/max attempts. |

Existing scope-requests/observability дополнены `notification`; live observability
имеет отдельный `notification_events`. Delivery-only изменения включены в cursor.
Historical checkpoints не переписываются и не дополняются более поздней delivery
информацией. Нет новой схемы execution state и migration scope_control.

## Persistence / failures

Outbox — отдельный SQLite v1, `synchronous=FULL`, отдельные notifications,
append-only events и hashed retry-idempotency records. Events защищены от UPDATE
и DELETE. GET открывает существующую БД read-only; не создаёт её и не вызывает
reconciliation. Startup/worker явно инициализирует только outbox.

Authoritative request сначала сохраняется существующим workflow. Worker читает
сохранённые genuine Pending decisions и восстанавливает пропущенные notification
records после restart. Он не вызывает managed startup-repair/publication/dequeue.
Первое binding сохраняется даже после изменения runtime. Перед SMTP проверяется
актуальность; SMTP выполняется без execution lock. Возможное письмо, которое
устарело прямо во время отправки, не даёт возможности stale approval.

Claim имеет durable lease/fencing. Если процесс упал после SMTP acceptance, но до
local success record, письмо может прийти повторно: SMTP обеспечивает здесь
**at-least-once**, не exactly-once. Повторное письмо не создаёт второй request или
scope revision. Обычный delivery failure повторяется только явно оператором;
автоматически восстанавливается лишь прерванный lease. Retry ограничен числом
попыток, не продлевает expiry; expired/stale/delivered записи не пересылаются.

## Qualification

```powershell
python scripts/run_email_decision_checks.py --output evidence/email-decisions-affected-regression.json
```

Все тесты используют C: temporary state, synthetic credentials, ASGI без TCP
listener и fake SMTP transport. Non-Delta: Borealis/project 9109, managed/parallel
fixtures 9217. Тесты проверяют immutable history/reviews, graph/assignment/queue
invariants и то, что ACK не становится transition. New-process тест импортирует
реальный API adapter с перенаправленными fixture paths и повторяет GET/exact receipt.

Coverage по acceptance: delivered→Approve/Reject; scanner/GET/HEAD; auth/CSRF;
expiry/replay; revision/assignment/content stale; local/email и browser/browser
races; exact/conflicting retry; failure/retry; pending/sent/decided restart;
new-process history; no graph/dequeue side effects; ACK invariants; decoded MIME,
URL/outbox/audit credential checks; legacy/managed/parallel non-Delta compatibility.

Точные tests, source fingerprint, pass/skip и **полный перечень непрогнанных suites**
находятся в `evidence/email-decisions-affected-regression.json`. Не выполняются:
полный repository regression, stress/load, настоящий SMTP/inbox, real mobile/
reverse-proxy deployment и любые live integration mutations. UI проверен synthetic
DOM/Node и responsive CSS; отдельный визуальный mobile/browser прогон не выполнен.

## План безопасного deployment / rollback — НЕ выполнен

1. Получить отдельное разрешение на exact final SHA и окно установки. Перепроверить
   фактические live PID/HEAD/dirty files/config/dependency pins/state; не использовать
   старый Delta revision как текущую истину. `run.bat`/`QUICKSTART.md` сохранить.
2. Квалифицировать именно SHA в новом изолированном checkout/state/port; не запускать
   dev `run.bat` на live paths. Dependencies не изменены: новые third-party пакеты
   не нужны. Секреты provision локально до запуска, без вывода значений.
3. Согласовать reachable HTTPS origin и защищённый front door/VPN. Не публиковать
   нынешние unauthenticated read APIs в Интернет: email ID не authentication.
   Forwarded scheme/host должны приходить только от доверенного reverse proxy;
   operator cookie должен быть Secure при HTTPS. Новые domain/certificate/firewall
   изменения требуют отдельного согласования, не являются частью этой разработки.
4. При установке приостановить всех writers, способных изменять общий runtime и
   prompt/latest artifacts. Учитывать, что `D:\Prompt` теперь junction на `C:\Prompt`:
   coherent backup должен сохранить target contents и metadata ссылки, не только
   сам reparse point. Не возвращать старую неполную копию D: поверх актуальной C:.
5. После остановки writers создать проверенный coherent backup canonical state,
   queues/history/reviews, prompt target, managed DB, launcher/config/dependency
   pins и существующего outbox с journal при его наличии. Секреты не включать.
   Сохранить exact IDs/revisions/counts/hashes. Останавливать сервис только после
   разрешения. 8025 не изменять ради email deployment.
6. Установить exact SHA. Runtime schema/migration не требуется: существующий v2
   scope state не переписывается. Только отдельный notification SQLite v1 создаётся
   startup. Сначала можно запустить email disabled, проверить read-only invariants;
   включение уведомлений отправит письма для уже существующих актуальных human
   Pending decisions, поэтому получатель/config и разрешение должны быть проверены.
7. Проверить readiness/read-only scope, сохранённые assignment/history/review/queues,
   отсутствие graph advance и delivery status. Не выполнять тестовое Approve/ACK/
   handoff на живом Delta для smoke test. Снять freeze после успешной проверки.
8. Rollback email: остановить mail worker/сервер согласованным порядком и вернуть
   exact предыдущую совместимую **v2** версию с сохранённым актуальным semantic state.
   Новый outbox сохранить: старая версия его не читает. Не восстанавливать старый
   snapshot поверх решений, принятых после backup, и не запускать pre-scope-control
   бинарник поверх scope_control. Старые email routes станут недоступны; обычный
   v2 `/execution` остаётся authoritative. При повторном включении вернуть тот же
   outbox, чтобы сохранить identities/leases/retry lineage; не удалять БД для resend.

## Отдельный follow-up — НЕ реализован

Human **Propose changes** → natural-language correction → coordinator control-plane
request → revised structured proposal → deterministic nginx-qa validation → exact
diff → **separate human approval**. Не включать частичную реализацию этого workflow
в email-релиз.

## Live boundary

Для email-разработки: **live deployment: NO; 18025 restart: NO; 8025 changed: NO**.
Отдельная разрешённая операция освобождения диска/переноса Prompt не является email
deployment; её подтверждения находятся во внешнем maintenance report, не в Git.
