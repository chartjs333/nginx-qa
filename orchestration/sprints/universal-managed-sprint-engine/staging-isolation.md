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
service checkout: D:\nginx-qa-staging\universal-managed-sprint-engine
owned state base: C:\nginx-qa-staging-state\umse-007
virtual environment: C:\nginx-qa-staging-state\umse-007\.venv
runtime root: C:\nginx-qa-staging-state\umse-007\runtime_state
prompt root: C:\nginx-qa-staging-state\umse-007\prompt
managed root: C:\nginx-qa-staging-state\umse-007\managed
legacy state root: C:\nginx-qa-staging-state\umse-007\legacy
host: 127.0.0.1
HTTP port: 18025
managed child ports: 18100-18199
```

`runtime_root`, `prompt_root`, `managed_root`, `legacy state root`, and the
virtual environment are disjoint children of one dedicated owned state base. The state base and
`service_root` are disjoint. These authoritative mutable roots must not be
created below the service checkout: that layout violates the frozen runtime
configuration invariant and is rejected before startup. A staging-only ASGI
entry point rebinds every legacy mutable path (history, registries, pending
sprints, attachments, screenshots and evidence) into the owned `legacy` root
before the legacy application is imported. All corresponding top-level paths in
the service checkout must remain absent. The ignored compatibility directory
`service_root/runtime_state` may be absent; if it already exists on an owned
restart, it must contain exactly one direct, non-reparse
`sequential-prompt-settings.json` with the isolated prompt templates. The
launcher does not create or use that compatibility file.

На этой машине `D:\.git` является внешним repository marker, поэтому mutable
roots и `.venv` размещаются под свежим dedicated base на `C:`; service checkout
остаётся по точному пути выше. Launcher не удаляет и не переименовывает внешний
repository marker и перед первой записью применяет тот же parent-repository guard,
что и importer.

State base принадлежит запуску только после атомарного создания
`.nginx-qa-staging-owner.json`. При первом `Setup` base обязан отсутствовать или
быть пустым, а ignored legacy state в service checkout обязан отсутствовать.
Marker получает случайный canonical `initialization_id` GUID. При restart его
формат и root-local placement проверяются, а статическая часть должна в точности
совпасть с roots, origin, branch, явно переданным approved SHA и provenance base
interpreter (path, Python version и SHA-256 executable). Неизвестное старое
состояние, включая прежний `C:\nginx-qa-staging-state`, автоматически не
принимается и не очищается: оператор сначала переносит его в отдельный quarantine
path либо создаёт новый clean checkout/state base.

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

The launcher starts `staging_host_app:app`, not `main:app`. The wrapper validates
the exact service root, owned state base, ownership marker, protected-root
separation and canonical non-reparse `legacy` root before importing `main`, then
recursively rejects every existing symlink, junction, reparse point, or canonical
alias below that root and rebinds all ten legacy mutable globals. Legacy queues
and scheduled state follow the rebound history parent into
`legacy/runtime_state`.

## Staging launcher

Нужен `run_staging.ps1` и/или `run_staging.bat`, который явно задаёт:

```text
NGINX_QA_HTTP_HOST=127.0.0.1
NGINX_QA_HTTP_PORT=18025
NGINX_QA_SERVICE_ROOT=D:/nginx-qa-staging/universal-managed-sprint-engine
NGINX_QA_PROTECTED_ROOTS=["D:/nginx-qa","D:/nginx-qa-umse","D:/Prompt"]
NGINX_QA_STAGING_STATE_BASE=C:/nginx-qa-staging-state/umse-007
NGINX_QA_STAGING_VENV_ROOT=C:/nginx-qa-staging-state/umse-007/.venv
NGINX_QA_STAGING_BASE_PYTHON=C:/Python312/python.exe
NGINX_QA_RUNTIME_ROOT=C:/nginx-qa-staging-state/umse-007/runtime_state
NGINX_QA_PROMPT_ROOT=C:/nginx-qa-staging-state/umse-007/prompt
NGINX_QA_MANAGED_ROOT=C:/nginx-qa-staging-state/umse-007/managed
NGINX_QA_GIT_FETCH_TIMEOUT_SECONDS=120
NGINX_QA_CHILD_PORT_RANGE=18100-18199
NGINX_QA_INSTANCE_ID=universal-managed-sprint-engine-staging
NGINX_QA_DISABLE_TELEGRAM=1
NGINX_QA_DISABLE_TUNNEL=1
```

Launcher должен отказаться от запуска, если порт занят или runtime root указывает
в live directory.

Launcher вызывается только с полным ожидаемым SHA:

```powershell
$sha = (git rev-parse HEAD).Trim()
.\run_staging.ps1 -Action Check -ExpectedCommit $sha
.\run_staging.ps1 -Action Setup -ExpectedCommit $sha
.\run_staging.ps1 -Action Start -ExpectedCommit $sha
```

Он проверяет локальный и remote branch head, standalone `.git`, provenance base
Python/`.venv`, весь диапазон `18025,18100-18199` и exact ownership marker до
любых setup/start mutations. При restart listener из child pool разрешён только
если его фактический socket-owner PID аутентифицирован durable lease, PID
receipt, OS process identity и membership в том же Windows Job. Durable leader
и socket owner могут быть разными PID для Windows venv redirector.

## Four-child restart qualification

Квалификация состоит из четырёх fail-closed фаз. Только `finalize` имеет право
записать `status: passed`:

```text
prepare -> awaiting_external_restart
verify-after-restart -> verified_awaiting_cleanup
cleanup -> cleaned_awaiting_live_after
finalize -> passed
```

Внутри `prepare` каждый mutating POST имеет отдельный durable checkpoint:
`project_intent_recorded` и `start_intent_recorded` записывают exact method/path/body
до запроса. Resume повторяет только неоднозначный запрос с тем же intent;
`project_provisioned` не создаёт project повторно, а `start_accepted` вообще не
делает POST. Сохранённые intent/project/start proofs проверяются до следующего
запроса и при несовпадении не перезаписываются.

Сначала в отдельной host-консоли запустить staging через launcher. Все три команды
принимают только полный SHA, который одновременно является локальным HEAD и
remote head назначенной ветки:

```powershell
$sha = (git rev-parse HEAD).Trim()
.\run_staging.ps1 -Action Check -ExpectedCommit $sha
.\run_staging.ps1 -Action Setup -ExpectedCommit $sha
.\run_staging.ps1 -Action Start -ExpectedCommit $sha
```

`Setup` обязателен перед первым запуском clean owned state base. На уже
инициализированном exact-owned base его повторяют только явным операторским
действием для переустановки зависимостей; для обычного restart используется
`Start`. Последняя команда остаётся foreground-процессом. Не закрывать эту
консоль до checkpoint `awaiting_external_restart`.

В рабочей консоли создать уникальные пути evidence и внешнего baseline. Snapshot
tool выполняет единственный разрешённый read-only health check: proxyless GET без
redirect на exact `http://127.0.0.1:8025/`, ожидает HTTP 200 и `text/html`, но не
читает response body. Дополнительно он проверяет единственного OS-владельца
`0.0.0.0:8025`, process start time и Git ref identity (`HEAD` и branch). Tool не
читает и не перечисляет live state, queue либо `runtime_state`, а также не
запускает Git status по live worktree. Output допускается только как новый
безопасный `.json` leaf непосредственно внутри exact owned evidence root. Каждый
существующий ancestor state base, ownership marker, evidence root и output
проверяется на canonical path и отсутствие reparse point до любого write;
UNC/device paths, ADS, reserved device names и trailing dot/space запрещены.

```powershell
$repoRoot = (Resolve-Path .).Path
$sha = (git rev-parse HEAD).Trim()
$runId = "umse-007-" + [DateTime]::UtcNow.ToString("yyyyMMdd-HHmmss")
$stateBase = "C:\nginx-qa-staging-state\umse-007"
$childPython = "$stateBase\.venv\Scripts\python.exe"
$runner = Join-Path $repoRoot "orchestration\sprints\universal-managed-sprint-engine\staging_qualification_e2e.py"
$snapshotTool = Join-Path $repoRoot "orchestration\sprints\universal-managed-sprint-engine\capture_live_snapshot.ps1"
$evidence = "$stateBase\evidence\$runId.json"
$liveBefore = "$stateBase\evidence\$runId-live-before.json"
$liveAfter = "$stateBase\evidence\$runId-live-after.json"

& $snapshotTool -OutputPath $liveBefore

& $childPython -B $runner prepare `
  --repo-root $repoRoot `
  --expected-sha $sha `
  --ref refs/heads/agent/umse-07-staging-qualification `
  --manifest-path orchestration/sprints/universal-managed-sprint-engine/staging-e2e-manifest.json `
  --git-address https://github.com/chartjs333/nginx-qa.git `
  --expected-child-python $childPython `
  --expected-runtime-root "$stateBase\runtime_state" `
  --expected-managed-root "$stateBase\managed" `
  --ownership-marker "$stateBase\.nginx-qa-staging-owner.json" `
  --run-id $runId `
  --managed-db "$stateBase\runtime_state\leases\managed-import.sqlite3" `
  --evidence $evidence `
  --live-baseline-json $liveBefore `
  --protected-root D:\nginx-qa `
  --protected-root D:\nginx-qa-umse `
  --protected-root D:\Prompt
```

После `awaiting_external_restart` нажать `Ctrl+C` только в host-консоли. Child
Windows Jobs не останавливать. В той же host-консоли снова выполнить:

```powershell
.\run_staging.ps1 -Action Start -ExpectedCommit $sha
```

Когда новый listener `127.0.0.1:18025` готов, в рабочей консоли выполнить:

```powershell
& $childPython -B $runner verify-after-restart `
  --evidence $evidence
```

Runner доказывает смену host birth identity, adoption ровно четырёх исходных child
identities без замены/duplicate leases и идемпотентный replay исходного start
request. После `verified_awaiting_cleanup` снова нажать `Ctrl+C` только в
host-консоли и дождаться освобождения `18025`. Затем выполнить:

```powershell
& $childPython -B $runner cleanup --evidence $evidence
```

Перед созданием writable store/supervisor `cleanup` повторно читает SQLite только
в read-only режиме и exact-сверяет sprint/fencing token, процессы, leases, child
health, listener owners и Windows Job identities с `post_restart`. Любой drift
завершает фазу до первого `stop()`. Затем production supervisor выполняет
authenticated stop всех четырёх Windows Jobs; runner проверяет `STOPPED`, released
leases и отсутствие listener на `18025,18100-18199`. Только после статуса
`cleaned_awaiting_live_after` снять новый внешний live snapshot и завершить run:

```powershell
& $snapshotTool -OutputPath $liveAfter
& $childPython -B $runner finalize `
  --evidence $evidence `
  --live-after-json $liveAfter
```

Baseline, after-snapshot и evidence — три разных файла внутри exact owned evidence
root. `finalize` требует свежий after-snapshot, снятый позже cleanup, exact schema и
точное совпадение live PID/start time, port, read-only health result и Git
identity, а также повторно проверяет отсутствие всех staging listeners. Любое
отклонение завершает run без `passed`.

## Live health guard

`capture_live_snapshot.ps1` выполняется непосредственно перед `prepare` и только
после `cleanup`. Он делает только разрешённый read-only GET корневого live
endpoint и ничего не записывает в live checkout. Изменение live PID, владельца
порта, health result либо Git identity — квалификационный FAIL. Live filesystem
state не является входом квалификации и остаётся полностью недоступным helper'у.

## Promotion

Promotion node формирует:

- commit/PR;
- migration notes;
- config defaults;
- backup/rollback plan;
- operator commands.

Он не выполняет live deployment.
