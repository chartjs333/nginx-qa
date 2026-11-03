# Telegram controls and lifecycle delivery

The launcher webhook dispatch recognizes `tml:` callback handles only after its
existing webhook-secret and strict bounded-JSON checks. It reads the real sender
from `callback_query.from`, requires the owner's private chat and fresh project
permissions, then validates the persisted operation, instance, attempt generation,
action and 24-hour expiry. Handles contain no credentials and grant no authority.
Use `/sprint_status <project phone>` to refresh expired controls for the owner's
latest instance. Callbacks are limited to 20 per user/project/minute.

Prepared instances have exactly three primary buttons:

1. **Запустить спринт** calls the existing authorized idempotent activation path.
   Running/terminal instances return their state. The callback receipt is answered
   before executor work; the answer does not claim successful activation.
2. **Открыть Web UI** uses a private-chat Mini App and the operator's HTTPS origin
   `NGINX_QA_LAUNCHER_PUBLIC_ORIGIN`, or the owned tunnel origin receipt. Its URL
   contains only a public opaque handle. The implemented gateway authenticates
   signed Telegram initData and scopes the resulting session. Until it is configured,
   the button explains the required setup; no private backend URL is exposed.
3. **Статус спринта** reads the scoped authoritative engine projection without
   provision, activation, graph advancement or executor calls.

Bot webhook registration includes `callback_query`. Group, inline, inaccessible
and business-message callback contexts are rejected. Existing non-launcher
Telegram imports retain their dispatch. Public status omits prompts, raw result
summaries, feedback, tokens, queue payloads and private paths. Stopped aggregate
history is not displayed as live work. Review decisions and commits come from
actual engine receipts; DONE is never interpreted as test PASS.

## Optional structured result report

The core engine's result summary is unstructured. To expose additional evidence,
an agent may submit a summary that is exactly this strict JSON object:

```json
{"launcher_report_v1":{"checks":[{"name":"integration","status":"PASS"}],"artifacts":["docs/evidence/checks.json"],"pr_url":null}}
```

Checks use PASS/FAIL/NOT_RUN/BLOCKED; at most 32 checks and 32 relative Git paths.
An optional PR URL must belong to the exact registered repository. Artifacts and
claims are bound to the engine's accepted result commit and explicitly labelled
**agent-reported**, not independently verified. Unknown/unstructured reports show
NOT_REPORTED. Invalid paths, extra fields or recognized credentials suppress the
report; no raw text fallback is sent.

## Durable transport boundary

The repository had no durable Telegram outbox. The existing email outbox is tied
to genuine human-decision/scope identities and cannot safely be repurposed by
inventing those bindings. Launcher transport intents therefore live in typed
tables in the **same ManagedImportStore database**, using its transactions and
the existing `telegram_api_json` provider. There is no execution scheduler here.

SQLite triggers capture a small frozen owner/lifecycle/instance snapshot in the
same transaction as operation creation or lifecycle change. Rollback removes both.
Created, Waiting, Running, Completed, Failed and Stopped intents are preserved;
replaying the same state does not enqueue another event. On upgrade, the latest
state is backfilled for existing operations lacking any transport history. An
indexed, transactional migration marker prevents rescanning history on subsequent
store initialization; failed backfill and its marker roll back together.
Messages label the historical event separately from the current safe status.

Delivery does not depend on a held project slot: completed/stopped notices remain
eligible after retirement. A read-authorized projection refresh also observes
terminal engine receipts when prepare/start permission was revoked. Each project
batch sends at most ten notices, with four project slots. Recovery/notifications
have independent single-flight host futures, and neither blocks engine ticks.
Every project child drains through unexpected errors. Host shutdown joins actual
in-flight provider threads before closing resource holders.

Each intent has a persisted 120-second lease, at most five attempts, exponential
retry delays capped at 300 seconds, a scoped Telegram message ACK, and only static
redacted error codes. Expired delivery leases recover on restart. A lost ACK can
produce a duplicate physical Telegram message: delivery is **at least once**.
It can never replay activation. Revoked read access suppresses disclosure.
The strict email decision outbox and managed graph queues remain unchanged.

References: [Telegram buttons](https://core.telegram.org/bots/api#inlinekeyboardbutton),
[callback acknowledgement](https://core.telegram.org/bots/api#answercallbackquery).
Real Telegram/mobile delivery is a separate external acceptance requirement;
isolated mocked Bot API tests do not prove it.
