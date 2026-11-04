# Apply recovery supplement (SDWU-001)

The original SDW-006 / A21 / E03 defect is reproducible on installed product
`a1841e420a517dfdb9ed2c5a4b44eaeb6b3286ca`: after a real 120-second plan expiry,
Apply returns `409 PLAN_STALE`, but the browser describes it as a lost response
and leaves its modal, project selector, diagnostics refresh and session check
locked. The before-fix JSON, screenshot and script are in
[`evidence/ui-recovery`](evidence/ui-recovery/). The script used a real issued
`setup.open` plan and waited 121,442 ms; it did not launch a model or sprint.

## Recovery behavior

Before sending Apply, the tab stores only its bounded request identity in
`sessionStorage`: schema version, actor, project, plan ID/digest and idempotency
key. It stores no cookie, CSRF token, plan arguments, effects or credentials.
Unavailable storage prevents sending. Invalid stored identity blocks new
operations rather than silently deleting an unknown request.

An explicit stale-plan rejection with a validated, bound durable `BLOCKED`
receipt closes the confirmation and permits diagnostics refresh and a new plan.
An HTTP status by itself is insufficient: a first-response 403 can arrive
after the effect, and an earlier lost request can still be queued before its
intent exists. Network loss, malformed responses, unbound legacy errors,
busy/context errors and authorization failures therefore retain the original
identity. Cancel/Escape closes its recovery dialog without deleting that key;
Escape during Apply cannot discard it. Reload never resends automatically.

The recovery button reads the original server plan and the exact-key receipt.
An absent receipt retains the key and offers an explicit same-key confirmation
only when current action permission permits it. It is not cancellation evidence.
Read-only project navigation, diagnostics and reauthentication remain available;
delayed inspection responses from an old project or session cannot replace the
current confirmation. A different actor cannot inspect or retry the attempt.

A terminal receipt resolves the HTTP uncertainty. A normal native
`selftest.start` or `selftest.stop` receipt in `APPLYING` with a native operation
ID also acknowledges the durable job: its existing status and owned Stop flow
remain available while agents work, including when Start permission is revoked.
The engine retains its one-active-project admission rule. `UNKNOWN`, `PLANNED`
and `APPLYING` without that native acknowledgement retain the key. Current status
can resolve them only for the exact operation bound by Apply/inspection.
No code path automatically creates or applies a replacement plan.

## API and durable fence

`POST /api/v1/projects/{project_id}/setup/plans/{plan_id}/inspect` accepts the new
`ApplyInspectionRequest` and returns `ApplyInspectionResponse` from
`contracts.schema.json`. Request fields are `schema_version: 1`, `plan_digest`
and the original `idempotency_key`. The response contains the authenticated
server `ActionPlan`, the recorded `OperationReceipt` or null, and
`retry_permitted`. It is private, same-origin, CSRF-protected and no-store.

Inspection checks actor/project/path ownership, current read permission and the
exact key/digest. Read permission is checked again before disclosure. It reads
the atomically replaced ledger without writing a lock file, intent, idempotency
entry or operation; it does not call a handler/status reader or reconcile native
state. `retry_permitted` conveys current action authorization, never an Apply
authorization token. Apply rechecks authority itself.

When Apply discovers expiry or changed authority, it persists an ordinary
`BLOCKED` / `PLAN_STALE` receipt for the exact key under the existing ledger lock
before raising `SetupApplyNotStarted` (a `ValueError` subtype). No handler/native
effect occurs. HTTP remains 409 with `error: PLAN_STALE`; the additive
`apply_outcome: NOT_STARTED`, plan ID/digest and receipt bind the negative result.
A queued same-key request then replays the same operation even if authority is
restored. Current read authorization is rechecked before disclosing this
negative receipt too; revocation returns 403 while retaining its durable fence.
Existing operation receipt and ledger schemas remain unchanged.

## Verification and limits

The focused Python suite passed 32 tests, including the production JS harnesses,
real isolated ledger/API authorization races, exact remote-SHA checkout guards
and deployment branch allow/reject checks. The final UI refinement passed its
two affected tests again: 9 existing categories and 11 recovery categories.
The raw logs include existing AnyIO unclosed-stream `ResourceWarning`s; there
were no test failures. `node --check` and `git diff --check` passed.

The backend tests verify zero effects for a stale negative receipt, replay of a
delayed original after authority restoration, read revocation after effect and
after negative persistence, passive inspection byte/mtime stability, and
owner/project/CSRF/origin fences. Browser fault cases in the JS harness use an
explicit synthetic DOM and HTTP fixtures; they are not claimed as live browser
qualification. The actual before-fix browser expiry reproduction is separate.

No installed host was upgraded or stopped in this task. Independent exact-result
reviews and SDWU-002 live post-fix mobile, keyboard, reauthentication, expiry and
reload qualification remain subsequent stages. Existing model qualification was
not rerun. Real Telegram credentials, actor/phone and deployment inputs remain
external requirements recorded in the earlier runbook.

The bookmark is tab-scoped, not a cross-browser operation registry. Do not erase
an unresolved bookmark to force a new action. If another plan creation has
pruned an expired unapplied server plan, lookup can return `PLAN_NOT_FOUND`; the
browser conservatively retains the identity for operator investigation. If a
receipt remains `UNKNOWN`, the existing native recovery process is required.

## Deployment scope

`run_staging.ps1` additionally accepts exactly
`codex/sdw-qualified-ui-recovery-20261104` for the setup wizard profile. Default
branches, other profiles, exact remote SHA/origin checks and ownership gates are
unchanged. This is permission for the later qualified deployment identity, not
evidence that deployment has happened.

## Native review rework 1: session response ordering

Runtime reviewer 7081 rejected `7a39bd90eae245f68da902d3d6974a8b76100b14`
with a reproducible P2 client-context defect. If session check A was delayed,
check B could accept a newer operator, then A could overwrite that actor/CSRF
and re-enable inspection of the old actor's bookmark. Backend authorization
still guarded the request; this finding did not demonstrate an authorization
bypass. The actual `REJECT` / `REWORK_ENQUEUED` receipt and original reproduction
are preserved in [`evidence/ui-recovery-rework-1`](evidence/ui-recovery-rework-1/).

Every session check now advances a generation. Session response, project list,
snapshot/history and error/finally handlers can commit only to their current
generation/context. A new check detaches old busy work so it cannot block fresh
diagnostics. Old advice, plan, inspection and status responses are also fenced;
in particular an old inspection 401 cannot restart authentication after B.
The unresolved Apply identity is preserved throughout. Owned Stop semantics,
backend contracts and installed services are unchanged.

The reviewer's exact probe was rerun against its frozen rejected export, with
only its output destination redirected; it reproduced the finding again.
The new production-JS fixture covers five categories (16 reordered cases):
authenticated/unauthenticated/failed session responses, delayed project lists,
snapshot/history success and failure, old action failures, and same-actor stale
advice/plan success while newer work is still pending. All seven affected
`tests.test_wizard_ui` tests passed, retaining the existing 9 request/receipt and
11 Apply-recovery categories. These are synthetic ordering tests, not installed
browser qualification. New native DONE, independent reviews and post-fix live
qualification are still subsequent steps at publication of this rework.
