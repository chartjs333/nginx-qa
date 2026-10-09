# Runtime scope and execution contracts

Requirements are pinned to Delta `e72e6f88abf81037a2afec408f8d023980607d66`.
Implementation starts from nginx-qa `d28726bbcbb552dd21f8cda1042f39f2ee2a21ec`.
This document describes isolated implementation, not deployment or live permission.

| Runtime | Authoritative assignment | Scope persistence and normal continuation |
| --- | --- | --- |
| Legacy sequential conditional graph | Existing assignment ID and node/phase/occurrence | Scope ledger and human decision share the locked Git configuration aggregate. Existing handoff/review code requires role token, exact context and its ACK. Two-review graph policy remains unchanged. |
| Legacy sequential queue graph | Existing issued assignment ID, then recorded ordinary queue delivery | A first delivery retains an already issued ID; future deliveries create their ordinary identity. New bindings inherit approved overrides. An ACK does not dequeue: a scope-aware ordinary phone handoff must be accepted before the next role can be claimed. |
| Legacy structured parallel group tasks | The actually delivered queue item ID, linked to existing task ID, cycle ID and task-node ID | Scope requests target that recorded delivery, not a fabricated global current assignment. Group handoff validates source assignment/context/ACK. New group claims expose their own effective scope and require their role token. |
| Managed workspace sequential/parallel | Existing task, reviewer or coordinator-context ID in SQLite | Scope and workflow changes use the same sprint file lock and `BEGIN IMMEDIATE` aggregate transaction as managed execution. Assignment publication, result/review/recovery submissions retain their existing workspace, graph, outbox and quorum checks. |

Unstructured legacy messages with no recorded task identity are not silently
turned into assignments. Scope mutation fails closed until an ordinary supported
task delivery establishes a real identity. Historical messages, assignments and
results are not backfilled with invented claims or timestamps. Unknown data stays
unknown in the read-only execution UI. Governed parallel tasks use the structured
group API; governed sequential tasks use their scope-aware identity/handoff flow.
Raw phone-channel claims cannot consume governed sequential work, and raw phone
handoffs cannot bypass the structured governed parallel source check.

## Roles and instruction precedence

Each affected task role, coordinator and newly issued reviewer needs its own role
credential, GET of its effective scope, exact-context ACK, and the same context on
ordinary result/review/recovery submission. A task's ACK is not a reviewer ACK.
The administrative credential is solely for the human operator/session bootstrap.
No credential is part of a stored request, prompt, source artifact, URI or command
argument. Existing DPAPI role selection must load the appropriate token for each
request; a previously running agent does not inherit a later shell's environment.

Effective instructions are explicitly authoritative over conflicting historical
issued text. For runtimes that never persisted an authored delivery message,
scope stores immutable profile/task snapshots without inventing a historic
message. The task projection renders the effective authored instruction; the
original raw delivery remains separate. Original conditional-graph delivery
snapshots retain their stronger message/projection integrity checks.

New reviewer instructions use the latest explicitly approved applicable reviewer
override. The reviewed result/commit is unchanged. Its source scope context is
retained separately and hash-linked in the reviewer's own scope context. Prior
review decisions/ACKs remain immutable; they do not count as ACKs for a new scope
or as approvals of another result.

## Durability and recovery boundaries

Human requests, validations, one authoritative decision, losing decision attempts,
new scope revision and effective bindings are committed in their owning aggregate.
The managed backend validates the complete runtime and scope lineage before
commit; validation failure rolls back the whole transaction. Scope/decision-only
attention events do not increment the managed graph-definition revision. A
separate execution cursor is used for actual execution/scope changes. Validation
does not invalidate itself by incrementing that cursor.

Legacy queues are separate persisted files backed by process-local queue memory;
they require the existing single-serving-writer deployment model, not multiple
Uvicorn workers. An OS-level lock protects the scope/decision aggregate across
processes, but does not make unrelated legacy queue writers safe. Freeze every
writer before backup/migration, as required by the deployment runbook.

New governed queue/group handoffs first persist a scope-bound publishing intent
under the configuration file lock. This CAS boundary prevents an amendment of an
affected active assignment while ordinary handoff publication is pending. Queue
publication uses a stable idempotency identity. A crash or failed completion write
is recovered by retrying the same ordinary handoff: no extra queue item is made.
Do not delete the intent or force the graph. If a normal target/gate rejects the
handoff, correct that ordinary operational condition under its own authority and
retry. Successful completion stores the frozen receipt for exact replay.

ACK changes only scope-authorization metadata and its audit cursor. It never
publishes an outbox item, completes an assignment, accepts a review, performs Git
work, advances a transition, or dequeues a task. Normal execution must pass all
of its existing gates afterwards. Read-only snapshots use persisted aggregate
reads, never managed `current_identity()` or generic `whoami` (which may publish
or claim work).

## Focused evidence

`tests/test_scope_runtime_adapters.py` covers managed revision lineage and stale
ACK rejection; managed operator decision racing; independent reviewer ACKs and
ordinary two-review completion; read-only views with publication disabled;
transaction rollback; parallel ordinary delivery/ACK/handoff/future scope; and
queue-graph ACK/no-dequeue plus recovery after injected handoff receipt failure.
Both handoff paths include no-duplicate retry checks. The parent verification
report records executed commands/counts for the final tree, not just method counts.
