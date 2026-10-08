# Asynchronous human-decision email: implementation plan

Base: `8581390511838efb15a69cb2fadac89b12483b3b`.
Branch: `codex/async-human-decision-email`.
Development checkout: isolated C: checkout; never either serving release tree.

## Boundaries

The existing scope-workflow request, validation, decision ledger, aggregate lock,
CAS and apply functions remain authoritative. Only genuine `pending` human
decisions trigger mail; prior authorization awaiting application does not.
No coordinator-mediated correction or new approval workflow is implemented.
No deployment, live migration, ACK, dequeue or graph transition is authorized.

## Steps

1. Add a durable SQLite notification outbox, append-only delivery audit, provider
   interface and SMTP implementation. Configuration is external and disabled by
   default; credentials are process-local environment values, never persisted.
   Recover claims/reconcile authoritative pending requests after restart. Delivery
   failure must not fail or advance execution. Retry preserves semantic identity.
2. Bind notification identity to the exact project/sprint/request, request/content
   digest, assignment identity, scope and execution revision. Email links contain
   only a public opaque identifier, not a credential. Existing operator pairing,
   HttpOnly session and CSRF authorize explicit POST actions. GET is read-only.
3. Route email Preview/Approve/Reject through the same locked backend operation;
   check binding inside the aggregate transaction. Preserve exact successful
   idempotent replay and reject stale/conflicting new decisions without partial
   application. Reconcile mail outside semantic transactions and never hold an
   execution lock during SMTP network I/O.
4. Reuse `/execution`: bind an email entry to its immutable request, show expiry,
   stale state, source/restrictions and exact server-generated diff. Show delivery
   state/retry on ordinary Pending decisions. Preserve ordinary Edit unchanged.
5. Test fake SMTP delivery/failure/retry, GET/scanner safety, authentication,
   stale/expired/replayed links, local/email decision races, idempotency,
   new-process durability, non-Delta compatibility, history/queue/graph/ACK
   invariants and credential non-disclosure. Run targeted affected regressions;
   explicitly list excluded suites and external tests.
6. Publish exact commit with configuration/security/deployment documentation,
   evidence and a separate unimplemented follow-up for Propose changes.

## Honest limitations

An SMTP success means the provider accepted the message, not proof of inbox
delivery/read. A crash after SMTP acceptance but before local recording can
produce duplicate notification mail (at-least-once delivery), never a duplicate
semantic decision. Mobile browsers require reachable HTTPS and normal operator
pairing; the email identifier grants no authority. No additional channel or
public-network deployment is included.
