# Scope decisions and execution UI: implementation plan

Requirements: Delta commit `e72e6f88abf81037a2afec408f8d023980607d66`,
`orchestration/sprints/isc-s16-continuous/handoffs/NGINX-QA-R23-SCOPE-CAPABILITY-HANDOFF.md`.
Code base: `d28726bbcbb552dd21f8cda1042f39f2ee2a21ec`.
Isolated checkout: `D:\nginx-qa-scope-v2`; branch:
`codex/scope-decisions-graph-ui`.

1. Extend scope control with append-only versioned revisions, original issued
   instructions, assignment-bound effective contexts and immutable ACK lineage.
   Keep ACK separate from normal graph execution and review gates. Add runtime
   adapters for legacy and managed execution without weakening their transactions.
2. Add structured scope requests and authenticated human decisions. Use exact
   server-validated proposal diffs, execution/scope CAS, content-bound approval,
   idempotent decisions, conflict audit and an operator session bootstrap without
   exposing bearer secrets to operators, prompts or evidence.
3. Add a universal read-only execution projection and Graph / Current assignment /
   Pending decisions / Timeline UI, exact saved checkpoints for historical views,
   visits, source-bound review gates, scope/ACK overlay and reconnect-safe polling.
   Wire checkpoint writes only into mutation transactions, never into GET routes.
4. Validate version 2 and later revisions, restart lineage, rollback-on-failure,
   competing operator decisions, review/rework continuation, role boundaries,
   retained legacy history and ACK side-effect exclusions. Check UI on Delta's
   compatibility fixture and another runtime fixture. Record results and any
   unavailable checks; provide a separate safe migration/deployment runbook.

All development and tests run in this isolated checkout with temporary stores and
test-only ports. Delta revision 74 is a compatibility fixture, not current CAS.
Any live inspection is read-only. No installation, restart, live migration,
amendment, ACK, handoff, dequeue or graph transition is authorized in this task.
The running service on 18025 and the protected 8025 instance are not modified.
