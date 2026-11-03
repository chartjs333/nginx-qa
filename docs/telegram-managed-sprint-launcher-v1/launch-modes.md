# Launch, recovery and cancellation

The launcher uses the existing importer, continuity graph, current-identity,
result/review APIs and process supervisor. It adds no model client or scheduler.
Telegram callbacks and the authenticated gateway call the same `launch`/`stop`
service functions; their transport surfaces are implemented in the next stages.

## Launch evidence

Manual uploads prepare an instance and remain Waiting. An explicit authorized
Start persists `launch_requested`. Autonomous uploads use the same path after
preparation. The current private-chat user/project/action policy is checked at
each boundary; preparation or producer access alone never grants Start.

Start first requires a READY acknowledgement from an operator-configured
existing managed agent-client service. Only then does the existing importer
activate the exact prepared attempt. Running additionally requires a scoped
ACCEPTED acknowledgement for the engine's actual initial assignment IDs. A lost
reply leaves EXECUTOR_UNAVAILABLE and the same activation/session is retried.
The default has no executor: it safely retains Waiting/Prepared without claiming
that a model or agent is working.

An existing service can expose these authenticated loopback HTTP endpoints:

| Endpoint | Request | Required response |
| --- | --- | --- |
| `/managed-client/prepare` | descriptor | descriptor + `status: READY`, `client_session_id` |
| `/managed-client/start` | descriptor + `initial_assignment_ids`, `client_session_id` | same fields + `status: ACCEPTED` |
| `/managed-client/stop` | descriptor + `client_session_id` | same fields + `status: STOPPED` |

The descriptor contains `protocol: nginx-qa-managed-client-v1`, operation ID,
canonical project ID, sprint ID and the exact sorted task/reviewer phone set.
Responses have an exact field set and preserve the original session. Configure
`NGINX_QA_LAUNCHER_EXECUTOR_URL` (explicit HTTP loopback IP and port, no path or
redirects) and `NGINX_QA_LAUNCHER_EXECUTOR_SECRET_REF`; the existing process secret
resolver supplies the bearer credential. No credentials enter agent payloads.
Calls have a ten-second timeout and a 32 KiB strict-JSON response limit.

The existing client service MUST durably deduplicate by operation and session,
resume accepted work after its own restart, and use the normal engine APIs with
independent reviewer identities. READY must not execute assignments. ACCEPTED
means the client has durably accepted the actual assignment set, not merely that
an HTTP server is alive. STOPPED means all its workers and in-flight writes for
that session have quiesced and it will never resume that session. Timeout is not
proof of either acceptance or cancellation. This adapter protocol is an optional
integration boundary, not a bundled autonomous executor.

Transport recovery runs on the owning ASGI loop with at most one batch in flight
and four project operations per batch. Ordinary continuity ticks keep running
while a client or Git call waits. Filesystem/SQLite work stays in worker threads.
Shutdown stops queued recovery work and waits for in-flight mutations before
closing resource holders; it never treats cancellation of an async wrapper as
proof that its worker thread stopped.

The existing reconciliation loop resumes publication/preparation and authorized
launch/stop intents from the same control database. Manual Waiting without a
Start intent never activates after restart. Permissions and repository policy
are reloaded. A pending Stop uses its current Stop grant independently of a
revoked preparation grant or a changed source policy. An import failure uses the
existing Coordinator RETRY_IMPORT lineage: the child inherits the original
launcher owner and returns to preparation, retaining the original error receipt.

## Cancellation evidence

Created and never-activated preparations can be cancelled locally. Failed import
errors and recovery history are preserved. The operation's reservation is retired
only in the same transaction as the cancellation proof. A live preparation lease
must settle before cancellation; a competing request cannot release it early.

Active Stop first records a server-owned stop fence under the existing per-sprint
lock. New graph effects, identity discovery, pending queue delivery, process spawn
and restart are fenced. The same external client session must acknowledge
STOPPED. Missing acknowledgement leaves STOP_PENDING and retains the reservation.
Native child processes then stop through the existing supervisor and ownership/
death checks. Their port and branch leases are released only after that proof.

The `stopped` runtime aggregate is explicit cancellation, not DONE/FAILED task
results. Existing assignments, reviews, result receipts, workflow and outbox are
retained unchanged as history, including pending entries that were not delivered.
A small cancellation receipt permits reconstruction of the exact pre-cancellation
snapshot. Its hash, original schema/invariants, the resulting resource retirement
and the corresponding control receipt are validated on every read. Historical
assignment statuses do not grant live work in a stopped runtime. No reviewer
decision or outbox delivery is invented. Exact already-accepted result/review
replays remain historical acknowledgements.

Terminal retirement checks both runtime evidence and the shared resource indexes.
A subsequent sprint receives a new instance namespace; its write branches must
still satisfy the existing create/adopt Git policy. Re-uploading a manifest whose
`create` branch already exists is not permission to overwrite that branch.

## Validation boundaries

Tests use disposable local Git repositories, actual engine identity/result/review
APIs, two distinct deterministic reviewer identities, a fake existing client and
real isolated child-process fixtures. They cover concurrent/repeated Start,
permission revocation, lost ACK, restart, preparation recovery and cancellation
before launch/during work/pending review. These tests do not demonstrate a real
Telegram conversation, external AI, public tunnel or phone/tablet session.
