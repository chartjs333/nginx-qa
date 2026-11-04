# Local setup advisor

The private wizard can use an already installed local Ollama model. This adds
text generation to the existing `SetupAdvisor`; it adds no agent, scheduler,
tool dispatcher or setup-operation authority. The Responses transport and the
explicit deterministic fallback remain available.

## Configuration

Set these values in the protected environment of the owned wizard host, then
restart that host using its verified ownership procedure:

```text
NGINX_QA_SETUP_ADVISOR_PROVIDER=ollama
NGINX_QA_SETUP_ADVISOR_MODEL=qwen2.5:7b
```

Ollama must already listen at `http://127.0.0.1:11434` with this local model
installed. The adapter has no configurable URL, model-pull call, cloud login or
API-key requirement. Model identifiers need an explicit tag, cannot contain
paths/URLs, and cannot name cloud models. Before sending diagnostic facts it
checks `/api/show` for local GGUF completion metadata and absence of remote
model/host fields. This is configuration validation of a trusted local service,
not cryptographic attestation of its process, model bytes or operating system.
Changing the local service or model is an operator action and needs renewed
qualification. No model template or question can change the fixed endpoint.

The API supplies the protected directory `<state_base>/setup/advisor`; no
browser or model can select it. Without durable state, local advice stays in
explicit fallback. The passive `provider.configuration` check reports local
configuration separately from credentials and live generation. Its PASS does
not assert a successful model request. Passive diagnostics make no network call.

## Request and output boundaries

Only projected check identifiers, status, scope, freshness, bounded error codes,
approved proposal templates and the sanitized question reach `/api/generate`.
The snapshot's raw evidence, paths, project identity, tokens and authority
fingerprint are omitted. No cookies or authorization header are sent. The local
service is trusted; question sanitization is a best-effort secret/location filter,
not a promise that arbitrary user prose cannot contain private information.

The request contains text and a small JSON schema, without tools/functions or a
dispatcher. A model may support tool-related token formats; this API adapter
never interprets them as operations. Extra tool/message output is rejected.
The schema fixes the snapshot ID, limits advice to one short claim and proposal,
permits only actual check IDs and exact server-approved proposal templates, and
marks local-model claims `UNKNOWN`. The existing schema, freshness and semantic
validators still validate every response. They do not establish that every
natural-language sentence is correct; the UI explicitly labels model advice as
recommendations. Setup actions still require the existing Plan/Apply flow.

Generation uses `stream:false`, temperature 0, 512 output tokens, a 4096-token
context and a two-minute model keep-alive. The whole metadata+generation call
has a 240-second deadline to cover a measured 89.61-second cold fixture startup;
normal live production-component calls completed in about 6–11 seconds during
this qualification. The diagnostic snapshot remains limited to 300 seconds,
and freshness is checked again before accepting model output. Request JSON is
bounded to 32 KiB, the generation envelope to 64 KiB and output text to 8 KiB.
Redirects, environment proxies, compressed responses and automatic retries are
disabled. Malformed, incomplete or rejected output yields an explicit fallback.

## Cancellation and uncertain requests

`ollama-request.json` is an exclusive, durable admission intent for one local
inference across all projects using this host state. It contains only a request
ID/hash, model, phase and timing; no prompt or credentials. A second request
cannot bypass it by using another project, another Python object or a restarted
host. The deployment contract remains one host per owned state root; separate
state roots do not coordinate a shared Ollama service and must not be presented
as a global provider semaphore.

Cancelling the HTTP caller does not claim that Ollama stopped. A retained,
deadline-bounded background task drains the existing response. The cancelled
idempotency key remains cancelled, and another generation is rejected until a
terminal provider response is received. A complete `done:true` reply releases
admission even when its model text fails application validation. A timeout,
oversize/lost/incomplete response or host crash retains an unresolved marker;
the next request returns `LOCAL_PROVIDER_ACTIVE_OR_UNRESOLVED` fallback. A direct
connection failure before delivery can release admission. This adapter never
kills the shared Ollama process and never interprets `/api/ps` or elapsed time as
proof that an outstanding inference retired.

For an unresolved marker, preserve it as evidence. An operator must establish
that the old provider request has ended, for example through a controlled
restart of the specifically owned Ollama service after checking other users of
that service. Stop the owned wizard while reconciling, archive the exact marker
in protected storage, then remove only that verified marker and restart the
wizard. Do not clear it merely because its deadline elapsed or the UI was
closed. There is intentionally no model-callable or automatic reset route.

## Qualification

```powershell
C:/nqt37/v/Scripts/python.exe -B -m unittest tests.test_ollama_advisor tests.test_llm_advisor tests.test_setup_private_api tests.test_setup_diagnostics_regressions tests.test_setup_diagnostics tests.test_wizard_ui -v
```

The automated suite uses explicit transport fixtures for timeout, cancellation,
cloud denial, response bounds, malformed output, secret redaction, passive
configuration and preserved native API/UI behavior. It does not count as a live
provider call. `tests/live_ollama_advisor.py --run-model` separately exercises
the production `SetupAdvisor` with a fresh real issued snapshot, validates actual
Ollama output and checks same-key replay. It performs no setup action or sprint
start. Supply snapshot, protected state directory and evidence output paths;
it never bypasses operator authentication to acquire a snapshot.

Use `--cancel-caller` to verify a real caller cancellation while the bounded
model request drains, and `--require-unloaded` to require that `/api/ps` reported
the selected model absent before dispatch. That metadata distinguishes a cold
model load from a warm call; it is never used to clear an uncertain admission
marker or claim process retirement. The test does not unload shared models.

Sanitized real normal/malicious-question and cancellation-drain evidence is in
`evidence/local-advisor/`. The original snapshot came from the still-running
previous qualified host; the new code was exercised as a production component.
Full private HTTP/browser acceptance of the newly deployed host is a separate
following qualification stage. Existing native sprint history was not changed
by these checks.

Primary protocol references: [Ollama generate](https://docs.ollama.com/api/generate)
and [structured outputs](https://docs.ollama.com/capabilities/structured-outputs).
Terminal HTTP evidence and the conservative unresolved-request behavior above
are implementation guarantees; no stronger cancellation guarantee is inferred
from those protocol pages.
