# Existing local provider discovered during continuation

On 2026-11-04, read-only discovery found an already running Ollama 0.40.11
listener on 127.0.0.1:11434 and an already installed qwen2.5:7b model. A real
tool-free `/api/generate` request returned valid schema-bound Russian diagnostic
advice (31 output tokens, 89.61 seconds including cold load). It did not execute
actions or require a new key. This was a fixture probe, not full wizard acceptance.

The installed Codex app-server was also probed against a local fake provider,
without credentials or a real model call. Explicit empty environments and
disabled execution/apps/plugins/hooks removed executable tools from the observed
inventory, but `request_user_input` remained. Empty dynamicTools alone is not a
security boundary. This is evidence for investigation, not a qualified advisor.

The user asked to finish using available resources. The future advisor task may
therefore qualify the installed local Ollama provider as the working default,
while preserving the existing Responses transport and strict validation. No
paid key, new model download, OAuth extraction, additional orchestration system
or weaker worker sandbox is needed for this option. Generation must be bounded,
tool-free, request-scoped, and cancellation must not release admission while
untracked work continues. Cold-load latency and resource use require explicit
tests; a fixture response cannot be promoted to live wizard acceptance.

Original probe receipts remain in the operator journal:
`C:/nqt35/client/sdw-continuation-20261104/local-provider-discovery.json`,
`local-provider-response-probe.json`, and `advisor-tool-probe/inventory.json`.

This amendment changes only the not-yet-issued advisor task through the native
versioned repair API. Active isolated-worker work and accepted history stay intact.

Primary documentation: https://docs.ollama.com/api/generate and
https://docs.ollama.com/capabilities/structured-outputs.
