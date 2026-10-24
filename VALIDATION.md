# HTTP-only Inbound Hub integration validation

## Result and scope

**Partial wire compatibility is confirmed; full adapter 1.1 compatibility with
stock Hub PR #2 is NOT confirmed and is blocked by missing Hub service APIs.**

The final targeted run passes **42 tests, zero failures/errors/skips**:

* 15 HTTP client/contract/security tests.
* 22 real nginx runtime/API tests against an independent HTTPS **1.1 contract
  simulator**, covering retries, callback recovery, correlation, concurrency,
  waiting dependencies, independent assignments and existing review gates.
* 5 tests against the **actual unchanged Hub executable** over public HTTP/HTTPS.
  Two of these deliberately detect incompatibilities; their pass is evidence of
  the gap, not evidence that the missing feature is implemented.

No full regression suite, Windows/Linux path audit, unrelated integrations, live
service, production queue/sprint/database, merge or deployment was used.

## Inputs and branch isolation

* nginx PR #13 base: `work/nginx-agent-channel-bridge-v1`,
  `3629e9a85075f715ac7d00ed4383bf89a0e3a8cc`.
* New correction branch: `work/bridge-http-only`; its PR targets PR #13's branch,
  so the existing Bridge implementation is not duplicated against main.
* Hub PR #2: `d31f6bd54f70a7e7e2e4295b43424b81dc272155`, unchanged.
* PR #14: `e717caaab0358d378dd5209666d92af638114185`; only a temporary runtime patch
  overlay is used for the directly relevant API coexistence checks below.
* Hub wheel: `inbound_agent_channel_bridge-0.1.0-py3-none-any.whl`, tested SHA-256
  `211acb8e6c24f11469dfdd03e0e4a551385171d51d98c414143a4e6bc3753697`.

The test controller provisions the Hub wheel/config/private state in its own
process environment and launches its public `python -m agent_channel_bridge`
entry point. nginx receives only the HTTPS endpoint and its own key/policy.
There are no Hub imports, database reads, artifact-directory access, or source
checkout assumptions in nginx. TLS certificates are test-only; validation is
never disabled. Each service owns separate temporary state. A loopback TLS proxy
fronts the native Hub HTTP listener, as its documented deployment model requires.

## Confirmed issues and fixes

| Finding | Reproduction / change |
| --- | --- |
| PR #13's embedded 1.1 companion imported Hub modules and queried its SQLite tables | Baseline architecture test failed. Retired embedded launcher; 1.1 adapter now reuses the existing HTTP client only. No Hub internals or DB fallback remain. |
| 2xx submit could acknowledge a different/missing request ID; malformed cancel/status/discovery accepted | Five baseline contract tests failed. Added action-specific response validation. Invalid receipts leave the nginx outbox pending with retry, never marked sent. |
| Non-ASCII callback digest raised `TypeError` instead of authentication rejection | Baseline signature test failed. Validate the HMAC hex representation before constant-time comparison; HTTP malformed/duplicate signature-header tests return 403. |
| Missing remote API was indistinguishable from transient HTTP failure | 404/405/501 now surface `CHANNEL_HUB_CONTRACT_UNAVAILABLE` (502). No unsafe v1 downgrade. |
| Previous integration harness imported Hub models/Gateway and shared in-process objects | Replaced by separate HTTPS peers and public-API tests. Previous qualification retained as historical, explicitly non-HTTP-only evidence. |

## Actual Hub API findings

Verified using its public OpenAPI and signed HTTP requests, not private DB calls:

1. `POST /v1/channel-requests` accepts the nginx v1 wire envelope and HMAC framing.
   Live scoped Bearer introspection and foreign-agent rejection work at this level.
2. A researcher authenticates via the actual mobile challenge/login API with
   Ed25519 fixture identity proofs. An observation returns via actual signed Hub
   callbacks to the original nginx agent/assignment/sprint/research context.
   Replaying the actual event is deduplicated. Logout blocks further observation.
3. Actual Hub process restart preserves v1 request deduplication and callback
   outbox delivery. A temporary 503 at nginx's callback endpoint is recovered
   after Hub restart; no duplicate event is inserted into nginx.
4. **No `/v1/nginx-qa-bridge` endpoint exists.** Service discovery/validation/status/
   cancellation/artifact endpoints required by 1.1 are unavailable. Mobile routes
   require a researcher session and are not interchangeable service credentials.
5. **Old v1 envelopes are rejected before deduplication** even with a fresh HTTP
   timestamp/HMAC. Recovery of an immutable request after >60 seconds cannot be
   guaranteed through the existing v1 route.
6. **v1's introspector consumes only `active` and the identity binding.** A real
   HTTP test removes `send_message` from nginx's channel entitlement while leaving
   the assignment active: stock Hub still accepts that operation. This is a
   confirmed remote contract gap, not an approved authorization behavior.

Fixing items 4–6 requires a versioned, Hub-owned HTTP implementation. Hub source
was not modified. The nginx client cannot securely fabricate current grants,
plugin/device/session/assurance proofs or recover those APIs from Hub files.
The precise remote 1.1 contract and migration boundary are in README.md.

## Exact setup and test commands

cwd: `/workspace/scratch/6b32e2b88e04/nginx-http`, unless indicated.

```sh
P=/workspace/scratch/6b32e2b88e04/nginx-qa/.venv/bin/python
T=/home/agent/.codex/skills/remote-skills
W=/workspace/scratch/6b32e2b88e04/http-hub-dist/inbound_agent_channel_bridge-0.1.0-py3-none-any.whl
```

Only existing test dependencies were used/extended in an isolated virtualenv.
`T` is a sandbox-permitted temporary parent outside Git; test directories below
it are private and cleaned. This is not a runtime/service setting or path audit.

Build unchanged Hub without writing into its repository:

```sh
# cwd: /workspace/scratch/6b32e2b88e04/inbound-hub
mkdir -p /workspace/scratch/6b32e2b88e04/http-hub-build
git archive d31f6bd54f70a7e7e2e4295b43424b81dc272155 channel_bridge | \
  tar -x -C /workspace/scratch/6b32e2b88e04/http-hub-build
"$P" -m pip install setuptools wheel
"$P" -m pip wheel --no-deps --no-build-isolation \
  /workspace/scratch/6b32e2b88e04/http-hub-build/channel_bridge \
  --wheel-dir /workspace/scratch/6b32e2b88e04/http-hub-dist
```

Result: wheel built successfully; source hash and wheel hash above. The first
build attempt failed because setuptools was absent; installing setuptools
84.2.1 and wheel 0.48.0 resolved that test-environment prerequisite. A preliminary
`pip install --no-deps --target /workspace/scratch/6b32e2b88e04/http-hub-installed "$W"`
also succeeded; final tests use the wheel in each private Hub process instead.

Baseline regressions before their corresponding fixes:

```sh
"$P" -m pytest -q tests/test_channel_http_contract.py \
  --junitxml=docs/agent-channel-bridge/http-evidence/baseline-contract.xml
"$P" -m pytest -q tests/test_channel_http_contract.py::test_non_ascii_signature_is_rejected_without_server_error \
  --junitxml=docs/agent-channel-bridge/http-evidence/baseline-signature.xml
```

Results: **6 failed** (five invalid HTTP responses plus prohibited imports), then
**1 failed** (non-ASCII signature TypeError). These failures are retained as evidence.

Final command after corrections, including actual Hub restart/outbox recovery:

```sh
mkdir -p "$T" && TMPDIR="$T" HUB_HTTP_WHEEL="$W" "$P" -m pytest -q \
  tests/test_channel_http_contract.py \
  integration_tests/test_channel_bridge.py \
  integration_tests/test_hub_public_http.py \
  --junitxml=docs/agent-channel-bridge/http-evidence/targeted-final.xml \
  > docs/agent-channel-bridge/http-evidence/targeted-final.txt 2>&1
```

**42 passed in 53.80s**, exit 0, no skips. The preceding complete run also passed
42 tests in 52.84s; the final rerun adds explicit actual-Hub callback-outage recovery.

Harness iterations (not hidden or counted as passed gates), using the same `P`,
`T`, and `HUB_HTTP_WHEEL` for real-Hub commands:

| Exact pytest selection/options | Outcome / correction |
| --- | --- |
| `-q integration_tests/test_channel_bridge.py --junitxml=docs/agent-channel-bridge/http-evidence/simulator-initial.xml` | Interrupted, exit 130; TLS keepalive/fixture shutdown needed bounded cleanup. No success claimed. |
| `-xq integration_tests/test_channel_bridge.py::test_http_roundtrip_wait_ack_and_review_gates` | 1 passed (5.28s) after subprocess cleanup correction. |
| `-xq integration_tests/test_channel_bridge.py --junitxml=docs/agent-channel-bridge/http-evidence/simulator-2.xml` | 1 passed, 1 failed: test-server shutdown waited on keepalive; bounded uvicorn shutdown added. |
| `-xq integration_tests/test_channel_bridge.py --tb=short --junitxml=docs/agent-channel-bridge/http-evidence/simulator-3.xml` | 19 passed before three additional correlation/reviewer/header cases. |
| `-xq integration_tests/test_hub_public_http.py --junitxml=docs/agent-channel-bridge/http-evidence/real-initial.xml` | 1 setup error: inherited SOCKS proxy caused missing socksio in stock Hub's environment-aware callback client. |
| `-xq integration_tests/test_hub_public_http.py --tb=short` | Same setup error diagnosed; private Hub subprocess environment now omits proxy variables so all traffic stays loopback. No Hub source changes. |
| `-xq integration_tests/test_hub_public_http.py --tb=short --junitxml=docs/agent-channel-bridge/http-evidence/real-2.xml` | 3 passed, 1 failed: test proxy shadowed `/openapi.json`; proxy's own documentation route disabled. |
| Final three-file selection above, before fixture registration fix | 37 passed, 4 errors: lint removed a shared pytest fixture import; explicit fixture registration corrected. |

The runner `scripts/run_channel_bridge_checks.py --hub-wheel "$W"` provides the
same three selections individually and rejects failures/errors/skips. It does
not require a Hub repository path or install Hub in the nginx interpreter.

## Static checks

```sh
"$P" -m ruff check nginx_qa/channel_hub_adapter.py nginx_qa/channel_hub_service.py \
  nginx_qa/channel_bridge.py nginx_qa/channel_models.py tests/test_channel_http_contract.py \
  integration_tests/http_support.py integration_tests/http_peer.py \
  integration_tests/test_channel_bridge.py integration_tests/test_hub_public_http.py \
  scripts/run_channel_bridge_checks.py
"$P" -m mypy --follow-imports=skip --ignore-missing-imports --check-untyped-defs \
  nginx_qa/channel_hub_adapter.py nginx_qa/channel_hub_service.py \
  nginx_qa/channel_bridge.py nginx_qa/channel_models.py
"$P" -m bandit -q nginx_qa/channel_hub_adapter.py nginx_qa/channel_hub_service.py \
  nginx_qa/channel_bridge.py nginx_qa/channel_models.py
"$P" scripts/export_channel_schemas.py --check
git diff --check
```

All pass, exit 0. No schema drift. No broad application lint/type/regression claim.
The architecture test verifies runtime channel modules have no Hub internal imports.

## Remaining limits

* Remote Hub 1.1 endpoints, immutable long-delay retry semantics and operation-
  scoped/assurance-aware introspection are required; modifying Hub is outside the
  authorized task. Full independent-service integration remains blocked.
* Simulator results are not real-Hub 1.1 acceptance. Actual public v1 wire/researcher/
  callback/restart tests cover only the implemented native surface.
* The local nginx restart test reopens its server/runtime/store objects; the Hub
  restart test terminates and starts the actual independent executable. Production
  supervisor, distributed failover, TLS deployment and hardware are not qualified.
* Existing production/hardware/security acceptance gates remain open. No production-
  ready claim, merge, deployment, path audit or live data access is made.

## PR #14 coexistence: only API-relevant checks

A detached temporary checkout of correction commit `4e5aca0` received only the
runtime-file diff of PR #14. No merge operation was used. The overlay was not
committed to the correction branch or copied into its PR. No path tests ran.

```sh
# cwd: nginx-http
 git worktree add --detach ../nginx-http-paths HEAD
 git diff 627b478e29f5f7254d279363790f46e92c114d8d origin/work/portable-path-configuration -- \
   main.py nginx_qa/git_provider.py nginx_qa/managed_import.py \
   nginx_qa/prompt_archive_migration.py nginx_qa/sprint_types.py \
   nginx_qa/workspace_manager.py staging_host_app.py nginx_qa/paths.py \
   nginx_qa/path-defaults.properties \
   > /workspace/scratch/6b32e2b88e04/http-paths-runtime.patch
# cwd: /workspace/scratch/6b32e2b88e04/nginx-http-paths
 git apply --check /workspace/scratch/6b32e2b88e04/http-paths-runtime.patch
 git apply /workspace/scratch/6b32e2b88e04/http-paths-runtime.patch
 mkdir -p "$T" && TMPDIR="$T" HUB_HTTP_WHEEL="$W" "$P" -m pytest -q \
   integration_tests/test_channel_bridge.py::test_http_roundtrip_wait_ack_and_review_gates \
   integration_tests/test_channel_bridge.py::test_restart_after_lost_receipt_and_delayed_callback \
   integration_tests/test_hub_public_http.py::test_stock_hub_v1_signed_request_mobile_response_and_callback \
   --junitxml=/workspace/scratch/6b32e2b88e04/nginx-http/docs/agent-channel-bridge/http-evidence/pr14-api-compatibility.xml \
   > /workspace/scratch/6b32e2b88e04/nginx-http/docs/agent-channel-bridge/http-evidence/pr14-api-compatibility.txt 2>&1
```

Patch check/application succeeded without conflicts. **3 passed in 13.04s**,
exit 0, no skips. This verifies the selected HTTP/mailbox/restart/review paths with
PR #14's runtime changes; it is not a new path-portability audit or full regression.

Additional changed-component compilation:

```sh
"$P" -m compileall -q nginx_qa/channel_hub_adapter.py nginx_qa/channel_hub_service.py \
  nginx_qa/channel_bridge.py nginx_qa/channel_models.py integration_tests/http_peer.py \
  integration_tests/http_support.py integration_tests/test_channel_bridge.py \
  integration_tests/test_hub_public_http.py tests/test_channel_http_contract.py \
  scripts/run_channel_bridge_checks.py
sha256sum /workspace/scratch/6b32e2b88e04/http-hub-dist/inbound_agent_channel_bridge-0.1.0-py3-none-any.whl
```

Compilation passed. Wheel hash matches the value above. Hub `git status --short`
is empty and its HEAD remains `d31f6bd54f70a7e7e2e4295b43424b81dc272155`.
