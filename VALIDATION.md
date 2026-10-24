# Agent Channel Bridge validation — 2026-10-24

**Draft; not production-ready.** Isolated nginx-qa / actual Inbound Hub server-code integration.
No live service, production database, working sprint/queue, main or source branch was modified.
No merge, deployment or production restart was performed.

## Source identity

- nginx-qa source: `3ac994996e5ffd5fc2e79229622dd6a8f0b15267`, `spec/agent-channel-bridge-v1`.
- Inbound Hub PR #2, unchanged: `d31f6bd54f70a7e7e2e4295b43424b81dc272155`.
- Inbound Hub PR #1 analyzed: `d518e2f1411da188699da47565f2f564da1fd95b`.
- Python 3.12, Linux container; disposable SQLite/artifact roots and fixture credentials.
- Package snapshot and machine-readable results: [evidence](docs/agent-channel-bridge/evidence).

## Required Bridge integration — PASS

```sh
HUB_BRIDGE_SOURCE=../inbound-hub/channel_bridge/src .venv/bin/python -m pytest \
  integration_tests/test_channel_bridge.py -q --disable-warnings --junitxml=../channel-final.xml
```

**51 passed, 0 failed, 0 errors, 0 skipped**, 29.97 seconds. One Starlette/httpx TestClient
deprecation warning. This is actual Gateway/ASGI/introspection/callback/SQLite/cryptographic
verification code, not a Hub contract simulator. Human observations, instrument HTTP responses,
live-provider responses and IdP signing keys are fixtures.

Coverage: full correlated exchange and immutable artifact retrieval; original node/occurrence
origin; 32 concurrent duplicate submissions; multiple agents/channels; independent graph work;
waiting/ack and two existing review gates; active reviewer/coordinator context binding; lost HTTP
acknowledgement; outage longer than v1's 60-second freshness; callback retry and Hub/receiver
restart; request/callback conflicts and replay; forged/stale signatures; scope isolation;
cancel/timeout tombstones; role/project policy and grant/session/plugin/key revocation; same-session
step-up; idle/max session duration; workstation switch; Offline/stale/unanswered call fallback;
read-only HTTP GET observation and video provider contract; config ACLs and secret-field rejection.

Managed graph integration uses the repository's existing continuity fixture and verifier. Its
schema-bound Windows paths remain unchanged; a disposable native Git-provider is injected for
construction on Linux. This proves the channel/store/graph/review transitions, not actual Windows
Git workspace/process execution. The final fixture graph completes only after both existing
review approvals. Frozen managed runtime/identity/review schemas are unchanged.

## Targeted existing regression — PASS except explicit platform skips

```sh
TMPDIR=/workspace/scratch/6b32e2b88e04/test-tmp .venv/bin/python -m pytest \
  tests/test_inbound_proposal_api.py tests/test_inbound_coordinator_contract.py \
  tests/test_pending_sprints_ui.py tests/test_queue_persistence.py \
  tests/test_scope_workflow.py tests/test_sprint_type_contract.py \
  tests/test_legacy_scope_control.py tests/test_operator_session.py \
  -q -rs --disable-warnings --junitxml=../target-final.xml
```

**138 passed, 256 subtests passed, 2 skipped**, 57.13 seconds, no failures/errors.
JUnit counts include subtests; do not add its aggregate again to the pytest headline.
Both skips are pre-existing Windows-specific ACL probes in `test_inbound_proposal_api.py`
(lines 2763 and 2859). **Neither skip is a passed Windows security gate.**

The standalone sprint contract suite also passed: **46 tests + 160 subtests**. These overlap
with the targeted run and are not additional unique coverage.

## Broader repository attempt — NOT PASSED

`python -m pytest tests --ignore=tests/test_channel_bridge.py -q --disable-warnings` was attempted
before the new integration suite was moved out of `tests/`. It encountered numerous failures and
platform skips and was interrupted (exit 130); no complete result/count or global pass is claimed.

An unchanged detached base checkout was used to diagnose the first managed-continuity failure:

```sh
python -m pytest tests/test_managed_continuity.py -q --maxfail=1 --disable-warnings
```

Base result: **1 failed** in fixture setup, `ValueError: managed_root must be absolute` for
`D:/nginx-qa-staging/managed` on Linux. The new Bridge suite supplies a native provider without
weakening runtime path validation. This does not qualify all skipped/failed native managed tests.

A separate unchanged-base diagnostic over scope/queue/pending suites produced **40 passed,
26 subtests passed, 1 failed**: the historical scope round-trip assertion that the history file
starts with its earlier bytes (`tests/test_legacy_scope_control.py:1649`). The final targeted
candidate run above passed that scenario. Do not infer that every broad-suite failure has been
attributed or repaired. Full native Windows managed execution/process regression remains open.

## Actual unchanged Hub regression

```sh
cd ../inbound-hub/channel_bridge
PYTHONPATH=src ../../nginx-qa/.venv/bin/python -m pytest tests -q -rs --disable-warnings
```

**43 passed, 1 skipped**, 2.34 seconds; same TestClient deprecation warning. The skip is the
mandatory browser test requiring `HUB_TEST_CHROMIUM`. **Browser acceptance is not passed.**
PR #2's own VALIDATION also records a prior Chromium SIGTRAP, not a browser acceptance result.
No Hub tracked source was changed. The run used the unmodified PR #2 code.

## Static, type, schema and security checks — PASS within stated scope

```sh
.venv/bin/ruff check nginx_qa/channel*.py integration_tests/test_channel_bridge.py scripts/*channel*.py
.venv/bin/python -m mypy --follow-imports=skip --ignore-missing-imports --check-untyped-defs nginx_qa/channel*.py
.venv/bin/python scripts/export_channel_schemas.py --check
.venv/bin/python -m compileall -q main.py nginx_qa integration_tests scripts
.venv/bin/python -m bandit -q nginx_qa/channel*.py -f json -o ../channel-bandit-final.json
.venv/bin/pip-audit --disable-pip --no-deps -r ../environment-freeze.txt --format json -o ../dependency-audit.json
git diff --check
```

Ruff clean; mypy clean for six new runtime modules (imports deliberately not type-checked);
schema generation matches committed models; compilation and whitespace checks pass.
Bandit reports **0 findings** over new runtime modules. Dependency audit reports **no known
vulnerabilities** in the complete installed environment snapshot (including development tools).
This snapshot is not a hash-locked production supply-chain attestation. No independent
penetration test, full-repository SAST or complete historical secret scan is claimed.

`scripts/check_channel_diff_secrets.py` checks staged added lines for high-signal private-key/API-token
patterns without printing matched values. This is a narrow diff scan, not comprehensive secret detection.

`scripts/run_channel_bridge_checks.py /absolute/inbound-hub/channel_bridge/src` reproduces the
required integration/static checks, verifies the exact Hub commit and tracked source cleanliness,
and rejects any skipped integration case. It does not start a deployed service or run live hardware.

## Remaining gates / limitations

1. Full native Windows managed runtime/process/Git and ACL regression, and diagnosis of the entire
   broad repository suite. The Linux fixture tests do not substitute for those gates.
2. Original Hub needs the explicit nginx-owned **1.1 companion composition**. Its unextended v1
   entry point lacks required discovery/cancel/artifact/policy interfaces and long-outage recovery.
   Cross-repo maintainer acceptance of that versioned solution remains a review decision.
3. Real trusted IdP, smart-card reader, authorized researcher/device/plugin policies and credentials;
   genuine mobile/browser/audio/video/instrument acceptance. No real endpoint was available.
4. Hub raw video artifacts and native mobile installed-module attestation remain unavailable.
   Video session initiation supports a configured provider; its real behavior is unqualified.
5. Distributed state/authorization, operational backup/restore/retention/load/soak and independent
   security review. Local persistence/negative tests are not production operational acceptance.
6. Legacy sprint queues retain their existing interfaces; this opt-in channel runtime is for
   active managed execution/reviewer/coordinator assignments. No physical instrument actuation.

These limitations are disclosed requirements/gates, not skipped successes. No production-ready claim.
