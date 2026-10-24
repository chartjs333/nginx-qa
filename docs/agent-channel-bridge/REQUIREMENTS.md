# HTTP-only requirement matrix

This matrix supersedes PR #13's in-process companion qualification. Historical
results are preserved in `VALIDATION-PR13-historical.md` and are not HTTP-only evidence.

| Requirement | nginx evidence | Actual stock Hub PR #2 |
| --- | --- | --- |
| Independent services, no foreign imports/DB/shared directories | Embedded composition retired; HTTP-only client; separate HTTPS process tests | Native package launched unchanged; only public API exercised |
| v1 request/event schemas, HMAC, correlation | Contract and callback tests | Actual signed intake and mobile-response callback verified |
| 1.1 discovery/validate/status/cancel/artifact | Wire-compatible remote client; response validation tested | Required service endpoint absent; BLOCKED |
| Async request/response, provenance | Real nginx with independent 1.1 simulator | v1 request/mobile response/callback verified separately |
| Restart and lost HTTP receipt | nginx store/runtime reopened; immutable request retry deduped by simulator | Actual Hub process restart/durable callbacks and v1 duplicate verified; >60s envelope retry rejected |
| Duplicate/conflicting/forged/stale callback | Reject or dedupe, exact scope preserved | Actual response replay deduped by nginx |
| Project/sprint/assignment/agent/research isolation | Negative binding tests, two concurrent agents and multiple channels | v1 rejects foreign agent through live introspection |
| Current operation/channel entitlements | Enforced by nginx admission/dispatch; contract requires remote recheck | Stock active-only introspector ignores operation entitlements; BLOCKED |
| Researcher session revoke | Remote API treated as authority; no local fabrication of grant/session proofs | Actual researcher logout blocks observation |
| Plugin/device/assurance/max-auth-age intersection | Existing policy preserved; HTTP-only remote enforcement required | Complete 1.1 enforcement not available/qualified; no internal-state fallback |
| Waiting dependency, independent work and review gates | Blocking, sibling work, reviewer evidence and two-review completion tested | No direct graph or queue mutation from remote Hub |
| PR #14 coexistence | Temporary runtime patch overlay + selected API tests only | No Hub dependency on Windows/Linux filesystem mapping |
| Production/hardware acceptance | Not claimed | Missing remote 1.1 implementation and operational/device acceptance |
