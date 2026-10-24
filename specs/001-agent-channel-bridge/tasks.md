# Development tasks — nginx-qa
- [ ] Inventory existing agent send/receive/auth APIs and document reuse.
- [ ] Freeze shared schemas with Hub and example payloads.
- [ ] Add active sprint channel request/status endpoints with strict auth.
- [ ] Build durable request outbox, status polling, timeout and retry.
- [ ] Receive authenticated Hub events with durable deduped inbox.
- [ ] Route correlated reply into active agent context; pending external input does not stall unrelated work.
- [ ] Add audit/provenance, read-only observations and operator visibility.
- [ ] Run isolated E2E and regression tests for proposal Play/Start and reviewer gates.
Done: tested two-way communication in disposable sprint, no production changes.


- [ ] NQ-10 Define role-based agent capability grants and their intersection with user, lab, plugin and device policies.
- [ ] NQ-11 Define step-up challenge/grant protocol with short-lived authorization, revocation and audit; no plaintext reusable PIN in agent messages.
- [ ] NQ-12 Verify grant on every privileged channel invocation and test unauthorized/expired/revoked scenarios.
- [ ] NQ-13 Contract-test plugin discovery, OS compatibility and authenticated mobile activation with Hub.


- [ ] NQ-14 Include configurable required assurance level on each protected agent/plugin operation; compare with authenticated session assurance and freshness.
- [ ] NQ-15 Test smart-card session start, shared workstation switching, expiry, revocation, and action-specific step-up without continuous card presence.
