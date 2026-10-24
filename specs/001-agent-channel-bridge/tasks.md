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
