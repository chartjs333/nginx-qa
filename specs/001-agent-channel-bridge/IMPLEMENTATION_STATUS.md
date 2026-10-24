# nginx-qa implementation status

Implementation branch: `work/nginx-agent-channel-bridge-v1`.
Source/target: `spec/agent-channel-bridge-v1` at `3ac994996e5ffd5fc2e79229622dd6a8f0b15267`.

The code, schemas, active managed assignment API, durable communication mailbox, signed callbacks,
request lifecycle, scoped authorization, scientist profiles, graph dependency guard, optional Hub
1.1 interoperability adapter and automated integration tests are implemented. Existing pending
proposal and Play/Start interfaces are unchanged. The unchanged Hub PR #2 is used by integration
tests, not replaced by a contract simulator.

- [Architecture, API, configuration, contract differences, limits](../../docs/agent-channel-bridge/README.md)
- [Requirement and test matrix](../../docs/agent-channel-bridge/REQUIREMENTS.md)
- [Executed commands/results and outstanding gates](../../VALIDATION.md)

Source task checkboxes are not used to claim operational/hardware acceptance. Windows platform
qualification, real credentials/IdP, browser/mobile/hardware/provider acceptance and independent
security review remain separate gates. Draft only; no merge, production service action or deployment.
