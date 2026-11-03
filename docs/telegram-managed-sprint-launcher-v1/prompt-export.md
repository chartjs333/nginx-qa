# Telegram authoring prompt

Enable `NGINX_QA_TELEGRAM_LAUNCHER_ENABLED=true` on the existing Telegram webhook
service. For the public gateway configure protected
`NGINX_QA_LAUNCHER_CONTROL_BOT_TOKEN` and
`NGINX_QA_LAUNCHER_CONTROL_WEBHOOK_SECRET` through the trusted secret provider,
plus the server-owned `NGINX_QA_TELEGRAM_LAUNCHER_POLICY_JSON`. The protected
reference settings and legacy private fallback are described in gateway.md. Keep actual credentials
outside the repository. With the feature disabled, existing Telegram behavior is
unchanged. Enabled requests require the webhook secret even for legacy payloads.

Example policy (replace the example user with an explicitly authorized operator):

```json
{
  "schema_version": 1,
  "grants": [
    {"user_id": "123456", "chat_id": "123456", "project_id": "9000", "actions": ["prompt"]}
  ],
  "projects": {
    "9000": {"repository_id": "main", "source_ref": "refs/heads/main", "checksum_paths": ["README.md"]}
  }
}
```

Use the canonical project phone registered by Project Manager 0001. The repository
alias must resolve to that project's registered Git address. There is no URL or
filesystem-path argument. Configure checksum paths to the source contract files
that the planned sprint needs, not credentials or generated runtime state.

In a private bot chat:

```text
/sprint_prompt 9000 Add isolated tests for the existing feature
/sprint_prompt 9000 --text Add isolated tests for the existing feature
```

The default response is a UTF-8 file; `--file` is also explicit. `--text` produces
numbered chunks measured in UTF-16 code units, with no Markdown interpretation.
For a bot-qualified command set `NGINX_QA_TELEGRAM_BOT_USERNAME` (without `@`).
Tasks are limited to 16 KiB, updates to 1 MiB, exports to 512 KiB and text replies
to 64 chunks. Recognized credentials in task text are rejected.

The export includes installed schemas, managed contract and architecture rules,
their SHA-256 digest, the actual resolved source commit and raw Git blob hashes.
Source context uses the existing isolated Git mirror; no sprint or pending sprint
is created, no workspace or agent is started, and no model API is called. Copy the
prompt to your external AI, then inspect its returned JSON. A deterministic test
fixture passing validation is not evidence of an actual external model round trip.

The implemented intake, preparation, controls and protected Mini App are described
in the adjacent guides. Prompt export itself remains a read-only authoring action. The suggested
new envelope explicitly sets `autonomous_mode=false`; omitting the flag from this
envelope is invalid. Unwrapped old legacy documents retain their own dispatch.
