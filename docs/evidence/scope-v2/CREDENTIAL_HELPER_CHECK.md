# Credential helper verification

Date: 2026-10-05. Development checkout: `D:\nginx-qa-scope-v2`.

## Result and safety boundary

**PASS:** real Windows DPAPI `CurrentUser` roundtrip and isolated HTTP checks
for `scripts/Authorize-ScopeBrowser.ps1` and
`scripts/Invoke-ScopeRoleRequest.ps1`. Existing Delta wrapper **file-format
compatibility is confirmed from source**, not by accessing production secrets.

No production vault contents were read, decrypted, enumerated or modified.
No request was sent to 18025 or 8025. No production wrapper was executed or
dot-sourced. No service restart, deployment, live ACK, scope mutation, claim,
handoff or migration occurred. No credential value or fragment is in this
document, command arguments, captured output or Git.

## Synthetic DPAPI and HTTP check

Host runtime: PowerShell **7.6.6**, under the current Windows user. The temporary
test generated three independent 32-byte random values encoded as 64 lowercase
hex characters: one admin token and two role tokens (`2750`, `2751`). These role
IDs are synthetic routing cases; they do not provision or change Delta roles.

The test stored only raw DPAPI-encrypted bytes in `.dpapi` files. The temporary
directory and credential files had owner-only ACLs with inheritance disabled.
All three ciphertexts were reread and successfully decrypted under
`DataProtectionScope.CurrentUser`; distinctness and exact plaintext roundtrip
were checked in memory without printing values.

A hidden temporary `HttpListener` process independently decrypted the same
synthetic files to authenticate requests. It listened only on
`http://127.0.0.1:55152/` (ephemeral test port; 18025 and 8025 were explicitly
excluded). The harness PID was `84576`.

| Check | Result |
| --- | --- |
| Operator pairing-code POST uses the admin header, not the role header | PASS |
| Lowercase pairing code is sent in the expected uppercase form | PASS |
| Role `2750` GET `scope-request-context` uses its role token, not admin | PASS |
| Subsequent role `2751` GET in the same session rereads that role's token | PASS |
| POST without `-AllowMutation` fails before any HTTP request | PASS |
| POST with `-AllowMutation` and a body reaches only the synthetic harness | PASS |
| All captured helper success/error output excludes every synthetic token | PASS |
| Only four HTTP requests were received; all matched the expected credentials | PASS |

The positive POST exercised a fake ACK endpoint in the harness; it did not
contact an application runtime or acknowledge an assignment. The harness
returned only synthetic context and authorization results. This is evidence
for helper transport/credential behavior, **not** an end-to-end browser pairing
test of the production operator-session implementation.

The child was stopped in `finally`. Its PID was verified absent and its test
port had no listener. The exact validated temporary directory, scripts,
ciphertexts and request records were removed. No synthetic credentials remain
in this evidence or in the repository.

## Existing Delta credential format: source-only comparison

Read-only source inspection was limited to these existing local helper files
under `C:\Users\madoev\AppData\Local\nginx-qa\ops\delta-18025`:

- `Common.ps1`: configured names, `Read-Credential`, DPAPI decoding and headers.
- `Delta-Operator.ps1`: the `Initialize` implementation, inspected but not run.
- `Invoke-DeltaRoleRequest.ps1`: role selection and request-header delegation.

The source declares `%LOCALAPPDATA%\nginx-qa\secrets\delta-18025` as the vault
and writes the following filenames (no vault access was needed to establish
this naming contract):

```text
NGINX_QA_SCOPE_CONTROL_ADMIN_TOKEN.dpapi
NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_2750.dpapi
NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_2753.dpapi
NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_2754.dpapi
NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_2791.dpapi
NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_2792.dpapi
```

`Delta-Operator.ps1` generates 32 random bytes, encodes them as lowercase hex,
converts the resulting 64-character string to UTF-8 bytes, calls
`ProtectedData.Protect(plain, null, CurrentUser)`, and writes the returned bytes
directly with `FileMode.CreateNew`. `Common.ps1` reads binary bytes, calls
`ProtectedData.Unprotect(cipher, null, CurrentUser)`, decodes UTF-8, and checks
`^[0-9a-f]{64}$`.

The new helpers expect **exactly this same format**: `<environment-name>.dpapi`,
raw binary DPAPI, no optional entropy, `CurrentUser`, UTF-8 plaintext and 64
lowercase hex characters. This is not CLIXML, `ConvertFrom-SecureString` text,
Base64-wrapped ciphertext or a JSON envelope. No credential conversion,
regeneration or copying into the repository is required by this format.
The existing and new helpers use the same request headers:
`X-Nginx-QA-Scope-Control-Token` for the operator and
`X-Nginx-QA-Scope-Token` for a role.

Compatibility is conditional on using the actual vault path and the Windows
identity that encrypted it, and on passing ACL checks at deployment time.
Neither the current contents of that vault nor current decryptability was
verified here. The older helper has additional fixed-account, vault-layout
and ancestor-path checks; file-format compatibility does not assert that the
two helpers have identical policy or endpoint allowlists. The old role helper
does not allow the new `scope-request-context` endpoint, so it is not itself a
replacement for the new role helper. No old helper was changed.

## Source hashes inspected

These hashes identify script source, never credentials or credential hashes.

| Source | SHA-256 |
| --- | --- |
| Existing `Common.ps1` | `3E728F5A3663C3FE0C10FD3DBF66071406EED6E4C49D694EA35A1F3331B1609D` |
| Existing `Delta-Operator.ps1` | `509683A0609712D96BE38CA50164352E1218D07383BA3F5BCD83E73B1EC27893` |
| Existing `Invoke-DeltaRoleRequest.ps1` | `A7A840D31FF17E342450E20F84C56CE13C47A8DF2F5CDA581024389EE372FEDC` |
| New `scripts/Authorize-ScopeBrowser.ps1` | `7479C7F13AAEB283439AB4F3BD4E87A76455463CAF0596684389BC8BAC7E6672` |
| New `scripts/Invoke-ScopeRoleRequest.ps1` | `3094E7A9E85323CE1DA10E4BF16A4A69312AC91949B31CC60B60256FECFA9199` |
