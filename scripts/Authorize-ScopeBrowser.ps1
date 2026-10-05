param(
    [Parameter(Mandatory)][ValidatePattern('^[A-Fa-f0-9]{10}$')][string]$PairingCode,
    [Parameter(Mandatory)][uri]$BaseUri,
    [Parameter(Mandatory)][string]$SecretDirectory
)
$ErrorActionPreference = 'Stop'
# Explicit operator authentication only. This does not apply scope or ACK work.
if ($BaseUri.Scheme -ne 'http' -or $BaseUri.Host -ne '127.0.0.1' -or $BaseUri.AbsolutePath -ne '/' -or $BaseUri.Query -or $BaseUri.UserInfo) {
    throw 'Only an explicit loopback HTTP origin is supported by the local DPAPI helper.'
}
$credentialPath = Join-Path $SecretDirectory 'NGINX_QA_SCOPE_CONTROL_ADMIN_TOKEN.dpapi'
$sid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$item = Get-Item -LiteralPath $credentialPath
if ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'Credential reparse points are forbidden.' }
$acl = Get-Acl -LiteralPath $credentialPath
foreach ($rule in $acl.Access) {
    if ($rule.AccessControlType -eq 'Allow' -and $rule.IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value -notin @($sid, 'S-1-5-18')) {
        throw 'Credential ACL grants another principal access.'
    }
}
$plain = $null
$secret = $null
$headers = @{}
try {
    $cipher = [IO.File]::ReadAllBytes($credentialPath)
    $plain = [Security.Cryptography.ProtectedData]::Unprotect($cipher, $null, [Security.Cryptography.DataProtectionScope]::CurrentUser)
    $secret = [Text.Encoding]::UTF8.GetString($plain)
    if ($secret -cnotmatch '^[a-f0-9]{64}$') { throw 'Credential format invalid.' }
    $headers = @{'X-Nginx-QA-Scope-Control-Token' = $secret}
    $payload = @{pairing_code = $PairingCode.ToUpperInvariant()} | ConvertTo-Json -Compress
    $null = Invoke-RestMethod -Method Post -Uri ([uri]::new($BaseUri, '/api/v1/operator/session/authorize')) -Headers $headers -Body $payload -ContentType 'application/json' -MaximumRedirection 0 -NoProxy -TimeoutSec 15
    Write-Output 'Browser session authorized. No scope mutation or ACK was sent.'
} catch {
    throw 'Browser authorization failed. Check the origin, pairing code, account and credential ACL.'
} finally {
    if ($plain) { [Array]::Clear($plain, 0, $plain.Length) }
    $headers.Clear()
    $secret = $null
}
