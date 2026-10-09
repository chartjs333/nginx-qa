param(
    [Parameter(Mandatory)][ValidatePattern('^[0-9]+$')][string]$Role,
    [Parameter(Mandatory)][uri]$BaseUri,
    [Parameter(Mandatory)][string]$SecretDirectory,
    [Parameter(Mandatory)][string]$Path,
    [ValidateSet('GET','POST')][string]$Method = 'GET',
    [string]$BodyFile,
    [switch]$AllowMutation
)
$ErrorActionPreference = 'Stop'
# Re-read the CURRENT role credential on every request: an existing sequential
# agent does not inherit environment variables from a different PowerShell.
if ($BaseUri.Scheme -ne 'http' -or $BaseUri.Host -ne '127.0.0.1' -or $BaseUri.AbsolutePath -ne '/' -or $BaseUri.Query -or $BaseUri.UserInfo) { throw 'Explicit loopback origin required.' }
$readPath = '^/api/v1/projects/[^/?#]+/assignments/[^/?#]+/(effective-scope|scope-request-context)$'
$writePath = '^/api/v1/projects/[^/?#]+/(assignments/[^/?#]+/effective-scope/ack|sprints/[^/?#]+/scope-requests|agents/' + [regex]::Escape($Role) + '/whoami)$'
if (($Method -eq 'GET' -and $Path -notmatch $readPath) -or ($Method -eq 'POST' -and $Path -notmatch $writePath)) { throw 'Path is not an allowed role endpoint.' }
if ($Method -eq 'POST' -and (-not $AllowMutation -or -not $BodyFile)) { throw 'POST requires explicit AllowMutation and a request body file.' }
if ($Method -eq 'GET' -and $BodyFile) { throw 'GET cannot carry a request body.' }
$credentialPath = Join-Path $SecretDirectory ('NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_' + $Role + '.dpapi')
$sid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
if ((Get-Item -LiteralPath $credentialPath).Attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'Credential reparse point forbidden.' }
foreach ($rule in (Get-Acl -LiteralPath $credentialPath).Access) {
    if ($rule.AccessControlType -eq 'Allow' -and $rule.IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value -notin @($sid, 'S-1-5-18')) { throw 'Credential ACL grants another principal access.' }
}
$plain = $null
$headers = @{}
try {
    $plain = [Security.Cryptography.ProtectedData]::Unprotect([IO.File]::ReadAllBytes($credentialPath), $null, [Security.Cryptography.DataProtectionScope]::CurrentUser)
    $secret = [Text.Encoding]::UTF8.GetString($plain)
    if ($secret -cnotmatch '^[a-f0-9]{64}$') { throw 'Credential format invalid.' }
    $headers['X-Nginx-QA-Scope-Token'] = $secret
    $arguments = @{Method=$Method; Uri=[uri]::new($BaseUri,$Path); Headers=$headers; MaximumRedirection=0; NoProxy=$true; TimeoutSec=30}
    if ($BodyFile) {
        $payload = [IO.File]::ReadAllText((Resolve-Path -LiteralPath $BodyFile).Path)
        if ($payload.Contains($secret)) { throw 'Credential in request body forbidden.' }
        $null = $payload | ConvertFrom-Json
        $arguments.Body = $payload
        $arguments.ContentType = 'application/json'
    }
    $response = Invoke-RestMethod @arguments
    $serializedResponse = $response | ConvertTo-Json -Depth 100
    if ($serializedResponse.Contains($secret)) { throw 'Credential-bearing response suppressed.' }
    Write-Output $serializedResponse
} catch {
    $statusCode = 'unavailable'
    $errorCode = 'REQUEST_FAILED'
    if ($_.Exception.Response) { $statusCode = [string][int]$_.Exception.Response.StatusCode }
    try {
        $errorBody = $_.ErrorDetails.Message | ConvertFrom-Json
        if ($errorBody.detail.error -cmatch '^[A-Z0-9_]{1,100}$') { $errorCode = $errorBody.detail.error }
    } catch {}
    throw "Role request failed: HTTP $statusCode / $errorCode. Credential details are suppressed."
} finally {
    if ($plain) { [Array]::Clear($plain, 0, $plain.Length) }
    $headers.Clear()
    $secret = $null
}
