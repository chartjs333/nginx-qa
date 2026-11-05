# Java/Maven assignments through the existing native client

`java-maven-v1` extends the existing ManagedCodexClient/WSL transport. The native
Managed Sprint Engine still creates assignments, owns the graph, requires two
independent reviews and accepts results. This profile does not create a scheduler.

## Admission

The protected service configuration may declare `project_templates`. Each entry
is a numeric native project ID with exactly these operator-owned fields:

```json
{
  "profile_id": "java-maven-v1",
  "repository_id": "main",
  "repository_url": "https://github.com/chartjs333/lite_final.git",
  "workspace_root": "C:/operator/managed/projects/9020",
  "branch_prefix": "agent/lite-",
  "allowed_document_paths": [
    "src/main/resources/templates/survey/recruitment_tracker.html",
    "src/main/java/net/mdsgene/admin/ui/survey/recruitment/RecruitmentTrackerServiceImpl.java"
  ]
}
```

The retained `allowed_document_paths` key now carries exact approved source paths
for this profile. It is not a glob. Permitted categories are Java main/test sources,
HTML templates, static JS/CSS, test JSON/XML/TXT, Markdown under docs and pom.xml.
Binary attachments, secrets and build scripts cannot be selected for writing.
An XLSX committed by the trusted ingress remains read-only model input.

After publishing an immutable native manifest, the authenticated trusted ingress
calls `POST /managed-client/admit` with exactly `project_id`, `manifest`,
`manifest_sha256`, `manifest_commit`, `git_manifest_path`, and
`allowed_document_paths`. SHA-256 is over the engine's `canonical_json_bytes`.
The requested paths must be a subset of the configured ceiling. The service
checks native schema/graph, immutable source commit, exact repository URL,
assigned branch prefix, two independent review phones and native provenance.
The request cannot choose host paths, credentials or a different repository.

The reply is `{status: "ADMITTED", project_id, sprint_id, manifest_sha256,
profile_id}`. Admission is durable and idempotent. A different pin for an active
project is rejected; same-pin replay is accepted. Restart revalidates admissions
against the current protected template. This receipt is not readiness or success.
The existing prepare/start handshake subsequently verifies the native descriptor,
sandbox readiness and actual native assignment IDs.

## Java environment

Only the separately owned `wsl2-bwrap` backend admits this profile. Provision the
following root-owned resources in that guest, while its worker is idle:

- JDK 8 at `/opt/nginx-qa-worker/java`, including `bin/java` and `bin/javac`.
- Maven available as `/usr/bin/mvn`.
- The project's pre-resolved offline Maven repository at
  `/opt/nginx-qa-worker/maven-repository`, without settings.xml or credentials.

Provisioning dependencies is an operator action, not a model action. The model's
shell remains network-disabled and cannot install dependencies. This repository
does not claim that a particular remote Maven repository or newly downloaded JDK
has been qualified. `prepare` executes real source-write/reviewer-readonly,
network, secret/history isolation canaries and requires Maven to report JDK 8.
Absent resources or a different JDK block readiness explicitly.

For an actual Maven run, the model can copy the sanitized `/work` source to
`/tmp/build`, copy the read-only `/maven-cache` into `/tmp/m2`, then run Maven
offline with `-o -Dmaven.repo.local=/tmp/m2` in that disposable source copy.
The trusted ingress must prime the dependency cache for the specific build first.
Missing offline plugins/dependencies are a failed check, never a passing result.

## Source and execution boundaries

An author can write only individually mounted exact source files. Parent
directories and Git remain read-only. Reviewers cannot change source. Both roles
may use bounded private `/tmp` for compilation/testing. Only approved source bytes
are exported back; model-owned build outputs or Git state are never imported.

For Java jobs the model cannot read `.git` or its history. Known secret file
categories (`.pem`, `.key`, key stores, `.env*`, auth/credentials/settings files,
application properties/YAML) are masked in the source view. This masking is not
a guarantee that arbitrary repository code contains no embedded secret. Trusted
operators must still select an appropriate source repository.

Reviewers receive `/task/source.diff`, computed by the trusted Windows parent
from the native author's initial/result commits and limited to approved paths.
They run independent fresh model contexts. Existing branch/origin/HEAD/scope
checks, exact remote publication proof, result replay and native review receipts
remain mandatory.

Java limits: 128 approved paths, 8 MiB per file/object, 8192 tracked files,
128 MiB current tree, 32768 objects/256 MiB historical expansion, 64 MiB compressed
Git bundle, and 20 MiB changed source bytes. Existing cgroup, wall time, process,
memory and output limits remain active. Documentation profile admission retains
its smaller source/history limits.

## Evidence

`tests.test_managed_coding_profile` tests durable admission, authority narrowing,
policy changes, active-project exclusion, schema/graph/provenance rejection and
both sides of path/mount policy. Existing worker and WSL tests exercise native
contracts and transport regressions. Mock/AST policy tests are not proof of an
actual Java/Maven or model run. Record live installed canary, Maven and native
author/two-review results separately for the chosen project and exact commit.
