# Madoev EEG — NGINX QA Managed Sprint runbook (28.10.2026)

## Current delivery
- **Pilot manifest:** `projects/madoev-eeg/orchestration/pilot_managed_sprint_v1.json` — `managed_workspace_v1`, sequential, **one** AR-04 worker assignment + coordinator + terminal states, **two independent reviewers**.
- **Full manifest:** `projects/madoev-eeg/orchestration/full_managed_sprint_v1.STAGED.json` — **eight backlog tasks**, three safe initial parallel tasks SOURCE-01 / AR-04 / DATA-06, dependencies, two reviewers and coordinator.
- **Launcher:** `projects/madoev-eeg/orchestration/launch_pilot.py` — manual, dry-run by default, **never starts full**.
- All files live in `chartjs333/nginx-qa` branch `project/madoev-eeg-lab`; **Delta and nginx-qa main are not modified**.
- These manifests have been checked against `schemas/managed-workspace-sprint-v1.schema.json`; input file SHA-256 values were verified against Git blobs.
- Local synthetic signal verification is not an NGINX QA runtime run.

## Safety boundaries
1. The source repository is PUBLIC. Do not commit real EEG/patient files, clinical records, patient identifiers, credentials, tokens, or confidential unpublished documents.
2. Not for diagnosing or treating disease. Organs-to-EEG-electrode mapping stays disabled until original montage/source evidence is independently verified. Historical treatment claims are hypotheses, not established efficacy.
3. Do not call production/live on port 8025. The qualified staging scenario used **Windows** and loopback-only **127.0.0.1:18025**. A Linux Docker runtime is **not** verified as equivalent for managed process supervision.
4. IMPORTANT: NGINX QA's managed start-from-git API currently has **no built-in application-layer caller authentication**. Never expose it directly on a public interface; use local loopback/secured gateway only.
5. JSON extra `program.status` and `requires_pilot_evidence` metadata are DOCUMENTATION, not enforced runtime gates. The operator is responsible for gating the full start.
6. No unattended overnight full launch is configured.

## Stage 1 — independent one-task pilot (operator action)
From the checked-out project branch on a correctly provisioned, separately isolated Windows staging server:

```powershell
python projects/madoev-eeg/orchestration/launch_pilot.py
# inspect DRY_RUN_NO_HTTP response
python projects/madoev-eeg/orchestration/launch_pilot.py --execute
```

The launcher sends:
1. `POST http://127.0.0.1:18025/project-manager/0001` with `git_address`, isolated context `github.com/chartjs333/nginx-qa#madoev-eeg-pilot-v1`, and project title.
2. `POST http://127.0.0.1:18025/api/v1/projects/{project_phone}/sprints/start-from-git` with `repository_id=main`, `ref=refs/heads/project/madoev-eeg-lab`, `manifest_path=projects/madoev-eeg/orchestration/pilot_managed_sprint_v1.json` and `idempotency_key=madoev-eeg-pilot-ar04-20261028-v1`.

Expected initial HTTP: **201** for a new run, **200** with `deduplicated=true` for an identical repeat. Record project phone, sprint ID, manifest commit, and initial assignment ID. Failure or unexpected response => stop.

**The operator must first verify the local staging service is the intended, approved Managed Sprint runtime and has an attached functioning executor.** A running server alone only enqueues an assignment. It does not prove an AI agent executed it.

## Pilot acceptance gate
The full sprint remains blocked until all conditions are independently demonstrated:
- Managed runtime identity = expected Git source, one AR-04 assignment, no unexpected process/job/branch.
- Agent actually executes the synthetic EEG tests, produces `docs/PILOT_AR04_EVIDENCE.md` in its assigned branch and supplies an immutable result commit and logs.
- Both independent reviewers accept the committed result; the sprint is in terminal `DONE` without rejected/unresolved rework.
- Operator independently verifies evidence/test reproducibility, no clinical claims, no private patient data, no changes to Delta or nginx-qa main.
- Reviewed pilot changes are merged into this dedicated Madoev branch (if used) **before** starting the full manifest; update/check ref and pinning accordingly.

Do not mistake a local synthetic test for proof that the Managed Sprint runtime has finished the pilot.

## Stage 2 — staged eight-task graph (NOT enabled)
The file `full_managed_sprint_v1.STAGED.json` is available but **not started**. It contains the eight original backlog tasks. In a later approved, separate project context, create a unique full-run idempotency key and call the same `start-from-git` endpoint with the **full** manifest path. Do not reuse the pilot context or key.

The first parallel workers would be: (1) historical source inventory, (2) synthetic AR validation, (3) de-identified data schema. Other tasks activate only through graph dependencies and two-reviewer transitions. If the original montage cannot be verified, organ-specific mapping remains blocked. Ethical approval is required before any human study or intervention.

## Migration
When `chartjs333/madoev-eeg-lab` PRIVATE repository is available, transfer the project branch with Git history, update `git_address`, `git.source_ref`, checksums/source pinning, agent branch policy, and API project context before first launch. Do not copy state databases or credentials. Use a separate instance and storage root, not Delta's runtime.

## First deliverable done locally (not via NGINX QA)
Synthetic AR(2) signal produced BIC-selected order 2; 9 synthetic windows accepted; B1/B2/P1 comparison worked; atlas guard refused unverified organ mapping. No patient data were used. This does not count as Managed Sprint pilot approval.
