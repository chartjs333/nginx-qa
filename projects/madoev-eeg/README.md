# Madoev EEG — isolated integration branch

This directory is the staging location for integrating the Madoev EEG research software with NGINX QA. Development is isolated in the `project/madoev-eeg-lab` branch; `main` must remain unchanged.

## Intended components

- Research-only EEG autoregressive order (AR/BIC) analysis and reproducibility tests.
- Historical atlas with unmapped/uncertain scalp positions explicitly marked; no inferred clinical electrode mapping.
- Before/after and multi-day longitudinal comparisons.
- Managed Sprint manifest, schema validation, Docker startup, and operator-triggered `start-from-git` flow.
- Quality and research ethics gates before any participant work.

## Current state

**Two Managed Sprint manifests are now staged:** an isolated AR-04 single-task pilot and a separate eight-task full research graph. The pilot has NOT been started in NGINX QA. A loopback-only pilot launcher and a release-gated runbook are available. No clinical/patient data are stored here. See [MANAGED_SPRINT_RUNBOOK_RU.md](docs/MANAGED_SPRINT_RUNBOOK_RU.md), [pilot JSON](orchestration/pilot_managed_sprint_v1.json), [staged full JSON](orchestration/full_managed_sprint_v1.STAGED.json), and [dry-run-first launcher](orchestration/launch_pilot.py).

## Migration to a separate repository

When a separate **private** `chartjs333/madoev-eeg-lab` repository exists, transfer the branch and retain Git history:

```bash
git clone https://github.com/chartjs333/nginx-qa.git
cd nginx-qa
git fetch origin project/madoev-eeg-lab
git switch project/madoev-eeg-lab
git remote add madoev https://github.com/chartjs333/madoev-eeg-lab.git
git push madoev project/madoev-eeg-lab:main
```

The new repository will contain the NGINX QA source, this branch's committed files, and inherited history. Review and update repository URLs, manifests, checksum pins, deployment secrets, and automation before starting an independent instance.

**Security:** The current `nginx-qa` repository is public. Never commit patient records, consent documents, API tokens, credentials, or unpublished confidential research data to this branch. Do not execute interventions based on this research prototype.
