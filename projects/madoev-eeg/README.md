# Madoev EEG — isolated integration branch

This directory is the staging location for integrating the Madoev EEG research software with NGINX QA. Development is isolated in the `project/madoev-eeg-lab` branch; `main` must remain unchanged.

## Intended components

- Research-only EEG autoregressive order (AR/BIC) analysis and reproducibility tests.
- Historical atlas with unmapped/uncertain scalp positions explicitly marked; no inferred clinical electrode mapping.
- Before/after and multi-day longitudinal comparisons.
- Managed Sprint manifest, schema validation, Docker startup, and operator-triggered `start-from-git` flow.
- Quality and research ethics gates before any participant work.

## Current state

Integration branch created; **this README is a placeholder, not a running sprint**. No clinical/patient data are stored here. The research package is prepared separately and requires a privacy and technical review before publication.

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
