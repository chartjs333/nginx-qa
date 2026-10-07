# Communication Profiles v1

Purpose: versioned, project-specific communication behavior for delegated correspondence.

A profile is split into:
- profile.yaml — machine-readable policy metadata;
- role.md — who the agent represents and its responsibilities;
- communication.md — tone, language and formatting;
- authority.md — permissions, approval boundaries and escalation rules;
- domain.md — domain knowledge and terminology.

The coordinator resolves project/domain first, then selects a profile pinned to an exact Git commit.

AUTO_REPLY authority is separate from sprint activation authority. A profile may authorize correspondence without granting Play/start-from-git.

## Required profile.yaml fields

- profile_id
- version
- language
- mode: MANUAL | DRAFT_ONLY | AUTO_REPLY
- role_file
- communication_file
- authority_file
- domain_file
- allowed_channels
- escalation_triggers
- max_autonomous_replies
- expires_at (optional)

## Audit

Every autonomous reply records profile_id, profile version, exact Git commit, policy decision, thread/message identifiers and timestamp.
