# Setup and Diagnostics Wizard v1 development sprint

Source: accepted Telegram Launcher commit `89f7f09b318a3d9d78ceb2d8c92b6e730e644626`.
`managed-sprint.json` is the direct start-from-Git development manifest;
`telegram-envelope.json` is a validated manual-mode transport example and is
not the activation path used for this development sprint.

Graph: architecture/inventory -> diagnostic core -> worker connection/self-test
-> allowed operations/recovery -> advisory LLM -> private Web UI -> qualification
-> release. Every accepted result requires two independent Reviewers. Coordinator
handles supported repair or external gates; no second engine or alias project.

The launch uses canonical project9000 on the existing control host18035 after
the prior terminal BLOCKED_EXTERNAL and resource retirement checks. Source code
work happens only in newly issued managed workspaces, never under a running host.
Implementation and live acceptance are initially NOT_RUN. The user authorized
creation and activation of this sprint; activation is not evidence that all work
has been implemented or that a persistent external executor is configured.
