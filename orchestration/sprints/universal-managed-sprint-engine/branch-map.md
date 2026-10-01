# Branch map — Universal Managed Sprint Engine

Все ветви создаются от `agent/universal-managed-sprint-engine-spec`.

```text
spec
 └─ agent/umse-01-architecture-contract
    └─ agent/umse-02-workspace-core
       └─ agent/umse-03-sprint-type-dispatch
          └─ agent/umse-04-transactional-import
             └─ agent/umse-05-concurrent-runtime
                └─ agent/umse-06-continuity-runtime
                   └─ agent/umse-07-staging-qualification
                      └─ agent/umse-08-legacy-regression
                         └─ agent/umse-09-release-qualification
```

Persistent reviewers:

- `review/umse-architecture`
- `review/umse-evidence`

Coordinator:

- `agent/umse-continuity-coordinator`

Каждый переход проходит двух reviewers. `NEED_DECISION`, `STOP`, `NO_GO` и
orchestration errors направляются Coordinator, который возвращает graph в нужный node
или завершает `BLOCKED_EXTERNAL`.

Ни одна ветвь не изменяет live nginx-qa directory/runtime. Runtime tests выполняются
только в staging copy на отдельном порте.
