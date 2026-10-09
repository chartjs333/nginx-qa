# Bootstrap — nginx-qa Universal Managed Sprint Engine

Репозиторий:

```text
https://github.com/chartjs333/nginx-qa.git
```

Sprint branch:

```text
agent/universal-managed-sprint-engine-spec
```

Manifest:

```text
orchestration/sprints/universal-managed-sprint-engine/sequential-sprint.json
```

Это обычный **declared sequential graph**, совместимый с текущим nginx-qa. Он
разрабатывает новый versioned sprint type, но сам не требует новой runtime-логики.

## Запуск

1. Импортировать `sequential-sprint.json` через текущий sequential graph import.
2. Получить identity только через общий `/api/v1/agents/whoami`.
3. Выполнять только выданный `active_task`.
4. Все рабочие ветви создавать от `origin/agent/universal-managed-sprint-engine-spec`.
5. Live nginx-qa не останавливать и не изменять.
6. Runtime tests запускать только из отдельной staging copy:
   `D:\nginx-qa-staging\universal-managed-sprint-engine`, port `18025`.
7. Outcome отправлять в current assignment endpoint с exact assignment ID, from/result
   commits и branch.
8. После accepted handoff снова получить identity через общий endpoint.
9. При вопросе вернуть `NEED_DECISION` или `STOP`; graph направит Coordinator.
10. Не выполнять promotion/deployment live instance в рамках sprint.
