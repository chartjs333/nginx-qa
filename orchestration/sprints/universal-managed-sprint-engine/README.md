# nginx-qa sprint: Universal Managed Workspace & Continuous Sprint Engine

Этот каталог содержит implementation sprint для самого nginx-qa.

## Важная совместимость

Спринт запускается как текущий declared sequential graph. Новая функциональность
реализует optional `sprint_type`, поэтому уже работающие legacy sprints остаются на
старом code path.

## Source

```text
repository: https://github.com/chartjs333/nginx-qa.git
implementation base: agent/groups-cycles-graph-ui@3c42efc1701a0297d3563d7d31e66e532b072394
sprint branch: agent/universal-managed-sprint-engine-spec
```

## Live/staging separation

Текущий работающий nginx-qa не используется как development checkout и не
перезапускается. Все runtime tests идут в отдельной staging copy:

```text
D:\nginx-qa-staging\universal-managed-sprint-engine
127.0.0.1:18025
child port pool 18100-18199
```

## Files

- `requirements.md` — полное ТЗ;
- `staging-isolation.md` — защита live instance;
- `acceptance-matrix.md` — проверяемые критерии;
- `branch-map.md` — ветви и роли;
- `bootstrap-ru.md` — запуск текущим sequential nginx-qa;
- `sequential-sprint.json` — import manifest;
- `FILE-MANIFEST.json` — Git blob SHA-1 и размеры sprint-файлов; сам manifest из списка исключён.

## Promotion

Sprint заканчивается release candidate и планом promotion. Он не копирует staging
state в live, не останавливает live server и не выполняет deployment автоматически.
