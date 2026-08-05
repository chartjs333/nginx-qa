# Autoresearch Round 1

Patient Resolution v2 прошел gate полностью:

- `10/10` артефактов валидны;
- protocol errors отсутствуют;
- `eligibleComparisons = 72`;
- patient resolution coverage = `100%`;
- unresolved comparisons = `0`;
- тесты: `75 passed`.

Теперь можно впервые переходить к реальному autoresearch по Markdown-скилам. Основные инфраструктурные и идентификационные ошибки больше не искажают метрику.

## Gate считается пройденным

```text
contract validity = 100%
patient resolution coverage = 100%
protocol invalid = 0
eligible comparisons = 72
```

Остается проверить распределение eligible positive/negative примеров по каждому из пяти pilot skills. Первый скил нужно выбирать не по общему F1, а по пригодности для контролируемого эксперимента.

## TASK-AR1-001 - Выбрать первый skill

### Агент

`BaselineEvaluationAgent`

### Задача

Построить ranking pilot-скилов.

Для каждого поля вывести:

```json
{
  "skill": "ataxia_HP_0001251",
  "evaluationType": "BINARY",
  "eligibleComparisons": 16,
  "positiveExpected": 7,
  "negativeExpected": 9,
  "unknownExpected": 0,
  "tp": 1,
  "fp": 2,
  "fn": 6,
  "tn": 7,
  "precision": 0.333333,
  "recall": 0.142857,
  "f1": 0.2,
  "dataQualityExclusions": 0,
  "autoresearchEligible": true
}
```

### Требования к первому skill

```text
evaluationType = BINARY или CATEGORICAL
eligibleComparisons >= 10
positiveExpected >= 5
negativeExpected >= 5
contractValidity = 100%
patientCoverage = 100%
dataQualityExclusionRate < 20%
```

### Артефакты

```text
output/autoresearch/pilot/skill-ranking.json
output/autoresearch/pilot/skill-ranking.md
```

Если ни один из пяти скилов не проходит gate, pilot scope нужно расширить до дополнительных PDF.

## TASK-AR1-002 - Зафиксировать baseline выбранного скила

### Агент

`ExperimentCoordinatorAgent`

### Задача

Создать immutable baseline experiment.

```text
experimentId = <skill>-round-001
activeVersion = v001
```

Сохранить:

```text
skills/autoresearch/c19orf12/<skill>/versions/v001.md
skills/autoresearch/c19orf12/<skill>/registry.json
output/autoresearch/<experiment-id>/baseline.json
output/autoresearch/<experiment-id>/baseline-errors.json
```

`v001.md` должен быть точной копией текущего active skill-файла.

### Registry

```json
{
  "skill": "ataxia_HP_0001251",
  "activeVersion": "v001",
  "status": "BASELINE_READY",
  "versions": [
    {
      "version": "v001",
      "status": "ACTIVE",
      "parent": null,
      "contentHash": "...",
      "metrics": {
        "precision": 0.0,
        "recall": 0.0,
        "f1": 0.0
      }
    }
  ]
}
```

Исходный active-файл нельзя изменять.

## TASK-AR1-003 - Создать medical-only error report

### Агент

`ErrorAnalysisAgent`

### Задача

Использовать только eligible medical errors выбранного skill.

Исключить:

```text
protocol errors
patient-resolution errors
data-quality exclusions
skill mapping errors
unsupported manual values
```

Категории анализа:

```text
MISSED_EXPLICIT_MENTION
MISSED_SYNONYM
MISSED_INDIRECT_DESCRIPTION
NEGATION_ERROR
RELATIVE_ATTRIBUTION
WRONG_PATIENT_ATTRIBUTION
COHORT_ATTRIBUTION
OVERGENERALIZATION
TEMPORAL_MISINTERPRETATION
UNKNOWN_WHEN_POSITIVE
POSITIVE_WHEN_UNKNOWN
```

### Формат

```json
{
  "skill": "ataxia_HP_0001251",
  "activeVersion": "v001",
  "metrics": {
    "tp": 1,
    "fp": 2,
    "fn": 6,
    "tn": 7
  },
  "errorCategories": {
    "MISSED_INDIRECT_DESCRIPTION": 4,
    "OVERGENERALIZATION": 2,
    "RELATIVE_ATTRIBUTION": 1
  },
  "examples": [
    {
      "pmid": 23278385,
      "patientKey": "23278385:F1:I2",
      "expected": "yes",
      "predicted": "-99",
      "classification": "FALSE_NEGATIVE",
      "evidenceIds": ["E000124"],
      "category": "MISSED_INDIRECT_DESCRIPTION"
    }
  ]
}
```

### Артефакты

```text
output/autoresearch/<experiment-id>/medical-errors.json
output/autoresearch/<experiment-id>/medical-errors.md
```

## TASK-AR1-004 - Сгенерировать три независимые гипотезы

### Агент

`HypothesisGeneratorAgent`

### Модель

Только локальная Ollama.

### Вход

- текущий `v001.md`;
- medical-only errors;
- baseline metrics;
- история экспериментов;
- ограничения `program.md`.

### Кандидаты

#### Candidate A - Recall

Фокус:

```text
MISSED_SYNONYM
MISSED_INDIRECT_DESCRIPTION
UNKNOWN_WHEN_POSITIVE
```

#### Candidate B - Precision

Фокус:

```text
OVERGENERALIZATION
RELATIVE_ATTRIBUTION
COHORT_ATTRIBUTION
```

#### Candidate C - Minimal balanced rewrite

Фокус:

- убрать неоднозначность;
- сократить правила;
- явно разделить positive, negative и unknown;
- не добавлять много новых терминов.

### Формат

```json
[
  {
    "hypothesisId": "H001-recall",
    "role": "RECALL",
    "description": "Добавить распознавание косвенных клинических описаний",
    "targetErrors": [
      "MISSED_INDIRECT_DESCRIPTION"
    ],
    "expectedEffect": {
      "recall": "increase",
      "precision": "may decrease"
    }
  }
]
```

## TASK-AR1-005 - Создать candidate skill versions

### Агент

`SkillMutatorAgent`

### Версии

```text
v002-recall.md
v002-precision.md
v002-balanced.md
```

Каждая версия должна:

- сохранять `Field Name`;
- сохранять `Skill Type`;
- реализовывать одну гипотезу;
- не содержать PMID;
- не содержать patient IDs;
- не содержать конкретных ожидаемых значений;
- не изменять другие skill-файлы;
- успешно проходить `SkillParser`.

### Дополнительная защита

Для каждого candidate сохранять semantic diff:

```json
{
  "version": "v002-recall",
  "parent": "v001",
  "hypothesisId": "H001-recall",
  "changes": [
    {
      "section": "Extraction Logic",
      "changeType": "EXPANDED",
      "summary": "Добавлены косвенные описания клинического признака"
    }
  ]
}
```

## TASK-AR1-006 - Оценить кандидатов

### Агент

`ExperimentCoordinatorAgent`

### Задача

Создать параллельные задачи:

```text
3 candidates x validation articles
```

Использовать тот же:

- PDF cache;
- Patient Index v2;
- evidence contract;
- local-only Ollama;
- `max-output-tokens=512`;
- final contract validator.

Для fairness все версии должны использовать:

```text
одну модель
одинаковую temperature
одинаковый patient index
одинаковый evidence corpus
одинаковый prompt version
одинаковый validation split
```

### Результаты

```text
output/autoresearch/<experiment-id>/candidates/v002-recall/evaluation.json
output/autoresearch/<experiment-id>/candidates/v002-precision/evaluation.json
output/autoresearch/<experiment-id>/candidates/v002-balanced/evaluation.json
```

## TASK-AR1-007 - Promotion decision

### Агент

`PromotionPolicyAgent`

Решение полностью детерминированное.

## Hard guardrails

```text
contract validity = 100%
patient coverage = 100%
eligible comparisons >= baseline eligible comparisons
data-quality exclusions не увеличились
protocol errors = 0
```

## Metric guardrails

Для маленького pilot-набора разумно использовать:

```properties
autoresearch.minimum-delta=0.02
autoresearch.min-precision=0.25
autoresearch.min-recall=0.10
```

Но candidate должен улучшить baseline хотя бы по одному из вариантов:

```text
F1 вырос минимум на 0.02
или
FN уменьшился без увеличения FP
или
FP уменьшился без увеличения FN
```

При равном F1 выбирать:

1. выше recall;
2. затем выше precision;
3. затем более короткий skill;
4. затем candidate с меньшим diff.

### Возможные решения

```text
PROMOTED
REJECTED_NO_IMPROVEMENT
REJECTED_GUARDRAIL
REJECTED_CONTRACT_REGRESSION
```

## TASK-AR1-008 - Записать experiment history

### Агент

`ExperimentCoordinatorAgent`

### `experiments.tsv`

```text
timestamp
experiment_id
skill
parent_version
candidate_version
hypothesis
tp
fp
fn
tn
precision
recall
f1
eligible_comparisons
contract_validity
decision
```

Пример:

```text
2026-07-21T12:00:00
ataxia-round-001
ataxia_HP_0001251
v001
v002-recall
Add indirect clinical descriptions
3
3
4
6
0.5000
0.4286
0.4615
16
1.0000
PROMOTED
```

## TASK-AR1-009 - Stop or continue

### Агент

`StopPolicyAgent`

После первого раунда:

```text
target reached -> SATISFACTORY
candidate promoted -> start round 2
нет улучшения -> еще один раунд с новыми гипотезами
три раунда без улучшения -> NO_IMPROVEMENT
```

Для pilot:

```properties
autoresearch.max-rounds=5
autoresearch.max-no-improvement-rounds=2
autoresearch.target-f1=0.80
```

`target-f1=0.90` для двух статей может быть нестабильным и слишком зависимым от единичных примеров.

## Важное ограничение малого pilot

При `72` eligible comparisons общая оценка уже полезна, но per-skill выборка может быть маленькой. Поэтому результаты первого autoresearch round следует считать engineering validation, а не доказательством обобщаемости.

Нельзя:

- использовать обе статьи одновременно как train и validation;
- показывать improver подробные validation errors после каждого раунда;
- выбирать версию по test;
- запускать неограниченное число mutation-итераций на тех же двух статьях.

Для двух статей оптимальный режим:

```text
статья A -> train error analysis
статья B -> validation promotion
```

Затем можно поменять роли только для диагностического cross-check, но не выбирать версию по сумме обоих направлений без отдельного test-набора.

## Рекомендуемая последовательность сейчас

1. Сформировать skill-ranking.
2. Выбрать один бинарный skill.
3. Зафиксировать v001 baseline.
4. Создать medical-only error report.
5. Сгенерировать три hypotheses.
6. Создать три candidate skills.
7. Запустить parallel validation.
8. Выполнить deterministic promotion.
9. Записать experiments.tsv.
10. Остановиться после первого раунда и вручную проверить diff.

На первом autoresearch round не нужно включать полностью автоматическое продолжение. Сначала нужно убедиться, что локальная модель улучшает общие медицинские правила, а не запоминает формулировки двух pilot-статей.
