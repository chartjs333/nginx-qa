# Baseline Evaluation v2

Contract Pilot v2 прошел максимально чисто: системный протокол больше не является источником ошибок. Теперь можно переходить к Baseline Evaluation v2, и только после него - к mutation.

Следующий спринт: Baseline Evaluation v2

Главная цель - разложить все расхождения на четыре независимые категории:

1. Medical extraction errors
2. Patient-resolution exclusions
3. Data-quality exclusions
4. Skill/manual mapping exclusions

В итоговые TP/FP/FN/TN должны попадать только записи, для которых:

```text
finalValid = true
patient resolved
manual value usable
skill mapping unambiguous
field evaluation strategy defined
```

## Распределение задач

## BaselineEvaluationAgent

### TASK-BL-001 - Реализовать eligibility pipeline

Для каждого expected value определить статус:

```java
public enum EvaluationEligibility {
    ELIGIBLE,
    EXCLUDED_PROTOCOL_INVALID,
    EXCLUDED_UNRESOLVED_PATIENT,
    EXCLUDED_DATA_QUALITY,
    EXCLUDED_SKILL_MAPPING,
    EXCLUDED_UNSUPPORTED_VALUE,
    EXCLUDED_MISSING_RESULT
}
```

Важно: при текущем pilot `EXCLUDED_PROTOCOL_INVALID` должен быть равен нулю.

Результат по каждой записи:

```json
{
  "pmid": 23278385,
  "patientKey": "23278385:F1:I2",
  "skill": "ataxia_HP_0001251",
  "expected": "yes",
  "predicted": "no",
  "eligibility": "ELIGIBLE",
  "classification": "FALSE_NEGATIVE"
}
```

## DataQualityAgent

### TASK-BL-002 - Преобразовать blocker report в machine-readable exclusion index

Из 282 blockers создать индекс, который evaluator сможет применять напрямую:

```text
runtime/evaluation-exclusions.json
```

Формат:

```json
{
  "entries": [
    {
      "pmid": 21981780,
      "patientKey": "21981780:F1:I3",
      "skill": "some_skill",
      "reason": "PATIENT_IDENTIFIER_DEFECT",
      "action": "REVIEW_MANUAL_DATA"
    }
  ]
}
```

Нельзя исключать весь PMID из-за одной проблемной строки. Исключение должно быть максимально точечным:

```text
PMID x patient x skill
```

## BaselineEvaluationAgent

### TASK-BL-003 - Пересчитать baseline только по eligible comparisons

Сформировать:

```text
output/evaluations/pilot-baseline-v2.json
output/evaluations/pilot-baseline-v2.md
output/evaluations/pilot-baseline-v2-errors.json
```

Обязательные секции:

```json
{
  "contract": {
    "finalValidArtifacts": 10,
    "protocolInvalidArtifacts": 0
  },
  "coverage": {
    "totalExpectedComparisons": 90,
    "eligibleComparisons": 0,
    "excludedComparisons": 0
  },
  "exclusions": {
    "dataQuality": 0,
    "skillMapping": 0,
    "unresolvedPatient": 0,
    "unsupportedValue": 0,
    "missingResult": 0
  },
  "metrics": {
    "tp": 0,
    "fp": 0,
    "fn": 0,
    "tn": 0,
    "precision": 0.0,
    "recall": 0.0,
    "f1": 0.0
  }
}
```

## Правила подсчета

### Missing result

Если artifact валиден, но для allowed patient отсутствует значение, это уже невозможно по новому контракту. Если такое все же случится, это:

```text
EXCLUDED_MISSING_RESULT
```

а не автоматически FN.

### -99

Нужно явно определить семантику:

```text
expected=-99, predicted=-99 -> match
expected=yes, predicted=-99 -> FN
expected=no, predicted=-99 -> FN или отдельный unknown error
expected=-99, predicted=yes/no -> FP-like over-extraction
```

Рекомендация: считать `-99` отдельным третьим классом для полной отчетности, а бинарные precision/recall считать только на yes/no, с отдельной метрикой:

```text
unknown accuracy
```

### Поля разных типов

Пять pilot skills могут иметь разные evaluation strategies. Перед общей агрегацией каждому нужен тип:

```text
BINARY
CATEGORICAL
NUMERIC
FREE_TEXT
ORDINAL
```

Не следует смешивать treatment-response и бинарный symptom field в один F1 без macro-разделения.

## Gate перед mutation

Mutation разрешается только если Baseline v2 показывает:

```text
protocolInvalid = 0
unresolvedPatient rate приемлем
eligible comparisons достаточно
skill mapping однозначен
manual values поддерживаются normalizer
```

Для первого skill-кандидата нужны минимальные требования:

```text
eligible positive examples >= 5
eligible negative examples >= 5
protocol validity = 100%
data-quality exclusion rate < 20%
patient coverage >= 95%
evaluation strategy = BINARY или CATEGORICAL
```

С двумя pilot-статьями это может не выполниться. Тогда первый mutation лучше запускать на более широком train-наборе из 51 PMID, а две статьи оставить как smoke/contract set.

## Первый Autoresearch Round

После Baseline v2:

## BaselineEvaluationAgent

### TASK-AR-001 - Построить medical-only error report

В отчет не должны попадать:

```text
protocol errors;
mapping conflicts;
manual defects;
unresolved patients;
unsupported values.
```

Только реальные:

```text
FALSE_POSITIVE
FALSE_NEGATIVE
WRONG_CATEGORY
```

## HypothesisGeneratorAgent

### TASK-AR-002 - Создать три независимые гипотезы

Для первого выбранного скила:

```text
Candidate A - recall-focused
Candidate B - precision-focused
Candidate C - simplification-focused
```

## SkillMutatorAgent

### TASK-AR-003 - Создать три candidate versions

Каждая версия меняет только один `.md` и одну гипотезу.

## EvolutionManagerAgent

### TASK-AR-004 - Оценить candidates

Схема:

```text
active + 3 candidates
x validation articles
-> parallel extraction
-> eligibility filtering
-> metrics
```

## EvolutionManagerAgent

### TASK-AR-005 - Выбрать победителя

Promotion только при:

```text
contract validity = 100%
eligible coverage не хуже active
F1 вырос минимум на delta
precision/recall guardrails соблюдены
нет новых data-quality exclusions
```

## Что делать с 282 blockers

Их не надо пытаться исправить до первого эксперимента целиком.

Разделите:

```text
58 FIX_SKILL_MAPPING
224 REVIEW_MANUAL_DATA
```

Сначала исправлять `FIX_SKILL_MAPPING`, потому что это инженерные ошибки, которые напрямую уменьшают usable evaluation set.

`REVIEW_MANUAL_DATA` следует:

```text
сохранять как exclusions;
не менять автоматически;
постепенно разбирать по приоритетным скилам.
```

Приоритет:

1. blockers для выбранных pilot skills
2. blockers для первого autoresearch skill
3. остальные по частоте

## Рекомендуемый порядок

1. TASK-BL-001 eligibility pipeline
2. TASK-BL-002 exclusion index
3. TASK-BL-003 baseline v2
4. выбрать первый skill по eligible coverage
5. TASK-AR-001 medical-only errors
6. TASK-AR-002/003 candidates
7. TASK-AR-004 evaluation
8. TASK-AR-005 promotion

Системная часть теперь готова. Следующий важный результат - не просто новый F1, а отчет, где каждый FN доказанно является медицинской ошибкой модели/скила, а не дефектом данных или сопоставления.
