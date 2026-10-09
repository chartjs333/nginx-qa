# Mutation Effectiveness Qualification

Round 2 отработал корректно, но результат указывает уже не на слабую promotion policy, а на две отдельные проблемы:

1. Train signal поврежден: 8 из 36 train-артефактов protocol-invalid.
2. Все кандидаты поведенчески эквивалентны baseline: `changedPredictions=0` для всех `v003-*`.

Поэтому Round 3 запускать рано. Сначала нужно доказать, что измененный Markdown действительно влияет на prompt и inference, а train-набор дает валидный error signal.

## 1. Закрыть train contract regression

Validation остается чистым, но autoresearch строит гипотезы по train. При `28 valid / 8 invalid` mutation agent анализирует неполную и смещенную выборку.

### Агент

`SingleSkillExtractionAgent`

### Задача

Добавить отчет:

```text
output/autoresearch/dystonia_HP_0001332-round-002/
  train-contract-diagnostics.json
  train-contract-diagnostics.md
```

Для восьми невалидных артефактов зафиксировать:

```json
{
  "pmid": "...",
  "initialValid": false,
  "semanticRepairAttempts": 2,
  "finalValid": false,
  "issues": [
    "MISSING_PATIENT_RESULT"
  ],
  "patientCount": 12,
  "evidenceFragmentCount": 438,
  "chunkCount": 3,
  "rawOutputTokens": 512,
  "truncated": true
}
```

Обязательная классификация:

```text
OUTPUT_TRUNCATION
PATIENT_CHUNK_MERGE_FAILURE
MODEL_PROTOCOL_NONCOMPLIANCE
EVIDENCE_CORPUS_TOO_LARGE
REPAIR_INEFFECTIVE
PATIENT_INDEX_DEFECT
VALIDATOR_DEFECT
```

Наиболее вероятный кандидат - truncation или chunk merge, поскольку train-статьи крупнее двух validation-статей, а лимит установлен в `512` токенов.

### Новый train gate

```text
train final contract validity = 100%
```

До этого medical-only train errors нельзя считать полными.

## 2. Проверить, что candidate skill реально попадает в prompt

Одинаковые predictions у трех разных mutations могут быть легитимны, но сначала нужно исключить wiring defect.

### Агент

`ExperimentCoordinatorAgent`

### Задача

Для baseline и каждого candidate сохранять:

```json
{
  "skillVersion": "v003-explicit-vocabulary",
  "skillContentHash": "...",
  "promptHash": "...",
  "promptSkillSectionHash": "...",
  "taskSkillPath": "...",
  "loadedRegistryVersion": "v003-explicit-vocabulary"
}
```

Hard checks:

```text
candidate skill hash != baseline skill hash
candidate prompt hash != baseline prompt hash
prompt skill section exactly matches candidate file
task points to immutable candidate version
```

Если candidate skill hash различается, а prompt hash нет:

```text
CANDIDATE_PROMPT_WIRING_DEFECT
```

Если prompt различается, но raw model responses идентичны, это уже поведение модели, а не pipeline defect.

## 3. Добавить mutation sensitivity test

Нужен специальный диагностический candidate, который не участвует в promotion.

### Агент

`ExperimentCoordinatorAgent`

### Задача

Создать искусственный probe skill, например:

```text
For diagnostic purposes, always return "yes" when the evidence
contains the exact word "dystonia"; otherwise return "-99".
```

Или еще сильнее - на одном синтетическом fixture:

```text
Always return "yes" for the supplied diagnostic patient.
```

Использовать только искусственный test fixture, не реальные validation данные.

### Цель

Проверить цепочку:

```text
candidate file
-> SkillParser
-> prompt builder
-> Ollama request
-> model response
-> validator
-> evaluation
```

Ожидается, что probe изменит хотя бы одну prediction.

Артефакт:

```text
output/autoresearch/diagnostics/mutation-sensitivity.json
```

Результат:

```json
{
  "baselinePrediction": "-99",
  "probePrediction": "yes",
  "promptChanged": true,
  "predictionChanged": true,
  "qualified": true
}
```

Если `predictionChanged=false`, Round 3 блокируется независимо от train metrics.

## 4. Измерить фактическую силу mutations

Текущий semantic diff проверяет структуру, но не показывает, насколько кандидаты отличаются от `v001`.

### Агент

`SkillMutatorAgent`

### Задача

Для каждой версии добавить:

```json
{
  "candidate": "v003-temporal-history",
  "parent": "v001",
  "charactersAdded": 214,
  "charactersRemoved": 18,
  "rulesAdded": 4,
  "rulesRemoved": 0,
  "normalizedTextSimilarity": 0.91,
  "newAcceptedTerms": [
    "previously documented dystonia"
  ],
  "newExclusionRules": [
    "differential diagnosis only"
  ],
  "mutationStrength": "LOW"
}
```

Классы:

```text
NO_EFFECT
LOW
MEDIUM
HIGH
```

`MUTATION_TOO_WEAK` должен определяться не только по метрикам, но и по semantic delta.

## 5. Сравнить raw responses baseline и candidates

`changedPredictions=0` еще не означает одинаковое поведение внутри модели.

### Агент

`ObservabilityAgent`

### Задача

Добавить сравнение:

```text
RAW_RESPONSE_IDENTICAL
RAW_RESPONSE_DIFFERENT_SAME_NORMALIZED_RESULT
DIFFERENT_EVIDENCE_SAME_VALUE
DIFFERENT_VALUE
```

Это позволит различить:

- candidate вообще не дошел до модели;
- candidate изменил reasoning/evidence, но не итог;
- candidate не затрагивает текущие примеры;
- projection или validator схлопывает разные ответы в один результат.

Особенно проверить safe output projection: он не должен случайно заменять candidate response baseline-результатом или читать неправильный result path.

## 6. Train contract: корректировка token budget

Не стоит глобально возвращать `8192`. Лучше вычислять лимит по числу пациентов.

### Агент

`SingleSkillExtractionAgent`

### Совместно

`WorkerRuntimeAgent`

### Задача

Например:

```text
baseTokens = 256
tokensPerPatient = 64
maxOutputTokens = clamp(
    baseTokens + patientCount x tokensPerPatient,
    512,
    4096
)
```

Для chunked extraction лимит должен считаться по пациентам конкретного chunk, а не всей статьи.

Также сохранять:

```text
requestedMaxOutputTokens
actualOutputTokens
finishReason
truncated
```

Если Ollama возвращает признак завершения или token counts, использовать их как источник истины.

## 7. Repair при truncation

### Агент

`SingleSkillExtractionAgent`

### Совместно

`WorkerRuntimeAgent`

### Задача

Truncated JSON не следует отправлять в обычный semantic repair без изменения условий. Для него нужен отдельный путь:

```text
OUTPUT_TRUNCATION
-> уменьшить patient chunk
-> повторить extraction chunk
```

Это infrastructure/execution adaptation, а не медицинская mutation.

## 8. Проверить validation confusion matrix semantics

Текущая validation confusion matrix:

```text
TP=0
FP=2
FN=14
TN=0
```

Она требует дополнительной проверки semantics:

- validation ожидает `10 positive + 6 negative`, но confusion matrix содержит `16` ошибок и ни одного правильного результата;
- вероятно, модель почти всегда выдает `-99`, а evaluator интерпретирует часть unknown predictions как FP/FN;
- нужно явно вывести prediction distribution.

### Агент

`BaselineEvaluationAgent`

### Задача

Добавить в review:

```json
{
  "expectedDistribution": {
    "yes": 10,
    "no": 6,
    "unknown": 14
  },
  "predictedDistribution": {
    "yes": 2,
    "no": 0,
    "unknown": 28
  }
}
```

И отдельную confusion matrix для трех классов:

| Expected / Predicted | yes | no | -99 |
| -------------------- | --: | -: | --: |
| yes                  |   0 |  0 |  10 |
| no                   |   2 |  0 |   4 |
| -99                  |   0 |  0 |  14 |

Так станет ясно, какие именно ошибки должен исправлять skill.

## Round 3 gate

Round 3 можно открыть только при выполнении:

```text
train contract validity = 100%
mutation sensitivity qualified = true
candidate prompt hashes differ from baseline
candidate files differ semantically from v001
prediction distribution проверено
train-only medical errors пересчитаны
```

## Сценарии после qualification

### Сценарий A - wiring defect найден

Исправить загрузку candidate version и повторить Round 2 с теми же `v003-*`. Новые version IDs не создавать, если inference фактически не использовал старые версии; в registry отметить предыдущий запуск как:

```text
INVALID_EXPERIMENT_CANDIDATE_NOT_APPLIED
```

### Сценарий B - pipeline корректен, но mutations действительно не влияют

Round 2 остается валидным `REJECTED_NO_BEHAVIOR_CHANGE`. Тогда Round 3 должен использовать более сильные, структурно разные mutations:

```text
v004-compact-decision-tree
v004-explicit-positive-negative-table
v004-evidence-first-classifier
```

Они должны менять не только словарь, а форму принятия решения.

## Статус Round 2

Рекомендованный итоговый experiment status:

```text
EXECUTION_COMPLETED
PROMOTION_REJECTED
DIAGNOSTIC_REVIEW_REQUIRED
```

Не помечать его как полностью завершенный autoresearch round до проверки восьми train contract failures и mutation sensitivity.

Ближайшая задача - не генерировать новые медицинские правила, а квалифицировать две цепочки:

```text
candidate skill -> prompt -> prediction
large train article -> valid extraction
```

После этого следующий mutation round будет опираться на достоверный train signal и доказанно работающий mutation mechanism.
