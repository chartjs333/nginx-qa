# Patient Resolution v2

Baseline v2 показывает реальную картину: extraction contract исправен, но основной bottleneck - Patient Resolver, а не медицинские skill-файлы.

Из 90 ожидаемых сравнений только 8 стали eligible. 80 исключений из-за `unresolvedPatient` означают, что 88,9% потенциальной оценки пока недоступны. При такой coverage запуск mutation преждевременен: четыре FN недостаточно репрезентативны, а новые версии skill будут оптимизироваться на слишком узком подмножестве.

## Цель

```text
unresolvedPatient: 80 -> не более 9
patient resolution coverage: 11.1% -> не менее 90%
```

При этом нельзя ухудшать строгий contract или автоматически угадывать идентификаторы без доказательств.

## Распределение задач

## PatientResolverAgent

### TASK-PR-001 - Диагностировать 80 unresolved comparisons

Сформировать отчет не только по expected values, а по уникальным пациентам.

Нужно разделить:

```text
UNRESOLVED_PATIENT
|-- NO_ALIAS_FOUND
|-- ALIAS_AMBIGUOUS
|-- MANUAL_ID_INVALID
|-- ARTICLE_USES_DIFFERENT_NUMBERING
|-- FAMILY_MAPPING_MISSING
|-- COHORT_ONLY_DESCRIPTION
|-- PATIENT_INDEX_INCOMPLETE
`-- RESOLVER_NORMALIZATION_DEFECT
```

Артефакты:

```text
output/patient-resolution/pilot-v2-diagnostics.json
output/patient-resolution/pilot-v2-diagnostics.md
```

Для каждого пациента:

```json
{
  "pmid": 21981780,
  "manualFamilyId": "F1",
  "manualIndividualId": "II-3",
  "patientKey": null,
  "status": "ALIAS_AMBIGUOUS",
  "articleAliases": ["patient 3", "III-3"],
  "candidateMatches": [],
  "affectedExpectedComparisons": 5
}
```

Важно считать:

- уникальных unresolved пациентов;
- число сравнений, потерянных из-за каждого пациента;
- частоту категорий;
- статьи с наибольшей потерей coverage.

## PatientResolverAgent

### TASK-PR-002 - Ввести Alias Evidence Store

Patient mapping тоже должен быть доказуемым, как extraction evidence.

Новая модель:

```java
public record PatientAliasEvidence(
    String patientKey,
    String alias,
    List<String> evidenceIds,
    AliasType aliasType,
    ResolutionConfidence confidence
) {}
```

Типы alias:

```text
EXPLICIT_INDIVIDUAL_ID
EXPLICIT_FAMILY_ID
PATIENT_NUMBER
PEDIGREE_LABEL
PROBAND_LABEL
RELATIONSHIP_LABEL
COHORT_INDEX
GENERATED_MANUAL_ALIAS
```

Пример:

```json
{
  "patientKey": "21981780:F1:I3",
  "aliases": [
    {
      "value": "patient 3",
      "type": "PATIENT_NUMBER",
      "evidenceIds": ["E000041", "E000052"]
    },
    {
      "value": "III-3",
      "type": "PEDIGREE_LABEL",
      "evidenceIds": ["E000044"]
    }
  ]
}
```

Resolver должен сохранять, почему конкретный alias был связан с canonical key.

## PatientResolverAgent

### TASK-PR-003 - Добавить staged resolution

Использовать несколько последовательных этапов.

### Этап 1 - Exact deterministic mapping

```text
manual ID == article ID
normalized manual ID == normalized article ID
```

Примеры нормализации:

```text
II-3
II.3
II/3
II 3
-> II3
```

### Этап 2 - Explicit alias mapping

```text
Patient 3
Case 3
Subject 3
Individual 3
Proband
Index patient
```

### Этап 3 - Family-scoped mapping

Alias считается валидным только внутри соответствующей семьи.

```text
Family 2, patient II-3
```

не должен совпадать с:

```text
Family 1, patient II-3
```

### Этап 4 - Local LLM proposal

Локальная модель предлагает соответствия, но не утверждает их:

```json
{
  "manualPatient": {
    "familyId": "F1",
    "individualId": "I3"
  },
  "proposedAlias": "patient 3",
  "evidenceIds": ["E000041"],
  "reason": "The article explicitly equates patient 3 with individual III-3"
}
```

### Этап 5 - Deterministic acceptance

Proposal принимается только если:

- alias существует в evidence store;
- evidence ID валиден;
- нет второго canonical patient с тем же alias;
- family context согласован;
- конфликтов нет;
- confidence policy соблюдена.

## DeterministicValidationAgent

### TASK-PR-004 - Проверять Patient Index

Добавить отдельную валидацию индекса до extraction.

Ошибки:

```text
DUPLICATE_CANONICAL_PATIENT
DUPLICATE_ALIAS
AMBIGUOUS_ALIAS
ALIAS_WITHOUT_EVIDENCE
UNKNOWN_EVIDENCE_ID
FAMILY_SCOPE_CONFLICT
MANUAL_PATIENT_NOT_INDEXED
ARTICLE_ALIAS_NOT_NORMALIZED
```

Patient Index должен получить статус:

```text
VALID
VALID_WITH_UNRESOLVED
INVALID
```

Extraction task можно запускать при `VALID_WITH_UNRESOLVED`, но evaluator должен учитывать только resolved patients.

## DatasetImportAgent

### TASK-PR-005 - Улучшить normalizer manual identifiers

19 blockers уже классифицированы как `PATIENT_IDENTIFIER_DEFECT`. Их нужно разобрать отдельно.

Normalizer должен сохранять:

```java
public record NormalizedPatientIdentifier(
    String rawValue,
    String normalizedValue,
    PatientIdentifierType type,
    boolean valid,
    String issueCode
) {}
```

Поддержать:

```text
числовые ID
римские pedigree ID
F1 / Family 1
P1 / Patient 1
I1 / Individual 1
proband
index
NA / unknown / -99
```

Но не смешивать:

```text
I1
II-1
```

без доказанной схемы статьи.

## DataQualityAgent

### TASK-PR-006 - Отделить исправимые ID-дефекты от ручной проверки

Для 19 `PATIENT_IDENTIFIER_DEFECT` присвоить действия:

```text
AUTO_NORMALIZABLE
ARTICLE_ALIAS_REQUIRED
MANUAL_DATA_REVIEW_REQUIRED
UNUSABLE
```

Автоматически исправлять можно только `AUTO_NORMALIZABLE`.

Manual Excel по-прежнему не менять. Исправление должно существовать как runtime mapping:

```text
runtime/manual-id-overrides.json
```

Формат:

```json
{
  "pmid": 21981780,
  "rawFamilyId": "family 1",
  "rawIndividualId": "patient 03",
  "normalizedFamilyId": "F1",
  "normalizedIndividualId": "P3",
  "source": "AUTO_NORMALIZATION"
}
```

## BaselineEvaluationAgent

### TASK-PR-007 - Добавить patient-resolution coverage metrics

Baseline report должен содержать:

```json
{
  "patientResolution": {
    "manualPatients": 0,
    "resolvedPatients": 0,
    "unresolvedPatients": 0,
    "coverage": 0.0,
    "comparisonsLost": 80,
    "byReason": {}
  }
}
```

Также нужны:

```text
coverage по PMID
coverage по skill
coverage по patient
```

Это позволит понять, действительно ли проблема общая или сосредоточена в одной статье.

## После исправления Patient Resolver

Повторить:

```bash
baseline-v2 \
  --config data/config.properties \
  --run-id pilot-contract-v2 \
  --patient-index-version v2
```

Новый gate:

```text
finalValidArtifacts = 10/10
protocolInvalidArtifacts = 0
patientResolutionCoverage >= 0.90
unresolvedPatient comparisons <= 9
eligibleComparisons >= 70
```

Если manual содержит реальные неоднозначности, допустим меньший coverage, но каждое исключение должно быть объяснено.

## Что делать с четырьмя FN сейчас

Сохранить их как предварительные медицинские ошибки, но не использовать для mutation.

Причины:

- выборка состоит только из восьми eligible сравнений;
- отсутствуют TN и FP;
- F1=0 отражает слишком маленький usable subset;
- изменение skill по четырем FN почти наверняка приведет к переобучению.

Статус:

```text
MEDICAL_ERROR_CANDIDATE
```

После Patient Resolution v2 evaluator должен проверить их снова. Некоторые FN могут исчезнуть или изменить patient attribution.

## Параллельная волна задач

Можно выполнять одновременно:

```text
PatientResolverAgent
  TASK-PR-001
  TASK-PR-002
  TASK-PR-003

DatasetImportAgent
  TASK-PR-005

DataQualityAgent
  TASK-PR-006

BaselineEvaluationAgent
  TASK-PR-007
```

После этого:

```text
DeterministicValidationAgent
  TASK-PR-004
```

Затем полный повторный pilot и Baseline v2.

## Gate перед первым autoresearch round

Mutation разрешается, когда для выбранного skill выполняется:

```text
contract validity = 100%
patient resolution coverage >= 95%
eligible positives >= 5
eligible negatives >= 5
data-quality exclusions < 20%
однозначная evaluation strategy
```

Если после Patient Resolution v2 две статьи все еще не дают нужного числа примеров, следующий шаг - расширить pilot PDF scope из 2 статей до большего поднабора из 51 PMID. Сначала можно выбрать 5-10 статей с максимальным числом размеченных пациентов и минимальным количеством data-quality blockers.

Сейчас приоритет однозначный: не улучшение Markdown-скилов, а восстановление patient-level evaluation coverage. Технический extraction pipeline уже готов к autoresearch; не готова пока идентификация пациентов, необходимая для достоверной метрики.
