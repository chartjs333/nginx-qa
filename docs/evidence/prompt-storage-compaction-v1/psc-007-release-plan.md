# PSC-007 exact-head operator package and release plan

Recorded: 2026-10-23

Sprint: `msv1-99559169b6513e1d081b2a7f9522cdb8a08babf09042ae878a44420b9d378ad5`

## Decision and authorization boundary

This package is a plan and an evidence index. It does not authorize or perform
a merge, deploy, service restart, live migration, authoritative-mode rollout,
garbage collection, deletion, or access to a real prompt root.

The only presently supportable rollout recommendation is a separately
authorized, fresh, isolated **shadow canary** using generated fixtures. The
candidate remains disabled by default (`legacy` mode). Authoritative mode,
production use, live migration, GC, and deletion remain **NOT AUTHORIZED**.

The accepted qualification decision is `BASELINE_PLUS_DIFF_PASS`. It is not a
full rerun of the current selector on the exact candidate head:

- historical aggregate baseline: 649 selected tests / 882 subtests, 0 failures,
  0 errors, 0 skips, but without a durable exact-ID/per-run receipt ledger;
- fresh change-scoped verification: 95 unique tests, 0 failures, 0 errors,
  0 skips;
- current reproducible selector inventory: 956 tests total (910 PSC-A15 plus
  46 qualification-tooling tests), `NOT_RUN_NOT_CLAIMED` as a full suite;
- fresh delta benchmark: four transitions, all 13 PSC-A05, 8 PSC-A16, and
  12 PSC-A17 checks true.

No statement in this package may be read as a full exact-head 956-test PASS.

## Exact provenance

| Role | Commit | Tree | Meaning |
| --- | --- | --- | --- |
| Upstream comparison base | `ff636bfb1bc27ae5a2f7aa2b7a0c1a5f1dde2e60` | `0e72f34228b49b33be0975fe06653acd3d0dc0d5` | `origin/main` at package preparation time |
| Historical runtime baseline | `54d9eb716a88a195e3b9ade19ed7a480ffee9ccc` | `c8d171a65558884aac3e3fc283c48c038185346d` | source version of the saved aggregate baseline only |
| Historical evidence carrier | `725c9f3ed2699cda3c8c2e65fee8431fa70d19b5` | `4917b9609f73c1429a075745af85cff06c44ae3e` | reworked historical evidence; not the current candidate |
| Exact tested runtime | `e862224c3e0d757d2cb799cc849503e837a01b00` | `f2550c899256fbbc8b6836f4dd3903eba8701508` | runtime/tooling content exercised by the fresh checks and delta benchmark |
| Rejected evidence carrier | `a962e2d3697335b2f1c8e11cd4e66da3ffe005ab` | `b19e754aca1c5c199e6adcadc411bbcb80fe5ce6` | evidence-only child with two wrong test identifiers/hashes; superseded |
| Accepted corrected evidence head | `6dec4c35c236ef62dd890ff65ba6c33b93a53c27` | `aadb60d0fa2d52b2c15e84145499d428e19eacaf` | PSC-006 source accepted 2/2 and source for PSC-007 |

`6dec4c35` is a direct child of `a962e2d3`, which is a direct child of
`e862224c`. The correction changes one evidence JSON file only. The release-plan
carrier is the documentation-only direct child that first adds this file; its
commit and tree are bound by the managed PSC-007 result because a Git commit
cannot contain its own object name.

At preparation time the accepted evidence head is 31 commits ahead and zero
commits behind the comparison base. The range is linear and contains no merge
commit. The ordered commit-list SHA-256 (one full commit per line, trailing
newline) is:
`8cf3c3f46f58c4eed11837364e302a0925403d1b2033f7e784e6a45f5628cf31`.

### Preserved commit chain

| Commit | Subject |
| --- | --- |
| `00dc4913888f5e09ab51386dc77a7c9aa9002796` | orchestration: add prompt storage compaction sprint |
| `14b7d1bcf2bc39f738a2a6c0c8eff4827de2ba2c` | orchestration: pin managed schema blob checksum |
| `3730a1502fc6059eb526d5e3434d01205c165730` | orchestration: preserve compactor prototype reference |
| `eceefae323d0813ca3822051fe9ac5c709dc30c6` | feat: define prompt archive storage v1 contract |
| `eae691d2529245a21f029239c70fdfd483659acd` | feat: add crash-safe prompt archive CAS core |
| `0ecc12abaa9e5dcf77f736f8f84e222282012b85` | feat: add incremental prompt archive |
| `1226560a06a8a325b979eae89e71199ad3a3f005` | feat: integrate prompt archive runtime storage |
| `7b9cf2607796a86c73796463f84c21544579cc7f` | feat: add prompt archive offline migration recovery |
| `1e1beb106fef2ec0269084be825929b7979e27ad` | fix: recover stranded migration journal temps |
| `77c5a4ce69165298d9d5a6d2b5ec16b43285dd9a` | feat: integrate authoritative prompt archive runtime |
| `40ffae805822d4fd2e62acd76abb63f4cef8a7ca` | fix: harden prompt archive migration recovery |
| `2e0647fb4a7f764bd65a560d699ec78b3a458e2e` | fix: reject ADS on migration publication temps |
| `6f3ad6af2f2b46dcb262afcb8094a0f0cda5a153` | feat: bind source metadata to prompt archives |
| `a205a358dd11470c899fb8d1f044181337d1880f` | feat: harden prompt archive migration recovery |
| `8302c327b8c8853f1deb26c3e2dc5864af9223c4` | feat: journal legacy latest runtime publication |
| `54d9eb716a88a195e3b9ade19ed7a480ffee9ccc` | fix: close prompt archive reachability gaps |
| `71c348c935b029eb1224121b62a37bf8d1cb0000` | docs: record PSC qualification benchmark adjudication |
| `725c9f3ed2699cda3c8c2e65fee8431fa70d19b5` | docs: preserve raw PSC benchmark evidence |
| `7c6389ab5868c9b409db98d107fd16c3b06aadb6` | fix: honor manifest-zero archive depth |
| `eb4ed7354e1a7051607fd815e939302b4cffe584` | test: add exact-head PSC benchmark |
| `c976334dd1dfa0bb4bfd872277dd1f1ffd0d0b9a` | test: add exact-head PSC qualification ledger |
| `e3540dfd1486aef5f5332404d2937d2c4abe6cd9` | fix: harden PSC qualification evidence |
| `7a9af600c41056c8cc10a6aad271be04172ef1fe` | fix: allow managed workspace under service state |
| `9ff42b3419c86ec4381a583af2f3dc57ac517e83` | fix: close PSC qualification path guard gaps |
| `9d4f8508696cf421f0394646bef54f291c943f86` | fix: preserve full benchmark isolation contract |
| `200bc9286424035ef9da3fec0e25c491683dd6d9` | test: harden PSC delta qualification evidence |
| `3dbf2d5e2690113a3ffbb0981cf436f85c828168` | test: close PSC Git integrity gaps |
| `cc48b56ad229c8be4e7794488d07423dd8456137` | fix: verify locked Git config after guard install |
| `e862224c3e0d757d2cb799cc849503e837a01b00` | fix: allow guarded subprocess pipe wrapping |
| `a962e2d3697335b2f1c8e11cd4e66da3ffe005ab` | docs: record PSC baseline plus diff qualification |
| `6dec4c35c236ef62dd890ff65ba6c33b93a53c27` | docs: correct PSC qualification test identifiers |

## Managed results and approvals

Every gate in this table has the two required applicable approvals: first the
runtime/storage reviewer (phone 2978), then the evidence/security reviewer
(phone 2979). FAIL/RESUME rows are accepted continuity decisions, not product
PASS claims.

| Node | Assignment | Outcome | Result commit | Result key | Review IDs (2978 / 2979) |
| --- | --- | --- | --- | --- | --- |
| `storage-contract` | `assignment-a192337cba6901d63f0f10b5` | `DONE` | `eceefae323d0813ca3822051fe9ac5c709dc30c6` | `result-e295b5be145d1d67c7c853d50e5d3bd7685efa1cf775c1b56c101eef10b15b08` | `review-3680062c6e45a0c504c18cc4` / `review-6ef3d6db8e04e990e7ae2307` |
| `cas-core` | `assignment-325b8cfadb69ef4439f9e7c6` | `DONE` | `eae691d2529245a21f029239c70fdfd483659acd` | `result-c0e6ef389fce8d38a249ec4ed9969794ec7911ea6b71aa2fd3aab310e466eaab` | `review-c2fd79d651590f0d45c87a92` / `review-1f6280cd0ec8ef9bbf9bab16` |
| `incremental-archive` | `assignment-f76e47b49ff2e35d74514226` | `DONE` | `0ecc12abaa9e5dcf77f736f8f84e222282012b85` | `result-87e3b0fcc180bf74070f8052ef20513ac595687e748f1e8ec27d17742ae2bdd2` | `review-874253211520ce693ea4dd68` / `review-d4eba811b6d72599de4999ba` |
| `runtime-integration` | `assignment-800e3a08e4cd32f19e041cda` | `DONE` | `1226560a06a8a325b979eae89e71199ad3a3f005` | `result-dc4fbf438287ecc5572163e375b891f00609f0484520a5133295d62cad3f53ac` | `review-1e8f1edd7a59f8e45ebd582b` / `review-0e56835b1b0d80496eafe203` |
| `migration-recovery` | `assignment-c0623aa84916c53b2dadb309` | `DONE` | `1e1beb106fef2ec0269084be825929b7979e27ad` | `result-3d8871c7ee9677b8912fc514f41fe6f2ed069c05c9065082af587bfc8d1e64d8` | `review-48398280157cbc58e95ecd88` / `review-71e24b8eaf73ed20481f8428` |
| `qualification` | `assignment-2e30ca8a94895fad8f738450` | `FAIL` | `1e1beb106fef2ec0269084be825929b7979e27ad` | `result-536186def26c51e31ffab5ef88f962b938377f90c859d826684e8e1aa81cd9fe` | `review-fcee87a3ffc028f3d9eafc7c` / `review-fdaf427fe47f32cad60dc85c` |
| `psc-continuity-coordinator` | `assignment-e130cb60c6da437e78e6a05d` | `RESUME_RUNTIME` | `1e1beb106fef2ec0269084be825929b7979e27ad` | `result-d3b7056db1c0fed344ddf65524083d7006445c478cacf897c0f1b296d6462f18` | `review-e851118c654bfa2fc3c7a598` / `review-e394cacde8a2a074f466223b` |
| `runtime-integration` | `assignment-0780745bbb4c26a42db3e6bb` | `DONE` | `77c5a4ce69165298d9d5a6d2b5ec16b43285dd9a` | `result-2d31acf0b1cdbbe2b2ccafd3afa483c80e56682d6f5afd1bacfa48c3015ae8da` | `review-0bddcde3fc2847fb5134a00d` / `review-029ebc9fcde87e377c774c70` |
| `migration-recovery` | `assignment-b482f5aadcfc926a26623eb9` | `DONE` | `2e0647fb4a7f764bd65a560d699ec78b3a458e2e` | `result-14c7bc79c49f4ff4f4eef78bb4ccf776dd4cc9b66040bbfe0bace7aebf75e81f` | `review-3e22808dd70aa0a02e49f09b` / `review-d8037cbf8cdbb2a215a68508` |
| `qualification` | `assignment-855a3a8379a40663fbea2222` | `FAIL` | `2e0647fb4a7f764bd65a560d699ec78b3a458e2e` | `result-cac20dbf5f45bd92fb01fe64de6cae918badd8ef5a34e3ed60867194310040fd` | `review-2ad5b0f819e4a38deec82949` / `review-1a4f8e66fb37d7d31bea9b7a` |
| `psc-continuity-coordinator` | `assignment-d9d4b2ba8af8ab597b4d142b` | `RESUME_RUNTIME` | `2e0647fb4a7f764bd65a560d699ec78b3a458e2e` | `result-e62b6925dfb657779b599c69826d5a249c08a410fd3d5d14d9d4a41ca42d1c64` | `review-2c40bb0afe24cd49c4ea0368` / `review-9595e32e4ba660c1b4e1e2b3` |
| `runtime-integration` | `assignment-c86d5d9043cd1f8f33ba7ade` | `DONE` | `6f3ad6af2f2b46dcb262afcb8094a0f0cda5a153` | `result-43d6d6334f8c4cb07c1115e59299ba2151ed823d4c4b542dde1d6730c1795dbe` | `review-6e2bfc408f313d3dca2637a7` / `review-8dc535417c8106ad9c80dd16` |
| `migration-recovery` | `assignment-06eebb271b06bc4d54b89be8` | `DONE` | `a205a358dd11470c899fb8d1f044181337d1880f` | `result-2f8d612cd39d3221ad1a1eec6c915485df50184b1795771b568c9f6ba48574dd` | `review-d69d8e974fb75b72e2f70532` / `review-1822e4ee86a3827cf2b29e14` |
| `qualification` | `assignment-777cd2909e549aae3bd45383` | `FAIL` | `a205a358dd11470c899fb8d1f044181337d1880f` | `result-693ef7576b0d7afe44e26a143fb89f504a9328b6208ce4c37a4aad93e1fea02a` | `review-719d1b13de3ad1bdd3848d4d` / `review-1f73b4463e7d6ae668e94557` |
| `psc-continuity-coordinator` | `assignment-797a5b438d8b019d9e69731c` | `RESUME_RUNTIME` | `a205a358dd11470c899fb8d1f044181337d1880f` | `result-59f17b1a7b0a592d7f081c323a0038e521709487e73d72b5b3019468e41633de` | `review-4627ec550edbe6f9c30c78ef` / `review-a31a8e2a03d4500ea0ce9521` |
| `runtime-integration` | `assignment-65013cd6cb6356d82889032a` | `DONE` | `8302c327b8c8853f1deb26c3e2dc5864af9223c4` | `result-787876d40b0fa2cc73767a80828a6fb538f1f7b559203218247645997f8b9da8` | `review-9e63ce995b8be3eba5a5d85b` / `review-6095e3f04f9c8fbfe1fc9709` |
| `migration-recovery` | `assignment-d848808c4125e052cd159017` | `DONE` | `8302c327b8c8853f1deb26c3e2dc5864af9223c4` | `result-b50efe60793d29f0effbcc24e62374e8db29a4b541e27cf69d5c65f1f8e99555` | `review-ba45f6843abbcc2dc26c5d68` / `review-a8b5dd93c6e5f084842bf43a` |
| `qualification` | `assignment-65ca915fa1d4424b014a986d` | `FAIL` | `8302c327b8c8853f1deb26c3e2dc5864af9223c4` | `result-bd1bcbfb1d7833daa1a10438ee0e21d5f0ed71119cab202d196fc3528a6fa6ed` | `review-69469b87055c80f65efef4db` / `review-e860bd2daac647ddfc23665d` |
| `psc-continuity-coordinator` | `assignment-a88704d85cda05168b8f0241` | `RESUME_RUNTIME` | `8302c327b8c8853f1deb26c3e2dc5864af9223c4` | `result-50ef86933e557f37058135b67bb1a6854b47811bc6f3c7954235a8621869f209` | `review-3cfd176c1459ac5d9574cd34` / `review-c13fc2d2a1d9cb1d26b1a030` |
| `runtime-integration` | `assignment-ce379cf511b7d8b361f05619` | `DONE` | `54d9eb716a88a195e3b9ade19ed7a480ffee9ccc` | `result-d1a339ca01eca92a01232cac7bbb12b3f1beeb2ddd57f18c763b70410ab2fe3c` | `review-fb7c057f126f7d02864d4f1b` / `review-2c7fe11ff89a5720a5af2801` |
| `migration-recovery` | `assignment-db03d2ee5ade74c7639988f9` | `DONE` | `54d9eb716a88a195e3b9ade19ed7a480ffee9ccc` | `result-2ef3fe6067ba7e142a4f1fa40a633c5d9fe2d66656dd61edb172b180f6246436` | `review-3dedc271c5be230e18e8fe30` / `review-e1bab5d4ec1f30769ac8ce9c` |
| `qualification` | `assignment-c6729d24e3efdc2cb1675969` | `PASS` | `6dec4c35c236ef62dd890ff65ba6c33b93a53c27` | `result-a0a33542f8c9b4c8dcc6dff0435f633f1d1c3f6e46cc064ac6db73c541f4df29` | `review-b2a9efa88092a663341e73c2` / `review-cef91ca1ce98868ef8e36447` |

There are 22 fully approved gates in the durable observability projection.
Three old evidence/security review bindings (`review-01c3cc7e1013113d6bb590b7`,
`review-4f4383bc9df40483e9d886e4`, and
`review-c8e650760a14c7f3ef5c45c8`) remain marked active on superseded results whose
paired runtime review rejected the gate. The graph has already created and
accepted their rework successors, has no pending decision, and routed to
PSC-007. They are not resubmitted or manually repaired. This is a disclosed
projection-maintenance risk, not approval evidence.

## Qualification evidence

| Artifact | Bytes | SHA-256 | Git blob |
| --- | ---: | --- | --- |
| `docs/evidence/prompt-storage-compaction-v1/psc-006-a05-a16-a17-benchmark.raw.json.base64` | 21,553 | `1e27f79429fa1161e9fd93970e59aeb9775312b61997e3318535d9d87e19994e` | `d4df7979488b3667990eca69de32a1b3b74944a0` |
| `docs/evidence/prompt-storage-compaction-v1/psc-006-benchmark-adjudication.json` | 7,398 | `1ab44d24f2fa19b5d7c51b60a946736b2710d10459950bec13d9f2169b7f6238` | `6e534becacdd561a6c82672f89d4846e40ec916e` |
| `docs/evidence/prompt-storage-compaction-v1/psc-006-delta-gap-e862224.json.gzip.base64` | 9,225 | `b7b21953410b035f2be0c1fcaee513983af89b96f310161d5c981bec2cf22839` | `a99304c84e64b79d3a14b41834d16f54517f3b0b` |
| `docs/evidence/prompt-storage-compaction-v1/psc-006-baseline-plus-diff-qualification-20261022.json` | 23,783 | `45d0d1deb2056257c158375c8acf8e9456eb00bf108e98f1cc652b7ca7777b7c` | `1f05fffa85bd7831baa94e2cb503d7fbf103da7f` |

The corrected report payload seal is
`b2aeade55c2ff199a4fd8dc1567376af0aff0713ef007839727e24c3d93552e7`.
The delta carrier decodes to 23,086 raw bytes with SHA-256
`ddd51b85f46efeaaf318bb4fed35fe7e62b723e905deb73ac06ba957c54a27a4`.

### Fresh checks retained without repetition

1. At `3dbf2d5e2690113a3ffbb0981cf436f85c828168`, 46 archive/runtime
   tests passed:
   `python -B -m unittest -v tests.test_prompt_archive_native_manifest tests.test_prompt_archive_runtime`.
2. At the same unchanged production/integration content, three exact
   publication/recovery integration tests passed. Their corrected ID-list
   SHA-256 is
   `5dd0428e594b6b08ef25b443c799e813ca435123a307c145f6700b9548bcb5e9`.
3. At exact tested runtime `e862224c`, 46 benchmark/selector/finalize and
   runner fail-closed tests passed:
   `python -B -m unittest -v tests.test_psc_exact_head_benchmark tests.test_psc_qualification_runner`.
4. The combined ordered 95-test ID-list SHA-256 is
   `481bfe62364b1148574580d6e9b491c0a28266acf4d967ec3b49167ada2cff06`.
5. The four-transition delta workload took approximately 10,163.9 seconds
   and recorded 3,759 CAS put attempts, 1,061 new objects, 2,698 dedup hits,
   zero forbidden attempts, zero unsafe-alias attempts, exact round-trips, and
   an unchanged protected-fixture inventory.

Commits after `3dbf2d5e` changed the qualification runner/tests and then evidence,
not the archive/runtime production modules or the three selected integration
tests. Tooling was therefore rerun on `e862224c`; unaffected results were
retained. The evidence-only changes from `e862224c` through `6dec4c35` do not
justify repeating runtime tests or the expensive benchmark.

## PSC-A01 through PSC-A20 disposition

The basis below applies the operator-approved baseline-plus-diff plan. It does
not silently replace the stricter original full-exact-head wording.

| ID | Disposition | Inspectable basis and limitation |
| --- | --- | --- |
| PSC-A01 | PASS under accepted package | Contract/CAS/incremental accepted results plus historical reconstruction evidence; unchanged implementation is ancestry-bound to the fresh candidate. |
| PSC-A02 | PASS under accepted package | Versioned semantic digest contract and schemas accepted in PSC-001/002; canonical equality remains distinct from raw-byte equality. |
| PSC-A03 | PASS under accepted package | Versioned ref namespace/schema and fail-closed unknown-version behavior accepted; no later conflicting change. |
| PSC-A04 | PASS under accepted package | CAS dedup/concurrency implementation and accepted tests; delta probe recorded nonzero dedup (2,698 hits). |
| PSC-A05 | PASS for measured scope | Historical seven-transition evidence plus fresh four-transition append/deep-checkpoint delta probe. Fresh probe intentionally omits middle insert/delete and full historical distribution. |
| PSC-A06 | PASS under accepted package | Accepted corruption/depth checks plus fresh exact-depth-512 acceptance and depth-513 rejection before mutation. |
| PSC-A07 | PASS under accepted package | Accepted storage concurrency coverage; relevant implementation unchanged after its reviewed result. |
| PSC-A08 | PASS under accepted package | Accepted runtime/migration fault-injection and fresh selected publication/recovery paths; not a new full fault-matrix rerun. |
| PSC-A09 | PASS under accepted package | Accepted recovery work plus three fresh cross-component publication/recovery integrations. |
| PSC-A10 | PASS under accepted package | Consumer inventory, accepted compatibility work, and fresh legacy/shadow/recovery integrations. External local consumers remain an operator inventory obligation. |
| PSC-A11 | PASS under accepted package | Accepted hot-path isolation tests; unchanged runtime path after the tested runtime. |
| PSC-A12 | PASS under accepted package | Accepted managed phone-specific authorization and non-enumeration contract; no live endpoint was exercised during release planning. |
| PSC-A13 | PASS under accepted package | Accepted traversal/alias/ADS/reparse/root guards plus current runner isolation tests and read-only source audit. |
| PSC-A14 | PASS under accepted package | Accepted API/source-metadata bounds and targeted scans. Opaque historical raw evidence is explicitly not claimed secret-free. |
| PSC-A15 | BASELINE_PLUS_DIFF_PASS | Historical 649/882 aggregate plus 95 fresh unique tests. Full current 956 selector is reproducible but `NOT_RUN_NOT_CLAIMED`. |
| PSC-A16 | PASS for measured scope | Historical benchmark retained at its source version plus fresh four-transition delta benchmark; historical full latency/distribution was not relabelled current. |
| PSC-A17 | PASS for measured scope | Fresh delta evidence recorded zero forbidden/unsafe/protected-fixture attempts and an equal full protected-fixture inventory. No live root was inventoried or read. |
| PSC-A18 | PASS for plan/default | Current source audit confirms default `legacy`, explicit isolated shadow root, and an additional exact gate for authoritative. No instance was enabled or restarted. |
| PSC-A19 | PASS | Scope/diff review and evidence show no automatic durable deletion/GC and no such action occurred. V1 remains append-only. |
| PSC-A20 | PASS for this operator package | Provenance, evidence, changed files, risks, isolated shadow procedure, disable/rollback, and merge gates are recorded here and in the managed result. No merge/deploy occurs. Strict full exact-head qualification remains not claimed. |

## Changed-file inventory

From comparison base `ff636bfb` through accepted evidence head `6dec4c35`:
96 paths, 88 added, 8 modified, 86,148 insertions, and 156 deletions. Category
counts are 15 runtime, 29 tests, 31 schemas, 9 documentation/evidence, 8
orchestration, 3 tools, and 1 prompt. The canonical
`{status,path,blob}` inventory SHA-256 is
`2bb4838b26b70bbafa23d9225b8e2a8b8577a0d3be3aaa2ec0c63aed5b708c28`;
the raw name-status inventory SHA-256 is
`54c4601b1f0f07bfeb61dce23a0198062fe310195a3fb94a5846ec69a0187a03`.

```text
M README.md
A docs/PROMPT_ARCHIVE_AUTHORITATIVE_BACKUP_RECOVERY_V1.md
A docs/PROMPT_ARCHIVE_CONSUMER_INVENTORY_V1.md
A docs/PROMPT_ARCHIVE_OFFLINE_MIGRATION.md
A docs/PROMPT_ARCHIVE_STORAGE_V1.md
A docs/evidence/prompt-storage-compaction-v1/psc-006-a05-a16-a17-benchmark.raw.json.base64
A docs/evidence/prompt-storage-compaction-v1/psc-006-baseline-plus-diff-qualification-20261022.json
A docs/evidence/prompt-storage-compaction-v1/psc-006-benchmark-adjudication.json
A docs/evidence/prompt-storage-compaction-v1/psc-006-delta-gap-e862224.json.gzip.base64
M main.py
M nginx_qa/managed_continuity.py
A nginx_qa/prompt_archive_api.py
A nginx_qa/prompt_archive_incremental.py
A nginx_qa/prompt_archive_json_spans.py
A nginx_qa/prompt_archive_latest.py
A nginx_qa/prompt_archive_metrics.py
A nginx_qa/prompt_archive_migration.py
A nginx_qa/prompt_archive_occurrence_store.py
A nginx_qa/prompt_archive_response.py
A nginx_qa/prompt_archive_runtime.py
A nginx_qa/prompt_archive_source_metadata.py
A nginx_qa/prompt_archive_state_view.py
A nginx_qa/prompt_archive_storage.py
M nginx_qa/scope_api.py
A orchestration/sprints/prompt-storage-compaction-v1/.gitattributes
A orchestration/sprints/prompt-storage-compaction-v1/README.md
A orchestration/sprints/prompt-storage-compaction-v1/acceptance-matrix.md
A orchestration/sprints/prompt-storage-compaction-v1/prototype-audit.md
A orchestration/sprints/prompt-storage-compaction-v1/qualification-selectors-v1.json
A orchestration/sprints/prompt-storage-compaction-v1/reference/prompt_storage_compactor_prototype.py
A orchestration/sprints/prompt-storage-compaction-v1/requirements.md
A orchestration/sprints/prompt-storage-compaction-v1/sequential-sprint.json
M prompts/sequential_sprint_launch_standard.txt
A schemas/prompt-archive-blob-v1.schema.json
A schemas/prompt-archive-compact-response-v1.schema.json
A schemas/prompt-archive-descriptor-stub-v1.schema.json
A schemas/prompt-archive-descriptor-v1.schema.json
A schemas/prompt-archive-envelope-v1.schema.json
A schemas/prompt-archive-error-v1.schema.json
A schemas/prompt-archive-index-node-v1.schema.json
A schemas/prompt-archive-journal-v1.schema.json
A schemas/prompt-archive-json-edge-v1.schema.json
A schemas/prompt-archive-json-value-v1.schema.json
A schemas/prompt-archive-latest-slot-v1.schema.json
A schemas/prompt-archive-locator-v1.schema.json
A schemas/prompt-archive-manifest-v1.schema.json
A schemas/prompt-archive-migration-analysis-v1.schema.json
A schemas/prompt-archive-migration-batch-journal-v1.schema.json
A schemas/prompt-archive-migration-batch-plan-v1.schema.json
A schemas/prompt-archive-migration-error-v1.schema.json
A schemas/prompt-archive-migration-evidence-v1.schema.json
A schemas/prompt-archive-migration-private-evidence-v1.schema.json
A schemas/prompt-archive-migration-reconstruction-evidence-v1.schema.json
A schemas/prompt-archive-migration-resume-evidence-v1.schema.json
A schemas/prompt-archive-migration-verification-v1.schema.json
A schemas/prompt-archive-occurrence-document-v1.schema.json
A schemas/prompt-archive-pending-operation-registry-v1.schema.json
A schemas/prompt-archive-pointer-v1.schema.json
A schemas/prompt-archive-publication-plan-v1.schema.json
A schemas/prompt-archive-record-v1.schema.json
A schemas/prompt-archive-ref-v1.schema.json
A schemas/prompt-archive-source-inventory-v1.schema.json
A schemas/prompt-archive-source-metadata-v1.schema.json
A schemas/prompt-archive-state-view-v1.schema.json
M tests/test_project_actor_import.py
A tests/test_prompt_archive_api.py
A tests/test_prompt_archive_auth.py
A tests/test_prompt_archive_contract.py
A tests/test_prompt_archive_incremental.py
A tests/test_prompt_archive_incremental_security.py
A tests/test_prompt_archive_incremental_sequences.py
A tests/test_prompt_archive_incremental_source_metadata.py
A tests/test_prompt_archive_json_spans.py
A tests/test_prompt_archive_latest.py
A tests/test_prompt_archive_metrics.py
A tests/test_prompt_archive_migration.py
A tests/test_prompt_archive_native_full_fixture.py
A tests/test_prompt_archive_native_manifest.py
A tests/test_prompt_archive_occurrence_store.py
A tests/test_prompt_archive_offline_runtime.py
A tests/test_prompt_archive_response.py
A tests/test_prompt_archive_response_shape_contract.py
A tests/test_prompt_archive_runtime.py
A tests/test_prompt_archive_runtime_integration.py
A tests/test_prompt_archive_runtime_legacy_latest.py
A tests/test_prompt_archive_source_metadata.py
A tests/test_prompt_archive_source_metadata_projection_store.py
A tests/test_prompt_archive_state_view.py
A tests/test_prompt_archive_storage.py
A tests/test_psc_exact_head_benchmark.py
A tests/test_psc_qualification_runner.py
M tests/test_sequential_prompt_ui.py
M tests/test_sprint_type_contract.py
A tools/prompt_archive_migrate.py
A tools/run_psc_exact_head_benchmark.py
A tools/run_psc_qualification.py
```

The release-plan carrier adds only this documentation path after `6dec4c35`.

## Disabled-by-default configuration

`PromptArchiveRuntimeConfig` defaults to `mode=legacy`.

- `NGINX_QA_PROMPT_ARCHIVE_MODE`: `legacy`, `shadow`, or `authoritative`;
- `NGINX_QA_PROMPT_ARCHIVE_ROOT`: required absolute, isolated root for shadow
  or authoritative;
- `NGINX_QA_PROMPT_ARCHIVE_MAX_HYDRATED_BYTES`: bounded hydration limit
  (default 256 MiB; hard maximum 512 MiB);
- `NGINX_QA_PROMPT_ARCHIVE_OPERATOR_GATE`: authoritative additionally requires
  the exact value `prompt-archive-authoritative-v1`.

Configuration loading occurs at application startup. A future mode change
therefore requires a controlled restart of only the separately authorized
isolated instance. Merely setting the authoritative gate is configuration, not
operator authorization.

## Future isolated shadow enable / observe / disable plan

This procedure is not executed by this sprint.

### Preconditions

1. Obtain a separate operator change record naming the exact release carrier,
   exact tested runtime/tree, fresh loopback port, child-port range, instance
   identity, registry, and mutually isolated service/state/runtime/managed/
   prompt/archive roots.
2. Use generated fixtures only. Do not point any source, inventory, archive, or
   protected-root argument at a real prompt root.
3. Confirm the archive root does not overlap code, service, runtime, managed,
   prompt, explicitly protected, or persisted prompt-template roots. The
   occurrence receipt root is intentionally derived beneath the isolated
   runtime root at `prompt-archive-occurrences/v1`; it must be separate from the
   archive root and must not overlap prompt/source/template roots. The isolated
   runtime root itself must be disjoint from every live/protected root. Alias,
   case, ADS, symlink/junction/reparse, or overlap uncertainty is STOP.
4. Name the owner who can stop the canary and the local mechanism that will
   collect payload-free process counters. There is no dedicated HTTP metrics
   endpoint in this version.

### Enable

1. Start once with archive mode/root/gate unset and capture legacy health plus
   generated-fixture response, latest-response, and `.prompt` semantics.
2. Drain and stop only the isolated instance. Keep its identity, registry, and
   non-archive roots fixed.
3. Select a new, empty, absolute archive root outside all protected roots. Set
   `NGINX_QA_PROMPT_ARCHIVE_MODE=shadow`, the isolated archive root, and an
   explicit bounded hydration limit. Keep the authoritative gate unset.
4. Start only the isolated instance. Require root validation and recovery to
   settle before readiness. Do not relax checks after an error.

### Observe

1. Drive bounded generated transitions only.
2. For every transition, verify the legacy transition/latest files remain full
   no-kind JSON and authoritative, and `.prompt` remains usable.
3. On successful shadow publication, verify the response advertises archive
   availability, hydrate through the authorized path, and compare raw length
   and SHA-256 to the already committed isolated legacy bytes.
4. Record `shadow_published`, `shadow_skipped`, `shadow_failed`, hydration,
   corruption, recovery, format, logical/new-physical/dedup counters. Require
   zero corruption/recovery failures and explain every skip/failure.
5. Record archive allocation/growth and payload-free diagnostics. Do not inspect
   live roots. Any unexpected protected-root attempt, partial descriptor,
   legacy-byte mismatch, unresolved recovery, or secret/payload in telemetry is
   STOP.

### Disable

1. Deny new canary ingress, drain, and gracefully stop only that isolated
   instance.
2. Set mode back to `legacy`; unset the authoritative gate; retain the same
   archive root, runtime root, registry, and identity. Do not delete or rename
   the archive root.
3. Restart the isolated instance and let normal recovery settle valid pending
   work before writes.
4. Verify new responses no longer create shadow publications, legacy output is
   still full and usable, and previously created V1 artifacts remain readable.

Shadow mode still writes cumulative legacy JSON first. It validates PAS1
publication safely but does not remove the legacy snowball cost. Only a future
separately authorized authoritative rollout can change that hot path.

## Rollback and storage compatibility

- Shadow rollback is disable-only because committed legacy bytes remain the
  authority. It does not copy, restore, rewrite, or delete files.
- A mode toggle does not convert descriptor stubs back to inline JSON, remove
  CAS objects/receipts/plans/journals/pointers/locators/pending operations, or
  roll back external latest state.
- If recovery or integrity validation fails, stop writers, preserve evidence,
  and escalate. Do not edit JSON, registries, journals, pointers, SQLite, or
  runtime state; do not start a legacy writer against an enrolled target with a
  missing/unvalidated archive root.
- A real authoritative latest-byte rollback is a separately gated, in-process,
  journaled `legacy_latest_publish` operation with `write_kind=rollback`; it is
  not file copy or manual overwrite and is outside the shadow plan.
- The cold coherent recovery unit is inseparable: complete archive root,
  occurrence/projection authority, every enrolled external latest file,
  transition stub, and its full sidecar. Restore is cold replacement, never
  in-place repair. The documented drill has not been executed on this candidate.
- Legacy no-kind inline bytes remain byte-exact readable. Shadow preserves full
  legacy response paths and adds descriptors only after success. Switching to
  legacy stops new PAS1 writes but retains reads of existing V1 artifacts.
- Authoritative response paths use descriptor stubs; inline-only consumers are
  incompatible until independently inventoried and qualified.

## Future separately gated work

### Garbage collection

V1 has no GC/delete mode. A future versioned contract must freeze reachability
and retention roots covering blobs, manifests, indexes, pointers, locators,
plans, journals, pending registries, rollback selections, migration evidence,
occurrence proofs, external latest sidecars, and sealed backups. It also needs
an epoch/concurrency fence, dry-run inventory, cold backup/restore drill, audit
receipts, and a non-deleting canary before any separate delete authorization.

### Live migration

The offline migration CLI accepts only explicit isolated copies and rejects live
roots; it is not a cutover tool. A live contract would require a cross-authority
quiescent snapshot/fence, complete external-consumer qualification, dual-read/
write cutover, forward-recovery and rollback evidence, capacity/observability
gates, and separate approval for exact instance and paths. No legacy or CAS
deletion may occur without another retention decision.

## PR and merge plan

No PR is opened by PSC-007.

1. The release-plan carrier must be a direct, documentation-only child of
   `6dec4c35` on `agent/psc-07-release-plan`, with the managed result binding its
   exact commit/tree and parent.
2. Before a future PR, fetch and revalidate the then-current protected base.
   Stop if ancestry is no longer fast-forward/merge-preserving or if the
   release-plan carrier includes code, schema, runtime, or test changes.
3. Publish without squash/rebase/force-push reconstruction. Preserve the
   reviewed chain and disclose the exact tested runtime separately from the
   evidence carrier.
4. Require distinct runtime/storage and evidence/security reviews of the exact
   PR head, branch-protection checks, no unresolved blocking threads, and an
   explicit operator decision on whether the disclosed baseline-plus-diff
   qualification is sufficient or a full exact-head run is required.
5. Merge only after a separate operator authorization. Do not deploy or enable
   a mode as a consequence of merge.

Current merge/deploy recommendation: **NO-GO without separate operator
authorization and protected-PR evidence**. Live promotion and authoritative
mode remain **NOT AUTHORIZED**.

## Residual risks and known limitations

1. The historical aggregate has no exact-ID/per-run ledger. The current
   956-test selector was not executed as a full suite on `e862224c`.
2. The delta workload did not rerun historical middle insert/delete or the full
   65-to-68 distribution, and historical latency/distribution is not current.
3. No isolated authoritative backup/restore drill or live rollout was executed.
4. Opaque historical raw evidence is not claimed secret-free; scans were
   targeted to the changed/current scope.
5. Three stale review bindings from superseded rejected gates remain projected
   active. No manual repair was attempted.
6. The cumulative change is large (96 paths and more than 86k insertions), so
   protected review and base refresh remain important.
7. Process counters are non-durable and have no dedicated HTTP export in V1.
8. Shadow does not eliminate legacy cumulative-write cost. V1 is append-only
   and has no retention/GC mechanism.
9. Repository consumer inventory cannot prove safety of unknown external/local
   inline-only readers.
10. The comparison base may move after this package; it must be refreshed before
    any PR without rewriting this accepted history.

## Recorded non-actions

During this sprint and release-plan preparation:

- no real prompt content was read, hashed, copied, migrated, or written;
- no service was stopped or restarted;
- no runtime JSON, SQLite, queue, registry, or managed state was edited;
- no durable archive/blob was deleted and no GC ran;
- no production or authoritative mode was enabled;
- no main merge, PR creation, deploy, live promotion, import, or start-from-git
  was performed.
