# Acceptance matrix

| ID | Проверка | Ожидаемый результат |
|---|---|---|
| COMP-01 | Import старого JSON без `sprint_type` | Поведение и сохранённое состояние совпадают с baseline |
| COMP-02 | `sprint_type=legacy_v1` | Эквивалент отсутствующему field |
| COMP-03 | Unknown sprint type | Reject до mutation |
| ISO-01 | Parent directory содержит чужой `.git` | Managed workspace использует только свой verified root |
| ISO-02 | Workspace root = drive root/home/install root | Reject |
| ISO-03 | Dirty workspace | Preserve + Coordinator, no clean/reset |
| BR-01 | Две write assignments на одну branch | Вторая не активируется |
| BR-02 | Diverged existing branch | Coordinator, no force/reset |
| PORT-01 | 4 concurrent child processes | 4 unique live leases/ports |
| PORT-02 | Restart | Leases восстановлены без duplicate allocation |
| PROC-01 | Child crash | Status FAILED, logs preserved |
| PROC-02 | Orphan PID | Reconcile before new assignment |
| IMP-01 | Checksum mismatch | Preflight failure, no assignment |
| IMP-02 | Missing transition | Preflight failure |
| IMP-03 | Failure during prepare | Old active sprint unchanged |
| HAND-01 | Duplicate result | ALREADY_ACCEPTED |
| HAND-02 | Crash after result before enqueue | Same transition completed after restart |
| CONT-01 | STOP | Coordinator receives full context |
| CONT-02 | Repair future graph | Completed history unchanged |
| STAGE-01 | Staging startup | HTTP 200 on 127.0.0.1:18025 |
| STAGE-02 | Live instance | Remains running, PID/state unchanged |
| STAGE-03 | Runtime state | No file sharing between live and staging |
| LEG-01 | Existing sequential graph tests | PASS |
| LEG-02 | Existing direct/Telegram/pending import tests | PASS |
| LEG-03 | Existing UI/history/queue persistence tests | PASS |
| REL-01 | Clean staging restart | PASS |
| REL-02 | Evidence export | Contains commits, tests, logs, manifest hash, no secrets |
