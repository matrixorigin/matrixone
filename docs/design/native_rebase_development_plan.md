# Native Rebase Delivery Boundaries

This is the maintained delivery map, not an append-only run log. The original
fixed R1 revision remains independently reviewable. Restricted R2 is developed
separately and stays disabled in production until its own acceptance is met.

| Milestone | Implementation boundary | Required executable proof |
| --- | --- | --- |
| R1 replay | Four protected endpoints; same-schema PK net-state replay into a new database; retain fixed U | Real SQL, supported/rejected values, partial failure/cancellation, commit failure, crash/restart, source DROP/GC protection |
| Inventory | Real tenant/DB-ID epoch row; directory writes and reservation in their original owners | Both competition orders, rollback, retirement, old-token rejection after same-name replacement |
| Producers/upgrade | Common lifecycle plus routine/settings owners; fresh bootstrap and shutdown migration | Representative DDL/restore, no-op exclusions, normal released-data migration, interrupted migration and old-backup recovery |
| Source data | Separate logical-DML dependency, opt-in WAL/checkpoint history | Pending/committed writers, physical maintenance control, actual WAL/checkpoint recovery and metadata rewrite |
| Publication | Candidate/base/history/pointer CAS/source retirement/COMMITTED in one transaction | Original CREATE-after-F loss case, source DML/DROP/name races, cross-CN same-TN and unrelated DB/tenant controls |
| Client recovery | PREPARING before snapshot; acknowledged single-send claim; fresh-connection recovery | Real child exits with snapshot in flight, before COMMIT and after server commit before client response |
| Repeat semantics | Durable new base and generation identity | Second Rebase only replays new changes; subsequent Merge and historical reads remain correct |
| Cost | Same qualified build and frozen old/native adapter | Small then W1, one worker/16-spine/seed42/one warmup/ten timed calls; costs reported separately |

Support remains same tenant/schema, ordinary explicit PK, and unchanged no-PK
multisets under full visible-data equality. Unsupported combinations fail before
business/candidate/pointer mutation. No full data_agent/SF100, Reset/Revert,
general allocator investigation or speculative branch service is required here.

Use existing transaction, lifecycle, snapshot and tenant-upgrade owners. Do not
substitute process locks or repeated scans for commit conflicts. Unknown request
outcomes retain resources until terminal proof; recovery is confirmation, not
re-execution. Normal branch/backend routing remains unchanged.

Run targeted vet/build before real tests, reuse exact-input valid evidence and
only repeat affected obligations after integration. Actual process death and
WAL recovery cannot be replaced by Stop/Start or simulated exceptions. Selected
cross-CN evidence does not certify every distributed schedule.

Keep wrappers, raw results, per-run manifests and review chronology in external
qualification artifacts. Deliver maintained regression SQL/results and tests,
source identities, support matrix, protocol docs, measured costs and clear
CI/QA limits. A Draft PR or successful happy path alone is not R2 completion.
