# Historical orphan object privilege maintenance

- Design version: **v1**
- Status: **proposed — implementation review blocked pending independent approval**
- Approved design revision: **pending**
- Independent review decision: **pending**
- Owning issue: [#27836](https://github.com/matrixorigin/matrixone/issues/27836)
- Related forward-cleanup issue: [#27723](https://github.com/matrixorigin/matrixone/issues/27723)
- Implementation PR: [#28120](https://github.com/matrixorigin/matrixone/pull/28120)
- Scope: bootstrap tenant-upgrade lifecycle, authorization catalog maintenance, tenant isolation, physical composite-primary-key scans, planner/storage ordered-limit contract, restart and mixed-version behavior

## 1. Design-review classification

This change requires a design review for both size and complexity:

- the complete PR changes about 836 non-generated production lines before later review fixes, exceeding the 500-line default trigger;
- it retains one tenant-upgrade goroutine as a long-running maintenance worker;
- it changes upgrade completion, retry, shutdown, restart, and recurring-work lifecycle;
- it reads and deletes authorization metadata across tenant boundaries;
- it permits every CN to perform duplicate idempotent maintenance;
- it adds a generic planner/storage contract for ordered limits on hidden composite primary keys;
- it intentionally uses process-local progress and probabilistic restart convergence.

The approved #27723 design cannot cover this work. Its non-goal explicitly assigns historical cleanup to #27836 and rejects a background worker for forward DROP correctness. This document owns only #27836.

## 2. Problem and evidence

Before #27745, successful object DROP could leave `mo_catalog.mo_role_privs` rows whose `obj_id` no longer exists in `mo_database.dat_id` or `mo_tables.rel_logical_id`. Logical object IDs are not reused, so no known privilege escalation follows, but repeated DDL churn leaves permanent catalog bloat.

A direct `DELETE ... NOT EXISTS ... LIMIT 1000` bounds deleted rows but not inspected or scanned rows. A logical five-column keyset query also does not imply bounded physical work: on 10,036 rows it allowed the Table Scan to input about 8,241 rows for a limited page. A process-local cursor starting at the minimum can additionally replay the same live prefix after every process restart.

The design must therefore distinguish:

1. **semantic page size** — at most 1,000 privilege candidates classified by one transaction;
2. **physical scan work** — ordered limit must reach the storage reader on the existing physical key;
3. **healthy-process completion** — a frozen finite tenant/account set must close;
4. **restart behavior** — process-local progress is lost and has no finite-restart convergence bound.

## 3. Definitions

- **Candidate**: one `mo_role_privs` row returned by the bounded physical-key query and considered for preservation or deletion.
- **Confirmed orphan**: a supported concrete database/relation grant with nonzero `obj_id` for which the corresponding logical ID is absent in the same tenant transaction snapshot.
- **Physical key**: `mo_role_privs.__mo_cpkey_col`, the existing serialized five-column primary key.
- **Account round**: a finite traversal whose account high-water is frozen when the round begins.
- **Tenant ring**: a finite traversal of keys visible up to a frozen physical high-water, split around a selected start threshold.
- **Maintenance page transaction**: one target-tenant transaction that examines no more than 1,000 candidates and deletes only confirmed orphans from that candidate set.
- **Healthy process**: a service process that remains running long enough for its periodic owner to execute successive pages without losing process-local state.
- **Restart opportunity**: a new process generation that reaches final-version maintenance and selects the relevant tenant.

## 4. Goals, non-goals, and constraints

### 4.1 Goals

1. Remove historical confirmed orphan database and relation grants.
2. Preserve global/account grants, valid live-object grants, hidden-child grants, zero IDs, and unknown legacy encodings.
3. Keep every statement tenant-local and prevent cross-tenant deletion.
4. Inspect and delete from at most 1,000 privilege candidates per maintenance transaction.
5. Bound physical candidate/high-water scans through the existing composite primary key without adding catalog indexes or hidden index tables.
6. Complete finite frozen account and tenant rings while a process remains healthy.
7. Remain idempotent under retries, duplicate CN execution, late tenant creation, restore, and mixed-version writers.
8. Keep normal tenant upgrades higher priority than historical maintenance.

### 4.2 Non-goals

1. No durable cleanup cursor, completion marker, new catalog table, or repurposed upgrade-task field.
2. No finite process-restart count guaranteeing visitation of a particular tenant key.
3. No wire/MORPC capability, protocol version, or admission fence.
4. No change to #27745 forward DROP/GRANT lifecycle semantics.
5. No new secondary index on `mo_role_privs` or live-object catalogs.
6. No operator SQL command or synchronous cluster-wide cleanup guarantee.
7. No proof that historical cleanup has globally completed forever.

### 4.3 Fixed constraints

- page size is 1,000 candidates;
- progress is process-local and published only after transaction commit;
- normal upgrade tasks run first;
- catalog schema and snapshot format remain unchanged;
- multiple CNs may execute the same logical work;
- no `mo_upgrade_tenant.from_account_id` mutation or reuse.

## 5. Required invariants and acceptance semantics

### 5.1 Safety invariants

1. One page classifies at most 1,000 `mo_role_privs` rows.
2. DELETE keys are a subset of that page and use the complete five-column logical primary key.
3. Every live-object lookup and DELETE executes with the selected account ID; relation lookup also checks `account_id = current_account_id()`.
4. `obj_id = 0`, unknown object/scope encodings, and recognized rows with a live logical ID are preserved.
5. A failed/retried transaction publishes neither cursor progress nor completion.
6. Physical reader-limit pushdown is enabled only when every scan predicate is a folded literal range on the same hidden physical primary key. Any residual or non-folded runtime expression disables reader truncation.
7. A maintenance error cannot fail or roll back an already committed normal tenant upgrade; the two activities use separate transactions.

### 5.2 Liveness semantics

- **Healthy-process guarantee**: after selecting a tenant with `N` rows at or below its frozen high-water, its two-segment physical ring finishes in at most `ceil(N/1000) + 2` page attempts that commit successfully. The additive bound covers segment transition and an exact-full-page terminal probe. The account round is finite because its account high-water does not move.
- **Restart limitation**: process restart discards account and tenant progress. New account/key starts have full support, so under independent entropy and infinitely many eligible restart opportunities each fixed existing tenant/key is visited with probability one. There is no deterministic finite-restart bound.
- **Issue acceptance interpretation**: “safe to resume after interruption” means retries/restarts cannot publish false progress, corrupt grants, or make cleanup non-idempotent. It does **not** mean deterministic continuation from the last committed key. This interpretation is an explicit accepted tradeoff only after independent design approval.

## 6. Selected architecture

### 6.1 Ownership

At service startup the existing upgrade framework creates four tenant-upgrade workers by default. After `finalVersionCompleted` becomes true, the first worker that wins `orphanPrivilegeMaintenanceWorkerRunning` remains as the sole process-local maintenance owner; the other workers exit. The owner stops when the service context is cancelled.

A separate `orphanPrivilegeMaintenanceRunning` CAS prevents overlapping local passes, including manually triggered upgrade pre-checks. No cluster-wide owner exists. Every active CN may have one local owner.

Ownership levels are therefore:

```text
cluster: no singleton; duplicate at-least-once work is allowed
CN/service: exactly one long-lived maintenance owner
pass: one CAS owner
account lookup transaction: local tentative state
page transaction: target tenant owns candidate classification and exact delete
```

### 6.2 Scheduling and priority

The owner wakes at `checkUpgradeTenantDuration` (default 10 seconds) and first runs the normal tenant-upgrade pass. Maintenance runs only when that pass finds no normal tenant work and the final version is complete. One wake processes at most one maintenance page; errors end that wake rather than creating an immediate retry loop.

### 6.3 Account traversal

At round start a system-account transaction reads the current maximum account ID as `roundHighWater`. A start in `[0, roundHighWater]` is derived from a per-process random seed. The owner traverses:

```text
[start, roundHighWater] then [0, start)
```

using ordered account lookup. New accounts above the high-water wait for the next round. Deleted or failing tenants advance the account cursor and can be revisited in a later round. Lookup transaction retries reconstruct tentative state from the last committed process-local state.

### 6.4 Tenant physical-key traversal

When an account is selected, a physical start threshold is generated. Half of starts use a valid serialized key shape with magnitude-distributed numeric fields, improving reachability around common small IDs. Half use a full-support byte distribution over lengths `[0, 956]`, ensuring every finite physical key has nonzero start probability independent of table cardinality.

The first page freezes the maximum physical key. The tenant traverses:

```text
[start, high-water] then [physical minimum, start)
```

Candidate/high-water SQL orders only by `__mo_cpkey_col`. Cursor and high-water are validated hex strings with a 956-byte decoded maximum. The cursor advances to the last examined physical key even if all candidates are live or conservatively preserved.

### 6.5 Candidate classification

Recognized database grants compare `obj_id` with `mo_database.dat_id`. Recognized table/view/relation grants compare `obj_id` with `mo_tables.rel_logical_id`. IDs are deduplicated before each live-object lookup. All uncertain encodings are preserved.

A confirmed orphan is deleted by exact five-column tuple, with a SQL `LIMIT 1000` as defense in depth. `AffectedRows` greater than the number of orphan candidates is an internal error.

## 7. Transaction, retry, and conflict model

### 7.1 Transaction sequence

A maintenance wake can execute:

1. the existing system-account normal-upgrade polling transaction;
2. when no tenant is selected, one system-account lookup transaction;
3. one target-tenant maintenance page transaction.

The account transaction commit is the publication point for account selection/start state. The tenant page transaction commit is the publication point for deletes and the next in-memory key cursor. Process-local state is assigned only after `ExecTxn` succeeds.

### 7.2 Retry

`ExecTxn` may rerun its closure. Account lookup resets tentative fields before each attempt. A page retry starts from the same committed input scan and overwrites only closure-local output. No retry observes a cursor produced by a rolled-back attempt.

### 7.3 Multiple CNs

CNs do not coordinate starts or pages. Two CNs can read the same candidate and issue the same exact-key DELETE. Outcomes are:

- one commits first and removes the orphan;
- another observes no matching row or receives a transaction conflict;
- a conflict/error rolls back that page and advances the local account so the tenant is retried in a later round.

Duplicate classification is safe because object IDs are not reused and DELETE is tenant-local and exact-key idempotent. This design accepts duplicate reads and conflict retries rather than introducing cluster ownership or leases.

### 7.4 Failure isolation

- missing/dropped/broken tenant: page fails, local account advances, later rounds may retry;
- candidate/live lookup/delete failure: page rolls back, no cursor is published;
- account lookup failure: account state does not advance;
- cancellation/shutdown: transaction follows executor cancellation; worker returns and releases local ownership with the service;
- panic/process crash: uncommitted transaction recovery remains owned by the existing transaction system; all maintenance state is lost;
- repeated errors: at most one error log per local owner wake, with no tight retry loop.

## 8. Planner/storage ordered-limit contract

The candidate page is physically bounded only when Sort/Limit sends an ordered-limit hint to the Table Scan. For a base table with hidden composite primary key, the planner may set `IndexReaderParam` only if:

1. the ordered column is exactly `__mo_cpkey_col` on that scan binding;
2. the limit is a positive static literal with no offset/rank/cardinality-reducing node between Sort and Scan;
3. every scan filter is a range comparison on the same physical-key column;
4. every bound unwraps to a non-NULL literal through pure binder-added casts;
5. bound and physical-key types are compatible;
6. there is no residual predicate.

Parameters, variables, and arbitrary no-column function expressions are not sufficient. They may be SQL runtime constants while remaining unavailable to the storage PK/block filter. Limiting before such a residual is evaluated can return incorrect rows.

Each physical source may produce its local top `K`; the existing global Sort merges those local candidates and applies the SQL limit. This is valid only because all accepted predicates are applied before local truncation. Unsupported shapes retain ordering hints but not a reader limit.

## 9. Capacity and performance budget

Let `C` be the number of active CN services and `T` the tick interval (default 10 seconds).

### 9.1 Rate bounds

- cluster page-attempt rate is at most `C/T` while no normal tenant upgrade is runnable;
- each CN has at most one page transaction in flight from this maintenance path;
- each page returns/classifies at most 1,000 privilege rows and deletes at most 1,000 rows;
- a newly selected nonempty tenant page executes at most six tenant SQL statements: high-water, an empty suffix candidate probe when start is above high-water, the wrapped candidate probe, database live-ID lookup, relation live-ID lookup, and DELETE; the usual non-wrap path uses at most five;
- account selection uses at most three reads: account high-water and up to two ring segments;
- transaction-log mutation per successful page is bounded by 1,000 privilege deletes; read-only pages add no privilege delete log records.

### 9.2 I/O model

- physical privilege candidate scan: ordered-limit work is bounded by page size plus reader/block granularity. The current 10,036-row object regression measures about 49 input rows for `LIMIT 1` and 1,049 for `LIMIT 1000`, with bounded `inputBlocks`;
- live database lookup worst case is `O(D)` over the tenant’s database catalog when ID filters cannot prune all blocks;
- live relation lookup worst case is `O(R)` over the tenant’s relation catalog;
- account traversal is ordered by account ID and returns one row;
- aggregate duplicate read work scales linearly with `C`; there is no cluster-wide I/O cap.

The design therefore bounds privilege candidates, SQL statement count, delete/log volume, concurrency per CN, and privilege scan input. It does not claim an absolute bound on live-catalog scan bytes or cluster aggregate bytes. Deployments with very large tenant catalogs or CN counts must use existing CN I/O/transaction dashboards during rollout. This linear amplification is an accepted tradeoff requiring explicit design approval.

### 9.3 Memory and retained state

One service retains one account/tenant state struct. Three hex physical keys are each at most 1,912 characters; candidate/object-ID slices and maps are at most 1,000 elements and live only for a page. Random raw start allocation is at most 956 bytes. No queue, unbounded map, channel, lease, or per-tenant persistent state is introduced.

## 10. Security and tenant isolation

The worker is internal bootstrap infrastructure and does not expose a user API. Every maintenance statement carries the selected account ID. Live relation queries additionally require `account_id = current_account_id()`. The DELETE names only exact logical primary keys read in that tenant transaction.

The classifier is deliberately conservative. It preserves global grants (`obj_id = 0`), unknown legacy scopes, live logical IDs, sequences represented as relations, views, and hidden child relations. Logical IDs are not reused, which makes stale exact-key deletion safe across generations.

The primary abuse/availability risk is background catalog I/O multiplied by CN count. Page/rate/concurrency bounds constrain but do not eliminate that risk. The worker does not acquire the #27723 lifecycle locks, but its catalog reads/deletes can still contend transactionally with concurrent authorization DDL. Maintenance cannot take precedence over normal upgrade work.

## 11. Compatibility and lifecycle

### 11.1 Mixed version

New CNs may run maintenance while old CNs still lack #27745 and can create additional orphans. There is no permanent completion claim; later rings can repair rows created during rollout. Duplicate new-CN execution is idempotent. Normal version upgrades always retain priority.

### 11.2 Upgrade and downgrade

There is no catalog migration. Activation occurs only after the local service observes the final version complete. Downgrading/removing the code requires no data conversion; already deleted orphan rows remain validly absent. A downgraded old writer may create new historical rows again.

### 11.3 Concurrent DDL

Candidate, live-object probes, and DELETE share one tenant transaction. A row seen as live is preserved and can be reconsidered next round after concurrent DROP. A newly inserted grant behind the cursor or above high-water waits for a later ring. #27745 owns forward DROP/GRANT serialization; this maintenance does not replace it.

### 11.4 Backup, restore, and tenant recreation

Because no cursor/schema is persisted, account snapshots contain only existing privilege/catalog data. A restored or late-created tenant is discovered in a subsequent account round. Same-account restore during a tenant ring may place rows behind its current cursor; a later ring revisits them. A process restart loses progress but cannot publish a stale cursor into restored data.

## 12. Rollout, observability, stop, and removal

### 12.1 Rollout

There is no runtime feature flag. Rollout is ordinary binary rollout, with activation gated by final-version completion. The PR remains draft until this design is independently approved and implementation evidence matches the approved revision.

Before rollout, required evidence is:

- full owning-package UT and focused race checks;
- object-backed 10,036-row physical scan evidence using `inputRows` and `inputBlocks`;
- non-folded runtime-bound correctness regression;
- tenant isolation, rollback, late tenant, and snapshot restore regressions;
- CI BVT/UT/SCA completion.

### 12.2 Observability

Current observability intentionally reuses:

- `orphan object privilege maintenance failed` logs with attached transaction cause;
- existing transaction conflict, storage scan, CN I/O, memory, and logtail dashboards;
- bounded diagnostic SQL when an operator needs to sample remaining orphan rows.

There is no durable progress/completion metric because the design cannot truthfully report global completion. No tenant-ID metric label is added, avoiding unbounded cardinality. Absence of errors is not proof that all historical rows were visited.

### 12.3 Stop and rollback

Service context cancellation stops the owner. Emergency operational stop requires stopping/rolling back the affected CN binary; no dynamic disable switch is added. Code rollback is schema-free. Errors already rate-limit naturally to the periodic tick rather than retrying tightly.

### 12.4 Removal

This is recurring best-effort maintenance, not a one-time migration with a completion marker. Automatic removal is not scheduled. A future removal PR must establish that all supported source versions contain #27745, historical cleanup is no longer required by product policy, and operational evidence shows acceptable residual metadata. Removing the worker never requires catalog downgrade.

## 13. Standards and architectural precedent

No SQL or MySQL interoperability standard defines how an engine must repair historical authorization metadata; this is an internal maintenance protocol. The applicable precedents are:

- **keyset/seek pagination** rather than offset pagination, so healthy-process work advances by a stable ordered key;
- **high-water snapshots** for finite rounds, preventing concurrent append from extending an active traversal forever;
- **at-least-once idempotent maintenance**, where duplicate delivery is safe and transaction commit is the publication point;
- **lease/singleton or durable-checkpoint workers** for deterministic distributed progress.

The selected design follows the first three precedents but deliberately rejects the fourth to avoid persistent state and cluster ownership. That deviation is why it cannot claim deterministic restart continuation or cluster-wide rate control.

## 14. Alternatives

### A. Tenant-local durable cursor

Persist cursor, high-water, wrap state, and tenant generation in each tenant catalog, updating progress atomically with DELETE. This provides deterministic `O(N/1000)` continuation after restart.

Rejected for this PR by explicit owner decision: it adds catalog schema/state, upgrade and snapshot contracts, concurrent-CN CAS ownership, tenant recreation semantics, and a durable cleanup lifecycle.

### B. Cluster task framework/singleton task

Run cleanup through a cron/daemon task to cap aggregate workers and expose task status. Without a durable per-tenant cursor it still cannot guarantee restart continuation; with one it inherits Alternative A. It also couples historical authorization repair to task-service availability and task migration.

Rejected: higher persistent/distributed complexity than the accepted per-CN duplicate model. It remains the preferred alternative if a future requirement demands cluster-wide rate control.

### C. Operator-triggered cleanup

Provide an internal command/tool that an operator runs during a maintenance window. It can expose progress and explicit rate controls.

Rejected: late-created/restored tenants and mixed-version writers would require repeated manual operation; coverage depends on runbook compliance. It is a valid emergency fallback, not the default lifecycle.

### D. One-time version upgrade

Clean all tenants as a blocking or persisted upgrade step.

Rejected: large tenants make upgrade duration unbounded, every transaction still needs paging, and a durable “complete” state is unsafe while older CNs can write new orphans. Reusing `mo_upgrade_tenant.from_account_id` would also change an existing upgrade-task contract.

### E. Maintenance secondary index on `obj_id`

Add an index and delete by orphan object ID.

Rejected: the hidden index table changes catalog/snapshot restore behavior and previously caused `mo_role_privs` restore failures. It also does not by itself provide tenant/key restart progress.

### F. Markerless per-CN physical-key rings (selected)

Use the existing physical composite key, fixed pages, finite healthy-process rings, and duplicate idempotent CN execution.

Selected because it preserves schema/wire compatibility, repairs late data, bounds page mutation and candidate work, and is removable by code rollback. Accepted drawbacks are linear CN amplification, no global completion fact, and no finite-restart convergence guarantee.

## 15. Deterministic verification matrix

| Contract | Evidence |
|---|---|
| Exact orphan classification | v4.0.6 table-driven UT plus embedded live/global/legacy/hidden-child controls |
| At most 1,000 candidates | oversized executor-result negative UT and SQL `LIMIT 1000` shape assertion |
| Physical scan bound | 10,036-row object-backed `EXPLAIN ANALYZE`; compare `LIMIT 1/1000` Table Scan `inputRows/inputBlocks` |
| Literal-only reader limit | focused planner positive/negative UT plus two-block `current_user()` non-folded counterexample |
| Focused negative-test independence | fresh builder/scan/sort for rejection; mutation removing literal gate must fail the test |
| Finite healthy tenant ring | suffix/wrap/high-water/full-page UT with deterministic physical keys |
| Commit-only progress | transaction rollback/retry injection; state and data remain unchanged |
| Process restart limitation | service reconstruction reachability test plus documented absence of finite-restart claim |
| Finite/fair account rounds | frozen high-water, wrap, sparse account, failure advancement, and randomized-start UT |
| One owner per service | repeated pre-check lifecycle UT and per-pass CAS overlap suppression under `-race` |
| Multi-CN idempotency | exact-key DELETE/affected-row contract and transaction-conflict reasoning; CI integration evidence |
| Tenant isolation | cross-tenant equal-object-ID embedded regression |
| Restore and late tenant | same-account/cross-account snapshot restore and late-tenant embedded regressions |
| Mixed-version convergence | markerless recurring behavior plus #27745 compatibility analysis |
| Resource hygiene | result-close path review, focused race, `go vet`, `git diff --check` |

## 16. Decision log

Proposed decisions requiring independent approval:

1. Historical cleanup is recurring markerless maintenance, not a durable migration.
2. Healthy-process finite completion satisfies deterministic liveness; restart liveness is explicitly probabilistic with no finite count bound.
3. Every CN may duplicate work; exact tenant-local deletes and transaction conflicts provide safety.
4. Aggregate I/O scales linearly with CN count; one owner per CN and a 10-second tick are the only built-in rate controls.
5. No catalog index/table/cursor, task-framework dependency, feature flag, protocol, or completion marker is added.
6. The existing hidden composite primary key is the physical scan contract; generic ordered-limit pushdown is literal-only and rejects residual runtime expressions.
7. Existing logs/dashboards are sufficient for this best-effort worker; no global progress metric can be truthful without durable state.
8. Emergency stop is service cancellation/binary rollback; no runtime kill switch is introduced.

## 17. Open review and approval record

There are no deferred implementation choices in this v1 proposal. The following tradeoffs are intentionally exposed for approval rather than hidden as implementation details:

- probabilistic restart convergence versus a durable cursor;
- per-CN duplicate I/O versus task-framework singleton ownership;
- no dynamic disable/progress metric versus additional operational state.

Until an independent reviewer approves an exact Git revision of this document, implementation review remains blocked.

```text
Change scope: complete issue #27836 implementation in PR #28120
Trigger: >500 production lines + background lifecycle + authorization/tenant boundary + restart/retry + shared planner/storage contract
Design: this document v1; status PROPOSED; reviewed revision pending
Blocking findings: independent design review/approval not yet recorded
Decision log: sections 3-16
Decision: REQUEST_CHANGES (approval gate not yet satisfied)
Implementation deviations: to be checked after design approval
```
