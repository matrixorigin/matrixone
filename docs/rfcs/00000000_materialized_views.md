- Status: experimental P1 implemented and locally validated; release gates pending
- Start Date: 2026-10-12
- Authors: Codex, at the user's request
- Design revision: 2 (Git4Data and validation scope added)
- User approval: 2026-10-12, "去实现吧"; P1 implementation and validation authorized.
- Implementation PR: [#30085](https://github.com/matrixorigin/matrixone/pull/30085) (Draft, experimental P1)
- Issue for this RFC: [#30079](https://github.com/matrixorigin/matrixone/issues/30079)
- Code baseline: `dd38098853ea197699ed3fac3e69dc53975ac061`
- Publication baseline: `3e50e1b32d8da61bbbf70c89989fb08b671809de` (fresh main)
- Reference implementation: StarRocks tag `3.5.7`; file blob hashes recorded in
  [the source manifest](../design/materialized_view_reference_manifest.json)

# Materialized Views for MatrixOne

# Summary

Introduce a read-only, stored-query relation with explicit refresh semantics.
The first release supports native tables, manual refresh, bounded periodic
refresh, and direct SELECT. Each refresh replaces the complete stored result
and its coverage metadata in one MatrixOne transaction. Readers see the last
committed result, never a partially refreshed result.

Use StarRocks' separation of catalog object, refresh execution, coverage
tracking, and optimizer candidate preparation. Implement these contracts with
MatrixOne's existing SQL planner, transaction-scoped SQL executor, task service,
and native storage. Do not port StarRocks FE edit-log or partition-version
protocols into MO.

Transparent rewrite is a separate release. By default a base-table query must
retain its current transactional semantics. An asynchronous MV is not safe for
rewrite merely because its most recent refresh succeeded. Incremental refresh
and synchronous maintenance are also separate from the initial implementation.

The approved design below includes later-stage proposals. The P1 prototype now
implements the subset documented in the implementation notes and execution
report; its feature gate is disabled by default. P2/P3 remain proposals.

# Motivation and Current-Code Evidence

Repeated grouped reports and native-table joins redo the same work. Users can
manually maintain summary tables, but there is no native persistent MV contract
connecting definition, refresh atomicity, authorization, recovery, and rewrite.
No runtime performance claim is made by this RFC.

The inspected checkout has the following relevant boundaries:

| Existing code | Observed responsibility | Design consequence |
|---|---|---|
| `pkg/sql/parsers/dialect/mysql/mysql_sql.y::create_view_stmt`, `pkg/sql/parsers/tree/view.go` | Logical-view grammar and AST | Add distinct MV ASTs; do not reinterpret CREATE VIEW. |
| `pkg/sql/plan/build_ddl.go::inferViewColumns`, `genViewTableDef`, `buildCreateView` | Bound output columns, expanded stars, persisted SQL context | Reuse semantic binding and output inference, not logical-view storage metadata. |
| `pkg/sql/plan/view_dependency.go::ViewDependency` | Account, database, physical/logical relation identities, version, binding names, snapshots | Reuse identity vocabulary; names alone are insufficient. |
| `pkg/sql/compile/ddl.go::Scope.CreateView` | DDL locking, relation creation, dependency hooks | Add dedicated MV DDL dispatch and transaction ownership. |
| `pkg/sql/compile/view_metadata.go::ViewMetadataRefreshEnabled` | Returns false; ordinary-view recovery layer is inactive | MV correctness cannot depend on activating this separate feature. |
| `pkg/catalog/view_metadata.go` | Existing ordinary-view metadata cluster-table definitions | Add separate MV catalogs with account-qualified keys. |
| `pkg/util/executor/types.go::SQLExecutor.ExecTxn`, `TxnExecutor` | Multiple internal SQL statements in one transaction | Own full-refresh atomicity here. Close every executor Result. |
| `pkg/util/executor/options.go::WithTxnIsolation`, `WithTxnMode` | Explicit isolation/mode for internal transactions | Refresh must explicitly select SI; do not inherit RC defaults. |
| `pkg/taskservice/sql_task_executor.go::executeStatements` | Iterates statements using frontend InternalExecutor calls | Reuse task infrastructure, not a DELETE/INSERT SQLTask body assumed atomic. |
| `pkg/cnservice/server_task.go` | Registers task executors, including SQLTask and ISCP | Register one MV refresh executor with explicit capability admission. |
| `pkg/sql/plan/query_builder.go` | Main logical-query transformations, joins and index selection | Future rewrite must enter before irreversible physical/index choices. |
| `pkg/sql/plan/optimize.go::defaultRules`, `BaseOptimizer` | Current rule list is small; not a StarRocks-style memo optimizer | Do not claim adding one rule here imports SPJG rewrite. |
| `pkg/vm/engine/types.go::Relation.CollectChanges`, `pkg/iscp/types.go::DataRetriever.GetToTS` | Change collection and coverage timestamp vocabulary | Potential future incremental substrate; not an existing strict-freshness oracle. |
| `pkg/frontend/snapshot.go::doResolveSnapshotWithSnapshotName`, `planSnapshotFromRecord` | Named snapshot TS, tenant and scope resolution | Reuse this owner for a historical-refresh extension. |
| `pkg/frontend/databranchutils/lineage_publication_lock.go`, `branch_protect_snapshot.go` | Shared snapshot/lineage barriers, retention and reclaim | Recording a source TS alone does not reserve history. |
| `pkg/frontend/data_branch_hashdiff.go::hashDiff`, `hashDiffIfHasLCA`, `hashDiffIfNoLCA` | Lineage-aware and fallback branch comparisons | Offline validation/delta investigation, not a cheap query-time version counter. |
| `pkg/bootstrap/versions/` | Versioned catalog upgrades with protocol requirements | Add migrations in the next allocated version, not an old release directory. |

Baseline branch was `work`; the worktree was clean at task start. Local
`origin/main` and merge-base were
`23d8f61252f3017dff218b530817a240206fd762`. That remote ref was not fetched, so
it is not a freshness claim. The existing branch contains substantial unrelated
committed work. This RFC only reviews the task's new documentation delta.

# Goals, Stages, and Invariants

## Release boundaries

| Stage | Deliverable | Required gate |
|---|---|---|
| P1 | Native-table MVs; manual/periodic full refresh; direct SELECT; lifecycle and recovery | Atomic replacement, tenant/privilege checks, complete catalog/DDL consumer audit |
| P2 | Bounded single-table aggregate rewrite | Default strict semantics; complete coverage proof or explicit stale-read opt-in |
| P3 | Partition refresh and single-table incremental aggregates | Partition identity, deletes/updates, retained-history and checkpoint proof |
| P4 | Multi-table rewrite, external catalogs, MV nesting, broader incremental maintenance | Separate reviewed extensions and data-source-specific proofs |

P1 supports projection/filter queries, inner equijoins, and GROUP BY with
SUM/COUNT/MIN/MAX/AVG over native, persistent tables in the MV's own database
and account. It supports a scalar aggregate without GROUP BY, including the
one-row result for empty input. Full refresh uses the normal query semantics;
it does not need mergeable aggregate states. Require explicit, unique output
names; expand SELECT * at creation so future additions cannot change the output.

Reject external/system/temporary/cluster/subscription/historical relations,
logical views, other MVs, recursive CTEs, correlated subqueries, set operations,
windows, DISTINCT aggregates, LIMIT/OFFSET, locking reads, SELECT INTO, side
effects, user variables, and volatile or context-sensitive expressions in P1.
Allow deterministic built-in functions with a persisted semantic context.
Unknown determinism is a rejection, not an optimistic assumption. Reject UDFs
initially, including deterministic declarations that have no verified version
and dependency contract.

Creation is deferred: the relation exists but is unpopulated until first refresh.
Initial population is an explicit operation. This keeps large refresh failures
separate from DDL success and avoids a hidden long-running CREATE transaction.

## Required invariants

1. **Atomic publication:** stored rows, `populated`, successful refresh ID,
   source snapshot TS, and definition epoch become visible in one commit.
2. **Single source snapshot:** all base tables in a refresh are read at one
   transaction snapshot S, using the definition's captured semantic context.
3. **No freshness inflation:** S is the actual source snapshot, not refresh end
   time, task heartbeat time, or the publishing transaction's later wall clock.
4. **Last-good retention:** a failed, cancelled, timed-out, or aborted refresh
   cannot delete or replace the previously committed result.
5. **Identity safety:** account/database/logical relation identity, physical
   binding, schema version, and definition epoch are validated, not just names.
6. **Serialization:** at most one refresh transaction can publish a given MV;
   duplicate delivery cannot apply the same logical run twice.
7. **Read-only public relation:** users cannot INSERT/UPDATE/DELETE/TRUNCATE,
   rename, mutate indexes, or ALTER its stored result through table syntax.
8. **Strict default:** an unproven or stale MV never silently changes a normal
   base-table query's transactional result.
9. **Security:** every metadata key and task carries account identity; system
   catalog access is separate from source/target data access.
10. **Bounded work:** refresh, waiting, retry, history, and optimizer candidate
    enumeration have admission bounds and cancellation owners.

# StarRocks Implementation Reference

The reference is deliberately pinned to tag 3.5.7 rather than an evolving main
branch. Blob hashes identify the inspected contents; no Java source is copied
into production. The manifest records all eight inspected files.

| StarRocks source and inspected symbols | Adopt in MO | Do not copy into P1 |
|---|---|---|
| [MaterializedView.java](https://github.com/StarRocks/starrocks/blob/3.5.7/fe/fe-core/src/main/java/com/starrocks/catalog/MaterializedView.java), `MaterializedView extends OlapTable`, `AsyncRefreshContext`, `MvRefreshScheme` | A stored relation plus definition, activity and coverage metadata | Java object serialization and FE global catalog ownership |
| [MaterializedViewAnalyzer.java](https://github.com/StarRocks/starrocks/blob/3.5.7/fe/fe-core/src/main/java/com/starrocks/sql/analyzer/MaterializedViewAnalyzer.java) | Bind and validate definition before creating metadata | Assume all StarRocks syntax/table restrictions match MO |
| [TaskBuilder.java](https://github.com/StarRocks/starrocks/blob/3.5.7/fe/fe-core/src/main/java/com/starrocks/scheduler/TaskBuilder.java) | Derive refresh tasks from durable MV metadata | A second independent scheduler or SQL body as authority |
| [PartitionBasedMvRefreshProcessor.java](https://github.com/StarRocks/starrocks/blob/3.5.7/fe/fe-core/src/main/java/com/starrocks/scheduler/PartitionBasedMvRefreshProcessor.java), `doRefreshMaterializedView` | Capture dependencies, build a refresh query, execute, then acknowledge coverage | Its partition synchronization, retries, and separate metadata update ordering |
| [MVVersionManager.java](https://github.com/StarRocks/starrocks/blob/3.5.7/fe/fe-core/src/main/java/com/starrocks/scheduler/mv/MVVersionManager.java), `updateMVVersionInfo` | Track coverage separately from job status; a changed join dimension affects all relevant output | Treat MO schema versions as StarRocks partition data versions |
| [MvRewritePreprocessor.java](https://github.com/StarRocks/starrocks/blob/3.5.7/fe/fe-core/src/main/java/com/starrocks/sql/optimizer/MvRewritePreprocessor.java), `prepare` | Indexed dependency lookup, bounded candidates, plan and freshness checks | Unbounded catalog scans or an unnecessary parallel plan-preparation pool |
| [MaterializedViewRewriter.java](https://github.com/StarRocks/starrocks/blob/3.5.7/fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/materialization/MaterializedViewRewriter.java) | Bound expression matching and residual-predicate proof | Immediate outer-join, view-delta, query-delta, or union compensation support |
| [AggregatedMaterializedViewRewriter.java](https://github.com/StarRocks/starrocks/blob/3.5.7/fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/materialization/AggregatedMaterializedViewRewriter.java) | Separate aggregate rollup, projection mapping and HAVING handling | Blindly sum finalized AVG or DISTINCT results |

StarRocks is architectural precedent, not a mandatory compatibility standard.
MO's own MVCC, typed SQL results, tenant model, and failure semantics are the
controlling contracts. A physical columnar relation remains sufficient; an
Aggregate table model is not a prerequisite for this MV feature.

# Proposed SQL Contract

```sql
CREATE MATERIALIZED VIEW sales_daily
REFRESH MANUAL
AS SELECT order_day, region,
          SUM(amount) AS total_amount,
          COUNT(*) AS order_count
   FROM orders
   GROUP BY order_day, region;

-- Foreground operation; returns only after refresh commits or fails.
REFRESH MATERIALIZED VIEW sales_daily;

-- Separate example: periodic, full, asynchronous refresh.
CREATE MATERIALIZED VIEW sales_daily_scheduled
REFRESH EVERY INTERVAL 5 MINUTE
AS SELECT order_day, region, SUM(amount) AS total_amount
   FROM orders GROUP BY order_day, region;

SELECT * FROM sales_daily;
SHOW MATERIALIZED VIEWS;
SHOW CREATE MATERIALIZED VIEW sales_daily;
SHOW MATERIALIZED VIEW REFRESHES sales_daily;
ALTER MATERIALIZED VIEW sales_daily REFRESH MANUAL;
ALTER MATERIALIZED VIEW sales_daily REFRESH EVERY INTERVAL 10 MINUTE;
DROP MATERIALIZED VIEW IF EXISTS sales_daily;
```

P1 intervals are positive integral SECOND/MINUTE/HOUR/DAY values normalized to
seconds, minimum 60 seconds. Missed intervals are coalesced into one run, not
replayed individually. The interval is measured from scheduled due times;
overlapping runs are skipped/coalesced, never accumulated unboundedly.

An unpopulated MV returns `MV_NOT_POPULATED`, not an empty result that could be
mistaken for a completed empty refresh. A populated MV returns its stored,
possibly old result. SHOW exposes source snapshot TS, last successful finish
time, last attempt status, definition epoch, last error, and next due time.
Do not label it CURRENT merely because a refresh finished.

Reject CREATE OR REPLACE and definition-changing ALTER in P1. IF NOT EXISTS
does not replace existing definitions, schedules, or grants. Unsupported
object-kind operations produce an MV-specific error. Existing logical VIEW
syntax, expansion and grants keep their own semantics.

CREATE/ALTER/DROP/REFRESH are rejected inside an active user transaction in
P1. Direct SELECT is allowed within user transactions and uses their normal
snapshot visibility. A future rewrite is prohibited in transactions with
local writes or historical bindings until explicitly proven safe.

# Catalog and Relation Model

## Ownership

Store the MV itself as one native relation with a new reserved relkind, proposed
`SystemMaterializedViewRel = "m"`. Audit availability before allocating it.
Its columns and physical storage are ordinary native columns/storage; it is
not `SystemViewRel` and must not carry `ViewSql` that triggers logical-view
expansion. Its stable table ID survives every refresh.

Propose an additive `MaterializedViewDef` marker in `proto/plan.proto::TableDef`
containing catalog format and definition epoch. Allocate the next unused field
only during implementation. The authoritative query definition is the MV
catalog row, not a duplicated serialized logical plan. Stored bound signatures
are derived and versioned; they may be discarded and rebuilt.

Use four new system-owned cluster tables, following the account-qualified
catalog pattern. Only trusted code writes them. All access filters account ID.

| Table | Primary / secondary keys | Authoritative fields |
|---|---|---|
| `mo_catalog.mo_materialized_views` | PK `(account_id, mv_id)`; unique `(account_id, database_id, name)` | logical/physical identity, definition epoch, expanded canonical SELECT, default DB, SQL mode, timezone, identifier/collation semantics, output schema signature, owner user/role IDs, desired refresh policy/version, requested/successful sequence, populated, successful run ID, typed source-version descriptor with full source snapshot TS, last-success timestamp |
| `mo_catalog.mo_mv_dependencies` | PK `(account_id,mv_id,source_logical_id)`; reverse index `(account_id,source_database_id,source_logical_id,mv_id)` | source physical ID, logical ID, binding names, schema version, referenced column sequence numbers/types; checked definition epoch |
| `mo_catalog.mo_mv_refresh_runs` | PK `(account_id,mv_id,run_id)`; history index `(account_id,mv_id,started_at)` | queued/running/succeeded/failed/cancelled attempt status, expected definition epoch, attempt count, publisher transaction ID, source TS, bounded error code/message and timestamps |
| `mo_catalog.mo_mv_account_state` | PK `(account_id)` | dependency-admission guard and transactional MV-count quota; created during tenant admission and removed by account lifecycle |

Lifecycle metadata distinguishes `populated` from definition activity and run
status. A running or failed attempt does not hide last-good data. Derived
invalidity comes from current binding checks; a cached ACTIVE flag alone cannot
authorize a scan, refresh, or rewrite.

Each source is native and in the same account/database in P1. Namespace and
owner identity are resolved before metadata lookup. A clone/restore creates new
physical and catalog identities rather than inheriting a live worker's run ID.

# Refresh Execution and Atomicity

## Existing execution primitives and a necessary change

Implement a dedicated refresh service in a proposed `pkg/mv` owning package.
It depends on executor/engine/task abstractions; it must not import frontend or
sql/compile and create an import cycle. Inject live privilege and catalog
binding checks from their owners. Manual and scheduled refresh share this
service, rather than maintaining separate SQL paths.

Use `SQLExecutor.ExecTxn` with explicitly selected optimistic SI for the full
replacement transaction. Optimistic write-conflict detection arbitrates
concurrent publishers through one MV catalog row. A durable run ID and expected
definition epoch fence stale retries. Runtime admission reduces duplicates but
is never the correctness mechanism.

The data replacement is `DELETE` plus `INSERT ... SELECT` in one transaction,
with publication metadata written in that transaction. Do not use TRUNCATE,
DROP/CREATE, RENAME swaps, INSERT OVERWRITE assumptions, or independent SQLTask
statements. Those alternatives have different identity/DDL/transaction rules.

## Proposed execution sketch

```go
// Design pseudocode. These MV-specific APIs do not exist yet.
func Refresh(ctx context.Context, key MVKey, runID RunID, sequence, epoch uint64) error {
    admission := acquireBoundedRefreshSlot(ctx, key.AccountID)
    defer admission.Release()
    checkLiveOwnerAndSourcePrivileges(ctx, key)
    ensureAttemptRecorded(ctx, key, runID, sequence, epoch)

    return sqlExec.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
        definition := loadMVAtTxnSnapshot(tx, key)
        if definition.LastSuccessfulRun == runID { return nil }
        requireDefinitionEpoch(definition, epoch)
        requireNonObsoleteSequence(definition, sequence)
        validateStableBindingsAndOutputSchema(tx, definition)
        // This authoritative row is written by refresh and MV lifecycle DDL.
        // Conflicts cause the entire publication to abort, never partial data.
        guardPublicationRow(tx, key, epoch, sequence, runID)

        sourceTS := tx.Txn().SnapshotTS()
        capability := mintInternalWriteCapability(key, epoch, tx.Txn())
        deleteStoredRows(tx, definition, capability)
        insertFromBoundDefinition(tx, definition, capability)
        requireUnchangedSnapshot(tx, sourceTS)
        publishCoverageAndSuccessfulRun(tx, key, epoch, sequence, runID, sourceTS)
        return nil
    }, optsWithAccountAndSemanticContext.
        WithTxnIsolation(txn.TxnIsolation_SI).
        WithTxnMode(txn.TxnMode_Optimistic))
}
```

Every operation above propagates errors; each Result is closed immediately by
the owning helper. Catalog operations use explicit system-tenant statement
options while source/target operations use the MV account. All participate in
the same TxnOperator. No read/write runs with the executor's implicit default
system account. These are proposed APIs, not a compilable implementation.

`guardPublicationRow` performs a conditional update on the authoritative MV
row, scoped by `(account,mv,epoch)`. Its affected-row result is checked. Under
SI two publishers that started from the same version cannot both commit their
writes to that row. A conflicting attempt retries with a new transaction and
fresh S, at most twice, unless the same run already succeeded. Source row
updates do not require blocking user DML during a refresh: S intentionally
defines the refreshed historical result.

The creation/dependency and source-DDL races need an additional **account MV
dependency admission row**, a bounded catalog guard following existing catalog
locking conventions. CREATE/DROP MV and source schema/identity DDL acquire its
transactional lock before inspecting or changing dependency membership. It
serializes only relevant DDL, not user DML, SELECT or refresh computation.
This guard is owned by the new MV catalog, not the inactive ordinary-view
recovery gate. Lock order is account lifecycle -> database catalog -> account MV
dependency guard -> object catalog identity; all DROP DATABASE/ACCOUNT, restore
and MV paths must follow it. Do not acquire the guard later from a path that
already holds the reverse order. Implementation must audit all DDL entrants.

P1 rejects source ALTER/RENAME/DROP and schema-changing index operations while
dependent MVs exist. No automatic rebinding to a different output schema is
allowed. TRUNCATE/COPY/RESTORE that changes a source's physical identity is also
rejected with dependencies in P1; ordinary INSERT/UPDATE/DELETE remain allowed.
DROP DATABASE/ACCOUNT removes the whole same-database dependency closure and MV
objects transactionally through lifecycle cleanup, rather than being blocked
forever by objects it owns. Refresh's publication-row conflict and native schema
fences prevent it committing after a concurrent MV drop or policy epoch change.

If a privileged restore/recovery path cannot satisfy these checks, it must
invalidate the MV and pause scheduling, never leave an apparently valid binding
to a different source. Every direct scan revalidates bound source identities;
invalid definitions return `MV_INVALID_DEFINITION`. There is no name-only repair.

## Visibility and failure boundaries

Let S be the refresh source snapshot and C the replacement commit. A query
snapshot Q before C reads the previous MV rows and corresponding metadata;
a snapshot after C reads the new rows and corresponding metadata. MO's exact
timestamp comparison convention controls equality at C; tests must use that
convention rather than inventing `<=` visibility. New source commits after S
are not covered even if they occurred before C.

Run history is operational; `LastSuccessfulRun` and coverage on the MV row are
the success authority. The success history update is part of replacement commit.
After an ambiguous commit response, recover by the same run ID/transaction
outcome before retrying. A delayed worker may update an error only if that run
has not already committed successfully; it cannot overwrite success history.
A client cancellation after commit cannot undo success.

Before commit: error/cancel/timeout rolls back DELETE, INSERT and publication.
After commit: lost task acknowledgment is safe to replay, because the durable
run ID makes the duplicate a no-op. If a newer run already succeeded, the old
task becomes obsolete and cannot publish an older snapshot. Run requests carry
a monotonic requested sequence; publication rejects sequences below the last
successful sequence as well as mismatched definition epochs.

# Task Scheduling and Lifecycle

Reuse `pkg/taskservice` allocation, heartbeat and cron infrastructure. Allocate a
new versioned `TaskCode_MaterializedViewRefresh` and typed context with account,
MV ID, definition epoch, requested sequence and run ID. Do not allocate it by
reusing an existing TaskCode; numeric values must be checked at implementation.
Register an executor in `pkg/cnservice/server_task.go` only on capable CNs.

MV metadata is schedule authority. CREATE/ALTER writes desired schedule policy
in its DDL transaction; task creation is not a transactional catalog side effect.
A bounded reconciler, submitted through existing task infrastructure, makes
deterministically named cron tasks agree with committed desired policy/version.
Crash between catalog commit and task creation leaves a recoverable intent.
Queued task context contains identity, not a stale SQL body. DROP removes intent
and fences tasks through the missing identity/epoch; reconciler removes orphan
cron entries. Do not run one reconciler goroutine per MV.

Scheduled and manual requests obtain monotonic requested sequences via the same
catalog API. At most one queued/coalesced request per MV is maintained.
Execution admission is per CN; manual and scheduled requests can still race
across CNs. SI publication conflict and sequence fencing close that race rather
than a claimed cluster-wide local semaphore.
Lost allocation or heartbeat cancels the executor context;
transaction abort/conflict rules remain authoritative even when a worker ignores
the task-service liveness signal temporarily. CN shutdown cancels owned work,
waits within the existing bounded shutdown policy, and leaves durable tasks for
reallocation. Restart rebuilds schedules from catalog and checks transaction
outcomes before changing RUNNING rows to failed/abandoned.

# Authorization, Read-Only Enforcement, and Consumers

P1 uses conservative invoker security for explicit SELECT: the caller needs
SELECT on the MV and on referenced sources. This intentionally avoids a new
definer-rights data-sharing capability. CREATE requires CREATE VIEW authority in
the target DB and SELECT on sources; REFRESH/ALTER/DROP require ownership or DB
administrative authority. No new public privilege enum is required in P1.
SHOW follows the same object visibility policy; run errors are sanitized.

Scheduled refresh executes as the stored owner user/role IDs and checks current
existence, active account, role membership and source privileges each run.
Internal executor session overrides carry identity but must not be mistaken for
proof that a live privilege check happened. Owner deletion/revocation suspends
new refresh; privilege restoration can resume without replacing last-good rows.
Restored objects with unresolvable owners remain paused. Automatic rewrite never
expands the caller's access: it checks original-query and MV privileges.

The public write denial is enforced in planner DML, DDL kind checks, and compile
execution after re-resolution, including prepared, LOAD DATA, foreign-key
cascade, maintenance/index and restore paths. Refresh uses an unforgeable
process-internal capability bound to account/MV/epoch/TxnOperator and exact
target relation. It is not a session variable, SQL hint, public role, or general
"skip permissions" option. Engine direct writes must remain restricted to the
same trusted internal boundary; all internal relation writers are audited.

Consumer audit must include `SHOW TABLES`, SHOW CREATE, DESCRIBE,
information_schema TABLES/COLUMNS/VIEWS, grants, engine plan/definition conversion,
`mo-dump`, backup, restore, clone, publication/subscription, stats/ANALYZE, DROP
DATABASE/ACCOUNT and all object-kind switches. P1 reports MVs as BASE TABLE to
standard information_schema TABLES for MySQL compatibility, excludes them from
logical VIEWS, and exposes MV-specific metadata via SHOW. Publication and clone
of an MV are rejected initially. Logical export emits CREATE MV plus its refresh
policy; it does not replay data as user INSERTs into a read-only object.

# Git4Data Integration and Data-Version Semantics

Git4Data should supply version/snapshot identity, historical-read resolution,
retention lifecycle and branch validation. Do not invent a second time-travel
engine for MVs. A branch name alone is not a complete data version.

| Value | Meaning | Authoritative owner |
|---|---|---|
| `definition_epoch` | SELECT/output/security definition and P1 policy fence | MV DDL/catalog |
| `source_version` | Data read: LIVE_TS, or later resolved named/branch snapshot | Txn/snapshot/Git4Data; persisted by refresh |
| `successful_sequence` / `run_id` | Requested refresh that published | MV service; not a source-data version |
| publication commit C | When output became visible | Txn commit, never source TS or wall clock |
| W | Complete source-change observation frontier | Future engine/change consumer |

P1 stores a LIVE_TS descriptor with the full physical/logical source timestamp
and dependency identities. It does not create a named snapshot for every tick.
The active transaction protects its read snapshot during refresh; retaining S
after commit is provenance, not a source-history reservation. Record the
publishing transaction/run ID in the atomic publication. Obtain C from the
actual transaction outcome for diagnostics if needed; do not write an unknown
future commit TS into the transaction. Coverage needs S, not C.

Concrete uses and release placement:

1. **P1 provenance:** SHOW identifies the exact source version. Do not truncate
   arbitrary MVCC timestamps to PhysicalTime. Existing named-snapshot APIs may
   define a physical-only boundary: obey their owner semantics rather than
   assume that boundary round-trips every native timestamp.
2. **Branch experiments:** a native branch table can be treated as a separately
   bound source after its lineage/identity lifecycle is verified. Create the MV
   against that branch to compare definitions and results before a release.
   P1 never automatically retargets the MV to main or merges results. Unknown
   branch-owner kinds are rejected; merge/COPY/delete must obey the source-DDL
   fences. Git4Data remains owner of branch bookkeeping.
3. **P1.1 historical refresh:** a separately reviewed extension can add proposed
   `REFRESH MATERIALIZED VIEW mv AT SNAPSHOT 'baseline'`. All sources bind at
   one existing account/database-scope snapshot S; the current MV publishes at
   later C. This extension is manual-only and stores resolved snapshot identity,
   scope and name. Snapshot name reuse does not reuse identity. P1 does not yet
   admit this syntax or historical bindings.
4. **Output history:** MO snapshots/PITR can eventually read old MV result
   versions after object-kind consumers are extended. A source snapshot S and
   an output snapshot after C are different versions. Historical output reads
   validate identities at historical Q, not require sources to exist in the
   current catalog. P1 rejects historical MV reads until that contract is proven.
5. **P3 deltas and validation:** branch Diff can compare two pinned versions
   offline and help evaluate incremental maintenance. It can scan/spill and
   requires schema/key/lineage compatibility, so never run public DIFF for every
   optimizer candidate. Reuse typed lower-level change collection only after
   proving complete interval semantics. Pick/Merge cannot substitute for SUM
   retraction, deletion handling or a multi-table incremental join algorithm.

Historical refresh acquires an identity-scoped retention reference before
reading and holds it through source-read completion. A persistent definition
promising future reruns at a named snapshot needs a durable reference until that
promise is removed. The P1.1 extension chooses RESTRICT for DROP of a referenced
named snapshot. PITR shrink, lineage GC, restore, quota and cancellation must
consume the same reference contract. Do not mark MV retention as `kind='branch'`
or keep every routine P1 source timestamp indefinitely.

Registry catalog identity -> SNAPSHOT/lineage barrier -> MV dependency guard
is the additional outer lock order whenever historical/branch operations
participate. Use the snapshot owner's existing transaction-mode-specific
lock/write-fence selection. Expose a transaction-aware owner adapter rather than
import frontend helpers into pkg/mv or open unrelated BackgroundExec transactions.

A named snapshot proves stable historical input, not absence of changes in the
live table. An MV on branch B cannot rewrite a query on main A merely because
they share an ancestor or schema version. Match the full source-version
descriptor. Current-source strict rewrite still needs S-to-Q coverage proof;
historical/branch rewrite remains a separately reviewed P4 contract.

# Transparent Rewrite: Separate P2 Contract

## Safe candidate matching

Match bound logical expressions, not SQL strings. Fingerprints include stable
source identity, column sequence/type/scale/collation, function identity,
normalized predicates, GROUP BY keys and semantic SQL context. Hash equality is
only candidate lookup: compare the structures to prevent collisions. Prepared
parameters remain typed placeholders and are validated per execution.

Start with single-table Scan/Filter/Project/Aggregate plans. Require query
predicates equal to MV pre-aggregation predicates in the first rewrite closure;
later predicate containment requires a separate typed implication proof.
Residual filters can only reference available grouping/projection columns.
Apply HAVING after the reconstructed aggregate. No LIMIT push-through, outer
join changes, DISTINCT rollup, or union compensation in P2.

For aggregate rollup, query group keys must be a subset of MV group keys.
SUM merges sums; COUNT merges counts rather than counting MV rows; MIN/MAX
merge extrema. AVG requires independently stored SUM and COUNT of the same
nullable input and finalization with zero-count/NULL, precision, overflow and
rounding behavior matching MO. Never AVG finalized averages. Empty-input scalar
COUNT returns zero, not NULL from SUM of an empty MV. Start with exact numeric
types whose widened intermediate/final casts are proved equivalent; floating
SUM/AVG and reassociation that changes overflow/rounding remain excluded.

## Freshness proof

Keep three values distinct: source schema version, data coverage timestamp S,
and source completeness frontier W. Existing catalog DDL watermarks do not prove
absence of DML. An asynchronous change consumer with W < Q does not prove that
there were no changes before query snapshot Q.

Strict rewrite requires a visible published result at Q and an authoritative
proof that every source has identical query-relevant data between S and Q,
plus unchanged source identities/schema. Introduce a narrowly scoped proposed
`ProveNoCommittedChanges(source, S, Q)` engine capability returning
`UNCHANGED / CHANGED / UNKNOWN`; UNKNOWN falls back. Its contract includes
inserts, deletes, updates, identity changes, history GC, replay and complete
observation through Q. `CollectChanges`/ISCP are investigation inputs, not an
automatic implementation of this proof. The API's implementation needs a
separate reviewed change-coverage protocol before strict rewrite is enabled.
Do not add synchronous per-MV work to every base-table commit to fake freshness.

An optional session stale mode can be designed without that oracle: proposed
`mv_rewrite_max_staleness_seconds`, default 0 meaning **strict**, allows a
successful result only when S is not newer than Q and age derived from source
TS is within the explicit positive bound. This is snapshot-age permission, not
a guarantee about refresh finish time. EXPLAIN reports the chosen MV, S, bound,
matched shape, and rejection reason. P1 introduces neither rewrite mode.

Only rewrite standalone read-only SELECT with no local writes, historical
binding, external security context, locking clause, or unsupported construct.
Add a dedicated query-building pass before physical/index selection, with
coherent column-tag and node remapping. Preserve the original plan as fallback.
Check fresh coverage at every execution, including prepared execution; a cached
successful rewrite is not a reusable freshness authorization. Admission consumes
the current transaction's catalog read stamp and workspace identity.

## Cost and bounds

Reverse dependency lookup is indexed, account-scoped, and capped at 32 candidates
and 10 ms per query. These are proposed initial budgets, not benchmark findings.
Budget expiration leaves the original plan. There is no permanent global plan
cache in the first rewrite closure; statement-local descriptors are released on
completion. Cost comparison uses existing statistics but correctness never
depends on cost estimates. Unknown MV statistics use the original plan.

# Incremental and Partition Refresh Roadmap

Partition refresh is partition-change tracking and recomputation, not generally
row-delta maintenance. Define stable partition IDs/generations and a mapping from
source partitions to output groups before shipping it. A changed unpartitioned
join dimension may require refreshing every output partition; never advance its
coverage after only the first partition, as highlighted by MVVersionManager.

For a first single-table incremental aggregate implementation, bootstrap a full
result at S, retain source change history, then apply a gap-free interval with
the storage API's actual endpoint convention. Persist applied interval/epoch and
aggregate results in one transaction. Use `DataRetriever.GetToTS()` for the
applied source bound, not the consumer transaction's newer SnapshotTS.
Retry cannot double-count an interval; history loss fails closed and triggers a
full refresh. Retention reservations must precede releasing the bootstrap source
snapshot. Deletes/updates need before-images or sufficient retained row state.

COUNT/SUM can use signed deltas with proved numeric semantics; AVG stores
SUM/COUNT; NULL-only and disappearing groups need input-count state. MIN/MAX
cannot generally retract a deleted extremum without additional state or group
recomputation. DISTINCT/HLL/BITMAP and multi-table delta joins remain separate
features. On-commit synchronous refresh is not part of this proposal's first
releases because it increases base-write latency and transaction lock scope.

# Resource Model and Operational Defaults

P1 configuration defaults are proposals:

| Resource | Bound / owner |
|---|---|
| MVs per account | 128; catalog admission; configurable with maximum checked by admin |
| Sources per MV | 16; binder; repeated references deduplicated for dependency count |
| Definition SQL / columns | 64 KiB / 256 columns; binder before persistence |
| Active refreshes | 1/account/CN and 2/CN; cancellable admission; cross-CN duplicate computation may conflict and abort |
| Waiting requests | 1 coalesced request/MV; no interval backlog |
| Refresh timeout | 300 seconds; executor context and transaction cleanup |
| Transient transaction retries | 2; cancellation-aware, new transaction each attempt |
| Output budget | 512 MiB logical encoded output per attempt in initial P1; measured in writer before commit; failure rolls back |
| Spill | Reuse query memory/temporary-disk admission; no independent unlimited refresh pool |
| History | Latest 100 attempts/MV, max 7 days; bounded catalog cleanup using existing task infra |
| Error text | At most 1 KiB sanitized text; never connection secrets or raw sensitive rows |
| Reconciliation | Indexed pages of 128 intents, existing task infrastructure, cancellation between pages |

Full refresh costs O(source scan + old-result delete + new-result write).
MVCC temporarily retains old and new rows plus tombstones; compaction and
transaction memory are material costs. Timeout and encoded-output limits do not
alone bound total transaction workspace, so the existing memory/disk admissions
must account for it and reject before resource exhaustion. Long snapshots retain
history. User DML has no new per-row MV-maintenance path in P1; only dependency
DDL and scheduled background work add overhead. No performance improvement
percentage is asserted without a representative no-profile benchmark.

Use low-cardinality metrics: active/queued refreshes by outcome, refresh duration,
admission rejects, retries, and stale/invalid rewrite rejections. Per-object
diagnostics belong in bounded SHOW history, not metric labels with arbitrary
account/view/run names. Failed refreshes expose last-good S and a clear error.

# Compatibility, Upgrade, Backup and Rollback

Feature gate defaults off until all relevant CN/TN/catalog consumers support the
new format/relkind/task code. Creation/refresh use durable cluster capability
admission; a process-local switch alone is insufficient. Capability selection
must prevent old CNs from claiming new tasks and receiving MV plans through
distributed execution. New capability bits and protocol numbers are allocated
through existing owners during implementation, not fabricated in this RFC.

Install catalog tables through fresh bootstrap and a new idempotent upgrade
entry. Keep ordinary-view tables and their inactive gates unchanged. A crash
mid-upgrade is repairable; no MV object can be created before admission succeeds.
With MVs present, old binaries must not serve them as writable ordinary tables.
Downgrade requires disabling schedules/rewrite, draining refresh transactions,
dropping MVs or exporting results into explicitly created ordinary tables, then
passing a catalog preflight. An old binary need not understand MV metadata.

Physical backup includes result rows, definitions, dependencies and publication
state from a consistent snapshot. Restored scheduling starts paused; rebuild
bindings/owners under restored account/database/table identities, regenerate
dependency indexes, and require a new full refresh before enabling scheduled
runs or rewrite. Old in-flight task IDs are invalid. Direct stored-result reads
are allowed only after restored identities/privileges are validated, with their
original S marked historical. P1 table/database selective restore into live
dependent objects is rejected as described above; account/cluster restore uses
the paused recovery path. Backup and PITR restore must not resurrect workers.

Rollback of the feature while staying on capable binaries keeps direct stored
reads available but disables new CREATE/refresh/rewrite after draining work.
DROP remains available for cleanup. A failed initialization never publishes a
half-created relation without its metadata: relation, dependencies, grants and
desired schedule intent commit together or roll back together.

# Implementation Map and Reviewable Work Units

All new filenames and symbols are proposed. Existing paths below were inspected.

| Work unit | Existing files to change / proposed additions | Completion boundary |
|---|---|---|
| W1 grammar and definition | `pkg/sql/parsers/dialect/mysql/mysql_sql.y`, tree AST and lexer keyword maps; new `tree/materialized_view.go`; `pkg/sql/plan/build.go`, `build_ddl.go`; new `plan/materialized_view.go`; `proto/plan.proto` | Parse/format round-trip; definition rejects unsupported constructs; typed output and captured context |
| W2 catalog and capabilities | `pkg/catalog/types.go`; new `catalog/materialized_view.go`; `pkg/frontend/predefined.go`; new bootstrap version entries; engine plan/definition conversion and relkind consumers | Atomic CREATE/DROP; version/identity-qualified catalogs; old-reader exclusion |
| W3 DDL/security/read-only | `pkg/frontend/authenticate.go`, `mysql_cmd_executor.go`; `pkg/sql/compile/compile.go`, `ddl.go`; DML builders and table resolution in `query_builder.go`; `pkg/util/sysview/` | Public SQL, prepared, internal and restore boundaries enforce kind and privileges |
| W4 atomic refresh | new `pkg/mv/{types,catalog,refresh}.go`; existing `pkg/util/executor` transaction options | One source SI snapshot; conflict-safe all-or-nothing replacement; durable publication record |
| W5 scheduling/recovery | `proto/task.proto`, `pkg/cnservice/server_task.go`; task-service adapter and `pkg/mv/schedule.go` | Desired-policy reconciliation, bounded tasks, idempotent restart/cancel/drop |
| W6 lifecycle/export | bootstrap, backup/restore/clone/publication owners, SHOW and dump consumers | Complete same-account/database removal; paused restore; safe export/downgrade |
| W7 P2 only | new `pkg/sql/plan/mv_rewrite.go`, `query_builder.go`, frontend prepared-execution guards; later engine freshness proof API | Typed matching, explicit freshness permission, fallback, execution-time revalidation |
| W8 P3 only | `pkg/iscp`, engine change-history owners, future aggregate-state implementation | Bootstrap retention, gap-free delta apply, idempotent checkpoint and retraction |

W1-W6 together constitute P1. No half-feature ships behind public syntax without
its DDL, write denial, recovery and compatibility closure. A new protocol field
requires updating generated readers/writers and distributed plan consumers in
the same implementation unit. Exact IDs are assigned by those PRs.

# Acceptance and Counterexample Matrix

The [validation plan](../design/materialized_view_validation.md) maps StarRocks
documentation to MO SQL fixtures, independent result oracles and paired
performance experiments. Future implementation tests are requirements, not
results of this design task.
Use minimum data, fake clocks and injected transaction barriers, not sleeps.

| Contract | Small deterministic oracle | Layer |
|---|---|---|
| Syntax / errors | All proposed statements round-trip; ordinary VIEW unaffected; reject each unsupported shape | parser UT + SQL BVT |
| Bound definition | Expanded star, renamed alias, NULL/decimal/date output, SQL mode/timezone restored; duplicate names rejected | planner UT + BVT |
| Full-refresh correctness | Independent SELECT at captured S equals stored output, including inner join with duplicate matches, empty input and all-NULL aggregates | SQL integration |
| Atomic visibility | Pause after DELETE and after INSERT before commit; another session sees only old result; after commit sees complete new result | integration barriers |
| Failure rollback | Inject delete/insert/metadata-write/commit errors; old rows and successful run ID/S unchanged | executor UT + integration |
| Source snapshot | Commit source updates between refresh statements; all sources/results still reflect S under SI | txn integration |
| Duplicate/concurrent runs | Same run twice; two distinct runs conflict; newer sequence commits before old worker retries; old worker cannot regress coverage | UT + two-CN integration |
| Ambiguous commit | Drop acknowledgment after commit; restart resolves outcome by run ID and does not replace SUCCESS with FAILED | restart/fault integration |
| DDL closure | Race CREATE MV with source ALTER/DROP, MV DROP with refresh, DB/account removal with cron; name/physical-ID reuse rejected | deterministic multi-CN barriers |
| Read-only | INSERT, LOAD DATA, UPDATE, DELETE, TRUNCATE, generic ALTER/DROP TABLE, index changes, prepared and internal unauthorized attempts rejected | planner/compile UT + BVT |
| Tenant/auth | Identical names and colliding IDs in two tenants; role revoked/deleted; owner restored with different ID; catalog reads never cross accounts | security BVT |
| Schedule | Fake-clock missed ticks coalesce; crash after intent commit; task survives CN exit; DROP fences orphan tasks | task UT + restart |
| Admission/cleanup | Timeout/cancel during admission, scan, write, shutdown and retry; admission/Result/spill resources released exactly once | UT + race in owning closures |
| Upgrade/backup | Catalog upgrade twice, interrupted migration, mixed old/new CN exclusion, backup restore paused, downgrade with objects refused | focused compatibility integration |
| P2 shape | Positive exact match and rollup; nearer negatives: different predicate/type/collation, missing dimension, DISTINCT, HAVING ordering, AVG-of-AVG, NULL/empty COUNT | typed plan UT + differential SQL |
| P2 freshness | Source DML after S with no schema change, W<Q, history gap, prepared reexecution, local txn writes, stale opt-in age at boundary | engine/plan integration |
| P3 delta | Interval retry, delete extremum, changed group, NULL transition, history GC, join-dimension update | change-consumer UT + restart |
| Git4Data identity | Full TS boundary, snapshot name recreated with new identity, main/branch with same schema and different rows | version UT + differential SQL |
| P1.1 retention | Snapshot DROP/PITR shrink races refresh; restored owner unresolved; failure releases pin | lifecycle barriers + restart |
| Historical result extension | Source S differs from publication C; sources absent now but present at historical Q | snapshot SQL integration |

Performance evidence after correctness: same query with/without MV on repeated
reports; source-write latency with MV disabled versus P1 enabled; refresh memory,
temporary disk and compaction cost; optimizer time with zero/32 candidates.
Measure warm and cold cache separately. Candidate and output admission boundaries
must be observable; no success claim from a timed-out or partially selected run.

# Alternatives and Drawbacks

| Alternative | Evaluation |
|---|---|
| Keep user-managed summary table + scheduler | Lowest product work, but no native write protection, atomic refresh/recovery or rewrite contract; remains fallback |
| Encode MV as ordinary logical ViewSql | Reuses view binding, but logical expansion cannot provide stored results; inactive ordinary-view lifecycle is unrelated |
| Add Aggregate table model first | Useful for storage reduction but not required to materialize arbitrary SELECT results; adds independent ingestion/retraction semantics |
| Per-refresh shadow relation + pointer swap | Can reduce long replacement transactions later; adds physical-generation GC, prepared-plan rebinding, clone/restore and atomic publication protocol; defer |
| Per-commit synchronous MV maintenance | Strong freshness but increases base-write latency/locks and complicates multi-table semantics; defer |
| New standalone scheduler/cache framework | Duplicates taskservice lifecycle and unbounded state; reject |

Selected P1 deliberately pays full result DELETE/INSERT and MVCC retention cost
for a stable relation ID and one native transactional publication boundary. It
is bounded and best suited to moderate-sized repeated summaries initially. Large
fact-sized results or short refresh intervals need the later reviewed generation
or partition approach. Full refresh avoids incremental algebra complexity but
does not make large refreshes cheap. Defaulting to no automatic rewrite limits
initial acceleration to explicit MV queries; this protects existing SQL results.

# Design Gate, Decision Log and Open Work

The complete feature is R3: it crosses parser, planner, catalog, compiler,
transaction, task and authorization owners; adds persistent catalog format and
background work; changes distributed/recovery boundaries. The repository
G-FEATURE-DESIGN gate applies regardless of production line count. This change
is R0 documentation only. Runtime tests are not appropriate evidence for it.

Accepted draft decisions: read-only native relation; P1 deferred population;
manual/periodic full refresh; explicit optimistic SI transaction; stable relation
ID; conservative invoker reads; live owner checks; strict rewrite default;
separate P2/P3 contracts; existing task infrastructure; no activation of ordinary
view recovery; no new Aggregate storage model.

Design review revision 2 checks the listed ownership/atomicity/failure/security
contracts against inspected source. **Implementation approval remains pending**:
an author-written draft cannot substitute for catalog, transaction, frontend,
task-service and optimizer owner approval. Tracking issue/PR is unassigned.
No production implementation or push is included in this task.

Before P1 implementation is authorized, reviewers must accept the selected
optimistic-SI publication and DDL admission model and allocate the relkind/catalog
protocol/task capability. The first implementation unit must demonstrate engine
storage round-trip of the relkind, system/user statements sharing a TxnOperator,
and SI publication conflict behavior. Failure of any prerequisite requires
revising this design, not silently weakening atomicity or account checks.

P2 strict change-proof implementation, P3 source-history reservations, external
Catalog behavior, definer-only read sharing, and shadow-generation storage are
separate follow-up designs with named owning components; they are not P1 promises.
The source of the future version/protocol number is the active release process,
not a hardcoded number inferred from this snapshot.

### Implementation notes — 2026-10-12

The local worktree now contains an experimental implementation; it is not yet
release-validated. The feature is disabled by `experimental_materialized_view=0`.
The native relation stores a versioned JSON property and relkind `m`, using the
existing persisted property protocol rather than adding a TableDef protobuf
field. RPC admission is 110, bootstrap version 4.0.15 creates the four global
cluster catalogs, and the additive typed task executor is TaskCode 17.

CREATE defers population. A full refresh uses optimistic SI with one exact
source timestamp and atomically replaces rows plus successful coverage. The
write capability binds account, relation and transaction ID. Refresh writers
remain on the current CN so the shared 512 MiB batch-byte counter covers every
parallel writer. Captured timezone includes the offset for fixed zones; captured
SQL mode is applied to both parsing and binding. Source admission happens before
view expansion or connector resolution. Qualified/UDF-sensitive calls, sampling,
parameters, temporary/external/historical sources and nested MVs are rejected.

Bounds are constants in this prototype, not additional user configuration:
128 MVs/account, 16 sources, 256 columns, 64 KiB canonical query, 300-second
refresh timeout, two transient retries. Admission uses 256 fixed hash buckets,
one active and one waiting request per bucket, two active refreshes/CN. Account
hash collisions conservatively share admission. Reconciliation discovers 128
due/expired records per page. History maintenance visits 16 views and at most
256 terminal run IDs per view per invocation; 100 entries/7 days is a retention
target enforced by periodic cleanup, not a hard instantaneous count.

DROP DATABASE restricts while MVs exist. Generic target/source DDL and direct
DML are protected; MV ownership grants and drop grant cleanup use the native
catalog. Logical clone/restore/PITR containing MVs currently reject rather than
reconstructing identity-qualified definitions. Physical restart verification is
prepared but has not run. P2 transparent rewrite and P3 incremental maintenance
remain outside this implementation.

Local evidence: focused unit tests, owning package tests, targeted race repeats,
Go build, and incremental vet. Public SQL, multi-CN/upgrade/restart and the actual
performance benchmark remain required gates. The local service could not bind a
TCP socket in the execution sandbox; the permission request was interrupted.
No speedup measurement or end-to-end success is claimed.


### Runtime validation update — 2026-10-13

The network permission was granted. The isolated native service completed
66 main SQL checks, three graceful restart checks and five native failure/drop
checks. Full refresh, scheduled execution, source ACL revocation, same-name
accounts, concurrent readers/publishers and rollback on bigint overflow passed.
The four explicit-query benchmarks measured median ratios of 7.43× (aggregation),
8.89× (rollup), 5.36× (join) and 1.04× (high-cardinality control).
See [the execution report](../design/materialized_view_results.md) for samples,
plans, resource limits and remaining release/topology gates. Earlier statements
that runtime access was blocked describe the initial attempt, not current status.

Nine additional two-CN checks passed, bringing the SQL total to 83. They cover
cross-node visibility, independent refresh on the second CN, simultaneous
publishers, stable view IDs, nonregressing source timestamps and terminal history.
The performance measurements above were taken with one CN before adding the
second node. Mixed-version upgrade/downgrade and crash-at-commit remain untested.

### Upstream publication preparation — 2026-10-13

The feature tracker is [#30079](https://github.com/matrixorigin/matrixone/issues/30079).
The publication branch starts at freshly fetched main
`3e50e1b32d8da61bbbf70c89989fb08b671809de`, 64 commits after the prototype baseline.
Main now reserves protocol 110 for MEDIUMINT and 111 for IndexSearchScan; this
feature therefore uses protocol **112**. The earlier implementation notes and
runtime results describe the prototype's then-available protocol 110.
The definition/task format, full-refresh contract and experimental default-off
gate are unchanged. Source integration preserves upstream CDC initialization,
variadic DROP lifecycle admission and the planner's new return signature.
Affected verification is rerun for publication; the original observations remain
explicitly tied to their baseline rather than relabeled as new-base evidence.

Publication revalidation passed 84 native SQL checks, including an added escaped
source-name case. Metadata statements retain their own parser/literal rules;
only persisted source statements inherit authoring SQL mode and timezone. This
avoids backslash-name mismatches under NO_BACKSLASH_ESCAPES without relaxing any
source identity check. Configured incremental lint now passes. The issue and
Draft PR retain explicit remaining release gates and later-phase scope.
