# LOAD DATA Unique-Index Lock Ownership

- Status: mandatory design review pending
- Design revision: v3
- Supersedes: v1 at commit `30a0943ec3`; unapproved v2 draft
- Tracking issue: [matrixorigin/matrixone#27775](https://github.com/matrixorigin/matrixone/issues/27775)
- Implementation PR: [matrixorigin/matrixone#27814](https://github.com/matrixorigin/matrixone/pull/27814)
- Authors: XuPeng-SH
- Required reviewers: one SQL planner/compile owner and one lockservice owner
- Last updated: 2026-08-29

## 1. Decision summary

Optimize only the modern planner's existing lock shape for a deliberately
narrow class of `LOAD DATA` statements:

```text
pessimistic RC transaction
AND modern LOAD plan
AND large-load path (Query.LoadWriteS3=true)
AND one finite planner input-size estimate of at least 1 GiB
AND automatic statement retry is enabled
AND no prepared/session-cached logical generation is reused
AND ordinary, non-temporary, non-partitioned user base table
AND real primary key
AND physical base primary key is BIGINT or the canonical binary VARCHAR
    composite-primary-key column
AND no outgoing foreign keys
AND no incoming foreign-key references
AND at least one existing default/BTREE synchronous UNIQUE row-lock target
    whose hidden physical primary-key type is exactly T_int64
```

The planner keeps the exact logical lock shape it produces on `main`: one
Exclusive full-domain base-table target followed by precise Exclusive row
targets for writable regular UNIQUE hidden tables. It uses only the existing
`NotCacheable` policy bit for direct large LOADs. Compilation performs a
two-pass, fail-closed analysis of that existing modern `LOCK_OP`. For each promotable
hidden target, it copies only the physical table ID and primary-key type into
statement-local state, removes that exact target from the runtime `LockOp`, and
acquires one Exclusive full-domain hidden-table lock after the existing base
lock. Hidden targets that are not promotable remain exact per-row locks.

The acquisition order is:

```text
metadata locks
  -> existing general/base table locks
  -> promoted hidden UNIQUE tables by ascending physical table ID
  -> construct the coordinator HLC strict upper-bound fence
  -> wait for logtail and advance the RC snapshot past that bound
  -> record a compiler-local execution proof
  -> one physical retry with the same transaction-owned locks and proof
  -> runtime data-source initialization on the retried generation
  -> first input row enters the pipeline
```

After all ownership locks are held, one coordinator HLC strict upper-bound
fence acts as the RC freshness linearization point for both the base and
promoted hidden tables. Under the configured max-clock-offset contract, every
lock-aware writer that committed before the last acquisition has a commit
timestamp no greater than that bound. Advancing logtail and the transaction
snapshot strictly past it, then physically recompiling, closes both active-wait
and no-active-conflict windows without relying on owner-local
`tableCommittedAt` surviving a bind change. A compiler-local execution proof
carries the exact target vector and completed fence only across ordinary
physical retries in the same `Compile.Run`. The retried generation reuses the
now-held transaction locks, validates that proof, and initializes all sources
at the refreshed snapshot.

Planning-time file existence checks, metadata/statistics reads, and the existing
prefix read used for `IGNORE LINES` remain as on `main`; they are outside the
runtime ownership linearization claim.

The optimization does not change planner routing, `plan.proto`, generic table
lock semantics, the unsupported-DML fallback, FK plans, pessimistic SI,
optimistic transactions, or any hidden physical row-lock target other than
`T_int64`. It does not modify any generic table-range implementation. Every
other hidden target type, including FLOAT/DOUBLE and serialized composite keys,
stays on its precise row lock in this PR.

Revision v3 corrects eight earlier design mistakes:

1. v1/v2 did not preserve RC visibility when a direct hidden writer committed
   after LOAD's snapshot but before hidden full-lock acquisition, and an
   owner-local commit timestamp alone is not generation-stable;
2. planner-generated full-lock targets and fallback metadata changes were
   unnecessary because the modern plan already contains all exact physical
   row targets needed for a compiler-local transformation;
3. admitting every generic table-lock key type is not required to fix the
   BIGINT UNIQUE reproducer, so v3 admits only `T_int64` and retains every
   other hidden target's row locks;
4. `LoadWriteS3=true` alone is not a large-workload guarantee: it can include
   roughly 1 MiB inputs and small compressed files, for which one future-clock
   wait would be a material regression;
5. inferring a completed fence merely because the mutable transaction snapshot
   is newer than a plan snapshot is fragile. V3 carries an explicit,
   fail-closed execution proof instead;
6. relying on every generic base-table range was broader than the evidence.
   V3 admits only the issue fixtures' `T_int64` base PK and canonical binary
   `T_varchar` composite-PK encoding, each with an independent range oracle;
7. fencing once per definition-rebuilt logical generation left optimization
   wait/retry amplification unbounded under catalog churn. V3 permanently
   disables promotion for the remainder of a Run before any logical rebuild;
   and
8. a large LOAD's external object size can change outside catalog invalidation.
   V3 uses the existing non-cacheable bit at the same size boundary so a direct
   execution cannot inherit stale admission statistics or ownership state.

## 2. Problem and evidence

Issue #27775 reports a reproducible indexed-LOAD regression on the same 3-CN
TKE topology and data source:

| Case | Previous good main | Regressed main | Change |
| --- | ---: | ---: | ---: |
| No index, 100M | 82.506 s | 88.392 s | 1.071x |
| Composite PK + indexes, 100M | 124.730 s | 268.632 s | 2.154x |
| PK + indexes, 100M | 116.962 s | 253.823 s | 2.170x |
| Composite PK + indexes, 1B | 1,150.165 s | 2,782.444 s | 2.419x |

The no-index control moved only 7-9%, while indexed cases moved 115-142%.
Profiles correlate the regression with lock contention: mutex delay rose 3.12x
and `_LostContendedRuntimeLock` became 59.1% of mutex delay in the bad run.

The changed code path is mechanically consistent with that evidence:

1. a real-primary-key LOAD already acquires an Exclusive base-table range lock
   before the pipeline starts;
2. the modern path still encodes and submits Exclusive keys/ranges for each
   synchronously maintained regular UNIQUE hidden table on every batch;
3. large loads repeatedly rebuild ownership for the same physical hidden table
   and can cross the lock-row budget, causing owner-side coarsening and extra
   lockservice bookkeeping/contention.

The exact issue fixtures use ordinary, non-partitioned tables with no FK
relationships and one `UNIQUE BIGINT(col4)` key. One base PK is
`BIGINT(id)`; the other is composite `PRIMARY KEY(id BIGINT, col1 TINYINT)` and
therefore uses the canonical binary VARCHAR composite column. They are handled
by the modern planner, and the single-part hidden UNIQUE primary key resolves
to `types.T_int64`. The v3 base and hidden range restrictions therefore still
cover both reported 100M shapes and the composite 1B shape.

### 2.1 Freshness counterexample found during review

The existing per-key hidden row lock passes its key batch to `doLock`, so RC
can ask `PrimaryKeysMayBeModified` whether a matching key committed after the
snapshot and request statement retry. `LockTableWithMode` passes `bat=nil`;
the current `hasNewVersionInRange` immediately returns false for a nil batch.

The local lock table still returns its latest `tableCommittedAt` as
`Result.Timestamp` for a successful no-conflict range acquisition. Therefore
the following reachable order is unsafe if precise hidden row locks are simply
removed:

```text
T0: LOAD obtains RC snapshot
T1: direct hidden-table writer commits key K and releases its row lock
T2: LOAD acquires hidden full-domain lock with no active conflict
T3: generic table-lock path sees bat=nil, does not refresh, and returns success
T4: LOAD duplicate scan starts at T0 and can miss K
```

A deterministic diagnostic probe against the real local lockservice reproduced
this exact result: the writer unlocked at `snapshot.Next()`, the later
`LockTableWithMode` returned `nil`, and the expected `ErrTxnNeedRetry` assertion
failed. The permanent implementation test must first fail on exact `main` and
then pass through the LOAD-only freshness API. The diagnostic probe itself is
not committed as a test that asserts broken behavior.

Further review found that `tableCommittedAt` cannot be the correctness fence.
It is owner-local, is not carried by `LockTable` binding metadata, and a new
`localLockTable` initializes it from that owner's `clock.Now()`. Lockservice RPC
does not currently install the MORPC HLC codec header, so a bind change/restart
does not itself causally advance the new owner's HLC from the requesting CN.
The new value may also represent a fresh owner or committed no-op rather than a
data mutation. Treating it as a generation-stable mutation timestamp would be
an unsupported assumption.

V3 therefore does not use `Result.Timestamp`, `HasConflict`, `HasPrevCommit`,
or `tableCommittedAt` to decide LOAD data freshness. It first acquires every
required ownership lock with ordinary data-version handling deferred, then
takes the coordinator clock's upper-bound timestamp. Waiting for applied
logtail and updating the RC snapshot strictly past that bound gives one
statement-wide fence without a new wire field, owner-state migration, storage
scan, or per-target retry.

This remains a root-cause hypothesis for the performance endpoint until the
controlled 100M/1B evidence in section 11 passes. Eliminating coarsening is
mechanism evidence, not endpoint proof. Existing 5M measurements improve the
indexed/no-index ratio by only about 2.5% after normalization and cannot close
the performance claim.

## 3. Goals and non-goals

### Goals

- Eliminate input-row/batch-proportional UNIQUE lock encoding and submission
  for the issue's modern pessimistic-RC LOAD class.
- Preserve serial ownership of each physical hidden table whose precise row
  locks are removed.
- Preserve RC visibility for commits before acquisition, including the
  no-active-conflict window.
- Keep memory, lock requests, retries, and ordering work schema bounded.
- Preserve cancellation, rollback, retry/re-entry, plan-cache isolation, and
  compiler-pool behavior.
- Prove the performance endpoint on the original 100M reproducer before merge.

### Non-goals

- Changing LOAD planner routing or the unsupported-DML fallback.
- Changing or repairing FK LOAD planning, validation, or lock behavior.
- Optimizing pessimistic SI or optimistic transactions.
- Optimizing temporary, partitioned, system, external, subscription, or direct
  physical hidden-table LOAD targets.
- Fencing referenced parents or changing disjoint-key FK concurrency.
- Optimizing secondary non-UNIQUE, asynchronous, cron-maintained, or irregular
  index families.
- Optimizing a base physical PK shape other than issue #27775's `T_int64` or
  canonical binary `T_varchar` composite-PK column.
- Promoting any hidden physical key type other than `T_int64`, or correcting
  generic table-lock endpoints for those types in this PR.
- Redesigning generic table-lock freshness, lock escalation, lockservice wire
  compatibility, or intention locking.
- Adding a new public SQL error class.

## 4. Eligibility contract

Eligibility is evaluated in compile against the exact modern plan before any
target is removed. The transformation activates only when every statement-level
condition below is true:

1. `Query.LoadTag` and `Compile.stmt` both identify the same LOAD, its
   duplicate mode is the default error mode rather than LOAD IGNORE/REPLACE,
   it has no multi-account `Accounts` target, and the current transaction is
   both pessimistic and RC with a non-empty transaction ID.
2. `Query.LoadWriteS3 == true`, `Compile.disableRetry == false`,
   `Compile.isPrepare == false`, and `Compile.planGenerationReused == false`.
   The reachable graph has exactly one LOAD `EXTERNAL_SCAN`; its `Stats.Cost`
   and `Stats.Rowsize` are finite and positive, and their overflow-checked
   product is at least the shared planner/compiler constant
   `plan.LoadUniqueLockPromotionMinEstimatedBytes = 1 << 30`. For current LOAD
   planning that product approximates known input bytes. Unknown-size, smaller,
   prepare-time and reused prepared/session-cache generations keep exact `main`
   behavior so the
   mandatory upper-bound wait and physical retry cannot regress
   latency-sensitive statements or inherit proof from another transaction. A
   freshly rebuilt prepared generation has `planGenerationReused=false` and is
   intentionally treated like any other fresh logical plan; admission depends
   on generation freshness, not on SQL protocol origin.
3. The `LOCK_OP` has a canonical non-nil `Node.TableDef` and one existing base
   target with `LockTable=true`, effective Exclusive mode, and a non-zero
   `TableId` equal to both `Node.TableDef.TblId` and its `ObjRef.Obj`. That
   target has `LockTableAtTheEnd=false`,
   `Block=false`, `HasPartitionCol=false`, and `LockRows=nil`.
4. The resolved base `ObjRef` is non-nil, has no `PubInfo`, does not opt out of
   metadata locking, names a non-system database, and has an object ID matching
   the base target and `TableDef`; the base is not a catalog system table.
5. `TableDef.TableType == catalog.SystemOrdinaryRel`,
   `TableDef.IsTemporary == false`, `TableDef.FeatureFlag == 0`, and
   `TableDef.Partition == nil`. The zero feature flag deliberately rejects an
   index table, partitioned parent/child, and every external-table subtype.
6. `TableDef.Pkey` exists and is not the fake primary key. The base target's
   key type exactly matches the authoritative physical PK column. It is either
   `types.T_int64`, or it is the canonical
   `catalog.CPrimaryKeyColName`/`CompPkeyCol` with `types.T_varchar`,
   `types.MaxVarcharLen`, zero scale, and binary charset. An ordinary
   user-declared VARCHAR PK and every other base physical type are ineligible.
7. `len(TableDef.Fkeys) == 0` and `len(TableDef.RefChildTbls) == 0`, regardless
   of the current `foreign_key_checks` value.
8. Every `TableDef.Indexes` entry has `TableExist=true`, an empty/default or
   BTREE algorithm, an empty algorithm-table subtype, and a successful false
   result from `indexplugin.IsAsync`. RTree, fulltext, vector, master, malformed
   async parameters, and every separate post-DML/cron maintenance topology
   disable the transform.
9. The reachable statement graph contains exactly one candidate ownership
   `LOCK_OP`; its copied base target is the statement's only pre-pipeline data
   table lock. Any additional full-table target or general data-lock source
   disables the optimization, keeping the ownership order and the number of
   optimization-induced retries independently auditable.
10. Every non-base lock target is recognized as an existing regular UNIQUE
   index target from this `TableDef`; an unknown target disables the whole
   transformation.
11. At least one recognized hidden target is promotable.

A recognized hidden target must satisfy all of these:

- it is an existing precise row target with both `LockTable=false` and
  `LockTableAtTheEnd=false`, `Block=false`, `HasPartitionCol=false`, and
  `LockRows=nil`;
- its effective mode is Exclusive;
- its `ObjRef`, physical table ID, object ID, and key type are non-zero and
  internally consistent;
- its object name equals exactly one `IndexDef.IndexTableName` with
  `Unique=true` and the statement-wide default/BTREE synchronous checks above;
- it maps exactly to one `MULTI_UPDATE.UpdateCtx` whose `ObjRef` and
  `TableDef.TblId` match the target. The hidden `TableDef` is the authoritative
  source of the physical primary-key type and metadata-lock identity; its
  primary-key definition and column must both exist and must not be fake;
- the target and matching update-context `ObjRef` both have `PubInfo=nil` and
  `NotLockMeta=false`, and agree on tenant, schema, object name, and object ID;
- it is already present in the modern planner's lock target list. Absence is
  authoritative for the existing static-NULL proof; compile does not recreate
  an omitted target;
- its lock-target key type exactly matches the hidden primary-key column type,
  including OID, width, scale, and charset; and
- that physical type is exactly `types.T_int64`.

There is no open-ended allowlist in v3. A recognized target whose hidden key is
not `T_int64` remains in the runtime `LockOp` with its exact `main` row-lock
behavior. In particular, scalar FLOAT32/FLOAT64 and composite keys represented
by a serialized byte/string physical key are not promoted.

The analysis is query-wide, two-pass, and atomic from the plan's perspective:

1. walk only nodes reachable from `Query.Steps`, reject invalid child indexes
   and cycles, and classify every reachable `LOCK_OP`, mutation context, and
   target without mutating a node or compiler lock state;
2. only after the complete positive decision, register value copies and build
   a filtered physical `LockOp` target list.

Missing metadata, an unsupported future lock shape, an unexpected extra
target, an identity mismatch, or contradictory duplicate metadata disables the
optimization and preserves the exact `main` target list. The intended candidate
set may be a type-selected subset (for example, promote `T_int64` while keeping
FLOAT row scoped), but that complete classified set is installed atomically or
not at all. A late validation failure cannot leave an earlier target promoted,
and ineligibility introduces no new user-visible internal error.

## 5. Required invariants

### 5.1 Safety

1. Before an optimized LOAD initializes a runtime data source or admits its
   first input row, the transaction owns an Exclusive full-domain lock for the
   base and every physical hidden table whose runtime row target was removed.
2. A row target is removed only for the exact validated physical table ID and
   key type copied into the promoted set.
3. After the final ownership lock is acquired, the coordinator constructs a
   non-empty HLC strict upper-bound fence. Under the configured max-offset
   contract, every lock-aware commit that completed before acquisition is no
   later than that bound. Logtail application and `UpdateSnapshot` produce a
   snapshot strictly greater than it before any source can initialize.
4. The no-active-writer order in section 2.1 and owner rebind/restart are
   covered. Freshness does not depend on `Result.Timestamp`, `HasConflict`,
   `HasPrevCommit`, or an owner-local timestamp surviving generations.
5. No Shared full-domain target is created by this optimization.
6. Small, retry-disabled, prepare-time/reused-plan, SI, optimistic, fallback, FK-related,
   temporary, partitioned, fake-PK, direct system/internal-target, and ordinary
   non-LOAD statements retain exact `main` row/table lock targets.
7. A statically NULL UNIQUE target stays absent only because the modern planner
   already omitted it. Compile neither reruns nor broadens that proof.
8. The existing base full-domain endpoint is admitted only for encoded
   `T_int64`, or canonical binary composite-PK `T_varchar` from encoded empty
   through `EncodeStringTypeMax`. The promoted hidden endpoint covers the
   complete encoded `T_int64` domain, including `math.MinInt64` and
   `math.MaxInt64`. Every other base shape is ineligible and every other hidden
   physical key type retains row locks.
9. Duplicate discovery of one identical physical target produces one
   Exclusive request. Different physical IDs are never coalesced.
10. The canonical logical plan and target flags are not rewritten by the
    optimization. Prepared execution, ordinary retry compilation, and an
    isolation-mode change cannot inherit a previous execution's filtered list.
11. Existing metadata locks cover the base and every promoted hidden mutation
    object before physical locks are acquired. Missing coverage disables the
    transformation.
12. Only the top-level coordinator compile owns and acquires the promoted slice.
    Remote fragments receive neither a second promoted request nor private
    freshness state.
13. A physical compile generation built before a freshness update is never
    reused. This includes relation handles, remote-scan tombstones, source
    parameters, and remote pipeline payloads derived from the old snapshot.
14. Snapshot ordering alone is never treated as proof that a fence completed.
    Admission after retry requires the same compiler-local execution-proof
    record, exact value-equal ownership target vector, transaction ID,
    non-empty fence, a later physical execution generation, and a current
    snapshot strictly greater than that fence. A logical-plan rebuild and a
    prepare-time/reused-plan execution cannot inherit this record.

The ownership linearization point for one target is successful completion of
its lockservice full-domain acquisition. Statement-wide ownership is complete
after the last target succeeds. The visibility linearization point is later:
application of the post-acquisition coordinator HLC fence to the RC
snapshot. Runtime source initialization is admitted only on the physically
rebuilt generation after both points are complete.

### 5.2 Liveness

1. Existing general/base locks keep exact `main` ordering. The LOAD-only hidden
   order is ascending physical table ID.
2. Current ordinary INSERT/UPDATE/DELETE/REPLACE plans for the eligible schema
   reach the base namespace before hidden row locks or writes; this precondition
   is frozen by modern and legacy plan-order tests.
3. One `Compile.Run` emits at most one optimization-induced ordinary data-retry
   signal, after base and all promoted hidden locks are acquired and the global
   upper-bound fence succeeds. An ordinary physical retry inherits the same
   local proof, validates its fence, and does not emit it again. Any logical-plan
   rebuild sets a root-owned sticky disable bit before rebuilding; that Run then
   uses exact `main` lock targets and can never construct another optimization
   fence, even if the rebuilt plan would otherwise qualify.
4. A wait terminates through lock acquisition, statement-context cancellation,
   lock timeout, transaction/deadlock handling, or transaction termination.
5. Failure after acquiring a prefix returns the substantive error. Locks remain
   transaction-owned and are released exactly once by commit/rollback rather
   than independently by the compiler.
6. A freshness retry retains acquired locks under the existing transaction,
   rolls back only the failed statement workspace generation, and re-enters
   with a refreshed snapshot.
7. Compiler pooling and retry-compile release clear every retained LOAD value
   before reuse.
8. Writes from earlier statements in the same explicit transaction remain
   visible through the workspace and are not rolled back by the LOAD freshness
   transition; reentrant row-to-range ownership cannot wait on itself.

This design does not claim a global cross-statement lock order. A transaction
that retained a direct physical-table lock from an earlier statement can still
form a cycle; existing deadlock detection remains the terminal owner.

### 5.3 Boundedness

For one optimized statement:

```text
promoted target count <= recognized regular UNIQUE row-target count
compiler memory       = O(plan nodes + index defs + update contexts + lock targets)
ordering work         = O(promoted target count log promoted target count)
new lock rows          = one encoded range pair per promoted target
ownership requests     = one base request + promoted target count
fence proof            = one exact schema-bounded target vector
new goroutines         = 0
new logging sites      = 0
HLC fence waits        <= 1 per Compile.Run
optimization retries   <= 1 per Compile.Run
```

The retained slice is schema bounded and recycled with the compiler. No input
row, batch, file size, retry count, or transaction duration can grow it. A
recognized target that remains row scoped keeps its existing input-dependent
cost; the issue fixture has no such target.

The no-index and ineligible fast paths perform one bounded reachable-plan scan
plus schema-index checks. They return before allocating candidate maps or
sorting slices when there is no synchronous UNIQUE definition or no precise
UNIQUE lock target. They add no lock request, log, metric label, storage lookup,
or data-source work.

## 6. Planner, compiler, and freshness contract

### 6.1 Planner and session-plan cache

The logical lock plan is unchanged. The planner still emits the exact base and
hidden row targets present on `main`; it adds no lock marker or protobuf field.

For modern LOAD, exact `main` already provides all required evidence in one
`LOCK_OP`:

- canonical base `Node.TableDef`;
- one base target marked for full-table acquisition;
- one resolved precise target for each writable UNIQUE hidden table not removed
  by static-NULL analysis;
- physical IDs, object references, batch expressions, and key types;
- matching `MULTI_UPDATE.UpdateCtx` entries with the authoritative hidden
  `TableDef` and mutation identity.

The unsupported-DML fallback is not enhanced and remains exact `main`. No
shared LOAD target builder, fallback `Node.TableDef` population, FK rerouting,
or new lock-plan field is introduced.

One execution-lifecycle policy does change: a LOAD external scan whose finite,
positive `Stats.Cost * Stats.Rowsize` reaches the same 1 GiB threshold is marked
with the existing `Node.NotCacheable` flag. The input object and its byte
estimate can change independently of MatrixOne catalog publication, and the
session cache retains both the old estimate and a plan that compilation may
otherwise specialize. Replanning a >=1 GiB LOAD is negligible relative to its
runtime and guarantees that every direct execution revalidates the size gate
and receives fresh statement-local ownership state. Smaller LOADs retain the
existing cache policy. `Compile.planGenerationReused` remains a fail-closed
defense for an already-admitted legacy cache entry and reused prepared plans.
Planner marking and compiler admission call one shared, overflow/NaN/Inf-safe
estimate helper and constant; they cannot drift into different boundaries.

### 6.2 Compiler

The top-level coordinator compile runs the query-wide two-pass predicate in
section 4 before physical node compilation. On a positive decision it:

1. records the sole candidate `LOCK_OP` identity and places a deep value copy
   of the base target in the existing `Compile.lockTables` path, marked as the
   LOAD ownership base, without calling the mutating
   `shouldPrePipelineLockTable` helper on the canonical target;
2. verifies each promoted target against its unique mutation context and copies
   the authoritative hidden primary-key type into a dedicated statement-local
   `loadIndexLockTables` value slice containing only table ID and physical key
   type;
3. sorts that slice by ascending physical table ID and coalesces only identical
   ID/type pairs;
4. when compiling that exact node, deep-copies each retained target and builds
   a stack-local/shallow node copy whose target slice excludes the copied base
   target and promoted hidden targets; `constructLockOp` consumes this copy;
5. leaves every non-promoted hidden row target and unrelated target unchanged;
6. never serializes the promoted slice into a remote fragment;
7. creates one coordinator-local `loadOwnershipGeneration` containing an exact
   value copy of the transaction ID and sorted base/promoted target vector; and
8. clear-and-nils compiler-owned value slices and nils the generation pointer
   on `Reset`, retry-compile release, normal release, and pool return; only the
   root terminal release makes its proof unreachable, while a borrower never
   clears shared contents. The sticky disable bit is also reset before any new
   execution. A pooled compiler does not retain a large backing array from a
   previous schema.

Neither the existing base-target removal nor the optimization is allowed to
assign to the canonical `Node.LockTargets`, mutate `LockTableAtTheEnd`, or store
a canonical target pointer in compiler state on this eligible path. The next
data retry therefore reclassifies the same complete logical plan; a prepared
execution in a different transaction/isolation mode sees the original targets.
All non-eligible statements keep exact `main` compilation behavior.

`prePipelineInitializer` retains existing metadata and general table-lock
steps. For an eligible statement, the candidate base is the only data entry in
`lockTables`; its acquisition and every promoted hidden acquisition use the
deferred-freshness helper in section 6.3. The base remains first and hidden IDs
remain ascending. The general map/comparator is not broadened into logical lock
groups, and ineligible statements retain its exact behavior.

The generation object is shared only by the root coordinator and ordinary
physical retry compiles for this `Compile.Run`. It is not a hash and has no
collision assumption: each retry classifier compares its complete sorted
value vector with the stored vector. Only the coordinator pre-run mutates its
fence fields, after the previous physical attempt is quiescent.

After the full set is owned, pre-run follows an explicit state machine:

- with an empty stored fence, construct the coordinator clock's strict
  upper-bound fence, call `UpdateSnapshot` through the logtail timestamp waiter,
  require the installed transaction snapshot to be strictly greater, then
  store the fence and current physical execution generation before returning
  one existing `ErrTxnNeedRetry`;
- with a non-empty stored fence, continue to source initialization only if the
  transaction ID and exact target vector still match, the current physical
  execution generation is strictly later than the recorded generation, and
  the current transaction snapshot is strictly greater than the stored fence;
- any partial, mismatched, equal/older, or same-generation state is a contract
  error. It cannot trigger another fence or admit execution.

`buildRetryCompile(false)` inherits the same generation object before
classification; the classifier must compare and bind that object rather than
replace it. Before any `buildRetryCompile(true)`, the root sets the sticky
optimization-disable bit. A successful rebuild publishes the new logical plan
with a nil ownership object and exact `main` targets; a failed rebuild is
terminal. Releasing a retry compile only nils its pointer; it never clears a
shared object still owned by the root compile. Prepare-time/reused-plan
executions never create the object, and remote fragments never receive it.

### 6.3 Deferred ownership acquisition and global HLC fence

Generic `LockTableWithMode` keeps its current behavior. The cross-package entry
point is deliberately narrow and always requests an Exclusive, non-DDL table
lock:

```go
func LockTableForLoadOwnership(
    eng engine.Engine,
    proc *process.Process,
    tableID uint64,
    pkType types.Type,
) (definitionChanged bool, err error)
```

It rejects a non-pessimistic-RC transaction. The compiler separately guarantees
that promoted hidden targets are `T_int64` and the copied base target is either
`T_int64` or the canonical binary composite-PK `T_varchar`; each retains the
same physical range it uses on `main`. The helper calls existing
`doLock` with `bat=nil`, `LockTable=true`, `changeDef=false`, and one new local
`LockOptions.deferDataRefreshToLoadFence` bit. That bit takes effect only after a
successful lock and existing definition-change detection: ordinary data
freshness branches return without consulting `Result.Timestamp`, scanning
storage, or updating the snapshot. Cancellation, timeout, deadlock, bind retry,
transaction failure, and definition-change behavior remain substantive and
unchanged. The option is process-local; no protobuf, RPC, lock mode, or owner
state changes.

After the base and every promoted hidden lock succeed, the compiler calls a new
shared clock helper:

```go
// package clock
func NowUpperBoundFence(c Clock) (timestamp.Timestamp, bool)
```

The helper rejects a nil clock or negative `Clock.MaxOffset()`, obtains
`Clock.Now()`'s upper bound, rejects negative physical time and
`math.MaxInt64`, then increments the physical time by one and sets logical time
to zero. Moving to the next physical tick is essential: the raw upper bound's
logical component is zero and does not dominate larger logical timestamps at
the same physical tick. Lock allocator's existing `newFenceTS` is refactored to
call this helper with equivalent behavior for every valid clock and fail-closed
behavior for invalid clocks, giving both users one overflow-tested
implementation.

Under the configured HLC max-offset contract, the resulting fence is later
than every cluster timestamp generated before this post-acquisition call. A
lock-aware writer releases its lock only with its terminal commit/rollback
outcome: `txnOperator.doWrite(commit=true)` registers unlock as a defer around
the commit RPC/result path, unknown commits transfer lock release to the
resolver, and RC commit waits for its own applied logtail before unlock. An SI
writer may unlock before its logtail is applied, so correctness does not rely
only on the RC optimization: its terminal commit timestamp is still bounded by
the later HLC fence. Thus every relevant lock-aware commit is bounded by the
fence and no later writer can enter the owned ranges. This ordering is frozen
by commit/unlock barrier tests for commit, rollback, and unknown resolution.

The compiler calls `txnOp.UpdateSnapshot(ctx, fence)`. The RC timestamp waiter
must wait until logtail is applied through the fence and install a snapshot
strictly greater than it. Empty/equal/older results fail the statement. The
compiler records the completed fence in its coordinator-local generation
object and then returns one existing `ErrTxnNeedRetry`; it does not initialize
a source or write a LOAD row.

Progress does not depend on user commits. TN creates heartbeat transactions at
the existing 2 ms cadence; although `OnEndPrePrepare` excludes their empty
stores from the data-tail table, `OnEndPrepareWAL` still enqueues them and the
ordered logtail publisher advances `To` with their PrepareTS. CN applies empty
update responses, coalesced by the configured logtail collection/progress
interval, and advances the global timestamp waiter only after every consumer
routine reaches that timestamp. A transport-level idle heartbeat only repeats
`sentThrough` and is not counted as progress. With no user writes, the fence
wait is therefore bounded by clock convergence plus TN heartbeat, progress
interval, network, queue, and apply latency. Because the coordinator fence is
its local HLC plus `MaxOffset`, while a TN wall clock may legally trail that CN
by `MaxOffset`, the worst skew-only component is approximately
`2 * MaxOffset`, not one offset. If TN
heartbeat or logtail delivery is unavailable, the wait ends only through the
existing statement context/client-close path; it does not create a goroutine or
private timer.

The retry is required even though runtime readers have not been initialized.
Physical compilation can already resolve relations and attach remote-scan
tombstones through `generateNodes` at the old snapshot. Rebuilding the physical
compile discards that generation and regenerates all relation, tombstone,
source, and remote-payload state. Continuing in place is not allowed.

On the data retry, base and hidden acquisitions are reentrant under the same
transaction. Section 6.2 admits source initialization only through the shared
local generation proof and its strict snapshot check, without a second HLC
wait. No owner-local commit timestamp or transported proof is used. Any
logical-plan rebuild instead activates the sticky exact-main fallback for the
remainder of this Run.

The permanent deterministic matrix must cover:

- no competing writer: one HLC-fence wait/retry, then reentrant execution;
- commit before full-lock request, with no active conflict;
- active base or hidden writer commit after the LOAD waits;
- active writer rollback;
- multiple hidden targets, proving all locks precede one global fence;
- local owner, remote owner, forwarded/mirror transaction, bind change, and
  owner restart/reallocation with clocks at the configured skew boundaries;
- exact transaction/target-vector proof inheritance on ordinary retry, sticky
  exact-main fallback on logical rebuild, and rejection of same-generation,
  prepare-time/reused, mismatched, or equal/older-snapshot state;
- clock-helper boundary tests for same-physical logical maxima, negative time,
  negative max offset, and `math.MaxInt64`, plus a timestamp waiter/operator
  stub that installs an equal or older snapshot, proving the compiler fails
  rather than loops or admits stale execution;
- an idle system with no user commit and CN/TN clocks at both allowed skew
  extremes, proving TN heartbeat progress carries the waiter beyond the future
  fence and measuring the approximately `2 * MaxOffset` upper case, plus
  stopped-heartbeat/disconnected-logtail cancellation;
- cancellation while acquiring each target and while waiting for the fence, lock
  timeout, deadlock, definition change, retry-transition failure, and terminal
  transaction lock release;
- fresh source initialization and regenerated remote tombstone/payload state on
  the one admitted retry.

## 7. Why hidden physical ownership is required

The base-table lock serializes supported ordinary DML because those plans reach
the base lock namespace before writing synchronous hidden indexes. It does not
by itself own a separate physical hidden-table lock namespace.

Direct DML against a resolved system index relation is planner-reachable, and
internal maintenance can address physical tables without proving that it
acquired this LOAD's base owner. Removing a hidden row lock while acquiring
only the base lock therefore leaves a concrete ownership and visibility hole.

One hidden Exclusive full-domain lock closes that hole for each promoted
target. For normal base-table writers it does not reduce concurrency: the
existing base Exclusive full-domain owner already blocks them for the LOAD
duration. The one HLC-fence retry is additional fixed pre-run work, not an
input-dependent lock operation.

It deliberately broadens availability impact for lock-aware clients that write
a physical hidden table directly. The old row locks blocked only keys reached
by the LOAD; the full-domain owner also blocks disjoint keys. A direct commit in
the snapshot-to-lock window is covered by the same mandatory statement fence,
even when its key is disjoint from the eventual input; it does not create one
retry per writer or target. A direct writer that first owns a physical key and
then requests another table can add a new wait/deadlock edge. Cancellation,
lock timeout, or a deadlock victim can therefore become visible in a workload
that previously did not conflict.

Even with no writer, every eligible large LOAD waits through one strict HLC
fence and physically recompiles once. Inputs below 1 GiB or with unknown size,
retry-disabled statements, and prepare-time/reused-plan executions are ineligible
specifically so this fixed correctness cost does not become a latency
regression or a guaranteed user-visible retry error.

Source opening and parsing occur after the fences. A missing/changed object,
decode error, or early pipeline failure therefore leaves the new hidden locks
owned by an explicit transaction until the client commits or rolls back, just
as the existing base lock is retained today. Ordinary base-table writers were
already blocked by that base owner; direct physical hidden writers experience
the additional availability impact and are included in failure-path tests.

This is a real accepted tradeoff, not an impossible counterexample. The scale
gate must reject material retry, timeout, deadlock, wait, or diagnostic
amplification. Writers that bypass the transaction lockservice are outside both
the old row-lock guarantee and this design.

## 8. Physical key-domain admission

V3 admits two base physical PK shapes:

- a real single-column `types.T_int64` PK; or
- the generated `catalog.CPrimaryKeyColName` whose authoritative
  `CompPkeyCol` is the canonical max-width binary `types.T_varchar` used for a
  composite PK.

The first base range is the existing
`Packer.EncodeInt64(math.MinInt64)` through
`Packer.EncodeInt64(math.MaxInt64)` pair. The composite range is encoded empty
bytes through `Packer.EncodeStringTypeMax`; every serialized composite tuple is
encoded as a string value strictly within those sentinels. A user-declared
VARCHAR PK does not qualify merely because it shares the OID.

V3 admits exactly one promoted hidden physical primary-key OID:
`types.T_int64`, with the same integer endpoints. No width/scale
reinterpretation, projection cast, or SQL source type can substitute for the
authoritative base or hidden physical primary-key column definition.

Independent oracle tests encode `math.MinInt64`, `math.MaxInt64`, `-1`, `0`,
and `1`, plus representative and boundary serialized composite tuples. They
check endpoint order and use the real lockservice to prove Shared and Exclusive
row requests at endpoints/interior values conflict with each full range. The
two actual #27775 planner fixtures must assert respectively a `T_int64` base
and canonical binary composite-PK base, and both must resolve the hidden UNIQUE
table to `T_int64`; hand-built compiler nodes are insufficient.

Every other OID defaults to its exact row lock. In particular, this PR makes no
claim about scalar FLOAT/DOUBLE extrema, decimal precision, temporal SQL
domains, UUID/string endpoints, or serialized composite-key bytes. Expanding
admission is a separate reviewed change with its own encoding oracle and scale
evidence.

## 9. Ownership and unhappy-path model

### 9.1 State transitions

| From | Event | Guard / linearization | To | Failure behavior |
| --- | --- | --- | --- | --- |
| planned | classify targets | complete positive two-pass result | main or candidate | exact main on uncertainty |
| candidate | compile value copies | canonical plan untouched | compiled | compile error; no locks acquired |
| compiled | acquire metadata/general/base | existing order succeeds | base-owned | substantive error; txn owns prefix |
| base-owned | acquire hidden target N | prior targets owned | acquiring | cancel/timeout/deadlock; txn owns prefix |
| acquiring | target reports definition change | stop immediately | definition retry | txn owns acquired prefix |
| acquiring | final hidden acquired | every target owned | ownership-complete | none |
| ownership-complete | construct HLC fence | valid next-physical upper bound | fencing | overflow/invalid clock; txn owns locks |
| fencing | update RC snapshot and record proof | logtail applied and snapshot > fence | retry-marked | cancel/client close/update error; txn owns locks and no proof is published |
| retry-marked | return one data retry | proof records txn, exact targets, fence, and generation | retry transition | transition error; txn owns locks |
| retry transition | recompile/re-enter | ordinary retry inherits exact local generation; later generation and snapshot > fence | fenced | mismatch is terminal; explicit txn retains locks until its terminal action |
| definition retry | set sticky disable, then rebuild logical plan | before rebuild; current catalog snapshot | exact main | old valid locks remain txn-owned; no later optimization fence in this Run |
| fenced | initialize runtime sources | ownership and freshness hold | running | statement error; txn owns locks |
| running | commit or rollback | transaction terminal action | terminal | lockservice releases ownership |

The compiler owns only bounded value records. The transaction/lockservice is
the sole owner of acquired locks. The compiler never unlocks a partial prefix.
After any logical rebuild, that Run uses the exact `main` target shape, but any
full hidden lock acquired by the earlier eligible generation necessarily
remains transaction-owned until commit or rollback. This can only reduce
availability; releasing it in the compiler would violate transaction lock
ownership. The transition is covered explicitly instead of describing the
transaction's accumulated locks as exact `main`.

### 9.2 Q1-Q3 audit

| Q | Resource/dependency | Owner and terminal path | Verdict required |
| --- | --- | --- | --- |
| Q1 | compiler target slice | compiler instance; cleared and nilled on every release/reuse path | no stale values or pooled backing-array retention |
| Q1 | execution proof | root coordinator object; ordinary retry borrows, logical rebuild disables and drops, release only nils borrower | no remote/cache/pool leak and no clearing while borrowed |
| Q1 | acquired range locks | transaction/lockservice; commit or rollback | no compiler cleanup/double unlock |
| Q1 | HLC fence/refreshed snapshot | stack value then transaction operator; retry generation consumes snapshot | fence dominates prior commits; snapshot is strictly later and visible to new sources |
| Q2 | base/hidden wait | lockservice waiter observing context, timeout, deadlock, holder unlock | every terminal path tested |
| Q2 | future logtail fence wait | transaction timestamp waiter observing context and client close | idle TN heartbeat progresses; outage cancels without private worker |
| Q2 | retry-marked acquisition | compiler continues only after successful lock | substantive error is not swallowed |
| Q2 | partial prefix | transaction remains owner; terminal txn action releases | no compensating wait/unlock |
| Q3 | target collection | schema UNIQUE count | finite and reset on reuse |
| Q3 | lock rows | one range pair per promoted target | independent of LOAD rows/batches |
| Q3 | retry/log amplification | at most one deterministic optimization retry per `Compile.Run`; no new log site | sticky rebuild disable plus finite >=1 GiB gate rejects storms |

Wait-for shape for the optimized path:

```text
LOAD caller
  -> existing base owner
  -> promoted hidden target 1 ... N
  -> conflicting direct physical transaction
  -> holder commit/rollback, caller cancellation/timeout, or deadlock resolution
```

After ownership completes, freshness has a separate acyclic progress chain:

```text
LOAD caller
  -> CN transaction timestamp waiter
  -> minimum applied frontier across CN logtail consumer routines
  -> ordered TN empty-progress publication
  -> TN heartbeat transaction/HLC reaching the coordinator fence
```

Heartbeat transactions do not acquire the LOAD's table locks, so this chain has
no edge back to the ownership wait graph. Context cancellation or txn-client
closure terminates it if TN/logtail progress is unavailable.

For the eligible schema, current ordinary INSERT/UPDATE/DELETE/REPLACE reaches
the base namespace before hidden locks/writes: modern paths order base targets
first, while legacy UPDATE/DELETE materialize a base-locked source before
hidden branches. Thus one ordinary statement cannot hold a hidden row while
waiting behind this LOAD's base owner.

That proof does not constrain locks retained from an earlier statement in the
same transaction or a direct physical client using arbitrary multi-table order.
Residual cycles use existing deadlock detection; no global order is claimed.

## 10. Alternatives and decision log

### 10.1 Keep all per-batch UNIQUE row locks

Correct but rejected for promotable targets in the issue workload. Work and
retained ownership scale with batches/keys and enter the coarsening path
implicated by the regression.

### 10.2 Use only the base-table lock

Rejected. Hidden UNIQUE tables are separate physical namespaces and direct
physical writers are reachable. Base-only ownership also cannot close the RC
visibility window for a hidden-only commit.

### 10.3 Generate full-table targets in the planner

Rejected. The modern plan already carries exact resolved hidden row targets.
Changing target flags in the logical plan complicates SI/optimistic behavior,
prepared-plan reuse, remote serialization, and retry generations without adding
evidence. Compiler-local promotion is narrower and reversible.

### 10.4 Optimize the unsupported-DML fallback

Deferred. Main's fallback lock node lacks the modern hidden row-target evidence
and canonical base metadata used by the transformation. Populating new fields
and resolving a second ownership contract is unnecessary for #27775. Fallback
therefore remains byte-for-byte plan-equivalent to `main`.

### 10.5 Optimize FK, partitioned, or temporary tables

Rejected from this PR. Their physical topology and wait-for graph differ from
the single ordinary base owner proved here. The reported workload requires none
of them.

### 10.6 Optimize SI or optimistic transactions

Rejected from this PR. SI needs an explicit stale-snapshot/WW-conflict contract
for no-active-conflict full locks. Optimistic execution does not use the
lockservice ownership mechanism whose contention this PR addresses. Both keep
exact `main` targets.

### 10.7 Change generic `LockTableWithMode` freshness

Rejected. Existing callers use full-table locks for operations that do not all
need row-data refresh. A global conservative retry would broaden behavior and
cost without a caller-specific proof. Only the eligible LOAD ownership helper
defers target-local data freshness and completes it with one statement-wide HLC
fence.

### 10.8 Use owner-local `tableCommittedAt` as the LOAD fence

Rejected. It detects useful stable-owner cases but is neither durable nor part
of lock-table binding migration. New owners synthesize it from their local HLC,
and lockservice RPC currently has no HLC codec header. The coordinator strict
upper-bound fence is generation-independent and needs no wire change.

### 10.9 Fix generic FLOAT/DOUBLE table ranges here

Deferred. The correction is valid but independent of the BIGINT reproducer and
widens the production diff. V3 keeps scalar FLOAT/DOUBLE row locks. A separate
change may fix and admit those types after independent review.

### 10.10 Add a logical lock group to `plan.proto`

Rejected. One eligible modern LOAD has one existing base owner and only local
Exclusive hidden promotions. A distributed grouping field adds no safety or
performance value.

### 10.11 Collect all precise keys before mutation

Rejected. It retains O(input rows) material, requires full input
materialization/spill before mutation, and still sends large lock sets.

### 10.12 Initialize every lock-table owner with a future generation fence

Rejected for this PR. Replacing `tableCommittedAt` with an upper-bound fence
would return a future timestamp on ordinary first row locks and force unrelated
RC statements to wait through the clock uncertainty window before their
key-specific version check. Keeping a separate owner-generation fence and
returning it only to LOAD requires new request/result capability fields and a
mixed-version fallback. That is a broader lockservice protocol change than one
coordinator-local fence for the admitted large LOAD class.

### 10.13 Infer fence completion from snapshot ordering

Rejected. An RC snapshot can advance for metadata, another table target,
prepared-plan reuse, or future pre-run helpers. `snapshot > planSnapshot` does
not identify which operation supplied the freshness proof. The explicit local
generation object records the exact transaction, target vector, fence, and
physical generation without a timestamp-origin inference.

### 10.14 Acquire and fence before physical compilation

Deferred. In principle this avoids the second physical compile, but current
metadata-target collection and retry handling live across physical compilation
and pre-run. Moving lock waits into `Compile` would need a new definition-change
rebuild loop and would retain transaction locks on later compile-only failures.
That architectural change is not justified until the 100M endpoint proves the
smaller existing retry mechanism is insufficient.

### 10.15 Reuse a large LOAD logical plan from the session cache

Rejected. File bytes and size can change without a catalog-version event, while
the cache retains the external-scan estimate used by admission. A cached plan
therefore cannot prove that the current execution still passes the fixed-cost
gate. The existing `NotCacheable` bit at the 1 GiB boundary is simpler and its
replan cost is negligible relative to the admitted workload. Reused prepared
plans remain exact main; a freshly rebuilt prepared generation may qualify.

### 10.16 Fence each definition-rebuilt plan generation

Rejected. Repeated catalog churn could produce one future wait and retry for
every rebuild because the generic retry loop has no optimization-specific
attempt cap. The root sticky disable makes any logical rebuild exact main and
bounds this optimization to one fence and one retry per `Compile.Run`.

## 11. Performance and acceptance gate

### 11.1 Cost model

Before for one promotable UNIQUE target:

```text
lock encoding/submission = O(input batches)
owner bookkeeping        = input-key dependent, with possible coarsening
```

After:

```text
compile classification   = O(reachable plan nodes + schema indexes +
                             update contexts + lock targets)
pre-pipeline requests    = O(promoted UNIQUE count)
sorting                  = O(promoted UNIQUE count log promoted UNIQUE count)
HLC/logtail fence        = at most one wait per Compile.Run
physical compilation    = two generations before first source initialization
batch-path lock work     = 0 for promoted targets
```

Every hidden type other than `T_int64` retains its existing batch cost. The
issue fixtures have one `UNIQUE BIGINT` target whose resolved hidden primary
key is `T_int64`, and therefore remove all covered UNIQUE batch-lock work.

Freshness adds no storage scan or input-key materialization, but it always adds
one strict HLC/logtail wait and one physical recompile to an eligible large
LOAD, including the no-writer case. At the allowed opposite CN/TN skew
extremes, the wait can approach twice the configured clock max offset (about
1 s under the default 500 ms HLC configuration) plus heartbeat and logtail
delivery/apply latency. `LoadWriteS3` alone is insufficient because its
current planner gate can admit roughly 1 MiB and small compressed inputs. The
additional finite 1 GiB input estimate, retry, and fresh-plan gates keep that
fixed cost off smaller, unknown-size, and reused statements. Large direct LOAD
plans are not inserted into the session cache, so repeated executions replan
and can qualify with current input statistics. The 1 GiB
constant is intentionally conservative for #27775; lowering it requires
separate normalized evidence around the proposed boundary.

Validation reports estimated input bytes, HLC wait duration, both compile
durations, and exactly one optimization-induced retry separately from runtime
lock savings; the endpoint gate decides whether the trade is worthwhile.

### 11.2 Controlled endpoint validation

Use the issue's 3-CN TKE topology, same runner, COS objects, SQL,
configuration, and tenant. Compare exact `origin/main` and exact PR head with
alternating runs to reduce time/environment bias.

Minimum evidence:

1. three successful runs per revision of the 100M no-index control;
2. three successful runs per revision of both 100M indexed cases;
3. median, min/max, and indexed/no-index ratio for each revision;
4. exact row count and error-free completion for every run;
5. lock request/coarsening counts and equal-window CPU/mutex profiles;
6. HLC-fence wait, first/retry compile duration, retry count, lock-wait count,
   deadlock count, and relevant log volume;
7. synthetic planner/compiler boundary cases just below and at 1 GiB, plus a
   real input near the boundary to reject a fixed-cost latency regression; and
8. one 1B indexed confirmation after the 100M gate passes.

Accept only if:

- the PR improves the median normalized indexed/no-index ratio by at least 30%
  versus exact current main;
- the PR's 100M indexed median is no more than 1.25x the issue's previous-good
  baseline after normalization by the same-run no-index control;
- the no-index median changes by no more than 15%, unless attributed with
  evidence to an environment or unrelated-code cause;
- no correctness, OOM, hang, retry storm, deadlock amplification, material new
  lock-wait regression, or log storm appears.
- every eligible successful Run without a logical rebuild has exactly one
  optimization-induced retry; an eligible Run that logically rebuilds has at
  most one before permanently falling back to exact main. A second fence retry
  or a user-visible optimization retry is a rejection. Unrelated retries are
  reported separately and must not be amplified by the change.

A 5M local run can validate mechanism but cannot pass this gate.

## 12. Compatibility, rollout, and operations

No plan or lockservice protobuf, lock mode, catalog object, on-disk data,
configuration, metric label, or durable version is added. Logical lock targets
are unchanged; the existing `Node.NotCacheable` policy bit is set only on
direct large-LOAD scans. Promotion and freshness state are local to the
compiling CN.
`clock.NowUpperBoundFence` only centralizes the lock allocator's existing
next-physical-tick construction; refactoring that caller must preserve exact
behavior for valid clocks and fail closed for invalid ones.

During a rolling CN upgrade:

- upgraded CNs promote admitted hidden row targets for eligible statements;
- older CNs retain their existing per-batch hidden row/range requests;
- both conflict in the same physical lock namespace;
- the performance benefit is per statement;
- no old CN must understand a new plan field or lockservice result.

Downgrade or rollback replaces/restarts CN binaries. Process-local compiler
state and plans disappear with the process. Existing transactions finish or
roll back under ordinary lockservice ownership; no migration or data rewrite is
required.

No per-row, per-batch, per-wait, or per-target logging site is added. The normal
eligible path reduces lock requests. Broader direct-physical conflicts can
increase existing wait diagnostics, and every successfully fenced Run
increments the existing statement-retry metric exactly once. A Run rebuilt
before fencing has no optimization retry; definition and unrelated retries
retain their existing accounting. The first ordinary RC retry is not emitted
by `fatalLog`; a second fence retry, repeated unrelated retry, or user-visible
retry can still use existing diagnostics. Scale validation
compares log volume and retry/wait cardinality and rejects amplification.

Existing lock wait duration, transaction wait-lock state, lockservice request
and coarsening metrics, statement retry count, statement duration, and CPU/mutex
profiles remain the primary diagnostics. No authentication, authorization,
tenant identity, hidden-table visibility, or trust boundary changes.

Primary operational risks:

| Risk | Impact | Control |
| --- | --- | --- |
| ineligible plan accidentally promoted | changed correctness/availability | two-pass positive predicate plus negative shape matrix |
| canonical lock targets mutated | SI/prepared/retry execution loses row locks | local filtered copy plus cross-mode cache/retry tests |
| large LOAD reuses stale file statistics or a specialized cached plan | wrong admission or missing statement-local state | existing `NotCacheable` bit at the same 1 GiB gate plus legacy-cache fail-closed test |
| stale hidden commit before lock | duplicate scan uses old snapshot | real lockservice no-active-conflict freshness test |
| target-local helper advances snapshot before all ownership | later target commit escapes global proof | defer ordinary data freshness; definition errors still stop |
| raw HLC upper bound does not dominate equal-physical logical TS | unseen commit could escape freshness | shared next-physical-tick helper plus boundary tests |
| HLC fence is ahead of applied logtail | fixed pre-run wait approaching `2 * MaxOffset`, timeout, or cancel | finite >=1 GiB/retry-enabled gate plus opposite-skew wait-latency evidence |
| TN heartbeat/logtail stalls while no user commits occur | fence wait does not make progress | idle-heartbeat progress test plus context/client-close cancellation test |
| small or unknown input is mistaken for large | fixed fence cost becomes a latency regression | finite external-scan byte estimate and boundary tests; `LoadWriteS3` is not sufficient alone |
| stale/mismatched retry state is treated as fence proof | source starts without a proved execution | exact transaction/target vector, later execution generation, and snapshot > fence checks |
| fence emitted twice | retry/compile/metric amplification | shared execution proof, sticky rebuild disable, and exact-one fence assertion |
| incomplete base or promoted endpoint | writer escapes ownership after the fence | admit only tested integer/canonical-composite base ranges and integer hidden range; every other base is ineligible and every other hidden type remains row scoped |
| hidden target without base owner | ordering/ownership hole | disable whole transformation before mutation |
| remote fragment repeats promotion | duplicate locks or inconsistent snapshot owner | coordinator-only state plus serialization test |
| direct physical writer uses a disjoint key | broader wait/retry/timeout/cycle | explicit approval plus conflict/retry/deadlock scale gate |
| partial acquisition on cancellation | locks retained in explicit txn | transaction ownership plus rollback-release test |
| logical rebuild activates fallback | superseded full locks remain and broaden waits | sticky exact-main rebuilt target test plus terminal commit/rollback release |
| source init/parse fails after fencing | explicit txn retains hidden locks | source-failure test and documented client rollback requirement |
| compiler reuse retains targets/capacity | wrong-table lock or pooled memory growth | clear-and-nil on every release/retry/pool path |
| mechanism does not fix endpoint | complexity without benefit | mandatory normalized 100M/1B gate |

## 13. Validation map

| Invariant | Cheapest deterministic proof | Public/scale proof | Nearest controls |
| --- | --- | --- | --- |
| exact statement eligibility | compiler classifier UT plus actual planner fixture | issue's modern indexed LOAD | estimated bytes below/at 1 GiB, NaN/Inf/zero/missing stats, admitted/other base PK shapes, `LoadWriteS3=false`, retry-disabled, prepare-time/reused generation, freshly rebuilt prepared generation, RC/SI/optimistic, fallback, LOAD IGNORE/REPLACE/accounts, FK/checks-off, system/subscription, ID mismatch, feature flag/partition/temp/fake PK |
| large-LOAD cache isolation | planner/frontend cache-policy UT | repeat identical direct LOAD SQL | >=1 GiB plan is not cached; below-threshold policy is unchanged; legacy cached generation fails closed to main |
| exact hidden mapping | compiler UT over existing targets and mutation contexts | LOAD result/count | static NULL absent, dynamic present, non-UNIQUE, non-default/BTREE, async/malformed params, `TableExist=false`, missing/duplicate/mismatched `UpdateCtx`, unknown extra target |
| atomic fail-closed transform | canonical-plan and filtered-copy UT | existing ineligible LOAD corpus | late mismatch after earlier valid target, contradictory duplicate, missing metadata coverage |
| base before hidden; acquire once | compiler recorder UT plus ordinary-DML plan-order UT | real lockservice competitor | reversed IDs, duplicate ID, modern and legacy INSERT/UPDATE/DELETE/REPLACE |
| deferred target freshness | lockop result-matrix UT | concurrent LOAD stress | no conflict, active commit/rollback, stale/no-op `tableCommittedAt`; no ordinary snapshot update before all locks |
| global no-active freshness | real lockservice + RC txn test | concurrent LOAD stress | writer commits before request; post-acquisition HLC fence causes one retry and refreshed re-entry |
| active conflict freshness | barrier-based real lockservice test | retry/wait metrics | base/hidden holder commit or rollback, then the same one global retry; caller cancellation/timeout |
| one fence for all targets | base plus two hidden targets | retry/compile-count metric | every acquisition precedes fence; one optimization retry; no second fence after re-entry |
| HLC fence construction | shared clock-helper UT and allocator equivalence test | fence-wait latency | max logical at raw upper physical tick, negative offset/time, overflow, configured skew boundaries |
| strict refreshed snapshot | timestamp waiter/operator contract test | retry-count metric | installed snapshot equal/older than fence fails; exact local proof with greater snapshot re-enters once |
| idle fence progress | real TN heartbeat plus timestamp waiter test | no-writer scale run | opposite allowed CN/TN skew reaches the fence via PrepareTS heartbeat; stopped heartbeat and disconnected logtail cancel |
| owner-generation independence | remote/proxy/rebind/restart/mirror lockservice UT | 3-CN restart/rebind stress | old owner timestamp absent/stale yet coordinator fence covers commit; cancel while logtail waits |
| schema/index race | metadata-lock plus physical-lock integration UT | concurrent ALTER control | add/drop/rebuild between plan and pre-run; definition retry rebuilds before source init; ineligible rebuild retains old txn lock until terminal action |
| retry-state isolation | repeated compile from same plan | prepared LOAD control | prepare-time/reused paths remain exact main; ordinary retry shares exact local proof; logical rebuild permanently disables promotion for the Run; mismatch fails closed |
| coordinator-only ownership | remote-plan serialization/compile UT | 3-CN issue run | one promoted request per physical target; no remote freshness state |
| cancellation/partial prefix | deterministic waiter barrier UT | txn rollback semantics | cancel before first and while later target waits; transition failure |
| explicit transaction history | prior-statement base/hidden write plus freshness retry UT | commit/rollback SQL control | prior write remains visible, current generation alone rolls back, no self-wait |
| compiler reuse | pooled compiler UT | N/A: internal lifecycle | fresh compiler equivalence after success/error/retry |
| physical type admission | independent integer/composite base and integer-hidden encoding oracles with real conflicts | both issue table shapes | integer min/max/interior; serialized composite empty/max/interior; user VARCHAR and all other base shapes ineligible; every non-int64 hidden OID retained, including FLOAT32/FLOAT64 and serialized composite |
| direct physical availability tradeoff | disjoint-row wait and cycle-termination UT | wait/retry/deadlock/log gate | commit, rollback, cancel, timeout, prior-statement lock cycle |
| no covered batch-lock work | exact call-count UT | 100M/1B benchmark/profile | no-index, mixed `T_int64` plus non-promoted target, and ineligible LOAD |
| no public FK/fallback change | exact main-vs-PR plan-shape snapshot | existing FK/fallback BVT | no new FK BVT because FK plans are unchanged |

UT data is limited to the minimum rows and targets needed to distinguish
states. Concurrency tests use explicit barriers and outer deadlines, not
sleeps. Performance assertions remain in the big-data harness rather than
UT/BVT.

## 14. Design review record

Change scope: large direct LOAD cache exclusion -> modern LOAD `LOCK_OP` ->
compiler-local hidden target promotion -> LOAD-only RC freshness wrapper ->
pre-pipeline acquisition -> one local execution fence proof and physical retry.

Mandatory triggers:

- crosses planner-output, compiler, transaction-snapshot, and lockservice
  ownership boundaries;
- changes a distributed concurrency/availability path;
- changes a material LOAD hot path;
- requires mixed-version, rollback, boundedness, and scale evidence.

Decision log:

| Revision | Decision | Rationale |
| --- | --- | --- |
| v1 (`30a0943ec3`) | superseded | preserving FK routing did not preserve FK lock behavior; implementation remained over-broad |
| v2 (unapproved draft) | superseded | fallback/planner full-target construction was unnecessary; generic table lock missed a concrete no-active-conflict RC freshness window |
| v3 | pending | >=1 GiB modern LOAD compiler promotion, large-LOAD cache exclusion, deferred target-local freshness, at most one coordinator strict-HLC fence per Run, sticky exact-main rebuild fallback, canonical lock-target preservation, issue-specific integer/composite base admission, and `T_int64`-only hidden promotion |

The v2 freshness blocker was proven with a real local lockservice diagnostic,
not only code inspection. V3 also rejects owner-local `tableCommittedAt` as a
generation-stable proof after auditing bind/restart transport, rejects mutable
snapshot ordering as proof, bounds opposite-skew wait at approximately twice
`MaxOffset`, and verifies idle TN heartbeat progress before using a future HLC
fence. Production implementation review and delivery remain
blocked while v3 is `mandatory design review pending`.

Approval must be traceable in PR #27814 and include both SQL planner/compile and
lockservice ownership perspectives. Record reviewer handles, links, decision,
and the exact approved design commit here before implementing or pushing
production changes.
