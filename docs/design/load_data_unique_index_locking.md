# LOAD DATA Unique-Index Lock Ownership

- Status: mandatory design review pending
- Design revision: v1
- Tracking issue: [matrixorigin/matrixone#27775](https://github.com/matrixorigin/matrixone/issues/27775)
- Implementation PR: [matrixorigin/matrixone#27814](https://github.com/matrixorigin/matrixone/pull/27814)
- Authors: XuPeng-SH
- Required reviewers: one SQL planner/compile owner and one lockservice owner
- Last updated: 2026-08-29

## 1. Decision summary

For a pessimistic `LOAD DATA` into a table with a real primary key, acquire one
Exclusive full-domain lock for the base table and one Exclusive full-domain
lock for each regular UNIQUE hidden table that the statement can write. Acquire
those locks once, before data-source initialization, in a deterministic local
order. The existing per-batch row locks for those covered physical tables are
then redundant and are removed from the LOAD pipeline.

This design deliberately does **not** change foreign-key planner selection or
foreign-key locking:

- an FK-related LOAD keeps the planner and validation path used on `main`;
- the optimization does not add Shared full-domain locks to referenced parent
  tables or hidden UNIQUE tables;
- ordinary INSERT/UPDATE/DELETE/REPLACE and fake-primary-key LOAD stay row
  scoped;
- no new lock mode, lockservice protocol, durable metadata, or cross-CN wire
  contract is introduced.

The narrower scope is intentional. A previous implementation revision routed
FK-related LOAD through the modern INSERT path and acquired a Shared full-domain
parent fence. That made a long child LOAD block every conflicting parent write,
including disjoint keys, for the complete LOAD duration. It also required a new
logical lock-group field in the plan protobuf. Neither change is necessary to
remove redundant UNIQUE-index lock work in issue #27775, so both are excluded.

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

1. A real-primary-key LOAD already acquires an Exclusive base-table range lock
   before the pipeline starts.
2. Each input batch still encodes and submits Exclusive keys/ranges for every
   synchronously maintained regular UNIQUE hidden table.
3. Large loads repeatedly rebuild ownership for the same physical hidden
   tables and can cross the lock-row budget, causing owner-side coarsening and
   additional lockservice bookkeeping/contention.

This is a root-cause hypothesis until exact-main/exact-PR controlled big-data
evidence passes the acceptance gate in section 10. A lower coarsening count is
mechanism evidence, not by itself endpoint proof.

## 3. Scope

Included:

- pessimistic `LOAD DATA` into a base table with a non-fake primary key;
- synchronously maintained regular UNIQUE hidden tables with `TableExist=true`;
- modern and unsupported-DML fallback LOAD planners, without changing which
  planner a statement selects;
- full physical key-domain encoding for every supported lock-table key type;
- deterministic acquisition, duplicate-target coalescing, cancellation,
  compiler reuse, and transaction-owned cleanup;
- existing indexed-LOAD BVT behavior plus focused planner/compiler/lockservice
  tests and a controlled big-data performance comparison.

Excluded:

- FK planner migration or new FK semantics;
- referenced-parent full-domain fencing;
- secondary non-UNIQUE indexes, which do not have uniqueness row locks to
  remove;
- asynchronous or cron-maintained index families;
- fake-primary-key LOAD, ordinary INSERT, and optimistic transactions;
- a general lock escalation redesign or a new intent-lock mode;
- changing whether users can address physical hidden tables.

## 4. Required invariants

### 4.1 Safety

1. Before a real-PK LOAD initializes any data source or reads a statement
   snapshot, its transaction owns Exclusive full-domain locks for the base
   table and every regular UNIQUE hidden table the LOAD can write.
2. Removing per-batch locks is legal only for a physical table covered by that
   transaction-owned Exclusive full-domain lock.
3. A statically NULL UNIQUE key may omit its hidden-table lock only when the
   existing projection/dedup/MultiUpdate analysis proves that the statement
   cannot emit a row for that hidden table. Dynamic or unknown values retain
   the lock.
4. A lock-table range covers the complete physical `Packer` encoding domain.
   For FLOAT/DOUBLE this includes both infinities, every NaN payload, both
   signed zeros, and all finite values.
5. If one physical target is discovered more than once, it is acquired once;
   Exclusive dominates Shared. Conflicting ownership metadata is an internal
   error, not a silent reorder.
6. Existing FK-related LOAD planner selection and validation remain unchanged
   from `main`. This optimization must not claim to repair pre-existing FK LOAD
   behavior.

The ownership linearization point for one target is successful completion of
its lockservice full-domain acquisition. The statement may initialize its data
sources only after every target has passed that point.

### 4.2 Liveness

1. All pre-pipeline targets use one deterministic order: logical base table,
   then physical base before hidden tables, then hidden-table physical name and
   physical ID as a tie-breaker.
2. The logical grouping used by the compiler is statement-local metadata. It is
   not serialized in `plan.proto` and cannot become a mixed-version contract.
3. Every lock wait terminates through acquisition, caller cancellation,
   transaction timeout/deadlock handling, or transaction termination.
4. A failure after acquiring a prefix of targets returns the original error.
   The transaction remains the lock owner; autocommit rollback or explicit
   transaction completion releases the prefix exactly once.
5. Compiler pooling clears every retained target before reuse. Work from one
   statement cannot lock tables for the next statement.

### 4.3 Boundedness

For one statement:

```text
target_count <= 1 base + writable regular UNIQUE hidden-table count
memory       = O(target_count)
sorting      = O(target_count log target_count)
lock rows    = exactly one encoded range pair per target
goroutines   = 0 added
```

Target count is schema-bounded rather than input-row-bounded. The hot batch
path does not allocate or encode UNIQUE lock rows after coverage is established.

## 5. State, ownership, and failure model

The statement transitions are:

| From | Event | Guard / linearization | To | Failure behavior |
| --- | --- | --- | --- | --- |
| planned | compile lock targets | valid base/hidden metadata | compiled | return planning error |
| compiled | acquire next target | prior targets acquired in canonical order | acquiring | context/deadlock error; txn owns prefix |
| acquiring | final target acquired | all full-domain requests succeeded | fenced | none |
| fenced | initialize data sources | ownership invariant holds | running | statement error; txn owns locks |
| running | commit | transaction commit | committed | lockservice releases ownership |
| running/failure | rollback | transaction rollback | rolled back | lockservice releases ownership |

The compiler owns only bounded pointers to immutable plan targets. The
transaction/lockservice owns acquired locks. The compiler must never attempt
independent unlock or partial cleanup because that would violate transaction
isolation and double-own cleanup.

Wait-for shape:

```text
LOAD caller
  -> canonical target N
  -> conflicting transaction
  -> commit / rollback / cancellation / deadlock resolution
```

No new channel, callback, worker, RPC class, retry loop, or synchronous cleanup
edge is introduced. Cancellation uses the statement context already passed to
lockservice. A cancelled explicit transaction may retain an acquired prefix
until the transaction is rolled back; that is existing transaction ownership,
not a leaked compiler resource.

## 6. Planner and compiler contract

Both LOAD planners call one helper that returns:

- the base-table Exclusive full-domain target first;
- one Exclusive full-domain target for each writable regular UNIQUE hidden
  table;
- the hidden table's own primary-key type as the range encoder type;
- enough object identity for deterministic local ordering and diagnostics.

The helper does not encode runtime row values. The modern planner may use its
static-NULL analysis to omit a target; the fallback planner has no equivalent
proof and remains conservative.

During compilation, the LOAD lock node identifies its base table through the
node's existing `TableDef`. The compiler stores the base/hidden relationship in
a local wrapper around `LockTarget`, coalesces by physical table ID, and sorts
that wrapper before `prePipelineInitializer`. No protobuf field is needed.

The planner still emits the lock node at the semantic point used on `main`.
Compilation consumes covered full-domain targets into the pre-pipeline set, so
the runtime LockOp does not re-encode or resubmit them for every batch.

## 7. Physical key-domain correctness

`LockTableWithMode` models the full table as one encoded range. Therefore its
range endpoints must be the extrema of the physical encoding, not merely SQL
numeric extrema.

For FLOAT/DOUBLE, `Packer` applies a total order over IEEE bits:

- negative encodings invert all bits;
- non-negative encodings flip the sign bit.

The physical minimum is the negative NaN bit pattern with all bits set; the
physical maximum is the positive NaN pattern with every payload bit set. The
same comparator must be used when a normal row batch is reduced to a range, or
NaN can make the selected endpoints depend on input order.

Focused tests must cover FLOAT32 and FLOAT64:

- negative NaN physical minimum;
- `-Inf`, largest negative finite, `-0`, `+0`, largest positive finite, `+Inf`;
- positive NaN physical maximum;
- multiple NaN payloads and input permutations;
- a real lockservice Exclusive row writer waiting behind the full-domain lock.

## 8. Alternatives

### 8.1 Keep per-batch UNIQUE row locks

Rejected. It preserves precision but repeats encoding, request construction,
owner bookkeeping, and possible coarsening in proportion to input batches. It
is the mechanism under investigation for #27775.

### 8.2 Rely only on the base-table lock

Rejected. Hidden UNIQUE tables are separate physical lock namespaces. An
independent physical-table writer or validator does not prove that it acquired
the base-table lock, so removing its row lock without a hidden-table fence
would create an ownership hole.

### 8.3 Use Shared hidden-table fences

Rejected. Shared is compatible with FK validation readers and does not own the
hidden-table write interval. A later Exclusive physical writer can race the
LOAD, and a reader can validate against an old snapshot before the parent LOAD
commits.

### 8.4 Collect all LOAD keys and lock precise rows once

Rejected for this fix. It retains O(input rows) lock material, requires full
input materialization/spill before mutation, delays first progress, and still
enters lockservice with large key sets. It is a distinct bulk-DML architecture,
not a minimal repair for redundant ownership.

### 8.5 Add a new intent mode compatible with disjoint parent writes

Rejected. A safe intent would need a new lock compatibility/protocol contract
plus a validation-generation boundary. It expands lockservice and rolling
upgrade risk without being needed for indexed LOAD.

### 8.6 Route FK LOAD through the modern planner and fence parents

Rejected from this PR. It changes SQL-visible planning/validation and makes a
child LOAD hold Shared full-domain parent locks for its entire duration. The
availability cost applies even to disjoint parent keys. Any future FK LOAD
redesign requires a separate issue, public BVT, contention budget, and approved
design.

## 9. Compatibility, rollout, and rollback

The final design changes no lockservice protobuf, lock mode, catalog object,
on-disk data, or durable version. Planner and compiler in one CN process share
the local grouping implementation. Remote lockservice nodes receive ordinary
range-lock requests they already understand.

During a rolling CN upgrade:

- upgraded CNs use one full-domain hidden-table request per LOAD;
- older CNs retain per-batch hidden-table row locks;
- both request forms conflict correctly in the same physical lock namespace;
- the performance benefit is per-statement and becomes cluster-wide when all
  query-serving CNs are upgraded.

Rollback requires only replacing/restarting CN binaries. Process-local plan
caches disappear with the process. Existing transactions finish or roll back
under the lock protocol with which they started; no migration or data rewrite
is required.

## 10. Acceptance criteria

### 10.1 Functional and unhappy paths

- Modern and fallback LOAD plans enforce the same base/UNIQUE ownership
  contract; the only permitted difference is a modern-plan target omitted by
  an existing static proof that no hidden row can be written.
- Fake-PK LOAD and ordinary INSERT remain row scoped.
- Statically absent UNIQUE rows are skipped; dynamic and non-NULL controls are
  fenced.
- Duplicate physical targets coalesce and strongest mode wins.
- Cancellation while waiting for target N removes that waiter; rollback then
  releases every previously acquired target.
- Empty input, parse failure, duplicate-key failure, retry/re-entry, explicit
  transaction rollback, and compiler reuse retain correct ownership/cleanup.
- Existing pessimistic indexed-LOAD BVT remains green. Because this scoped
  design preserves FK planner selection, no new LOAD+FK public contract is
  claimed; if the final diff changes that fact, a deterministic multi-session
  LOAD+FK commit/rollback BVT becomes mandatory.

### 10.2 Performance

Use the issue's 3-CN TKE topology, same runner, COS objects, SQL, configuration,
and tenant. Compare exact `origin/main` and exact PR head with alternating runs
to reduce time/environment bias.

Minimum evidence:

1. three successful runs per revision of the 100M no-index control;
2. three successful runs per revision of both 100M indexed cases;
3. median, min/max, and indexed/no-index ratio for each revision;
4. exact row-count and error-free completion for every run;
5. lock request/coarsening counts and equal-window CPU/mutex profiles;
6. one 1B indexed confirmation after the 100M gate passes.

The optimization is accepted only if:

- the PR improves the median normalized indexed/no-index ratio by at least 30%
  versus exact current main;
- the PR's 100M indexed median is no more than 1.25x the issue's previous-good
  baseline after normalization by the same-run no-index control;
- the no-index median does not change by more than 15%, unless the difference
  is attributed to an environment or unrelated-code cause with evidence;
- no correctness, OOM, hang, retry-storm, or material new lock-wait regression
  appears.

A 5M local run is useful for mechanism iteration but cannot pass this endpoint
gate.

## 11. Observability and operational risk

No per-row or per-batch log is added, so the design does not create a log-storm
path. Use existing lock wait duration, transaction wait-lock state,
lockservice request/coarsening metrics, statement duration, and profiles.

Primary risks and controls:

| Risk | Impact | Control |
| --- | --- | --- |
| Hidden-table range is incomplete | concurrent write escapes ownership | physical-domain endpoint tests and real lockservice conflict test |
| Target order differs from another DML path | deadlock/retry amplification | base-first and stable physical-name order; focused competing-path race test |
| Lock acquired for an unwritten NULL index | needless contention | reuse existing static-NULL proof only |
| Partial acquisition on cancellation | locks retained in explicit txn | transaction owns prefix; deterministic rollback-release test |
| Compiler reuse retains old targets | unrelated statement locks wrong table | clear-on-pool test |
| Full hidden-table fence blocks direct disjoint physical writes | lower concurrency | accepted and schema-count bounded; ordinary base-table DML is already blocked by base owner |
| Hypothesis does not fix endpoint regression | complexity without benefit | controlled 100M gate before approval/merge |

## 12. Validation map

| Invariant | Cheapest proof | Public proof | Additional evidence |
| --- | --- | --- | --- |
| planner emits exact ownership set | planner UT | existing indexed-LOAD BVT | plan inspection |
| compiler acquires once in canonical order | compile UT with injected recorder | not separately SQL-visible | real lockservice competing transactions |
| complete FLOAT/DOUBLE domain | fetcher UT + bitwise oracle | existing numeric DML behavior | real lockservice waiter |
| cancellation/partial ownership terminates | deterministic channel/barrier UT | transaction rollback behavior already covered | focused race stress |
| no per-batch UNIQUE lock work | operator/compile call-count UT | indexed LOAD completes | 100M/1B benchmark and profiles |
| no FK contract change | exact final diff + planner route UT | existing FK corpus | no new parent fence in plan |

## 13. Approval record

Implementation review and merge remain blocked while this document is in
`mandatory design review pending`. Approval must be traceable in PR #27814 and
must include both SQL planner/compile and lockservice ownership perspectives.
After approval, record reviewer handles, links, decision, and the approved
revision here before delivering implementation changes.
