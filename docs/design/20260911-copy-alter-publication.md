# COPY ALTER publication protocol

- Issue: [#28319](https://github.com/matrixorigin/matrixone/issues/28319)
- Implementation PR: [#28418](https://github.com/matrixorigin/matrixone/pull/28418)
- Revision: 1 (2026-09-11)
- Status: proposed design for review; implementation is in the linked Draft PR.
- Target branch: `main`

This document is the versioned design artifact for the COPY ALTER lock
shortening change. It is deliberately separate from the implementation
summary. A maintainer design decision is still required before the PR is
considered ready to merge.

## Problem and scope

The COPY ALTER path used to acquire the global View/SNAPSHOT lifecycle
protection before copying rows and building indexes. A COPY ALTER on table A
therefore blocked an unrelated operation on table B. The B session retained
its table protection while waiting for the global row, which amplified the
delay into client timeouts and connection failures.

The optimization applies only when all of these conditions hold at admission:

1. the front end owns an automatic-commit transaction;
2. the transaction uses pessimistic read-committed semantics;
3. no earlier user statement has left workspace history in the transaction;
4. the operation is a supported permanent-table COPY ALTER and is not restore
   or an internal caller with an existing transaction.

Explicit transactions, snapshot isolation, optimistic transactions, restore,
and callers that already own a transaction keep the existing consistency path.
The change does not add SQL syntax, a user switch, a catalog column, a wire
field, or a persistence-format version.

## Contract and invariants

The operation remains one transaction with one final commit. The replacement
relation and its physical index tables are private to that transaction until
the publication step succeeds.

* The source table's directory/data locks protect the source identity while
  preparation runs. The global View and SNAPSHOT lifecycle gates are not held
  during row copy or synchronous index construction.
* `T_data` is selected once after the source locks are held and is the fixed
  source-data boundary. When lineage is created, the same value is the
  lineage `cloneTS`.
* `T_catalog` is the refreshed publication read boundary. It may be newer than
  `T_data`; refreshing it never changes which source rows were copied.
* Commit time is the visibility point of the new business relation. It is not
  substituted for either `T_data` or `T_catalog`.
* Publication acquires lifecycle gates in the single order **View then
  SNAPSHOT**. The SNAPSHOT row keeps the existing `FOR UPDATE`/no-op UPDATE
  write barrier used by optimistic validation and GC.
* Publication rechecks source table ID, logical ID, branch/history owners,
  Snapshot/PITR coverage, View state, foreign-key/publication metadata and
  final task identities after the gates are held. A preparation-time “no
  owner” result is never authoritative.
* Normal gate contention waits and rereads the publication directory while
  reusing the prepared relation. It does not copy the source a second time.
* A private coordination conflict may request at most one complete retry. The
  failed transaction is rolled back and fully cleaned before a new transaction
  and a new transaction ID begin; a commit with an uncertain result is never
  replayed.

## State and ownership

The COPY ALTER private context is bound to one transaction, attempt and target
relation. Its lifecycle is:

```text
Preparing -> Prepared -> AcquiringGates -> Publishing -> AwaitingCommit
     |          |             |              |              |
   abort      abort         retry/abort    abort          commit/abort
```

The context records the source and logical IDs, `T_data`, the prepared
relation/index identities, deferred task descriptions, attempt generation and
the selected admission mode. Every internal statement option carries the
context reference only for this operation and validates transaction identity,
phase and target relation. Session-wide reusable options are not changed, and
the binding is cleared on success, rollback, compile failure or cancellation.

The outer executor owns the transaction when it is the automatic-commit front
end. A caller-owned transaction remains caller-owned; the COPY ALTER path does
not replace it or replay its preceding statements. An independent internal
executor may use the same two phases only when it created and owns its whole
transaction.

## Preparation phase

Preparation first resolves the source relation under the existing directory,
data and required index locks, then records the source identity and selects
`T_data` using the existing lock-held boundary algorithm. It performs privilege,
type, primary-key and unsupported-operation checks before creating side effects.

In the same transaction workspace it then:

1. creates the uncommitted replacement relation with the source logical ID and
   relation kind;
2. copies rows at `T_data`, including the source workspace overlay required by
   the existing transaction semantics;
3. prepares auto-increment state and copies unaffected indexes;
4. synchronously rebuilds affected physical indexes when the algorithm
   requires it; and
5. records structured descriptions for tasks that will be attached to the
   final relation.

The private preparation option is accepted only by the internal CREATE/DROP
   calls owned by this context. It defers View/PITR/ISCP/idxcron lifecycle
   publication for the temporary relation. It does not bypass normal
   privileges, constraints, data writes, physical index construction or
   failure cleanup. Index construction and task registration are separate
   operations: an index may be built while no background task is registered.

Preparation performs read-only history checks only. It does not compact the
global lineage DAG or remove unrelated expired owners. The replacement and
physical objects remain invisible outside the transaction.

Before entering `Prepared`, all local and remote preparation executors have
finished and their results are readable from the same workspace. No callback
may publish a temporary task or View dependency after the phase transition.

## Publication phase

After preparation, the operation enters a short coordination section:

1. acquire the View lifecycle row with the original cancellation and wait
   boundary;
2. acquire the SNAPSHOT lifecycle row with the existing `FOR UPDATE` and
   FastFail policy;
3. advance the workspace snapshot with `Workspace.AdvanceSnapshot` to a
   value no earlier than the current transaction snapshot and the gate-lock
   service time, then create fresh publication-read contexts;
4. execute the existing SNAPSHOT no-op UPDATE write barrier; no proactive
   snapshot advance occurs after that write;
5. reread and validate the source, branch DAG, Snapshot/PITR owners, View
   metadata, foreign keys, publications and task targets; and
6. protect required old generations, atomically replace the source relation,
   migrate final metadata and register tasks for the final relation only.

The operation does not restore the old transaction snapshot or move it
backwards. `T_data` remains independent and continues to define copied data.
If either gate row is absent or not ready, the operation fails through the
normal cleanup path; a missing row is not treated as a successful lock.

Lineage cleanup is split at this boundary. Synchronous old-generation
reference maintenance needed to make the replacement safe remains in the
publication transaction. Catalog-wide compaction of unrelated history is left
to the existing GC path and is never used to shorten this critical section.

The final View update or invalidation is performed once for the business
relation. Temporary relation names never become View dependencies or published
background tasks. Task metadata is generated after the final table ID and name
are known; arbitrary SQL string substitution is not used.

## Concurrency and linearization

* A different-table ALTER can copy and build indexes while A is paused in
  preparation. Its only serialization point is the short publication section,
  so B can acquire its own table and commit.
* A same-table ALTER waits on the source table lock. Once the predecessor
  commits, it rereads the current source identity and definition before using
  or discarding its prepared result.
* A Snapshot/PITR owner published after preparation is observed by the
  publication reread. If it invalidates the planned replacement, the attempt
  rolls back and the bounded retry starts from a fresh transaction.
* A Snapshot/PITR owner published after the replacement commit resolves the
  committed relation and its commit boundary through the normal catalog path.
* Optimistic writers continue to use the existing SNAPSHOT write barrier and
  validation. The optimization does not replace `FOR UPDATE` with a shared
  read or remove the UPDATE.
* View invalidation, owner protection and old-generation reclamation are
  serialized by the same View -> SNAPSHOT order, so no reverse lock edge is
  introduced.

## Retry, prepared statements and cleanup

Ordinary publication waiting is not a retry of COPY ALTER. A retry marker is
created only by the COPY ALTER coordination helper and is bound to the current
transaction and attempt generation. Other lock errors, business errors,
cancellation, cleanup failures and commit errors retain their original result.

For the single permitted complete retry, the executor stops and drains all
preparation work, rolls back the whole transaction, verifies temporary relation,
task, lineage and lock cleanup, restores session state, and reparses the
statement in a new transaction. Text and binary prepared execution retain the
captured AST binding, database remap, case mode, SQL mode and parameters; a
prepared retry cannot compile against a newly selected default database before
that binding is restored. Each attempt owns an independent plan and generation.

No savepoint, nested statement transaction, partial replay or client-visible
timeout extension is introduced. At most two complete attempts share the
original request deadline.

Failure injection and cancellation at relation creation, row copy, index
build, each gate, directory refresh, history protection, old-table deletion,
rename, task registration and commit must leave either the original committed
relation or the complete committed replacement. A later ALTER must succeed;
there may be no half-replaced catalog, temporary task, orphan index or lineage
edge.

## Alternatives and non-goals

* Holding the global gates throughout preparation is rejected because it keeps
  the reported serialization and timeout behavior.
* Independently committing the replacement is rejected because it breaks one-
  transaction atomicity and caller rollback semantics.
* A generic savepoint or nested `StartStatement` is rejected because it would
  change workspace write boundaries and obscure ownership of rollback.
* Removing the SNAPSHOT UPDATE or changing it to `FOR SHARE` is rejected
  because optimistic validation and GC depend on the existing write conflict.
* Recopying on every directory version change is rejected because contention
  could turn the latency improvement into unbounded duplicate work.
* Explicit transactions, SI/optimistic modes, restore and the separate #28317
  long-lived-lock behavior are not changed by this revision.

## Validation and rollout gates

The implementation must retain the production-entry regression in
`pkg/tests/issues/issue_28319_test.go` and the distributed case
`test/distributed/cases/ddl/alter_copy_publication.sql` with its `.result`.
The distributed case uses the existing `ALTER TABLE ... ALGORITHM=COPY`
syntax; no new SQL command is introduced. It asserts historical reads, View,
auto-increment, foreign keys, ADD/DROP PRIMARY KEY and rollback behavior.

Required evidence before merge:

1. deterministic different-table and same-table barriers, including one-copy
   publication reuse;
2. Snapshot/PITR/lineage/View/GC interleavings and owner-scope coverage;
3. prepared text and binary execution across database changes and a real
   two-transaction retry;
4. synchronous and asynchronous index/task paths, cancellation and every
   publication failure boundary;
5. explicit-transaction, SI, optimistic, multi-CN and rollback compatibility;
6. affected package tests, race tests, BVT `.sql/.result` comparison, and the
   original nightly ADD/DROP workload; and
7. paired baseline/head measurements for 1/2/4/8 different-table workers,
   including every failed attempt, lock/copy/index/commit timing, CPU and I/O.

The hard performance criteria are: a paused A does not prevent B from
committing; copy and synchronous index work are outside the global gate;
ordinary contention does not increase copy count; single-worker median does
not regress by more than 10%; concurrent throughput and P95 improve on the
paired baseline; and no unexpected connection failure or timeout appears in
three equivalent nightly runs. Missing remote or workload evidence remains a
pending gate and cannot be replaced by a small local test.
