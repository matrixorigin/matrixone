# Apply ordinary appends outside the TN commit queue

**Status:** Proposed; implementation review is pending.

**Related issue:** [matrixone#30623](https://github.com/matrixorigin/matrixone/issues/30623)
**Implementation PR:** [matrixone#30629](https://github.com/matrixorigin/matrixone/pull/30629)

## Why this design is required

The change in #30629 moves ordinary append allocation and append installation out of
the serialized TN commit-queue path. It changes ownership and ordering across the
transaction layer, table append state, append MVCC, object catalog entries, transfer,
rollback, and replay. The implementation is therefore reviewed as a concurrency and
lifecycle design, rather than as a local queue optimization.

The goal is to reduce queue work while retaining the ordering-sensitive parts of
deduplication and commit preparation. The change must not make a row visible before
its transaction is committed, and must not lose a transfer-generated tombstone or a
rollback cleanup operation.

## Current and target architecture

The old path performs row allocation, append installation, pre-prepare deduplication,
prepare-commit, and commit preparation in the transaction worker. The target path is:

```text
transaction append
  -> Freeze: select appendable object, reserve row range, install ordinary append
  -> Freeze: perform the required local transfer/dedup work
  -> pre-WAL queue: ordered pre-prepare/dedup and prepare-ts assignment
  -> pre-WAL queue: PrepareCommit, including queue-side transfer-generated changes
  -> WAL/apply queues: durable commit and final MVCC/catalog publication
```

`tableSpace.FreezeApply` is the publication boundary for the physical destination of
ordinary appends. `ReapplyFrozen` is used when transfer changes the tombstone workspace
after the stable destination has been reserved; it reapplies the same logical rows to
the already reserved destination instead of allocating a second destination.

The queue continues to own transaction ordering, prepare timestamp assignment, and
the transfer/delete work that can be generated while flush or merge commits. It does
not assume that ordinary append installation is still pending in the queue.

## Invariants

### Row reservation and visibility

1. A row range is reserved by exactly one transaction and has a stable physical
   destination. Retry or reapply uses the existing destination and cannot allocate a
   second logical row.
2. Installing an append into an object is not the same as making it visible. Append
   MVCC and transaction/catalog state remain authoritative for visibility.
3. A transaction receives its prepare timestamp only in the ordered pre-WAL path.
   Freeze must not publish a committed state or bypass deduplication.
4. Append nodes are ordered by their prepare timestamp once one exists. Before that,
   the uncommitted state is retained and is treated as not visible by pre-prepare
   readers, except for the reader transaction's own changes where the existing
   visibility contract permits them.

### Deduplication and transfer

1. Freeze and pre-prepare use their phase-specific visibility rules; neither phase
   may treat a physically installed but uncommitted append as committed data.
2. Transfer mappings are applied to the tombstone workspace before the corresponding
   duplicate decision is finalized. Transfer-generated delete/append work that is
   created by flush or merge remains represented in the queue-side path.
3. A merge/flush drop and its replacement create entry are observed consistently by
   transfer, incremental object traversal, replay, and rollback. A stale source row
   must not survive merely because the catalog entry and transfer mapping were read at
   different lifecycle points.
4. Reapplying frozen rows is idempotent with respect to the reserved destination and
   preserves the row cardinality used by transfer mapping.

### Failure, rollback, and restart

1. Failure in `PrepareApply` or `ApplyAppend` closes the outstanding append drivers
   and releases reservations/handles owned by the transaction.
2. A transaction that fails deduplication or prepare-commit cannot leave a visible
   append, PK-index entry, or transfer sink that outlives the transaction.
3. Rollback of a transferred tombstone cleans up published temporary objects and
   ownership markers, and does not remove objects owned by another transaction.
4. WAL/replay reconstructs the append and MVCC state without relying on an in-memory
   freeze marker. Restart may rebuild appendable-object bookmarks/max-commit state;
   those are accelerators and never visibility authority.

## Linearization points and state transitions

For an ordinary append, the logical transition is:

```text
workspace append
  -> table append lock selects object and reserves destination
  -> FreezeApply installs the physical row range
  -> transaction remains uncommitted and is visible only under txn MVCC rules
  -> ordered pre-prepare performs dedup and assigns prepare-ts
  -> PrepareCommit records the commit ordering
  -> ApplyCommit publishes committed MVCC/catalog state
```

The reservation/installation point is not a commit linearization point. The commit
linearization remains the existing MVCC/apply-commit publication. For transfer rows,
the transfer mapping and its cleanup ownership are part of the transaction's prepare
and commit/rollback lifecycle; a failed publication must be recoverable without
leaking the temporary sink or leaving a half-transferred tombstone.

## Lock and wait ordering

The required lock order is:

```text
table append lock
  -> per-appendable-object admission/gate
  -> object/append MVCC lock
```

The queue-side writer path may have priority over a Freeze writer for the same object,
but the priority gate is only an admission mechanism. It must not be held while
waiting for transaction completion, WAL, I/O, another object, or a table lock. No path
may acquire the object MVCC lock and then acquire the admission gate or table append
lock. This prevents Freeze/PrepareCommit inversion and avoids turning a per-object
optimization into a table-wide serialization point.

Read-side object-list traversal uses the published immutable/read snapshot. Writers
copy and publish a new tree snapshot; readers do not copy the tree and do not observe
partially written nodes.

## Resource bounds

* Admission state is per appendable object, not per table history object.
* No goroutine is created per object or per transaction solely for FreezeApply.
* A Freeze operation owns only its transaction append contexts and reserved row ranges;
  rollback releases every driver and temporary reference on all error paths.
* Queue-side transfer sinks and retry/reapply state are bounded by the transaction's
  transfer batch and are closed on commit, rollback, or cancellation.
* Object-list bookmarks/max-commit values are bounded in-memory metadata and may be
  recomputed after restart.

## Compatibility and recovery

This is a transaction execution-order change, not a format change. Existing WAL and
replay must remain authoritative for durable state. Mixed-version operation must not
assume that a remote reader can see a Freeze-only in-memory marker; the reader must
use the normal MVCC/catalog state. On restart, any partially completed in-memory
Freeze is discarded and replay reconstructs the transaction outcome.

## Alternatives considered

* **Keep all append work in the queue:** simplest ownership model, but retains the
  queue CPU and lock contention that motivated #30623.
* **Use a table-wide priority gate:** protects ordering but serializes unrelated
  appendable objects and defeats concurrent append progress.
* **Use an object-level gate with FreezeApply:** chosen design. It limits contention
  to one appendable object, makes queue priority explicit, and leaves the queue as the
  owner of ordering-sensitive prepare/commit work.

## Validation and acceptance criteria

The implementation is acceptable only when all of the following are evidenced:

1. `go test -count=1 ./pkg/vm/engine/tae/db/test` passes, including rollback,
   transfer, merge/flush, and duplicate-row cases.
2. Optimistic-transaction tests pass with concurrent append, update, delete, transfer,
   and replay activity.
3. A write-heavy multi-PK workload shows no increase in duplicate/WW errors and no
   orphan transactions; queue stage metrics cover PreparingWait, PrePrepare, prepare
   commit, WAL wait, and apply wait.
4. CPU and mutex profiles show that moving ordinary ApplyAppend does not create a new
   object-list, MVCC, or admission-gate hotspot. Freeze p95/p99 and queue p95/p99 are
   recorded separately.
5. Object-list scan metrics distinguish data/tombstone objects, appendable and
   non-appendable objects, max-commit filtering, and Freeze from pre-prepare samples.

The review gate for this document is explicit: owners must approve this revision (or
an updated revision) before the implementation is considered design-approved. Open
questions and any invariant change must be recorded here and linked from the PR.
