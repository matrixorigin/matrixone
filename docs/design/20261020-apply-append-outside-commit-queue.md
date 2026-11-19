# Apply ordinary appends outside the TN commit queue

**Status:** Approved.
**Approved by:** `misuxi` (manual approval)

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
  -> table append lock selects/publishes a referenced carrier
  -> object freeze lock and MVCC reserve the destination
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
selection:       table append lock (no object freeze/gate/MVCC wait)
reservation:     object freeze lock -> object admission/gate -> MVCC lock
materialization: object admission/gate -> MVCC lock
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

## Selection and reservation lock boundary (2026-10-24)

The table append lock owns candidate selection, validation, creation, and
publication only. Every attempt reads the latest shared candidate through a new
local handle. The old transaction-local handle cache is removed. No publication
occurs after reservation, so a delayed owner cannot restore an older candidate.

Normal appendable carriers are independently committed through
`AllocateAndPublishCommitTS`. Publishing a non-owning candidate before reservation
does not publish transaction rows. The appender reference protects the selected
object; its freeze lock and existing MVCC gate serialize row reservation after
the table lock is released. Capacity and freezing are monotonic. A frozen loser
releases its reference and retries selection without retaining the old handle.
The preparing-transfer branch keeps its existing parent-prepare timestamp and
read-wait/replay contract; it does not allocate an independent visibility boundary.

Candidate rejection validates state and releases references without synchronous
sealing. Capacity exhaustion seals with the final reservation; compaction owns
lifecycle sealing. A legacy-frozen or recovered object can retain an unknown
commit bound until then, which forces conservative scanning rather than skipping
rows. Allocator selection must not acquire an object's freeze or MVCC locks while
holding the table lock.

Physical placeholder materialization follows release of both allocation locks.
Capacity checks use reserved rows, not vector length. Another transaction can
reserve or install a later payload first; materialization extends short vectors
without clearing existing values. Once an AppendNode is attached, padding and
rollback ownership are completed before any additional cancellation exit or
fallible bookkeeping. Failed rows retain their physical offsets until cleanup;
removing padding would break collection of a trailing aborted range.

The current Freeze/pre-prepare context reaches reservation attempts. Cancellation
is checked after acquiring the table lock before selection/creation, and after
acquiring the object freeze lock before attaching a range. These are per-object
admission boundaries, not per-row polling. Preparing-transfer appends retain a
noncanceling lifecycle context after the parent has entered preparation. A
cancellation racing carrier creation may leave one reusable committed empty
carrier; it cannot remove another transaction's committed rows.

Observability uses cached, bounded scan/bookmark counter handles and publishes
counts per object group, with separate Freeze/pre-prepare phases. It avoids
per-object clock reads and metric label lookup. Queue timers sample only executed
work. Queue wait includes producer backpressure; apply wait includes worker
admission. Freeze measures the entire store call, including errors. Append-max
gauges read current counts inside their existing publication mutex, preventing
delayed callbacks from restoring stale snapshots. Retirement still prevents
republication after catalog removal. Unconnected queue-depth/per-step metric
definitions, unused scan statistics, and successful-read/retry diagnostics are
removed.

Local validation distinguishes pinned main, the rebased feature, and each lock
change. The initial padding-only change retained dominant table-lock contention
and was rejected as a completed optimization, even though its final throughput
and client tails were close to main. Acceptance requires current-candidate phase
histograms, CPU/mutex profiles, exact committed-data accounting, and end-to-end
results. Temporary baseline timing probes and macOS CPU-affinity limitations
must be identified separately. Historical throughput figures cannot satisfy the
current candidate's performance gate.
