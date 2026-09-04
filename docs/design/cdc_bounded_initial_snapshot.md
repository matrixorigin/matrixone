# Bounded and retry-safe CDC initial snapshots

Status: implementation design under review for MatrixOne PR #27939. This
document describes the protocol implemented by the PR. Independent design
approval is still required by the repository's R3 process.

## Scope

This change fixes one concrete failure mode: a large initial CDC snapshot can
leave tables waiting behind retained engine batches for a long time. The fix
must bound both CN-side retained batches and target-side transaction work
without making retries incorrect.

The change applies to newly created tasks whose public
`InitSnapshotSplitTxn=true` option is encoded with the internal
`stable-epoch-v1` marker. Existing tasks keep the legacy atomic behavior.

It does not introduce a general target ownership service, cross-task target
deduplication, external-target PITR coordination, or a hard upper bound on one
engine batch. Those are separate product problems and are not needed to solve
#27863 safely.

## Problem

There are two independent resource boundaries:

1. A CN must not retain an unbounded number of source batches while target SQL
   is slower than source scanning.
2. One target transaction must not grow with the whole table.

The old split path committed batches while a retry could choose a newer source
snapshot. If a source row was deleted, or its primary key changed, between the
two attempts, replaying the newer snapshot could not remove the old key already
committed to the target.

Using one target transaction for the whole table avoids that retry bug but
removes the target-side bound and contradicts the default split option. Large
tables can then accumulate target locks, undo/redo state, commit work, and
timeout risk without a limit.

## Required invariants

The implementation must preserve all of the following:

1. **Stable source epoch.** Every attempt for one source table generation reads
   the same source timestamp `S`, including after a CN restart.
2. **No premature progress.** Intermediate target commits never advance the
   watermark. It changes from empty to `S` only after all snapshot batches have
   succeeded.
3. **Idempotent replay.** A retry replays `REPLACE` data from the same immutable
   snapshot `S`, so already committed groups converge to `S`.
4. **Ordered catch-up.** Mutations after `S` are applied later through the
   incremental interval `(S, next]`.
5. **Bounded target work.** A split transaction contains at most eight engine
   batches. It rotates before adding a batch that would cross 512 MiB of
   measured allocations. Because an engine batch is indivisible, a single
   batch may itself exceed 512 MiB.
6. **Bounded CN concurrency.** At most eight initial-snapshot batches are in
   flight on one CN. Admission occurs before `collector.Next`, and only one
   newly admitted batch may remain unmeasured.
7. **Fail-safe compatibility.** A stable-epoch task uses a distinct daemon task
   code. A CN that does not implement the protocol cannot claim it. An unmarked
   task never guesses how to resume a partial bounded snapshot.
8. **Exact execution ownership.** Every target effect and buffered watermark is
   tied to the immutable daemon claim generation that created its pipeline. An
   old pipeline cannot borrow a newer Resume/Restart token from the same
   in-process executor. Stable watermark SQL is monotonic, so a delayed old
   writer cannot regress a value already persisted by a replacement.
9. **Serialized takeover.** Target effects from two owners of the same CDC
   task/table are serialized with a target-side advisory lock. The lock covers
   an actual DDL or transaction interval, not pipeline idle time.
10. **Durable generation detection.** Every stable pipeline start reloads the
    epoch for its current source table ID. An epoch row for a retired ID causes
    an incomplete fresh generation to reset the target before replay.

## Implemented protocol

### Compatibility marker

When a new task requests split mode, task creation stores the public boolean as
`false`, adds the internal `_InitialSnapshotProtocol=stable-epoch-v1` marker,
and assigns `TaskCode_InitCdcStableEpoch`.

Capable CNs register both legacy and stable task codes. Older CNs register only
the legacy code, so task dispatch rejects them before claim. Persisting the
public boolean as false is defense in depth: software that understands only the
old option falls back to one atomic transaction.

### Per-table stable epoch

Before starting a reader or sinker for an empty-watermark table, the executor
persists or retrieves `S` in `mo_catalog.mo_cdc_snapshot`, keyed by:

`(account_id, task_id, db_name, table_name, source_table_id)`.

`S` is the current source transaction snapshot when that table generation is
discovered, capped by an explicit `EndTs` before persistence. It is not task
creation time. Persisting the capped endpoint also lets a completed bounded task
remain distinguishable from an incomplete pre-epoch snapshot after restart.
This matters for wildcard/database tasks that discover a table much later.

An insert with an ambiguous result is followed by a read of the durable row. A
committed row is reused; otherwise the operation remains retryable. Rows are
retained across pause, restart, and source-table generations. Task cancellation,
deletion, and orphan cleanup remove them.

The epoch is loaded on every stable pipeline start, including when the
watermark is already non-empty. A watermark below the current epoch proves the
initial snapshot is incomplete and is treated as empty in memory. If a retired
source ID also exists, target initialization drops/recreates the table under the
ownership lock before replay. A newly reconstructed epoch with a non-empty
watermark and no retired generation is rejected: silently guessing after epoch
metadata loss could mix two unknown source images.

Before reading, a restarted stream waits until its current transaction snapshot
can observe persisted `S`. It opens source changes at exactly `S`. If `S` or
required incremental history is no longer readable, the stream fails closed
instead of silently selecting a newer epoch.

### CN batch admission

One process-wide limiter is shared by all CDC executors on a CN. A table must
acquire a permit before `collector.Next`. The permit follows the returned batch
through `ChangeData`, `DecoderOutput`, and the sink command, and is released by
the terminal owner. Error and cancellation paths either transfer or release it
exactly once.

The limiter is FIFO and chooses concurrency in `[1, 8]` from:

`available cgroup/host memory / 4 / learned batch-size estimate`.

The initial estimate is 256 MiB and memory-discovery failure falls back to two
in-flight batches. This is an admission estimate, not a byte reservation: one
indivisible batch may exceed both the estimate and one quarter of current
headroom. Requiring the first unknown batch to report its allocation before
another unknown admission prevents a burst based only on a stale estimate.

Only the FIFO head samples procfs/cgroup state, and sampling occurs outside the
limiter mutex. Release and cancellation therefore do not wait behind filesystem
I/O.

### Target transaction groups

For an initial snapshot at `S`:

1. Begin a target transaction when the first non-empty batch arrives.
2. Before adding a batch that would exceed eight batches or cross 512 MiB,
   validate the owner and commit the current group without a watermark.
3. Release the target advisory lock after that committed transaction. The next
   group reacquires it and validates the same immutable owner claim.
4. At `NoMoreData`, commit the final group, validate ownership again, enqueue a
   fenced watermark update to `S`, and release the target lock.
5. For an empty snapshot or empty incremental round, enqueue only the fenced
   watermark; no target lock or synchronous taskservice heartbeat is needed.

The asynchronous watermark writer validates one shared immutable owner fence
once per task generation in each flush, rather than once per table. A stale
generation's buffered values are dropped. Values from a newer generation that
arrive concurrently remain in the uncommitted cache for its next flush. Because
claim validation and SQL cannot be one transaction across taskservice and the
watermark updater, stable-task upserts also compare the physical/logical
timestamp in SQL and retain the durable maximum. Legacy updates are emitted in
a separate batch and keep their historical rewind behavior.

`InitSnapshotSplitTxn=false` and unmarked legacy tasks keep one atomic initial
snapshot transaction.

### Daemon-claim and target fencing

Taskservice persists and publishes a monotonic `last_run` claim token for Start,
Resume, and Restart. The CDC executor creates one immutable `OwnerFence` object
for each published claim. Every table pipeline created in that generation
captures the same object. Publishing a later claim replaces only the pointer
used by future pipelines; existing pipelines retain the old token and fail the
next durable check.

Claim checks use a five-second timeout. A transient periodic-heartbeat storage
or network error is not proof of supersession; the runner keeps the generation
and retries. Only explicit `ErrInvalidTask` removes and cancels that exact local
generation.

For each task/table, the sink derives a MySQL-compatible advisory-lock name
from `(account, task, sink database, sink table)`. It polls `GET_LOCK` in
one-second intervals, checking pause/cancel and the immutable daemon claim
before each attempt and again after acquisition. The same pinned target session
executes the protected DDL or transaction.

The lock is effect-scoped:

- initialization holds it across create/use/drop/create and then releases it;
- each data transaction reacquires it before `BEGIN`;
- commit failure and rollback release it;
- successful intermediate commits release it immediately;
- a final commit releases it after the fenced watermark update has been
  accepted by the updater;
- close performs idempotent best-effort cleanup.

This boundary is deliberate. A taskservice-partitioned old CN that is idle or
stuck collecting the next source batch owns no target lock, so a replacement is
not blocked forever. If old SQL is actively in flight, the replacement waits;
after acquisition it validates its newer claim before any effect. An old waiter
that wakes later validates its obsolete claim and releases without target work.

The lock serializes generations of the same CDC task/table. It does not prevent
an operator from configuring a different CDC task to write the same physical
target; cross-task target ownership is outside this PR.

## Ownership and wait analysis

| Resource | Acquired by | Terminal release |
| --- | --- | --- |
| Snapshot permit | reader before `collector.Next` | collector error, non-snapshot result, decoder cleanup, or sink command completion |
| Source batch | collector | sink command completion or reader cleanup |
| Target transaction | sink executor | commit, rollback, or close |
| Target advisory lock | sink executor for one effect interval | post-commit release, rollback/error, DDL completion, or close |
| Stable epoch row | task/table generation | terminal task cleanup or orphan cleanup |
| Owner fence | executor generation | immutable object becomes unreachable after all old pipelines stop |

The principal wait chain is:

`limiter permit -> source batch -> target lock -> target transaction`.

There is no reverse edge from the target lock to the limiter. Retained batches
are bounded even while waiting for a target. Releasing the lock after every
transaction prevents an idle source reader from extending this chain across
groups.

## Failure matrix

| Event | Target/watermark state | Recovery |
| --- | --- | --- |
| Read or SQL failure before group commit | earlier groups; empty watermark | rollback active group and replay `S` |
| Crash or ambiguous result after intermediate commit | group may be present; empty watermark | replay `S`; `REPLACE` converges |
| Source DELETE or PK change during retry | partial snapshot at `S` | replay `S`, then apply `(S, next]` mutation |
| Final target commit succeeds but owner changed | complete target may exist; no new watermark from old owner | old fence fails, replacement replays `S` |
| Old watermark SQL passes its check, then stalls across takeover | old value may arrive late | stable monotonic upsert cannot replace the newer durable watermark; a pre-epoch value still identifies an incomplete recreated generation on restart |
| Watermark SQL persistence fails | target committed; fenced value remains retryable | retry async persistence or replay after restart |
| Persisted source history expired | partial target possible | fail closed; operator must recreate after checking/resetting target |
| Pause/cancel during collection | no active target effect after cleanup | release permit; resume reuses `S` |
| Old owner idle after taskservice partition | no target lock | replacement can acquire lock and old generation fails its next fence |
| Old owner blocked in target SQL | active transaction and lock | replacement waits; old cleanup releases; replacement revalidates then replays |
| Old owner waits behind replacement | replacement effect completes first | old post-acquire claim check fails before DDL/DML |
| Resume/Restart on same runner | old pipelines retain old fence | future pipelines use new token; old effects fail closed |
| Transient heartbeat backend error | claim not proven stale | retain local generation and retry heartbeat |
| Source table dropped/recreated | retired epoch row remains | fresh source ID gets new `S`; target reset occurs under lock |
| Stable task reaches an old CN | unresolved executor code | task remains unclaimed until a capable CN is available |

## Performance model

For `N` source snapshot batches, target commit count changes from approximately
`N` in the historical per-batch path to `ceil(N/8)`, unless the 512 MiB
grouping threshold creates smaller groups. Compared with a whole-table
transaction, target transaction state is bounded.

Each non-empty group adds target advisory-lock acquisition/release and daemon
claim checks. At the #27863 scale this is tens of short control operations over
a multi-minute scan, not one operation per row or SQL statement. Releasing per
group is preferred over a pipeline-lifetime lock because it bounds takeover
wait when source collection stalls.

Empty polling rounds no longer synchronously heartbeat taskservice per table.
Fenced watermarks are buffered and one shared claim is checked per active task
generation per asynchronous flush. This avoids work proportional to
`table_count / polling_frequency` while retaining the final persistence fence.
Stable and legacy keys share the CN updater but are emitted as separate SQL
batches only when both kinds are present in one flush. Stable upserts add four
small timestamp component casts per conflicting row to make the durable update
monotonic; there is no per-row network round trip.

The limiter bounds batch count, not exact memory. The learned estimate reacts
immediately upward and decays gradually; this favors avoiding repeated
wide-batch oversubscription over maximizing concurrency after a transient wide
batch.

## Alternatives rejected

- **One transaction for the whole table:** retry-safe but unbounded and ignores
  the public split default.
- **New source epoch on each retry:** bounded but incorrect after source DELETE
  or primary-key changes.
- **Persist a cursor for every committed group:** adds ordering/cursor recovery
  state that is unnecessary when replay at stable `S` is idempotent.
- **Hold the advisory lock for pipeline lifetime:** serializes takeover but lets
  an idle, partitioned, or source-stuck old owner block a replacement forever.
- **Release the lock before an active transaction finishes:** improves liveness
  by violating target serialization and is therefore unsafe.
- **Hard byte reservation below `collector.Next`:** the next engine batch size
  is unknown. Enforcing it requires a source-side batch-splitting contract,
  which this change does not have.
- **Target staging tables or global ownership metadata:** substantially changes
  DDL, privileges, cleanup, and product semantics beyond the reported issue.

## Validation contract

Deterministic tests must cover:

- stable epoch selection, restart reuse, late discovery, EndTs capping, and
  clock-skew waiting;
- stale-history failure without epoch replacement;
- limiter FIFO, cancellation, one-unobserved admission, adaptive sizing,
  exact-once release, and race behavior;
- eight-batch/512 MiB group rotation and one oversized indivisible batch;
- no watermark for intermediate commits and final-only publication;
- partial commit plus source DELETE/PK change converging after replay and tail;
- claim-token immutability across Resume/Restart;
- shared-fence watermark validation and stale buffered watermark rejection;
- restart with a non-empty watermark reloading the durable epoch, recreated-ID
  reset classification, equal-watermark claim ordering, and monotonic stable SQL;
- target-lock exclusion, release after every transaction, reacquisition,
  rollback/error cleanup, and stale waiter rejection;
- transient heartbeat retention versus explicit supersession cancellation;
- legacy atomic fallback and old-CN executor rejection.

The issue-scale acceptance run at implementation head
`e0c092ef38c1aa1afb21d46a075e148b1410e91c` replicated ten TPCC tables
(approximately 5.09 million rows) in 235 seconds. All source/target counts and
per-table final watermarks matched, bidirectional order/order-line consistency
differences were zero, and peak `mo-service` RSS was 3,942,996 KiB.

A separate failure-injection run observed 196,608 partially committed rows with
an empty watermark, then changed the source with a DELETE and primary-key
update. Restart at the same epoch plus incremental catch-up converged to exact
source/target primary-key equality.
