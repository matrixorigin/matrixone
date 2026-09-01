# Bounded and retry-safe CDC initial snapshots

Status: reviewed design for MatrixOne PR #27939

## Problem

The CDC initial snapshot has two independent resource boundaries:

1. A CN must not retain an unbounded number of engine batches while target SQL
   is slower than snapshot reads.
2. A target transaction must not contain an unbounded number of snapshot
   batches.

The historical `InitSnapshotSplitTxn=true` path bounded the second resource by
committing individual batches, but retries selected a new source snapshot while
the watermark was still empty. If a source row was deleted or its primary key
changed between attempts, the new snapshot no longer contained the old key and
could not remove the row already committed to the target.

Keeping the whole initial snapshot in one target transaction repairs that
correctness bug, but it removes the target-side bound. Large tables can then
accumulate target locks, undo/redo state, commit work, and timeout risk without
a configured limit. It also silently contradicts the default public split
option.

## Invariants

The implementation must maintain all of these invariants:

1. **Stable source epoch:** every attempt of one initial snapshot reads the same
   source timestamp `S`, including attempts after a CN restart.
2. **No premature progress:** a partial target commit never advances the CDC
   watermark. The watermark changes from empty to `S` only after every snapshot
   batch has succeeded.
3. **Idempotent replay:** retrying any committed group uses `REPLACE` data from
   the same immutable snapshot `S`; replay therefore converges to that snapshot.
4. **Ordered catch-up:** source mutations after `S` are processed only by the
   incremental interval `(S, next]`, after the initial watermark is published.
5. **Bounded target work:** a split target transaction contains at most eight
   engine batches and at most 512 MiB of measured batch allocations. One engine
   batch is the unavoidable minimum unit.
6. **Bounded CN retention:** admission happens before `collector.Next`, one
   newly admitted batch must be measured before another unknown batch is
   admitted, and a permit is released exactly once by the terminal owner.
7. **Compatibility is fail-safe:** tasks created before this protocol have no
   stable-epoch marker. They retain the atomic single-transaction behavior; the
   implementation never guesses an epoch for an already partial legacy task.

## Protocol

New task creation persists an internal protocol marker in `additional_config`.
The already persisted `task_create_time` is the stable initial snapshot epoch.
The marker distinguishes new tasks from legacy tasks without a catalog schema
change. It is not a user option. For rolling-upgrade safety, a task requesting
split mode stores the legacy public boolean as `false` plus the internal marker:
an old CN therefore chooses its safe atomic path, while a new CN recognizes the
marker and restores the requested bounded behavior.

For a marked task with `InitSnapshotSplitTxn=true` and an empty watermark:

1. Wait until the current transaction snapshot is at least the persisted epoch
   `S`. This avoids reading a future timestamp under clock skew.
2. Open source changes at exactly `S` (capped by an explicit `EndTs`).
3. Begin a target transaction and stream snapshot batches into it.
4. Before adding a batch that would cross either group limit, commit the current
   group without updating the watermark, then begin a new target transaction.
5. On `NoMoreData`, commit the final group and update the watermark to `S`. For
   an empty table, update only the watermark.
6. Subsequent rounds use the ordinary dynamic transaction snapshot and process
   the incremental interval after `S`.

`InitSnapshotSplitTxn=false` remains a single atomic target transaction. A task
without the internal marker also uses that conservative path even if its old
configuration says split.

## Failure and lifecycle analysis

| Event | Durable target state | Watermark | Recovery |
| --- | --- | --- | --- |
| Read or SQL failure before a group commit | Earlier groups plus prior task state | Empty | Roll back current group; replay snapshot `S` |
| Crash after a group commit | A subset of snapshot `S` | Empty | Replay snapshot `S` with `REPLACE` |
| Ambiguous group commit result | Possibly that group | Empty | Replay snapshot `S`; duplicate rows converge |
| Final commit fails | Zero or more complete groups | Empty | Roll back if possible; replay snapshot `S` |
| Target commit succeeds, watermark persistence lags | Complete snapshot `S` | Empty or stale | Replay `S`; eventual watermark update converges |
| Source DELETE or PK change during retry | Snapshot state at `S` | Empty | Replay `S`, then apply mutation in `(S, next]` |
| Stable epoch or later incremental history is no longer readable | Partial or caught-up target state may exist | Empty or non-empty | Fail closed; never reset to a different full-snapshot epoch silently |
| Pause, cancel, or stream close | Earlier committed groups only | Empty | Release batch permit; roll back current group |
| Legacy task lacks protocol marker | No new partial-commit behavior | Empty | Use one atomic target transaction |

The batch permit ownership chain is:

`limiter -> collector call -> ChangeData -> DecoderOutput -> sink command`.

Every error or cancellation edge either transfers that ownership exactly once
or releases it. Permit release does not wait for memory discovery, target SQL,
or a transaction lock.

## Performance model

For `N` snapshot batches, the historical implementation issued approximately
`N` target commits. The bounded protocol issues `ceil(N/8)` commits unless the
512 MiB byte limit produces smaller groups. Compared with the unbounded atomic
implementation, it caps target transaction amplification; compared with the
historical per-batch path, it reduces commit round trips by up to eight times.

The CN limiter remains adaptive from one to eight in-flight batches and uses at
most one quarter of cgroup-aware available memory according to its measured
batch estimate. Memory discovery is outside the limiter mutex so release and
cancellation remain non-blocking with respect to procfs/cgroupfs access.

## Alternatives rejected

- **One transaction for the whole table:** correct on retry, but resource use is
  unbounded and the public default split option is ignored.
- **Commit batches while selecting a new epoch on retry:** bounded but incorrect
  after source DELETE or primary-key changes.
- **Persist a per-group cursor:** requires catalog migration and source scan
  ordering semantics; it adds state without avoiding replay requirements.
- **Use a staging target table:** changes target DDL, privileges, cleanup, and
  identity semantics, and is disproportionate to the problem.

## Validation contract

Deterministic tests must prove:

- stable-epoch selection, end-time capping, and clock-skew waiting;
- the eight-batch and byte group boundaries;
- no watermark update for intermediate commits;
- replay after partial commit plus source DELETE/PK-change converges after tail;
- commit/begin/read errors roll back only the active group and remain retryable;
- stale stable snapshots fail closed;
- legacy tasks use the atomic compatibility path;
- limiter FIFO, cancellation, exact-once release, and race behavior.

The issue-scale TPCC case remains the end-to-end performance acceptance test;
unit tests validate protocol correctness and resource bounds without weakening
coverage or substituting sleeps for synchronization.
