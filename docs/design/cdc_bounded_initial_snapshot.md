# Bounded and retry-safe CDC initial snapshots

Status: proposed design for MatrixOne PR #27939; independent design approval
for the current revision is pending.

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
7. **Compatibility is fail-safe:** protocol-marked tasks use a distinct daemon
   executor code that old CNs do not register, so an old CN cannot claim a task
   after bounded groups have committed. Unmarked legacy tasks retain the atomic
   path; the implementation never guesses an epoch for a partial legacy task.

## Protocol

New task creation persists an internal protocol marker in `additional_config`.
The marker distinguishes new tasks from legacy tasks and is not a user option.
A task requesting split mode stores the legacy public boolean as `false` plus
the internal marker and uses the
`InitCdcStableEpoch` daemon executor code. New CNs register both the legacy and
stable-epoch codes. Old CNs register only the legacy code, and task dispatch
resolves the executor before its compare-and-swap claim, so they cannot acquire
a marked task or publish a later watermark after a partial bounded commit.
Keeping the public boolean false is defense in depth for tools that read task
configuration, not the ownership fence.

Each marked table pipeline synchronously obtains its own stable epoch before it
starts a reader or sinker. The epoch is the current source transaction snapshot
at the time that table generation is discovered, not `task_create_time`.
`mo_catalog.mo_cdc_snapshot` stores it under
`(account_id, task_id, db_name, table_name, source_table_id)`. A restart of the
same source table ID reuses the persisted value even if the new transaction has
a later snapshot. A recreated source table has a new ID, so the retired logical-
table row is replaced with a fresh epoch before the new pipeline is published.
This permits wildcard/database tasks to discover tables long after task creation
without reading before the table existed or outside retained history.

For a marked task with `InitSnapshotSplitTxn=true` and an empty watermark:

1. Persist or retrieve the table-generation epoch `S` before publishing either
   pipeline goroutine.
2. Wait until the current transaction snapshot is at least persisted `S`. This
   avoids reading a future timestamp after restart or under clock skew.
3. Open source changes at exactly `S` (capped by an explicit `EndTs`).
4. Begin a target transaction and stream snapshot batches into it.
5. Before adding a batch that would cross either group limit, commit the current
   group without updating the watermark, then begin a new target transaction.
6. On `NoMoreData`, commit the final group and update the watermark to `S`. For
   an empty table, update only the watermark.
7. Subsequent rounds use the ordinary dynamic transaction snapshot and process
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
| New CN disappears after a bounded group; old CN polls the task | Partial snapshot `S` | Empty | Old CN cannot resolve `InitCdcStableEpoch` and does not claim; a capable CN replays `S` |
| Wildcard task discovers a table after task creation or retention expiry | None for the new table | Empty | Persist that table generation's current snapshot and begin at that epoch, independent of task creation time |
| Table is dropped and recreated under the same logical name | Prior generation may have completed or failed | Old logical-table watermark is replaced by detector lifecycle | Replace the retired source-table-ID epoch before publishing the new pipeline |
| Epoch INSERT reports an ambiguous failure | No reader has started for that generation | Empty | Retry reads the durable row first; it reuses a committed epoch or safely chooses a candidate if none committed |
| Task is cancelled, restarted, or deleted | Existing target data follows task command semantics | Task metadata is removed/recreated as appropriate | Delete table epochs with task watermarks; periodic orphan cleanup removes rows whose task no longer exists |

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
- **Persist a per-group cursor:** adds source scan ordering and cursor recovery
  semantics. The implemented catalog state stores only one immutable epoch per
  active table generation and continues to rely on idempotent replay.
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
- new-CN partial commit plus DELETE/PK change cannot be claimed by a legacy
  executor and converges exactly after a capable-CN handoff;
- a wildcard task that discovers a table after the task epoch is outside
  retention selects a current table-generation epoch, reuses it after an
  intermediate commit and restart, applies DELETE/PK-change tail mutations,
  reaches exact target equality, and advances a live watermark;
- limiter FIFO, cancellation, exact-once release, and race behavior.

Unit tests validate protocol correctness and resource bounds without weakening
coverage or substituting sleeps for synchronization. The issue-scale TPCC case
is the end-to-end performance acceptance test.

### Issue #27863 acceptance result

The terminal issue-scale run completed on the exact implementation head
`e0c092ef38c1aa1afb21d46a075e148b1410e91c` on 2026-09-02. It used a freshly
built `mo-service`, a fresh data directory, and ten TPCC tables on the same
source and MatrixOne target endpoints. The task reached terminal initial-
snapshot equality in 235 seconds. Progress was sampled every five seconds;
large tables advanced in bounded increments throughout the run instead of
stalling on retained engine batches. Peak `mo-service` RSS was 3,942,996 KiB.

| Table | Source rows | Target rows | Final watermark |
| --- | ---: | ---: | --- |
| `bmsql_config` | 4 | 4 | `2026-09-01 22:52:56.036237168 -0400 EDT` |
| `bmsql_customer` | 300,000 | 300,000 | `2026-09-01 22:52:55.976237067 -0400 EDT` |
| `bmsql_district` | 100 | 100 | `2026-09-01 22:52:55.952191038 -0400 EDT` |
| `bmsql_history` | 300,000 | 300,000 | `2026-09-01 22:52:56.031447528 -0400 EDT` |
| `bmsql_item` | 100,000 | 100,000 | `2026-09-01 22:52:55.982337393 -0400 EDT` |
| `bmsql_new_order` | 90,000 | 90,000 | `2026-09-01 22:52:55.907715381 -0400 EDT` |
| `bmsql_oorder` | 300,000 | 300,000 | `2026-09-01 22:52:55.891970278 -0400 EDT` |
| `bmsql_order_line` | 2,999,795 | 2,999,795 | `2026-09-01 22:52:55.999679580 -0400 EDT` |
| `bmsql_stock` | 1,000,000 | 1,000,000 | `2026-09-01 22:52:56.073969240 -0400 EDT` |
| `bmsql_warehouse` | 10 | 10 | `2026-09-01 22:52:55.916025702 -0400 EDT` |

The target-side TPCC order/order-line consistency check was empty in both
directions:

- grouped `oorder.sum(o_ol_cnt)` minus grouped `order_line.count(ol_o_id)`: 0;
- grouped `order_line.count(ol_o_id)` minus grouped `oorder.sum(o_ol_cnt)`: 0.

A separate failure-injection run on the same head observed 65,536 target rows,
paused with 196,608 rows already committed and an empty watermark, then applied
a source DELETE and primary-key change before resume. It converged to 2,999,794
rows; the source-minus-target and target-minus-source primary-key differences
were both empty. This exercises partial commit, same-epoch replay, and tail
catch-up through a real `mo-service` rather than only the deterministic unit
