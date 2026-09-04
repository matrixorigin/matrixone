# Bounded and retry-safe CDC initial snapshots

Status: proposed follow-up protocol revision for MatrixOne PR #27939. The
bounded snapshot implementation predates the generation, reservation, restore,
and compaction contracts below; those items are merge requirements, not claims
about the current code.

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
2. **No premature or stale progress:** a partial target commit never advances
   readiness. A table is ready only when its persisted active source table ID
   and generation token match a non-empty watermark. The matching watermark
   changes from empty to `S` only after every snapshot batch has succeeded; a
   watermark from a retired generation is never ready.
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
7. **Compatibility is fail-safe:** each persisted protocol revision uses a
   distinct daemon executor code. The generation/reservation protocol below
   uses `InitCdcStableEpochV2`; CNs that register only legacy or current
   `InitCdcStableEpoch` cannot claim it. Unmarked legacy tasks retain the atomic
   path. Existing V1 bounded tasks require the migration procedure below; the
   implementation never guesses generation state for a possibly partial task.
8. **Claim ownership is fenced across both systems:** every target commit and
   watermark publication renews the exact persisted `(task_runner, last_run)`
   daemon claim. Each physical target `(target server, sink database, sink
   table)` has one MySQL-compatible user lock, held by a pinned target session
   for the pipeline lifetime. The lock name intentionally excludes the CDC
   task ID. DDL and data transactions use that session. A replacement claim
   generation or a different task mapped to the same target must acquire the
   same lock and then validate its ownership before target work, so target
   effects are serialized even when a COMMIT outlives the 30-second
   taskservice lease. The claim travels with asynchronously buffered
   watermarks and is rechecked by the final SQL writer.
9. **Logical generation changes are durable and fenced:** the active source
   table ID, a monotonically increasing generation token, lifecycle state,
   epoch, and
   generation-qualified watermark are persisted. Every old pipeline fails a
   generation check before DDL, DML, watermark publication, or epoch
   compaction, even when it shares the task's `(task_runner, last_run)` claim.
10. **One task owns one target:** a physical target table may be owned by at
    most one non-terminal CDC task. Explicit table mappings reserve their
    targets in the task-creation transaction. Wildcard/database mappings reserve
    each target atomically at discovery, before epoch persistence or pipeline
    publication. Duplicate reservations fail closed. Resume, restart, and
    recovery revalidate the reservation before acquiring the target-side lock.
    The lock is the runtime backstop for aliases and overlapping owners, not a
    substitute for durable uniqueness. Ownership is released only after all
    target writers stop; a later owner must reset the released target.
11. **Restore and PITR are fail-closed:** MatrixOne catalog recovery cannot
    restore an external target to the same point in time. Restored CDC tasks
    therefore persist `REBUILD_REQUIRED` before normal scheduling can resume.
    They cannot continue from restored epochs or watermarks. An operator must
    run the explicit rebuild protocol, which resets the target and starts a new
    initial-snapshot generation, before normal execution becomes runnable.

Claim validation is bounded to five seconds and is also performed immediately
before target initialization DDL, including the generation-change DROP/CREATE.
Legacy atomic tasks do not pay this additional control-plane round trip.
A transient periodic-heartbeat storage or network error does not prove claim
loss: the runner retains the local generation and retries on the next tick. An
explicit `ErrInvalidTask` proves that the durable `(task_runner, last_run)` claim
was superseded; the runner first removes that exact local generation from
heartbeat ownership, then cancels it asynchronously. The pinned target lock
remains held until any already-running SQL returns and cleanup closes the
session; this is the serialization point that prevents a replacement from
completing ahead of an ambiguous old operation.

## Protocol

New task creation persists an internal protocol marker in `additional_config`.
The marker is versioned and is not a user option. The current implementation's
bounded tasks use protocol V1 and `InitCdcStableEpoch`. The complete protocol in
this document uses V2 and the distinct `InitCdcStableEpochV2` executor code.
Capable CNs register legacy, V1, and V2 codes; older CNs cannot resolve V2.
Task dispatch resolves the executor before compare-and-swap claim, so an
incapable CN cannot acquire a V2 task. Keeping the legacy public split boolean
false is defense in depth for tools that read task configuration, not the
ownership fence.

### Protocol upgrade

Catalog schemas and the V2 executor code must be available cluster-wide before
the feature gate creates or upgrades a V2 task. A V1 task is paused and all its
pipelines are joined before migration. If every mapped table has a non-empty
watermark, the migrator holds each physical target lock, proves exclusive
reservation, verifies the current source table ID, and atomically seeds one
`ACTIVE` generation plus its qualified watermark. Any empty watermark,
unresolved target conflict, changed source table ID, ambiguous metadata, or
inability to obtain a stable target server ID requires reset-and-rebuild; it
cannot be upgraded in place. The task changes its marker and executor code only
in the transaction that seals all V2 metadata. A crash before that commit
remains paused V1; a crash after it is claimable only by V2 CNs.

Each marked table pipeline synchronously obtains its own stable epoch and
generation token before it starts a reader or sinker. The epoch is the current
source transaction snapshot when that table generation is discovered, not
`task_create_time`. A restart of the same active source table ID and generation
token reuses the persisted epoch even if the new transaction has a later
snapshot. This permits wildcard/database tasks to discover tables long after
task creation without reading before the table existed or outside retention.

### Persisted table-generation fence

`mo_catalog.mo_cdc_table_generation` is authoritative for one logical table and
has the unique key `(account_id, task_id, db_name, table_name)`. It stores
`source_table_id BIGINT UNSIGNED`, `generation_token BIGINT UNSIGNED`,
`snapshot_epoch VARCHAR(128)`, `state VARCHAR(32)`, `restore_id UUID`, and
`updated_at TIMESTAMP`. The token is allocated by CAS as the previous token plus
one. States are `INITIALIZING`, `ACTIVE`, and `REBUILD_REQUIRED`.
`mo_cdc_snapshot` retains prior epochs for replay/reset evidence but is not the
active-generation selector.

`mo_cdc_watermark` keeps its logical-table unique key and is extended with
`source_table_id` and `generation_token`. A generation transition replaces that
row with an empty watermark for the new token in the same transaction as the
active-generation CAS. Every later write uses
`WHERE source_table_id = ? AND generation_token = ?`; updating zero rows is a
fence failure. Legacy rows without both fields are never ready for a marked
task. Readiness requires one `ACTIVE` generation row and a non-empty watermark
whose source table ID and token exactly match it. The watermark API,
asynchronous buffer, and final SQL writer carry and compare both fields;
task-level identity alone is insufficient.

The generation transition is:

1. In one MatrixOne transaction, compare-and-swap the current generation,
   invalidate its readiness, persist `INITIALIZING(new source_table_id, G, S)`,
   and retain the retired epoch. A same-token retry reuses `G` and `S`.
2. Cancel the old pipeline and join its reader, sinker, and target session before
   publishing the new pipeline. Join is bounded by target SQL timeout plus local
   cleanup timeout. Timeout leaves `INITIALIZING` and fails closed.
3. Acquire the physical target-table lock and reread both the exact daemon claim
   and `INITIALIZING(..., G, S)`. Only then reset the target and start the new
   snapshot. Every DDL/DML transaction rechecks `G` after lock acquisition.
4. Intermediate commits retain `INITIALIZING` and an empty generation-qualified
   watermark. On final target commit, atomically persist the matching watermark
   and change `G` to `ACTIVE` in MatrixOne.
5. Watermark publication and retired-epoch compaction use a CAS on `G`. An old
   pipeline from the same task run may acquire the target lock later, but its
   retired token fails before it can perform any target or metadata effect.

If the source table is recreated again while `G` is still `INITIALIZING`, the
detector does not append another durable epoch. It cancels and joins G, acquires
the target lock, revalidates G, and resets the target first. It then atomically
replaces G with `INITIALIZING(G+1, Snew)` and deletes epochs older than G+1.
A crash before the metadata transaction leaves G initializing and repeats the
safe reset; a crash after it retries G+1/Snew. This reset-before-replace path
keeps rapid recreation within the two-row transition bound.

No pipeline is published between steps 1 and 2. A crash at any transition
re-enters from the durable state: `INITIALIZING` retries `G/S`, `ACTIVE` resumes
tail processing, and `REBUILD_REQUIRED` admits only the rebuild executor.

| From | Event | Durable transition / guard | Result on failure |
| --- | --- | --- | --- |
| Missing | first discovery | reservation plus insert `INITIALIZING(G1,S1)` | no pipeline; retry the same tokens if commit was ambiguous |
| `ACTIVE(G1)` | source table ID changes | CAS G1 to `INITIALIZING(G2,S2)` and invalidate G1 readiness | G1 remains authoritative if CAS did not commit |
| `INITIALIZING(G)` | source table ID changes again | cancel/join G, lock and reset target, then replace with `INITIALIZING(G+1,Snew)` and compact older epochs | keep G and repeat reset if replacement did not commit |
| `INITIALIZING(G)` | retry/restart | exact task claim, reservation, source ID, G, and S must match | fail closed; never allocate G2/S2 |
| `INITIALIZING(G)` | final group committed | CAS matching watermark and generation to `ACTIVE(G)` | remain initializing and replay G/S |
| Any non-terminal | restore fence | persist `REBUILD_REQUIRED(restore_id)` | unresolved restore fence blocks scheduling |
| `REBUILD_REQUIRED` | explicit rebuild admitted | dedicated executor creates `INITIALIZING(Gnew,Snew)` while normal scheduling remains fenced | remain rebuild-required |
| `ACTIVE(Gnew)` | retired epoch compaction | target lock plus claim/reservation/Gnew checks | retain rows and block further generation churn at the bound |

For a marked task with `InitSnapshotSplitTxn=true` whose current table
generation is `INITIALIZING(G, S)`, regardless of any retired generation's
watermark:

1. Verify the durable target reservation and retrieve the already persisted
   generation token `G` and epoch `S` before publishing either pipeline
   goroutine.
2. Wait until the current transaction snapshot is at least persisted `S`. This
   avoids reading a future timestamp after restart or under clock skew.
3. Open source changes at exactly `S` (capped by an explicit `EndTs`).
4. Acquire the target-server/database/table ownership lock, revalidate the
   daemon claim and `INITIALIZING(G, S)`, then run initialization DDL and every
   target transaction on that pinned session.
5. Before adding a batch that would cross either group limit, validate and renew
   the exact daemon claim and generation token, commit the current group without
   publishing readiness, then begin a new target transaction.
6. On `NoMoreData`, commit the final group, then atomically publish watermark
   `(source_table_id, G, S)` and transition `G` to `ACTIVE`. For an empty table,
   publish only that metadata transaction. Revalidate both fences before the
   target commit and metadata publication.
7. Subsequent rounds run only while `G` remains `ACTIVE`, use the ordinary
   dynamic transaction snapshot, and process the incremental interval after
   `S`.

`InitSnapshotSplitTxn=false` remains a single atomic target transaction. A task
without the internal marker also uses that conservative path even if its old
configuration says split.

### Target ownership and partial visibility

The per-target lock and durable reservation have different roles. The
system-owned cluster metadata table `mo_catalog.mo_cdc_target_ownership` has
the unique physical-target key
`(target_server_id VARBINARY(32), sink_database_key VARBINARY(256),
sink_table_key VARBINARY(256))`. It also stores `account_id BIGINT UNSIGNED`,
`task_id UUID`, logical source identity, `reservation_token UUID`,
`state VARCHAR(32)`, and `updated_at TIMESTAMP`. The service performs global
uniqueness checks; tenant sessions cannot read or mutate other ownership rows.
States are `RESERVED`, `ACTIVE`, `RELEASING`, and `TOMBSTONED`. A conflict never
degrades to an unlocked or shared target.

The target adapter canonicalizes identity before reservation:

- authenticate and obtain a stable server identity (`@@server_uuid` for MySQL,
  or the adapter's equivalent cluster UUID); adapters without a stable identity
  are not eligible for bounded split mode;
- lowercase the URL scheme, canonicalize IPv4/IPv6, convert DNS names to
  lowercase IDNA without a trailing dot, and materialize the default MySQL port
  `3306`; credentials and connection-option ordering are not identity fields;
- normalize database/table identifiers according to the connected target's
  `lower_case_table_names` behavior. Case-sensitive targets retain exact bytes;
  case-insensitive targets store the target-normalized form.

Explicit table mappings acquire all reservations in the `CREATE CDC`
transaction; partial acquisition rolls back the task. Wildcard/database tasks
cannot reserve future tables at creation. At discovery they first resolve the
physical target identity, then atomically acquire `RESERVED` and persist the
`INITIALIZING` table-generation row and epoch in one MatrixOne transaction.
Only that committed tuple may publish a pipeline. An ambiguous commit is
resolved by rereading the same reservation/generation tokens, never by choosing
new ones. Conflict moves the task to a non-retryable error until ownership is
released; it never skips the table silently.

`RESERVED` is retry-owned by its exact task/generation token and becomes
`ACTIVE` when the pipeline is published. Terminal cleanup first moves it to
`RELEASING`, stops and joins every writer, then writes `TOMBSTONED`. A new task
may replace a tombstone only while holding the target-side table lock and must
reset the target before publishing its generation. Tombstones are retained for
at least the task lease plus maximum target SQL timeout, then garbage-collected;
there is at most one ownership row per physical target.

The target-side lock name is derived from the stable server-scoped database and
table keys and excludes account/task/generation IDs. It serializes claim and
task handoff and protects against endpoint aliases or a stale process that
passed catalog validation. Acquisition has a bounded timeout and rechecks both
reservation and table-generation tokens before DDL or DML.

| From | Event | Guard / atomic operation | To |
| --- | --- | --- | --- |
| Absent | explicit task creation | insert all target keys in the CREATE transaction | `RESERVED` |
| Absent | wildcard discovery | insert reservation together with generation/epoch | `RESERVED` |
| `RESERVED(token)` | publish pipeline | same task and generation token | `ACTIVE(token)` |
| `RESERVED(token)` | crash/retry | reread and resume only the same token | `RESERVED(token)` |
| `ACTIVE(token)` | cancel/drop | CAS after preventing new writers | `RELEASING(token)` |
| `RELEASING(token)` | all writers joined | exact token and no live target session | `TOMBSTONED(token)` |
| `TOMBSTONED(old)` | new task reuse | target lock held; allocate new token and require target reset | `RESERVED(new)` |

Split mode deliberately exposes committed snapshot groups to ordinary target
queries before readiness is published. Partial visibility is part of the public
split-mode contract, not an atomic snapshot guarantee. V1 has no target-local
readiness marker: readiness requires an `ACTIVE` source-side generation row and
a non-empty `mo_cdc_watermark` with the same source table ID and generation
token. Operators must not release the target to consumers until both match. A
consumer that cannot consult or be gated by this source-side signal, or that
requires all rows to appear atomically, must use
`InitSnapshotSplitTxn=false`.

Cancel or drop during the initial snapshot may leave committed partial groups
in the external target. Once task metadata is removed those rows have no
target-local readiness signal and the target is considered contaminated. It
must be reset before reuse; a later CDC task is never allowed to
adopt it and continue with a newly selected epoch.

### Restore and PITR

Backup/restore and PITR cover MatrixOne data but not an independently managed
target. Restoring CDC catalog rows is necessary but never proves that target
rows belong to the restored epoch.

`mo_cdc_restore_fence` is scheduler-visible control metadata that is not
rewound with tenant/database/table PITR. It stores `restore_id`, `scope_kind`,
object IDs, and `state` (`PREPARED`, `APPLIED`, `REBUILDING`, `COMPLETE`). For a
full-cluster restore, the restore manifest and bootstrap control record provide
the same monotonic fence before CN task scheduling starts. Missing or ambiguous
restore provenance is treated as an unresolved `PREPARED` fence.

The crash-safe ordering is:

1. Persist `PREPARED` in non-restored control metadata before applying restored
   catalog data or allowing any affected daemon claim/heartbeat renewal.
2. The scheduler predicate rejects affected normal CDC executors; cancel and
   join existing pipelines. The restore fence is also checked before every
   target DDL/DML transaction and watermark write. Apply the restore only after
   all old target sessions have terminated; timeout aborts the restore and
   leaves `PREPARED` durable.
3. In one post-restore MatrixOne transaction, mark affected table generations
   `REBUILD_REQUIRED`, attach `restore_id`, invalidate their readiness, and move
   the fence to `APPLIED`. A crash before or during this step still sees
   `PREPARED` and remains fenced.
4. `RESUME CDC TASK <task_name> 'rebuild'` changes the task to `REBUILDING` and
   selects the dedicated rebuild executor. Plain Resume and Restart are rejected.
5. The rebuild executor reacquires every target reservation/lock, atomically
   replaces old watermark/epoch metadata with a new `INITIALIZING(G, S)` while
   scheduling remains disabled, drops and recreates the target, and replays the
   complete snapshot. V1 supports reset-and-rebuild only; there is no ambiguous
   "fully reconcile" shortcut.
6. After the final target commit, one MatrixOne transaction publishes the
   generation-qualified watermark, changes the generation to `ACTIVE`, and
   marks the affected rebuild/fence `COMPLETE`. Normal scheduling becomes
   eligible only after this commit. A crash earlier retries the same `G/S` while
   remaining rebuild-only.

Scope is conservative: cluster restore affects every CDC task; account restore
affects tasks owned by or sourcing that account; database/table PITR affects
every task whose explicit or wildcard selector can match the restored object.
If dependency resolution cannot prove non-intersection, the whole task is
marked `REBUILD_REQUIRED`. Reset-and-rebuild is complete only when all selected
targets have been recreated, the snapshot at `S` has committed, each matching
generation watermark is non-empty, and no affected generation remains outside
`ACTIVE`. Clearing the restore fence before these conditions is forbidden.

### Epoch retention and compaction

Epoch rows are retained while a generation is partial and while a replacement
generation may need proof that the target contains retired rows. After the
replacement generation has reset the target, completed its initial snapshot,
and durably published a matching generation-qualified watermark, the compactor
acquires the target lock and rechecks the task claim, target reservation, and
`ACTIVE` generation token in the same order as a writer. It deletes only epochs
whose token is older than that exact active token. A retired pipeline may later
acquire the lock, but its token fails before target or metadata effects. The
active generation's epoch remains until terminal task cleanup.

The steady-state bound is one epoch row per active logical table. A generation
transition may temporarily retain the active and retired rows. More than two
rows for one logical table, or total rows above `active tables + transitions`,
is an operational invariant violation: emit a metric/alert, stop admitting
another generation for that table, and retry compaction. Cleanup failure must
not silently permit unbounded generation churn.

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
| Pause or stream close | Earlier committed groups only | Empty | Release batch permit; roll back current group; retain epoch for same-generation retry |
| Cancel or drop during initial snapshot | Earlier committed groups may remain externally visible | Task metadata is terminal or removed | Stop all writers before releasing ownership; classify the target as contaminated and require reset before reuse |
| Legacy task lacks protocol marker | No new partial-commit behavior | Empty | Use one atomic target transaction |
| Owner disappears after a bounded group; an older-protocol CN polls the task | Partial snapshot `S` | Empty | The CN cannot resolve the task's versioned executor and does not claim; a protocol-capable CN replays `S` |
| Wildcard task discovers a table after task creation or retention expiry | None for the new table | Empty | Persist that table generation's current snapshot and begin at that epoch, independent of task creation time |
| Table is dropped and recreated under the same logical name | Prior generation may have completed or failed | Retired watermark is immediately not ready | CAS the durable active generation to `INITIALIZING(G2, S2)`, cancel/join G1, then reset and replay under the target lock with G2 checks |
| Old table pipeline wakes after its replacement completes in the same task run | Replacement target state | G2 is active and ready | G1 may acquire the lock, but its generation CAS fails before DDL/DML/watermark/compaction; it exits without effects |
| Wildcard discovery maps to an already-owned target | Existing owner's target state | Unchanged | Atomically reject reservation before epoch persistence or pipeline publication; the target-side lock independently covers endpoint aliases |
| Old owner is blocked in target COMMIT when another CN claims its expired taskservice lease | Old transaction may be ambiguous | Empty or advanced | The replacement waits on the target ownership lock. After its heartbeat receives the explicit supersession fence, the old generation is removed from local heartbeat ownership and canceled, retains the lock until its in-flight SQL terminates, and cannot publish a watermark. The replacement then acquires the lock, revalidates its newer claim, and replays to exact state. |
| Old owner waits for the target lock while a replacement completes | Replacement target state | Empty or advanced | After acquiring the released lock, the old owner revalidates its obsolete daemon claim, releases the lock, and performs no target operation. |
| Resume or Restart advances `last_run` on the same runner | Existing target state | Preserved | Persist the new token while the request status remains retry-owned, publish it to both runner heartbeat and executor fences, then admit replacement work and publish Running with the same token. |
| Epoch INSERT reports an ambiguous failure | No reader has started for that generation | Empty | Immediately reread the durable row; reuse it if committed, otherwise classify the failure as retryable so the detector attempts the claim again |
| Task is restarted | Existing target data and partial snapshot groups remain | Preserve checkpoint metadata | Retain and reuse every table-generation epoch exactly like its watermark; restart must never choose a new epoch after a partial target commit |
| Crash after restore starts but before CDC rows are marked | Target rows may be ahead of restored catalog | Restored or missing | Non-restored `PREPARED` fence blocks normal scheduling; recovery finishes marking affected generations `REBUILD_REQUIRED` |
| MatrixOne is restored or rewound while the external target remains at a later point | Target rows may correspond to epochs absent from the restored catalog | Restored or missing | Dedicated rebuild executor resets targets and replays new generation; plain Resume/Restart and normal target effects remain fenced |
| Crash during rebuild after target reset or partial groups | Reset or partial new snapshot | Empty for rebuild generation | Remain rebuild-only and replay the same durable G/S; never expose restored progress as ready |
| Replacement generation completes and publishes its watermark | Exact replacement snapshot plus caught-up tail | Matching G is non-empty | Under target lock, recheck claim/reservation/active G and compact only older epochs; alert and stop further generation admission if the bound cannot be maintained |
| Task is cancelled or deleted | Existing target data follows task command semantics | Task metadata is removed | Delete all table-generation epochs with task watermarks; periodic orphan cleanup removes rows whose task no longer exists |

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
- **Delete the prior generation epoch as soon as its replacement is
  published:** unsafe because an overlapping old owner can delete the
  replacement row (or vice versa). Compaction is delayed until the replacement
  has reset the target, completed its initial snapshot, and published progress.
- **Use a staging target table:** changes target DDL, privileges, cleanup, and
  identity semantics, and is disproportionate to the problem.

## Decision log

- Source table recreation is a table-generation transition, not merely a new
  snapshot epoch. Readiness and every irreversible effect carry a persisted
  generation token.
- Target ownership is physical target-server/database/table ownership, not
  task-local ownership. Both a durable uniqueness reservation and a target-side
  lock whose name excludes task ID are required. Explicit mappings reserve at
  task creation; wildcard/database mappings reserve at discovery.
- MatrixOne restore/PITR never implies rollback of an external target. V1 marks
  affected generations `REBUILD_REQUIRED` behind non-restored scheduler fences;
  only `RESUME CDC TASK <name> 'rebuild'` can run reset-and-rebuild.
- Retired epochs are kept only through generation reset and completion, then
  compacted to a steady-state single row per logical table.
- Split mode exposes partial target contents and has only a source-side
  readiness signal. Workloads requiring target-local or atomic readiness use
  atomic mode.
- The `e0c092ef` issue-scale run remains historical evidence. Acceptance of the
  final protocol requires an exact merge-candidate-head run after every
  semantic protocol change.

## Implementation status

This document is the required protocol, not a statement that every row below is
implemented. As of parent implementation head `ceedc8463`:

| Area | Status |
| --- | --- |
| Stable per-table epoch, bounded grouped commits, task claim fence, permit lifecycle | Implemented and covered by focused tests |
| Target session lock | Implemented only as `account_id + task_id + sink database + sink table`; does not satisfy physical-target ownership |
| V2 protocol marker/executor code and crash-safe V1 migration | Required; not implemented |
| Generation-qualified active state, watermark/readiness, and old-pipeline fence | Required; not implemented |
| Durable target reservation, wildcard discovery CAS, canonical server/table identity | Required; not implemented |
| Non-restored restore fence, `REBUILD_REQUIRED`, and rebuild executor/API | Required; not implemented |
| Safe retired-epoch compaction and operational bounds | Required; not implemented |

PR #27939 is not implementation-complete while any required row remains open.
These contracts may instead move to separately approved implementation PRs, but
the current PR must then remove behavior and claims that depend on them and use
the conservative atomic path for every uncovered lifecycle.

## Validation contract

Deterministic tests must prove:

- stable-epoch selection, end-time capping, and clock-skew waiting;
- the eight-batch and byte group boundaries;
- no watermark update for intermediate commits;
- replay after partial commit plus source DELETE/PK-change converges after tail;
- commit/begin/read errors roll back only the active group and remain retryable;
- stale stable snapshots fail closed;
- legacy tasks use the atomic compatibility path;
- legacy/V1 CNs cannot claim V2 tasks; V1 migration seeds only fully provable
  active generations and otherwise requires rebuild, with crashes on both sides
  of the marker transaction remaining safely claimable;
- new-CN partial commit plus DELETE/PK change cannot be claimed by a legacy
  executor and converges exactly after a capable-CN handoff;
- partial old generation, CN exit, source recreation, and fresh capable-owner
  discovery forces target reset and converges to the new generation;
- source recreation invalidates the old non-empty watermark before target reset;
  readiness remains false until the new source table ID/token becomes `ACTIVE`;
- a second recreation while G is still initializing follows reset-before-replace,
  retries safely across both crash points, and never exceeds the two-row epoch
  transition bound;
- an old pipeline with the same task claim blocked before the target lock wakes
  after G2 completes and fails its generation check before every target and
  metadata side effect;
- synchronized A-blocked/B-claim/A-completes handoff proves B cannot enter the
  target boundary until A releases it, B's replay is the final exact target
  state, and A cannot regress the watermark;
- Resume and Restart both publish their new claim to subsequent runner
  heartbeat plus target/watermark fences before replacement work is admitted;
- one transient heartbeat error followed by recovery retains and renews the
  live generation, while explicit supersession removes and cancels the old
  owner and permits replacement startup after lease expiry;
- ambiguous epoch INSERT tests cover both committed-response-lost and
  definitely-not-committed outcomes;
- a wildcard task that discovers a table after the task epoch is outside
  retention selects a current table-generation epoch, reuses it after an
  intermediate commit and restart, applies DELETE/PK-change tail mutations,
  reaches exact target equality, and advances a live watermark;
- two tasks configured for the same physical target cannot both pass creation,
  resume/recovery admission, or the target-side lock, including endpoint aliases;
- two wildcard tasks discovering the same target concurrently have exactly one
  reservation winner before epoch persistence/pipeline publication; ambiguous
  reservation commit reuses its token;
- endpoint canonicalization covers DNS case/trailing dots, IPv4/IPv6 spelling,
  omitted/default ports, credential/query differences, server aliases, and each
  `lower_case_table_names` mode;
- restore/PITR marks CDC tasks `REBUILD_REQUIRED`, rejects Resume/Restart and
  normal target effects, and permits execution only after reset-and-rebuild;
- restore crashes at every PREPARED/APPLIED/REBUILDING boundary remain fenced;
  cluster/account/database/table scopes and wildcard intersections select the
  conservative affected task set;
- repeated source recreation compacts completed retired epochs to the stated
  per-logical-table bound and cleanup failure blocks additional churn;
- split-mode queries can observe partial groups, readiness remains false until
  the final watermark, and atomic mode exposes no partial initial snapshot;
- limiter FIFO, cancellation, exact-once release, and race behavior.

Unit tests validate protocol correctness and resource bounds without weakening
coverage or substituting sleeps for synchronization. The issue-scale TPCC case
is the end-to-end performance acceptance test.

Before merge, the exact merge-candidate head must rerun the issue-scale
snapshot and record final source/target equality, partial-commit CN takeover,
a target COMMIT held longer than the taskservice lease, transient heartbeat
failure and recovery, and source recreation with target reset. Results from an
older implementation are historical evidence only and cannot satisfy this
gate.

### Historical Issue #27863 result

The following terminal issue-scale run completed on the then-current
implementation head
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
tests. It predates the final ownership, restore, compaction, and heartbeat
protocol and is not final-head acceptance evidence.
