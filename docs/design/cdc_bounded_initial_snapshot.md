# Bounded and retry-safe CDC initial snapshots

Status: approved for implementation in MatrixOne PR #27939 by XuPeng-SH in
[review 5120209126](https://github.com/matrixorigin/matrixone/pull/27939#pullrequestreview-5120209126)
after reviewing the exact protocol/code revision at `7fa37b9d61`. This
status-only update records the R3 decision; it does not change the protocol.
The daemon-completion ownership defect demonstrated at
`64a946ca54858db0d4d5c378f5e93450ded20e82` is corrected by the generation-owned
completion implementation and regression tests described below.

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
   in-process executor. A captured Running claim authorizes effects only while
   its durable daemon state remains `Running`; retaining runner and `last_run`
   after a control request does not retain authority. Resume/Restart may use
   their request status only after publishing a new `last_run`, and that new
   generation remains valid when promoted to Running. Before target replay, each
   owner publishes its monotonic daemon-claim generation in the table watermark
   row. Stable watermark SQL matches that generation on the same row, so a
   delayed old writer cannot certify target data changed by a replacement.
9. **Serialized takeover.** Target effects from two owners of the same CDC
   task/table are serialized with a target-side advisory lock. The lock covers
   an actual DDL or transaction interval, not source collection or pipeline
   idle time.
10. **Durable generation detection.** Every stable pipeline start reloads the
    epoch for its current source table ID. An epoch row for a retired ID causes
    an incomplete fresh generation to reset the target before replay.
11. **Generation-bound progress.** A stable watermark is the ordered pair
    `(source_table_id, timestamp)`, not a timestamp alone. A higher source table
    ID replaces the retired generation even when its timestamp is lower; an old
    owner can never overwrite progress from a higher source table ID.
12. **Generation-owned diagnostics.** Every stable-task `err_msg` set or clear
    is update-only and durably conditional on the same `owner_generation` as
    progress. Losing an owner fence stops only the obsolete execution generation;
    it can neither publish its own error nor overwrite a replacement's diagnostic.
    A transient fence-backend failure remains retryable. Reader retirement keeps
    the bounded retry record for a replacement pipeline in the same daemon
    generation. The matching owner fence remains with that diagnostic so owner
    replacement can remove both atomically; terminal task cleanup also removes it.
13. **Bounded generation metadata.** Once target initialization for generation
    `G` succeeds, snapshot epochs below `G` are deleted. Table IDs are monotonic,
    so an obsolete owner can delete only generations older than itself and can
    never delete a replacement's retry anchor. A detector that can already see
    a higher durable generation fails before inserting a stale retry anchor.
14. **Single local daemon admission.** A runner reserves a task ID before the
    durable claim CAS and transfers that reservation atomically into its local
    heartbeat/executor registration. A stale heartbeat must not let one CN
    advance its own durable generation while the preceding local execution is
    still registered.
15. **Consumer-exit propagation.** If the sink command consumer exits before
    producer cleanup, every blocked or later sender is released, transferred
    batch ownership is closed exactly once, and a terminal error is visible
    even when rollback cleared the original lifecycle error.

## Implemented protocol

### Compatibility marker

When a new task requests split mode, task creation stores the public boolean as
`false`, adds the internal `_InitialSnapshotProtocol=stable-epoch-v1` marker,
and assigns `TaskCode_InitCdcStableEpoch`.

Capable CNs register both legacy and stable task codes. Older CNs register only
the legacy code, so task dispatch rejects them before claim. Persisting the
public boolean as false is defense in depth: software that understands only the
old option falls back to one atomic transaction.

The generation columns added to the existing watermark catalog are separately
gated by cumulative protocol v48. Older binaries insert positional six-column
watermark rows, so `ALTER TABLE` must wait until every CN writer runs code that
uses an explicit column list. Stable task creation is rejected while the common
deployment protocol is below v48; legacy and `NoFull` CDC remain available.
After the column is installed, an operational rollback must not reintroduce a
pre-v48 CN whose six-value positional insert cannot address the eight-column
table. Roll back the catalog change before lowering the common protocol, or
keep all CDC writers at v48 or later.

### Per-table stable epoch

Before starting any stable reader or sinker, the executor loads or persists `S`
in `mo_catalog.mo_cdc_snapshot`, keyed by:

`(account_id, task_id, db_name, table_name, source_table_id)`.

The watermark row stores `owner_generation`, the positive microsecond rank
derived from the daemon task's strictly monotonic `last_run`. The first
watermark read is used only to decide whether a missing epoch may be created.
Non-empty same-generation progress without an epoch fails before INSERT, so
that rejected attempt cannot manufacture metadata trusted by its retry.
Progress from a retired source-table generation remains eligible for normal
table-recreation recovery.

On every stable pipeline start, including explicit `StartTs` and `NoFull`,
startup advances the watermark row's `owner_generation` with `GREATEST`,
verifies that its exact generation won, and rereads both watermark fields. The
claim and stable checkpoint update serialize on that same watermark row.
Therefore either the old checkpoint commits first and the replacement reread
observes it, or the replacement claim commits first and the old checkpoint's
owner equality condition is false. Only after this ordering point may target
initialization or replay begin. Keeping ownership on the watermark row avoids
inventing snapshot-epoch rows for modes that intentionally have no initial
snapshot.

Stable progress checkpoint writes are update-only: pipeline startup owns
creation of the progress row. This is required because `RESTART` deliberately
deletes watermark rows. If an old owner passed its preflight before that
deletion, a progress upsert could otherwise recreate the deleted row after the
restart. An update instead affects zero rows, so the retired generation cannot
resurrect progress. Unexpected manual deletion can therefore cause replay after
process restart, but cannot advance or fabricate durable progress; operators
must not manually delete stable-task watermark rows.

`S` is the current source transaction snapshot when that table generation is
discovered, capped by an explicit `EndTs` before persistence. It is not task
creation time. Persisting the capped endpoint also lets a completed bounded task
remain distinguishable from an incomplete pre-epoch snapshot after restart.
This matters for wildcard/database tasks that discover a table much later.

Before an insert, the executor checks for a higher durable source generation and
fails retryably without adding stale metadata. An insert with an ambiguous
result is followed by a read of the durable row. A committed row is reused;
otherwise the operation remains retryable. A post-insert higher-generation
check closes the concurrent-claim window. Rows are retained across pause,
restart, and source-table generations. Task cancellation, deletion, and orphan
cleanup remove them.

The epoch is loaded on every stable pipeline start, including when the
watermark is already non-empty. Stable watermark rows also carry the source
table ID that produced them. A watermark from another generation, or a current
generation watermark below the epoch, proves that the initial snapshot is
incomplete. The stream therefore reads `empty -> S`; it does not merely change a
frontend-local timestamp that the reader never consumes. A retired watermark
generation causes target initialization to drop/recreate the table under the
ownership lock before replay, even if an older epoch row was already compacted.
A newly reconstructed epoch beside non-empty progress claiming the same source
generation is rejected: silently guessing after epoch metadata loss could mix
two unknown source images.

The highest epoch for any other source-table ID is read with the current epoch.
If it is greater than the catalog table ID observed by this pipeline attempt,
the catalog view is stale: the attempt returns a retryable error before target
DDL/DML instead of treating the future retry anchor as a retired generation.

After target initialization has successfully reset generation `G`, epoch rows
with source table IDs below `G` are deleted under the same immutable owner
fence. A completed current generation also removes any retired row left by a
crash between target commit and cleanup. The deletion is one-way
(`source_table_id < G`), so a delayed generation `G-1` cleanup cannot remove `G`
or any later retry anchor. A crash before cleanup leaves extra rows; a crash
after cleanup still has the old generation-bound watermark, which
deterministically requests the same target reset again. Thus steady-state
metadata is one row per logical table, with only concurrently transitioning
generations retained temporarily.

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

Batch ownership is represented by the `ChangeData` fields, not inferred from a
successful or failed method return. A non-nil field remains caller-owned. Every
processor path that transfers a batch to a snapshot group, `AtomicBatch`, or
sinker clears the corresponding field immediately. After every processing
attempt, the table stream calls `ChangeData.Clean`; it therefore releases only
untransferred batches and permits. This rule covers errors both before and after
partial transfer without a second cleanup branch or asynchronous double-free.
Rows needed for metrics are captured before transfer, so observability never
reads a batch that an asynchronous sink may already have released.

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

A stream may retain several admitted batches while staging one target group. It
uses a nonblocking admission attempt that never bypasses FIFO waiters. If no
slot is immediately available, it commits the partial group and releases those
permits before joining the FIFO. This prevents multiple streams with partial
groups from consuming every permit and then waiting on one another.

### Target transaction groups

For an initial snapshot at `S`:

1. Stage source batches for one bounded group without opening a target
   transaction or acquiring the target advisory lock.
2. Before adding a batch that would exceed eight batches or cross 512 MiB,
   flush the current group. A group also flushes when admission backpressure
   prevents immediately acquiring another source-batch permit.
3. Only after the complete group has been collected, acquire the target lock,
   validate the owner, begin a transaction, send the staged batches, and commit
   without a watermark. Release the lock immediately after that transaction.
4. The next group repeats collection without retaining the target lock.
5. At `NoMoreData`, commit the final group, enqueue a fenced watermark update
   to `S`, and release the target lock. The same single post-lock validation
   covers this effect interval; the watermark has separate local and durable
   generation guards.
6. For an empty snapshot or empty incremental round, enqueue only the fenced
   watermark; no target lock or synchronous taskservice heartbeat is needed.

The asynchronous watermark writer validates one shared immutable owner fence
once per task generation in each flush, rather than once per table. An explicit
lost-claim result drops that generation's buffered values. A transient
taskservice/storage result leaves them retryable instead of misclassifying the
owner as stale. Values from a newer generation that arrive concurrently remain
in the uncommitted cache for its next flush. Because claim validation and SQL
cannot be one transaction across taskservice and the watermark updater,
stable-task updates require the existing watermark row's `owner_generation` to
match the buffered claim. They then compare
`(source_table_id, physical, logical)` in SQL: higher source generation wins
unconditionally, and timestamps are monotonic only within one generation. A
replacement on the same CN also removes non-durable cache tiers owned by the
previous fence and replaces the active in-memory fence, preventing either stale
cache priority or a rejected old SQL completion from poisoning the replacement.
Legacy updates are emitted in a separate batch and keep their historical rewind
behavior.

Stable error writes and first-success clears use the same immutable claim token.
They first validate the exact taskservice claim, covering a control request whose
replacement has not reached watermark admission yet. Their SQL then joins only
an existing watermark row whose `owner_generation` equals that token; it never
upserts, closing takeover after the validation. Together these close both
directions of the takeover race:
an obsolete reader cannot make a healthy replacement appear failed, and it
cannot erase the replacement's real error. Stream cleanup persists the final
owned diagnostic before retiring local progress state. It retains the one bounded
diagnostic record and its task-generation fence so a replacement pipeline in the
same daemon generation advances the existing retry count instead of restarting
at one. Publishing a newer owner fence atomically drops the old record, and
terminal task cleanup removes both progress and diagnostics. Legacy tasks retain
their historical error upsert because they have no durable owner column.

`InitSnapshotSplitTxn=false` and unmarked legacy tasks keep one atomic initial
snapshot transaction.

### Daemon-claim and target fencing

Daemon dispatch has a separate, short-lived local admission map keyed by task
ID. Admission is reserved before the durable ownership CAS; no storage or
executor operation runs while the map mutex is held. A successful CAS transfers
the same object from admission into the local running map under that mutex, and
all error paths release the reservation. This prevents both concurrent poll
handlers and a stale-heartbeat self-takeover from creating a durable generation
that has no local heartbeat owner. It does not block takeover by another CN.

Taskservice persists and publishes a monotonic `last_run` claim token for Start,
Resume, and Restart. The CDC executor creates one immutable `OwnerFence` object
for each published claim. Every table pipeline created in that generation
captures the same object. Publishing a later claim replaces only the pointer
used by future pipelines; existing pipelines retain the old token and fail the
next durable check.

The durable column is `timestamp(6)`, and token generation truncates the clock
to microseconds before applying the strictly-monotonic increment. Both fresh
bootstrap and the protocol-gated catalog upgrade install that precision. The
task runner remains the only periodic lease renewer. Effect fencing performs a
read-only, autocommit lookup for the exact `(task_id, task_runner, last_run)` and
the statuses authorized by the captured claim. A Running claim accepts only
current Running. A claim captured after Resume/Restart advanced `last_run`
accepts its matching request status and the later Running publication. All
other combinations lose effect authority. Validation neither begins an explicit
transaction nor changes `last_heartbeat`. A storage error leaves ownership
unknown and is retryable.

Claim checks use a five-second timeout. A transient periodic-heartbeat storage
or network error is not proof of supersession; the runner keeps the generation
and retries. Only explicit `ErrInvalidTask` removes and cancels that exact local
generation.

Local Resume/Restart admission and claim-loss publication share a nonblocking
gate. A heartbeat cannot cancel the old in-memory claim while its replacement
is between durable CAS and local publication; it retries on the next tick.
After publication, the failed heartbeat token must still equal the local token
before removal. A relinquished local routine cannot admit another generation.

The same distinction is preserved below taskservice. `ErrInvalidTask` is
wrapped as an owner-lost control result; transaction cleanup still runs, but the
old stream and pipeline-creation path do not publish it to shared table error
metadata. Other owner-fence backend errors are wrapped as retryable failures.

For each task/table, the sink derives a MySQL-compatible advisory-lock name
from `(account, task, sink database, sink table)`. It polls `GET_LOCK` in
one-second intervals. The sink initialization, connection attempts, and lock
polls inherit the table-callback lifecycle context. Each connection attempt has
a ten-second ceiling and each lock query has a two-second hard deadline,
independent of the user-configured DML timeout. Before
and after every poll it checks local context, pause, and cancel state, so a
successful response racing a control event cannot become effect authority. A
zero response alone means lock contention and continues polling; NULL,
unexpected values, query failure, and the hard deadline exit as retryable lock
failures rather than spinning. Immediately after acquisition it performs
exactly one fresh read-only validation of the immutable daemon claim. The same
pinned target session then executes all protected DDL or one transaction; there
are no redundant per-statement or pre/post-commit claim checks.

The lock is effect-scoped:

- initialization holds it across create/use/drop/create and then releases it;
- source collection and bounded-group staging never hold it;
- each data transaction reacquires it before `BEGIN`;
- commit failure and rollback release it;
- successful intermediate commits release it immediately;
- a final commit releases it after the fenced watermark update has been
  accepted by the updater;
- close performs idempotent best-effort cleanup.

If `RELEASE_LOCK` returns an ambiguous transport or timeout error, closing a
`database/sql.Conn` alone is insufficient because it may return the physical
session to the pool with the user lock still held. The executor marks that
driver connection bad before close, forcing physical-session discard. A clear
zero/NULL response means this session owns no lock and needs no such action.

This boundary is deliberate. A taskservice-partitioned old CN that is idle or
stuck collecting the next source batch owns no target lock, so a replacement is
not blocked forever. If old SQL is actively in flight, the replacement waits;
after acquisition it validates its newer claim before any effect. An old waiter
that wakes later validates its obsolete claim and releases without target work.

If Resume/Restart publishes a replacement on the same CN, an old fence also
fails an in-process identity check before reaching taskservice. If an old target
commit was already in flight, `UpdateWatermarkOnly` compares the incoming fence
with the active local fence and refuses to repopulate cache state cleared by the
replacement. Cross-CN delayed checkpoints are rejected by the watermark row's
durable owner-generation predicate.

The lock serializes generations of the same CDC task/table. It does not prevent
an operator from configuring a different CDC task to write the same physical
target; cross-task target ownership is outside this PR.

## Correction: generation-owned daemon completion

This correction belongs to #27863 / PR #27939. It addresses the deterministic
counterexample in [review 5117936681](https://github.com/matrixorigin/matrixone/pull/27939#pullrequestreview-5117936681).
Normal and race runs of the review-only reproducer both failed at the revision
named above; neither the earlier 55 validation nor existing takeover tests
exercise completion of the old startup after replacement publication.

### Root cause and required contract

A task ID identifies a durable task, and a runner identifies a CN. Neither
identifies one execution: the same CN can reacquire the same task after another
owner. The execution identity is the immutable claim
`C = (task_id, task_runner, last_run)`. Local registration additionally has an
object identity `L`; Resume/Restart can reuse `L` while changing `C`.

Heartbeat-loss handling already removes an old registration without joining
its startup. The old startup can later return an error. Its completion writes
a whole daemon row using only status/runner guards and removes the local entry
by task ID. This can rewind a new claim, erase its heartbeat registration, or
release a replacement as if its startup had failed. Requesting cancellation
does not revoke the old callback's references or prove its completion.

The correction enforces these invariants across the whole lifecycle:

1. Capture `C` when work is admitted. Deferred completion must not obtain its
   identity by rereading a mutable `daemonTask` or a newly queried catalog row.
2. Durable completion matches the originating `C` and the expected control
   status in the same SQL mutation. A prior successful ownership SELECT is not
   sufficient. Zero matched rows means superseded work, not a new task failure.
3. Updates have explicit field ownership. Error reporting changes error payload
   and update time, not runner, heartbeat, or last-run. Restart-claim release
   may clear runner/heartbeat and restore RestartRequested only under the
   originating-claim CAS; it must not rewind last-run or unrelated metadata.
4. Local detach and associated pause/control bookkeeping match both `L` and
   `C`. Checking only the pointer misses same-object reuse; checking only the
   task ID deletes another object. Validation and removal are atomic relative
   to local claim publication, including the durable-CAS/local-publication gap.
5. Attach and initial factory admission preserve the originating identity.
   Reading current task configuration must not authorize an old factory to
   borrow a replacement's claim or attach its routine to the replacement.
6. Losing shared ownership does not suppress cleanup of resources exclusively
   owned by the old execution. Conversely, cleanup must not clear replacement
   lifecycle state. Successful CDC startup completion retains the existing
   intentional registration behavior.

### Correction scope and alternatives

The implementation uses claim-scoped, field-specific mutations plus
generation-conditional local registration changes. Reuse the existing
microsecond claim token and matched-row semantics; no new catalog table,
timestamp format, target lock, or row-processing work is needed. Apply the same
contract to normal startup failure, fresh restart takeover failure, and
Resume/Restart completion. Preserve the public legacy CDC behavior and audit
shared non-CDC callers before changing an internal taskservice API.

Rejected alternatives:

- Pointer-conditional map removal alone leaves both durable ABA and
  same-object generation reuse unprotected.
- A local mutex alone cannot exclude other CNs, and a check followed by an
  unfenced SQL write still has a takeover window.
- Waiting for all old work before allowing takeover makes recovery depend on
  the failed owner; timeout is not proof that old callbacks disappeared.
- Whole-row writeback with only a claim predicate still rewinds a concurrent
  heartbeat within the same claim. Field ownership is required as well.

`UpdateDaemonTaskError` updates only details/update time for an exact Running
claim. Its release form additionally clears the lease and restores
RestartRequested. Pause/Cancel/Resume/Restart status completion uses the narrow
status API with the expected status, runner, and LastRun, preserving a heartbeat
renewed during the local operation. Claim admission also compares the observed
LastRun before advancing it. Timestamp predicates bind `time.Time` parameters
through the same driver as claim writes: formatting an SQL literal independently
can mismatch a DSN using `loc=Local`, even when memory tests pass.

Local completion holds `claimLifecycle` while checking its immutable origin
and conditionally removing its exact registration. Resume/Restart admission and
heartbeat-loss handling use TryLock, leaving the durable request/heartbeat
retry-owned while completion runs. The completion defer executes only after the
factory returns, which is after `Start` closes its attempt's done notification.
Resume/Restart joins that attempt, not the task-runner wrapper's completion
defer; there is no reverse join edge. No map/task-state mutex is held across
completion SQL, whose context is bounded to five seconds. Map removal keeps
the map lock through pause-bookkeeping cleanup, preventing a replacement from
being inserted between those two local effects.

The factory validates its original claim before creating resources, and Attach
checks that claim and the original registration. Before Attach, the factory owns
lifetime cleanup; afterward, the runner does. In particular, a failed old
`Start` must not cancel the shared executor lifetime after a same-object Resume
has admitted a replacement. A superseded completion CAS retains an **attached**
routine for the newer control request. It must not retain a registration whose
factory already failed before Attach: that registration can never acquire a
routine, yet would keep renewing the lease and strand all control requests.
Completion seals that original local claim with `claimLost` and conditionally
detaches it, leaving the entire superseding durable row unchanged. Queued
Resume/Restart callbacks cannot reopen the retired object. The dispatcher can
then finalize pause/cancel or admit a fresh recovery after the existing lease
expires; this does not add a lease-release write or weaken takeover predicates.
Before factory completion, a missing routine still represents pending admission
and is retained. Factory completion itself supplies the phase boundary, so no
additional lifecycle state or background worker is required.

An attached executor whose startup failed must also remain controllable:
`Failed -> Pausing -> Paused` is valid, alongside its existing Resume/Restart/
Cancel transitions. Failed is not proof of finished cleanup; Pause closes and
joins residual readers, fences delayed callbacks, and preserves the normal
watermark-flush barrier. Repeated Pause performs no second cleanup. Failed-state
metrics are retired on the first transition, and the paused gauge advances only
after successful pause completion (including a retry from Pausing).

A backend error is not proof of supersession: the
failed current execution is canceled and detached, without an unfenced write or
continued renewal of a failed owner.

### Acceptance map for the correction

| Boundary | Required oracle |
| --- | --- |
| Normal start and fresh restart, old completion after same-CN ABA | replacement claim/status/heartbeat survive; replacement heartbeat succeeds |
| Same object with a newer Resume/Restart claim | old callback cannot adopt the newer token or remove its registration |
| Attach before/after supersession | only the originally admitted routine can publish into its own registration |
| Concurrent heartbeat and error reporting within one claim | error updates preserve the newer heartbeat and unrelated metadata |
| Current-owner success/failure and duplicate completion | intended status/retry outcome; one effective local cleanup owner |
| PAUSE/CANCEL/Resume/Restart superseding a delayed callback | newer durable control request and its retry ownership survive |
| Factory fails before Attach while a control request supersedes it | no dead-owner renewal; unchanged durable request; dispatcher reaches terminal status or attaches a fresh owner |
| SQL error, cancellation, and ambiguous completion result | no unfenced fallback or lease revival; bounded retry/cleanup ownership |
| Claim admission between durable CAS and local publication | no false removal and no wait cycle between admission and completion |

The regression suite extends existing one-row lifecycle fixtures with barriers:

- `TestDaemonStartupCompletionFencesReplacement`: normal/fresh restart crossed
  with same-CN ABA and same-object Resume; joins the delayed completion and
  checks the entire replacement row, registration, and heartbeat authority.
- `TestDaemonCompletionStorageFieldOwnership`: current/stale/missing claims,
  foreign owners, newer control status, preserved heartbeats, and duplicate
  release; SQL errors and affected-row errors are covered by
  `TestDaemonTaskErrorSQLPreservesLease`.
- `TestDaemonCompletionCleanupOwnership`: successful startup, current failure,
  backend timeout, superseding control request, and duplicate callback cleanup.
- `TestDaemonPreAttachFailureRecoversControlRequests`: legacy/stable CDC crossed
  with normal/fresh-restart admission and all four control requests (16 cases).
  Verifies pending-admission retention, terminal factory cleanup, stopped
  heartbeat renewal, unchanged durable request, rejection of old Attach/queued
  callbacks, real dispatcher recovery, and duplicate completion after recovery.
  One stored row and explicit lease-expiry input suffice; no sleeps, service,
  additional production hook, or volume fixture is needed.
- `TestCdcTask_Pause` covers running and failed attached executors, including
  residual-reader Close/Wait exactly once, callback fencing, and duplicate
  Pause. It reuses a one-reader mock and a buffered notification without the
  former notification-draining goroutine. State-machine tests additionally
  prove Failed -> Pause -> Resume -> Running; existing tests cover starting,
  already-paused, and failed watermark-flush/retry paths.
- `TestDaemonAttachFencesOrigin` and `TestCDCFactoryRejectsSupersededClaim`:
  obsolete factories cannot attach to or adopt a replacement.
- `TestLifecycleCompletionPreservesClaimAndHeartbeat` and
  `TestControlStatusCompletionFencesClaim`: all four local control completions
  preserve concurrent heartbeats and reject newer claims.
- `TestDaemonClaimSQLRoundTrip`: reuses the embedded single-CN cluster and one
  row to check real SQL microsecond precision, parameter encoding, unchanged
  matched rows, conditional error/status writes, and all claim-condition
  consumers. No independent cluster or volume fixture is added.

The focused concurrency cases use measured adaptive race repetition (100 runs
per fast case, in separate focused invocations); owning taskservice/frontend
packages also run in race mode. Real SQL and consumer package checks complement
memory tests. The 55 test uses a locally built service on NVMe and exact small
source/target comparisons for upgrade, snapshot, PK update/delete/insert,
pause/resume, repeated restart, rejected unreachable sink, and process restart.
ABA ordering is proved with deterministic injection, not claimed from live
single-CN SQL timing or from a throughput benchmark.

| Audit | Closure |
| --- | --- |
| Q1: ownership | Original claim owns deferred writes; original registration owns detach; Attach transfers lifetime cleanup to generation-checked runner completion. |
| Q2: waits | Start's done precedes wrapper completion; local admission never joins that wrapper; heartbeat/control admission uses TryLock; completion SQL has a deadline. |
| Q3: growth | No new queue, goroutine, polling loop, or retained callback history; one completion per existing admitted startup. |

Validation of this correction (2026-09-05):

- CGo wrapper, `-count=1 -timeout=240s -coverprofile=...`, owning/consumer
  packages: taskservice 25.350s (90.3%), frontend 12.004s (65.3%), cnservice
  8.544s (64.1%), embed 44.309s (84.9%). The added refreshable-storage assertions
  also passed separately (0.051s); no production code changed afterward.
- Owning race packages: taskservice 28.039s, frontend 28.537s; taskservice
  rerun after the final cleanup/storage tests passed in 27.122s. Six focused
  generation/lifecycle cases each passed 100 race repetitions.
- 55: `make -j12` passed; SHA-256 of all eight changed production files matched
  the local correction. Existing 10-row task, new 3-row snapshot, mutations,
  pause/resume, two restarts, and unreachable-sink rejection passed. Restarting
  the same service/data preserved six rows and replicated a seventh newly
  inserted row, with zero durable table errors. This is single-CN, same-service
  MatrixOne source/sink validation, not a multi-CN or external-MySQL claim.
- Remote evidence is retained under
  `/mnt/nvme/xupeng/mo-cdc-validation/completion-{build,verify,after-restart}.log`.
  The owned validation service was stopped; NVMe retained about 120 GiB free.

No throughput gain is claimed for this correction. The performance constraint
is no added per-row/per-batch work and no new polling loop, retained completion
history, or blocking control-path dependency. This correction restores the
existing claim-ownership contract without a new catalog schema or persisted
protocol. Delivery of the requested bug fix did not independently approve the
whole stable-epoch feature. The subsequent R3 decision recorded in the Status
section approved the complete design and closed the design/merge gate for
implementation revision `7fa37b9d61`; this paragraph records that chronology
and does not reopen the gate.

The follow-up pre-Attach/Failed-state control recovery correction was validated
against PR head `8d031b3cd5517d1367c2e59be1c24a3aa353fc78`: the original eight
review-only failures became green, and the committed recovery matrix covers 16
cases across both CDC executor codes. Normal owning-package tests passed
(taskservice 25.512s, 90.8% coverage; frontend 12.883s, 65.3%); owning race tests
passed (27.023s and 24.527s). Four taskservice lifecycle tests and the frontend
Pause test each passed 100 focused race repetitions. Scoped golangci-lint found
zero new issues. Both focused suites also passed under race on 55 with matching
production/test source hashes. Those 55 runs were deterministic package tests,
not a new service/BVT or throughput run; they created no service or persistent
test data. Logs are `pre-attach-fix-tests.log` and `failed-pause-fix-tests.log`
under the existing remote evidence directory above.

Validation of the local-admission and consumer-exit correction (2026-09-06):

- The same-CN stale-heartbeat recovery, concurrent admission, storage-error
  cleanup, and consumer exit on context/pause/cancel are deterministic barrier
  tests. Each new or changed concurrency case passed 100 independent race
  repetitions; the pre-existing unassigned-runner Restart case also passed 100
  race repetitions after its dispatcher boundary was corrected.
- Full normal packages passed: taskservice 26.384s, CDC 12.003s, frontend
  25.778s. Full race packages passed: taskservice 27.236s, CDC 14.028s,
  frontend 34.195s. Scoped `go vet` passed for all three packages.
- No SQL, wire protocol, row-processing, or target statement semantics changed,
  so no new service/BVT or throughput claim is made for this lifecycle-only
  correction.

## Ownership and wait analysis

| Resource | Acquired by | Terminal release |
| --- | --- | --- |
| Snapshot permit | reader before `collector.Next` | collector error, non-snapshot result, untransferred `ChangeData.Clean`, or sink command completion |
| Source batch | collector; then the non-nil `ChangeData` field until explicitly transferred | untransferred `ChangeData.Clean`, processor-group cleanup, `AtomicBatch.Close`, or sink command completion |
| Sink command consumer | one `mysqlSinker2.Run` invocation | publishes its terminal error, then closes `consumerDone`; blocked/later senders close commands and transferred permits |
| Target transaction | sink executor | commit, rollback, or close |
| Target advisory lock | sink executor for one effect interval | post-commit release, rollback/error, DDL completion, or close |
| Target connection attempt | one sink initialization or explicit connection check | publish live pool on success; close on ping/cancellation failure and close probe-only pools immediately |
| Stable epoch row | task/table source generation and replay endpoint | next successfully initialized higher table generation, terminal task cleanup, or orphan cleanup |
| Watermark owner generation | latest admitted daemon claim for one task/table | next stable owner claim or watermark-row cleanup |
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
| Final target commit succeeds while owner changes | complete target may exist; old buffered watermark is locally or durably fenced | replacement reacquires the lock and replays `S`; its effect is last |
| Old watermark SQL passes its first check, then stalls across takeover | replacement may be ready to replay | replacement claims the same watermark row before replay; either it observes the old checkpoint first or the old checkpoint loses its owner equality check |
| Epoch metadata is missing beside current-generation progress | target image is not attributable to a durable epoch | fail before creating an epoch; repeated admission remains rejected |
| Watermark SQL persistence fails | target committed; fenced value remains retryable | retry async persistence or replay after restart |
| Persisted source history expired | partial target possible | fail closed; operator must recreate after checking/resetting target |
| Pause/cancel during collection | no active target effect after cleanup | release permit; resume reuses `S` |
| Old owner idle after taskservice partition | no target lock | replacement can acquire lock and old generation fails its next fence |
| Old owner blocked in target SQL | active transaction and lock | replacement waits; old cleanup releases; replacement revalidates then replays |
| Old owner waits behind replacement | replacement effect completes first | old post-acquire claim check fails before DDL/DML |
| Pause/cancel while a target-lock poll is queued or in flight | no target effect from the waiter | callback-context cancellation interrupts the query; the post-query check rejects a racing success and releases the pinned session |
| Target connection setup stalls with DML timeout disabled | no target effect has started | each attempt ends within ten seconds; pause/cancel interrupts it earlier, failed handles are closed, and pipeline setup remains retryable |
| Target lock query loses its response | lock ownership may be ambiguous | the independent two-second deadline ends the attempt; best-effort same-session release runs and an ambiguous release discards the physical connection |
| `GET_LOCK` returns NULL or an unexpected value | no trusted target authority | exit after one query as retryable; do not busy-loop on an error response |
| Daemon status becomes PauseRequested/Paused/CancelRequested/Canceled with the same runner/generation | tuple remains but a captured Running claim is revoked | status-aware validation rejects DDL/DML and cleanup does not persist a table-data error |
| Resume/Restart publishes a new generation while retaining its request status | replacement must initialize before Running can be published | only the newly captured request-generation is admitted; the old Running generation fails status/token matching, and the replacement remains valid after promotion |
| Owner validation times out or taskservice is unavailable after lock acquisition | no target effect has started | release advisory lock and connection; classify as retryable, not superseded |
| Advisory-lock release response is lost | server may or may not have released it | clear executor ownership and discard the physical session instead of returning it to the pool |
| Resume/Restart on same runner | old pipelines retain old fence | local identity check rejects future old effects; delayed old watermark admission is ignored |
| Transient heartbeat backend error | claim not proven stale | retain local generation and retry heartbeat |
| Heartbeat remains stale and the same CN polls before recovery | old local generation is still registered | reject the duplicate at local admission before durable CAS; recovered heartbeat renews the unchanged generation |
| Two poll handlers select the same task concurrently | neither owns the task yet | one local reservation reaches durable CAS; the other returns without storage work |
| Sinker consumer exits before producer send/rollback | active target transaction or transferred batch may remain | wake senders through `consumerDone`, close each dropped command/permit, republish a terminal error after `ClearError`, then reader cleanup closes the executor and releases transaction/lock ownership |
| Transient owner-fence check error | claim state unknown | retain/retry the operation and buffered watermark; do not publish a permanent table error |
| Explicit owner-fence loss | obsolete execution generation | clean up local target state and stop without writing shared table `err_msg` |
| Source table dropped/recreated | retired epoch row remains | fresh source ID gets new `S`; target reset occurs under lock |
| Stable task reaches an old CN | unresolved executor code | task remains unclaimed until a capable CN is available |

## Performance model

For `N` source snapshot batches, target commit count changes from approximately
`N` in the historical per-batch path to `ceil(N/8)`, unless the 512 MiB
grouping threshold creates smaller groups. Compared with a whole-table
transaction, target transaction state is bounded.

Each non-empty group adds target advisory-lock acquisition/release and exactly
one read-only daemon-claim lookup after lock acquisition. It adds zero daemon
row writes and no explicit taskservice transaction. At the #27863 snapshot
scale this is tens of short control operations over a multi-minute scan, not
one operation per row or SQL statement. Releasing per group is preferred over
a pipeline-lifetime lock because it bounds takeover wait when source collection
stalls.

Local daemon admission adds one map lookup and a short mutex interval per task
claim, not per heartbeat, source row, or batch. Consumer-exit observation adds
one receive arm to the existing blocking command send. Both changes are outside
the successful row-processing hot path and add no queue, polling loop, timer,
or background goroutine.

The same rule applies to the incremental tail. With a continuously non-empty
table at the default 200 ms interval, claim traffic is at most five read-only
lookups per second per table. The rejected implementation performed four
heartbeat UPDATE transactions per effect (before/during target lock and
before/after commit), or about twenty serialized task-row writes per second per
busy table. For 100 busy tables this removes roughly 2,000 task-row write
transactions per second and leaves about 500 exact-claim reads. One fresh read
cannot be removed without weakening the post-takeover stale-waiter guarantee.

Empty polling rounds no longer synchronously heartbeat taskservice per table.
Fenced watermarks are buffered and one shared claim is checked per active task
generation per asynchronous flush. This avoids work proportional to
`table_count / polling_frequency` while retaining the final persistence fence.
The first successful round performs one exact-claim read before clearing a
diagnostic, and terminal error publication performs one more; neither repeats
on ordinary successful polling rounds or scales with rows/batches. Stable error
publication also skips the legacy cache-miss watermark read. Failed-stream
retirement retains at most one existing diagnostic record per table and adds no
SQL, queue operation, allocation proportional to history, or hot-path work.
Stable and legacy keys share the CN updater but are emitted as separate SQL
batches only when both kinds are present in one flush. Stable upserts add four
small timestamp component casts plus source and owner generation comparisons
on the watermark row; there is no extra metadata join or per-row network round
trip. Pipeline admission adds one watermark-owner UPDATE and one ordered
owner/progress read per table startup. Initial-snapshot tasks also perform the
existing epoch metadata operations. These operations are not on the per-batch
or per-row data path.

The CN-wide initial-snapshot limiter is enabled only when startup classification
proves that a full snapshot is pending. A restarted stream whose durable
generation and timestamp already cover its epoch begins directly in incremental
mode, so its first `collector.Next` does not enter the single-unobserved global
admission path.

The limiter bounds batch count, not exact memory. The learned estimate reacts
immediately upward and decays gradually; this favors avoiding repeated
wide-batch oversubscription over maximizing concurrency after a transient wide
batch.

Validation of the steady-state owner-fence correction on 2026-09-05:

- deterministic 100-effect call-rate coverage observes exactly 100 read-only
  claim validations, with SQL-mock expectations proving no validation
  transaction or heartbeat UPDATE;
- post-acquire stale owner, target-lock wait cancellation, ambiguous lock
  release, same-CN retired-fence watermark admission, and cross-owner target
  ordering all pass focused tests;
- `pkg/cdc`, `pkg/taskservice`, `pkg/frontend`, and `pkg/cnservice` pass their
  complete normal suites; the first three pass their complete race suites;
- the focused concurrency/unhappy set passes 100 race repetitions, and scoped
  golangci-lint reports zero new issues.

These deterministic checks prove operation count and ordering, not production
network latency. The issue-scale snapshot result below remains the available
live throughput evidence; no new multi-CN tail benchmark is claimed.

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
- **Cache owner validation for a local TTL:** lowers read count but is unsafe
  across CN clock skew. An old owner can retain a locally valid lease, wake
  after the replacement releases the target lock, and write last.
- **Use heartbeat UPDATE as the effect check:** proves exact ownership but turns
  every table effect into a write transaction on the same daemon-task row;
  heartbeat renewal belongs to the task runner, not the data path.
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
- real SQL claim round trips, duplicate heartbeat ownership, microsecond-apart
  replacement fencing, and monotonic generation across clock rollback;
- old heartbeat responses during the durable/local publication gap and after
  publication, duplicate local admission, and admission after ownership loss;
- idempotent claim-precision upgrades and task-index checks against relation
  definitions (task tables intentionally have no `mo_indexes` mirror rows);
- successful restart emits no timeout report; actual timeout retains its error;
- shared-fence watermark validation and stale buffered watermark rejection;
- takeover admission ordered against delayed checkpoint SQL, including partial
  replacement replay and same-CN cache publication;
- repeated missing-epoch admission with current-generation progress remaining
  rejected without catalog mutation, while retired-generation recovery remains
  allowed;
- restart with a non-empty watermark reloading the durable epoch, recreated-ID
  reset classification, first collection from an empty generation-local
  position, atomic watermark/generation loading, malformed-catalog rejection,
  equal-watermark claim ordering, and generation-aware monotonic SQL;
- stable tasks with explicit `StartTs` binding all owner-fenced progress to the
  source generation even though they do not split an empty initial snapshot;
- completed-task restart bypassing the initial-snapshot limiter while incomplete
  and recreated generations remain admitted;
- owner loss during pipeline creation and target commit without shared
  `err_msg`, stale-owner error set/clear rejection, update-only missing-row
  behavior, cleanup-after-diagnostic progress retirement, retry counts advancing
  across replacement pipelines through the non-retryable boundary, diagnostic
  gauge retention, full task cleanup, and retryable transient fence failures;
- repeated table generations retaining a bounded epoch set, including a delayed
  older cleanup that cannot delete a newer anchor and a known-stale detector
  that cannot add another low-generation row;
- target-lock exclusion, release after every transaction, reacquisition,
  rollback/error cleanup, stale waiter rejection, local cancellation during
  an in-flight or just-successful poll, hard per-poll deadline, one-query
  NULL/unexpected-result rejection, one owner validation per acquisition, and
  zero owner validations while the lock is busy;
- exact read-only status-and-generation authority matching, including rejection
  of every control/terminal status for an old Running claim and admission of only
  the newly published Resume/Restart startup generation, no explicit validation
  transaction, backend-error propagation, and no redundant refreshable-storage
  ping;
- cancellation-aware connection retry/Ping and closure of failed or probe-only
  database handles;
- same-CN delayed post-commit watermark admission after replacement publication;
- transient heartbeat retention versus explicit supersession cancellation;
- legacy atomic fallback, six-column insert compatibility, v47/v48 catalog and
  creation barriers, and old-CN executor rejection.

The issue-scale acceptance run at implementation head
`e0c092ef38c1aa1afb21d46a075e148b1410e91c` replicated ten TPCC tables
(approximately 5.09 million rows) in 235 seconds. All source/target counts and
per-table final watermarks matched, bidirectional order/order-line consistency
differences were zero, and peak `mo-service` RSS was 3,942,996 KiB.

A separate failure-injection run observed 196,608 partially committed rows with
an empty watermark, then changed the source with a DELETE and primary-key
update. Restart at the same epoch plus incremental catch-up converged to exact
source/target primary-key equality.
