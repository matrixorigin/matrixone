# SQL task execution ownership and recovery

Implementation: [PR #32331](https://github.com/matrixorigin/matrixone/pull/32331).
Issues: [#32279](https://github.com/matrixorigin/matrixone/issues/32279),
[#32280](https://github.com/matrixorigin/matrixone/issues/32280), and
[#32281](https://github.com/matrixorigin/matrixone/issues/32281).

## Problem and scope

A SQL task with no timeout may run indefinitely, including across several
statements. Age, CN membership, and a heartbeat timeout do not prove that its
SQL has stopped. The existing age reaper can therefore admit a second run
while the first is still executing. The async failure repair also guesses a run
from task/account/CN, and can corrupt a later run; async completion and run
repair can be committed separately, leaving a permanent RUNNING record.

Fix #32279, #32280 and #32281 together at the task-storage boundary. Ordinary
SQL statements receive no additional query, RPC or per-statement check.

## Design decision

| Approach | Correctness and recovery | Cost and limitation |
| --- | --- | --- |
| Existing age-based reaper and guessed repair | Can admit overlapping SQL and modify the wrong run; split commits can strand RUNNING metadata. | Small, but does not satisfy the ownership contract. |
| Lease and fence every business effect | A lease cannot itself stop SQL; fencing is safe only where every effect, including external effects, accepts the fence. | Broad execution and integration changes, with a new availability dependency. |
| Durable replicated result journal | Could recover known results after the executor process is lost. | Adds persistent infrastructure, upgrade, bounded admission and crash-recovery obligations. |
| Atomic run/async identity and bounded live receipts (selected) | Prevents guessed ownership changes and lets the live service publish a known outcome without rerunning SQL. | A process loss may retain RUNNING and need operator reconciliation; receipts are not durable. |

The selected boundary addresses all three failures without adding a check to
ordinary SQL. It deliberately does not claim exactly-once business effects or
automatic recovery when the owner and its in-memory receipts are lost.

## Ownership and normal execution

* Under the definition row lock, an existing RUNNING run for the same
  `(account_id, task_name)` blocks admission, even if the definition was
  dropped and recreated. Duration and timeout alone never release it.
* A scheduled async row has one internal `sql_task_run_id` pointer. In one
  transaction, validate its runner/epoch, definition, schedule generation and
  exact firing ID; insert RUNNING and set the pointer. Manual execution uses
  the same RUNNING barrier without an async row. An executor starts the gate or
  body only after successful admission.
* The existing `TaskStorage` contract exposes the atomic ownership operations
  and bounded ID scan with value-only requests, so storage decorators can
  forward the same transaction boundary. `TaskService.SQLTaskService` identifies
  the single owner of SQL task recovery. A service wrapper must return that
  owner and the same authoritative storage object; otherwise admission fails
  before SQL. Neither a second recovery service nor a second completion path
  is created.
* Persist the known run result and complete its exact async pointer in one
  transaction. Retry attempts atomically finish attempt A, insert RUNNING B,
  and, for scheduled tasks, move the pointer before B starts; manual retries
  use the same transition without an async row. The already admitted executor,
  not a new-request admission guard, owns the retry. It can retry a failed
  metadata transition for at most five seconds while its execution and service
  remain live. An uncertain COMMIT is reconciled against the frozen result,
  both exact run IDs and, for a scheduled run, its runner, epoch and pointer.
  Only that same live executor may start B after reconciliation proves its
  ownership. No business SQL is run by the metadata receipt worker. Ordinary
  SUSPEND and SET SCHEDULE do not cancel retries of an already claimed firing.
  A late owner or scheduler completion
  must never update another run. A typed/wrapped/joined `ErrTxnUnknown` keeps
  the RUNNING barrier, preserves the original error, and does not rerun SQL.
  Once that unknown reason is durably recorded, release the receipt credit but
  retain RUNNING. This differs from an unknown metadata COMMIT/ACK, which
  retains the already known business result for reconciliation.
* Deleting a definition does not delete its run history. A suspended or
  modified schedule invalidates previously queued, unclaimed scheduled
  firings; ordinary business errors do not permanently disable later firings.

Admission binds identity and reserves capacity from the locked definition,
not caller-supplied names; a mismatched run identity is rejected. Completion
updates only outcome fields and leaves admitted identity, trigger and start
timestamps unchanged. `trigger_count` stays monotonic; SUSPEND, schedule
replacement and FORCE atomically set both counters to
`max(trigger_count, schedule_generation) + 1`. Cron uses a generation
and counter CAS; ALTER changes only its own fields and stale RESUME cannot undo
a newer FORCE. Sequence overflow is rejected. This pointer is narrower than a
new generic task status and permits exact recovery; time-based takeover cannot
prove that SQL has stopped.

## Metadata failure and recovery

TaskService owns a lazy, bounded, metadata-only receipt worker. It is shared
by manual and scheduled runs, makes one bounded storage attempt at a time,
round-robins two FIFO shares, moves failed receipts to the tail and uses a
shared capped backoff. It exits when idle. It never retains SQL text, session,
business transaction or an unbounded error chain. A successful known SQL body
returns OK plus a warning containing its run ID if metadata remains pending;
the user must not rerun the body. A known body error retains its original
identity. Once storage recovers, the live TaskService publishes the exact saved
result without executing SQL again. Service shutdown or process loss can leave
a RUNNING barrier; no in-memory worker is a durable owner after process loss.
Closing the service reports abandoned receipts with a fixed-key event and a
fixed-label counter. An operator must isolate the old executor, reconcile its
business effects and inspect the exact run before using FORCE. Automatic
recovery across process loss would require a separate durable handoff; it is
not guaranteed when durable storage is unavailable.

A metadata transition failure does not consume a configured business retry.
Retry and exact reconciliation share one five-second budget and stop on the
execution context or service shutdown. If the budget or ownership ends before
B can safely start, the invocation reports that the retry was not performed;
the existing receipt worker repairs only metadata. A retry can repeat earlier
successful statements in the task body: this protocol does not promise
exactly-once business effects. Manual EXECUTE TASK retains the existing
client-disconnect behavior; scheduled tasks retain their runner cancellation.

Manual EXECUTE remains synchronous on the frontend request stack. The original
request context is passed to lookup; a nonblocking provisional guard is acquired
first. After lookup, the existing frontend detaches its cancellation and
deadline before durable run admission. Consequently the request's default
24-hour session deadline does not bound the remaining wait, and a disconnect
or KILL after detachment does
not stop the task. TaskService shutdown requests execution cancellation. A
positive task timeout bounds the body of each attempt; a zero task timeout
does not impose a whole-task limit. Each internal statement still uses its own
session timeout (24 hours by default), so consecutive statements can run for
more than 24 hours in total. Admission and metadata operations use their own
bounded budgets. The receipt worker owns only metadata, not the SQL body. In
particular, this design does not promise that an expired client request returns
while its admitted body continues; making that promise requires a separately
bounded execution handoff and shutdown protocol.

Reserve a bounded receipt credit before inserting a run. Manual executions and
scheduled executions use separate shares of 4096 and
`min(P + max(P, 8), 16384)` credits, respectively, for runner parallelism P;
each authoritative tenant/name has at most eight per share. A full share
rejects/defer admission before SQL. Admission has an independent bounded provisional guard so
repeated failed admissions cannot consume unbounded memory or goroutines. The
manual provisional guard is keyed by account/name and the scheduled guard by
parent; both are released after admission. Acquire the manual guard before the
first definition lookup. Guard keys are limited to 512 bytes for a name and
50 for a parent, and total guards to `4096 + S + 1`; compute S with checked
arithmetic before clamping. Retained error text is at most 4096 bytes; run and
async runner identifiers are at most 512 and 200 bytes, matching their UTF-8
column limits, while canonical parent and metadata IDs are at most 50 bytes. A
credit transfers to the worker at most once. Two different tentative run IDs
may refer to the same async firing after an uncertain admission rollback, so
receipts are not deduplicated by async ID. Reconcile uncertain COMMIT by locking
and reading the exact run and async identities; never infer the business result
from a missing acknowledgement. Metadata transactions use one owned
connection and context-aware BEGIN/statements/COMMIT; an uncertain connection
is discarded rather than returned to the pool. The foreground completion try
uses a nonblocking authoritative account/name guard and a total five-second budget, transferring its credit
to the worker on failure. After async expiry or GC, finish only the exact run. A FORCE-marked UNKNOWN run
or a demonstrably deleted account supersedes a receipt. A confirmed successful
SQL task whose visibility sync is incomplete returns a warning, not a false
business failure; built-in CN discovery, RPC fanout and local wait share one
five-second budget.

A firing that has not acquired a run has not executed its gate or body. For an
overlap, malformed or stale firing, attempt one exact async CAS on
`(id, RUNNING, runner, epoch, sql_task_run_id=0)` with a bounded context.
At most one such operation runs per TaskService; if busy or unavailable, defer
without keeping a receipt. If the CAS was not committed, the firing can be
reconsidered later, including running once after the earlier run finishes. If
it committed but its acknowledgement was lost, admission sees Completed and
cannot execute it. A CAS racing a new claim can only complete an unclaimed row.
This is reconsideration of an unclaimed firing, not replay of an already-started
SQL body. It is not a guarantee that every firing executes: the existing
scheduler can complete an unclaimed firing when its CN disappears. SQL
executor dispositions, including deferrals and
owned completions, bypass the runner's generic Complete path, which does not
check the pointer. A missing SQL executor also defers rather than calling
generic Complete. A custom SQL executor wrapper must perform only side-effect-free
checks before delegating business execution to the built-in SQL executor with
the same context. If such a wrapper returns early, the runner uses the same
unclaimed CAS to record its result; it cannot complete a concurrently claimed
firing. If the runner stopped and the wrapper returned only that cancellation,
no precheck result has been selected: defer the unclaimed firing instead of
recording a failed execution. A real precheck error remains a result even if
the runner stops concurrently, including when the error is joined with a
cancellation; an early nil is a selected success. A wrapper must return its
context's stop identity when that stop is its only outcome, and meaningful
precheck failures must have an independent error identity. The existing
`func(context.Context, task.Task) error` contract cannot distinguish a bare
independent `context.Canceled` from the same runner stop if both race. Error
conversion can flatten a joined failure into a cancellation-only type; wrappers
must preserve the precheck identity before that conversion. Once the completion
CAS has been issued, a canceled caller or lost acknowledgement
does not prove that the CAS rolled back. Non-SQL tasks retain their existing
lifecycle. Scheduler
expiry may complete or garbage-collect the async row; it is not proof that the
body stopped and must never mark even its exact pointed-to run terminal on the
basis of lost heartbeat. Generic heartbeat/allocation
preserves the storage-only pointer.

A runner without a SQL-task owner excludes SQL firings in its storage query
before LIMIT so they cannot starve other tasks. This does not advertise CN
capabilities to the scheduler or guarantee reassignment of an unsupported SQL
firing; service decorators must preserve the query condition.

## Bounded recovery work and scheduling

Normal runner fetching keeps its existing service query. An unclaimed SQL
defer (including a busy guard, exhausted credit, unavailable store, or failed
CAS) or a nonempty receipt worker enables exceptional fetching. A full normal
page with locally running or recently completed entries removed after LIMIT
also arms a fair wave when queue capacity remains. Every successful normal
page from an untrusted service or storage decorator likewise arms a wave,
whether short or full: the decorator may omit an earlier row while still
returning a full page. Reuse the existing query and
holder path; do not issue a separate trust probe. A monotonic failure
generation prevents a concurrent defer from being lost while a scan finishes.
A complete scan with no withheld siblings, no newer defer and an idle receipt
worker exits this mode.
If the obstruction remains, a later normal poll can arm another bounded wave;
an untrusted decorator can therefore cause
continuing periodic scans, whose cost must be measured.

Before dispatch, validate every normal result as RUNNING on this runner; SQL
firings also require an available SQL-task owner. A decorator can replace
query conditions, so the result itself is not proof of those properties. The
existing running-task owner deduplicates under its lock when enqueueing: an
already queued, running, or recently completed ID is a no-op, not a full
queue. This also covers completion racing with a duplicate in the same page.
An untrusted normal result can contain more rows than requested. When an owner
can arm a fair wave, inspect at most 32 rows per poll and no more than the
configured limit; that wave revisits other persisted IDs using decorated
exact-ID queries. Without an owner, inspect a compliant response up to the
configured limit because there is no fair wave to revisit a truncated prefix.
In either case retain at most the available queue capacity for grouping and
dispatch. The memory already returned by a decorator cannot be bounded by the
runner. Without an owner, progress for unrelated tasks still requires a
decorator to preserve the non-SQL restriction before applying LIMIT; a running
prefix that itself fills that limit can still delay later work.

The runner groups a fetched batch by canonical SQL parent and submits at most
one firing per parent, choosing the smallest task ID as a stable order. It
leaves siblings in storage and continues submitting other parents and non-SQL
tasks. This avoids repeatedly racing the parent admission guard; the guard
remains necessary so a stalled admission cannot occupy every runner slot.
Grouping enters exceptional fetching once. Within a fair wave, it records a
single withheld-work flag and continues the cursor past the inspected page;
when the wave finishes, that flag starts a new wave to revisit the withheld
siblings. Immediately rewinding every grouped sibling could reduce a 32-row
page to one task per turn and delay unrelated parents behind a blocked one.
An entry rejected because the runner queue is full still rewinds immediately
and sets the same pending-work flag, including for new arrivals. This keeps
the candidate eligible when the current wave rolls over.
The selected runner entry records whether siblings were withheld. After that
executor returns a selected, non-deferred outcome,
the runner removes it, releases its slot and offers a nonblocking wakeup to
the existing fetch loop. Guard contention or any other deferred outcome does
not wake immediately. The hint checks runner lifecycle, not the removed entry's
context, which removal cancels. A wakeup is a scheduling hint, not evidence of claim,
completion or rollback. It causes at most four fetch attempts, and the runner
allows at most 32 additional attempts per configured fetch interval. A full
queue, cancellation, storage error, or submission of a SQL firing stops the
burst. Stopping after submission limits competition between pages while the
selected firing reaches admission; another hint can still race, so the
existing parent guard remains authoritative. The normal timer
reconsiders work when hints are coalesced, exhausted or lost. This adds no
worker or persistent per-parent registry. A long-running selected executor
does not wake siblings until it returns, but periodic fetching can still
observe its RUNNING barrier and complete an exact overlapping firing.

Only in exceptional mode, isolate non-SQL and SQL fetches so a poisoned SQL
parent cannot consume all fetch slots. Each lane maintains a bounded cursor
and an end-of-pass high-water task ID. SQL additionally alternates an old
snapshot scan and a separate finite snapshot of new arrivals; continuous
arrivals cannot prevent either from advancing. Fetches advance over every
inspected ID, including locally running tasks, and wrap after the frozen high
water; it must not advance past candidates not yet inspected or a candidate
rejected by a full runner queue. Late allocations/commits are revisited on a
later pass. Rotate lanes by
attempt even for a fetch limit of one, empty results or errors. If a selective
fast query times out, or its source is untrusted even when it returns a full
page, fall back to a bounded primary-key page that advances.
Each fetch, including result validation and grouping, shares the deadline
derived from the runner context and configured fetch timeout. Stopping the
runner cancels in-flight queries and cooperative result inspection. Cancellation
does not turn into a storage failure or move a cursor past an uninspected or
unqueued candidate. During multi-row point-result inspection, leave the cursor
before the uninspected ID and return already selected partial results through
the existing queue-rewind path. Context checks do not bound a decorator that
ignores cancellation or the memory it already returned. Service and storage
decorators preserve that context. A decorated page is not proof that every
earlier ID was inspected, even if the page is full; use the raw-page fallback
unless the queried store is known to apply its conditions and LIMIT directly.
The fetch loop may place an outer deadline around the existing query helper and
batch grouping; any inner query deadline must be no later than that deadline.
If it does, retain the separate runner context for lifecycle classification:
a fetch deadline while the runner is live still arms recovery and retains the
fetch-timeout cause, whereas runner shutdown must not create a failure marker.
Enqueued task contexts remain children of the runner, not the short fetch
deadline. If inspection or grouping stops before enqueueing, rewind the first
unqueued fair candidate. For an unqueued normal candidate, arm a wave if the
runner is live and an owner is available; reuse a generation already signaled
by the page or a concurrent defer. Without an owner, the timer retries.
The fallback reads an unfiltered ordered window of at most 32 lightweight rows:
ID, status, runner, executor, parent and claim pointer. It never decodes task
options or retains SQL bodies. Skip a row without a point lookup only when all
needed snapshot fields are known and prove it cannot match this lane; query
every possible match by exact ID through the decorated service and inspect its
returned rows for the exact identity and eligibility, ignoring other IDs and
dispatching at most one match. Unknown or nullable fields are not proof. The
high-water query remains global and unfiltered. A row that becomes eligible
after its snapshot can be revisited by a later wave or normal poll; queue-full
rewind and cancellation must not skip an unqueued candidate. Decorators may
restrict or post-filter query results, but must not fabricate a task or rewrite
its persisted identity or eligibility fields. Existing in-repository
production implementations forward authoritative rows; test decorators narrow
or inject faults. Custom implementations must preserve this contract.
Query limits,
contexts and runner queue limits remain bounded even when the configured fetch
limit is one. A single recently failing canonical SQL parent is temporarily
excluded before the SQL fetch limit and skipped in the existing wait queue;
this prevents its already-queued work from repeatedly occupying an execution
slot. Only an actual unclaimed metadata failure sets this marker, not ordinary
guard or holder contention, capacity pressure, or cancellation of the caller.
It stays through a successful old-lane query, subject to a finite hard retry
deadline, then causes a new scan; it never changes a firing's ownership. The
single marker bounds memory, and does not promise isolation from arbitrary
numbers of simultaneous failing parents. Neither an individual query plan nor
its scan cost is bounded by the result limit; the raw page may use a table
scan and dense pages can still require one decorated lookup per eligible row.
No parent registry, per-run timer or new background scanner is introduced.

When a stale generation is encountered, reuse that executor and the single
no-claim slot to remove old unclaimed firings in bounded batches for at most
five seconds. Lock the original task definition, then delete at most 128 rows
per transaction, restricted to that parent, SQL executor, Created/Running,
zero pointer, and either the exact legacy ID prefix or the fixed-width
`sqltask2:<taskID>:<sequence>` range below the locked generation. A claimed
run, another parent, and the current/new generation are never touched. Repeat
only while batches remain full and the time budget permits; the next stale
firing continues cleanup. A full batch, timeout or failure keeps exceptional
fetching active even if the original single-firing CAS succeeded. A lost COMMIT
acknowledgement is safe to retry.
This work is opportunistic, uses no new worker, and does not claim constant
latency or fix the pre-existing global HAKeeper scheduler scan. LIMIT bounds
deleted rows, not rows scanned: sparse old/new mixes require a real plan and
forward-progress measurement. If a batch cannot progress, stop within the
budget, retain exceptional fetching and retry with bounded diagnostics.

Malformed options in an existing async row can still prevent the existing
HAKeeper query from decoding its result; exceptional runner fetching does not
repair that pre-existing scheduler data-corruption limitation. Use fixed-key
rate-limited events and fixed-label aggregate metrics. Do not
emit a log or metric series per failing task on each retry. Catalog-dependent
or non-SQL fetch failure must not prevent the other lane from making progress.
Repeated storage-refresh failures and ordinary scheduled-task statement errors
use fixed-key log budgets at their individual reporting sites. Panic,
unknown-commit and rollback-failure diagnostics do not consume those budgets;
run history, statement accounting, and ordinary client SQL logging remain intact.

## Operations and compatibility

If an `ALTER TASK` client disconnects or is canceled after sending the
statement, the server may still commit it. The client error alone does not
establish rollback, and a `SHOW TASKS` read does not prove the earlier request
has finished. Reconcile the request outcome before deciding whether to retry;
a repeated suspend or schedule change can advance the generation again and
invalidate additional unclaimed firings.

`ALTER TASK <name> RESET RUNS <run_id> FORCE` uses the existing task-admin
privilege and affects only the specified tenant/name/run, even when the original
definition was dropped. Before using it, isolate the old executor and
reconcile any partial or committed business effects. FORCE does not stop SQL
or undo effects. In one transaction it marks that run UNKNOWN, completes only
its exact async pointer, and suspends/increments the generation of a current
same-name definition, if present. It records actor, time and the previous
reason. Its transaction locks account, current same-name definition, exact
async row, then exact run, rechecking a preliminary identity lookup. The
account lock serializes CREATE even when the old definition is absent.
Repeating it on a terminal run does not suspend a successor. A lost
COMMIT reply is reported as uncertain and repeating the same command is safe.
`SHOW TASK RUNS` appends `runner_cn`, the last recorded owner, not a liveness
proof; clients requiring an exact column count must accept the extra column.

Fresh catalogs and the pending v4.0.7 cluster upgrade add the pointer, schedule
generation and `(account_id,status,task_name)` run index. The v4.0.0 task DDL
remains fixed at its released definition; the existing v4.0.7 owner applies
the new entries when upgrading a released catalog. MatrixOne requires a
shutdown upgrade, with all old services stopped before the new binary starts.
The released v4.2.8 binary at commit
`10d204b8763f19b2b37ba2147d1d4c1a7828d6d4` uses catalog 4.0.6; the
upgrade target here is 4.0.7. A current-engine test that stages the released
catalog shape verifies upgrade logic but is not an old-binary upgrade test.
Legacy queued SQL firings are invalidated rather than run with stale ownership.
Restores must keep task definitions, async rows and runs from one consistent
snapshot. Downgrade is unsupported.

## Validation

Test admission, completion and A-to-B retry, lost/late COMMIT, no-claim CAS
racing a claim, DROP/recreate, FORCE/RESUME, tenant isolation, shutdown,
cancellation, timeout, real and unknown SQL errors, and recovery after a long
metadata outage. Challenge same-parent bursts, long and blocked admissions,
unrelated work, coalesced wakes, queue-full rewind, wave rollover and shutdown.
Challenge raw-page snapshots with concurrent allocation, nullable or malformed
data, decorators and cancellation, including a late allocation behind a locally
running or post-LIMIT-filtered prefix. Challenge a blocked parent with more
than one page of withheld siblings followed by another parent. Challenge stale
batches with concurrent admission, malformed
IDs, generation bounds, and large backlogs. Validate fresh and upgraded
catalogs, SQL permission/warning behavior, one- and multi-CN operation,
non-SQL isolation, fixed logging, queue bounds and no business replay.
For the upgrade fixture, isolate the staging scheduler's storage and drain the
CN task service before modifying task tables; restart with the normal stores
before exercising the upgrade. Compare performance separately for normal work
and for exceptional fetching; the result is pending a valid current-base run.

Compare baseline and change in alternating runs for manual and scheduled task
latency/throughput, realistic history and concurrent unrelated work. Target
p95 at most max(10%, 2 ms) above baseline and throughput at least 95% of
baseline; investigate and optimize a repeatable miss before delivery. Review
write amplification before adding any async index. Run affected-package race
and incremental SCA on the final rebased tree; do not infer distributed or
performance guarantees from mocks alone.
