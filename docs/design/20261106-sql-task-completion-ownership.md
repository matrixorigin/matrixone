# SQL task execution ownership and recovery

## Problem and scope

A SQL task with no timeout may run indefinitely, including across several
statements. Age, CN membership, and a heartbeat timeout do not prove that its
SQL has stopped. The existing age reaper can therefore admit a second run
while the first is still executing. The async failure repair also guesses a run
from task/account/CN, and can corrupt a later run; async completion and run
repair can be committed separately, leaving a permanent RUNNING record.

Fix #32279, #32280 and #32281 together at the task-storage boundary. Ordinary
SQL statements receive no additional query, RPC or per-statement check.

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
  use the same transition without an async row. An uncertain transition is
  reconciled against both run IDs without starting B. Ordinary SUSPEND and
  SET SCHEDULE do not cancel retries of an already claimed firing. A late owner or scheduler completion
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
firing. Non-SQL tasks retain their existing lifecycle. Scheduler
expiry may complete or garbage-collect the async row; it is not proof that the
body stopped and must never mark even its exact pointed-to run terminal on the
basis of lost heartbeat. Generic heartbeat/allocation
preserves the storage-only pointer.

## Bounded recovery work and scheduling

Normal runner fetching keeps its existing single query. An unclaimed SQL defer
(including a busy guard, exhausted credit, unavailable store, or failed CAS)
or a nonempty receipt worker enables exceptional fetching. A monotonic
failure generation prevents a concurrent defer from being lost while a scan
finishes. A complete scan with no newer defer and an idle receipt worker exits
this mode.

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
fast query times out, fall back to a bounded primary-key page that advances.
Service and storage decorators preserve the caller's context. Since a decorator
can filter after LIMIT, a short decorated page is not proof of exhaustion; use
the raw-ID fallback unless the queried store is known to apply LIMIT last.
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
its scan cost is bounded by the result limit; if exclusion queries time out,
the bounded raw fallback may advance slowly. No parent registry, per-run timer
or new background scanner is introduced.

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
Repeated storage-refresh failures and scheduled-task internal statement errors
share fixed-key log budgets; run history, statement accounting, and ordinary
client SQL logging remain intact.

## Operations and compatibility

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

Fresh catalogs and a new v4.0.8 cluster upgrade add the pointer, schedule
generation and `(account_id,status,task_name)` run index. Preserve the frozen
v4.0.7 entries/offset and the original historical DDL, so an existing or
interrupted v4.0.7 upgrade can complete before v4.0.8. Require the new writer
protocol before the cluster migration. This protocol check alone cannot prove
that a partitioned older process has stopped, so upgrading also requires all
old writers to be stopped or isolated. Legacy queued SQL firings are
invalidated rather than run under the new protocol. Restores must keep task
definitions, async rows and runs from one consistent snapshot. Downgrade is
unsupported until services are stopped and pending async items and
RUNNING/UNKNOWN runs are reconciled.

## Validation

Test admission, completion and A-to-B retry, lost/late COMMIT, no-claim CAS
racing a claim, DROP/recreate, FORCE/RESUME, tenant isolation, shutdown,
cancellation, timeout, real and unknown SQL errors, and recovery after a long
metadata outage. Challenge stale batches with concurrent admission, malformed
IDs, generation bounds, and large backlogs. Validate fresh and upgraded
catalogs, SQL permission/warning behavior, one- and multi-CN operation,
non-SQL isolation, fixed logging, queue bounds and no business replay.

Compare baseline and change in alternating runs for manual and scheduled task
latency/throughput, realistic history and concurrent unrelated work. Target
p95 at most max(10%, 2 ms) above baseline and throughput at least 95% of
baseline; investigate and optimize a repeatable miss before delivery. Review
write amplification before adding any async index. Run affected-package race
and incremental SCA on the final rebased tree; do not infer distributed or
performance guarantees from mocks alone.
