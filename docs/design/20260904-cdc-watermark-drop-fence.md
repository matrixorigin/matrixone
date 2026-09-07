# CDC watermark fencing during task deletion

Status: implemented in PR #27989 for issue #27666.

## Problem and contract

`DROP CDC TASK` removes the durable task row and all of that task's watermark
rows. Before this change, an executor on any CN could finish a delayed
watermark write after those deletes and recreate an orphan watermark.

The required postcondition is:

> Once deletion of a CDC task commits, no writer can commit a watermark whose
> `(account_id, task_id)` no longer exists in `mo_catalog.mo_cdc_task`.

This is a database-wide invariant. A CN-local flag can reduce local work during
shutdown, but it cannot establish the invariant because the task owner and the
frontend executing DROP may be different CNs.

## Ownership model

| State or resource | Owner | Release condition |
| --- | --- | --- |
| CDC task existence | `mo_cdc_task` primary-key row | DROP transaction commits |
| Cross-CN write permission | lock on the matching task row | guarded watermark statement commits or aborts |
| Local callback generation | `CDCTaskExecutor.callbackMu` and generation | every admitted callback exits |
| Local reader generation | reader shutdown completion channel | every captured reader exits |
| Local terminal tombstone | shared updater on the owner CN | callback and reader completions close |
| Buffered watermark jobs | shared updater queue | terminal delete observes completion of every persistence phase in the admitted queue batch |

The task row is the single durable authority. The local tombstone is not a
distributed deletion record and therefore must have a bounded, explicit local
release owner.

## Durable serialization protocol

Every watermark INSERT or UPSERT is an `INSERT ... SELECT` whose source joins a
locking read of the matching task row:

```sql
SELECT account_id, task_id
FROM mo_catalog.mo_cdc_task
WHERE account_id = ? AND task_id = ?
FOR UPDATE
```

The locking read and target write execute in one transaction. This gives only
two relevant orderings:

1. **Writer locks first.** DROP waits. The writer commits, then DROP deletes the
   task and all watermarks, so the final state has neither.
2. **DROP locks first.** The writer waits. After DROP commits, the locking read
   observes no task row and feeds zero rows to INSERT, so it cannot recreate a
   watermark.

Multiple task IDs in one flush are rendered in deterministic
`(account_id, task_id, db_name, table_name)` order, removing map-iteration
variance from the SQL and its candidate lock order. Each generated statement
repeats a task predicate only once, contains at most 200 watermark rows, and
targets at most 256 KiB of SQL. A storage deadlock remains a normal retryable
statement failure rather than a correctness failure. The updater serializes
all statements with its persistence mutex. A partial multi-statement success
is safe: the operations are idempotent UPSERTs, the task-row fence applies
independently to every statement, and a retry keeps the newer in-memory
watermark.

## Local shutdown protocol

Cancellation increments the callback generation and installs a local deleted
tombstone before it waits. It then:

1. cancels the callback/lifecycle context;
2. waits for callbacks admitted to the old generation;
3. closes and waits for readers, retaining the aggregate completion channel
   even when a prior Pause/Failed shutdown or the current bounded wait has not
   completed;
4. drains matching updater work and deletes durable watermarks;
5. releases the local tombstone only after both callback and reader completion
   owners have finished. If the callback wait timed out, the asynchronous owner
   takes a final reader snapshot after all callbacks exit, covering readers
   published after the first snapshot.

Reader shutdown atomically detaches only the exact instances captured by its
snapshot and transfers their ownership to one aggregate completion channel. It
never performs a later blind map clear: a reader published by an
already-admitted callback after the snapshot remains visible to the final scan,
while a repeated cleanup cannot start duplicate waiters for an already-owned
reader.

The flush job is completed only after the queue callback has finished all of
its persistence phases. This matters because the queue may coalesce an
error-watermark UPSERT and the flush barrier into one callback: completing the
barrier after the normal watermark phase but before the error phase would let
the final DELETE overtake that older UPSERT.

Pause and permanent-table-error paths also retain reader completion ownership,
so a later DROP from `Paused` or `Failed` cannot release the tombstone while an
old reader is still alive. Resume and Restart also reject recovery while that
completion owner remains open; they must not remove the pause fence or publish
a replacement generation beside an old reader. A Restart callback-drain timeout
fences the timed-out generation and restores local `Failed`, matching the
durable `RestartRequested` retry owner instead of stranding the executor in
`Starting`. That timeout path immediately signals and registers completion for
all visible readers but does not add the normal synchronous reader wait after
the restart deadline has already expired; the next retry observes the retained
completion owner.

Callback admission, count changes, completion-channel rotation, and channel
closure all occur under `callbackMu`. This prevents a zero-to-new-generation
handoff from replacing a completion channel while an older callback is about
to close it, which could otherwise strand cancellation until timeout.

Taskservice publishes `Canceled` before invoking the local routine. If local
cancellation fails, it restores `CancelRequested` only when both the terminal
state and original runner still match. A newer owner or state therefore cannot
be overwritten by an old failure path.

## Rejected alternatives

- **Only delete watermarks after stopping the local executor.** This does not
  cover a different CN and cannot serialize with an already executing write.
- **Permanent process-global tombstones.** They do not cross CN boundaries,
  leak for every stale-owner cleanup, and can suppress a future task if an ID
  is reused.
- **A stateless cleanup callback on any task runner.** Generic daemon
  cancellation is not equivalent to `DROP CDC TASK`; it may leave the durable
  task row present. Running cleanup before a conditional terminal-state CAS
  also leaves cleanup ownership ambiguous when that CAS loses a race.
- **One unbounded SQL statement per flush.** It amplifies construction,
  allocation, parsing, planning, and lock-hold time for many-table tasks.

## Failure and compatibility behavior

- A failed local terminal delete remains retryable and retains the tombstone.
- A timeout bounds synchronous cancellation but does not discard callback or
  reader completion ownership; reclamation continues asynchronously.
- If a task row is already absent, guarded writes succeed as no-ops. This makes
  late and duplicate work harmless.
- Rolling deployments must not rely on the strict invariant until all CDC
  writers use the guarded SQL. During a mixed-version interval, an old writer
  can still use the unguarded form; the existing orphan-watermark scanner is
  retained as eventual repair. No catalog schema or wire-protocol change is
  introduced, so rollback is code-only.

## Validation plan

- Unit tests cover running, paused, failed, timeout, retry, delete failure,
  callback admission/drain for both Pause and Cancel, aggregate reader-drain
  ownership, Resume/Restart rejection while an old reader still owns work,
  Restart callback-drain timeout recovery, Cancel from Pausing, cache cleanup,
  and superseding owner or state paths.
- SQL construction tests cover escaping, deterministic ordering, predicate
  deduplication, row bounds, and byte bounds for insert, watermark update, and
  error update forms.
- A two-CN embedded-cluster test executes the production guarded SQL against
  the real catalog and forces both lock orderings. It observes an actual task
  table lock waiter before releasing the winner and asserts that both task and
  watermark counts are zero afterward.
- Both lock orderings share one test-only transaction lifecycle: readiness is
  separate from terminal completion, and waits observe startup errors and
  cancellation. Cleanup cancels and joins the transactions before catalog
  teardown, even when an assertion aborts the scenario. Lightweight injected
  executors cover pre-callback, statement, and commit failures, cancellation,
  expired wait contexts, and cleanup ordering without starting another cluster.
- Focused race tests cover callback/reader cancellation and updater lifecycle.
- A deterministic same-batch test blocks an admitted error-watermark UPSERT and
  proves that the terminal flush barrier cannot complete ahead of it.
- A deterministic reader-publication test publishes a successor while captured
  shutdown is blocked and proves that exact-instance cleanup neither hides the
  successor nor creates duplicate waiters on retry.
- The guarded construction benchmark records allocations and latency at 1,000
  and 5,000 tables to prevent reintroducing quadratic concatenation.
