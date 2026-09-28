# Replay promotion merge bootstrap (#29415)

Status: approved local promotion contract; corrective review follow-up below.
Implementation PR: https://github.com/matrixorigin/matrixone/pull/29421.
Base: `origin/main` at `5a96035fc792b07b7a28e2f509dc0dcd4236e048`.

## Scope and evidence

The replay TN constructs its merge scheduler before WAL replay completes. A
two-engine test opens the replay TN, commits a table on the writer, waits until
the replay TN can read it, then intercepts local write admission during
`SwitchTxnMode(ctx, 2, "")`. The scheduler reports `NotExists=true`. The same
test fails on clean main and PR #29416. See the [reproducer][repro].

`SwitchTxnMode(1/2)` and `WithTxnMode(DBTxnMode_Replay)` have no in-repository
production callers. The debug RPC calls only modes 3/4. Normal TN startup uses
Write mode. This design changes only code reached through the dormant
Replay-to-Write command. It does not enable that command from a service, change
wire/catalog/storage formats, or add work to normal startup or merge hot paths.
External Go callers cannot be excluded by repository search alone; this is an
explicit limit of the no-production-impact claim.

The existing controller still has TODOs for distributed writer fencing,
logtail tunneling, and write forwarding. This design repairs the **local
promotion bootstrap** under a caller-provided guarantee that the prior writer
is already fenced and there are no concurrent direct `DB` or RPC callers. No service
may expose this switch to production until those conditions are enforced.
It does not claim that live TN migration is supported. The integration test
must close/fence its writer before requesting promotion.

## Invariants and ownership

1. With exclusive local control, `SwitchTxnMode` returns success only after WAL
   replay has stopped at its handoff point, the catalog/settings view is newer
   than the last replayed commit, the existing merge scheduler has reconciled
   that view, and all local write services have started successfully. The
   existing RPC server starts in Local state, so `TxnLocalHandle` is **not** an
   admission barrier. Concurrent RPC or direct DB traffic is outside this
   local-only contract and must be prohibited by its future caller.
2. No stale scheduler work or config is published between the replay barrier and
   local write admission. The stopped scheduler is reconciled in place, keeping
   the identical supporter pointers and task observers for surviving tables.
   New tables are added. Dropped tables are removed from map and heap at once;
   an in-flight observer retains its supporter and resource controller pointer
   until completion. Settings absent from the fresh snapshot clear old overrides.
3. Once replay has crossed its one-way `ReplayForWrite` boundary, any failure or
   cancellation makes this DB instance terminal for promotion. The controller
   returns the original error, rejects retries, and blocks new transactions.
   The caller owns close/reopen after the control command returns. It must not
   synchronously close the DB or RPC server from the controller callback.
4. The controller owns the promotion phase and terminal error. Replay control
   owns its worker and cancellation. The merge scheduler owns its supporters,
   task observers, and generation queues. The settings reader owns and closes
   every partial batch on error. No new background worker or persistent state
   is added.

## Sequence

1. Reject a prior terminal promotion error. Require a fresh Replay-open DB:
   non-nil replay controller, scheduler never started, old writer fenced, and
   no concurrent DB/RPC callers. A promotion-only preflight checks
   `stopped=true` and a never-run generation: `generation=nil` on this main
   base, or a generation with an open `stopCh` if the constructor initializes
   it (as in PR #29416). A completed Start/Stop leaves a closed stop channel.
   Test fresh and Start/Stop states. Write-to-replay-to-write lacks a WAL barrier and fails
   closed. Also require the stopped scheduler's shared message queue to be
   empty. `SendConfig` and `SendTrigger`
   can enqueue untagged messages even before first Start; a nonempty queue
   fails closed before state mutation. The exclusive-control precondition
   excludes concurrent senders, making the check stable. Then request
   `ReplayForWrite`, wait for its worker with the command context, and
   propagate replay failure.
2. Require the prior writer to be fenced and catalog replay to be quiescent.
   Select `max(TxnMgr.Now(), TxnMgr.MaxCommittedTS.Next())` after the replay
   worker joins. Enumerate active catalog tables from the now-quiescent
   catalog; this enumeration is not an MVCC snapshot. Read visible
   `mo_merge_settings` rows using one offline transaction at that timestamp.
   A new promotion-only reader accepts context, returns scan errors, strictly
   decodes both JSON and trigger parameters, and closes partial batches.
   A missing settings table means an empty settings set only on an explicit
   table-not-found result; missing `mo_catalog` or scan failure is an error.
3. While the scheduler is stopped, reconcile its supporters by table ID:
   preserve each surviving pointer and in-flight count, add only missing IDs,
   and remove dropped IDs from map and heap. The observer closure owns a
   removed supporter's eventual task release. Reset all overrides and apply
   the fresh settings set, including deletions. Clear the old constructor
   `bootstrapMsg` before `Start`, so it cannot replay a stale settings closure.
   The legacy `OnMergeDone` path has no current
   caller; validate that no promotion event can reintroduce a removed supporter.
4. Before crossing the replay barrier, require all three write-only cron job
   names to be absent. The Replay cron specification permits an optional
   `GCLockMerge` job, so checking only the Replay job set is insufficient.
   Attach `Catalog.SetMergeNotifier` after reconciliation while the catalog is
   quiescent and set the stopped scheduler's paused flag synchronously. Switch
   the transaction manager to Write **before** starting write-capable workers;
   the caller's exclusive-control precondition prevents new direct/RPC writes
   during the remaining setup. Start the flusher and disk cleaner, then start
   the reconciled scheduler paused and set DB mode Write. Resume the scheduler
   with a query barrier. A canceled caller may stop the handoff before the
   resume message is queued; once queued, the resume is the commit point and
   the query barrier waits without caller cancellation. This prevents reporting
   failure after merge work may have begun. Add the three write-only cron jobs
   only after the barrier. With the exclusive caller and absent-name preflight,
   `AddJob` has no reachable ordinary error after the first job starts. A failure
   after the replay barrier is terminal and must latch `OnException` before
   returning. Skip the existing `SwitchTxnHandleStateTo(TxnLocalHandle)` call:
   fresh Replay-open has no forwarding transition, and the server defaults to
   Local. That call is not an admission barrier. Return success only after all
   steps finish.

## Errors, cancellation, and recovery

- Before the replay handoff, failure preserves Replay mode. After the handoff,
  latch a terminal error, call `TxnMgr.OnException`, detach the catalog notifier
  only if it was attached after the replay worker joined, then stop any started
  scheduler and write services. Return the original error; cleanup operations
  on this path have no error return.
  Never report success because rollback succeeded.
- `StopForWrite(ctx)` observes cancellation and cancels the replay worker. If
  cancellation returns before the worker joins, leave replay transaction flags
  unchanged, keep the terminal error latched, and let DB close join the worker.
  Do not retry promotion on the same DB instance.
- After a resume message is queued, the private query barrier uses a detached
  context. A fresh scheduler with no concurrent sender or Stop must process the
  queued resume and query in order; a caller cancellation after the commit point
  does not reverse a transition that may have started merge work.
- The strict reader uses a promotion-only block scanner whose lower-level scan
  never closes the shared partial batch. Its caller is the sole close owner on
  every success and error path; it does not call `HybridScanByBlock`, which
  closes the batch internally on some errors but not others. The existing
  normal-startup reader and scanner are unchanged.
- A settings scan error or timeout does not substitute a partial/default batch.
  No scheduler or write endpoint is published. Existing background workers are
  stopped by their current owner; the TN lifecycle owner closes/reopens after
  the command returns.
- This local fail-stop does not implement a distributed request gate or old
  writer fence. Promotion cannot be exposed to production until those separate
  contracts are designed and verified.

## Alternatives and cost

- Recreate the scheduler: less code, but loses in-flight task accounting and
  changes references held by the catalog/DB. Rejected.
- Asynchronously enqueue replayed table/config events: leaves an interval in
  which writes or merge scheduling can observe stale state. Rejected.
- Reconcile the stopped scheduler before admission: selected. Work is O(active
  tables + settings rows) and occurs once per explicit promotion; normal Write
  startup, transaction, and merge paths execute no added instruction or I/O.
  The snapshot batch is bounded by existing table size and released on every
  path; no retained copy is required after reconciliation.

## Validation and delivery gate

The pinned baseline diagnostic reproducer fails with its writer DB closed
before promotion. The final regression reuses that two-engine setup but checks
a completed successful switch and the public scheduler query; the old
`TxnLocalHandle` interception is removed with the dormant branch's redundant
call, so it is not retained as the final oracle.
Extend it with a late settings row and absent/deleted row.
The no-replay-control local switch and a pre-Start queued scheduler message
must fail closed. Cover replay error, canceled
wait, settings read failure, one-way retry rejection, and surviving supporter
task count. Run selected tests in normal/race modes, owning packages, CGo
wrapper, incremental SCA, and a no-change call-graph/diff audit of normal TN
startup and merge hot paths. No SQL BVT applies because no production service
can invoke this mode switch. Do not submit a production implementation until
this exact design revision has been reviewed and approved under `mo-dev`.

[repro]: https://github.com/XuPeng-SH/matrixone/blob/9599e3dbe5/pkg/vm/engine/tae/db/test/issue29415_repro_test.go

## Review corrections (2026-09-27)

Independent `gpt-6-sol` / `xhigh` design review classified these as corrections
to the approved local invariants, with no new service, protocol, or recovery
contract. The approved revision remains `f41de8c29248e05af185ee3bf973454e3d89f4a7`.

- A future WAL timestamp must advance the shared DB/transaction-manager clock,
  not just the offline settings snapshot. After replay joins, update that clock
  only when behind the replay high-water mark and verify a strictly newer
  timestamp before enabling writes. A clock that cannot advance takes the
  existing terminal failure path. No normal transaction code changes.
- In sequence step 4, disk GC is conditional on `DisableGC == false`; only
  checkpoint and lock-merge cron jobs are unconditional. Preserve the existing
  normal-startup cron specification rather than changing production behavior.
- The promotion-only decoder rejects unsafe tombstone counts, nonpositive
  overlap depth, and nonpositive vacuum decay duration before starting the
  scheduler. Existing normal settings parsing and scheduling are unchanged.

Validation extends the existing two-engine fixture with a real committed write
after promotion, exact recovered trigger content, disabled GC, and failed clock
advancement. Pure settings-domain cases use a lightweight unit table, with one
invalid persisted setting retaining the terminal failure/retry/admission oracle.
Clock update and domain validation occur once per promotion (constant work per
setting); no new worker, retained state, or hot-path operation is introduced.

## Promotion count budget correction (2026-09-27)

The previous lower-bound check still accepts `math.MaxInt` for either
`TombstoneL1Count` or `TombstoneL2Count`. Both fields survive catalog JSON
encoding and decoding. Local promotion then reports success, but
`GatherTombstoneTasks` uses each count as the initial capacity of a pointer
slice and panics with `makeslice: cap out of range`, even for an empty table.
The real two-engine fixture reproduced the successful but unsafe promotion;
isolated consumer probes reproduced the panic for both fields.

The promotion-only setting reader will require each count in `[1, 65536]`,
rejecting larger values before scheduler start through the existing terminal
failure path. This bound allows the two pointer backing arrays together at most
`2 * 65536 * 8 = 1048576` bytes of settings-driven preallocation per gather on
64-bit targets, or half that on 32-bit targets. It is a deliberate 1 MiB local
budget, not an existing merge scheduler limit. Current defaults are 4 and 2;
existing tests use values through 100. The bound does not constrain growth
from the actual number of tombstone objects, and no general OOM guarantee is
claimed. The normal-startup settings converter and scheduler remain unchanged.

Alternatives considered: guarding only integer overflow still admits enormous
allocations; clamping changes the requested merge threshold without telling the
caller; changing `GatherTombstoneTasks` would affect the current merge hot path.
Rejecting invalid promotion settings at the reader makes the one-way handoff
fail closed without changing any enabled production path.

Validation: for each field independently, round-trip the setting through the
catalog JSON codec, accept 65536, and reject 65537 and `math.MaxInt`; exercise
the accepted boundary with an empty consumer. Persist both invalid upper-count
variants in the two-engine fixture and assert the original error, blocked retry
and transaction admission, and absence of write-only cron jobs. Reuse the
existing normal, future-clock, cancellation, and race results when their
semantic inputs are unchanged. Run focused CGo tests and incremental static
checks on affected packages. No SQL BVT applies because no production service
invokes local Replay-to-Write promotion.
