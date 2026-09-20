# Query-local ready-task scheduler for Scope execution

Status: implemented as a compatibility-preserving event-orchestration phase in
this PR. The VM operator yield contract is now defined, but production
operators still use the blocking compatibility lane.

## Motivation

`Compile.runOnce` and `Scope.MergeRun` used the process-wide `ants` default
pool for three different kinds of work:

* independent root scopes;
* child scopes that a merge scope starts and then waits for; and
* remote-notification I/O loops.

The default ants pool is shared with MORPC and lockservice. It is elastic, so
it does not provide query-local admission, useful ownership/debug information,
or a way to reason about a query's ready work. Replacing it with a small fixed
pool directly is unsafe: a parent MergeRun can occupy the last worker while
waiting for a child, producing a deadlock.

## Event-orchestration model

Each `Compile` owns one `scopeTaskScheduler` for its execution generation.
The scheduler has:

1. a ready queue and workers for root scopes;
2. event tasks whose completion is delivered by a continuation;
3. a query-owned dependency lane for blocking VM/remote operations;
4. a task wait barrier used before Scope/Process release; and
5. cancellation and panic propagation back to the query Process.

Root work is submitted with a label and runs as either a short ready task or an
event task. An event task starts a continuation and returns its ready worker;
the continuation calls `done` only after the corresponding scope state reaches
a terminal event. Every accepted task is counted, and `Compile.clear` waits
for the count to reach zero before releasing operators or the Process.

`MergeRun` now has an event state for ordinary, lazy UNION ALL, and
ordering-sensitive TP merge topologies. Child scope, parent pipeline, and
remote-notify completion are events; the state advances from short ready tasks
without occupying a ready worker while waiting. The public synchronous
`MergeRun` entry point remains for nested/compatibility callers whose parent
operator still invokes a child merge inline. The dependency lane still starts
a query-owned goroutine for blocking VM and network operations. It is a
deliberate blocking-island boundary, not the final fixed-worker VM scheduler.

The VM pipeline now exposes `pipeline.Continuation`. `Prepare` and output
metadata setup are performed once, and each `Step` executes one `vm.Exec`
quantum. The historical `Pipeline.Run` API is an adapter over this interface,
so callers keep their behavior while the scheduler migration can be staged.
`StepWaiting` is intentionally not emitted by existing operators yet: their
`Call` implementations can still block internally. The next operator
migration must turn those waits into readiness events before a continuation can
run directly on the finite ready queue.

The scheduler also exposes a guarded `submitContinuation` entry point for that
next phase. A waiting step must provide an `OnReady` registration callback; the
scheduler worker is released immediately and the callback re-admits the
continuation when the external event fires. A continuation that reports
`StepWaiting` without a registration fails closed. No existing blocking
operator is routed through this entry point yet.

## Event and ownership contract

| Event | Owner | Required action |
| --- | --- | --- |
| Root accepted | Compile scheduler | enqueue one labeled ready/event task |
| Child/remote dependency accepted | Merge event state | account it and publish a completion event |
| First execution error | Scope/Process | cancel sibling Scope trees; scheduler continues cleanup |
| Task panic | scheduler | log the task and cancel the Process with a converted error |
| Compile return/release | Compile | wait for all accepted tasks, close ready workers, then release Scope/Process |
| New prepared execution | Compile.Reset | retire the previous scheduler before replacing the Process |

No scheduler task outlives its Compile-owned Process. This is especially
important for startup SQL: a failed bootstrap statement is reported through the
normal `runOnce` result channel, while debug logs identify the query, scheduler
ID, lane, root, merge event, and error. It is not silently detached into a
process-wide worker.

## Remaining blocking boundary

The VM currently executes parent and child operators in a blocking call chain.
The event state removes the scheduler-worker wait, but the actual pipeline
island can still block on `vm.Exec`, pipeline backpressure, or remote I/O. A
strict bounded worker pool for those operations requires one of these changes:

* every blocking operator yields a continuation/event and returns its worker;
* dependency readiness is represented as a state machine and re-enqueued; or
* orchestration workers and blocking I/O workers are separate pools with an
  explicit admission policy.

Until then, moving those operations into the finite ready queue is incorrect.
This PR isolates ownership, makes MergeRun orchestration event-driven, and
keeps the blocking boundary explicit. `submitDependency` remains the
compatibility lane until each operator has a stateful continuation and a
readiness callback; replacing it with a finite blocking worker pool would
deadlock parent/child MergeRun topologies.

## Cancellation and failure behavior

The scheduler never turns a task error into success. Scope closures still send
their `scopeRunResult`; the first error cancels the sibling Process trees as
before. A rejected task is returned to the caller, which performs the existing
terminal receiver cleanup. A panic is converted with MatrixOne's standard
`moerr.ConvertPanicError`, logged with abbreviated SQL, and cancels the query.

`wait` is idempotent and is called both at the end of `runOnce` and from
`Compile.clear`. This covers normal statements, retries/prepared TP execution,
and remote pipeline handlers that enter `MergeRun` directly.

## Verification in this PR

`scope_scheduler_test.go` covers:

* ready-root and dependency execution;
* event-task completion barriers;
* a single-worker parent/child case that would deadlock with one shared finite
  queue;
* panic-to-cancellation reporting; and
* rejection after scheduler retirement and context cancellation.

`pkg/sql/compile` tests also cover ordinary MergeRun, lazy UNION ALL, and
remote-notify compatibility paths after the event state was introduced.

The Docker smoke test starts a single MatrixOne instance from this branch,
waits for the SQL port, executes bootstrap-style DDL/DML and aggregation/CTAS
queries, then verifies that the process remains alive after a statement error.
