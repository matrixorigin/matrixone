# Query-local ready-task scheduler for Scope execution

Status: implemented as a first, compatibility-preserving phase in this PR.

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

## Phase-one model

Each `Compile` owns one `scopeTaskScheduler` for its execution generation.
The scheduler has:

1. a ready queue and workers for root scopes;
2. a query-owned dependency lane for child scopes and remote notifications;
3. a task wait barrier used before Scope/Process release; and
4. cancellation and panic propagation back to the query Process.

Root work is submitted with a label and runs on workers owned by the Compile.
The root worker count is the number of compiled roots (at least one), which
preserves the existing root-level concurrency while removing those tasks from
the global pool. Every accepted task is counted, and `Compile.clear` waits for
the count to reach zero before releasing operators or the Process.

The dependency lane intentionally starts a query-owned goroutine per accepted
dependency. Current MergeRun is synchronous: its parent waits for child and
remote results. Scheduling that child on the same finite ready queue would
deadlock when the parent owns the last worker. The separate lane is therefore
the safe event boundary for this phase, not an accidental second global pool.
It can be changed to cooperative ready-queue admission after MergeRun/VM
operators gain a yield/resume contract.

## Event and ownership contract

| Event | Owner | Required action |
| --- | --- | --- |
| Root accepted | Compile scheduler | enqueue one labeled ready task |
| Child/remote dependency accepted | Compile scheduler | account it and run it on the dependency lane |
| First execution error | Scope/Process | cancel sibling Scope trees; scheduler continues cleanup |
| Task panic | scheduler | log the task and cancel the Process with a converted error |
| Compile return/release | Compile | wait for all accepted tasks, close ready workers, then release Scope/Process |
| New prepared execution | Compile.Reset | retire the previous scheduler before replacing the Process |

No scheduler task outlives its Compile-owned Process. This is especially
important for startup SQL: a failed bootstrap statement is reported through the
normal `runOnce` result channel, while debug logs identify the query, scheduler
ID, lane, root, and error. It is not silently detached into a process-wide
worker.

## Why this is not yet a fixed worker pool for all operators

The VM currently executes parent and child operators in a blocking call chain.
`MergeRun` also waits on child/remote completion channels. A strict bounded
worker pool would require one of these changes first:

* every blocking operator yields a continuation/event and returns its worker;
* dependency readiness is represented as a state machine and re-enqueued; or
* orchestration workers and blocking I/O workers are separate pools with an
  explicit admission policy.

Until then, a finite queue for dependencies is incorrect. This PR isolates the
ownership and ready queue without changing the VM's blocking semantics. The
next phase can replace only `submitDependency` after the continuation contract
exists.

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
* a single-worker parent/child case that would deadlock with one shared finite
  queue;
* panic-to-cancellation reporting; and
* rejection after scheduler retirement and context cancellation.

The Docker smoke test starts a single MatrixOne instance from this branch,
waits for the SQL port, executes bootstrap-style DDL/DML and aggregation/CTAS
queries, then verifies that the process remains alive after a statement error.
