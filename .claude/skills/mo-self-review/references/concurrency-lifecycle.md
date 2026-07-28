# Concurrency And Lifecycle Reasoning

Use this reference when a diff changes shared state, cancellation, close/terminal
operations, retry/restart, pooling, callbacks, or asynchronous cleanup. Derive
tests from the model below; do not copy a fixed list of cases.

## 1. Start From Invariants

Write the invariant before reading implementation details:

| Dimension | Invariant |
|---|---|
| Safety | At most one incompatible operation owns a transition at a time. |
| Liveness | Every wait dependency reaches a release, cancellation, or bounded timeout. |
| Ownership | Each resource has one effective cleanup owner; transfer changes the owner, not the count. |
| Generation | Work from generation N cannot mutate, close, or reopen generation N+1. |
| Boundedness | Queues, callbacks, retries, and retained references have a bound or recycle point. |

An implementation is correct only if all five hold across success, failure,
cancellation, retry, close, and reuse.

## 2. Build The State And Ownership Model

For each changed object, record:

1. States and generations.
2. Events that can arrive concurrently.
3. The owner allowed to perform each irreversible side effect.
4. The linearization point that grants or transfers ownership.
5. Every transition, including failed admission and rollback of a partial claim.
6. The condition that permits reuse or reopening.

Use a transition table:

| From | Event | Guard / linearization point | To | Side effects | Failure transition |
|---|---|---|---|---|---|
| idle | terminal request | atomic claim | active | seal new work | release claim |
| active | caller gives up | ownership transfer | draining | cancel work | remain draining |
| draining | final worker exits | one-shot callback | closed | cleanup once | closed with error |
| closed | restart admitted | generation claim | initializing | reset state | closed |

Names are illustrative. Derive actual states from the code. Missing rows are
review findings until proven unreachable.

## 3. Draw The Wait-For Graph

Treat every potentially blocking operation as an edge, not only explicit
`Wait` calls:

```
caller -> mutex -> current owner -> channel/RPC/I/O -> remote release
```

Include mutex/RWMutex acquisition, condition variables, channel send/receive,
future/result waits, goroutine joins, callbacks, retry loops, and synchronous
cleanup. Follow each edge until a guaranteed local termination is found.

### Control-path independence

Cancellation, close, abort, timeout handling, health checks, and fail-fast
rejection are control paths. A control path must not depend on a resource held
by the data path it is supposed to stop or reject. In particular, verify that:

- rejection does not acquire a lock held across downstream work;
- cancellation does not enqueue behind the blocked operation;
- cleanup notification does not require a receiver that may already have exited;
- timeout handling does not synchronously perform unbounded cleanup;
- diagnostic/error construction does not read mutable generation state without synchronization.

If the control path shares such a dependency, its latency and termination are
those of the blocked data path; it is not fail-fast.

## 4. Close The Generation Boundary

For restart, retry, pooling, or object reuse, trace:

```
old work stops -> old cleanup completes -> reuse claim -> initialize sealed
-> admission succeeds -> publish/open new generation
```

Check both directions:

- old callbacks/tokens/references cannot affect the new generation;
- the new generation cannot be observed before all of its gates are ready;
- failed initialization restores a valid closed/retryable state;
- reopening one gate cannot race another close or seal operation;
- pooled objects are not returned while an old owner can still read or mutate them.

## 5. Derive The Test Matrix

Generate tests as a product of semantic axes, then reduce only equivalent cells:

| Axis | Derive values from |
|---|---|
| Entry | All public operations sharing the state or resource |
| Phase | Before claim, active, draining, cleanup blocked, closed, reinitializing, reopened |
| Competitor | Same operation, conflicting operation, cancel/close, restart/reuse |
| Failure | Context cancellation, internal timeout, downstream error, blocked dependency |
| Resource | Empty and non-empty ownership: workspace, lock, connection, callback, queue |
| Oracle | Result/error, latency bound, state, exact side-effect count, resource release |

The matrix is complete when every transition and invariant is observed, not
when a remembered list of test names is present.

### Deterministic concurrency tests

- Use channels, callbacks, condition observation, or injected blockers to prove
  the tested phase has been reached.
- Do not use `time.Sleep` as the phase barrier.
- Give the test context a larger budget than the internal timeout being tested.
- Assert fail-fast latency with a short outer deadline and verify it did not fire.
- Count irreversible side effects and require exactly once, not merely non-zero.
- Make test cleanup release blockers even after an assertion fails.
- Run new concurrency tests with `-race`; stress the focused transition set with
  `-count=N`, then run the entire owning package under `-race` once.

## 6. Validate The Closure

Use layered evidence after the final edit or rebase:

1. Focused regression proving the original invariant violation.
2. Focused race stress across transition boundaries.
3. Full owning-package tests and race tests.
4. Tests for every package changed by the complete diff.
5. Static analysis and a dependent build.
6. Benchmarks only for changed hot paths; report allocations and contention.

No PASS/FAIL result, a surviving test process, or output produced before the
last semantic change is not fresh evidence.

Also validate the delivery boundary, not only the working directory:

- `git status --short --untracked-files=all` shows every intended artifact;
- ignore rules do not silently exclude a new source, test, script, or fixture;
- the staged diff contains the same functional unit that was tested;
- generated or local-only files are not required for the staged change to work.

A file existing locally is not evidence that reviewers, CI, or the final commit
will receive it.
