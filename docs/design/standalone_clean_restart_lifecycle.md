# Standalone clean-restart lifecycle

Status: design for PR A/B/C review

## Problem and invariants

In standalone mode the process owns CN, TN, LogService/HAKeeper, proxy and
auxiliary services. The old shutdown path cancelled one global stopper, which
allowed CN withdrawal and TN transaction cleanup to race the services they
needed. A CN could therefore leave an owner visible until its fence expired,
while a transaction already waiting for WAL durability observed a closed
LogService. A subsequent process with the same persistent identity then failed
admission or had an unknown commit outcome.

The lifecycle invariant is:

```text
CN normal close and final withdrawal
  -> TN quiesce and drain accepted handlers
  -> TN storage close
  -> LogService/HAKeeper close
  -> process-level metrics and trace flush
```

The implementation does not change SQL, DDL, Raft or CDC wire messages,
persistent formats, owner fences, or the default discovery timeout.

## Ownership and state transitions

The process supervisor owns role ordering and is the only caller that advances
between dependency phases. Instances of the same role may close in parallel.
Each role task is registered before it starts and reports one terminal result.
The supervisor transitions through `proxy`, `cn`, `python`, `tn`, and `log`;
an error or timeout stops the transition and returns a non-zero process result
without invoking the global stopper to cancel still-needed dependencies.

TN transaction RPC has a private lifecycle:

```text
accepting -> quiesced -> draining -> closed
```

Quiesce closes network and local ingress and seals the accepted-handler count.
Drain waits on a zero transition rather than polling. Accepted handlers retain
their replica, storage and WAL dependencies until they reach a terminal state;
queued requests are cancelled exactly once. Storage is closed only after a
successful drain. A bounded four-minute drain is used internally; timeout
leaves dependencies alive and returns `ErrTxnDrainTimeout`.

The WAL driver has one completion owner per accepted committer. Every accepted
entry receives exactly one success or error terminal notification before an
append failure escalates through the existing fail-stop path. Driver close has
one shared four-minute deadline and never performs a second unbounded waiter
wait after that deadline.

## Shutdown and failure behaviour

`SIGTERM`, `SIGINT`, HAKeeper shutdown, and startup-failure cleanup all enter
the same idempotent supervisor operation. A clean CN withdrawal is attempted
while HAKeeper is still available. If withdrawal fails, the error is recorded
and the old-owner fence remains authoritative; the failure is not reported as
a clean handoff. SIGKILL and process crashes do not send withdrawal and retain
the existing owner-fence behaviour.

An asynchronous configured-Proxy initialization failure is delivered to the
main wait loop, so the process enters fatal cleanup without waiting for an
unrelated signal or HAKeeper command. Fatal cleanup preserves role ordering,
then reaps any dynamic CN children that were already started.

If a role cannot close within its budget, dependent roles are not closed and
the process exits non-zero so recovery can determine unknown commit state. A
commit that has entered WAL durability is never described as rolled back merely
because a client disconnected or a shutdown context expired.

Dynamic CN children receive `SIGTERM` during the graceful phase. The signal and
Wait operation use the same child process handle. If that phase budget expires,
every still-owned child receives `SIGKILL`, and shutdown waits for the
corresponding child reap before returning; a non-zero owned PID is a terminal
cleanup failure.

## Alternatives rejected

* Cancelling the global stopper concurrently is simple but closes providers and
  consumers in an order that recreates the race.
* Increasing discovery or owner timeouts masks a failed handoff and makes
  recovery slower without fixing the dependency violation.
* Only reordering role `Close` calls does not seal new TN producers or provide
  an exactly-once terminal result for accepted WAL waiters.

## PR boundaries and rollback

PR A contains only the process supervisor, role ordering, dynamic-CN graceful
  coordination, and CN withdrawal observability. PR B adds the private TN RPC
  quiesce/drain lifecycle and its tests. PR C independently fixes WAL waiter
  terminal ownership and the single close deadline. None changes the public
  service interface or FULLTEXT2 code. Each PR can be reverted independently;
  the pre-change global shutdown remains the fallback until its replacement is
  deployed and validated.

## Validation matrix

Unit and race tests cover ordering, role errors/timeouts, repeated shutdown,
withdrawal failure, dynamic CN SIGTERM/SIGKILL/reap, Python-provider ordering,
queued and active TN requests,
drain timeout, WAL success/error/close races, and exactly-once waiter
completion. End-to-end validation uses a disposable standalone: graceful close
with and without in-flight commits, ten same-UUID restarts with the default
discovery timeout, and a SIGKILL owner-fence control. FULLTEXT2 digests and an
INSERT/UPDATE/DELETE probe are checked on cycles 1, 5, and 10. No shared
services or old persistent templates are modified.
