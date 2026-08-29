# TN-Ordered Logtail Read Barrier and Authentication Catalog Freshness

- Status: Review required
- Related issue: #27834
- Last updated: 2026-08-29

## 1. Summary

MatrixOne normally allows a transaction to start from the newest logtail that
its local CN has already applied. That policy avoids a global freshness round
trip on ordinary SQL, but it does not provide the real-time ordering required
by operations such as authentication: a catalog mutation may have committed on
one CN while a later login on another CN still reads the older catalog state.

The previous repair advanced the login snapshot beyond the local HLC
uncertainty upper bound. It was safe only under the configured clock-skew
assumption, and an idle healthy cluster paid approximately the default 500ms
maximum clock offset on every connection. The delay was unrelated to actual
logtail lag.

This design introduces a generic, linearizable logtail read barrier. The TN
places a marker in the same FIFO as committed transactions, the existing
single logtail publisher sends the marker response after all preceding update
responses, and the CN waits for its normal apply pipeline to reach the exact
frontier returned by the marker. Authentication is the first consumer, not a
special implementation path.

## 2. Required Contract

For one successful call `AcquireLogtailReadBarrier(ctx)` on a CN:

```text
commit C completes before barrier B begins
    => C precedes B in the TN publication order
    => C's logtail response precedes B's response on the CN stream
    => B returns only after the local apply pipeline has applied C
```

The barrier may order a commit concurrent with the call on either side. It must
not return while a commit that completed before the call remains invisible.

The negations are useful review and test oracles:

- a completed commit remains invisible after a successful barrier;
- a no-work barrier waits for a fixed clock-uncertainty interval;
- a barrier response overtakes an earlier update response;
- cancellation, reconnect, shutdown, or queue pressure leaks a waiter or
  blocks global logtail publication.

## 3. Scope

The primitive is an optional engine capability. It is suitable for callers
that need a real-time read boundary without changing the default transaction
freshness policy. Authentication uses it for all catalog state involved in a
normal login, including account, user, password, role, grant, system-variable,
and login-database checks.

This change does not:

- impose a barrier on ordinary transaction creation;
- add catalog-table or authentication-specific logic to TN or disttae;
- fan out to every CN or depend on a CN membership snapshot;
- change transaction isolation or split authentication across snapshots;
- persist barrier state;
- change the bootstrap-only special-user path.

## 4. Ordering Proof

The implementation closes four existing ordering stages rather than inventing
a second replication path.

### 4.1 Commit completion to TN queue order

`OnEndPrepareWAL` admits a transaction into the logtail manager FIFO before the
transaction can complete. Therefore every commit whose successful response was
observed before a later barrier request has already entered the FIFO before the
barrier marker can enter it.

### 4.2 TN queue order to publication frontier

The manager collects transaction logtails in parallel but publishes them in
FIFO/PrepareTS order. A barrier splits a batch into transaction segments:
transactions before it are fully published first, the marker then captures
`previousSaveTS`, and transactions after it are scheduled afterwards. Work
that cannot contribute to the barrier therefore cannot delay it by competing
for collection workers.

The returned timestamp is the exact last published frontier. It is not
`clock.Now()`, an uncertainty estimate, or a timestamp that forces future
progress.

### 4.3 Publication order to stream response order

TN transaction callbacks and barrier markers enter the same notifier channel.
The single logtail sender processes that channel in order and enqueues both
update and barrier responses on the same per-session response queue. A barrier
response therefore cannot overtake an earlier accepted update response.

Normal publication may omit a response when all table tails in an event are
irrelevant to one session. Before acknowledging a barrier, the sender therefore
forces an empty progress update to the exact frontier when that session has not
already been advanced through it. This is not a synthetic data apply: the
ordered empty update proves that every relevant earlier update has already
entered the same response sequence. It also prevents barrier latency from
depending on the periodic transport-heartbeat interval.

### 4.4 Stream response order to local visibility

The CN receive loop consumes barrier responses internally and correlates them
by request ID. Receiving the response proves only that prior update responses
were received in order; application may still be asynchronous. The engine
therefore waits on the existing timestamp waiter until the local apply frontier
reaches the returned TN frontier. Only then does the public engine barrier
return. A forced empty progress update traverses the same per-consumer apply
queues, so it cannot advance the waiter past an earlier queued table update.

## 5. Authentication Integration

Immediately before constructing the authentication background executor, the
frontend:

1. invokes the engine's generic logtail read barrier;
2. monotonically installs the returned frontier as a session minimum;
3. asks the transaction client to confirm the effective session minimum;
4. creates one background transaction for every catalog read used by the
   authentication decision.

The second transaction-client check is normally immediate because the engine
barrier already waited for local apply. It remains necessary when an upstream
or reused session already carries a minimum later than the returned frontier.
The session minimum is never lowered.

A failure is fail-closed. Missing engine/transaction-client capabilities,
protocol errors, cancellation, stream failure, timestamp-wait failure, or an
applied timestamp below the required minimum rejects the login.

## 6. Concurrency, Ownership, and Boundedness

- A manager barrier owns one buffered completion channel. Once admitted, TN
  queue progress never depends on whether its caller is still waiting. The
  marker, not the canceled caller, retains and eventually releases admission.
- Queue admission is context-aware. A canceled producer withdraws its pending
  count before ownership transfers; shutdown can still drain all admitted
  items without waiting forever.
- Each CN admits at most 100 concurrent barriers before allocating request IDs
  and pending-map entries. Additional callers wait context-aware, so connection
  bursts cannot grow retained correlation state or the stream request queue
  without bound. Request IDs are monotonic and responses may complete out of
  order. Return, cancellation, stream breakage, and client closure all remove
  or abandon the entry without spawning a per-request goroutine.
- The logtail server admits at most 100 barriers globally across the complete
  shared publication path. A slot remains owned through TN marker processing,
  notifier ordering, and response hand-off; shutdown draining releases queued
  events. The manager independently admits at most 100 markers, equal to one
  queue batch. Thus authentication load can neither fill the 10,000-entry
  commit FIFO nor accumulate in the notifier and backpressure ordinary logtail
  publication. Additional server calls wait before the FIFO and honor their
  request context.
- The normal receive goroutine remains the only stream reader. Barrier
  responses are control messages and are not exposed to the logtail dispatcher.
- The existing per-session response queue remains bounded. A congested session
  fails and reconnects instead of blocking the global publisher.
- Reconnect swaps the concrete logtail client under a dedicated lock. A caller
  that captured the previous client observes stream closure and fails; it never
  races an unsynchronized pointer replacement.

## 7. Failure and Restart Semantics

- Cancellation before queue admission admits no marker.
- Cancellation after admission stops the caller, while the buffered marker
  completion lets the queue continue independently.
- TN shutdown rejects new admission and drains already admitted queue work
  under the existing queue lifecycle.
- CN stream send or receive failure marks the stream broken. Pending barriers
  fail and reconnect obtains a new stream; barrier IDs and responses never
  cross stream generations.
- A barrier is ephemeral. Restart requires no replay or cleanup of persisted
  state.
- If an earlier logtail callback cannot transfer ownership to the notifier,
  the same stream cannot provide a successful ordered barrier response; the
  operation fails rather than claiming visibility.

## 8. Rolling Upgrade Compatibility

The request and response are introduced at `MORPCVersion39`. Deployment keeps
the active protocol version at the oldest live service. While it is below 39,
authentication uses the previous HLC-uncertainty fence, which is slower but
preserves the existing correctness contract. Only a fully upgraded cluster
sends the new wire messages.

The fallback and its clock-offset configuration validation remain until the
old protocol is no longer supported. There is no persisted-data migration.

## 9. Performance Model

The barrier adds no work to ordinary SQL. The transaction-only manager path
performs one atomic pending-barrier load and then follows the original
collection/publication code without scanning or type-asserting the batch twice.
The generic `SafeQueue.Enqueue` path remains the original direct channel send;
only the new request-scoped API pays context checks and a cancellation select.

For a barrier caller, cost is:

```text
one bounded request/response on the existing TN logtail stream
    + publication of transactions already ahead of the marker
    + actual local apply lag to the returned frontier
```

When intervening commits contain no table subscribed by the caller, the server
may add one empty progress response before the barrier response. Its size and
apply work are constant; it replaces an otherwise unbounded wait for the next
periodic heartbeat.

There is no fixed sleep, no clock-offset wait, no all-CN fan-out, and no scan of
catalog tables. In an idle healthy cluster, latency should be network and queue
scheduling scale rather than the configured 500ms uncertainty interval. Under
load, the wait reflects real causal work that must become visible.

The marker divides manager batches so post-barrier collection does not consume
workers needed by pre-barrier transactions. Collection and publication for
normal transaction-only batches retain their previous parallel/ordered shape.
Adjacent barriers at the same FIFO position share one captured frontier and do
not create empty collection segments. Admission is concurrency-bounded rather
than rate-limited, so an uncontended login pays no artificial pacing delay.

## 10. Alternatives Rejected

### Remove the authentication wait

This restores low latency by violating the real-time authorization invariant.
It can authenticate against catalog state older than an already acknowledged
security mutation.

### Use local or current HLC time

A local timestamp does not prove which TN commits have entered publication. A
future uncertainty timestamp forces idle wall-clock progress and couples
latency to clock configuration instead of causal logtail work.

### Query or synchronize every CN

The required owner is TN commit/publication order, not the set of current CNs.
Fan-out adds O(cluster size) work and still needs a policy for membership races
and partial failures.

### Broadcast every security mutation

This overfits one catalog category, moves coordination to the mutation path,
and creates ambiguous commit results when broadcast fails after commit. Other
linearizable-read consumers would still need another mechanism.

### Disable sacrificed freshness globally

That imposes freshness coordination on unrelated SQL hot paths. The engine
barrier lets only operations with an explicit real-time contract pay for it.

## 11. Validation Matrix

White-box tests must establish:

- queue cancellation withdraws pending ownership and shutdown completes;
- CN and TN admission bounds stop correlation/FIFO growth, canceled waiters
  exit, and completed markers make their slots reusable;
- a caller canceled after TN admission cannot release the marker's slot before
  the queue has processed that marker;
- the manager returns the exact published frontier and rejects missing
  publishers or canceled callers;
- parallel barrier requests are correlated correctly even when responses
  arrive out of order;
- send failure breaks the stream and cancellation removes pending state;
- an update accepted before a barrier is written before its response;
- filtered logtails force ordered empty progress instead of waiting for a
  periodic heartbeat;
- the engine waits for local apply and rejects missing waiters or regressing
  timestamps;
- authentication preserves a later existing session minimum and fails closed
  on every missing/error capability;
- protocol v38 and below use the correct legacy fallback, while protocol v39
  uses the new primitive.

Black-box validation must cover at least two CNs and both mutation directions:

```text
CN-A: revoke/suspend/drop/change credential; commit returns
CN-B: later login must observe the mutation

CN-A: grant/resume/create/change credential; commit returns
CN-B: later login must observe the mutation
```

The test must route the mutation and login to distinct CNs, avoid sleeps as a
correctness mechanism, and repeat enough times to exercise stream scheduling.
Performance validation compares fresh-connection latency on the same binary,
configuration, host, and warm state. Coverage is not satisfied by a mocked
frontend-only test; the real TN queue, stream response, CN apply waiter, and
authentication transaction must all be exercised.

`TestIssue27834CrossCNAuthenticationReadsLatestCatalog` provides that topology
cell on the shared two-CN embedded cluster. CN-0 repeatedly changes credentials
and revokes/grants an explicit role; each committed mutation is immediately
checked through a fresh connection to CN-1 without retry or sleep.
