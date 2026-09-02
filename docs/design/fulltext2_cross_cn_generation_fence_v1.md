# FULLTEXT2 cross-CN cache generation fence v1

Status: implementation design for issue #27788

## Problem and invariant

FULLTEXT2 MATCH objects live in a process-local `VectorIndexCache`.  A CDC tail
append currently removes only the writer CN's entry, so another CN can continue
to serve the old `(base timestamp, tail chunk)` generation until the periodic
pull notices it.

After the tail transaction commits generation `G`, every live v43 CN must install
`required(identity) >= G` before acknowledging the notification.  After a CN
claims eviction for `G`, no new MATCH load or search may publish or acquire an
object whose durable generation is lower than `required(identity)`.  Searches
that acquired the old cache reader before the eviction claim may finish on that
snapshot.

The tail commit is the durable linearization point.  Notification is a
post-commit repair protocol: its failure never rolls the transaction back and
never repeats persistence.

## Identity and generation

`CacheIdentity` contains the runtime account ID, database name, storage hidden
table name, and metadata hidden table name.  Its key uses length-delimited
components under a `fulltext2:` prefix.  It therefore does not alias across
tenants, databases, embedded separators, or DROP/recreate of the same visible
index (recreate receives new hidden table names).

The account ID is supplied by the runtime boundary: MATCH uses
`SqlProcess.GetAccountID`, ISCP uses `DataRetriever.GetAccountID`, and DDL uses
the transaction context.  JSON table-function configuration is not trusted for
tenant identity.

`Generation{BaseTimestamp, TailChunk}` is ordered lexicographically.  The base
component is a reserved row in the metadata hidden table.  CREATE, REBUILD, and
MERGE first seed that row from the maximum legacy segment timestamp and then
increment it under the marker primary-key lock in the same transaction as the
rewrite.  Base deletion retains the marker, even when an empty REBUILD writes no
segments.  New segment metadata no longer contributes a CN wall-clock value to
generation ordering.  A higher durable base component therefore dominates a
tail reset after MERGE/REBUILD across clock skew or rollback.  Both generation
fields are read by one SELECT statement so one transaction snapshot cannot
combine values from two statements.

## Local fence state machine

Each process owns an exact registry keyed by `CacheIdentity.Key()`:

```
absent -> required=G, claiming -> claimed
claimed(G) -- newer H--> claiming(H)
claiming/claimed(G) -- older/equal--> unchanged
absent/claimed -- freshness unknown--> transient
transient -- successful durable read/fence--> cacheable
```

Only one caller may claim a generation.  It first advances the reusable
load-generation epoch, then removes the exact cache key, then marks the current
required generation claimed.  Equal, duplicate, old, and no-cache deliveries
are idempotent.  If a newer generation arrives during a claim, the old claim
does not mark the newer generation complete.

The map reserves space for 1024 identities initially, but 1024 is not a product
limit.  Exact lower bounds grow with the active cached identity set.  Existing
housekeeping removes an identity's registry state only after the generic cache
has neither a loading nor a loaded object for that key.  Every later cold load
must pass a fresh auto-commit generation read before global publication, so the
registry can converge without reopening an old-snapshot publication window.
This prevents a cap+1 rotation from either forgetting an unvalidated lower
bound or cumulatively turning every identity into a permanent one-shot load.
Registry memory is proportional to active cache identities, rather than to
MATCH count, historical lifecycle count, or fence count.  DROP/recreate remains
isolated by new hidden-table identities.

Freshness uncertainty is exact and temporary.  A query error, per-entry
timeout, or whole-sweep deadline marks the identity transient but does not
remove its warm global object.  Subsequent MATCH callers load from their own
transaction snapshot into a one-shot object and destroy it after the statement;
they cannot replace the global entry.  Push delivery may raise the exact lower
bound but never clears freshness uncertainty, because an older delayed push is
not proof that no newer durable generation exists.  The first later successful
current-generation read that is not below the installed lower bound clears the
transient state.

Recovery is demand-driven and bounded per identity.  At most one MATCH caller
runs the current-generation probe; other concurrent callers stay transient.
Failed probes back off from 100 ms to 30 seconds.  This also lets an identity
recover after a push or TTL eviction removed the old sweep object, without
turning a metadata outage into one probe or global reload per concurrent query.

A load reads its transaction-snapshot generation before data and checks the
registry both after the generation read and immediately before publishing its
`Index`.  At the publication terminal, a non-transient load also performs one
five-second-bounded auto-commit generation read.  Only an exact match may remain
in the process-global cache.  A lower, newer, unknown, or dropped generation
destroys the in-progress object and returns the cache's dedicated retryable-load
marker; uncertainty routes the retry through a one-shot object.  This terminal
is what makes registry reclamation safe even for an arbitrarily old caller
transaction or a delayed push.  FULLTEXT2 opts into four coherence attempts
with context-aware 5/10/20 ms backoff; other algorithms keep the existing cache
retry contract.

## Push protocol and ownership

MORPC v43 adds method 40 and request/response fields 42/43.  The request carries
the four identity components and two generation components.  A receiver first
installs the monotonic requirement, then claims eviction.  It responds with its
current required generation and `EvictionClaimed`.  The sender records ACK only
when the returned generation is not lower than the request and the claim flag is
true.  MORPC v42 receivers and unknown methods are failures, never ACKs.

Each CN service owns one publisher and injects it into the ISCP executor
factory.  The publisher has a 1024-identity coalescing queue, four broadcast
workers, at most sixteen simultaneous target RPCs, and a two-second timeout per
RPC.  A newer generation replaces an older queued generation; if the older item
is already active, it yields at the next retry boundary and only the newer item
continues.  Per target it
uses attempts at 0, 100 ms, 500 ms, 2 s, 10 s, and 30 s; successful targets are
not resent, and the working-CN inventory is refreshed for each attempt.
Queue saturation, cancellation, or exhausted targets are logged and left to
pull recovery; they are never returned to the committed CDC transaction.

Two default-off fault points provide deterministic acceptance barriers without
adding product configuration or changing the MATCH hot path.  The ISCP consumer
triggers `fulltext2_after_tail_commit_before_fence` only after a tail transaction
with at least one durable segment has committed and immediately before the local
fence is installed or fanout is enqueued.  A `wait` action can therefore prove
that persistence completed while both notification paths have not started;
`notifyall`, removal, context cancellation, or process termination releases the
barrier.  Empty flushes never trigger it.

Immediately before a target RPC calls `SendMessage`, the publisher triggers
`fulltext2_fence_drop_send/<target-service-id>`.  A non-zero return reports that
attempt as unacknowledged without calling the client.  Configuring the exact
target with frequency `1:1::` and action `echo(1)` drops only its first attempt,
so later attempts exercise the ordinary bounded retry path and other targets do
not consume the fault.  Removing the fault restores normal delivery.  Both
points are inert while fault injection is disabled and are intended only for
deterministic validation; they do not change persistence or retry ownership.

Shutdown stops ISCP producers before closing the publisher, waits for publisher
workers to exit, and closes the query client last.

## Pull recovery and lifecycle

The cache runs a single-flight freshness sweep with a 30-second whole-sweep
deadline, sixteen fixed workers, and a five-second per-entry context clipped by
the remaining sweep deadline.  Fresh entries are retained.  A successfully
observed stale entry installs its higher durable generation and is claimed
fail-closed using the exact snapshotted cache object, so a late result cannot
evict a replacement published under the same key.  Query errors and entries
that cannot complete before the deadline enter the temporary transient state
above; the sweep does not bulk-evict them, so a metadata outage cannot trigger
periodic O(N) index reload work.  Each entry has one terminal outcome (`fresh`,
`stale`, `query_error`, or `deadline`).  Those four fixed labels feed a counter,
sweep duration feeds a histogram, and the cache emits only one aggregate
summary log per sweep.  MATCH performs no catalog SQL on a normal warm hit.
Pull repairs sender crash, lost RPC, and new-CN join; it is not a successful ACK
substitute.

DROP clears the exact cache, reusable pools, load generation, and local registry
entry.  A remote pull that definitively observes `NoSuchTable`/`BadDB` claims
the exact cached object stale.  A delayed push may temporarily reinstall an
exact lower bound, but housekeeping removes it once no cache object owns the
key.  An old caller snapshot still cannot republish after that reclamation,
because the auto-commit publication terminal observes the missing hidden table
and forces a transient one-shot load.  Ordinary query errors use the same
uncertainty path without pretending the table was dropped.  Recreate is
isolated by new hidden names.  MERGE/REBUILD keep their existing
transaction-local invalidation and are repaired remotely by pull; adding a
cross-transaction DDL notification is outside this issue unless a safe
post-commit hook already exists.

During a rolling upgrade, v43 senders treat v42 as unsupported.  New CNs retain
the pull fallback, but old serving CN binaries cannot provide the new guarantee.
Strict multi-CN QA and issue closure therefore require every serving CN to run
v43.

## Alternatives and non-goals

- A catalog read on every MATCH is rejected because it moves SQL I/O onto the
  hot path.
- A shorter generic TTL is rejected because it does not create a commit fence
  and changes unrelated vector algorithms.
- Push without a monotonic local fence is rejected because an in-flight load can
  publish after eviction.

Classic FULLTEXT, other vector algorithm semantics, and unrelated ISCP error
channel behavior are unchanged.  This change does not add a durable broadcast
outbox; bounded pull is the crash-recovery mechanism.

## Verification contract

Unit tests cover identity separation; generation ordering; duplicate, old, and
out-of-order deliveries; no-cache and in-flight load/search; bounded retry;
publisher coalescing, partial/all failure and cancellation; mixed versions; and
DROP/recreate cleanup.  Race tests use deterministic barriers.  SQL BVT covers
the CDC path but is not per-CN proof.  Correctness acceptance additionally
requires direct connections to at least two independent CN processes, bound to
their backend IDs, with both caches prewarmed before INSERT/UPDATE/DELETE and
rollback checks.  Lost-RPC, post-commit sender interruption, restart, and join
must converge through push or pull without lost, duplicate, phantom, or stale
rows.
