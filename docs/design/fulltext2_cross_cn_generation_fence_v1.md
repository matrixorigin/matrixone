# FULLTEXT2 cross-CN cache generation fence v1

Status: implementation design for issue #27788

## Problem and invariant

FULLTEXT2 MATCH objects live in a process-local `VectorIndexCache`.  A CDC tail
append currently removes only the writer CN's entry, so another CN can continue
to serve the old `(base timestamp, tail chunk)` generation until the periodic
pull notices it.

After the tail transaction commits generation `G`, every live v41 CN must install
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

Each process owns a bounded registry keyed by `CacheIdentity.Key()`:

```
absent -> required=G, pending -> claiming -> claimed
claimed(G) -- newer H--> pending(H)
pending/claimed(G) -- older/equal--> unchanged
```

Only one caller may claim a pending generation.  It first advances the reusable
load-generation epoch, then removes the exact cache key, then marks the current
required generation claimed.  Equal, duplicate, old, and no-cache deliveries
are idempotent.  If a newer generation arrives during a claim, the old claim
does not mark the newer generation complete.

The registry keeps at most 1024 exact entries.  A claimed lower bound is not
simply forgotten: before reclaim, its identity is added to a fixed 16-Kbit,
process-randomized retired Bloom filter.  A transaction may have fixed an older
snapshot before the claim and start its first MATCH only later, so every retired
identity bypasses the process-global cache permanently for that process.  It is
still queryable through a one-shot load/search object that is destroyed after
the statement; an old snapshot can therefore complete without publishing state
that a later transaction could acquire.  Bloom false positives only disable
caching for an additional FULLTEXT2 identity; they do not change results or
availability.  DROP/recreate remains isolated by new hidden-table identities.

If every exact slot is still pending, installation remains fail-closed: it
bumps a process-wide FULLTEXT2 epoch, non-blockingly claims every visible
`fulltext2:` cache entry, and returns no ACK.  Existing pending fences stay in
their slots.  A load begun before the bump fails its pre-publish epoch check;
after a claim completes, the sender retry can retire that lower bound and
install the omitted identity without a process restart.

A load reads its durable generation before data and checks the registry both
after the generation read and immediately before publishing its `Index`.  A
lower generation destroys the in-progress object and returns the cache's
dedicated retryable-load marker.  FULLTEXT2 opts into four coherence attempts
with context-aware 5/10/20 ms backoff; other algorithms keep the existing cache
retry contract.

## Push protocol and ownership

MORPC v41 adds method 40 and request/response fields 42/43.  The request carries
the four identity components and two generation components.  A receiver first
installs the monotonic requirement, then claims eviction.  It responds with its
current required generation and `EvictionClaimed`.  The sender records ACK only
when the returned generation is not lower than the request and the claim flag is
true.  Unsupported v39 receivers and unknown methods are failures, never ACKs.

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

Shutdown stops ISCP producers before closing the publisher, waits for publisher
workers to exit, and closes the query client last.

## Pull recovery and lifecycle

The cache runs a 30-second single-flight freshness sweep, with at most sixteen
checks and a five-second context per entry.  A higher durable generation is
installed and evicted in the same sweep.  MATCH performs no catalog SQL on a
warm hit.  Pull repairs sender crash, lost RPC, and new-CN join; it is not a
successful ACK substitute.

DROP clears the exact cache, reusable pools, load generation, and fence.
Recreate is isolated by new hidden names.  MERGE/REBUILD keep their existing
transaction-local invalidation and are repaired remotely by pull; adding a
cross-transaction DDL notification is outside this issue unless a safe
post-commit hook already exists.

During a rolling upgrade, v41 senders treat v40 as unsupported.  New CNs retain
the pull fallback, but old serving CN binaries cannot provide the new guarantee.
Strict multi-CN QA and issue closure therefore require every serving CN to run
v41.

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
