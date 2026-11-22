# Concurrent index-cost observations

Status: accepted for implementation; issue [#33059](https://github.com/matrixorigin/matrixone/issues/33059). Design reviewed against base `f3be95188736bf00f47e23a0e223628375bed7c8` before implementation.

## Problem and contract

Index costing may refine a covering-index winner using cached statistics and block metadata. A transient failure to acquire a nonblocking lock discards that observation and retains the ordinary winner. With ordered data this can select an index that reads more than a block-pruned table scan. Holding the statistics coordinator reproduced the fallback. A separate concurrent SQL probe found further misses on the metadata cache's index-shard lock and the engine's partition-map lock even though the metadata was present. The original CI log does not identify its failing gate.

Eligible readers should share already-published, coherent data without excluding one another or unrelated producer/waiter coordination. The observation must remain nonblocking: actual writers, missing or invalid metadata, changed read domains, cancellation, and exhausted budgets retain their existing failure behavior. An estimate is a cost hint, not authority over rows or schema. The same SQL can still receive different plans when an observation is genuinely unavailable.

## Ownership and synchronization

### Statistics

`GlobalStats` retains one authoritative pair of statistics and schema-version maps. Its existing mutex remains responsible for producer generations, cleanup, condition predicates and notification. A separate read/write mutex protects the maps and the in-place statistics fields copied by the observation. Ordinary cache hits use a read lock; the optional observation uses `TryRLock` and copies its complete scalar input under that lock. Data writers hold the coordinator and then the cache write lock. The cache lock is a leaf: it is released before acquiring other application locks, broadcasting, waiting, or revalidating the transaction.

Waiters keep the coordinator across their durable predicate check and condition wait registration; their cache read lock is released before inspecting the producer or waiting. Publication installs the value and its optional schema binding in one cache critical section. After a cache-lock wait, explicit refresh rechecks request and owner cancellation before committing, and automatic publication rechecks owner cancellation. Patch and remote publication retain their existing cancellation contracts. Removal retires cache and producer state under the coordinator; an old pointer or generation cannot authorize a replacement publication. A real cache writer can still make the try-read decline.

### Metadata cache

S3-FIFO provides an explicit opt-in constructor for caches whose values support concurrent `Retain` calls on the same value. The existing constructor and its serialized callbacks stay unchanged. Only the object metadata cache opts in: its immutable `metaBytes` are Go-owned and `Retain`/`Release` are no-ops. An admission-free `Get`/`TryGet` on an opted-in cache takes a shared index-shard lock; calls with an admission callback and all structural mutations remain exclusive. A successful lookup retains its value while the entry is protected and preserves normal caller ownership, admission cleanup and hit-frequency accounting.

Shared hits increment the existing saturated atomic frequency. The opted-in cache serializes the main-queue frequency decrement with the index write lock, in the existing policy-to-index order. While a shared reader holds the index lock, the frequency can only increase to its maximum of three, bounding the hit's compare-and-swap work. The write lock is released before requeueing. An actual mutation, admitted callback or pending writer may make `TryGet` decline. The index mutex becomes an RWMutex even for default caches, increasing fixed per-cache overhead by roughly 4 KiB on linux/amd64; no per-entry allocation or background work is added.

### Latest partition lookup

For an existing `(database ID, table ID)` entry, the normal accessor reads under `Engine.RLock`. A present nil remains a present entry. On absence, it releases the read lock, takes the existing write lock and checks again before constructing a partition. It never upgrades a lock or preconstructs an unused partition. Cleanup and creation retain the existing exclusive lock and lifetime rules. The optional observer still uses `TryRLock`, never creates a partition, and validates snapshot, subscription and transport identity before accepting its result.

## Alternatives and validation

Reading mutable statistics without a lock would race with partial patches and lose pointer/version validation. A second snapshot cache would add publication and eviction ownership. A blanket shared S3-FIFO lookup would silently allow arbitrary retention and admission callbacks to overlap. An opt-in try-only lookup would still conflict with ordinary metadata reads. Blocking the optional observation, retrying in a loop, or forcing a single scan strategy would change its latency or plan-quality tradeoffs.

Deterministic tests cover coordinator and reader coexistence, actual-writer fallback, cancellation while publication waits, coherent schema binding, removal/republication, partial block-observation rejection, reference and admission ownership, cache eviction and frequency accounting, and existing/present-nil/concurrent-first partition lookups. Existing ordered and scattered SQL controls verify plan choice and returned rows. Race tests cover the owning packages and named SQL integration case. Matched baseline/candidate measurements report hit availability, cost and concurrent SQL plan counts; they are observations, not universal latency or production-incidence guarantees.
