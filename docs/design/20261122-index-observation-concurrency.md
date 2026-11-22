# Concurrent index-cost observations

Status: accepted for implementation; issue [#33059](https://github.com/matrixorigin/matrixone/issues/33059). Design reviewed against base `f3be95188736bf00f47e23a0e223628375bed7c8` before implementation.

## Problem and contract

Index costing may refine a covering-index winner using cached statistics and block metadata. A transient failure to acquire a nonblocking lock discards that observation and retains the ordinary winner. With ordered data this can select an index that reads more than a block-pruned table scan. Holding the statistics coordinator reproduced the fallback. A separate concurrent SQL probe found misses on the engine's partition-map lock and the metadata cache's index-shard lock. The original CI log does not identify its failing gate.

Eligible readers should share already-published, coherent statistics and partitions during unrelated producer or waiter coordination. The observation stays nonblocking and declines on writer contention, missing or invalid input, changed read domains, cancellation, or exhausted budgets. The result is a cost hint. Table identity, schema, and row visibility still come from their owning snapshots.

## Ownership and synchronization

### Statistics

`GlobalStats` retains one pair of statistics and schema-version maps. The existing mutex protects ordinary reads and coordinates producers, cleanup and waiters. The planner uses `observationMu.TryRLock` to copy the required scalar inputs. Writers must hold both locks, taking the observation write lock inside the coordinator only for mutations. The observer releases its lock before revalidating the transaction. Writers release it before notifying waiters or other nodes.

Ordinary reads have no wait on the observation mutex. A publisher can wait for an observation that has already acquired it. New observations decline as soon as a writer has reserved it. Evaluating blocking ordinary reads on a separate data mutex produced long publication tails in mixed `Get`/`PatchStats` tests, so this design keeps ordinary operations on their original mutex.

Waiters hold the coordinator from the cache predicate check to condition wait registration. Publication installs the value and its optional schema binding in one critical section. Explicit refresh rechecks request and owner cancellation after acquiring the observation write lock. Automatic publication rechecks owner cancellation at the same boundary. Removal retires statistics and producer state under the coordinator. Pointer, schema, and generation checks reject stale observations and publications.

### Latest partition lookup

For an existing `(database ID, table ID)` entry, the normal accessor reads under `Engine.RLock`. A present nil remains a present entry. On absence, it releases the read lock, takes the existing write lock and checks again before constructing a partition. It never upgrades a lock or preconstructs an unused partition. Cleanup and creation retain the existing exclusive lock and lifetime rules. The optional observer still uses `TryRLock`, never creates a partition, and validates snapshot, subscription and transport identity before accepting its result.

## Alternatives and validation

Reading mutable statistics without a lock would race with partial patches and lose pointer/version validation. A second snapshot cache would add publication and eviction ownership. Blocking the optional observation, retrying it, or forcing a scan strategy would change its latency or plan-selection contract.

The metadata-cache experiment exposed a tradeoff under concurrent reads and fills. Shared index reads improved try-lookup availability, while the initial design made a frequency decrease wait for index readers. A second design removed that wait and preserved exact saturated frequency. The tested versions still showed slower fills in some capacity-pressure workloads, so this change uses the existing metadata cache. Contention there can still make block input unavailable.

Deterministic tests cover coordinator and reader coexistence, writer fallback, publication, cancellation, partial block-observation rejection, and partition creation and reuse. The SQL controls check both ordered and scattered data and the returned rows. Concurrent SQL measurements report plan choices with the final scope and preserve real fallback cases.
