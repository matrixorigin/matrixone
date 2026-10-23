# Demand-sized startup allocation

WAL payloads and FIFO indexes use their existing owners and grow with demand.
These allocation choices do not change connection lifetime, cache admission,
persistence or wire formats, and do not constitute a new memory limit.

## WAL record ownership

[Logservice client pooling](../../pkg/vm/engine/tae/logstore/driver/logservicedriver/clientpool.go)
keeps eager backend connections, factory/retry policy, pool size and write tokens.
An idle client does not require a payload record. Its first append allocates at
least the configured reserve and initializes the exact entry length and
backend-owned header, including for a zero-length first entry.

Exclusive checkout owns the record through append/retry; return transfers that
ownership to the pool. An unused return needs no record. Subsequent growth and
oversized-return policy stay with the same owner. If every client writes, its
memory can approach the original footprint; lazy payload allocation does not
make the backend connection pool lazy.

## FIFO index ownership

[FIFO construction](../../pkg/fileservice/fifocache/fifo.go) creates non-nil shard
maps without reserving 1024 entries per shard. Index storage grows with occupancy;
shard locks, callbacks, ghost identity, pending-byte accounting and data capacity
retain their existing semantics. Dense insertion may pay map-growth cost earlier,
and maps are not promised to shrink after a historical peak.

## Regression and retirement boundaries

[Client-pool tests](../../pkg/vm/engine/tae/logstore/driver/logservicedriver/clientpool_test.go)
retain unused checkout/return, first zero/small/large append, buffer reuse, backend
record identity, cancellation, factory failure and pool-close controls.
[FIFO tests](../../pkg/fileservice/fifocache/fifo_test.go) and its consumers retain
construction, admission/eviction and dense-use contracts. These are existing
regression responsibilities, not new test results reported by this document.

The old embedded trace-queue sizing experiment is retired with the transaction
trace runtime. It is not a current acceptance requirement. The
[retirement contract](20261003-retire-transaction-data-trace.md) still protects
inert configuration/session declarations, catalog registrations and historical
data during the supported rollback window; removing queue workers did not
satisfy those separate declaration-removal prerequisites.

The [original design and experiment](https://github.com/matrixorigin/matrixone/blob/6cbe375b49696021d72778baa292a66287f080bd/docs/design/UT_STARTUP_ALLOCATION.md)
preserves the local heap profile, tradeoffs and validation scope. A historical
startup allocation reduction is neither a leak diagnosis nor a current Linux
whole-CI speedup. RSS/native memory, race runtime and file cache are not measured
by a Go heap profile alone.
