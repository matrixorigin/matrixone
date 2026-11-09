# Scoped column sharing and warm disk fills on 4.2

This design adapts [#28413](https://github.com/matrixorigin/matrixone/pull/28413)
and [#28918](https://github.com/matrixorigin/matrixone/pull/28918) for the
4.2 FileService to address [#32223](https://github.com/matrixorigin/matrixone/issues/32223).
The upstream design and measurements describe main; they are not evidence for
this branch's allocator, cache admission, or performance.

Concurrent readers can miss MemCache for the same immutable column and each
read and decompress a warm DiskCache extent. The goal is to avoid that duplicate
work when requests overlap, while preserving the existing cache policy, read
results, cancellation, and resource ownership. This does not change the SQL,
wire, catalog, or on-disk formats and does not claim to resolve all causes of
an incident-scale performance regression.

## Ownership and eligibility

The existing 4.2 MemCache and FIFO cache own eviction and admission. S3FS and
DiskCache continue to call their existing `EnsureNBytes` guards before cache
allocation. A new fill is attempted only for a known warm disk entry, a decoded
size no larger than current cache capacity, and no active memory-pressure target.
This is an admission **hint**, not a reservation or a guarantee that a later
cache update will succeed. Existing cache entries may be evicted; rejection,
eviction, or pressure changes after election must leave every follower able to
read independently. No separate counter attempts to represent all outstanding
allocations.

ObjectIO opts in only scoped, single-column array Top-N reads whose complete
IOVector remains alive until the borrowed column view has been consumed. Only
owned row coordinates and distances leave that scope. The descriptor identifies
the immutable conversion and validation semantics. The FileService-local key
also includes the canonical object path, offset, compressed and decoded sizes,
policy, codec, and parameters. Paths must identify immutable, non-reused objects.
Custom vector caches, skipped memory reads or writes, caller-owned input buffers,
streams, writers, invalid descriptors, and multiple selected entries bypass
sharing. Other reads and FileService implementations keep their existing paths.

## Two distinct lifetimes

A disk-fill generation owns a notification and participant tickets, not data.
The leader rechecks memory, reads the disk cache, and signals after its deferred
memory update completes. If the disk read is incomplete, it signals before
continuing to remote or object storage. Followers wait at most one second,
then recheck memory; a miss continues through their normal read path. A leader's
error or cancellation is not transferred to a follower. A pressure change may
prevent a new generation but does not prevent a bounded join to an existing one.

A decode generation owns one retained reference to a successfully converted,
validated immutable column. Each participant receives its own data reference
and a lease attached to the final IOEntry; the complete IOVector releases both.
The last lease removes only its own generation and releases its registry
reference. Persistent cache references are independently owned by the cache.
A follower waits at most 200 ms for conversion. Timeout, abandonment, or an
unsuccessful leader causes that follower to run its own conversion; a read that
falls through to another storage tier does not rejoin and wait again. A caller's
own cancellation takes precedence when already observable at wakeup. Existing
DiskCache conversion-error fallback and S3FS cache-update error behavior remain
unchanged. If a disk conversion fails after observing a close, the guarded read
stops before trying another tier; a canceled or expired caller keeps its own
context error. A complete result already selected from disk remains valid.

Fill and decode lifetimes differ: a fill ends at publication or disk fallback,
whereas decoded data remains alive until all scoped consumers release it. They
therefore use separate bounded generations under one registry rather than
conflating publication with consumer ownership. No I/O, conversion, cache update,
or data release runs under the registry mutex.

## Bounds and shutdown

Fill coordination allows at most 64 live generations and 128 participants per
generation. Decode sharing has separate 64-generation and 128-participant limits
and an addressable-backing budget of at most the smaller of 64 MiB and initial
MemCache capacity. Admission charges the allocator's conservative size class
before conversion. A result is shared only when its logical and retained
addressable sizes are known and fit the charge. Allocator-owned `Bytes` retains
its capacity across `Slice`; an arbitrary caller-supplied `NewBytes` view has
unknown backing and bypasses sharing. ObjectIO forwards size information without
exposing or replacing its validated-column marker.

This bound covers backing retained by admitted sharing generations, including
old generations waiting for final release. It is not a bound on RSS, allocator
overhead or pools, persistent cache, compressed input, or independent fallback
allocations. Reaching a limit bypasses sharing instead of waiting for capacity.

Close seals new sharing and wakes waiters. Cache retirement is deferred until
active guarded reads and their cache updates finish, without waiting for a
returned Top-N consumer. Existing consumers retain valid data until they release
their vectors. Generation identity prevents late release from removing a newer
generation; panic and error paths abandon unfinished work and release ownership.

## Validation contract

Exercise real cache admission and real tiers: use a scoped pressure target that
rejects the decoded entry, verify the memory miss and disk/object-store counters,
and hold overlapping vectors until their results are inspected. Separately
verify an admissible warm-disk fill with controlled leader, follower, and
publication barriers. Cover independent keys, memory hits, cold reads, quota
exhaustion, failed and canceled leaders/followers, timeout, incomplete disk
reads, close, panic, and final release. A persisted ObjectIO/BlockIO Top-N
consumer must preserve row and distance results for distinct queries, selections,
and bounds.

Compare 4.2 sharing off/on using the same data and policy. Benchmarks must report
decodes and storage/cache counters so a memory-hit workload cannot masquerade as
warm-disk sharing; retain memory-hit and independent-key controls. Historical
main measurements are not a substitute for branch-specific evidence.
