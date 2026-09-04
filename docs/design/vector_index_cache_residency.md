# Named-snapshot index reads and bounded cache retention

- Issues: [#27941](https://github.com/matrixorigin/matrixone/issues/27941),
  [#27927](https://github.com/matrixorigin/matrixone/issues/27927)
- Implementation: [#28018](https://github.com/matrixorigin/matrixone/pull/28018)
- Design review revision: 2026-09-05. CPU validation completed; real-GPU validation pending.

## Problem and invariants

A historical base-table scan must not join candidates from a current index.
The original fulltext/fulltext2/HNSW/CAGRA/IVF-PQ table functions executed their
nested index SQL in the current transaction. After an update, historical MATCH
could return a row for a word absent from its historical text; vector top-K
could return no rows after the historical join rejected current-only candidates.

The planner passes ScanSnapshot through the table function. SqlProcess binds
the historical timestamp and effective execution account together. The index
SQL clones the transaction at that timestamp. The cache key uses exactly the
same effective timestamp. A historical load cannot populate the current key.
Publisher identity takes precedence over snapshot identity for a subscribed
table, matching the base-table scan. Same-key readers share the load.

Classic fulltext is not an in-memory index cache consumer. IVF-FLAT's relation
scanner already binds its snapshot; its cache interface must still compile.

## Accepted limitations

Indexes update asynchronously. Reading their persisted state at a named
snapshot fixes mixing historical rows with current candidates; it does not
prove the index had caught up to the base table when that snapshot was taken.
Tests establish CDC convergence before snapshot creation and before checking
the current-query control.

Cache retention is different from total process memory. mmap pages, query
workspace, allocator overhead, in-progress loads, and other subsystems are not
all represented by GetIndexSize. The governor is a target for reusable index
residency, not an allocator reservation or a promise that concurrent queries
cannot exhaust RAM/VRAM. Native allocation and per-device admission checks
remain necessary. A separate system-wide admission controller is out of scope.

A valid query larger than the retention target can execute if the algorithm's
own allocation gates permit it. Its index is transient and is retired after
its readers return. This can make repeated oversized queries slower; an
operator can raise the target when the deployment has enough memory.

LOCAL spill files retain explicit owner cleanup. HNSW unlinks newly created
files after successful mmap, so process teardown releases those blocks.
Files left by a crash before mapping may remain. A configured CN UUID is not
a process-liveness lease: first-use directory lookup must not delete files
that an overlapping previous process could still need to open. Safe orphan
collection needs a separate lifetime/lease contract and is not added here.

## Scope decision and alternatives

The PR keeps historical reads, size accounting, pre-load reclamation,
post-query retirement, and LOCAL-fileservice spill placement. It does not add
nrow/build_ts columns, change the catalog upgrade identity, or change ALTER
relkind handling. The unused provenance extension required a migration and
old/new writer compatibility unrelated to the historical read contract.

The existing four-column vector metadata and six-column FULLTEXT2 metadata,
including their writer formats, remain unchanged. No protocol version bump or
new schema migration is needed for this fix. Upgrading a production cluster
therefore does not require rewriting every hidden metadata table. Downgrading
to an older binary reintroduces the historical-read bug, but not a schema
mismatch. An experimental deployment of an earlier unmerged revision that
already widened tables is not a supported rollback baseline.

Alternatives considered:

1. Unbounded per-timestamp caching: good reuse but one client can retain many
   complete historical indexes before TTL runs.
2. Fixed generation counts and refusal: index sizes vary greatly; a count
   either wastes capacity or fails unrelated valid queries. Per-index counts
   do not bound aggregate cache retention.
3. Query-owned loads only: avoid retention, but lose same-key sharing and
   ordinary repeated historical-query reuse.
4. A hard byte admission scheduler: requires accurate allocation and workspace
   reservations for every backend, query cancellation, fairness, and per-device
   placement. GetIndexSize estimates cannot provide that contract. Do not
   disguise an approximate cache policy as this scheduler.

The selected design uses bytes for retention, shares reusable entries, and
makes entries transient when reclaiming idle entries cannot meet the target.

## Budget and ownership

max_index_cache_size and max_gpu_index_cache_size are dynamic GLOBAL variables.
GLOBAL is per account in MatrixOne. A tenant's explicit value restricts that
tenant. The SYS account's value restricts aggregate retention on each CN.
A tenant cannot raise the CN target by setting its own value or a session value.

Zero selects automatic sizing, rather than an unreachable 64 TiB ceiling:

- Host: one quarter of the smaller nonzero host/cgroup limit.
- GPU: one half of the physical visible devices' aggregate memory.
- Unavailable capacity measurement: a finite 256 MiB retention target.

Automatic limits are initialized once per cache. Operators can explicitly
resize targets after deployment or vertical resizing. The GPU total is only
an aggregate retention policy: each backend still validates the demand on
each actual device, including replicated and simulated placement.

Cached ownership is the account that executes the index read. Cross-account
snapshots and subscriptions resolve that owner's cap, not the caller's cap.
Catalog reads retain only service identity and memoized values, never a query
session or transaction. Successful values survive transient refresh failures.
The first failed lookup uses the automatic target. Refresh attempts are
rate-limited to 15 seconds; they use independent, bounded SQL contexts.

## Lifecycle and reclamation

The cache map owns a loaded index until one eviction claimant removes it.
The existing per-entry lock protects loading, searching, and destruction.
An eviction's teardown signal closes only after destruction finishes, so a
retry cannot reuse the same algorithm while an old wrapper is destroying it.

On a cache miss:

1. Preload reads metadata or measures artifacts.
2. Reclaim idle entries for the estimate.
3. Load materializes the index and publishes its measured size.
4. Reclaim again against actual retained bytes, protecting the loader's first
   search. If the target still cannot be met, mark this entry transient.
5. Execute the search. Success, error, and panic unwind the read lock before
   attempting transient retirement.
6. The last reader that can obtain the entry's write lock removes and destroys
   the transient entry. New attempts after removal load a fresh generation.

Capacity eviction obtains TryLock and holds it through the eviction claim,
map removal, and destruction. It never unlocks an availability probe and then
waits for a victim's lock. Busy entries are skipped; there is no second pass
that synchronously waits on an unrelated query. Protecting each loader through
its first search also prevents competing misses from evicting one another
before either query can run. TTL, explicit invalidation, and shutdown retain
their own existing lifecycle contracts.

Reclaim is coldest-first. At the CN scope the requesting tenant gives up its
own idle entries before other tenants' entries. Host and device usage are
separate: a host-only victim cannot relieve VRAM pressure. Atomic size samples
avoid reading a backend concurrently with its destruction.

## Maintenance and performance

A warm cache has no misses to refresh a memoized cap. The existing maintenance
task therefore refreshes SYS policy and then applies it. The policy update
latency is the maintenance cadence (normally 2.5 minutes), plus the bounded
catalog query; 15 seconds is a lookup memo lifetime, not a 15-second rollout
promise. Per-tenant policy changes take effect on that tenant's next miss.
Maintenance does not retain a user transaction or block the ticker on a new
catalog read. Existing freshness checks retain their cadence.

Ordinary hits do not resolve policy, enumerate entries, sort victims, read
metadata, or create a speculative cache wrapper/channel. They load the existing
map entry, execute under its read lock, and check whether retirement is needed.

A miss enumerates resident entries, O(N), and sorts them, O(N log N), only
when that arena requires reclamation. Repeated eviction details are DEBUG;
cumulative eviction counts/bytes are available through EvictionStats.

HNSW reports usearch MemoryUsage after loading. The persisted model is mmapped
from the LOCAL fileservice, and its file size is not a substitute for native
heap bookkeeping. Existing metadata cannot estimate that cost before Load;
this is an explicit limitation, shared with the existing IVF-FLAT load path,
rather than a reason to introduce a schema migration. FULLTEXT2 preloads its
existing document count; CAGRA/IVF-PQ measure host and device artifact components.
Their permanent hardware gate precedes eviction; the situational free-memory
gate runs after reclamation and before deserialization.

## Validation and rollout

Preserve the public historical/current controls for classic fulltext,
FULLTEXT2, HNSW, CAGRA, and IVF-PQ. Keep public schema-generation tests asserting
the existing metadata column count and writer shape.

Focused cache tests cover:

- same-timestamp sharing and distinct historical/current identities;
- all victims busy, a busy victim with an idle alternative, and exact-fit versus
  oversized entries in either arena;
- retirement on success/error/panic and after the last shared reader;
- backend invalid-state errors propagated without cache retries, even when
  post-query retirement has concurrently destroyed the entry;
- catalog-source cap changes without a miss, refresh failure, tenant ownership,
  and independent host/device accounting;
- realistic automatic limits, missing capacity information, integer boundaries,
  and tenant settings that cannot raise the automatic CN target.

Run owning-package tests and focused race tests for these lifecycle transitions.
Compare warm-hit allocations and latency before/after; performance measurements
are benchmarks, not wall-clock assertions in unit tests. Run the snapshot and
variable BVT cases in normal comparison mode, and state CPU versus real-GPU
evidence explicitly. Do not infer GPU execution from ordinary CPU CI.

Validation of this revision:

- Full cache, HNSW, FULLTEXT2, sqlexec and CPU plugin-schema package tests passed.
- Full cache race run and ten repetitions of the new deterministic cache
  lifecycle/configuration counterexamples passed. Spill ownership tests also
  passed ten repetitions under race.
- Native HNSW tests verify post-load MemoryUsage and file/handle cleanup.
  Planner/TVF snapshot, compile snapshot transport, IVF-FLAT account/snapshot
  and ISCP spill-routing tests passed.
- Public CPU BVT covers classic fulltext, FULLTEXT2 and HNSW historical/current
  answers, global-variable scope and a one-byte tenant cache target. The
  low-budget control is an exact scan, so it cannot warm the index and bypass
  the next-miss policy under test.
- Warm SearchInto microbenchmark: zero bytes/allocations per hit, versus
  352 bytes/four allocations with the previous speculative-wrapper pattern.
  This isolates cache wrapper allocation; it is not a SQL throughput claim.
- The real CAGRA/IVF-PQ snapshot and admission-order GPU suites remain a
  pre-merge requirement; a macOS CPU run does not satisfy that gate.
