# Filtered IVF PRE read cost and placement

## Scope and evidence

Issue #29531 reports a filtered PRE throughput gap between main and 4.2.
This change is based on main `c2abd6a54b7cd3e13c1b1494388cd7a81b8369d4`
and covers reader policy and required-membership scan placement. The initial
read-policy repair alone does not remove the warm throughput regression.

The typed relation scanner bypasses the ordinary SQL scan adapter. Its readers
can prefetch whole data objects before applying an exact filter. The reader
also discards context FileService policy, and the early scalar filter loader
hardcodes policy zero. Filtering few rows consequently does not guarantee
that unrelated object bytes avoid prefetch/cache admission.

Logical extent sizes and cache-hit counters do not measure actual I/O or
actual decompression. A legacy LZ4 extent can require full decompression even
for one selected row. Suppressing prefetch does not eliminate that cost.

## Ownership and implementation

| Owner | Contract |
| --- | --- |
| `scanEntriesInDomain` | For membership or scalar residual filters, request `SkipFullFilePreloads`. Metadata, centroids, and unfiltered entries retain their default policy. |
| `RelationScanRequest` | Carry a value `ReadPolicy`; no new execution interface or persistent state. |
| `relationScanner.ScanRelation` | OR the request with inherited FileService flags in a local context. Never mutate `proc.Ctx`. |
| `readutil.Reader` | Initialize from the read context. Above the existing block threshold, OR `SkipMemoryCacheWrites`; preserve other flags and the threshold boundary. |
| `blockio.ReadDataByFilter` | Pass the same explicit policy to both cached-search and general scalar-column loaders. |
| `LocalDisttaeDataSource.Next` | Skip data-object prefetch when requested; retain the existing ordinary scan behavior. |
| `RemoteDataSource` | Skip only data-object prefetch; retain tombstone prefetch and batch cursor advancement. |

Existing merge readers already forward context to their children; they need no
new policy field or setter. Both capable and fallback readers consume the same
request. `PostFilterTopOnly` cannot identify a filtered scan: unfiltered DESC
and unsupported distance expressions also use it. Passing a distance function
to `SetOrderBy` is invalid because the data-source order adapter expects a
column reference.

## Invariants and limits

- No changes to write, flush, merge, writer policy, object encoding, chunk
  size, storage layout, catalog, or existing stored data.
- Preserve exact membership, `MustApply`, scan-local placement constraints, snapshots and tombstones,
  probe count, candidate budget, distance qualification, recall, and ties.
- Preserve necessary scalar/function/cast/type and distance-bound fallbacks;
  no duplicate ranking, decoder, cache, allocator, or scheduling path.
- Existing full-object disk cache files remain usable by range reads. Existing
  decoded memory hits retain their behavior.
- Dense reads can save few bytes while issuing more range requests. Measure
  this control; a filtered request is not proof of lower latency.
- The proposed no-write shared-decoder/allocator extension is excluded. It
  lacked a relevant query witness and a complete peak-memory bound. The
  original 31-block diagnosis does not reach the 1024-block threshold.

## Validation

Reuse existing fixtures and expected BVT results. Required unit evidence:
request classification; inherited flags and caller-context isolation; real
reader threshold equality and greater-than boundaries; both early scalar
loader branches with visibility/tombstones; local three/four-range controls;
remote data prefetch suppression with tombstone and cursor preservation.
Observe terminal prefetch calls for positive controls. Negative controls use
an isolated service without an IO pipeline, so an erroneous submission fails
synchronously; pipeline shutdown alone cannot prove that nothing was queued.

Run affected normal and race UT and incremental repository SCA. Run existing
vector BVT twice with comparison and teardown, including membership, INCLUDE,
prepared ranges, quantized/narrow types, nulls, DML synchronization and Top-K
consumers. Independent QA must report failures and uncovered scenarios.

Benchmark versions sequentially against identical logical data and explicit
resource limits; main before/after must use the same stored objects. Separate
cold object reads, compressed disk hits, and decoded memory hits. Record actual
bytes/requests, results, plan domain and budget, repeated latency/throughput,
and variation. Preserve the distinction between a causal reproduction and
coverage of the original Wiki10M deployment.

## Causal reproduction

A 40k × 128D fixture retains a membership domain of 800 keys, above the
100-key exact-search shortcut, lists=64, probe=5, K=10. Eight batches of
ordinary synchronous inserts followed by hidden-entries flush create multiple
objects. Versions run sequentially with separate storage namespaces.

| Fixed input and stored index within each version | Median QPS |
| --- | ---: |
| main, two CNs available, 16 MiB each, required search executes locally | 32.63 |
| main, same objects, 64 MiB query cache | 206.20 |
| 4.2, two CNs, 16 MiB each | 191.09 |
| 4.2, same objects, one eligible CN, 16 MiB | 27.52 |

Main's 16/64 MiB controls retain the same eight entry blocks and 40000:60
filter rows; disk-cache reads disappear with sufficient decoded capacity.
Both CNs are measured: 4.2 performs repeat memory reads remotely without disk
reads or cache conversions, while main's entry work remains local. Ten query
Top-10 sets match across versions, with independent membership, uniqueness,
distance ordering, and equal recall checks. This supports cache placement as
the mechanism in this reproduction; it does not extrapolate its exact speedup
to the original 10M × 768D deployment. Extent bytes, returned scan batches,
and memory-read attempts are not physical I/O or decode counts.

## Required PRE placement

Reuse the existing broadcast SEMI join. Its complete PK build input is merged
and dispatched to every probe CN; each local HashBuild publishes exact keys
only after consuming EOS. Each vector reader validates its local required
domain before opening entries. Keep the vector shards beneath this SEMI;
merge candidates through the existing outer join and Top. No new runtime
filter transport, protobuf payload, cache, worker pool, or lifecycle is added.

A shared pure plan qualifier accepts one synchronous, single-round, integer-PK
IVF scan with its direct required left SEMI and ordinary row-fetch INNER join.
Transparent wrappers and regular INDEX accesses must retain direct PK equality
and resolved source identity. Build/probe tags must be unique and complete;
no partial build limit, shuffle, right join, shared source, APPLY, asynchronous
search, first-round/bucket expansion, CTE/window/sequence or adaptive vector
selector is admitted. Unknown shapes retain local execution.

`GetExecType` uses that same qualifier. Only ForceOneCN table scans inside
fully verified regular INDEX access subtrees can be exempt from the query-wide
veto; their original flags remain for scan placement. Other ForceOneCN nodes
and existing query-wide restrictions remain effective. A required vector that
fails the final qualifier remains local even if an earlier planner pass cleared
its own flag. Do not create a second scheduler or temporarily override exec type.

Work estimation reuses ScanWork: after cheap semantic qualification, at most
one hidden-table resolve and one existing Stats call per vector node. Later
phases consume that result. Unknown/invalid work or fewer than two objects
keeps local execution; cancellation propagates. Stats may refresh synchronously
on a cold/expired cache: measure first, hot and expired planning costs. Having
ScanWork does not enable local DOP without its existing hint.

The existing worker-constraint phase requires a readonly workspace, coordinator
membership, compatible selected workers and CPU centroid dispatch. Failure
selects the actual local scheduler path for the entire query. Required scans
never drop an incompatible shard while keeping a distributed identity.

The existing broadcast build owner proves a complete Dispatch-All source.
The existing final runtime-filter topology validator checks one colocated
producer per consumer, all logical partitions, and complete broadcast edges
from that same source, including local registers and remote UUID receivers.
Ordinary local INDEX producers are IndexBuild operators, not HashBuild.
Keep their consumer/producer graph local. Missing/duplicate/partial/Any/Shuffle
edges fail before execution; required filters cannot be disabled to continue.
Remote producer and consumer must share the query message board.

## Reader, route and protocol contracts

- Distributed required generations use one reader per CN. Preserve logical CN
  count/index and assign memory/workspace rows only to the coordinator via
  IsRemote, including when its stable ordinal is nonzero. Existing local DOP
  retains its own partition override. Metadata/centroids remain replicated;
  entries use existing stable ObjectID sharding and snapshot visibility.
- Every shard retains the full original budget B and probe. The candidate union
  is bounded by P×B and contains global Top B; existing downstream operators
  merge it. Residual predicates may see more candidates than a single B window;
  disclose that behavior. Preserve existing ties, casts, bounds and fallbacks.
- Qualified distributed required readers use a private immutable CPU centroid
  policy, a discriminator in the existing centroid-cache key, and the existing
  pure-Go constructor. Preload reserves host bytes; Load and cached Search
  verify the same backend. Existing default/GPU keys remain separate. Origin
  GPU dispatch stays local; a remote nil resolver cannot change the route.
- Add the next MORPC capability version (102 on this base). Existing fragment
  visitors compute the maximum required version, including nonzero required
  shards. Compilation, actual destination, same stream and decode all enforce
  it. Old/unknown workers cause whole-query local fallback; rollback after
  compilation fails closed with existing cancellation and cleanup.
- Reuse native admission, snapshot clone, generation reference ownership,
  construction unwind and Close. Broadcast duplicates PK/domain state per CN
  and increases cluster memory/network cost; it never broadcasts embeddings.
  Account for concurrent HashBuild/domain scratch and possible simultaneous old
  and CPU centroid cache entries within existing admission limits.

## Validation and review

Test actual completed SQL plans and compiled graphs, not only handmade shape
fixtures. Cover ordinary INDEX local placement, complete broadcast source and
per-CN domain, delayed/error/empty/PASS/cancel build, nonzero coordinator,
unflushed/deleted rows, generation cleanup, mixed/unknown/rollback protocol,
CPU route and cache contamination, and the nearest unsupported local controls.
Use existing fixtures and deterministic barriers. Run incremental SCA, affected
normal/race UT and existing vector BVT with unchanged expected results.

Candidate performance uses the same main stored objects, queries, domain,
probe and budget at 16 MiB per CN, then the 64 MiB hot control. Verify stable
object coverage, identical centroid route and quality, per-CN cache/conversion
activity and repeated QPS. A faster query without the expected mechanism is
insufficient. Reuse the completed 4.2 and cache-size counterfactuals; larger
nightly data is a deployment coverage limitation, not a substitute for this
causal test or a reason to expand local resource use.

The Q1 policy design was approved by GPT-6.1-sol xhigh from Astra xhigh v2
(SHA256 c6c95bca15aacc79baeef7a87deaf1a5dba5b44a4095013a447acbf7f5d43d56).
Placement follows Astra xhigh v4, independently reviewed by GPT-6.1-sol xhigh
(SHA256 7b3535a3281efd5ebc568b3165a3765c309323979b4807b262b81e3558c9af99).
Rollback restores local required placement and prior read policy. There is no
persistent migration or write change; new CPU cache keys expire normally.
