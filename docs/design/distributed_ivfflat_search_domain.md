# Distributed IVFFlat search domains

- Status: in review
- Tracking issue: matrixorigin/matrixone#27854
- Implementation PR: same branch/PR as this document
- Design decision: preserve the former TVF's domain-before-search phase while
  keeping `VECTOR_INDEX_SCAN` typed and optimizer-visible

## Problem

Filtered IVFFlat PRE execution currently places `VECTOR_INDEX_SCAN` on one CN
and gives it one reader because its PK membership payload is available only on
the current-CN message board.  The TVF implementation waited for membership and
then launched a normally compiled inner entries query, so the expensive scan
retained ordinary CN and reader parallelism.  The typed scan removed generated
SQL but also removed that phase boundary and parallel execution width.

## Invariants

1. Scalar PRE membership is a mandatory exact search domain, not an optional
   runtime optimization.  Empty membership produces no candidates; missing or
   malformed required membership fails the query.  Correlated APPLY retains its
   row-local post-check until it has an equivalent domain phase.
2. The complete filtered PK stream is broadcast through the existing join
   topology.  Each participating CN builds the same complete domain before any
   vector reader on that CN opens.
3. Every participating CN observes the same immutable domain for one statement
   execution generation.  A retry uses a distinct generation and cannot consume
   an older message.
4. Entry objects have one physical owner per search.  Each CN owns a disjoint
   object partition and its local readers own disjoint portions of that
   partition.
5. Exact integer membership and safe distance ranges are applied by storage
   before local Top-K heap admission.  Each reader emits at most the candidate
   budget; the coordinator owns the global merge and final SQL recheck.
6. Cancellation and every producer, broadcast, reader, and partial-open error
   release domain payloads, cache leases, engine readers, heaps, and batches
   effectively once.
7. Domain memory remains under the existing broadcast HashBuild/query-memory
   admission.  Each CN reconstructs one exact filter and shares it across DOP;
   the filter and centroid ranking are not multiplied by reader count.

## Execution protocol

The planner keeps the final SEMI join but marks its membership payload as a
required vector search domain.  The established broadcast-join compiler sends
the globally complete filtered PK stream to one HashBuild on each probe CN.
Every vector scope waits on its local message board before constructing readers,
so the domain-before-search phase requires no new cross-CN message protocol.

When every selected CN supports the protocol, synchronous scalar/prepared
single-round searches may use all scheduled CNs and their reader DOP.  Older
clusters, async indexes without a global visibility watermark, correlated
APPLY, adaptive multi-round/include searches, explicitly disabled rollout, and
unsupported PK domains keep the established coordinator-only path.

Within a CN, one search session decodes the exact domain and ranks centroids
once, then expands the CN partition into scheduled, disjoint local reader
shards.  Every entries reader receives a share of the same exact filter;
storage intersects it with visible rows before distance-range and Top-K work.
Readers own separate scanners, heaps, results, and child contexts.  Adaptive
multi-round and correlated searches retain one reader because their cursor or
query vector is row-local.

## Alternatives and decision

- Post-Top-K membership is rejected because nearer nonmembers can occupy the
  bounded heap and under-fill the exact result.
- Disabling storage Top-K is rejected because it materializes and scores the
  selected 768-dimensional entry vectors in CN, recreating #27854's critical
  performance failure.
- The selected path uses the existing exact integer `docfilter` as a storage
  PK filter before vector Top-K, with one admitted filter and one centroid route
  per CN search generation.

## Compatibility and rollback

The required-domain plan field is append-only and gated by `MORPCVersion45`,
the next cumulative version after authoritative `main` consumed version 44 for
the validated MongoDB explicit-query payload. Open branches do not reserve
versions: before every push this branch merges newest main and renumbers only
if main has actually consumed its gate. A cluster below the gate executes
required domains coordinator-local. No catalog or hidden-table format changes.
The existing `forceOneCN=1` hint and intrinsic ForceOneCN fallbacks restore
local execution.

## Validation and acceptance

Deterministic tests cover the protocol and planner fallbacks, required-domain
UNIQUE/PASS/malformed terminals, multi-CN DOP selection, disjoint combined shard
ordinals, child-context cancellation, exact filter-before-Top-K ordering, and
the scalar/correlated reader-count contracts.  The full planner, compiler,
APPLY, and IVFFlat package suites remain the local merge gate.

Distributed BVT must return the same PRE results at one-CN and multi-CN width,
including an exact empty domain.  A persisted 8,192-by-768 benchmark must show
that exact membership is applied before storage Top-K without materializing
nonmember embeddings.  Wiki-10M evidence must record one-CN and three-CN QPS,
latency, CPU, peak memory, scanned bytes, and recall.  Three-CN throughput must
be at least 95% of the former TVF, no lower than current main, and at least 25%
above the one-CN candidate; recall may not drop by more than 0.001 and CPU,
memory, and scanned embedding bytes must remain within 110% of the former TVF.

Local persisted-object evidence on an AMD Ryzen 9 7950X3D, Go 1.26.4,
8,192 rows by 768 dimensions, ten iterations and three repetitions:

- local exact-membership Top-K: 0.333-0.338 ms/op and 3 MiB materialized
  embeddings/op;
- storage exact-membership Top-K: 0.199-0.203 ms/op and zero materialized
  embeddings/op.

The storage path is 1.64-1.70x faster at this boundary. Wiki-10M deployment
evidence remains a merge gate rather than being inferred from this benchmark.
