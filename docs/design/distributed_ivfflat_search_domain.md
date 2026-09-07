# Coordinator-local IVFFlat search domains

- Status: in review
- Tracking issue: matrixorigin/matrixone#27854
- Implementation PR: same branch/PR as this document
- Design decision: preserve the former TVF's domain-before-search phase and
  local reader parallelism while keeping `VECTOR_INDEX_SCAN` typed and
  optimizer-visible. Required domains remain coordinator-local until the
  representative distributed performance gate is satisfied.

## Problem

Filtered IVFFlat PRE execution currently places `VECTOR_INDEX_SCAN` on one CN
and gives it one reader because its PK membership payload is available only on
the current-CN message board. The TVF implementation waited for membership and
then launched a normally compiled inner entries query. The typed scan removed
generated SQL but also removed that phase boundary and local reader width. This
change restores the exact phase boundary and local DOP without multiplying an
unmeasured domain, scan, and merge cost across CNs.

## Invariants

1. Scalar PRE membership is a mandatory exact search domain, not an optional
   runtime optimization. Empty membership produces no candidates; missing or
   malformed required membership fails the query. Correlated APPLY retains its
   row-local post-check until it has an equivalent domain phase.
2. The coordinator builds the complete filtered PK stream before any local
   vector reader opens.
3. Every local reader observes the same immutable domain for one statement
   execution generation. A retry uses a distinct generation and cannot consume
   an older message.
4. Entry objects have one physical coordinator-CN owner per required-domain
   search. Local readers own disjoint portions of that object partition.
5. Exact integer membership and safe distance ranges are applied by storage
   before local Top-K heap admission. Each reader emits at most the candidate
   budget; the coordinator owns the merge and final SQL recheck.
6. Cancellation and every producer, reader, and partial-open error release
   domain payloads, cache leases, engine readers, heaps, and batches effectively
   once.
7. Domain memory remains under the existing HashBuild/query-memory admission.
   The coordinator reconstructs one exact filter and shares it across local
   DOP; the filter and centroid ranking are not multiplied by reader count or
   CN count.

## Execution protocol

The planner keeps the final SEMI join, marks its membership payload as a
required vector search domain, and forces that vector scan onto the coordinator
CN. Every local vector scope waits on the coordinator message board before
constructing readers, so the domain-before-search phase requires no new
cross-CN message protocol.

Synchronous scalar/prepared single-round searches may use the coordinator's
reader DOP. Async indexes without a global visibility watermark, correlated
APPLY, and adaptive multi-round/include searches retain one reader when they
own mutable or row-local state.

One coordinator-CN search session decodes the exact domain and ranks centroids
once, then expands its object partition into scheduled, disjoint local reader
shards. Every entries reader receives a share of the same exact filter; storage
intersects it with visible rows before distance-range and Top-K work. Readers
own separate scanners, heaps, results, and child contexts.

## Alternatives and decision

- Post-Top-K membership is rejected because nearer nonmembers can occupy the
  bounded heap and under-fill the exact result.
- Disabling storage Top-K is rejected because it materializes and scores the
  selected 768-dimensional entry vectors in CN, recreating #27854's critical
  performance failure.
- The selected path uses the existing exact integer `docfilter` as a storage
  PK filter before vector Top-K, with one admitted filter and one centroid route
  per coordinator-CN search generation.
- Cross-CN domain broadcast is deferred. It adds complete-domain replication,
  per-CN reconstruction, scan fan-out, global merge, and concurrent CPU/memory
  amplification that the available storage microbenchmark does not measure.

## Compatibility and rollback

The required-domain plan field is append-only, but a required-domain scan is
always coordinator-local and is never serialized to another CN. Therefore this
change claims no new MORPC capability version. No catalog or hidden-table format
changes are made. The existing `forceOneCN=1` hint remains a diagnostic control;
the required-domain invariant itself fails closed to the same placement.

## Validation and acceptance

Deterministic tests cover required-domain UNIQUE/PASS/malformed terminals,
coordinator placement, local DOP, disjoint local shard ordinals, child-context
cancellation, exact filter-before-Top-K ordering, and the scalar/correlated
reader-count contracts. A multi-service public test proves that default planning
still returns the exact non-empty and empty PRE results without distributing the
required domain. The full planner, compiler, APPLY, and IVFFlat package suites
remain the local merge gate.

A persisted 8,192-by-768 benchmark must show that exact membership is applied
before storage Top-K without materializing nonmember embeddings. Enabling a
future distributed route requires separate Wiki-10M evidence for one-CN and
three-CN QPS, latency, CPU, peak memory, scanned bytes, and recall. Three-CN
throughput must be at least 95% of the former TVF, no lower than current main,
and at least 25% above the one-CN candidate; recall may not drop by more than
0.001 and CPU, memory, and scanned embedding bytes must remain within 110% of
the former TVF. Those are enablement gates for that future route, not evidence
claimed by this coordinator-local change.

Local persisted-object evidence on an AMD Ryzen 9 7950X3D, Go 1.26.4,
8,192 rows by 768 dimensions, ten iterations and three repetitions:

- local exact-membership Top-K: 0.333-0.338 ms/op and 3 MiB materialized
  embeddings/op;
- storage exact-membership Top-K: 0.199-0.203 ms/op and zero materialized
  embeddings/op.

The storage path is 1.64-1.70x faster at this boundary. This result does not
justify cross-CN rollout; that route remains absent until the representative
deployment gate above is satisfied.

## Scan-work costing revision

Approved in the design-first conversation for #28002 before implementation:
use hidden entries-table statistics for physical scan work, keep output width
separate, and require controlled Wiki A/B before default enablement. This is an
advisory costing fix, not a new distributed protocol or memory owner.

The typed rewrite currently scales source-table blocks by nprobe/lists and
uses the 16-byte `(pk, score)` output width to select reader DOP. A data-free
planner diagnostic with 10M rows, 3162 lists and five probes selects DOP 1 for
1221, 2442 and 8192 source blocks. The former TVF separately planned its wide
entries scan; restoring reader capability alone does not restore that costing.

Resolve the entries relation using the source identity and snapshot. Its row
and block counts, scaled by `min(1, max(1,nprobe)/lists)`, form the proportional
initial-scan estimate. Scalar selectivity, distance thresholds and LIMIT do not
discount this work. Vector bytes per entry come from the entry column's size
statistics, or its stored element type and dimension. Positive object counts
cap reader DOP; missing or invalid estimates choose one reader. Statistics
errors must not create new query failures, except that cancellation propagates.

Optional `VectorIndexScan.scan_work` records estimated rows, blocks, vector
bytes per entry and object count. Output row width and result cardinality keep
their original meanings. The generic DOP pass uses the existing wide-scan
policy on this separate estimate and respects operator, query and worker caps.
Only synchronous, uncorrelated, single-round scans get an estimate; async,
adaptive and correlated paths remain single-reader. Copies and serialization
preserve the advisory fields; old plans without them remain safe. Ignoring
these fields only affects scheduling, so no MORPC capability is introduced.

The estimate is not a physical pruning guarantee: skew, object fragmentation,
historical index versions and the small-domain full-centroid fallback can make
actual reads differ. EXPLAIN must label these numbers as estimates and show DOP.
Changing output Rowsize to the vector width, forcing every query to all CPUs,
and reintroducing nested SQL are rejected alternatives.

Validation covers hidden/source-stat divergence, stored vector widths,
missing/invalid estimates, snapshots/tenant identity, copy/serialization,
single-reader exceptions, DOP caps and rewrite-to-reader execution. Public PRE
results and lifecycle coverage remain mandatory. No new resource lifecycle,
catalog format or index representation is introduced.

This revision's default-enablement gate is separate from the future cross-CN
gate: pinned main/candidate, identical index contents and queries, fixed warm-up,
three ABBA blocks at concurrency 1 and 100, and per-run latency/QPS/recall,
CPU, peak memory and scanned bytes. Require lower median single-client latency,
concurrency-100 median QPS at least main, p95/p99 and CPU/memory within 110% of
main, and recall loss at most 0.001. Missing or inconclusive evidence does not
pass. The existing two-round measurements predate this revision and do not
satisfy it. No large dataset download or remote dispatch is automatic.
