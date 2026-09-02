# Distributed IVFFlat search domains

- Status: in progress
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

1. PRE membership is a mandatory exact search domain, not an optional runtime
   optimization.  Empty membership produces no candidates; missing or malformed
   required membership fails the query.
2. The complete domain is built once from the globally complete filtered PK
   stream before any vector reader opens.
3. Every participating CN observes the same immutable domain for one statement
   execution generation.  A retry uses a distinct generation and cannot consume
   an older message.
4. Entry objects have one physical owner per search.  Each CN owns a disjoint
   object partition and its local readers own disjoint portions of that
   partition.
5. Membership and safe distance ranges are applied before local Top-K heap
   admission.  Each reader emits at most the candidate budget; the coordinator
   owns the global merge and final SQL recheck.
6. Cancellation and every producer, transport, reader, and partial-open error
   release domain payloads, cache leases, engine readers, heaps, and batches
   effectively once.
7. Domain transport is bounded.  Payloads larger than the existing 64 MiB
   pipeline boundary retain coordinator-only execution instead of being
   chunked or approximated.

## Execution protocol

The planner keeps the final SEMI join but marks its membership payload as a
required vector search domain.  The filtered PK build is globally merged on the
coordinator.  Remote vector scopes register their exact statement/execution
message-board generation and wait before reader construction.  After the build
finishes, the coordinator publishes one serialized exact domain per target CN.

When every selected CN supports the protocol, synchronous scalar/prepared
single-round searches may use all scheduled CNs and their reader DOP.  Older
clusters, async indexes without a global visibility watermark, correlated
APPLY, adaptive multi-round/include searches, explicitly disabled rollout, and
oversized domains keep the established coordinator-only path.

Within a CN, the IVFFlat plugin creates one search session, resolves the active
version and centroid routing once, builds the entries readers with scheduled
parallelism, and returns one bounded shard reader per engine reader.  The last
shard reader releases the shared session.

## Compatibility and rollback

The remote message command and required-domain plan fields are append-only and
gated by the next available cumulative MORPC version.  A cluster below that
version never plans distributed required domains.  No catalog or hidden-table
format changes.  The optimizer hint `vectorIndexLocal=1` restores the existing
coordinator-only path.

## Validation and acceptance

Deterministic tests cover exact/empty/coordinator-only domains, registration and
delivery failure, cancellation, retry generations, typed membership, disjoint
reader ownership, local/global Top-K equivalence, partial construction cleanup,
and all compatibility fallbacks.  Public PRE/POST threshold SQL must return the
same results in one-CN and multi-CN execution.

The merge gate includes a persisted 8,192-by-768 DOP benchmark and fresh Wiki
10M concurrency-100 evidence.  On equivalent fresh clusters the candidate PRE
throughput must reach at least 95% of the former TVF baseline, recall must not
decrease, peak RSS must stay within 110%, and threshold-PRE throughput must be
at least 90% of filter-PRE throughput in the same run.
