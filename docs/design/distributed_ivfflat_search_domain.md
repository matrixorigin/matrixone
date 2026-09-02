# Distributed IVFFlat search domains

- Status: implemented
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
5. Membership and safe distance ranges are applied before local Top-K heap
   admission.  Each reader emits at most the candidate budget; the coordinator
   owns the global merge and final SQL recheck.
6. Cancellation and every producer, broadcast, reader, and partial-open error
   release domain payloads, cache leases, engine readers, heaps, and batches
   effectively once.
7. Domain memory remains under the existing broadcast HashBuild/query-memory
   admission.  No second transport copy or independent queue is introduced.

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

Within a CN, IVFFlat expands the CN partition into scheduled, disjoint local
reader shards.  They share the process-local centroid cache generation while
owning separate scanner, heap, result, and child-process state.

## Compatibility and rollback

The required-domain plan field is append-only and gated by the next available
cumulative MORPC version.  A cluster below that version never plans distributed
required domains.  No catalog or hidden-table format changes.  The existing
`forceOneCN=1` optimizer hint and intrinsic ForceOneCN fallbacks restore
coordinator-only execution.

## Validation and acceptance

Deterministic tests cover the protocol and planner fallbacks, required-domain
UNIQUE/PASS/malformed terminals, multi-CN DOP selection, disjoint combined shard
ordinals, child-context cancellation, exact filter-before-Top-K ordering, and
the scalar/correlated reader-count contracts.  The full planner, compiler,
APPLY, and IVFFlat package suites remain the local merge gate.

Distributed BVT must return the same PRE results at one-CN and multi-CN width.
Performance evidence must show that additional DOP reduces the entries-scan
critical path without increasing recall loss; the filter-first path keeps
memory bounded to one shared serialized domain plus per-reader lookup state,
one batch, and the candidate budget.
