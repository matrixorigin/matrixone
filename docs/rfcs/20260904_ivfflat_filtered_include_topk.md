- Status: drafted
- Start Date: 2026-09-04
- Authors: iamlinjunhong
- Implementation PR: https://github.com/matrixorigin/matrixone/pull/28139
- Issue for this RFC: https://github.com/matrixorigin/matrixone/issues/27891

# Filtered IVFFLAT INCLUDE Top-K reads

## Summary

This RFC defines the filtered IVFFLAT INCLUDE Top-K read used by #28139.  It
pushes the exact residual predicate ahead of the vector heap for the supported
persisted-reader case and obtains predicate, vector, and deferred result
columns in one pinned ObjectIO request.  It does not change IVF's approximate
candidate semantics, SQL filter semantics, ordering semantics, or the fallback
for a reader that cannot provide the optional capability.

## Problem and invariant

The original INCLUDE path could spend most of its time in the vector-index scan
on CA 10M because it scored candidates before applying an exact INCLUDE residual
predicate and made a second object read for result columns.  The required
invariant is:

> For every supported persisted reader, the exact residual predicate is applied
> to physical rows before those rows enter the Top-K heap; rows copied into the
> output are exactly the heap winners in the existing SQL order.

The residual predicate remains exact.  IVF candidate selection remains
approximate and is deliberately not promoted to an exact-recall claim.  Empty
predicate survivors produce an empty result, not the Top-N convention of a nil
selection meaning all rows.

## Scope and fallback matrix

| Reader/query condition | Action | Result contract |
| --- | --- | --- |
| Selected persisted reader implements `engine.FilteredTopKReader`; ascending supported vector order; safe range | Fused read: copy predicate inputs, filter, Top-K vector entries, copy winner deferred columns | Same exact residual predicate and ordering as the existing path |
| Persisted reader without the optional contract | Existing generic two-read path | No behavior change |
| Appendable or in-memory reader | Existing reader path | No behavior change |
| Unsupported reader or unsafe range/DESC order | Existing non-storage Top-K/range handling | No unsafe pushdown |
| Runtime membership filter | Existing membership construction and entry filtering precede this capability | Membership errors/empty sets retain existing behavior |
| Invalid fused inputs, duplicate column roles, nil selector/pool/destination | Reject with an invalid-input error before ObjectIO work | No partial caller result is committed |

`FilteredTopKReader` is an optional Go interface, not a persisted, wire, SQL,
or catalog contract.  Existing readers need not implement it.  The planner and
reader selection preserve the generic fallback when it is absent.

## Data flow, ownership, and bounds

For each physical block, ObjectIO pins cache-backed vectors for the predicate
columns, one 1024-d vector order column, and deferred projected columns in one
request.  Predicate vectors are copied into the reader-owned vectors because
the SQL expression evaluator consumes them after the request is released.  The
Top-K search borrows the pinned vector entry only during the call.  Deferred
vectors are borrowed only until winner physical row numbers are known, then only
winner rows are copied to caller-owned destinations.  `ReleaseIOVector` is the
single release owner on every success and error path.

The per-reader heap is created by the reader and discarded with that reader; it
is not shared across scans.  Its retained winner state is O(K), while the
selection is bounded by the block's physical row count B.  At most one fused
request pins `P + 1 + D` columns for a block, where P is predicate columns and D
is deferred projected columns.  For a 1024-d `float64` vector the vector payload
is `8192 * B` bytes before ObjectIO/cache representation overhead; predicate and
deferred payloads add their encoded column sizes.  Concurrent scans multiply
the pinned-request bound by their concurrent block reads, so the existing
ObjectIO cache/admission policy remains the capacity control.  Fusion trades one
request's wider pin set for removal of the second deferred read and avoids
materializing non-winners into caller-owned vectors.

## Alternatives and operations

The status quo performs predicate/vector work and deferred projection in two
reads; it has a narrower individual pin but repeats I/O and copies.  A fused
read that filters after Top-K is rejected because it violates the invariant.
Copying every deferred column before heap selection is rejected because its
caller-owned allocation grows with all survivors rather than K.  Adding a new
mandatory reader interface is rejected because appendable and generic readers
have correct existing behavior.

There is no rollout toggle or compatibility migration: the interface is
optional, all unsupported paths retain their old implementation, and no
persistent or mixed-version format changes.  Existing query errors and ObjectIO
errors are propagated.  Plan shape and the existing ObjectIO/cache metrics are
the operational diagnosis points; a reader can fall back by not implementing
the optional interface.

## Verification and acceptance

Focused ObjectIO tests cover fused-input validation and release/error behavior;
`pkg/vm/engine/readutil` covers merge-reader selection; `pkg/vectorindex/ivfflat`
covers supported/unsafe range, DESC, and membership controls.  Existing
`test/distributed/cases/vector/vector_ivfflat_include_*` cases cover the public
INCLUDE SQL path.  These are functional acceptance tests; they are not a
substitute for the scale gate below.

The performance acceptance run must use the exact pushed PR head on the CA 10M
lane: 10,000,000 rows, `VECF64(1024)`, IVF `lists=256`, `probe_limit=10`, three
measured rounds after one warm-up, each of selectivity 1/10/50 percent and
K=1/10/100.  It must record p50, returned rows, and recall against a separate
exact `mode=force` oracle for INCLUDE, PRE, and POST.  INCLUDE must retain its
exact residual-predicate result and its p50 must not exceed 10 times the fastest
corresponding PRE/POST p50.  The table below is intentionally pending; the
earlier main-only run is not evidence for this PR head.

| Selectivity | K | INCLUDE p50 / recall | PRE p50 / recall | POST p50 / recall | Gate |
| --- | ---: | --- | --- | --- | --- |
| 1% | 1, 10, 100 | pending exact-head lane | pending | pending | <= 10x |
| 10% | 1, 10, 100 | pending exact-head lane | pending | pending | <= 10x |
| 50% | 1, 10, 100 | pending exact-head lane | pending | pending | <= 10x |

## Drawbacks and unresolved questions

Fusion can increase the peak pin footprint of one request, particularly for
wide deferred projections and concurrent scans; the capacity model above is
the admission constraint and the CA run is the required operational evidence.
No design question is left for implementation.  This RFC remains drafted until
an independent design review advances it under the repository RFC process; that
approval is independent from the exact-head CA acceptance evidence.
