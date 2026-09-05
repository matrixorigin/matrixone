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

The per-reader distance heap is created by the reader and discarded with that
reader; it is not shared across scans. Its row/distance companion is a second
max heap bounded to `min(B, K)`: an accepted candidate replaces the companion's
current worst entry rather than being appended to a historical result slice.
After the shared distance cutoff is final, entries above that cutoff are removed
and the remaining rows are restored to physical input order. Thus both distance
and row/distance winner state are O(K), even for a strictly descending sequence
where every row improves the cutoff. The selection is bounded by the block's
physical row count B. At most one fused request pins `P + 1 + D` columns for a
block, where P is predicate columns and D is deferred projected columns. For a
1024-d `float64` vector the vector payload is `8192 * B` bytes before
ObjectIO/cache representation overhead; predicate and deferred payloads add
their encoded column sizes. Concurrent scans multiply the pinned-request bound
by their concurrent block reads, so the existing ObjectIO cache/admission policy
remains the capacity control. Fusion trades one request's wider pin set for
removal of the second deferred read and avoids materializing non-winners into
caller-owned vectors.

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

Focused ObjectIO tests cover fused-input validation, release/error behavior, and
an adversarial descending-distance sequence that proves the row/distance
companion never exceeds K capacity; `pkg/vm/engine/readutil` covers merge-reader
selection; `pkg/vectorindex/ivfflat` covers supported/unsafe range, DESC, and
membership controls. Existing
`test/distributed/cases/vector/vector_ivfflat_include_*` cases cover the public
INCLUDE SQL path.  These are functional acceptance tests; they are not a
substitute for the scale gate below.

The performance acceptance protocol is one warm-up followed by three measured
rounds on the exact pushed PR head on the CA 10M lane: 10,000,000 rows,
`VECF64(1024)`, IVF `lists=256`, `probe_limit=10`, each of selectivity 1/10/50
percent and K=1/10/100.  It must record the warm-round p50, returned rows, and
recall against a separate exact `mode=force` oracle for INCLUDE, PRE, and POST.
It must separately record the first cold query for every cell.  INCLUDE must
retain its exact residual-predicate result and its warm-round p50 must not
exceed 10 times the fastest corresponding PRE/POST warm-round p50.  The earlier
main-only run is not evidence for this PR head.  The available exact-head run is
[33941908938/job/101251498026](https://github.com/matrixorigin/mo-auto-test/actions/runs/33941908938/job/101251498026),
which verified MatrixOne `a51969dd979974c394340006d32ad6b8c0e20745`; its
[attempt-2 artifact](https://github.com/matrixorigin/mo-auto-test/actions/runs/33941908938/artifacts/9964172403)
contains the raw plans, JSONL, and summary.  It did **not** execute a separate
warm-up: its rounds 1--3 are the only executions.  Consequently it is not an
execution of this acceptance protocol and does not establish a cold-start bound
or a protocol-acceptance result.  The entries below are retained as exact-head
diagnostic evidence only: `all-three-round p50 ms / returned rows / recall`.
The displayed p50 happens to select a warm sample in the affected cells, so it
must not be read as hiding or bounding round 1.  Comparisons use the fastest
PRE/POST p50 for the same cell.

| Selectivity | K | INCLUDE p50 / rows / recall | PRE p50 / rows / recall | POST p50 / rows / recall | Artifact p50 ratio |
| --- | ---: | --- | --- | --- | --- |
| 1% | 1 | 67.9 / 1 / 0.00 | 324.5 / 1 / 0.00 | 313.2 / 0 / 0.00 | 0.22x |
| 1% | 10 | 74.1 / 10 / 0.10 | 445.8 / 10 / 0.00 | 326.4 / 0 / 0.00 | 0.23x |
| 1% | 100 | 126.9 / 100 / 0.05 | 350.4 / 100 / 0.04 | 329.7 / 0 / 0.00 | 0.38x |
| 10% | 1 | 132.3 / 1 / 0.00 | 876.8 / 1 / 0.00 | 324.9 / 1 / 0.00 | 0.41x |
| 10% | 10 | 116.6 / 10 / 0.10 | 940.2 / 10 / 0.10 | 321.3 / 4 / 0.10 | 0.36x |
| 10% | 100 | 138.4 / 100 / 0.09 | 906.6 / 100 / 0.08 | 320.8 / 11 / 0.08 | 0.43x |
| 50% | 1 | 264.1 / 1 / 0.00 | 619.7 / 1 / 0.00 | 305.7 / 1 / 0.00 | 0.86x |
| 50% | 10 | 266.0 / 10 / 0.10 | 585.5 / 10 / 0.00 | 323.5 / 10 / 0.00 | 0.82x |
| 50% | 100 | 266.5 / 100 / 0.14 | 542.4 / 71 / 0.12 | 329.6 / 71 / 0.12 | 0.81x |

The artifact records a material first-query cold path.  Its INCLUDE round-1
samples were:

| Selectivity | K=1 | K=10 | K=100 |
| --- | ---: | ---: | ---: |
| 1% | 247951.0 ms | 157200.7 ms | 117466.1 ms |
| 10% | 70130.7 ms | 40417.0 ms | 14200.2 ms |
| 50% | 2106.1 ms | 318.6 ms | 259.4 ms |

For the 1% cells, the corresponding round-2/round-3 samples were 67.9/67.4 ms
(K=1), 74.1/67.7 ms (K=10), and 79.2/126.9 ms (K=100).  The p95 values in the
raw summary preserve the same cold samples.  This RFC makes no claim that a
first query is acceptably bounded: the changed reader contract covers
filter-before-heap, bounded winner retention, and fused ObjectIO reads; it does
not introduce cache priming, cache admission, or a cold-query latency guarantee.
The artifact alone cannot attribute the cold cost to a particular component.
The conservative operational disposition is therefore to keep cold-start
performance unaccepted until the stated warm-up-plus-three-round protocol is
run and its cold samples are reported separately; no functional correctness or
steady-state performance claim above is a substitute for that evidence.

The job's terminal `result_cardinality` failure is not an INCLUDE result or
performance failure: it unconditionally required the approximate PRE/POST
baselines to return K rows.  Seven baseline cells were deterministically short
(for example POST 1% returned 0 for K=1/10/100), while every INCLUDE cell
returned K and all nine diagnostic p50 comparisons were within 10x.  This validator-contract
failure is tracked in [#27891](https://github.com/matrixorigin/matrixone/issues/27891);
the issue also records the independent main-lane observation that approximate
IVF must not be compared as an exact `mode=force` result.

## Drawbacks and unresolved questions

Fusion can increase the peak pin footprint of one request, particularly for
wide deferred projections and concurrent scans; the capacity model above is
the admission constraint.  The available CA artifact is diagnostic evidence,
not the required warm/cold operational acceptance evidence.  This RFC remains
drafted until an independent design review advances it under the repository RFC
process; that approval is independent from the exact-head diagnostic evidence.
