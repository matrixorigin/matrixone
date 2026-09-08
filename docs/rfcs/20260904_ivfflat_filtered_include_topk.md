- Status: in-progress
- Start Date: 2026-09-04
- Authors: iamlinjunhong
- Implementation PR: https://github.com/matrixorigin/matrixone/pull/28139
- Issue for this RFC: https://github.com/matrixorigin/matrixone/issues/27891
- Design-review decision: pending independent approval

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

The performance acceptance protocol is one recorded cold/warm-up query followed
by three measured rounds on the exact pushed PR head on the CA 10M lane:
10,000,000 rows, `VECF64(1024)`, IVF `lists=256`, `probe_limit=10`, each of
selectivity 1/10/50 percent and K=1/10/100.  It records the measured p50,
returned rows, and recall against a separate exact `mode=force` oracle for
INCLUDE, PRE, and POST, and records the first cold query separately for every
cell.  INCLUDE must retain its exact residual-predicate result and its measured
p50 must not exceed 10 times the fastest corresponding PRE/POST measured p50.

This protocol was executed successfully against MatrixOne
`c492e554eb201fa00deaf4205956a3728278d3a2` in
[run 34172928231, job 101896622990](https://github.com/matrixorigin/mo-auto-test/actions/runs/34172928231/job/101896622990),
using the trusted lane revision `c7a9608a43711b0db3407dd36fb44836aa67a81b`.
The [artifact](https://github.com/matrixorigin/mo-auto-test/actions/artifacts/10037522712)
contains 108 query events: 27 cells with one `warmup`/`cold_start` event and
three `measured` events each.  `cold_start_recorded_separately` is true and the
validator reported no gate failures.  The table below reports the p50 of the
three measured rounds as `latency ms / rows / recall`; the ratio is INCLUDE
against the faster PRE/POST result for the same cell.

| Selectivity | K | INCLUDE p50 / rows / recall | PRE p50 / rows / recall | POST p50 / rows / recall | Artifact p50 ratio |
| --- | ---: | --- | --- | --- | --- |
| 1% | 1 | 71.1 / 1 / 0.00 | 522.6 / 1 / 0.00 | 311.0 / 0 / 0.00 | 0.23x |
| 1% | 10 | 77.3 / 10 / 0.10 | 485.6 / 10 / 0.00 | 336.6 / 0 / 0.00 | 0.23x |
| 1% | 100 | 98.1 / 100 / 0.08 | 552.0 / 100 / 0.06 | 340.7 / 0 / 0.00 | 0.29x |
| 10% | 1 | 139.6 / 1 / 0.00 | 1254.8 / 1 / 0.00 | 349.9 / 0 / 0.00 | 0.40x |
| 10% | 10 | 144.2 / 10 / 0.00 | 1208.6 / 10 / 0.00 | 323.3 / 2 / 0.00 | 0.45x |
| 10% | 100 | 157.0 / 100 / 0.08 | 1211.9 / 100 / 0.06 | 343.4 / 11 / 0.06 | 0.46x |
| 50% | 1 | 308.6 / 1 / 1.00 | 588.2 / 1 / 0.00 | 334.5 / 1 / 0.00 | 0.92x |
| 50% | 10 | 280.3 / 10 / 0.10 | 551.4 / 10 / 0.20 | 348.3 / 10 / 0.20 | 0.80x |
| 50% | 100 | 294.6 / 100 / 0.05 | 510.8 / 72 / 0.09 | 340.3 / 72 / 0.09 | 0.87x |

The same artifact records the first cold query separately for all 27 cells.  The
table below reports those rows as `latency ms / rows / recall`; these samples are
not included in the measured p50 or the 10x warm-performance gate.

| Selectivity | K | INCLUDE cold / rows / recall | PRE cold / rows / recall | POST cold / rows / recall |
| --- | ---: | --- | --- | --- |
| 1% | 1 | 256889.9 / 1 / 0.00 | 97465.7 / 1 / 0.00 | 349.7 / 0 / 0.00 |
| 1% | 10 | 186663.1 / 10 / 0.10 | 99886.1 / 10 / 0.00 | 352.2 / 0 / 0.00 |
| 1% | 100 | 127961.0 / 100 / 0.08 | 87851.1 / 100 / 0.06 | 344.3 / 0 / 0.00 |
| 10% | 1 | 86942.1 / 1 / 0.00 | 84313.2 / 1 / 0.00 | 307.9 / 0 / 0.00 |
| 10% | 10 | 21350.7 / 10 / 0.00 | 70601.7 / 10 / 0.00 | 312.7 / 2 / 0.00 |
| 10% | 100 | 9536.3 / 100 / 0.08 | 10855.0 / 100 / 0.06 | 350.8 / 11 / 0.06 |
| 50% | 1 | 1578.9 / 1 / 1.00 | 3707.1 / 1 / 0.00 | 318.1 / 1 / 0.00 |
| 50% | 10 | 281.5 / 10 / 0.10 | 543.9 / 10 / 0.20 | 315.6 / 10 / 0.20 |
| 50% | 100 | 278.2 / 100 / 0.05 | 516.7 / 72 / 0.09 | 704.7 / 72 / 0.09 |

The cold path is materially longer for selective INCLUDE and PRE cells (for
example, INCLUDE 1% is 256.9 s / 186.7 s / 128.0 s for K=1/10/100).  The
measured acceptance gate deliberately does not claim a cold-start latency bound:
the changed reader contract covers filter-before-heap, bounded winner retention,
and fused ObjectIO reads, but does not introduce cache priming, cache admission,
or a cold-query SLA.  The cold samples are disclosed for operations and future
capacity work; they are not hidden by the measured p50.  Approximate PRE/POST
short rows are valid observations under `probe_limit=10`, while every INCLUDE
measured cell returned K rows and every performance ratio passed the `<=10x`
gate.  The previous run 33941908938 remains diagnostic-only historical evidence;
it is not used as the acceptance result for this head.

## Drawbacks and unresolved questions

Fusion can increase the peak pin footprint of one request, particularly for
wide deferred projections and concurrent scans; the capacity model above is the
admission constraint.  The exact-head warm/cold acceptance evidence is now
recorded, while the design-review decision remains pending independent approval.
The RFC is `in-progress` as required by the repository process after review; it
does not self-certify independent approval.
