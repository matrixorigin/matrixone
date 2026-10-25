# Reuse typed fixed-width selection copying

Related: #30838, #30845. Baseline: `aeaa273daf588a9988ba423915d3ae6b8c82124a`.

## Problem and scope

`vector.Union` and `UnionInt32` select rows for joins, batch regrouping,
shuffle and hash-build spill. Their shared `unionT` copies non-NULL fixed
values with scalar assignments for widths 1/2/4/8, but uses a dynamic-length
`copy` for every wider value. The compiler cannot lower that loop to a
fixed-size assignment: every selected Decimal128/UUID value calls `memmove`.

On the baseline, the existing hash-join projection benchmark, extended to
Decimal128, spends 38.68% of CPU samples in `runtime.memmove` for 256 matches
from one source (4.99 seconds sampled). This diagnoses one materialization
cost; it is not evidence of a query speedup or whole-issue completion.

The scope is the data-only fixed-width selection kernel. No join planning,
match grouping, output-order, admission, metadata, encoding or storage
lifetime contract changes. The design gate applies because this changes a
shared hot path. The implementation follows design approval and measured
target/control comparison.

## Responsibility and alternatives

`pkg/vectorize/shuffle.FixedLengthShuffle` already owns the typed data-only
selection loop. Reuse it for `unionT`, admitting both int32 and int64 indices
through a type parameter. A small private vector adapter bounds the source
and appended destination byte ranges with Go slicing, then presents typed
views using the existing `util.UnsafeSlice` utility. Views have explicit row
counts; allocation padding is not exposed as additional source rows.
Retain the existing 1/2/4/8-byte scalar paths. Dispatch wider physical
widths in a separate private helper and reuse the typed loop for
12/16/20/24/32-byte values. Keep the current dynamic copy fallback for other
widths. This confines the added dispatch and typed-view setup to the values
that currently pay per-row dynamic copy costs.

Compared alternatives:

* Routing all widths through typed views improved bulk narrow copying, but
  independently repeated controls showed a 3–8% slowdown for single narrow
  values. Reject that expansion; it is not required to eliminate wide-value
  dynamic copies.

* Adding five more handwritten loops removes wide copies but duplicates the
  existing selection kernel and type-width loops.
* A multi-source gather API could batch scattered build projections, but adds
  admission and metadata integration before proving this simpler shared cost.
* Regrouping matches and restoring order adds copies and temporary memory.

The nullable fixed path retains its skip-NULL loop and dynamic value copy.
Bound its appended destination range once before that loop, avoiding repeated
prefix-offset calculations per selected value. NULL bitmap ownership and
untouched NULL payload bytes remain unchanged.

Typed physical copying is independent of query identity, data distribution,
and selection length. Existing NULL, constant and varlen paths remain the
owners of their current semantics. This is a scoped optimization, not a claim
to solve every scattered-gather cost.

## Invariants and costs

The destination receives exactly the selected bytes in order, including
duplicates; its existing prefix remains intact. The typed loop neither
allocates nor retains source references. Source and destination are used only
during the synchronous call. Vector growth/accounting, NULL/grouping bitmaps,
prepared parameter/string metadata, and reset/free remain in `vector`.
Allocation rejection occurs through the same preflight and growth calls.
No new state, cache, retry, configuration, persisted bytes or goroutines exist.
Cancellation/early termination retains the operator's existing cleanup path.

Cost remains O(selected rows), with the same output and copied byte count and
zero additional memory. The desired saving is dynamic per-row copy dispatch.
The risk is extra typed-adapter cost or code-layout effects on existing narrow,
small, nullable, constant and varlen paths; such regressions block delivery.

## Validation and acceptance

* Byte-exact independent expectations for every supported width, both index
  widths, empty/repeated/out-of-order selections, append prefix and reuse.
* Existing NULL/constant/grouping/metadata, borrowed validity, allocation
  failure and cleanup tests; full vector and directly affected consumer UTs.
* Real hash-build/probe operator tests cover result boundaries and reset.
* Frozen vector and hash-join projection benchmarks compare baseline/candidate
  serially with balanced repeated runs. Include widths 1/2/4/8 as controls,
  all wide layouts, 0/1/256 rows, NULL, varlen, contiguous/scattered matches.
  Profiles are separate from scoring. Confirm any suspected regression with
  an independent longer comparison; do not trade it for wins elsewhere.
* Incremental SCA and focused race checks; review final implementation and
  evidence with the requested gpt-6.1-sol/xhigh model.

No public SQL behavior changes, so existing consumer/result tests establish
the integration contract; kernel benchmarks do not establish query latency.
Full #30845 acceptance, including SF10/SF100 and Q1-Q22, remains pending unless
those campaigns are actually completed. Reverting this change has no data
compatibility implications.
