# Narrow selected-copy specialization

Owning issue: #30845. This is one independently measured execution candidate.

## Contract and owner

Preserve physical values, selection order and duplicates, destination prefixes,
NULL/grouping/provenance and allocation/error ownership. Change only flat,
non-NULL four/eight-byte payload stores in `unionT`. The existing owner keeps
preflight, capacity admission, length publication and metadata authoritative.
There is no SQL, public API, format, configuration or retained-lifetime change.

## Implementation

Copy the first selected value through the existing checked scalar indexes. This
is also the complete single-row operation. When more rows remain, check the
last destination byte once, then advance the first-row pointer for rows 1..N-1.
Keep the original checked source-byte indexing and uint32/uint64 stores. These
stores preserve every logical type's bits, including NaNs and signed zero.

For width W, the first checked index admits `oldLen*W`; the checked tail index
is `(oldLen+len(sels))*W-1`. Existing capacity admission guarantees complete
values before the first scalar store, as in the original implementation. Before
subsequent pointer-derived stores, both ends of the append interval are checked.
For every remaining index `1 <= i < len(sels)`, the W-byte store at offset `i*W`
stays in that interval. Copy order remains 0..N-1. The pointer stays local; no
allocation, borrowed reference, eligibility scan or slice-header setup is added.

The zero-selection early return and wide, NULL and varlen paths keep their
implementations. Fixed constants reuse one checked append window for both the
initial value copy and the existing doubling broadcast, avoiding duplicate
window admission and receiver-field reloads. Growth happens before a window is
formed, so valid self-appends read the original prefix from the current backing
allocation.

## Cost correction

The first typed-gather prototype added helper/two-view setup for a single scalar
store. Keeping the one-row store removed that measured regression. Real joins
also make many short multi-row calls, where per-call setup can offset payload
loop savings. Exact-head measurements retained a slower Q9 signal; same-binary
controls showed substantial process variation, so the entire query shift is not
assumed to be caused by this mechanism.

Q21's unscored shape count finds about 343,000 eligible calls averaging ten rows.
This revision removes helper/view admission and the extra byte-window header
setup from those short calls. It peels the existing first scalar store and checks
the remaining destination interval once. There is no empirically tuned batch-size
crossover, new abstraction or build-alignment change.

## Evidence and acceptance

Reuse the existing selection fixture for both selector widths, logical types,
bit patterns, windows, repeated/reversed rows, prefixes, growth, NULL/grouping,
provenance and source independence. Validate the new pointer arithmetic with a
focused checkptr run, run the final owning package once and retain its real
BatchSet consumer evidence. Inspect the common-owner frame before benchmarks.

Measure complete owners, including tiny, constant/NULL and wide controls. Use
unchanged local TPC-H Q1-Q22; exclude cold runs and keep profiles separate. Do
not verify benchmark inputs, schemas, row counts, results or checksums.

The user revised acceptance to "if no regression, can still commit": confirmed
component work reduction is sufficient without a confirmed TPC-H speedup.
Reject confirmed PR-caused slowdowns, including indirect ones. Investigate
observed slower signals with focused same-binary controls rather than repeat
unaffected suites. Report inconclusive query gains and control variation
explicitly; they are not proof of an end-to-end speedup. Publish source/binary
pins and resource limits with the PR template. The broader issue remains
separate from this slice.
