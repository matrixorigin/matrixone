# Narrow selected-copy specialization

Owning issue: #30845. This is one independently measured execution candidate.

## Contract and owner

Preserve physical values, selection order and duplicates, destination prefixes,
NULL/grouping/provenance and allocation/error ownership. Change only flat,
non-NULL four/eight-byte payload stores in `unionT`. The existing owner keeps
preflight, capacity admission, length publication and metadata authoritative.
There is no SQL, public API, format, configuration or retained-lifetime change.

## Implementation

Keep the direct scalar store for one selected value. For multiple selections,
construct one byte window covering the complete destination append range, then
advance row pointers only inside that window. Copy each source value with the
existing checked source-byte indexing and uint32/uint64 stores. These stores
preserve every logical type's bits, including NaNs and signed zero.

For width W, the checked destination window is
`v.data[oldLen*W:(oldLen+len(sels))*W]`. Its construction validates both ends,
including a negative or invalid start. For every loop index
`0 <= i < len(sels)`, the W-byte store at offset `i*W` stays within that admitted
window. The pointer and window remain local to the existing call. No allocation,
borrowed reference, eligibility scan or per-row destination check is introduced.
Source-index checks remain as in the original scalar loop.

The zero-selection early return and wide, constant, NULL and varlen paths keep
their implementations. Growth happens before the window is formed, so valid
self-appends read the original prefix from the current backing allocation.

## Cost correction

The first typed-gather prototype added helper/two-view setup for a single scalar
store. Keeping the one-row store removed that measured regression. Real joins
also make many short multi-row calls, where per-call setup can offset payload
loop savings. Exact-head measurements retained a slower Q9 signal; same-binary
controls showed substantial process variation, so the entire query shift is not
assumed to be caused by this mechanism.

This revision removes the helper and typed-view admission cost for all multi-row
calls. It hoists a redundant destination check into one span admission and keeps
the scalar loads/stores. There is no empirically tuned batch-size crossover,
new abstraction or build-alignment change.

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
