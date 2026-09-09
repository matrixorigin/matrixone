# Selected-vector sharing in fused reads

- Status: implemented; approved in the conversation before implementation.
- Base: main `9f87b6249889107c3c108c133a4c3c8f3bc273ee`.
- Implementation branch: `perf/fused-vector-decode-sharing`, delivered in one PR.
- Related issue: [#24097](https://github.com/matrixorigin/matrixone/issues/24097), following #28413 and #28434; does not close the umbrella issue.

## Contract and scope

Fused INCLUDE reads load filter columns, a vector and projected columns in one
IOVector. The existing sharing guard accepts only single-entry requests, so
concurrent fused reads cannot reuse uncached decoded vectors. Share exactly one
selected vector entry at any position without changing the combined request,
column order, policy, exact filtering, ranking or result materialization.

The selected data is immutable and scoped to the complete IOVector. Only its
conversion/validation result is shared; I/O and later cache updates remain local
to each request. Do not include entry position, sibling columns, query vectors
or predicates in the existing physical-extent/codec sharing key.

The user chose one vector entry, not arbitrary multi-entry sharing. No buffer
recycling, new pool, decoded idle cache, chunk-native fused execution, format
change, additional parallelism or request splitting is included.

## Integration

Preserve ReadOneBlock and its existing single-column option. Add
ReadOneBlockWithScopedDecode with a selected requested-list position, validated
against the column/type inputs. The private builder marks only that requested
position when it resolves to a normal stored array column. Missing/synthetic
columns and custom factories remain unmarked. Mark before physical-entry
compaction so a preceding synthetic column cannot shift the target.

Thread a private selected-position argument through readColumnsData. Ordinary
callers pass -1; LoadColumnsDataIntoAndTopN passes len(columns), the vector's
position immediately after its filter columns. The public fused loader signature
is unchanged. Other array columns are not implicitly opted in.

After the initial memory-cache lookup, prepareSharedDecode scans for a sole
nonempty sharing codec. Zero or multiple marked entries use ordinary reads.
An already satisfied or otherwise ineligible selected entry is not wrapped.
Keep existing service, policy, custom-cache, size, buffer and stream exclusions.

Retain only the selected physical index, original converter and one lease;
there is no collection of per-entry state. Preserve the existing read guard
through all remaining sibling work and deferred cache updates. Finalize against
the current entry at that index because storage helpers can replace entry values.
Restore the converter and attach the ticket to the final result, or release the
ticket if no result remains. ObjectIO relocation copies the complete IOEntry.

## Ownership and failure closure

| Audit | Required behavior |
|---|---|
| Q1: ownership | Each successful request owns its normal decoded reference and one ticket in the complete IOVector. Existing success/error cleanup releases both. A valid shared result survives an unrelated sibling or cache-update failure in another request. |
| Q2: termination | The existing 200 ms follower bound, independent caller cancellation and Close behavior remain unchanged. The enclosing read guard spans sibling conversion and deferred updates; Close does not wait for Top-K consumers. |
| Q3: bounds | Constant new per-request state; retain existing initial byte budget, 64 generations and 128 participants per generation. No new idle retention or capacity waits. Unmarked sibling allocations remain under their existing ownership. |

The registry state machine, concrete validated-data representation and metrics
remain unchanged. A failure before the selected converter runs must still end
the read guard. A later failure must release an already acquired ticket through
normal caller cleanup, not poison a complete vector result used by another query.
Retry paths must not leave a converter wrapper or ticket attached to a stale
entry copy. All-memory-hit reads still return before sharing preparation.

## Alternatives and compatibility

Splitting the vector into a separate request would discard the fused read's
I/O benefit and is rejected. General multi-entry sharing adds wrapper collection
and ordering/lifecycle state without helping this single-vector consumer yet.
The selected-entry extension reuses the existing bounded sharing mechanism.

There are no SQL, wire, catalog, storage or configuration changes. Other
FileService implementations ignore the optional descriptor as before. Existing
single-column and chunk-native callers retain their behavior. Rollback removes
the fused opt-in without any data migration.

## Validation and acceptance

Use small combined-read fixtures with the target first, middle and last. Cover
partial memory hits in either direction, all-hit and unmarked controls, multiple
markers, invalid positions, synthetic relocation and other unmarked array columns.
Exercise direct storage reads, streamed full-object capture and per-entry fallback,
including converter restoration and success/error cleanup.

Use barriers for errors before/after conversion, cancellation of either role,
timeouts, Close, deferred-update failure and next-generation reuse. Reuse the
existing sharing state-machine tests. A real fused consumer must retain independent
filters, query vectors, thresholds and projected results while sharing identical
validated vector backing; selector/materialization failures must release it.

Run owning packages, focused adaptive race stress, coverage, relevant IVF INCLUDE
BVT and full SCA. For performance, compare five matched main/candidate samples
using the same combined request and legacy LZ4 data under pinned-cache pressure.
Require one vector conversion across admitted overlapping readers, lower CPU and
decoded allocation, and unchanged I/O request shape. Hot-cache, single-reader and
unmarked controls must remain within 5% median regression. No unmeasured workload
or Wiki-10M performance claim is made.
