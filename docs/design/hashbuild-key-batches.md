# Bounded HashBuild key collection

Related issue: [#30125](https://github.com/matrixorigin/matrixone/issues/30125).
Implementation: [main #32191](https://github.com/matrixorigin/matrixone/pull/32191)
and [4.2-dev #32256](https://github.com/matrixorigin/matrixone/pull/32256).

Collected join keys are an aligned sequence of batches of at most 8,192 rows,
not one vector spanning the whole build. This change does not alter JoinMap row
addressing, retained build batches, planner thresholds or MPool allocation limits.

All collected columns in a batch have the same row count. The ordinary path
collects newly inserted hash groups; the HashOnPK path collects all rows under the
planner's existing primary-key uniqueness assumption. Neither adds another
deduplication mechanism. Collection appends are transactional across columns;
failure restores the previous tail and destroys unpublished batches. Variable
payload and encoded size are checked before growth; a byte boundary may produce
a shorter batch. One indivisible oversized key remains a size refusal.

Rollback failures join the original append error after attempting every column's
restoration. An invalid checkpoint discards the entire collected-key owner and
remains terminal; it cannot fall back to PASS or retry a partially restored set.

Ordinary IN keeps its existing wire/probe contract and its single-batch fast path.
Numeric values are copied and sorted directly in the final admitted encoding;
other sorted runs are merged into that encoding without constructing a
whole-build vector. Composite expressions execute per aligned
batch. Membership consumers receive one complete immutable chunked payload and
build one membership structure; they never probe each chunk independently.

Raising the allocator ceiling only moves the contiguous-allocation failure.
Discarding oversized optional filters is valid, but cannot replace required
membership before Top-K. Bounded collection therefore updates both the producer
and every membership consumer while retaining ordinary IN's existing encoding.

The payload owns its encoded buffers through a reference-counted lease.
Publication transfers one reference; surviving reader generations retain their
own references. Failure, cancellation, Reset and final drain release their owners. An
optional size/admission refusal can disable the optional filter, never a required
prefilter before Top-K. Empty, missing, PASS and DROP remain distinct.

Validation covers row/byte boundaries, column alignment, late allocation errors,
publication and parallel-reader lifetime, signed zero/NULLs, join result parity,
and required IVF filtering before Top-K. Inject small byte limits rather than
allocating gigabytes. Compare no-filter, single-batch and multi-batch CPU costs,
allocations, peak memory and end-to-end joins against the parent. No repeatable
OLAP slowdown is accepted without an explicit decision. CUDA validation is outside
the requested delivery scope; shared CPU/GPU source contracts remain consistent.

## 4.2-dev backport

The release retains its TVF search architecture. IVF and fulltext consume all
chunks. IVF uses an exact SQL IN predicate for small sets and for required
non-integer sets; larger integer sets use an exact tagged reader filter, while
optional non-integer filtering can use an approximate Bloom filter. Fulltext
uses the existing tagged reader filter and is currently planned as optional.
No index-plugin reader refactor or native-filter ABI change is included.
Required non-integer IVF keys retain the release's exact SQL predicate and its
8 MiB global limit on literals and separators across all chunks. A
complete predicate that exceeds this existing limit returns an error, never an
approximate or partially applied prefilter. Payload references use a local
atomic reference count because the release has no generic buffer-lease package.

## Decision and release integration

The failure addressed here is a single collected-key vector reaching the
per-allocation limit even when the HashBuild can otherwise continue. The goal is
to bound each key allocation and publish a complete filter, without changing the
hash table's uniqueness, JoinMap contents, or result semantics. This does not
bound retained build rows, all query memory, or total required membership bytes;
those remain subject to the existing query/CN admission and spill policies.

The alternatives are materially different:

* Keeping one vector preserves the simplest representation, but leaves the
  contiguous-allocation failure and full-vector growth costs.
* Raising the allocation limit merely moves that failure and increases a single
  allocation's peak and blast radius. It does not establish an ownership bound.
* Dropping every oversized filter is valid for an optional ordinary IN filter,
  but loses pruning and cannot satisfy required membership before Top-K.
* Aligned bounded chunks preserve complete membership and avoid a whole-build
  vector. They require accounted directories, chunk-aware consumers, and a
  bounded merge for the existing ordinary IN encoding; those costs are accepted.

The 4.2-dev integration must also retain the composite-primary-key runtime
filter from [#32400](https://github.com/matrixorigin/matrixone/pull/32400).
The planner maps JOIN condition slots into probe composite-primary-key order.
Each aligned chunk evaluates those ordered components and computes serialization
bounds and values from the converted vectors. Supported components are direct
columns and same-signed 8/16/32/64-bit integer casts to an equal or narrower
width, as checked by the existing tuple contract. `serial` skips NULL rows;
`serial_full` encodes NULL markers. Unsupported layouts, including float tuples
without signed-zero equivalence, are not assumed safe. All resulting runs
contribute to one complete IN payload. If optional narrowing overflows, discard
all completed runs and publish whole-filter PASS, never a partial IN. A required
membership input must never be silently dropped or approximated. Unsupported or
malformed layouts fail open only where the existing contract permits it, while
cancellation and lifecycle failures remain errors.

## Ownership, failure and compatibility

HashBuild owns each aligned chunk and its allocation accounts until reset or
free. A multi-column append either commits all components or restores its old
tail and releases unpublished chunks. Directory and payload metadata are
admitted before allocation. A rollback invariant failure remains terminal;
cancellation and lifecycle/accounting errors are not converted to PASS.

An IN result is sent only after every run has been validated and merged; a
failed run releases all prior temporary runs. For membership, publication
transfers a reference to the message board. Receivers obtain message copies and
must successfully retain the payload before using it; if drain wins the race,
retain fails and the payload must not be used. Each successful retain is paired
with one release. Copied messages share a once-guard for destruction of the
board reference. Sending to a closed board destroys the rejected message.
Close can seal the board without draining it. Reset of a multi-CN board returns
a new board, while the old board's queued references still require final drain
after its owners have exited. Failure, cancellation and normal destruction
release their respective owners. Cancellation is checked between chunks and in
some row loops, but the numeric full-output sort and an individual native Bloom
AddVector are not interruptible inside those calls; chunking alone does not
bound their cancellation latency.

For required membership, missing, invalid, or PASS is an error and cancellation
returns its cause. A producer can send PASS to wake waiters after failure, but a
required receiver must not treat it as successful unfiltered search. DROP denotes
a truly empty build. In the existing IVF planner
topology the membership side is also the build of an INNER JOIN, so a true DROP
produces an empty final result even if the TVF receives no payload; whether the
TVF runs is execution-topology dependent. This argument does not excuse a failed
or incomplete collection being incorrectly published as DROP.

`RuntimeFilterMessage` broadcasts on the current CN and does not implement
cross-CN serialization. Its membership payload is therefore a process-local
producer/consumer contract; ordinary IN keeps its existing encoded data format.
The resulting tagged reader filter can still travel to a remote CN in the
background SQL data source. Existing IVF/fulltext Sorted64 protocol-version 10
gates and the serialized-tuple protocol-version 8 gate remain necessary. With
the gates down, the serialized tuple filter is not constructed; IVF retains
the relational plan, and fulltext disables the affected pushdown. No
catalog, storage, native-filter ABI, or planner threshold change is intended.
These gates do not by themselves prove arbitrary mixed-version rollout safety.

## Cost and acceptance

Each collected chunk holds at most 8,192 rows and may be split earlier by its
component-vector byte limits. A composite tuple's encoded chunk can still exceed
the encoding limit and be refused rather than split again. For N retained keys
and C chunks, peak ordinary-IN production can
include the original keys, all serialized runs, the final contiguous encoded
output, O(C) merge metadata, and one chunk's conversion scratch at once;
concurrent consumers can additionally hold their own membership structures. The
final output is still constrained by the int32 row and MPool single-allocation
limits: oversize optional IN becomes PASS, while required membership must be
complete or fail. HashBuild accounts and consumer CN throttler reservations
have separate owners and admission paths. No aggregate required-set bound is
introduced. Ordinary IN retains the single-chunk fast path. Multi-chunk
conversion adds per-chunk executor setup and per-row component work; numeric
output sorting is O(N log N), while other paths sort runs and merge in
O(N log C).

Acceptance covers empty, single- and multi-chunk collections; component order,
NULLs and alignment; reordered components with direct and narrowing casts;
signed and unsigned boundaries; a narrowing cast in a later chunk; a later-chunk out-of-range
value that yields whole-filter PASS; and bounded admission, rollback, cancellation
and release without partial publication. Exercise required PASS, true DROP,
partial initialization, cancellation, repeated Reset and concurrent reader/board
drain. Required IVF membership must consume
all chunks before Top-K, or fail without publishing a partial filter.
Use deterministic small byte bounds for chunk/failure injection and retain the
real 8,193-row boundary case. Compare no-filter, single-chunk and multi-chunk
costs with the release baseline when the affected hot path changes; report data,
allocations and noise rather than treating a microbenchmark as an end-to-end
guarantee. CUDA execution, multi-gigabyte workloads and mixed-version rollout
remain explicitly unverified unless separately tested.
