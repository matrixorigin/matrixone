# Bounded HashBuild key collection

Related issue: [#30125](https://github.com/matrixorigin/matrixone/issues/30125).
Implementation: the bounded-key collection PR containing this document.

Collected join keys are an aligned sequence of batches of at most 8,192 rows,
not one vector spanning the whole build. This change does not alter JoinMap row
addressing, retained build batches, planner thresholds or MPool allocation limits.

All collected columns in a batch have the same row count. Hash insertion remains
the sole uniqueness authority. Collection appends are transactional across columns;
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

The payload owns its encoded buffers through the existing buffer-lease primitive.
Publication transfers one reference; surviving reader generations retain their
own references. Failure, cancellation, Reset and Close release all owners. An
optional size/admission refusal can disable the optional filter, never a required
prefilter before Top-K. Empty, missing, PASS and DROP remain distinct.

Validation covers row/byte boundaries, column alignment, late allocation errors,
publication and parallel-reader lifetime, signed zero/NULLs, join result parity,
and required IVF filtering before Top-K. Inject small byte limits rather than
allocating gigabytes. Compare no-filter, single-batch and multi-batch CPU costs,
allocations, peak memory and end-to-end joins against the parent. No repeatable
OLAP slowdown is accepted without an explicit decision. CUDA validation is outside
the requested delivery scope; shared CPU/GPU source contracts remain consistent.
