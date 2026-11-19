# Streaming unordered ROW_NUMBER

ODKU assigns a global source ordinal before its generated UNIQUE lookup joins,
then restores that order with a global SORT before PRE_INSERT_UK. Lookup joins
can reorder actions across batches. The final SORT remains mandatory; a single
CN, one scope, or an estimated ordering property does not replace that contract.

The ordinary WINDOW implementation materializes its input. Unpartitioned,
unordered ROW_NUMBER needs no future rows, so this materialization adds a full
payload copy without contributing to the result. The optimization uses the
existing WINDOW rather than introducing an operator or protocol field.

## Eligibility and placement

Only a sole ROW_NUMBER expression with no PARTITION BY or ORDER BY, no distinct
argument, zero arguments, and no PartitionTopN uses the streaming path. Prepare
still materializes and validates frame expressions before choosing the mode.
Other windows retain their existing execution and retained-copy admission.

compileWin still merges its inputs into one coordinator scope with one worker.
The per-instance uint64 counter therefore numbers one stream across batches.
Keeping vm.Window also retains existing join placement protection. No plan,
pipeline protobuf, capability check, fallback, or lock selection changes are
needed. Ordinary ROW_NUMBER and REPLACE consumers share this bounded execution
path; their public results remain unchanged.

## Ownership and lifecycle

The container holds one borrowed child batch separately from its owned ctr.bat.
Each Call emits at most DefaultBatchSize rows, exhausting that input before
calling the child again. Views borrow data/area; WindowWithAllocation admits
new NULL, grouping and provenance bitmap storage under WINDOW's account. The
ordinal vector uses the same account and fills its backing directly without a
second temporary value slice. An independent output header avoids modifying the
child's vector list. Analyzer/provenance metadata follows ordinary WINDOW output.

The preceding output is released before another output or child Call. Offset and
counter advance only after the vector and views have been admitted completely.
Yield and empty batches do not consume ordinals. A terminal child batch with data
is fully drained before returning ExecStop, without an additional child pull.
Overflow and cancellation fail the execution; partial output allocations are
freed immediately. Reset, Free and Prepare reuse clear borrowed references and
counter state without cleaning child-owned batches.

The previous retained-copy accounting repair remains necessary for windows that
need their full partitions. Its initial-copy/growth-rejection and generation
cleanup tests now use full-partition COUNT, rather than require ROW_NUMBER to
retain a copy it no longer needs.

## Validation and performance evidence

Tests cover uneven/oversized batches, yield/empty/final-data handling, overflow,
cancellation, child failure, ordinal/view admission rejection, reset/reuse,
ordinary/separate/shared accounts, constant and NULL/grouping views, source reuse,
and terminal resource cleanup. Sequential SQL checks retain the ODKU witnesses,
constraint-priority and physical-index oracles, cross-batch input and four INSERT
SELECT order/limit/empty controls, plus ordinary ROW_NUMBER and REPLACE controls.

BenchmarkRowNumberFullDrain consumes every output through EOF, checks total rows
and the final ordinal, and includes Reset/Free. Immutable source setup is outside
timing. It reports Go allocations, WINDOW account peak, owned input-copy backing
and mpool peak. Ordinary source-vector backing is reported separately because
it can live outside mpool accounting. First-output latency
alone is not equivalent work and is not used as the claimed speedup.

Removing WINDOW's own full-input payload copy does not bound the entire query.
Lookup joins and the required O(N log N) SORT retain their costs and existing
accounting/spill behavior. Bounded SQL wall times must be reported separately
from operator-only measurements, with control variability and correctness checks.
