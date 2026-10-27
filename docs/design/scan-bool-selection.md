# Direct boolean selection in table scans

Owner: #30850, under #30838. Base: current authoritative main; independent of #31034.

## Contract and evidence

Q4's retained main profile has 8.90% exclusive user samples in evalFilter and 4.98% in boolean GetValue. These motivate removing per-row interface calls, not a predicted speedup. Preserve the exact surviving row sequence, SQL UNKNOWN-as-rejection, predicate/error evaluation order, and late-materialization original-row mapping.

## Implementation

For each already-evaluated, type-checked non-constant boolean result, obtain its fixed bool slice once. Select rows with a direct loop; split nullable/non-nullable cases once per vector and only inspect null bits for true values. Keep the existing cached wrapper and behavior in the constant branch. Retain selection buffers, compaction points, memory accounting, original-row composition and cleanup. No predicate fusion, reorder, new state, or changed reader contract.

The source result remains owned by its expression executor. The slice is borrowed only within the current evalFilter call and never retained. Error, zero-result and deferred-column behavior remain at their existing owners. No new API, wire/catalog field, goroutine or allocation is introduced.

## Validation and acceptance

Use small owning-package tests for empty/tiny/boundary masks, true/false/NULL, constants, multiple filters and errors, eager/late reads and original selections. Reuse existing reader fixtures and cleanup. Cover representative batches in a benchmark without inspecting benchmark results. Run owning table_scan tests, directly affected reader tests, and configured static checks. SQL correctness is a separate small fixture.

Compare this candidate alone with matching clean main using normal build flags. Require attributed mask CPU/instruction reduction and improved unchanged Q4/Q6/Q12/Q19 latency, plus full SF10 Q1-Q22. Exclude cold runs; keep profiles separate; use the existing qualified 16 GiB CN cache/32 GiB whole-application campaign and zero measured I/O/spill/swap gates. No input/result verification in benchmark scripts. TPC-H is primary; disclose small scalar observations and reject material batch/allocation or query regressions. Do not publish without target improvement.
