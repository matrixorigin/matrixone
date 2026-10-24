# Caller-sized validation for grouped decimal SUM

Owner: #30847; umbrella #30838.

## Final scope

This PR adds tests and benchmarks only. Production SUM code is identical to main. No cache, executor state, arithmetic, update/scatter order, API, wire or cancellation behavior changes.

## Review finding and correction

The initial range-rejection optimization built a min/max and empty-slot index once a 255-entry local table filled. Its 8192-row single-call benchmark showed a gain, but group.buildOneBatch calls BatchFill with at most hashmap.UnitLimit (256) rows. In such a call the index can be built for at most the last row, cannot amortize its scans, and regresses reordered group IDs. The initially claimed 5.07x kernel gain does not describe normal grouped execution and is withdrawn.

The range index and all production probe changes are removed. An index-free contiguous-membership alternative also failed the caller-sized benchmark and is not retained. Future optimization of #30847 requires a design grounded in the real caller's work and state lifecycle.

## Validation contract

Benchmark public aggregate dispatch with 8192 rows consumed in hashmap.UnitLimit-sized calls, original vector offsets, and state allocated outside timing. Include cardinalities 4/254/255/256, sequential and permuted high-cardinality IDs, and a high endpoint. Keep explicitly named large-call sequential/permuted controls to expose the misleading amortization assumption. Allocation counts and timing belong to benchmarks, not wall-clock assertions in unit tests.

Extend the existing small boundary fixture for both caller-sized chunks and a whole call, with independent expected sums/NULLs, constants, offsets, unmatched IDs, repeated local groups and sparse group IDs. Retain the physical-overflow-order witness because it protects direct updates and deferred scatter. No service, cluster, input dataset, sleeps or benchmark result-verification script is needed. No new SQL-visible behavior requires BVT; the existing head's successful CI and owning-package tests remain evidence for unchanged production code.

## Review classification

Focused removal of an unsuccessful optimization and correction of test coverage; no new feature or major refactor. Design gate is exempt for this review fix. Original approved optimization design is superseded by the final test-only scope above.
