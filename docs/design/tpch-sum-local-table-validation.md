# Caller-sized validation for grouped decimal SUM

Owner: #30847; umbrella #30838.

## Final scope

The grouped SUM change adds tests and benchmarks only. Production SUM code is identical to main; its cache, executor state, arithmetic and update/scatter order are unchanged.

## Regression disposition

The frozen comparison of main `517a0a412b` with the rejected implementation
`f4beca75c7` measured +14.65% for caller-sized permuted groups and +155.68% for
the large-call permuted control. Those results are failures, not accepted
tradeoffs or performance claims for the current head. The range index was
removed before the current test-only SUM revision; the final diff against
main contains no production file under `pkg/sql/colexec/aggexec`.

The remaining production change belongs to HAKeeper CN mutation deadlines,
not SUM execution. The retained tests/benchmarks preserve the real caller's
256-row boundary and the reordered-ID controls for future optimization work.

## Review finding and correction

The initial range-rejection optimization built a min/max and empty-slot index once a 255-entry local table filled. Its 8192-row single-call benchmark showed a gain, but group.buildOneBatch calls BatchFill with at most hashmap.UnitLimit (256) rows. In such a call the index can be built for at most the last row, cannot amortize its scans, and regresses reordered group IDs. The initially claimed 5.07x kernel gain does not describe normal grouped execution and is withdrawn.

The range index and all production probe changes are removed. An index-free contiguous-membership alternative also failed the caller-sized benchmark and is not retained. Future optimization of #30847 requires a design grounded in the real caller's work and state lifecycle.

## Matched current-source check

A separate current-source comparison used main `6fb6531432` and the
PR source `5affe39d58`. Both production SUM blobs are
`e282f8d3188a3d172fe5209104117648dea7c938`; no non-test aggexec file differs.
Ten balanced process pairs used one physical CPU, GOMAXPROCS=1, GOAMD64=v3 and
GOEXPERIMENT=simd. Each case ran twice per process; the first result was excluded.
The complete controls, samples and uncertainty are retained in the PR discussion.

The large-call permuted control measured 400.272 versus 404.339 microseconds per
8192 rows; the paired estimate is +0.95% with ratio interval [0.9958, 1.0228].
Caller-sized permuted calls measured 130.859 versus 131.376 microseconds, with
paired estimate +0.40% and interval [0.9863, 1.0119]. These uncertain differences
are not the removed implementation's +155.68% and +14.65% results. Every scored
control reported zero allocations. No current SUM optimization or speedup is
claimed; this check confirms which source the historical failure describes.

## Validation contract

Benchmark public aggregate dispatch with 8192 rows consumed in hashmap.UnitLimit-sized calls, original vector offsets, and state allocated outside timing. Include cardinalities 4/254/255/256, sequential and permuted high-cardinality IDs, and a high endpoint. Keep explicitly named large-call sequential/permuted controls to expose the misleading amortization assumption. Allocation counts and timing belong to benchmarks, not wall-clock assertions in unit tests.

Extend the existing small boundary fixture for both caller-sized chunks and a whole call, with independent expected sums/NULLs, constants, offsets, unmatched IDs, repeated local groups and sparse group IDs. Retain the physical-overflow-order witness because it protects direct updates and deferred scatter. No service, cluster, input dataset, sleeps or benchmark result-verification script is needed. The SUM changes introduce no new SQL-visible behavior requiring BVT. Current validation must be tied to the rebased source; historical passing runs do not establish a green pipeline.

## Review classification

Focused removal of an unsuccessful optimization and correction of test coverage; no new feature or major refactor. Design gate is exempt for this review fix. Original approved optimization design is superseded by the final test-only scope above.

## HAKeeper caller-budget correction

The CI failure in `TestCDCNoFullPublicTakeoverLifecycle` occurred while its deferred CN C label cleanup returned a Dragonboat timeout. CN label update, patch and deletion each detached their preflight checker-state read into a background context with a fixed two-second deadline, even though the mutation already had a caller-owned deadline. They now use that context for both the read and proposal, matching CN work-state updates. The CDC cleanup and takeover assertions remain intact. No retry loop, global deadline change, new hook or wire change is introduced.

The existing store fixture checks all four mutations with an already-expired deadline and an unknown CN. This distinguishes deadline rejection from the detached read's missing-CN error or successful no-op deletion, without sleeps or another cluster. Normal and race tests also cover the existing successful label/patch/delete/work-state paths and the real multi-CN takeover consumer. The original CI timeout has not been reproduced locally, so this correction does not claim to eliminate every cause of HAKeeper stalls.
