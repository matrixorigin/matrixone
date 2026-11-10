# Covered FULLTEXT2 score TopK: ANLI evidence

This optimization bounds the internal search only when the covered FULLTEXT2
path has already proved exact predicate/projection coverage and the SQL order
is the driving MATCH score descending. The candidate bound is LIMIT+OFFSET;
the final SQL sort and pagination remain in place. Other orders, additional
sort keys, residual qualification, FOUND_ROWS, rank options, and unsupported
pagination receive no new candidate bound from this optimization. Existing
non-covered optimizations retain their own eligibility rules. SHOULD and IS NULL semantics
are unchanged. An internal K bounds retained candidates, not documents visited.

## Implementation and version boundary

The original planner optimization was commit
`7447e4bd633730d4b90241273d2f7bf1cc0cbe69`. Its observation-only child was
`90af16daa17c9167e67a90c73025a257d69a4efa`.
This PR ports the optimization to main's INDEX_SEARCH_SCAN representation and
its existing IndexSearchScan.CandidateLimit consumer. It does not include the
diagnostic patch, change the search kernel/cache policy/index format, or change
the ANLI runner's index defaults.

The experiments below are historical evidence on those frozen versions. They
are not performance or deployment acceptance for the ported PR head. The B/C
comparison changes index coverage on one optimized MO version; it does not
isolate the planner commit from all other coverage/Join effects.

## Small-data correctness

[Source 38204870972, attempt 1](https://github.com/matrixorigin/mo-nightly-regression/actions/runs/38204870972)
and its independent owner completed successfully on original MO 7447e4bd and
harness `9cef304556422f1e7ce254b70f66d3ab65dfa9f7`.
A/B/C four-table static qualification, actual cross-segment B/C data, DML,
rollback, untouched-table isolation, rebuild, controlled CN replacement and
12-table content equality passed under FT2_ASYNC_900_V1.
Original strict60 failures (B: 2, C: 6) and strict180 failures (C: 2) remain
failures. This optimization does not provide immediate read-your-writes or fix
index/cache freshness. Original runtime mechanism evidence stays INCONCLUSIVE.

## Query-bound mechanism probe

[Source 38357952931, attempt 1](https://github.com/matrixorigin/mo-nightly-regression/actions/runs/38357952931)
used diagnostic MO 90af16da, harness
`f3208bb8637719f993c4f15d8f81aa82f7defc31`, and artifact 11794027116.
One C Q1 over 658 fixture rows and three actual segments passed the independent
Top200/content oracle and query/statement/node/invocation/round hash binding.
The terminal receipt observed planned/effective/runtime/resolved K=200,
656 exact IS NULL evaluations, 440 rejections, zero predicate read errors,
SearchInto, Emit=false, no fallback, and no post-limit residual qualification.
These observations belong only to that probe/version, not every timed query.

## Real-data paired Q1 latency

[Source 38527421291, attempt 1](https://github.com/matrixorigin/mo-nightly-regression/actions/runs/38527421291)
used MO 90af16da with diagnostic collection disabled, harness
`a680221ef85eb638b2ff6890655ac83b8d151462`, comparison artifact 11883269765,
and immutable image digest
`5efa631494a4a6ef2ae7fb7d9db15e9055bd633e8753300da6f400854acf4f9c`.

- B: FULLTEXT2(question), ngram; C: same index with INCLUDE(delete_flag).
- One real table per arm, 715679 rows, equal full-row content digest before and
  after timing: `ac539166d5083392832942d7d7e6940d2037052e3f650f7c28cf55317a9d3a28`.
- Source snapshot digest:
  `4173dcd0cc358f81a22bb856b7a2ed006cf3869ef9bc83b9c7333b6ca494bf77`.
- Fixed 100-query parameter pack digest:
  `c22b38e37018bd86d47f079e24d0cdf66b0bfe2f44dccb4e6930e5d9d6292c70`.
- Parameters were seeded from the first 1024 primary-key-ordered eligible input
  rows; this is not a production-traffic-representative sample.
- Hot cache, no cache reset, one serial client; five groups, each BC and CB.
  Each arm performed 20 warmups and 100 timed requests: 400 warmups and 2000
  retained timed samples total. All ten pairs were valid.
- All 100 untimed exhaustive B oracle checks passed for both arms, with legal
  cutoff ties. Load, index build, convergence, oracle and plan collection were
  outside the timed samples.

Nearest-rank pooled percentiles over 1000 samples per arm:

| Arm | p50 ms | p95 ms |
| --- | ---: | ---: |
| B | 594.386 | 1318.612 |
| C | 195.277 | 769.484 |

The median of the ten per-pair median sample ratios B/C was 2.627073.
Pair ratios ranged from 2.542170 to 2.789526 (standard deviation 0.070775).
This statistic is not the ratio of the pooled p50/p95 values.
Both orders showed improvement. No SQL errors or TP restarts were recorded.
Before/after observations retained a stable set of 81 competing workloads;
maximum observed node CPU was 10%, memory 46%, with no flagged interference.
This remains a shared-environment result, not proof of isolation or absence of
transient storage/network interference. It does not measure classic FULLTEXT,
the full two-stage nqft request, mixed/vector load, or concurrent throughput.

## Terminal ownership and cleanup

[Independent owner 38538289582](https://github.com/matrixorigin/mo-nightly-regression/actions/runs/38538289582)
waited for source terminal before cleanup. Its installation receipt was
persisted before environment creation. Finalization artifact 11884675133
reported exact namespace UID cleanup, five owned PVs absent and COS storage
PASS. Client close and both database drops passed. Independent local read-only
checks confirmed namespace, Pods, PVCs and the five PV names absent.
CBS remains explicitly USER_WAIVED / UNVERIFIED / required=false; PV absence
does not prove backing-disk destruction.

## Adoption boundary

Developer validation for the main-based port: focused and full planner tests,
full FULLTEXT2 tests, search-consumer package tests, vet=all, incremental lint,
formatting and whitespace checks passed. On clean base
`fab7b5c0430a475bf64ad1619ce4d5249013a138`, the same public score-alias SQL
has a nil candidate bound and fails the mechanism regression. The candidate
retains the expected bound. New-head service BVT execution and Linux CI remain
pending; the new canonical SQL/result regression is included for that gate.

Use the measurements to review the optimization candidate, not to claim whole
ANLI or FT1/FT2 speedup. New-head CI/BVT and reviewer acceptance remain separate
gates. Freshness requirements, index storage/build/update costs, multi-client
performance, and complete Q0-Q3/A-B-C matrix acceptance are not closed by this
experiment.
