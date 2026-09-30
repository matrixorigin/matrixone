# Cartesian DML remediation: delivery evidence

Scope: [PR #29527](https://github.com/matrixorigin/matrixone/pull/29527),
issues #29497, #29533 and #29534. The selected design is
[revision v6](20260930_cartesian_transient_stats.md).
Base: `c2abd6a54b7cd3e13c1b1494388cd7a81b8369d4`.
The original PR head `f93762ed90e618f477a37ecee640ce8395c72d8e` is the regression control.
Design and final review reuse GPT-6.1-sol / xhigh; the primary agent implements
and runs adversarial QA. No exact row-count scan is added to planning.

## Contract and adversarial checks

Missing published statistics use existing partition memory/object metadata and
transaction INSERT metadata as a conservative bound. Read-only workspace admission
is O(1); writing transactions inspect their existing log with TryLock. Unknown
metadata fails closed. Published statistics remain owned by the existing producer;
anonymous observations cannot pass the completed-statistics fast cache. Existing
plan/compile generation owners rebuild on changed source counts, without mutating
borrowed plans. RIGHT SEMI preserves physical child 1 after swap.

- Typed tests cover persisted object rows versus metadata-batch length, rollback,
  unrelated writes, mutex contention, snapshot cancellation, remote/combined
  incomplete observations, published-map immutability, integer overflow through
  scan/AGG/shuffle, and auto-ID source-step traversal.
- Both compiler contexts cover historical observations, named empty statistics,
  refresh within three seconds and own writes. Actual final-plan LIMIT 0 and
  COUNT models retain their established bypass. Prepared admission preserves
  stable compile identity and rejects statistics errors before execution.
- Public natural estimates: 5-by-8 yields 40; inserting five own source rows in a
  transaction yields 80; 500 unrelated writes leave 40. Full result and rollback
  oracles pass. Actual JDBC ServerPreparedStatement SELECT/INSERT reuse grows
  from 40 to 40,040 rows, with independent ordinal/key/payload oracles, duplicate
  error 1062 atomicity, rollback and subsequent writes.
- Actual 1.2M-row workload at 64 MiB passes with and without a primary key in
  default and accurate-statistics modes. Checks include every row's key/payload
  relation, count/sums/ranges, duplicate atomicity, rollback and follow-up writes.
  Existing diagnostic summaries prove automatic spill: HashBuildSpillStarts=1,
  query cap 67,108,864 bytes, peak 67,091,184 bytes, pipeline_failed=false.
- Normal BVT comparison passes 42/42 twice on the same instance; each teardown
  restores seven system databases. Test-owned services exit normally.

## Performance

Same native inputs, Go 1.26.4, one CN, local NVMe, 64 MiB query budget. Four
sequential phases alternate clean base and candidate, twice each. Default,
accurate and intentionally stale-high modes produce 360 successful scenario-phase
results (90 comparisons, 2,128 timed SQL statements). Setup and independent result
checks are outside timing. Medians below pool the two phases for each binary.

| Default mode | Base ms | Candidate ms | Ratio |
| --- | ---: | ---: | ---: |
| Cartesian INSERT without PK | 5.362 | 5.009 | 0.93 |
| Cartesian INSERT with PK | 10.337 | 8.712 | 0.84 |
| Cartesian INSERT with multiple UNIQUE | 18.403 | 18.576 | 1.01 |
| Cartesian INSERT with regular index | 5.126 | 5.212 | 1.02 |
| Cartesian subquery DELETE | 5.087 | 5.017 | 0.99 |

These replace the original PR's repeated 2.25–5.53x tiny-writer regressions.
Accurate-mode upsert is 9.757→9.564 ms; REPLACE is 8.543→9.947 ms (1.16x,
rounds 1.19/1.14), with unchanged physical shape. The preliminary 3x upsert/REPLACE
outlier did not repeat. These timings are observations, not a universal guarantee.

Additional matched controls pass 36/36. Each ordinary case has 50 warmups and
200 samples per phase; binary prepared SELECT has 50 warmups and 100 samples.
Stable binary SELECT is 0.407→0.417 ms (1.02x). Read-only cached SELECT, COUNT,
LIMIT 0 and CROSS are 0.98/0.94/1.00/0.99x; inside a transaction with 500 unrelated
inserts they are 1.03/1.00/0.99/1.00x. Public growth/unhappy oracles also pass on
both binaries in every phase.

The writing-workspace metadata benchmark costs 0.794 µs / 37.654 µs / 1.093 ms
at 100 / 10,000 / 100,000 existing entries. It is linear in metadata entries,
not data rows. Reported fixture allocations include the mocked operator lookup.
No additional per-table counter/cache/state machine is introduced.

Deliberately stale-high published counts retain a sensitivity to corrected costs:
CROSS GROUP is 0.474→0.992 ms (2.09x), despite unchanged physical shape. Its
runtime cause is not established by profiling. Multiple-UNIQUE is 54.443→57.466 ms
(1.06x), with differing round ratios. Query-cap admission may select shuffle/spill
for stale estimates. This tradeoff remains explicit; precision is not obtained by
scanning tables or overriding the published statistics owner. Cold storage, S3,
concurrent-load tails, multiple CNs and the original 300M rows were not benchmarked.

## Validation and provenance

Production build and normal owning packages pass: plan, frontend, compile,
disttae, plus upgrade. The four shared-admission owning packages also pass race;
the contention counterexample passes 100 race repetitions. Focused cancellation,
prepared-error, snapshot/cache, remote and overflow counterexamples pass.
Incremental golangci-lint across the five affected packages passes. Current and
clean-base molint diagnostics are identical (18 pre-existing findings).

Baseline binary SHA-256:
`50c8a5bf8ffba569e8beaca04fe0d5fe1dd4325d36ecaa3fc1c5448e6317ae15`.
Candidate binary SHA-256:
`5a01f6a298af799e805c3ae432a07340a238e1b8c16f50a6095441275014cf27`.
The candidate build records HEAD `9358b8f2a91e5b452ec8bdad25b0c9d3fd04dbba`
and complete base-to-working-tree diff SHA-256
`5f4da9fed6f251d3556499a3d84e1dd0e07e763cfb1d28ad67c1abef0aa98236`.
Later source changes are a comment wrap and additional tests/documentation;
production semantics match the tested binary. Local raw logs are retained outside
this repository; this document and the PR provide reviewer-accessible results.
New-head CI is not awaited, as requested. Older green CI does not certify this
remediation revision; Iceberg is outside the requested scope.
