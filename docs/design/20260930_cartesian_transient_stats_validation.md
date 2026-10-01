# Cartesian DML remediation: delivery evidence

Scope: [PR #29527](https://github.com/matrixorigin/matrixone/pull/29527),
issues #29497, #29533 and #29534. The selected design is
[revision v8](20260930_cartesian_transient_stats.md).
Current main/base: `0d3687004d712e0878d63f2e848f69c5df5d1c0f`.
Historical performance control: `c2abd6a54b7cd3e13c1b1494388cd7a81b8369d4`.
The original PR head `f93762ed90e618f477a37ecee640ce8395c72d8e` is the regression control.
Design and final review reuse GPT-6.1-sol / xhigh; the primary agent implements
and runs adversarial QA. No exact row-count scan is added to planning.

## Follow-up verification of the three reopened findings

The previous delivery review at `aa83017933` was reopened by finite AUTO_INCREMENT
prefetch, deleted appendable objects and inconsistent SizeMap denominators.
All three have focused red/green evidence; the changes use their existing owners.

- Default cache, 3 actual INT rows, finite 4.29B or near-MaxInt64 estimates:
  durable reservation remains 10,000; a second independent allocator using the
  same store successfully writes 10,001. A 9-row batch with a configured range
  of 4 reserves its actual demand and the next allocator writes 14. No shared
  preAllocate clamp changes real batches. The obsolete compiler graph DFS is
  removed; the planner hint only uses the existing signed conversion.
- Sealed appendable fixtures exercise snapshots before/equal/after deletion,
  future objects and unknown zero-row metadata. Current visible object rows
  remain small, while visible unknown metadata still fails closed. Public SQL
  flushes the 5-row source, adds five own rows and plans/writes exactly 80 CROSS
  rows, then verifies full content and rollback.
- Width tests cover 5→10,005 rows, an 8 KiB observed payload, rounding/zero bytes,
  invalid denominators, individual and summed uint64 overflow and immutable
  published maps. Public wide-row growth retains rowsize=8200.00 rather than
  shrinking its old byte totals across the new row denominator.
- Real public SQL with a 4.29B estimate inserts only five INT AUTO_INCREMENT
  rows. The persisted mo_increment_columns offset remains 10,000; after a full
  owned service restart, a cold allocator successfully writes INT 10,001.
- Current normal owning packages pass: incrservice, disttae, compile, plan and
  frontend. Focused race passes for allocator and relation boundaries, and the
  compiler constructor cases pass race. Incremental lint of all three changed
  packages reports zero issues. One initial race link and lint run were stopped
  after prolonged tool-stage inactivity; raw SIGQUIT diagnostics are retained.
  Successful reruns bound tool GOMAXPROCS=2; no product change masks this event.
- Fresh two-round controls versus clean base pass 36/36: cached SELECT and COUNT
  are 1.00x, CROSS 1.00x, LIMIT0 1.03x; binary prepared is 0.465→0.413 ms.
  The unrelated-500-write controls remain 0.93–1.04x. All four phases pass real
  prepared growth, atomicity and rollback oracles and clean service teardown.
- Dedicated AUTO_INCREMENT comparisons alternate pre-follow-up v2 and current
  binary twice. Five-row writes are 9.293→9.114 ms (0.98x; rounds 1.30/0.94);
  20,000-row writes crossing the normal allocation unit are 57.037→45.784 ms
  (0.80x; rounds 0.83/0.79). Each phase has two warmups and nine timed samples,
  with complete count/distinct-ID/sum/range checks outside timing. There is no
  demonstrated repeated normal-path slowdown; timings are not universal bounds.

Pre-integration follow-up binary SHA-256:
`e1a2cb485edc0d83f4af4979d96223f86bcbea8760af8389d40f2e40d6c3ff91`.
Its source starts at `aa83017933` plus the focused working changes. The only
production change after that build is the allocator interface's documentation
comment; the execution semantics match the tested binary. Original capacity,
spill and BVT evidence below remains qualified evidence for their unchanged
owners, not a claim that the original 300M workload was replayed after this fix.

## Integration with the updated PR main

The remote main merge `a8a24d7c30` is preserved, with current base
`0d3687004d712e0878d63f2e848f69c5df5d1c0f`. Its default Sirius adapter
uses the existing explicit offload gates and the untagged CN stub. No optional
Sirius/GPU backend is claimed tested. The integrated default production build
passes; binary SHA-256:
`138b41b881b1db9879e7352aacde70020a354a0ec92e78de5e5ebe6a4f8c11a9`.

All five owning packages pass on the integrated source: incrservice, disttae,
compile, plan and frontend. The first GOMAXPROCS=2 compile run fails five
file-fanout fixtures whose fixed three-scope assertions require
at least three runtime execution slots; GOMAXPROCS=4 reruns the complete
compile package successfully. No product or test code is changed to hide the
configuration-dependent failure. Race and incremental lint evidence above
cover the unchanged focused production owners.

Public SQL is repeated on this binary with fresh logs: flush plus own writes
plans and writes 80 rows with full content and rollback; wide growth retains
8200-byte rows; finite 4.29B AUTO_INCREMENT estimates reserve only 10,000,
and a full service restart writes INT 10,001. Both owned services exit cleanly
and the catalog returns to seven databases. Historical matched performance
controls above remain qualified measurements of the focused correction, not
a performance comparison against the newer main merge.

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
  scan/AGG/shuffle. AUTO_INCREMENT persistence is now protected at the bounded
  speculative allocation owner, not by traversing the source plan.
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

## Initial v2 performance evidence

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

## Initial v2 validation and provenance

Production build and normal owning packages pass: plan, frontend, compile,
disttae, plus upgrade. The four shared-admission owning packages also pass race;
the contention counterexample passes 100 race repetitions. Focused cancellation,
prepared-error, snapshot/cache, remote and overflow counterexamples pass.
Incremental golangci-lint across the five affected packages passes. Current and
clean-base molint diagnostics are identical (18 pre-existing findings).

Baseline binary SHA-256:
`50c8a5bf8ffba569e8beaca04fe0d5fe1dd4325d36ecaa3fc1c5448e6317ae15`.
Initial v2 candidate binary SHA-256:
`5a01f6a298af799e805c3ae432a07340a238e1b8c16f50a6095441275014cf27`.
The candidate build records HEAD `9358b8f2a91e5b452ec8bdad25b0c9d3fd04dbba`
and complete base-to-working-tree diff SHA-256
`5f4da9fed6f251d3556499a3d84e1dd0e07e763cfb1d28ad67c1abef0aa98236`.
Later source changes are a comment wrap and additional tests/documentation;
production semantics match the tested binary. Local raw logs are retained outside
this repository; this document and the PR provide reviewer-accessible results.
New-head CI is not awaited, as requested. Older green CI does not certify this
remediation revision; Iceberg is outside the requested scope.


## Reopened CI repair evidence

The exact `91e413abca` CI run failed UT and coverage on the same Adaptive Top
fixture, plus 7 PROXY and 32 PESSIMISTIC BVT statements. Dependent summary
checks also failed. This supersedes earlier delivery PASS; it is not a claim
that the repaired revision has passed CI. Iceberg is excluded by user direction.

BOOL producer and persisted metadata tests fail before repair and pass afterward.
A real disk writer/reader fixture preserves legacy false/false raw bytes, exposes
conservative read bounds and preserves current raw false/true writes and area
length. Clean-main forced-block-filter control proves the producer bug predates
this PR. The unchanged native BOOL PICK BVT passes after the common-owner repair.

Warm same-SQL and same-handle vector controls reproduce stale FORCE after AUTO
is disabled, while cold planning returns the required POST empty result. The
original empty expectation is retained. Added parameterized handle executions
prove repeated AUTO→POST→AUTO changes. Cache UTs cover both defaults, normalized
same values, invalid SET, exactly-once AST free and preservation of prepared handles.

Normal owning vector, objectio, readutil and index packages pass. Focused zonemap
consumers and session-cache tests pass. The initial full vector run encountered an
existing timezone-dependent assertion; UTC matches the CI environment and passes.
Existing /tmp object fixtures were owned by another process/user; full objectio
passes in a private mount namespace without changing those files. Full frontend
passes (24.369s), focused session-cache race passes (1.181s), and the selected
vector/objectio/frontend vet checks pass. Final malformed BOOL-bound coverage
also passes; non-BOOL metadata read views retain zero allocations.
The final exact-source production build passes. Incremental golangci-lint
(Go 1.26.4, v2.6.2) on the four newly changed owning packages reports zero issues;
qualified prior incremental checks are reused for unchanged package closures.

GPU-tagged tests are **NOT VERIFIED**: this environment has neither the CUDA
toolchain nor a GPU device. The mo-dev CGo reference requires the whole GPU set
after a shared vector edit; CPU results do not satisfy that separate gate.
On 2026-09-30 the user explicitly instructed immediate push of these repairs.
That instruction overrides the skill's pre-push GPU gate for this delivery only;
the GPU suite remains NOT VERIFIED, with no GPU correctness/performance claim.

All 13 affected BVT scripts pass normal comparison twice on the same owned
service: 2,508/2,508 statements each round, zero failures, ignored or abnormal.
The catalog returns to seven databases and owned service exit is 0. Range JOIN,
REUSE, nonzero SpillRows/SpillSize, exact content and mode-off empty results are
retained. Full BVT uses the repository default process budget; the dedicated
64MiB capacity/performance evidence above remains separate. No CI wait or full
PROXY/PESSIMISTIC topology-pass claim is made from this normal comparison.
