# Conditional FULLTEXT2 stream Top-K validation

Validated on 2026-10-27. This is the follow-up to the row-key/score batching work
in Draft PR #31407. The preceding stage's measurements remain separate; its
approximately 5% query improvement is not the baseline for the causal comparison
below.

## Binaries and protocol

The matched baseline is `706ef7af57259f67cd34363315e2a9ca015889d4`, which already
contains the preceding optimizations and main through
`baa83f4b96e56ec214f92c68f7b56539b9e0fa45`. The candidate is
`280445e36fd907679c34601b6fd9f0f687073a04`. Later documentation and BVT-only
changes do not alter the tested production files.

Both binaries used the same verified native libraries, dictionaries, Go 1.27.1,
`GOEXPERIMENT=simd`, and `GOAMD64=v3`. SHA-256:

- Baseline: `82ab86033f9e4e91046dbdf28758c2348c35d5cff0ebcb3d2e0d0d8090ff6f22`.
- Candidate: `967d7a303b61f2446cdfdb8449f1299048da820f9c0ba30f39e36df3acc84dd9`.

Host 50 ran one engine at a time: CPU 0-7, 8 CPU quota, 24 GiB hard memory cap,
20 GiB pre-stop guard, swap disabled. The client used CPU 8 and a 2 GiB cap.
CN/TN/log file caches were 4 GiB/512 MiB/256 MiB and the index-cache limit was
4 GiB. The benchmark sessions used 1 TiB aggregation/join/sort spill thresholds;
global defaults were unchanged. No build or Go tests ran alongside the engine.

The existing complete MS MARCO corpus contained 8,841,823 passages. Schema,
analyzer (`gojieba`), body-only `POSITION_FREE` index and original SQL were kept:
PK plus BM25 score, positive score filter, score DESC/PK ASC, LIMIT 1000. There
were no stored/generated columns, extra covered fields or approximate results.

The 20 fixed diagnostic DL19 topic IDs were 19335, 47923, 87181, 87452, 104861,
130510, 131843, 146187, 148538, 156493, 168216, 182539, 183378, 207786, 264014,
359349, 405717, 443396, 451602 and 489204. This is not the full 200-topic run.

Each phase ran 20 pre-measurement EXPLAIN ANALYZE statements, one full-fetch
warmup per topic, three timed full-fetch repetitions per topic and two final
EXPLAIN ANALYZE statements. Thus there were 102 workload executions per phase,
including 22 diagnostic executions excluded from timings. Those diagnostics
already warmed the cache. CPU is engine cgroup CPU during each fetch, including
background activity, rather than isolated operator CPU.

## Full-corpus A/B/A

| Metric | Baseline A0 | Candidate B | Baseline A2 |
| --- | ---: | ---: | ---: |
| Sum of 20 per-topic median latencies, seconds | 29.852755 | 4.212406 | 30.075846 |
| Sum of 60 timed fetches, seconds | 90.339623 | 12.722601 | 95.671210 |
| Engine CPU during 60 timed fetches, core-seconds | 111.634958 | 13.445887 | 108.802577 |
| Captured complete fetches / result rows | 80 / 80,000 | 80 / 80,000 | 80 / 80,000 |

Relative to the faster baseline latency sum, the candidate is 7.09 times as fast
(85.89% less elapsed time). Relative to the lower baseline timed CPU total, it
uses 87.64% less CPU. Every one of the 20 candidate median latencies is lower
than both corresponding baseline medians.

All three cross-phase comparisons passed: A0/B, B/A2 and A0/A2. Comparison
includes native column metadata, exact ordered primary-key bytes and FLOAT32
score bits. It does not sort the results again or apply a numeric tolerance.
All 22 candidate EXPLAIN ANALYZE checks showed actual Fulltext Index Scan output
of 1000 rows; baseline outputs ranged from 180,766 to 8,659,636 rows.

A0 rebuilt the original index in 224.991 seconds, outside query timing. B and
A2 reused the same hidden index/meta table UUIDs without rebuilding. A harness
metadata selector missed `ftv2_meta`, so the separate build_ts/tail generation
capture is empty; identical generation contents are not asserted from UUIDs
alone. All 22 diagnostic executions exercised the collector and therefore
passed its runtime visibility proof. Activation was not separately instrumented
inside each of the 80 ordinary full-fetch executions.

All completed phases recorded zero OOM, swap, spill reservation and budget
rejection events. Whole-engine memory peaks were 9.947/5.107/5.612 GiB; A0
includes index rebuilding, so these are not matched query-only peak-memory
measurements. The additional collector retains at most 8192 references (about
192 KiB), plus owned output for winners. This is not a guarantee of zero OOM for
arbitrary corpora or concurrency.

## Controls and limitations

Normal owning-package tests/vet, public planner binding controls, incremental
lint, focused repeated race tests and owning-package race tests passed. Tests
cover the real default VARCHAR comparison identity, ties, pagination limits,
visibility fallback, stale/tail generations, cancellation and buffer ownership.

The fresh-service SQL harness passed 33 semantic controls plus 63 small-query
captures for each accepted baseline/candidate round. It covers ties, offsets,
empty results, residual filtering, prepared execution, own uncommitted deletion,
rollback, foreign uncommitted insertion and old-snapshot versus committed
deletion. Ordered IDs and score bits were compared to the baseline.

The added `fulltext2_conditional_topk.sql` BVT passed with the existing mo-tester:
25/25 commands succeeded, no failures/ignored/abnormal commands, empty error
report, unchanged hand-written expected results and no residual case database.
It ran on a separate fresh tiny-only service, never the corpus service. An
initial runner attempt stopped before any case because an empty `outfiles`
configuration produced a null array; the private runner configuration was
corrected and that failed attempt remains in the evidence.

An early candidate round was 0.4-0.6 ms slower than an earlier baseline on tiny
queries. An adjacent baseline/candidate rerun did not reproduce that increase:
median LIMIT/no-hit/residual latencies were 1.166/1.355/1.533 ms versus
1.066/0.916/1.113 ms. All rounds are retained; this is not proof of zero overhead
or universal absence of regressions.

A separate large fallback control added a residual primary-key predicate to
topic 359349, leaving the official 20-topic SQL unchanged. Each A/B/A phase
ran one warmup and five timed fetches. Actual index output remained 8,659,636
rows, proving the ordinary stream was exercised. Median wall times were
2.249274/2.268912/2.296766 seconds and median CPU was
2.805056/2.872726/2.900300 core-seconds. Candidate values lie between the two
baselines; this control did not show a consistent regression. The three exact
cross-phase comparisons passed for all six captures per phase (18,000 captured
row observations across the three phases, not distinct document IDs). These
timings are excluded from the main performance table.
Against A0 alone the candidate was 0.87% slower in wall time and used 2.41% more
CPU; against A2 it was 1.21% faster and used 0.95% less CPU. A2's final two wall
samples were 3.142/3.007 seconds versus approximately 2.30 seconds earlier. All
five samples per phase are preserved rather than discarding slower observations.

The public KILL QUERY attempt did not observe the query before it completed;
it is **not exercised**, not a cancellation pass. Same-connection health and a
fresh exact-result query passed afterwards. Deterministic tests cover collector
cancellation and ownership. Existing tokenization/dirty-document-frequency
preprocessing is outside the collector's periodic cancellation bound.

The optimization intentionally falls back for unsupported SQL shapes,
collations, source changes, CDC tails, own writes and uncertain visibility.
Conservative source-change retention can also prevent activation for an older
index, even without a recent user write. It does not remove the source join or
change analyzer/BM25 arithmetic, and does not implement WAND pruning.

The previous ES 8.17 reference for these 20 topics is 0.618128 seconds as a sum
of medians. The candidate is 6.81 times that historical result, versus 46.29
times for the preceding optimization stage. ES was not rerun in this stage;
tokenization/IDF differ. This comparison is not a new relevance evaluation,
concurrency result or a claim about all PMC/MS MARCO/BEIR/TREC workloads.

Full raw evidence, including startup/harness failures, is retained in the local
`fulltext-topk-followup-20261027` investigation directory. Performance totals
exclude failed or non-activated attempts.
