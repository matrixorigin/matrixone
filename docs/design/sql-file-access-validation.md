# SQL file access validation record

## Validated snapshot on main `97415db10bd`

The rebase adds ALTER TABLE CHECK support. All 172 existing delivery files
remain byte-identical before the following reader-ownership cleanup. The two
consumer-local reader/service wrappers now share one fileservice owner, keeping
the same close order, error and owned/borrowed contracts. No policy or DML work
is added by this cleanup.

The final requested rebase onto `8669eedae682f7aea093c6cab3d1b8877f91a8dc`
adds only window spill admission-error normalization. The tested reader,
consumer and parser/planner contracts are unchanged; their evidence below
retains base `97415db10bd` and is reused, not relabeled as fresh final-base runs.

| Item | Status |
| --- | --- |
| Source design | Shared reader cleanup approved before implementation; final source-review decision is recorded separately in `sql-file-access-review.md`. |
| Focused validation | Five owning/consumer packages pass the selected normal and once-only race checks (51 named race tests); the full Datalink package also passes normal. Six affected packages pass fresh vet, molint and incremental lint; format/err-check/diff checks pass. Earlier evidence below retains its tested revision. |
| Same-binary −2.13% elapsed | Compares old/new complete validation within one candidate binary; not the whole PR against main. |
| Whole-PR/main performance | **Unresolved; blocks merge acceptance.** Historical failed timing cells remain below and in the CSV. |
| PR update | The existing explicit update-only exception allows rebase/push with this timing gate unresolved; it does not approve merging. |
| GPU-native validation | Still unverified under the existing exception for PR update. |
| Remote CI and review threads | Consult the live PR; local source fixes and replies do not imply passing CI or resolved threads. |

The focused checks use Darwin arm64 / Go 1.27.1 and the existing qualified CPU
native generation, with unchanged native/module inputs. No cluster or benchmark
campaign was started. The first normal command passed fileservice/parser but
stopped dependent packages on an unused import; the next passed function/plan
but exposed the new test wrapper's missing ETL marker. Both were corrected, the
complete Datalink package passed, and all five selected packages then passed
race together. These earlier failures remain in the ledger and are not labeled
green commands. The prior Darwin fileservice sync-dir baseline failure remains;
these are focused passes, not a green whole-fileservice suite.

Raw commands, source/test fingerprints, terminal logs and package-by-package
normal reconciliation are in `revision-20261016/review-followup-97415/` under
the issue30185 ledger. Unchanged public SQL/CDC/two-CN/BVT evidence is reused
with its original scope. New helper/consumer and parser/planner checks replace
only the conclusions affected by this cleanup/rebase.

The sections below are revision-qualified historical evidence. No earlier result
is relabeled as a new measurement on this base, and no failed sample is removed.

## Parsing optimization on bases `6b081a1fcfe` / `85d03e833eb`

Initial validation uses main `6b081a1fcfe`; the final requested rebase uses
`85d03e833eb`. All eight optimization source/test files remain byte-identical.
Native CGo/third-party sources and module inputs are unchanged; the Makefile
change affects only the UT scheduler, not native build targets or flags. The shared URL/stage and
range resolver now supports error-only CAST and final-value admission without
CSV path materialization. Existing complete-object/string APIs remain adapters
over the same owners. Provider classification and final-row selection remain
unchanged. No cache, pool, ambient authority or execution path is added.

Focused normal and race tests pass for Datalink, stage, path/CSV interpretation,
CAST and final-row eligibility. Differential cases use the standard CSV writer
and the original path parser, including returned partial paths/errors, quoted
fields, Unicode/invalid UTF-8, CR/LF, colons, empty/dot paths and raw named
services. Datalink controls cover range error precedence, nested/disabled/cyclic
stages, policy refusal and the deliberately weaker legacy CAST contract.

A single-process component comparison freezes the pre-optimization
`6d0e16ba23a` helpers, including Stage materialization; three alternating
old/new pairs of 150 ms per case retain every sample. Canonical one/16-stage
admission reduces 1088 to 688 B/op and 16 to 13 allocations; paired component
time medians improve 21.24%/22.21%. CAST improves 30.70%, 1008 to 608 B/op,
16 to 12 allocations. Complete-object and public-string controls retain their
allocation counts with paired times +0.16%/-0.19%. CR/LF compatibility fallback
retains 9912 B/op and 33 allocations, with paired time +4.08%; this is a small
measured fallback cost, not a zero-cost claim. An initial fallback result added
112 B/one allocation and failed the preset gate; explicit argument ownership
removed that escape, and its failed evidence remains retained.

One real SQL fixture compares frozen old and new **complete validation**, never
checker omission, in one binary and cluster. After 32 warmup rounds, a forced
flush/read/GC starts with 19 data objects, 18 tombstone objects and zero memory
row records. Four fixed ABBA/BAAB blocks use four clients and 4096 rows; all 16
cells assert 8192 actual validation calls. Exact stored rows, the durable CDC
commit barrier and search results pass. Paired block medians are allocated
bytes -4.67%, allocation count -4.03%, process CPU -0.57% and elapsed time
**+6.16%**; elapsed blocks range -16.39% to +25.05%. Normal GC is counted, and
the user's other applications keep running. These short intervals do not fully
isolate external load or establish population P99 or a SQL speedup.

The final base changes internal SQL context and backend dedup execution, so
one protocol-identical SQL comparison is repeated specifically for those
invalidated dependencies, not to replace unfavorable samples. It again passes
all 16 actual-call proofs and final row/CDC/search checks. Its paired medians
are allocated bytes -2.35%, allocation count -2.06%, process CPU -5.14% and
elapsed -2.13%; elapsed blocks are +6.71%, -8.28%, -0.42%, -3.84%. The earlier
+6.16% result remains qualified to base `6b081a1fcfe`; the cohorts are not pooled.
These remain short shared-host observations, not a population P99 guarantee.
The cached-stage component evidence is reused because its source, data, native
settings and timed call graph do not use the changed backend/executor paths.
Final-base focused normal/race and configured static results are recorded
separately under `systematic-opt/rebase-85d/`.

**Overall PR/main performance acceptance remains unresolved.** The earlier
large stalls also occurred before flush on the same unmodified candidate
binary, so an exclusive flush-path cause is not established. No slow-event
trace has identified their root cause. Local component gains do not supersede
those historical failed gates. Raw plans, controls, failed/fixed samples and
terminal evidence are retained in `revision-20261016/systematic-opt/` and
`source-cause/` under the issue30185 ledger. No heavy cohort was repeated.

## Allocation cleanup on source `3465deb8298` / base `c2b990cd712`

On parent `3465deb8298` / main `c2b990cd712`, the private Datalink resolver
returns offset/size scalars; only the unchanged public ParseDatalink API creates
its range slice. Empty queries skip the empty query-map allocation. Nonempty
query decoding, case folding, duplicate-key behavior, parse/error order and
range checks retain the existing implementation. No new cache or provider path.

Owning Datalink normal tests, focused parse/authorization race tests, vet, molint
and incremental lint pass. Existing tables additionally check bare `?`, uppercase
and repeated keys, unknown/malformed query pairs, empty/overflow/range failures
and the public empty-URL/nil-slice error contract. A matched three-by-200ms
actual-NewAsync benchmark uses only a test overlay for the original Datalink
source: both 1/16-stage cases reduce 1144 to 1080 B/op and 18 to 16 allocations;
median component times are 892.9 to 867.3 ns and 899.3 to 880.6 ns. These short
observations establish allocation removal, not end-to-end SQL acceptance.

The parent source's diagnostic run applies 32 alternating source updates over
4096 rows with four clients and verifies exact rows/CDC/search. CPU/trace
instrumentation is enabled, so this is not an acceptance benchmark. Candidate
source time is 4.340s versus main 3.925s; process CPU samples total 10.95s versus
10.71s. The new checker accounts for about 190ms / 1.74% of candidate process CPU;
that denominator includes background work and does not bound foreground latency
or indirect GC effects. Cumulative commit wait is about 8.48s versus 7.59s, and
Raft proposal wait 7.38s versus 6.61s. These overlap across goroutines and must not
be added or treated as elapsed time; they do not establish why waits differ.

**Overall SQL performance acceptance remains unresolved.** The two earlier
same-binary focused cohorts retain all paired results: source UPDATE median
-50.55% in the first, +24.01% in the repeat (+36.23%, +24.01%, -79.53%). Noisy or
favorable samples are not removed. The scalar cleanup is a bounded local
improvement, not proof that those SQL latency differences are fixed. Raw design,
profiles, source overlays and normal/race/static receipts are under
`revision-20261016/admission-opt/` in the issue30185 ledger.

## CSV producer optimization on base `c2b990cd712`

Rebased without conflicts onto main `c2b990cd712`. Fileservice now owns one
service-record encoder used by Stage S3/HDFS and Datalink HDFS. Ordinary fields
avoid the standard writer's 4 KiB streaming buffer; quoted/large records retain
the standard writer. Exact CSV bytes, trailing LF, query/range handling and
admission rules are preserved; no cache, pool or persistent state is introduced.

At the user's explicit request, this revision uses focused tests and short
microbenchmarks instead of another full SQL/race/performance campaign. Previous
broad results below retain their original revisions. They are not current-base
full-suite results, and failed performance gates are not erased.

On Darwin arm64, the same-base real NewAsyncDatalink microbenchmark uses test-only
overlays to compare original producer code with the shared encoder (three
200 ms samples per case). One-stage admission medians are 1072 -> 906.2 ns;
16-stage medians are 1097 -> 939.2 ns. Both reduce 5272 -> 1144 B/op and
20 -> 18 allocations. These are isolated component observations, not SQL TPS/P99.
The encoder-only ordinary case reduces median 306.5 -> 131.0 ns and
4224 -> 96 B/op. Quoted fallback adds about 25 ns with unchanged allocations;
long fallback has unchanged allocations and median 5865 -> 6222 ns in this
short sample. The fallback preflight is bounded at 4096 bytes per field.

The later complete Darwin bulk subset on base `ac55588b8d4` also remains
**not accepted**: indexed one-stage source UPDATE had paired median +38.17%
with 0/6 pairs within +10%; indexed INSERT medians were +8.90% (one stage) and
+6.88% (16 stages). All 18 bulk processes passed functional assertions; the
ordinary portion was interrupted by its host-load guard. This producer change
removes a measured allocation mechanism but has not demonstrated that it
eliminates those end-to-end slowdowns. No no-regression claim is made.

Targeted current-source normal/race tests cover exact writer differential tables,
fuzz seeds, existing Stage S3/MinIO/HDFS success/failure fixtures, quoted
credentials/query and Datalink HDFS ranges. Full generated-input fuzzing and
cluster reruns are not repeated. Raw source overlays, commands and receipts:
`revision-20261016/codec-light/` in the issue30185 ledger.

## UPDATE ownership on base `ac55588b8d4`

The 12 existing patches rebase unchanged onto main `ac55588b8d4` (prior-PR
head `380740aac56`). This amendment selects async Datalink admission from the
modern UPDATE owner's final assignment map. An unrelated UPDATE carries its
old source without resolving it; explicit, automatic and generated source
assignments remain checked. Repeated writable physical aliases retain full
admission. Acquisition always checks its current resource policy. Datalink and
stage resolution reuse the already parsed URL, and the shared service parser
avoids CSV buffering only for a proven single unquoted record. No new cache,
worker, persistent state, identity grant or normal-query policy hook is added.

On base `ac55588b8d4`, normal tests pass all four owning packages (plan, Datalink,
stageutil, fileservice); focused race tests include the real final-assignment
producer. All 22 derived CPU packages pass vet, molint and configured incremental
golangci-lint; formatting, err-check and diff checks pass. GPU-tag-only owning
packages remain outside this CPU scope under the explicit native exception. All three public SQL scenarios pass normal/race: authorization,
tenant-stage CDC and two-CN persistence. The amended existing CDC fixture checks
two source columns and rows, status-only writes after source retargeting,
explicit same-value/repeated-alias denial with complete rollback, current-role
acquisition denial, repair and actual durable-tail/search/old-term removal.
Linux amd64's same tenant-stage CDC test also passes (96.56 seconds command).
The Darwin writer fixture derives its injected directory from the actual
writer's canonical filename; production behavior and failure assertions remain.

**Performance acceptance is incomplete; do not infer no regression.** The
complete Darwin cohort on main `c46b893f616` retained all 30 terminal passing
processes, 288 bulk measurements, 72 cell verifications and 24 ordinary reports.
[Every bulk pair and allocation median](sql-file-access-datalink-performance.csv)
is retained, including failed timing gates. Indexed 16-stage status UPDATE
allocation medians per 4096 rows were 178.09 MiB prior PR, 30.99 MiB main and
30.18 MiB candidate. These process-wide cumulative allocations show the intended
reduction; they are not retained heap or isolated validator allocations.

The same cohort does **not** pass the timing criteria: unindexed one-stage
source UPDATE had a paired median +27.54%, only 1/6 pairs within +10%, and other
timing gates failed. Ordinary pair 4 lacks a valid measurement window; its main
process took 2877 seconds with a recorded 1888-second observation gap. Unrelated
compilation also overlapped part of the bulk work. These facts invalidate
acceptance, without establishing that external load caused every slowdown.

Independent Linux validation rebuilt all three latest-base binaries against
one qualified native CPU generation. Both attempts were retained as
ENVIRONMENT_INVALID because unrelated Rust compilation was observed. Attempt 1's
qualification was stopped; attempt 2 stopped during its 60-second environment
observation. No Linux diagnostic/formal cohort was run, and neither attempt is
performance evidence. No other tasks/services were stopped and no replacement
samples were selected. A quiet measurement window is still needed to complete
the declared performance gates. GPU-native validation remains explicitly
excepted; CI results after publication remain separate.

The revision-qualified older results below are retained as historical evidence,
not relabeled as this amendment's performance acceptance. Current amendment raw
logs, accepted designs, all failed attempts and receipts are under the
issue30185 task ledger's `revision-20261016/` directory.

Issue #30185, PR #30212. Earlier broad validation snapshot:
`9961057360d29b7afab0fb6d0afc7a1a92c32055`.
Base: `461bc8e34920959e5fc141e3d1b37673a90894e1`.
Each measurement below retains its actual source revision.

## Classic admission and cleanup on base `eb18dbfa030`

This historical snapshot uses delivery base `eb18dbfa030`; main `8795f3699b4` reverted fulltext
planner change #30189. The obsolete test-call boolean amendment remains removed.
The admission amendment began on `83222fa8371`, including JSONLine vector-array
handling. The final delivery rebase adds only ADDTIME/SUBTIME fractional-precision
matching; the PR Go patches and test fingerprints remain unchanged.
Earlier measurements below retain their original source revisions.

Classic async admission now aggregates each checker expression. The checker
still evaluates every indexed value, but the nonstreaming executor retains one
result row with one fixed-width sum per indexed Datalink column. This bounds
**retained admission output**, not the whole scan, index build or process memory.
The additional aggregate computation applies to that preflight, with no change
to ordinary DQL/DML, the shared executor API or the existing resource owners.

The new regression reuses the existing stage/CDC cluster. It invokes production
`Hooks.HandleCreateIndex`, executes its unchanged generated query through the
real nonstreaming internal executor, inspects every result batch before closing
it and verifies sums against input cardinality. Empty, one-row NULL, 8193-row
NULL, valid remote, late forbidden second-column and canceled-request controls
have distinct oracles. The serial generate_series source's 8192+1 batch layout
is checked, avoiding an assumption about stored-table scan order. Public failed
CREATE also checks hidden-table, index-catalog and CDC-task rollback.

### Focused results for the classic admission amendment

The pre-fix regression fails because 8193 source rows retain 8193 result rows.
After the correction, empty, one-row and 8193-row sources each retain one row,
one batch and 32 allocated vector bytes for two checks. Each nonempty sum equals
its source row count; the late second-column refusal and cancellation register
no task. The new bounded-result subtest costs 0.10s normal / 0.52s race within
the reused fixture (no additional cluster).

The full classic-plugin owning package passes normal (1.195s) and race (1.549s).
The existing public `TestSQLFileAccessTenantStageCDC` with the added regressions
passes normal (87.640s) and race (93.620s). Derived from the complete delivery
scope, all 22 CPU packages pass vet, molint and incremental golangci-lint on
main `83222fa8371`. Formatting, `make err-check` and diff checks pass. Broader
unchanged evidence below is reused with its original revision and limits.

### CSV failed-output cleanup

A later complete review identified an additional CSV owner mismatch: both error
cleanup closures used Background and lost the policy required for reacquisition.
A new local pre-fix owner regression writes a real row using the original opener,
writer and closer, injects an error after the finished CSV exists, and confirms
that the failed output survives. An earlier invalid fixture run is retained and
excluded; its missing session pool was corrected before this reproduction.

The CSV owner now snapshots the file-operation policy once. Both cleanup callers
preserve that context; the helper detaches failed cancellation/deadline and adds
the existing 30-second budget before acquisition/deletion. This is a context-aware
I/O budget, not a hard interrupt for native syscalls. The original finalization
error is retained. Reacquired ETL is closed once; borrowed SHARED is not closed.

Direct and cached-stage regressions cover successful file preservation, late
finalization failure, cancellation and role change after writer completion,
plus refusal of a current public role despite a stale administrator context.
Helper controls cover expired deadlines, canceled parents, absent/nonprivileged
policy, live bounded deletion and borrowed-service ownership. The tests reuse
existing frontend fixtures and add no cluster. Broader unchanged validation
remains revision-qualified. Full frontend package normal (26.499s) and race
(35.981s) pass on the pre-rebase source. Delivery rebase validation repeats all
10 owner and 6 helper cases in normal/race and all 22 CPU static checks; the
whole-package results are reused for unchanged CSV behavior. The exact
commands, source hashes, terminal exits and logs are retained in the local
`csv-cleanup/` ledger; earlier public, provider and persistence evidence remains
valid only for its unchanged semantic inputs.

## Historical functional and static evidence

- Real MySQL tests `TestSQLFileAccessAuthorization`, `TestSQLFileAccessTenantStageCDC`
  and `TestSQLFileAccessPeerPersistence` pass normal (97.108s) and race (104.800s).
  They cover actual post-CREATE INSERT/UPDATE CDC-tail/search and old-term removal,
  remote stage repair, local-to-TEXT CDC, DEFAULT/source/view controls, binary
  rollback, local dump behavior, and actual two-CN pipeline execution.
- Those runs used `75721c07211` on main `501c85ecdd6`. The later backup correction
  preserves ETL normalization by passing `forETL=true`: its admitted arguments
  and behavior are equivalent. The rebase through `bb2b725d0af` changes only
  shard-cache updates; these scenarios use no sharded tables. The final rebase
  also includes fulltext ordering, JSON_KEYS and Starlark changes. A fresh final
  production-revision run passes all three real SQL tests (95.257s), including
  actual fulltext CDC/search and two-CN execution. Earlier public race evidence
  is reused for the unchanged ownership/cancellation behavior; it is not
  represented as a fresh final-head race run.
- Normal owning checks cover admission, process, CRT, stage, planner/scalars,
  external read/write, backup, table functions, frontend and compilation.
  The 13-package race group has twelve passing packages; fileservice fails only
  the baseline sync-dir case below. Arrow/Parquet retain cancellation and
  allocation-release oracles. The final backup correction passes focused
  fileservice/backup normal and race checks, preserving each factory's HDFS mode.
- Derived 22-package CPU vet, molint and incremental golangci-lint pass again
  on the final base, including the test-call correction described below.
  `make err-check`, gofmt and diff checks pass. Molint exit success does not
  assert that all existing informational diagnostics disappeared.
- Owned temporary files, fake credentials and intercepted requests independently
  test factory default refusal, raw/parsed consistency, no ambient credentials,
  initial/refresh role credentials, cancellation, errors without fallback, and
  the existing explicitly anonymous MinIO mode. HDFS parsing/selector controls
  do not claim a live HDFS deployment test.
- Earlier Java BVT remains recorded at its original revisions: 1744 comparisons
  across six repeated stage/fulltext cases, followed by 74 comparisons for the
  async-source amendment and cleanup postconditions. These are historical proofs;
  the provider amendment is checked through real SQL normal/race controls above.

`TestLocalWriterCloseCleanup/etl/sync_dir` expects an injected error but receives
nil on Darwin. The exact race selection independently fails on clean main
`501c85ecdd6` (3.191s). Latest main changes no fileservice/native inputs. Full
fileservice suites are **not claimed green**; no assertion was weakened or skipped.

Then-current main `461bc8e3492` also omitted the new `orderOverwritten` argument in one
`tryApplyCoveredFulltext2` test call. Clean main fails to compile the planner
tests. The earlier amendment supplied `false`, matching the original fixture behavior
and the other direct calls; production fulltext code and assertions are unchanged.
After that correction, full planner normal/race runs still failed
`TestCoveredFulltext2ScoreLimitOnBoundSQL/score_alias` and `score_alias_offset`:
the expected search candidate LIMIT is absent. A clean-main comparison with an
overlay containing **only** the same compile-call correction reproduces both
failures. The final upstream rewrite removes the inner implicit sort, while
the existing LIMIT helper requires that sort. These are recorded upstream
failures at that revision, not green planner suites; no expectation was removed to hide them.
The four focused planner tests for async final binding, external-file admission,
Hive admission before listing and the corrected equivalent-sort fixture pass
both normal and race on that recorded base.

## Ordinary SQL performance

[Per-run data](sql-file-access-performance.csv) includes all 34 reports, actual
source/binary identities, dataset digests, errors and retained raw-log hashes.
The A/A arms both identify the same main source and binary.

Darwin ARM64, 10 logical CPUs, 16 GiB, Go 1.27.1, GOMAXPROCS=8, without race.
Every run starts an independent real CN/TN/logservice cluster and a non-system
sysbench tenant over MySQL TCP. Four tables have 10000 deterministic rows each,
primary/secondary indexes and matching complete initial data digests. Each case
checks cardinality; final cleanup checks no residual tables. Formal measurements
run without other tests or compilation.

Standard sysbench point-select, indexed-update and read/write SQL is retained.
BEGIN/COMMIT use text because prepared BEGIN is unsupported; other statements
retain their chosen mode. One-second warmup is excluded. P99 is successful
client event latency from per-thread monotonic microsecond histograms, since
the local stock histogram is empty; mixed workloads measure whole transactions.
The table uses medians of paired relative changes, not ratios of independent
medians. Positive TPS is faster; positive P99 is slower. All samples are retained.

Three alternating pairs, twelve measured seconds per case, compare main
`501c85ecdd6` with `75721c07211` (24 reports):

| Workload | PS mode | Threads | Paired TPS change | Paired P99 change |
| --- | --- | ---: | ---: | ---: |
| oltp_point_select | disable | 32 | +14.02% | -21.85% |
| oltp_read_write | auto | 32 | +15.21% | -58.91% |
| oltp_read_write | disable | 8 | +9.97% | -8.62% |
| oltp_update_index | auto | 8 | -6.28% | +18.77% |

A fresh-cluster, update-only confirmation uses thirty measured seconds, three
pairs, main `bb2b725d0af` versus production `7b2f2571999`:

- Paired TPS changes: -13.60%, -0.14%, -1.35%; median **-1.35%**.
- Paired P99 changes: +55.06%, +6.34%, -2.01%; median **+6.34%**.
- Independent medians: 817.73/806.65 TPS and 16.241/16.158 ms P99 (base/candidate).
  Those ratios answer a different question from paired relative changes.

A/A controls run the **same baseline binary** for both labels, with fresh data,
thirty measured seconds and two pairs:

- Binary SHA256: `d14a80180426be273ee1657dc6cc70b5baf3323ad1d579bbe5ad8704ec2337bf`.
- Paired TPS changes: +1.68%, +2.12%.
- Paired P99 changes: +3.16%, +2.46%.

All 34 valid reports have zero SQL errors/reconnects; all sixteen test processes
exit successfully. An initial overlay invocation selected no tests and was
rejected, excluded from those counts. Rebuilt probe binaries have explicit
nonempty selection receipts. Temporary diagnostic source files are removed.

The longer confirmation does not reproduce the initial slowdown's magnitude.
A/A also varies with identical code. This does **not** attribute every A/B
difference to host load, establish statistical equivalence, or exclude a small
regression. Paired P99 remains +6.34% in the isolated confirmation. The shared
machine and two/three pairs limit the conclusion; no zero-overhead or production
P99 guarantee is made. File policy/provider construction does not enter ordinary
SQL; the complete PR still changes bounded identity refresh and indexed-DML
metadata work. The measured residual differences remain visible.

## Retained artifact-transport evidence

The artifact/parameter owners are semantically unchanged by later admission and
backup amendments. Matched main `c789d870a5d4` / PR `b9f720f4ef45` runs cover
concurrency 1/4/8, multiple 8 MiB batches, 65536-byte chunks and packed frames:
168 transport runs, 96 native fulltext/HNSW CREATE/reindex operations and twelve
foreground competition reports, with content/search validation and zero
frontend SQL errors. Large random transport paired timing medians improve
4.31%-11.24%, while cumulative Go allocations increase about 11%-14%.

Earlier vector-transport sampling, including transaction/cache/background state,
found old/new peak RSS 2846/2621 MiB and mpool 638/366 MiB at eight concurrent
writers. The 8 MiB ceiling bounds pending parameters, **not** total transaction,
heap, mpool or RSS. These are prior-revision observations, not a fresh cloud or
final-head native performance run; ordinary SQL was measured separately above.

## Review and limits

The complete range is mapped by ownership. Previously reviewed unchanged
closures are reused; new implementation, test and document hunks are reviewed
separately. Design records are pinned in `sql-file-access-review.md`. No blanket
internal identity, missing-context trusted fallback, policy store, worker or CI
change remains. The source-owner inventory supplements real runtime tests and
is not a proof about arbitrary future Go code. Earlier source reviews found no new confirmed implementation blocker; the
current amendment's incomplete performance gate is stated above. Reproduced
baseline failures and historical performance limits remain explicit. This record does not assert merge readiness or green CI.

GPU-native CAGRA/IVF-PQ compilation/runtime remains unverified under the user's
explicit exception. ONNX and live cloud/HDFS deployment execution are not proved
by CPU/local fixtures. Update all SQL-serving CNs; older CNs do not enforce this
contract. Remote CI is a separate result after push.

Raw prompts/model provenance, source hashes, command/exit receipts, histograms,
failed attempts and host samples remain in the local issue30185 ledger under
`cdc-closure/`, `oltp/` and `index-perf/`; samples were not filtered by outcome.
