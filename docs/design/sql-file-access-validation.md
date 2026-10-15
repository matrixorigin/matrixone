# SQL file access validation record

Issue #30185, PR #30212. Earlier broad validation snapshot:
`9961057360d29b7afab0fb6d0afc7a1a92c32055`.
Base: `461bc8e34920959e5fc141e3d1b37673a90894e1`.
Each measurement below retains its actual source revision.

## Latest amendment and rebase

Current main: `83222fa8371`; the prior main `8795f3699b4` reverted fulltext
planner change #30189. The obsolete test-call boolean amendment remains removed.
The latest upstream delta fixes JSONLine vector-array handling; this amendment
changes only classic async FULLTEXT Datalink admission and its regression proof.
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

### Current focused results

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
is not a proof about arbitrary future Go code. No new confirmed implementation
blocker remains for PR update; the reproduced baseline failures and performance
limits remain explicit. This record does not assert merge readiness or green CI.

GPU-native CAGRA/IVF-PQ compilation/runtime remains unverified under the user's
explicit exception. ONNX and live cloud/HDFS deployment execution are not proved
by CPU/local fixtures. Update all SQL-serving CNs; older CNs do not enforce this
contract. Remote CI is a separate result after push.

Raw prompts/model provenance, source hashes, command/exit receipts, histograms,
failed attempts and host samples remain in the local issue30185 ledger under
`cdc-closure/`, `oltp/` and `index-perf/`; samples were not filtered by outcome.
