# SQL file access validation record

Issue #30185, PR #30212. Production revision: `7b2f25719998a08391f89ca5269b8d4bb5e1ba28`.
Base: `bb2b725d0af4601d02ebf9cc68b1816a2b300022`.

## Functional and static evidence

- Real MySQL tests `TestSQLFileAccessAuthorization`, `TestSQLFileAccessTenantStageCDC`
  and `TestSQLFileAccessPeerPersistence` pass normal (97.108s) and race (104.800s).
  They cover actual post-CREATE INSERT/UPDATE CDC-tail/search and old-term removal,
  remote stage repair, local-to-TEXT CDC, DEFAULT/source/view controls, binary
  rollback, local dump behavior, and actual two-CN pipeline execution.
- Those runs used `75721c07211` on main `501c85ecdd6`. The later backup correction
  preserves ETL normalization by passing `forETL=true`: its admitted arguments
  and behavior are equivalent. The latest main changes only shard-cache updates;
  these scenarios use no sharded tables. Range-diff confirms unchanged patches.
  The public evidence is explicitly reused, not presented as a fresh final-head run.
- Normal owning checks cover admission, process, CRT, stage, planner/scalars,
  external read/write, backup, table functions, frontend and compilation.
  The 13-package race group has twelve passing packages; fileservice fails only
  the baseline sync-dir case below. Arrow/Parquet retain cancellation and
  allocation-release oracles. The final backup correction passes focused
  fileservice/backup normal and race checks, preserving each factory's HDFS mode.
- Derived 22-package CPU vet, molint and incremental golangci-lint pass; the
  subsequent three-package and final fileservice deltas pass those checks too.
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

## Ordinary SQL performance

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
is not a proof about arbitrary future Go code. No confirmed implementation
blocker remains for PR update; performance claims retain the stated limits.

GPU-native CAGRA/IVF-PQ compilation/runtime remains unverified under the user's
explicit exception. ONNX and live cloud/HDFS deployment execution are not proved
by CPU/local fixtures. Update all SQL-serving CNs; older CNs do not enforce this
contract. Remote CI is a separate result after push.

Raw prompts/model provenance, source hashes, command/exit receipts, histograms,
failed attempts and host samples remain in the local issue30185 ledger under
`cdc-closure/`, `oltp/` and `index-perf/`; samples were not filtered by outcome.
