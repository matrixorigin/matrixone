# Index health measurements and suggestions

Owner: #30346; follows the read-only statistics work in #32698.
Decision date: 2026-11-17. The user authorized implementing the reviewer's follow-up
and updating the design. This document fixes the previously open API and policy
choices before implementation; recommendations are diagnostic heuristics.

## Contract

`mo_index_stats(database, table, index)` keeps three literal arguments and its four
columns. IVF summary metrics gain growth, training-sample adequacy, sampled
new/old drift and `suggestions`. Fulltext2 summaries gain tail and dead-base
measurements and suggestions. Key indexes and classic fulltext keep their scope.
No query performs maintenance or persists its result.

`mo_index_analyze(database, table, index [, options])` is the explicit IVF deep
check. It has the same columns and privilege/source checks. `options` is a JSON
literal with `sample_count` (default 100, range 1..256), `recall_target` (default
0.95, range (0,1]) and `retrain` (default false). Unknown options are errors.
It adds exact-neighbor bucket-coverage curves and optional sampled retraining.
The independent entry preserves the normal entry and makes full-source work
explicit; an implicit full scan or fourth argument on the normal entry was
rejected. Unsupported metrics return an explanatory analysis status.

Summary fields added by this extension:

| Field | Meaning |
| --- | --- |
| `growth_ratio` | Source non-null-vector growth since this generation's build. |
| `training_sample_ratio` | Actual training samples / (30 × configured lists). |
| `baseline_status` | Missing, invalid, unsupported-format or stale baseline; omitted when usable. |
| `drift.sample_count`, `new_sample_count`, `old_sample_count` | Actual reservoir and historical membership counts. |
| `drift.new_old_residual_ratio` | Sampled new-cohort MSE / old-cohort MSE. |
| `drift.new_rows_in_largest_5pct_buckets_ratio` | Fraction of sampled new keys in the largest ceil(5% × K) buckets. |
| `drift.approximate` | Present for reconstructed quantized entry vectors. |
| `analysis.recall` | `kind:centroid_coverage`, actual query count, k=10, target, `(nprobe,recall)` curve, optional new/old curves and smallest evaluated `recommended_probe_limit` meeting target. |
| `analysis.retrain` | Sample count, iteration limit, `approximate:true` and current/retrained `sse_ratio`; present only on request. |
| `tail_chunk_count`, `tail_frame_count`, `tail_size_bytes` | Fulltext2 CDC-tail stored rows, distinct frame IDs and serialized payload bytes. |
| `base_dead_ratio` | Fulltext2 max(0, 1 − source rows/base documents); absent without a base. |
| `suggestions` | Summary-only array of `{action,reason,params?,variable?,value?}`; observing it never performs the action. |

Unavailable component metrics are omitted and accompanied by `status` in that
component. `analysis.cohort_status` explains unavailable build-snapshot controls;
coverage without cohort comparisons may still be computed. A source-count or
fulltext maintenance read failure preserves unrelated statistics and adds
`health_status`.

## Build metadata and compatibility

The existing IVF key/value metadata table gains one optional `diagnostic_build`
record. Its JSON contains `format:1`, `index_version`, `build_ts` (full physical-
logical snapshot timestamp), `nrow` (the build's source non-null-vector count),
`train_nsample` (actual valid training vectors), `source_table_id` and
`historical` (whether the build source had a committed historical view).
The centroid builder writes it in the same transaction as that generation;
CREATE, REINDEX and asynchronous force-builds share this writer. Empty/untrained
builds do not claim a trained baseline. Sampling limits and transaction snapshot
are recorded from the actual build, not the DDL submission time.

Readers accept only a supported format and matching index version. Missing,
malformed, stale or future records make baseline-dependent values unknown;
search, updates and existing statistics do not require the record. Source-ID
changes also disable historical cohort identification. No catalog migration or
mandatory rebuild is required. Shutdown upgrade from existing metadata is
supported; mixed-version operation and downgrade are not product requirements.
Version binding also prevents accidentally reusing a leftover record after a
writer that did not refresh it rebuilt the index. Backup/restore retains the
optional record through the normal hidden-table path.

Source growth compares the same source count as the build (`COUNT(vector)`),
not an async entry count with a source count. It is `(N-nrow)/nrow`; zero baseline
count makes it unknown. Training coverage is `train_nsample/(30*lists)`.

## Cheap drift

The existing IVF scan maintains a bounded deterministic reservoir of 100 entry
keys, centroid IDs and decoded vectors, in addition to its exact counts and
geometry. A single historical PK-only lookup classifies these keys by existence
at `build_ts`; no historical vector join or unbounded key set is retained.
"New" means a currently indexed key absent at the build snapshot; updates of an
existing key belong to the old cohort. Reused keys have the same identity rule.

The summary's `drift` object reports actual sample/cohort counts, the ratio of
new to old mean squared residual, and the fraction of sampled new keys in the
largest ceil(5%*K) current buckets. Ratios are estimates, not population counts.
Quantized samples inherit reconstruction uncertainty. Missing retained history,
zero old residual, absent cohorts, untrained centers and numerical/size limits
have explicit statuses; unavailable values are never replaced with zero.
Historical membership is optional and a failed lookup preserves other metrics.
A retained timestamp is not guaranteed merely because the baseline is retained.

## Deep analysis

Read a bounded uniform sample of original source vectors; prioritize available
new keys found in that sample while retaining old-query controls. This bounded
lookup does not enumerate all post-build keys: rare new data can be absent from
the reservoir, and zero new-query count makes that comparison unavailable.
Stream the source once and maintain
exact top-10 neighbors per query, excluding the query key itself. Retrieve only
the resulting neighbor keys' indexed centroid IDs. Rank each neighbor bucket
among the query's centroids and report coverage for nprobe 1,2,4,...,K.
This is `centroid_coverage`, not replayed ANN recall: quantized ranking loss and
other search pruning are not measured. Missing indexed neighbors invalidate the
coverage claim. Async analysis requires a durable watermark covering the same
source read view; an unavailable historical system observation makes the deep
check unavailable, rather than attributing maintenance lag to bad centroids.

Optional retraining uses the existing CPU k-means implementation with the same
K, a bounded source sample, ten iterations and one worker. Report current /
retrained SSE on that sample, actual sample count and approximation status.
It neither writes centroids nor changes the index. The ratio estimates sampled
geometric improvement, not latency or production recall improvement.

Source sampling reuses the bounded streaming reservoir; SQL does not retain a
vector/key top-N before admission. Vector dimension limits the requested reservoir
size, reserving half the 64 MiB ceiling for keys and training. Sampled retained
state, neighbor heaps and training workspace each have a 64 MiB admission ceiling
(excluding SQL executor batches and the existing centroid state). Retraining
requires at
least K sampled vectors and uses at most 4096; otherwise that component reports
a resource/sample limit. Cancellation propagates through streaming and training;
all batches, iterators and native allocations have lexical cleanup owners.
Sampling and exact-neighbor streaming each scan the source. Exact-neighbor CPU
cost is O(S*N*d), centroid coverage O(S*K*d), and default drift
adds bounded O(S*d) retained state plus a historical PK read. No background task,
cache, retry service or result-history table is introduced.

## Suggestion policy

Suggestions appear only on summary rows, with action, reason and optional params
or session variable/value. An empty array means no actionable suggestion from
available measurements. Unknown measurements do not imply a healthy index.
At most one REINDEX suggestion is emitted, in priority order:

- Lists outside [sqrt(N)/2,2*sqrt(N)], or lists=1 with N>1: suggest ceil(sqrt(N)),
  bounded to [1,N]. Empty tables and no-op parameter changes produce no advice.
- Training sample below 30*lists: suggest retraining when the current source has
  enough rows to meet that target; otherwise expose the limitation only.
- Deep coverage below target around max(1,K/8), or new-query coverage at least
  ten percentage points below old queries with at least ten queries per cohort:
  suggest REINDEX. Also report the smallest evaluated probe count meeting target.
- Optional sampled retraining with current/retrained SSE >=1.2: suggest REINDEX.

SET probe_limit suggestions describe the measured current generation and must be
re-evaluated after REINDEX. Drift ratios are evidence; they have no invented
universal rebuild threshold. Existing center mismatch retains its fixed-partition
meaning and never masquerades as recall.

Fulltext2 uses its idxcron owner's tail gate and rebuild/merge decision. Report
stored tail chunks, tail frames, serialized tail payload bytes and estimated
base dead fraction. The threshold is the owner's configured value (default
1024); mode is REBUILD when the owner's dead-base rule exceeds 30%, otherwise
MERGE. Do not invent a pending-document count for frames storing nrow=0.
Scheduling intervals and auto-update enablement remain scheduler concerns.

## Validation boundaries

Pure tests cover baseline decode/generation mismatch, policy boundaries, bounded
sampling, cohort ratios, exact-neighbor/self exclusion, bucket coverage, and
cancellation/resource limits. Extend the existing SQL fixture for create/update/
REINDEX baseline publication, legacy metadata, privileges, new-cluster diagnosis,
explicit deep opt-in and fulltext suggestions. Persisted JSON round trips and
absent/stale records prove the additive upgrade contract; failures must not
publish a new baseline without its matching generation. Existing statistics,
quantization and cache lifetime oracles remain in force.

A baseline also records whether the build source had a committed historical
view. Transaction-local source writes disable historical cohort comparisons;
their pre-commit SnapshotTS cannot describe those rows. Counts and training
coverage remain usable. Deep source sampling and historical membership retain
only bounded keys/vectors; they never join a whole source-vector side.
