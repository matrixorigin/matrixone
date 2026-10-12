# Public Sirius acceptance campaign

Design version: 3. Owners: MatrixOne public SQL, compile/CN evidence and
`optools` campaign tooling. Tracks MO #28968 and parent #28966.

The user approved this design before implementation on 2026-10-09 and selected
one complete D PR, combining runner, lifecycle tests and full campaign evidence.
The approved implementation sequence remains #29690. C #29757 merged as af5a75c5f9. D starts from authoritative main containing
that merge. Sirius #30 merged as 955b6881 on 2026-10-11; D now pins that merged
native source for the guarded release SDK. The user approved this
implementation revision on 2026-10-11 after selecting one complete D PR and
separate allocation profiling without a native observability prerequisite. E/F and Docker changes are excluded.

## Contract and choice

For every admitted campaign cell, the immutable dataset, canonical SQL, public
result/error metadata and exact values must agree between native MO and embedded
MO-reader execution. An embedded result additionally requires matching query
identity, real completed GPU work, exact capability/profile, no fallback and
healthy terminal cleanup. The negation is any descriptor/value/NULL/error drift,
misattributed telemetry, incomplete schedule, missing required evidence or
unbounded retained state. No benchmark SQL rewrite, float coercion, unchecked
narrowing or skipped required query can satisfy the contract.

Version 1 of `optools/sirius_parity.py` requires five routes including embedded
TAE, materializes rows in runner JSON and retains complete results. The selected
version 2 requires native MO and embedded MO only, uses bounded transient spools,
and retains safe summaries. Leave version-1 historical records/tests readable;
an explicitly versioned v2 input selects the new contract. Extending the legacy
TAE route would violate the approved MO-reader scope. Keeping rows in memory
would violate result-size bounds. Splitting runner and acceptance PRs was offered
and rejected by the user. D stays draft while implementation or required local
evidence is incomplete; final acceptance also requires CI and reviewer approval.

## Routes, data and schedule

Required routes are `mo-native` (ordinary public SQL) and `embedded-mo` (the same
SQL with the existing explicit GPU hint). Optional Flight profiles declare their
equivalent supported query coverage at preflight. Unsupported/unavailable
comparisons are N/A. A later execution failure cannot become an unsupported
query, and partial coverage is never reported as a full-suite total. Version 2
has no embedded-TAE route or gate. Legacy v1 artifacts retain their old meaning.

Freeze a manifest per SF1/SF10 dataset containing generator/source-file hashes,
DDL/table schema, row counts and a dataset digest. Use decimal TPC-H quantities
and the existing canonical Q1-Q22 SQL; do not replace these with the planner's
legacy integer-quantity DDL. Record SQL hashes, exact MO/Sirius/SDK revisions,
compiler/Pixi/CUDA/driver versions, hardware, resource limits and session settings.
Sidecar provenance is required only for declared Flight profiles. Dataset writes
are prohibited during the campaign; ordinary MO snapshot/GC ownership remains.
Flight-TAE comparisons, when supplied, retain their separate launcher-verified
TN no-GC contract and are explicitly labeled historical-reader comparisons.

For each scale, run Q1-Q22 with streams 1 and 4 as correctness controls, then
streams 2 with one excluded warm-up and five measured suites, followed by ten
separate Q9 repetitions. Rotate route order deterministically; never overlap GPU
benchmarks. The required two-route schedule has 744 executions across both scales.
Keep stream controls outside performance samples and distinguish cold/warm runs.

## Executor, interfaces and ownership

A Go runner uses the existing MySQL driver and consumes one immutable RunSpec
through the existing `MO_SIRIUS_CAMPAIGN_RUN` interface. It produces one bounded
JSON descriptor, not a JSON array of rows. Configuration/secrets are supplied by
private configuration files, never echoed into diagnostics or shell arguments.
The controller owns an isolated service workspace, PID identities and shutdown.
A separate campaign worker owns runner descendants through the existing exclusive
Linux subreaper cleanup. Long-lived services are siblings of that worker, so a
failed runner cannot reap the controller's services. Keep services alive through
warm-up/measured suites and restart embedded CN only for stream-profile changes.

Public rows and ColumnTypes provide the independent native-MO oracle. Obtain
physical OID/precision/scale/nullability through the native frontend's existing
bound-result-column owner, with bounded default-off query-local schema evidence.
Match it to embedded verified output descriptors; do not infer physical decimal
width from precision. Evidence contains account/query identity, descriptors and
schema digest, with no raw headings or values. This adds no result protocol field,
SQL endpoint, persisted format, global result registry or new numeric ABI layout.

The v2 result descriptor carries complete schema, row count, spool-relative name,
byte count and digest. The worker accepts only its own regular files beneath the
private scratch root and rejects traversal, symlinks, duplicate ownership,
truncation, digest mismatch and trailing data. One owner releases each spool after
its oracle pair validates, and on every exception/cancel/timeout path. Partial
phase workers cannot publish a complete acceptance result; final schedule
validation owns the full-campaign verdict.

Encode decimal coefficients/scale exactly without floating conversion, preserve
integer signedness and NULLs, and validate empty results with their schema. Use
at most 32 MiB comparator memory and 4 GiB total scratch, with directory mode
0700/files 0600. Quota exhaustion is a failure. Compare streaming ordered rows;
ties may be compared as multisets within their SQL ordering peer group, without
adding SQL tiebreakers. Different LIMIT-boundary membership fails exact parity.
Unordered results use bounded external comparison and retain duplicate counts.
Floating tolerance is disabled by default and applies only to explicitly
approved approximate columns. Validation never relies solely on digests.

Wall time begins immediately before query dispatch and ends after full result
draining/streaming serialization. First-row time is observed on the first actual
row; empty results record absence. Setup, telemetry collection, sorting and
comparison are excluded. Record backend timing and PID-scoped CPU measurements
separately. Fetch actual query identity and drain late SQL errors before accepting
success; producer cleanup must not replace the numeric error owner.

## Resources, failure and artifacts

Keep 64 MiB input/result windows, one active native query and sixteen waiters.
Existing leases, task resources, result callbacks and joined close paths own all
transfers. Do not substitute charged-byte counters for measured memory peaks.
Collect existing allocator/pool observations and PID-scoped host/CPU/GPU sampling,
recording source, units, interval and query/process/lifetime scope. Missing required
evidence blocks acceptance; GPU/pinned fields are N/A only for native MO. Compare
query-owned counters and warmed allocated-pool/process baselines after cleanup.

Stop on the first malformed output, missing/misattributed event, public mismatch,
unexpected cancellation or unhealthy cleanup. Preserve current fail-fast behavior,
empty-output-directory preflight and fixed sanitized `failure.json`. Retain only
`campaign.json`, sanitized `runs.jsonl`, schema/count/digest summaries, timing
tables, lifecycle/resource evidence, baseline results and provenance. Remove raw
row spools. Failure cleanup closes SQL handles, stops the owned runner process
group, joins descendants and closes only controller-owned services; unknown
quiescence is fatal and cannot be labeled healthy reuse.

## Validation and readiness

Extend existing tests for typed/schema drift, 0/1/2-row results, duplicates,
spool boundaries/corruption, quota exhaustion, wrong identity, duplicate/missing
events, late errors, timeouts, private-output ownership and partial schedules.
Reuse existing public/native/bridge fixtures before adding expensive setup.
Map each behavior to the cheapest independent oracle and keep benchmark cost out
of ordinary unit-test assertions.

Lifecycle evidence includes paused consumption, blocked input/result credit,
cancel before admission and during work, writer failure, queue saturation,
operation-owned errors, ordinary healthy reuse and fatal quarantine. Use observable
barriers and existing fault seams, with zero retained query credit after close.
Reuse semantically valid native proofs and produce current-consumer evidence for
any changed boundary. Run affected owning suites and applicable race/static gates.

Publish complete Q1-Q22 medians/sums, separately labeled median suite times,
Q9 results and raw repetitions. Enforce the existing 10% numeric regression
policy with fresh matched baselines: Native A for historical scalar regressions
(especially checked-add64), Native B for aggregates, identical shared case IDs,
operands, toolchain/hardware and timing boundaries. Missing required baseline
evidence means incomplete acceptance. Preserve the Flight+MO ratio limit 1.0
where equivalent coverage exists. No performance waiver is implied.

Reconcile warmed process/pool and sanitizer baselines, including the existing
41,386,248-byte cuDF pinned-pool report and generic CUB diagnostic. Unresolved
value, lifecycle/resource, sanitizer or performance gates keep D draft. Default
cutover and Flight retirement remain E/F under a separate recovery-readiness
design; a nil/unready lease manager is never proof of no old consumers.

Design review: PASS for the approved contract and ownership model. Remaining
execution gates are the merged native prerequisite and terminal release acceptance
evidence. C is merged; immutable SF1/SF10 inputs and matched numeric baselines
are now provisioned and verified. Implementation
deviations require this document and affected decisions to be reviewed again.

## Revision 3 implementation decisions

The 744 timing executions remain unprofiled. A separate resource schedule runs
132 native/embedded pairs (Q1-Q22, SF1/SF10, streams 1/2/4), plus explicitly
excluded warmups. Nsight Systems captures CUDA allocation/free records from CN
startup; SQLite memKind classifies pinned/device allocations. Existing tagged
pool observations and PID/Go allocator sampling remain separate sources.
Profiling never supplies benchmark timings and adds no numeric ABI or native API.

The session-only mo_sirius_campaign_evidence boolean defaults off. It emits
bounded executed-generation OID/precision/scale/nullability and the shared
Sirius schema digest, without names or values. The public runner retrieves
last_query_id() on the same physical connection after full drain and matches
exactly one event by account, query UUID and service generation.

The Go public runner consumes MO_SIRIUS_CAMPAIGN_RUN and private configuration,
streams typed NDJSON rows to 0600 relative spools beneath a 0700 scratch root,
and returns only bounded metadata/count/bytes/digest. Rows are bounded to 1 MiB;
comparison uses 32 MiB working memory and 4 GiB total comparison scratch.
V1 dispatch, routes, artifacts and tests retain their meaning.

The controller owns persistent separate LOG/TN/CN roles. The worker and services
are siblings. Both the worker and controller enter exclusive subreaping; the
outer controller joins descendants orphaned by a failed worker. CN restarts occur
only at stream-profile changes, retaining committed immutable datasets.
Defaults: 6 GiB GPU, 16 GiB native host, existing 64 MiB windows, one active plus
sixteen waiting queries, one-hour query and three-hour profiling deadlines.

Merge readiness requires all exact/public/cleanup/resource/sanitizer/performance gates,
matched Native A/B/current baselines and required CI/reviewer approval. The
known cuDF pinned-pool and CUB reports require matched controls and steady
allocation evidence. Complete implementation and local evidence permit marking
D ready for review. Required CI and reviewer approval gate final acceptance and
merge. E/F and Docker work remain excluded.

## Implementation and execution

Version 2 requires Python 3.11 or newer and the public Go consumer at
`cmd/mo-sirius-campaign`. `optools/sirius_campaign.py serve` creates separate
LOG/TN/CN roles in a private workspace for loading. Preserve that workspace's
configuration and durable data across subsequent runs. Provision each scale
with `optools/sirius_dataset.py` and the runner's `load` command. The load report
checks each source CSV and compares the loaded table's typed row multiset with
the generator; a matching row count alone is insufficient.

Use `sirius_campaign.py freeze` with the source root, packaged service, runner,
dataset directory and both load reports to capture actual source/binary/SDK,
hardware and toolchain identities. Credentials remain in private `runner.json`.
`run` consumes that frozen provenance and an empty output directory; add
`--resource` for the separate allocation campaign. Run serially, without
concurrent builds, GPU tests or dataset writes. The controller owns service
shutdown and deletes invocation-owned row spools and raw trace/log workspaces.
Its retained artifacts include safe run records, CSV repetitions, Markdown
timing tables and resource summaries.

For diagnosis after an interrupted resource run, `--resource --resource-phases
5 6` runs only SF10 streams 4 and 2, with their original schedule identities.
Phases 1..6 are SF1 streams 1/4/2 followed by SF10 streams 1/4/2. This mode
publishes `resource-diagnostic.json` with `partial: true`, `complete: false` and
`diagnostic-runs.jsonl`, after joined cleanup. It never writes a complete campaign
summary or timing verdict, even when all six phases are selected. Preserve each
capture's actual provenance; diagnostic or mixed-source captures cannot replace
the uniform complete release campaign.

Each resource configuration runs an excluded complete warmup suite and a final
Q1 cleanup probe. Existing native tagged pool snapshots remain distinct from
Nsight physical allocation/free events and PID sampling. Native pool peak tags
are lifetime high-water marks; they are never presented as query-only peaks.
Nsight exports normalize timestamps to the Unix epoch before matching bounded
runner intervals. Allocation tracing must start with CN and report complete
CUDA collection. Native MO GPU/pinned query measurements remain N/A.

The owned CN admits one GPU query at a time. MO's public UUID and the native
engine's independently minted UUID are distinct; complete, consecutive native
window accounting attributes existing pool tags by admission order. QueryEnd
is logged before the embedded engine/plan destruction. Preserve that snapshot;
when it retains GPU bytes, require the actual successor window's idle begin to
show zero allocated/reserved bytes after public drain/close. Never relabel the
earlier snapshot as zero or accept an unidentified successor.

Nsight launches through a private PID/start-generation record written before
exec. The controller also owns an outer subreaper, so it joins escaped runner
descendants if the inner worker dies. It publishes full campaign artifacts only
after all owned roles and descendants quiesce. Exception diagnostics include
only a fixed stage, sequence and repository source line, with no error body.
Allocation capture disables optional all-API and device-event tracing. Private
profile workspaces have a separate 48 GiB cap, including profiler intermediates;
each exported SQLite trace is bounded to 16 GiB and parsed with an 8 MiB cache.
The live allocation map holds at most 65,536 generations in memory, then spills
exact acquisition/free matching to a private SQLite ledger with an 8 MiB cache
and a strict 4 GiB page quota. Duplicate generations and unmatched frees remain errors.
SQLite's event ordering also uses temporary sorting files; see its
[sorting implementation](https://github.com/sqlite/sqlite/blob/master/src/vdbesort.c).
The exclusive parser disables memory mapping and worker threads, uses private
temporary storage, limits address space to 1 GiB and each sorting file to 8 GiB,
and restores the environment, limits and signal handler after success or failure.
Reserve the two sorting files and the ledger against the invocation's 48 GiB
quota. Retain safe per-phase query envelopes before deleting each joined raw
phase workspace so completed traces cannot accumulate across configurations.
Report generation and export have the bounded three-hour profiling deadline;
SF10 intermediates and conversion take longer than the small SF1 preflight.
The first SF1 capture contained 2.55 million memory events and a 717 MiB SQLite
export; the separate trace bounds allow the required tenfold scale. Go's direct
`exit_group` skips native atexit finalizers and leaves Nsight's CUDA trace
incomplete. Allocation profiling runs the unchanged packaged service under the
bounded GDB exit wrapper in `optools/sirius_profile_exit.gdb`. It validates the
linux/amd64 runtime exit instruction sequence, preserves nonzero exits, and
invokes libc exit after successful Go shutdown so native finalizers flush CUDA.
This is profiling-only; the 744 timing executions remain uninstrumented. The
wrapper participates in the frozen collector hash. Nsight discards environment
snapshots. The physical scope covers CN startup through process destruction.
Reject CUDA/CUPTI completeness warnings for that PID or global scope;
unrelated traced child processes cannot invalidate PID-scoped measurements.
The result/comparison scratch quota remains 4 GiB. Unprovisioned Flight profiles
are declared N/A before execution.

The 64 MiB native input windows apply to each binding; aggregate input credit
across several bindings can exceed 64 MiB. Bindings are limited to sixteen.
Shared CTE execution revealed that the native importer inlines reference
relations while MO input queues are destructive. The compiler now gives each
inlined occurrence an independent reader on the same statement snapshot,
without changing canonical SQL or Flight's shared-step behavior. Expansion has
node, depth and reader bounds and preserves the caller's plan. Sirius #30 also
preserves retained VARCHAR identity casts and rejects reused/unreachable MO
inputs before native work starts. The release pin remains merged code; unmerged
development validation is explicitly labeled and cannot satisfy final acceptance.

`optools/sirius_acceptance.py` combines complete timing/resource records with
actual lifecycle, sanitizer and matched numeric baseline evidence. Missing,
partial, changed or misattributed evidence fails closed. Passing local gates
sets `local_acceptance_complete`; required CI and reviewer approval still gate
final acceptance. The current implementation does not infer those decisions
from a local test or an incomplete run.

Early consumer failures cancel the SQL query before closing unread rows, so the
MySQL driver's close cannot keep draining a blocked producer until the query
deadline. Cancellation affects only that query's child context; the retained
failure still includes its original value/writer/quota and observed close errors.
Acceptance crosslinks allocation baselines to the actual measured/probe snapshots
and checks unique, ordered CUDA spans. Matched sanitizer classifications retain
raw exit status 99 and the exact candidate/control error and leak counts; extra
findings or duplicate controls fail acceptance.

Retained summaries reconnect each mandatory and declared optional route with its
native-MO oracle cell. Cardinality, physical descriptors and schema identity must
agree after spools retire. Raw spool hashes need not agree for unordered rows or
ordered peers; the live full-row comparator remains the value oracle. The final
artifact command checks the archive's recorded `checksums.json`, including every
consumed input, and parses the same bytes it hashed. A missing, changed or
redirected artifact fails before acceptance. `--checksums` can select the manifest
explicitly; its default is the timing directory's parent.

The controller also checks the whole workspace during target/readiness waits,
profiler finalization, export and shutdown. The exporter uses the existing owned
Service process abstraction, with the same three-hour finalization/export bound
and regular quota checks. A quota or deadline failure kills and joins that owner;
adopted descendants still join through the outer controller. Failed cleanup does
not restore admission or publish complete evidence. Small live-writer tests
exercise quota growth during finalization/export and preserve the original
reduced-quota counterexamples.

## Bounded input batching integration

Sirius #31 merged as `b953bb5a87788e17557929987208b3a0068d933b`.
This follow-up pins that merged revision and rebuilds the guarded release SDK.
Native input now collects toward the existing 32 MiB expanded target, flushing
on EOS, source bounds or producer credit pressure. Both readiness and direct
claims enforce the policy; immutable size/string/NULL summaries remain charged
to the 64 MiB input window. Public ABI and exact numeric semantics are preserved.

The user approved the batching-plus-MO-integration plan with EOS/pressure-only
flushing on 2026-10-12. Premerge development validation uses MO base `64b7a43852`,
Sirius `955b6881` versus `585536bf`, the same frozen provider and immutable
datasets, two streams, one excluded warmup and five measured repetitions. Q1 GPU
medians improve from 0.775129 to 0.185689 seconds at SF1 and from 9.296148 to
2.217461 seconds at SF10; median input units fall from 600 to 12 and 7,264 to 117.
All 22 SF1 public comparisons pass exactly. These targeted development results
retain their identities and do not replace the original D release records.

Merged-source release validation requires the complete 744-execution timing and
correctness schedule plus the separate 264-execution resource schedule, existing
numeric/lifecycle/sanitizer gates and a fresh matched full-suite regression limit
of 5%. Generate captures outside Git and preserve their actual source/binary/SDK
identities. E and numeric execution changes remain separate; no sidecar repository
retirement is required.

The merged-source release capture uses MO `81db83e921` on base `080365f013`,
Sirius `b953bb5a` and guarded SDK
`4f2235b64347a2642dbad1328de2f031cfbfc401ce114ed55bafcb0e68802ee6`.
All 744 timing/correctness executions and the separate 264 resource executions
pass, with six zero-growth SF1/SF10 stream 1/2/4 allocation profiles. A fresh
744-execution old-native campaign uses the same MO base and semantic inputs.

| GPU median full suite | Old pin `955b6881` | Merged pin `b953bb5a` |
| --- | ---: | ---: |
| SF1 | 4.216747 s | 2.537705 s |
| SF10 | 49.884309 s | 28.475628 s |

Median suites and sums of query medians pass the 5% regression limit. Q1 medians
fall from 0.876390 to 0.230886 seconds at SF1 and from 9.446339 to 2.359481 seconds
at SF10; median input units fall from 670 to 12 and 7,258 to 117. GPU full-suite
execution remains slower than native MO on this RTX 3070.

Final validation closes all ten lifecycle cases, fresh production numeric ABI
and 96 MiB result ownership checks, public runner/compiler normal and race
cleanup checks, and actual GPU data/cancellation plus cuVS/Sirius coexistence.
Native control normal/ASan+UBSan+LSan/TSan runs pass 91 cases/750 assertions;
their actual `585536bf` execution revision is retained and qualified by complete
native-tree equality with merged `b953bb5a`. Fresh GPU memory checking passes
30 cases/34,509 assertions. Four raw sanitizer statuses remain 99, reconciled
only against the exact Native A CUB and no-GPU-work cuDF pinned-pool controls
and all six current steady profiles. Fresh matched numeric cases each retain
21 samples; the maximum ratio is 1.021216 against the 1.1 limit.

Linked native test builds refreshed a receipt-covered static archive. The real
SDK target and guarded consumer build regenerate validation SDK `f65b21f4`,
with identical native source, compilers and explicit link arguments. Reports
retain this actual consumer execution context separately from the preserved
measured service/runtime bundle and SDK `4f2235b6`; no capture is relabeled.
The artifact acceptance command reports local completion, with required CI and
reviewer approval remaining. Generated captures and raw repetitions stay outside
Git; the original D archive below retains its original measurements.

The delivery sync includes authoritative main `11914526ba` (MEDIUMINT,
prepared-parameter, SHOW GRANTS, window and Parquet fixes). Canonical SQL,
native bridge/lowering, reader ownership, public campaign evidence and
collectors are unchanged. The new type/protocol branches concern MEDIUMINT or
prepared parameters; the immutable campaign uses ordinary column domains and
text-protocol queries. All eight affected SQL owning suites and all 22 actual
native preparations pass after this sync. The distributed-IVF protocol fixture
now explicitly selects its required centroid route through `gpu_mode=0`,
preserving all supported/old/unknown/cancellation assertions. Its earlier
GPU-default failure reproduced on the old-native baseline. This test correction
changes no production dispatch or captured measurement.

## Release validation record

The generated development and release captures are preserved in contributor-fork
tag `evidence/sirius-d-4230ce3d14`, at commit
`4230ce3d143e77a8b4be83feb47bd26858a8d459`. The
[immutable release evidence tree](https://github.com/aunjgr/matrixone/tree/4230ce3d143e77a8b4be83feb47bd26858a8d459/docs/design/evidence/sirius-campaign-release)
contains the original observations, provenance, numeric samples, lifecycle and
sanitizer proofs, source qualifications and review-fix evidence. These generated
records are outside the current source tree; application code and regression
tests use no checked-in campaign captures. Earlier history remains intact.

The measured source is MO `9814347c1ce38ec12f51412ef3176ede1501574d`, merged Sirius
`955b6881afa6e92d0995f43e3fe9c383d86981c0`, and guarded release SDK
`29bca11a537abf00d481581a423023591dbabda9b0fdd73d06c4a697e8ce01ce`.
All 744 unprofiled timing and 264 resource executions passed; six allocation
profiles had zero unexplained warmed growth and all ten numeric ratios passed
1.1 (maximum 1.026527). Raw sanitizer status 99 is preserved with matched Native A
CUB and no-GPU-work 41,386,248-byte cuDF pinned-resource controls. Median native/GPU
suite times were 1.804130/4.789524 seconds at SF1 and 27.947777/52.826967 at SF10;
the GPU route was slower overall on the recorded RTX 3070. Original contexts,
binary hashes, PIDs and measurements are preserved. Earlier captures do not
retrospectively certify the later continuous disk-quota monitor.

To verify the original archive with the current validator, fetch the preservation
tag and export into a temporary workspace; the source checkout need not change:

```sh
git fetch https://github.com/aunjgr/matrixone.git refs/tags/evidence/sirius-d-4230ce3d14
task_evidence_dir=$(mktemp -d)
git archive --format=tar 4230ce3d143e77a8b4be83feb47bd26858a8d459 \
  docs/design/evidence/sirius-campaign-release > "$task_evidence_dir/release-evidence.tar"
printf '%s  %s\n' \
  28cfb19a161bc89aa917bf9add42e496b425a3b71f63399670d6daa43cfb269e \
  "$task_evidence_dir/release-evidence.tar" | sha256sum -c -
tar -xf "$task_evidence_dir/release-evidence.tar" -C "$task_evidence_dir"
task_evidence_root="$task_evidence_dir/docs/design/evidence/sirius-campaign-release"
python3 optools/sirius_acceptance.py \
  --timing "$task_evidence_root/timing" --resource "$task_evidence_root/resource" \
  --allocations "$task_evidence_root/allocations.json" \
  --lifecycle "$task_evidence_root/lifecycle.json" \
  --sanitizer "$task_evidence_root/sanitizer.json" \
  --baselines "$task_evidence_root/numeric-baselines.json"
```

The archive SHA-256 is fixed above; its internal `checksums.json` has SHA-256
`98cc3821567fd995c8f129a2370e85308cb179940efc8fb49df86d7f11dfa7bb`.
The unchanged CLI verifies every listed artifact and returns
`local_acceptance_complete: true`. CI and reviewer approval remain separate final
acceptance and merge gates; an archived local result does not infer them.
