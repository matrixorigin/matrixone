# Public Sirius acceptance campaign

Design version: 3. Owners: MatrixOne public SQL, compile/CN evidence and
`optools` campaign tooling. Tracks MO #28968 and parent #28966.

The user approved this design before implementation on 2026-10-09 and selected
one complete D PR, combining runner, lifecycle tests and full campaign evidence.
The approved implementation sequence remains #29690. C #29757 merged as af5a75c5f9. D starts from authoritative main containing
that merge and retains merged Sirius #29 at 908ffc75. The user approved this
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
and rejected by the user; D stays draft until the whole closure passes.

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
execution gates are C merge, provisioned immutable datasets/valid baselines and
terminal acceptance evidence, not permission to infer success. Implementation
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
are siblings; only the worker enters exclusive subreaping. CN restarts occur
only at stream-profile changes, retaining committed immutable datasets.
Defaults: 6 GiB GPU, 16 GiB native host, existing 64 MiB windows, one active plus
sixteen waiting queries, one-hour query and three-hour profiling deadlines.

Readiness requires all exact/public/cleanup/resource/sanitizer/performance gates,
matched Native A/B/current baselines and required CI/reviewer approval. The
known cuDF pinned-pool and CUB reports remain to be reconciled. D stays draft
until that proof is terminal. E/F and Docker work remain excluded.
