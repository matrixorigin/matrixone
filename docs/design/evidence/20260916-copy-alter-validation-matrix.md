# COPY ALTER validation matrix

## Revision 4 design phase (2026-10-15)

Current production subject: `f7903a01aa528bb05b5662d6b2bdd859cf4212f5`.
Inspected main: `8795f3699b40504376343fa1acda8b4619f0c76e`.
The PR remains Draft, conflicting and CHANGES_REQUESTED. The
[revision-4 protocol](../20260911-copy-alter-publication.md) is proposed;
[maintainer approval](../20260911-copy-alter-publication-review.md) is PENDING.
This documentation update changes no executable code or runner. Production
repair and conflict resolution follow that explicit decision.

The old retained broad admission still makes the historical split unreachable
for ordinary pessimistic RC at the production subject. No present optimization
or exact-new-head acceptance is claimed. The following assertions are prepared
obligations, not tests already authored or executed.

| Requirement | Executable evidence required after approval | Current state |
| --- | --- | --- |
| Real fast-path reachability | Real query observer: narrow shared admission, one copy/transaction, complete exclusive-attempt observation yielding zero | NOT_RUN |
| Independent-table preparation | Pause A; B prepares and commits before release. Separate two-preparation rendezvous, bounded cleanup on every exit | NOT_RUN |
| Same-table exclusion | Deterministic source-lock wait plus correct final data/definition | NOT_RUN |
| Pre-copy broad fallback | Inject changed identity/protection; zero copies before rollback, distinct next transaction, at most one broad retry under original deadline | NOT_RUN |
| Failure/receipt lifecycle | Cancel and failure at preparation/replacement/commit boundaries; source intact, no temporary/task residue, stale/wrong actor/transaction/target receipt rejected | NOT_RUN |
| Compatibility | Query/text/binary/owned-executor entries; explicit/SI/optimistic/complex/View-enabled paths remain broad; historical owner/restore/GC and multi-CN controls | NOT_RUN |
| Canonical SQL correctness | `test/distributed/cases/ddl/alter_copy_publication.sql/.result`, intentional extension if needed; preserve Snapshot/View/FK/allocator/rollback oracles | NOT_RUN on repaired head |
| Ubuntu race budget | Non-empty per-test `-race -json`, every new/modified top-level case <=45.55s, verified again in exact-head CI | NOT_RUN |
| Formal A/B | Frozen main vs repaired head, same rebuilt toolchain/native/driver/config; 1/2/4/8 workers, three pairs each, B/H–H/B–B/H | NOT_RUN |
| Performance thresholds | Median paired TPS >=0.90 and P50 <=1.10 at 1 worker; TPS >1 and P95 <1 at each 2/4/8; all failures and per-pair ratios retained | NOT_RUN |
| Fixed reconstructed nightly | Three admitted TKE runs, pinned inputs, 100 tables, ADD/DROP 50+50 users, 5min, only 1068/1091 expected; original topology and independent storage cleanup | NOT_RUN |
| CI / reviewer / QA acceptance | Required checks at repaired head; separate replies to all five threads, implementation re-review and QA decision | PENDING |

The original full acceptance matrix is retained. Revision 3's 1/2-only scope
proposal is withdrawn. No result below waives a missing revision-4 requirement.

## Reusable Ubuntu evidence, not current-head acceptance

The completed personal Ubuntu run `c105-20261015T081332Z-a9617d` used base
`8a4c84f4516d5098de9eef50b8afecc7524f37a2` and head
`5be67e3694712bc208e42c8db4c6032a2d896f65`. Twelve full-size 1/2-worker arms
completed 720 requests, plus eight smoke requests. Recorded independent
verification establishes terminal worker/watchdog, cgroup, port and alias
cleanup and exact source/binary/native/driver/profile identities for that run.
Its old exclusive-gate position assertions do not match revision 4 and must be
replaced, not disabled or relabeled. No old performance percentage transfers
to the present PR. Rebuild using the Go version required by both frozen sources
(currently 1.27.1), add 4/8-worker arms and require new same-generation smoke.
The runner is not ready to launch revision 4 until those changes are frozen
and their contracts pass. See the [runbook](20260916-copy-alter-performance-runbook.md).

The user has already accepted the fixed nightly reconstruction; no new approval
of that reconstruction is requested. Pin the new subject/image separately,
retain the immutable historical input lock and report missing historical
receipts explicitly. Current topology/timeout/runtime evidence still requires
actual execution and does not fit the personal Ubuntu A/B footprint.

## Historical record (superseded for current delivery)

Candidate: `d1fdf00bf6b315b06a6133d2cce07c91af52da3f`
Target baseline used for local builds: `upstream/main`
(`8a4c84f4516d5098de9eef50b8afecc7524f37a2`). That historical snapshot recorded Ready; it does not describe current Draft state.

The status in this file is evidence status, not a claim that GitHub review or
deployment QA has approved the change.

| Requirement | Command or artifact | Status | Evidence boundary |
| --- | --- | --- | --- |
| Different-table and same-table production regressions | `mo-cgo-test -count=1 -run '^TestIssue28319' ./pkg/tests/issues` | PASS | Full issue-28319 selection passed in 28.658s before the perf-only counter-format commit; the production code is unchanged by that commit. |
| Split refresh terminal matrix | `mo-cgo-test -race -json -count=1 -run '^TestIssue28319CopyAlterRefreshTerminal(InMemory\|ObjectBacked)' ./pkg/tests/issues` | PASS | All 12 top-level tests passed at `e5748ddd31`; longest terminal event was 11.32s. The final commit only changes the build-tagged performance driver. |
| Individual race repetition | Per-test adaptive commands under `/tmp/28418-race-focused-e5748` | PARTIAL | Commit, catalog-refresh, old-dropped and renamed cases completed; later repetitions were stopped after disk exhaustion and an external cluster-admission holder. The tasks-published repetition reached the cleanup ALTER and timed out at 120s under high repetition; it is not marked PASS. |
| Owning package normal/race | `mo-cgo-test ./pkg/sql/compile`, `./pkg/frontend`, and corresponding `-race` runs | PASS | Normal and race package runs passed on the same production content; the build-tagged driver is excluded. |
| CGo build/vet | `go build` and `go vet` with MatrixOne CGo link flags | PASS | Both commands passed before the perf-only counter-format commit; no production package changed afterward. |
| Performance driver compilation | `mo-cgo-test -tags issue28319_perf -run '^$' ./pkg/tests/issues` | PASS | Driver v2 compiles and the test is discoverable as `TestIssue28319Performance`. |
| Candidate screening, 2 workers, 8192 rows, 80 ALTERs | Four embedded modes (`query`, `text`, `binary`, `executor`), driver v2 | PASS (candidate screening) | Exact final-SHA artifacts under `/tmp/28418-perf-final-*-d1fdf`; each has 80 records, 80 `copy_started`, 80 `copy_completed`, 0 errors and `source_revision=d1fdf00bf6`. These are not a baseline/head comparison. |
| Candidate single-worker screening | Embedded `query`, 1 worker, 8192 rows, 40 ALTERs | SCREENING PASS | Exact final-SHA artifact `/tmp/28418-perf-final-query-1w-d1fdf`; 40 records, 40 starts/completions, 0 errors. The paired performance gate remains pending. |
| Paired baseline/head A/B | Same tool, data, build flags and isolated directories; 1 and representative 2 workers, three alternating rounds | NOT_RUN | No isolated performance environment and no final baseline artifact were available in this turn. |
| Original nightly ADD/DROP workload | Original runner revision, parameters and error contract | BLOCKED_ENVIRONMENT | The exact runner/data/timeout identity is not recoverable from the local workspace; reconstructed 8192-row screening is not an exact nightly replay. |
| Current-head BVT | `alter_copy_publication.sql/.result` | NOT_RUN | Historical `de39bce1a5` evidence was 31/31; the new head must receive a fresh CI/BVT result. |
| Maintainer design decision | Revision 3 scope proposal | PENDING | The conditional handoff is not a maintainer approval. The lock order and acceptance-scope proposal are recorded in the design and review documents. |
| Deployment QA | QA handoff below | BLOCKED_ENVIRONMENT | Requires an isolated deployment, topology, workload owner and explicit QA assignee. |

## Candidate screening summaries

The four final driver-v2 screening runs use `d1fdf00bf6` and are retained as
candidate diagnostics; they must not be described as formal A/B results.

| Entry | Workers | Records | Copy starts/completions | Errors | P50 / P95 / max |
| --- | ---: | ---: | ---: | ---: | ---: |
| query | 2 | 80 | 80 / 80 | 0 | 112.2 ms / 147.6 ms / 241.1 ms |
| text prepared | 2 | 80 | 80 / 80 | 0 | 181.8 ms / 365.5 ms / 669.0 ms |
| binary prepared | 2 | 80 | 80 / 80 | 0 | 167.2 ms / 688.7 ms / 1255.7 ms |
| independent executor | 2 | 80 | 80 / 80 | 0 | 178.3 ms / 387.6 ms / 438.4 ms |
| query | 1 | 40 | 40 / 40 | 0 | 61.0 ms / 81.8 ms / 92.4 ms |

The timing spread is a reason to require a paired baseline; it is not evidence
of a regression or an improvement by itself. The driver records configured
row size only as workload metadata and marks physical row/byte counters
`UNAVAILABLE` until a production observer can provide them.
