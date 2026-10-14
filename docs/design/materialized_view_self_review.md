# Experimental materialized-view delivery review

Review date: 2026-10-13. Scope is the local task delta against explicit task-start
HEAD `dd38098853ea197699ed3fac3e69dc53975ac061` on branch `work`:
33 modified tracked files and 32 new files, no staged changes. No commit, push or
PR was created. The branch contains substantial pre-existing committed work;
it is excluded from this delivery scope. Remote default-branch freshness was
not re-established, so this is not a review of the entire branch for merging.

The user approved design revision 2 with “去实现吧” and subsequently requested
additional validation cases. The implementation follows the experimental P1
full-refresh scope; P2 rewrite and P3 incremental refresh remain separate.
StarRocks reference provenance is recorded in the source manifest.

## Change and risk map

| Closure / files | Invariant, owner and consumers | Risk and evidence |
|---|---|---|
| SQL admission: parser grammar/keywords/generated parser, new tree AST, plan materialized_view, build/query/constraint/DDL/DML/show/clone hooks | Distinct read-only relation; deterministic bounded native-table query; dependency capture before optimization/expansion; persistent identity; gate/protocol admission. Consumers are native CTAS relation creation, SELECT/DML/DDL binders and compile. | R2/R3: parser/full suite and plan contract/creation/catalog tests; real deferred/empty/NULL/join/duplicate/unsupported query tests. Generated parser derives from grammar; generation reported zero conflicts. |
| Catalog/protocol/upgrade: new catalog definition/tests, defines const, bootstrap upgrade and v4_0_15 | Account-qualified global metadata, immutable format-1 definition, stable physical/logical IDs and schema version. Creation/bootstrap and restart readers agree; protocol 110 required. | R3 compatibility: catalog decode/DDL tests, upgrade handler tests, fresh native initialization, tenant initialization and physical restart. Mixed-version migration/downgrade not executed. |
| Frontend authorization/dispatch: materialized_view, authenticate, executor/self_handle/status/stmt_kind/variables, snapshot/PITR guards | Owner/admin writes, MV plus source SELECT for reads, live definer identity/role/ACL for tasks, account isolation, no active user transaction; unsupported logical clone/restore reject. DROP failure preserves session DB. | R3 security: privilege/global ownership/transaction tests; real revoked-source, owner, SHOW and two-account cases; failed DROP regression. Logical restore rejection is source-reviewed, not an end-to-end restore claim. |
| Refresh transaction: mv service, compile materialized_view and DDL/scan/DML/scheduler/sql_executor hooks, defines capability, executor parser mode | Intent precedes publication; stored data, coverage and success history commit in one SI transaction. Exact account/table/transaction capability; current-CN writes; source identity and snapshot checked; sequence/epoch fencing. | R3 state/concurrency: failure injection at transaction boundaries, snapshot drift, stale task, lost commit response, transient retry, exact capability and nested statement tests. Native overflow rollback, restart, concurrent readers and two-CN publishers passed. |
| Scheduler/lifecycle: mv task, CN service/task wiring, task proto/generated code | Shared periodic reconciliation; stable run identity; queue identity fits existing VARCHAR(50); bounded retries/deadlines; live definer admission and stale epoch handling; bounded history pages. Existing CN stopper owns cron registration. | R3 liveness/recovery: exact task/reconcile/failure race repeats, full mv suite, native scheduled refresh, terminal two-CN history. Large-scale scheduler and process crash at commit remain release validation gaps. Generated task enum aligned with proto. |
| Resource control: defines output budget, insert/multi_update writers and tests | Fixed admission buckets/capacity; cancellation releases every acquired slot; two active refreshes/CN; shared 512 MiB inserted-batch quota charged on local and S3 paths. Every successful executor result is closed. | R3 bounds/resources: admission/counter/writer tests and race repeats, memory-pool cleanup assertions, failure cleanup deadline. Quota bounds output batches, not total query working memory or storage capacity. |
| Public validation/delivery: test/materialized_view, RFC, reference manifest, case inventory, behavior/performance results, this review | Independent numeric/NULL oracle, same-source full-result comparison, native server rejection rather than transport failure, scoped fixtures, reproducible paired measurements and honest phase boundaries. | R2/R3: 83 native SQL checks, 50 alternating pairs per measured workload, EXPLAIN and raw observations retained, harness vet/build, formatting and diff checks. B04/B06–B08 remain unexecuted. |

Correctness, consumer contracts, compatibility/security, performance/operations,
platform/delivery and test architecture apply to all executable closures above.
State/concurrency, liveness, resource lifecycle and boundedness apply to refresh,
catalog fencing, scheduler and writer closures and are covered by their listed
proofs. Pure documentation has no runtime owner, wait, allocation or wire format;
its applicable lenses are evidence accuracy, provenance and delivery.

## Ownership, waiting and growth

- Q1: refresh owns its cancellation, acquired admission slots and executor
  results; defers release on success and rejection. Native transaction execution
  owns commit/rollback. Diagnostic cleanup has a separate bounded context and
  does not publish data. CN stopper owns registration retries.
- Q2: account/global admission waits select cancellation; refresh/task execution
  has a 300-second deadline, state reads 30 seconds, diagnostic cleanup 5 seconds,
  and registration attempts 30 seconds. Transaction retry is at most three
  attempts. No new unbounded application wait or SQL-originated capability exists.
- Q3: 256 fixed account buckets, two pending per bucket, two active refreshes/CN,
  128 views/account, 16 sources, 256 columns, 64 KiB query and 512 MiB output-batch
  quota. Discovery pages have 128 rows; history visits 16 views and at most two
  128-ID pages each. The 100-run/7-day history policy is periodic retention,
  not an instantaneous global cardinality bound.

Cross-CN publication linearizes at the metadata/data transaction commit. A
conflicting attempt retries or fails with native w-w conflict; a superseded
sequence cannot replace a newer publication. Source IDs and schema versions
prevent rebinding a recreated name. Schedule epochs and stable background run
IDs prevent stale-policy publication and duplicate task replay.

## Validated evidence and decision

- `delivery-focused.log`: 22 top-level tests, 65 subcases, ten packages passed.
- `owning-delivery.log`: catalog, mv, v4_0_15, MySQL parser, insert and multi_update
  full package suites passed after the runtime fixes.
- Delivery race JSON: each of task identity, reconciliation identity and refresh
  failure boundaries ran separately with `-race -count=100` and an anchored exact
  test name. Earlier counter/admission/timezone race evidence remains valid.
- Incremental production vet (`vet-delivery.log`) and harness vet passed.
  Native service and SQL harness built. Formatting and `git diff --check` passed.
  Configured golangci-lint was unavailable; no full-lint or full-repository test
  success is asserted.
- Native behavior: 66 main checks, three graceful restart checks, five overflow
  failure/cleanup checks and nine two-CN checks passed. Task-owned SQL fixtures
  were removed. Case-level evidence and original-file hashes are checked in.
- Performance: B01/B02/B03/B05 each retained 50 paired observations, verified all
  result rows and saved EXPLAIN. Timing was single-CN; topology tests were run
  afterwards. The median correction preserves original observations and records
  the method. High-cardinality results show little improvement.

Local experimental P1 implementation and its documented validation are complete.
There is no observed unresolved failure in the executed scope. A production
release/merge recommendation remains pending mixed-version migration/downgrade,
crash-at-commit, cold-cache/tail and larger scheduler validation, and configured
lint/full release checks. These gates are explicit, not treated as passing or
waived. Logical MV clone/restore, historical reads, branch-aware refresh,
incremental maintenance and automatic rewrite remain outside P1.

The runtime production evidence uses the final production semantic inputs;
subsequent edits add harness topology checks and documentation only. Build/test
logs are retained in `/workspace/mv-build`. The checked-in execution report and
JSON provide the durable case and measurement record.

## Publication review — 2026-10-13

This follow-up review uses the fresh explicit PR base
`3e50e1b32d8da61bbbf70c89989fb08b671809de`, verified by fetching origin/main.
The original HEAD is its ancestor (64 upstream commits); all original feature
changes were applied in a separate worktree. The publication delta is 66 files:
33 modified existing files and 33 additions, including one new publication
evidence JSON. The map above covers the same behavioral closures.

The three integration conflicts preserve upstream CDC runtime initialization,
variadic DROP lifecycle admission and the planner's three-result table binding.
Protocol 110/111 are preserved; MV authoring/reading now requires 112. An extra
test rejects authoring floor 111. Generated parser output was regenerated from
the merged grammar. No unrelated upstream commits are included in the PR delta.

Final new-base evidence:

- 22 focused top-level tests / 66 subcases and the six owning package suites
  passed; the complete mv suite passed again after the context isolation fix.
- Native library, service and validation runner built. Configured incremental
  golangci-lint 2.14.0 passed with zero issues across the changed package closure.
  It found missing complete stream/error/cleanup checks in the runner; these
  were fixed and rerun, including prepared-result equality and typed cancellation.
- An escaped source name under NO_BACKSLASH_ESCAPES failed before the fix and
  passed after it. Metadata keeps its default parser/literal rules; persisted
  source SQL alone inherits authoring mode/timezone. The three captured-context
  subcases passed 100 exact-name race repetitions after the fix.
- 84 native SQL checks passed: 67 main, 3 graceful restart, 5 overflow/cleanup,
  9 two-CN. Fixture cleanup completed. Their original hashes, case lists and raw
  result documents are in `materialized_view_publication_results.json`.
- Publication timing samples ran alongside lint/race build work and are retained
  as diagnostics. The isolated prototype measurement artifact is not overwritten
  or promoted to new-base measurement evidence.

The local implementation/evidence blockers found in publication preparation are
closed. Issue #30079 and the Draft PR are the review vehicle for the experimental
P1 scope. The production release gates listed above remain pending; draft
publication is not production approval, a full CI pass or closure of P1.1/P2/P3.
The initial review's statements about an unpublished worktree and unavailable
lint refer to that earlier review; this section supersedes them for publication.

Published implementation source commit:
`f9cf6f1bd638d482e4361d7272cde94488fe98d2`. Draft implementation PR:
[#30085](https://github.com/matrixorigin/matrixone/pull/30085), linked to project
tracker [#30079](https://github.com/matrixorigin/matrixone/issues/30079).
Head repository is `dengn/matrixone`, branch
`feat/materialized-views-p1-20261013`; base is `matrixorigin/matrixone:main`.
The upstream repository rejected direct ref creation under its repository rules,
so publication follows CONTRIBUTING.md through the existing user fork.
The local link/provenance update changes documentation only. After PR creation,
the remote branch merged newer main commits `175a645a85` and `e5111936b9` via
`e2ba50650e`; this upstream integration is preserved. Original runtime/lint/race
evidence remains pinned to the verified implementation source above. Current-head
integration/CI evidence is separate and is not inferred from those earlier runs.
Post-merge focused MV tests passed in the parser, planner, frontend and compiler
packages (`post-merge-focused.log`). This verifies the affected local integration
closure; it is not a rerun of the earlier complete native SQL/performance matrix.

## CI correction review — 2026-10-13

The original full CI failed at commit `417811f5949dc04c9bd6d74d624bce2b852506c2`.
Its failures are covered by [the CI triage record](materialized_view_ci_triage.md).
The review closure is restore ownership (R3), pessimistic source publication
and DDL admission (R3), cron lifecycle/retry (R2), upgrade/port fixture consumers
(R2), and license/exact catalog result fixtures (R0/R1).

Native pessimistic source-name locks are retained through MV publication.
Dependencies are checked after the existing DDL admission frontier, preserving
Git4Data and index waiting policies. Optimistic publication still uses its
conflict fence. Cluster restore now consumes the same explicit catalog skip
policy as bulk copy; the typed negative control still drops a user cluster
table. MV and lineage registration have independent stopper-owned retries.
Existing cancellation tests exercise their shared retry implementation.

Focused coverage reproductions, embedded cluster counterexamples, five owning
package suites, license headers, configured incremental lint and molint passed.
The three cron cancellation/retry tests each passed 20 race-enabled repeats
(60 top-level passes, no data races). The targeted native BVT run passed all 12 failed files (2,741 executed
statements); 67 main MV checks and nine cross-CN checks passed. New full CI must
be reported separately at the corrected head. Draft status and the previously documented production-release
proof gaps remain in force; no CI green or new speedup is inferred from local
checks.


The follow-up SCA failure at `7b0d6f04a7` was the Makefile's independent error
constructor policy, after the complete license check passed. Catalog metadata
errors now use `moerr`, with a typed error-class oracle. The full catalog suite,
`make err-check`, and configured catalog lint passed. Earlier success-path,
restore/admission, and cron evidence is unchanged; the new full CI is pending.


The complete CI at `955c3f787a` passed build, arm64 SCA, full coverage UT and
PROXY BVT. Pessimistic BVT had only three missing-catalog-row result mismatches
in two additional fixtures. All eight full inventory fixtures were audited;
the two corrected files passed complete isolated mo-tester runs. This is an
R0 fixture/documentation follow-up with unchanged source behavior. Prior code
and concurrency evidence is reused; full fixture-head CI and the still-queued
ordinary UT remain explicitly pending.


Main merge review: `a321a75afc` + `35c8dd5854` (2026-10-13). Both text conflicts
were resolved from their owning contracts: regenerate the parser from combined
grammar; retain main's cohort port-range admission. Auto-merged frontend, planner
and compile hooks preserve MV dispatch, authorization and single-CN refresh.
The main-side JSON EXPLAIN protocol mismatch was reproduced at the clean source
baseline and fixed through the existing typed index-search/plugin boundary.
All prior distinct renderer oracles are retained using current protocol fields,
with additional malformed metadata, fulltext and opaque-options controls. Six
owning suites, 11 gossip fixture tests, seven-package configured lint and vet,
and the error-constructor policy passed. Molint exited 0 with two advisory
recover diagnostics on unchanged, checked main-side helper bodies. No new shared
state, wait, persistence format or native dependency was introduced; prior MV
race/restart proof remains valid for its unchanged contracts. Delivery review
has no unresolved blockers for this merge correction. Complete merge-head CI
and the previously recorded Draft production-release gaps remain separate.


Continuation review (2026-10-14): exact integration inputs `cb53485ffe` + main
`bcd4cb99e7`; ordinary compatibility/fixture corrections, approved MV design
revision unchanged. R2 renderer/grammar closure adopts main's producer/hook/BVT
contract without removing prior oracles. R3 protocol admission is closed across
planner, frontend, compile, upgrade and latest-version consumers with 112/113
negative/control cases. R1 NULLIF and private-ledger fixtures preserve stronger
complete-state/serialization/resource oracles. R3 topology proof includes the
actual Push-mode commit handoff; it makes no global-freshness claim. Created
private generation/ledger cleanup is registered immediately and occurs after
operators/scopes finish; cleanup does not abort before releasing resources.
Eight package suites, 30 repeated root cases, refreshed configured lint/vet,
137 native BVT statements, 67 main MV checks and 180 causal two-CN checks pass.
No unresolved blocker for this correction; current-head full CI and the existing
Draft production-release gates remain separately tracked.

Final main alignment at `f02952f7bf9c3294d5cb7876a07888d2380d48a9`: clean
merge, complete compile and plan suites pass, configured lint reports zero
issues. Both MV gates and main's ordinary-view admission are retained. One
redundant, identical empty-index early return is removed; the retained return
has the same no-op contract and owning-suite evidence is semantically valid.
Native evidence reuse is bounded to the unchanged non-ODKU/non-view-replacement
paths, as recorded in the CI triage document. No unresolved merge blocker.
