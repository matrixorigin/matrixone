# DROP DATABASE: reuse lifecycle admission without reordering cleanup

Design revision: **3**, 2026-09-26. **Supersedes revision 2 (REQUEST_CHANGES). Grouped branch reclaim and up-front lifecycle admission are withdrawn. Pure multi-DROP batching remains rejected as the fix.** The exact revision-3 document (SHA256 `2e048fa5acdaf51ab64388c7001db8ced67a32b813d621321e65e8daab70ad6f`) passed independent design review (SHA256 `6c2dbcf555ae25db467c5091320ef78d0d312287d3b9af2cb5a331ede88e4496`). Implementation checkpoint: `6b095210e78`.

Issue: https://github.com/matrixorigin/matrixone/issues/27575. Implementation PR: pending. This document records the approved P3 design; P3 remains a partial fix because the quiet 1,000-table and busy-system performance gates have not passed.

Inspected worktree/base: `/tmp/mo-issue-27575`, `9c79a6edfcad5ce9635d91b0f1646d0bde375cc3`, branch `codex/issue-27575-drop-database-serial`. Designer owns only this artifact and changed no tracked source. Source/GitHub reads are this agent's evidence; SQL measurements were supplied by the parent and require its raw evidence bundle.

Requested model/effort: GPT-6 Astra, xhigh. Actual invocation identity is supplied by the parent spawn record; this agent has no independent model-introspection API and requested no substitution. Skills applied: mo-dev and its design, validation, testing and index-plugin references.

## 1. Revised decision

Use one ordered DROP TABLE coordinator for single-table, public multi-table and internal database-drop invocations. It owns one **call-local admission boolean**, initially false. The per-table primitive requests lifecycle admission at its exact existing location after live temporary/no-op resolution and before persistent database/relation locks. The first successful request executes the existing barrier and marks admission; later members reuse it. The primitive retains every other operation, including branch reclaim/compaction **at its current per-table location before partition cleanup and before the next member**.

DROP DATABASE supplies contiguous groups of at most 32 names through the existing internal multi-DROP SQL path. It keeps its own earlier lifecycle gate because that protects database/FK/view work before table dispatch. Replace the database's quadratic hidden-name membership scan with an exact-name set. No new execution option, database-only fast path, fallback, ancestor-gate cache, transaction-global state, worker, service API or persistent format is introduced.

The hypothesis is that eliminating N repeated writes to the same lifecycle row materially reduces growing workspace cost. Existing branch probes, CCPR probes and other per-table cleanup remain. SQL grouping alone failed the measured control; only the combined candidate with eliminated lifecycle writes is proposed. It must meet the unchanged 1,000/2,000-table end-to-end gate or it is not a delivered issue fix.

This is the minimum coherent ownership refactor after review rejected broader regrouping. G-FEATURE-DESIGN remains applicable through material catalog hot-path/lifecycle ownership impact; the independent reviewer must approve this exact version before implementation. R3 proof covers transaction/callback effects, public ordering and performance. Delete replaced code rather than retaining a second path; make no unrelated CLONE, timeout, callback-framework or engine/workspace changes.

## 2. Evidence and limits

E1: The [issue](https://github.com/matrixorigin/matrixone/issues/27575) reports one nightly occurrence on `4.2-dev` `92db870eecaccc6ceea6b42f004170d14daaa6ab`: DROP DATABASE after five minutes of table churn lasted about two hours and ended with ERROR 2013. The 10,000-name workload bounds potential live tables but statement-attempt counters do not measure them. Original cluster logs/profiles are gone. Neither regression status nor the disconnecting component is proven.

E2: [The production comment](https://github.com/matrixorigin/matrixone/issues/27575#issuecomment-5836653456) reports visible TN relation drops in about 24 ms, preceded by CN latency. It has no end-to-end duration/live count. It supports CN investigation, not a quantitative attribution of the incident.

E3: At the exact design base, source proves per-name nested DROP TABLE dispatch, repeated per-table lifecycle barriers/metadata probes, a nested linear hidden-name membership scan, and final sequential engine deletion. `runSqlWithResultAndOptions` shares the outer TxnOperator and disables internal statement increments/retries. These are source facts, not an executed incident reproduction.

E4: Parent's exact-main standalone public MySQL baseline, isolated owned service on port 16001 with separate 330xx service ports; setup excluded from DROP timing:

| Live plain tables | Setup seconds when supplied | DROP DATABASE seconds | Public postcondition |
| --- | ---: | ---: | --- |
| 0 | — | 0.047 | Catalog absence and later new-client SELECT 1 passed |
| 20 | — | 0.139 | Passed |
| 100 | — | 0.646 | Passed |
| 500 | 4.703 | 2.425 | Passed |
| 1,000 | 11.781 | 6.292 | Passed |
| 2,000 | 20.073 | 15.594 | Passed |

These are supplied single-run observations, not medians or confidence bounds. QA established that SELECT 1 used a new mysql process; E4 proves later service health, not survival of the connection that ran DROP. Same-connection DROP; SELECT 1 with reconnect disabled and catalog orphan checks by saved database ID remain required. The service was cleanly stopped after experiments. They demonstrate the current path at known scale and do not reproduce the two-hour failure. Doubling 1,000 to 2,000 tables increased elapsed time by about 2.48×; that is a reason to inspect growth, not proof of its owner.

Later, the parent completed three controlled baseline samples at each mandatory scale: supplied medians are **6.434 seconds at 1,000 tables** and **17.877 seconds at 2,000 tables**. Preserve the raw samples/configuration in its evidence bundle. Reuse these baseline measurements for candidate comparison when relevant build/setup/resource inputs match; they are not candidate results.

E5: On the same base/service, the parent tried public `DROP TABLE IF EXISTS t1,...,tN; DROP DATABASE`:

| N | Multi-table DROP seconds | Empty DB DROP seconds | Total versus direct DB DROP |
| --- | ---: | ---: | --- |
| 100 | 0.591 | 0.029 | 0.620 versus 0.646 |
| 500 | 3.534 | 0.030 | 3.564 versus 2.425 |
| 1,000 | 8.372 | 0.031 | 8.403 versus 6.292 |

This falsifies the expectation that merely using existing multi-DROP is a material solution. It is an intentionally imperfect control: public DROP TABLE does not inherit all internal DROP DATABASE flags and it planned an unbounded N-member statement. It is not a benchmark of the new ownership refactor or even an exact bounded internal batching patch. Do not reinterpret it as either.

E6: For one direct 1,000-table database drop, the parent found **3,003 logged** nested SQL executions in one outer transaction: 1,001 updates of the SNAPSHOT feature-registry lifecycle row, 1,000 CCPR table probes, 1,000 branch metadata probes, plus two other logged statements. Their logged SQL execution durations sum to about 0.805 seconds of 6.292 seconds end-to-end. Disabled-log SQL, including per-table PITR updates/merge-setting deletes, is omitted; 3,003 is not total SQL count. Lifecycle logs span 6.187 seconds, with cumulative positions 250=1.255, 500=2.674, 750=4.386, 1000=6.187 seconds. Increasing intervals suggest growing transaction/workspace cost, but logged durations exclude planning/setup and cannot locate the full gap without profiling.

At 2,000 tables the parent also observed 2,001 lifecycle updates, 2,000 CCPR probes and 2,000 branch probes. Lifecycle timestamps were cumulative 500=2.273, 1000=6.096, 1500=10.445, 2000=15.433 seconds: the final 500 took about 4.99 seconds versus 2.27 for the first 500. This strengthens the need for a scale-slope check, while ownership attribution remains a profiling hypothesis.

Existing-fix search: parent exact-issue PR search was empty. This designer also read results for `drop database`, `"DROP DATABASE" performance`, and `"DROP TABLE" batch`; no visible title implements this refactor. Related lifecycle/privilege/workspace fixes exist, and open #27013 redesigns workspace. This is a bounded search, not proof of no overlapping work; recheck before publishing.

## 3. Confirmed review blockers and why the revision narrows scope

**B1 — independent temporary retirement.** Multi-DROP preserves parsed member order (`build_ddl.go:5528-5537`). A direct-client temporary drop uses session ownership; `Session.RetireTemporaryTable` removes the alias and forgets undo (`frontend/temporary_ddl.go:79`). Existing `TestTemporaryDropTransactionOwnershipAndFailure` (`temporary_ddl_test.go:389-438`) distinguishes that path from an internal executor's transactional temporary delete. Mixed persistent/temporary DROP is admitted by the frontend (`frontend/txn.go:701-705`). Therefore `DROP TABLE temporary_t, persistent_t` must retire the temporary prefix even if the persistent member's lifecycle gate subsequently fails. Revision 2's up-front gate changed that result. Lazy admission at the original point fixes it without preclassification.

**B2 — statement rollback is not transaction rollback.** An explicit transaction can roll back only the failed statement via `Workspace.RollbackLastStatement`, restore the session statement journal, and remain open (`frontend/txn.go:1108-1141`, default `mo_rollback_txn_on_error=0`). Transaction event callbacks are not thereby undone. `incrservice.Delete` queues by transaction ID (`service.go:295`), and `handleDeletesLocked` processes that queue when the later transaction commits (`service.go:1242`). ISCP drain cleanup is likewise registered on transaction RollbackEvent/CommitEvent (`iscp_util.go:440-473`).

Counterexample: `BEGIN; DROP TABLE p1_plain,p2_autoinc`, with a p1 branch-probe error. The original path stops before p2. Grouped reclaim could first queue p2's auto-increment deletion, fail the delayed p1-containing probe, restore table catalog/data by statement rollback, then process p2's stale deletion callback at `COMMIT`. Merely citing transaction callbacks as “rollback-safe” was incorrect. The same reasoning challenges later ISCP/service hooks.

Revision 3 therefore removes grouped reclaim entirely: no pending-ID slice, return-ID protocol, flush boundaries, deferred reclaim or branch/partition reordering. The original p1 cleanup/error boundary precedes any p2 hook. Consequently `DROP TABLE persistent_t,temporary_t` with p1 reclaim failure also naturally stops before independent temporary retirement. A statement-aware callback redesign would be a separate, substantially larger contract and is not justified for this optimization.

The retained optimization does not claim to fix pre-existing callback behavior inside an already-visited table. It must preserve the visited-table prefix on execution failure, and it must not newly expose a later table's callback after an earlier table's cleanup error.

## 4. Source map and claim

| Exact-base source | Contract/cost |
| --- | --- |
| `pkg/sql/compile/ddl.go:137-383` | DB gate, exclusive lock, RC snapshot advancement, FK/view preparation, selection, nested table drops, engine deletion and tail |
| `ddl.go:252-299` | O(R×H) hidden-name membership; legacy IndexDef children must remain recognized |
| `ddl.go:4139-4160` | Existing single/multi executor split; multi deep-copies each member |
| `ddl.go:4162-4531` | Live temporary/noop resolution, lifecycle SQL at 4205, then table-specific hooks, branch reclaim at 4508, then partitions |
| `pkg/sql/compile/alter.go:278` | Barrier executes stable feature-registry SQL in caller's transaction |
| `pkg/sql/compile/compile.go:10835`; `sql_executor.go:383,565` | Outer TxnOperator/options/context propagation and disabled nested statement increment/retry |
| `pkg/sql/plan/build_ddl.go:5510-5689` | Existing multi-table plan construction/validation and view/sequence IF EXISTS noops |
| `pkg/vm/engine/disttae/engine.go:709`; `txn_database.go:195` | Residual relation filtering/deletion, table/column rowid work, ordered catalog entries and workspace caches |

E6 proves repeated lifecycle barrier execution in the target workload. A successful barrier already protects the invocation through its transaction; repeating the same write before every member does not acquire a new owner. The optimization retains the first barrier at the same execution boundary and changes no transaction/snapshot ownership. Its timing/scale benefit remains a hypothesis requiring profiling and measured acceptance; no source argument proves the two-hour incident or disconnect cause.

## 5. Invariants and exact API

1. Preserve the ordered temporary/noop prefix and every table's existing cleanup-before-next-table boundary. Gate failure after an independent temporary prefix does not undo that prefix. Branch failure before a later table prevents all later hooks, aliases and callbacks.
2. Every active persistent member executes only after lifecycle admission, at the same transaction/snapshot ownership boundary. Pure temporary/noop invocations do not acquire that gate. Admission is remembered only after success and never across calls/retries/transactions.
3. Use the same per-table primitive, planner validation, temporary owner, lock order, error classes and side effects for all callers. Preserve existing SkipDataBranchReclaim semantics for ALTER/TRUNCATE replacements; do not overload IgnoreForeignKey.
4. All nested database-drop groups use the same TxnOperator with disabled internal statement increments. No group commits/savepoints/local retries or fallback execution. Both statement-only and whole-transaction rollback remain the existing frontend's responsibility.
5. Hidden children are parent-owned, selected names retain enumeration order, ErrNoSuchTable collection exceptions remain narrow, and tenant/physical/logical IDs keep their original meanings.
6. Bound internal database-drop planning to B=32 names; retain no all-N generated SQL/deep-copied plan list. No new background owner, goroutine or retry state.

The concrete coordinator contract can be implemented with the smallest state surface:

```go
// Only Scope.DropTable creates/owns this boolean for one invocation.
func (s *Scope) dropTableSingle(
    c *Compile, qry *plan.DropTable, lifecycleAdmitted *bool,
) error
```

`Scope.DropTable` normalizes the existing one-plan versus Tables-list representation into one ordered loop. It initializes `lifecycleAdmitted := false`, skips nil entries as today, deep-copies one member, and calls the same primitive with that pointer. Do not keep the old separate single-table execution path or add a compatibility wrapper just for tests. A local closure with identical lifetime is an acceptable equivalent, but a coordinator type, generic callback framework or Compile/TxnOperator field is unnecessary.

Inside the primitive, leave empty-name, live session alias resolution, vanished-temporary-plan protection and IF EXISTS/noop handling in their current sequence. Replace only the original lifecycle block:

```go
if !isTemp && !*lifecycleAdmitted {
    if err = c.lockDataBranchLineageOwnerLifecycle(); err != nil {
        return err
    }
    *lifecycleAdmitted = true
}
```

Everything after that block stays in order, including database/table/storage locks; CCPR/FK checks; all task/cache/catalog/service hooks; branch reclaim/compaction; partition cleanup; and return. No upfront target scan is needed and no temporary-specific new branch is added. Existing `sessionTemporaryDDLOwner` still distinguishes direct-client nontransactional retirement from SQL-executor transaction ownership; this optimization does not reimplement that decision.

An invocation/retry receives a fresh false boolean. A successful barrier is reused only within that ordered call, while the same outer transaction retains its existing lifecycle ownership. Nested partition/internal invocations get their own local admission state, as do successive bounded database groups. This deliberately accepts a few remaining redundant gates to avoid an ancestor/transaction cache and invalidation protocol.

## 6. DROP DATABASE batching and related cleanup

Keep its prologue and tail verbatim in behavior: database lifecycle and optional View gates, exclusive DB lock, pessimistic RC AdvanceSnapshot without rewind, subscription handling, FK rewrites, database privilege/View metadata cleanup, external-FK check, engine deletion, database PITR/jobs/aliases and affected rows.

Replace `ignoreTables []string` and its nested membership loop with a `map[string]struct{}`. Insert exactly the same feature-flagged index/partition names and all legacy IndexDef child names; retain TableDefs reads/errors. Preserve two-pass selection because a child may precede its parent. Iterate existingRelations, never the map. Filtering into `existingRelations[:0]` can remove the redundant second string-slice allocation if no later consumer needs the unfiltered list. Do not classify hidden tables by name prefix.

One small DDL-local helper constructs each contiguous at-most-32-name SQL with `strings.Builder`, fully qualifies and quotes each database/table identifier with `quoteMySQLIdent`, and calls the existing `runSqlWithOptions(...WithDisableLog().WithIgnorePublish())`. The outer-plan helper supplies IgnoreForeignKey plus the existing TxnOperator/DisableIncrStatement/frontend/resolver/timezone/lower-case context. Empty input does nothing; a final singleton naturally uses the existing planner's singleton representation.

There are ceil(N/32) internal SQL invocations, each releasing its Compile/result before the next. This is neither N single-table invocations nor the measured all-1,000-name public statement. Keep the same affected rows `len(deleteTables)`, including current view/sequence-noop counting. Views/sequences remain for engine residual deletion; do not manufacture DropTable plans for them. Stop at the first error; no retry-as-single fallback.

Expected production owner: `ddl.go` only; private primitive signature, coordinator normalization, bounded SQL helper and set replacement. Rough budget is a few dozen new semantic lines and deletion of the old nested scan/separate executor path. No return-ID conversion, branch-reclaim rewrite or per-algorithm handling remains. Existing direct primitive tests get a fresh local boolean or move lifecycle assertions to Scope.DropTable; do not preserve obsolete wrappers or delete distinct oracles.

## 7. Side effects, concurrency and unhappy paths

All per-table work remains before the next member: CCPR enforcement; per-plan FK SQL/constraint changes; ISCP unregister/drain/fences; idxcron unregister; every plugin drop/cache hook; mo_indexes and external mappings; parent/hidden relation deletion; privilege and View handling; journaled session alias cleanup; auto-increment/shard callbacks; PITR/merge settings; branch reclaim/compaction; partition deletion. Their exact existing intra-table order is retained. Database-level publication/subscription/View/FK/PITR/privilege/job handling stays intact. disttae still owns table-column catalog adjacency, logical-ID indexing and workspace caches.

This is essential, not merely a small-diff preference. Current plugin hooks may evict reloadable caches, temporary retirement can be independent, and service callbacks can outlive statement rollback. Revision 3 does not reorder a later table's hooks before a preceding table's fallible cleanup. It makes no blanket claim that those effects roll back together.

| Boundary | Required behavior |
| --- | --- |
| Temporary/noop prefix, then first persistent gate failure | Preserve earlier independent temporary retirement; current persistent lookup/locks and later work do not run |
| Temporary internal-executor prefix, then failure | Existing parent transaction/statement journal owns physical and alias rollback; never convert it to independent retirement |
| Per-table lookup/check/hook/reclaim/partition failure | Return its error immediately before next table, even inside a multi-DROP group |
| Explicit-transaction statement failure then COMMIT | No callback/service deletion for a later untouched table; test this separately from full ROLLBACK |
| Planning failure for current internal group | No member of that group runs; previous groups remain in outer rollback unit; no new independently committed temporary cleanup is introduced by internal executor |
| Engine.Delete or database-tail failure | Preserve original outer statement/transaction rollback behavior and no partial group commit |
| Cancellation/lock timeout/retry | Existing contexts/lock service/outer retry own it; no new timeout, background work or local retry; a fresh call starts unadmitted |
| Concurrent CREATE/CLONE before DB lock grant | Keep RC AdvanceSnapshot visibility and retained SnapshotTS; no orphan catalog rows |
| Concurrent DML | Existing per-table storage lock/order; barrier is already held from first admission |
| Crash/restart/unknown commit | Existing transaction recovery, no new progress persistence or auto-retry assumption |

Multi-member planning happens before execution. Public multi-DROP already has this property. For internal database groups, the database exclusive-lock/RC snapshot protocol and outer FK cleanup/IgnoreForeignKey protect member definitions; hidden children must be excluded before planning. A later group-member planning error may occur before earlier member cleanup would have run in the old single-statement loop; require equivalent durable failure/rollback outcome, not identical parser-error timing. Do not bypass validation to force equivalence.

No new wait graph edge or worker is introduced. Sequential table lock and cleanup ordering remain; repeated admission SQL is removed only after the first successful barrier. There is no catalog/wire/config/format change. Upgrade/mixed-version and restart use existing contracts; separate restart testing is not required solely for this local optimization. Actual statement/full rollback and concurrency evidence are required.

## 8. Resource/performance model and rejected alternatives

For N ordinary plain tables in DROP DATABASE, successful lifecycle writes change from N+1 to ceil(N/32)+1. **Branch participation probes remain N; CCPR table probes remain N.** All required per-table cleanup remains. This explicit count is the white-box hypothesis, not proof of elapsed improvement. Ordinary public multi-DROP uses at most one successful lifecycle barrier; pure temporary/noop invocations use zero.

Membership changes from O(R×H) to expected O(R+H), with O(H) local map space. Added invocation state is one boolean. Internal SQL/plan state is O(B × largest accepted name/table-definition size), B=32, plus one current deep copy. Normal 64-Unicode-character identifiers produce roughly 17 KiB full-group SQL; internal generated names may be longer, so no universal byte cap is claimed. Existing relation lists, transaction catalog writes/locks/service callbacks and per-table branch DAG reads retain their existing bounds/costs. The patch is not a cap on total transaction memory.

Hypothesis: fewer writes to the identical feature-registry row reduce growing workspace processing between the logged SQL calls in E6. Profiles must check the unexplained elapsed gap, allocations and scale slope. Remaining branch/PITR/merge/index/rowid SQL may dominate enough to fail acceptance.

Rejected: pure multi-DROP (E5), unbounded all-N statement, parallel shared-workspace drops, per-table transactions, direct Engine.Delete, database-only SkipMetadata, transaction-wide admission cache, and broad bulk metadata rewrite. Also rejected by confirmed review: upfront admission (B1), grouped/deferred branch reclaim (B2), and the proposed temporary-only flush workaround (does not protect later persistent service callbacks). Fixing callbacks to be statement-aware is out of scope for this candidate.

## 9. Orthogonal submitted tests

Reuse existing compile ddl tests, alter lifecycle-admission tests, temporary_ddl_test.go ownership fixture, frontend session journal controls, plugin-drop tests and public multi-DROP/FK/grant/PITR/branch cases. Do not add another embedded cluster for a local loop/boolean. An options-capable spy is needed because NewMemExecutor discards options; restore service globals immediately with t.Cleanup.

| Cell | Required oracle |
| --- | --- |
| Lazy common admission | Two persistent members execute one gate before the first member lookup/locks; pure temporary/noops execute zero; a new invocation/retry executes a fresh gate. Verify table order, not only SQL count |
| Direct-client temp-first gate failure | Planned `[temporary_t,persistent_t]`; inject gate error. Exactly one prior retirement, alias absent with physical retirement ownership preserved; persistent table and later members untouched |
| Direct-client persistent-first reclaim failure | `[persistent_t,temporary_t]`; fail persistent branch probe. Temporary lookup/retirement never occurs and alias remains; parent rollback restores persistent catalog/data as applicable |
| Internal-executor temporary rollback | Same originating session, existing temporaryDDLInExecutorTxn/internal ownership; no RetireTemporaryTable callback. Later failure followed by parent statement/transaction rollback restores temp table/data/alias. Keep the existing direct-delete failure preserves alias control. A nontransactional stub alone cannot prove rollback |
| Persistent callback prefix | `BEGIN; DROP TABLE p1_plain,p2_autoinc` with p1 branch error and default statement rollback, then COMMIT. p2 Delete callback is never registered, allocator/data/table remain usable. Distinct ISCP later-target control proves no later drain/fence was installed |
| Selection/bounds/escaping/options | Hidden child before parent, legacy metadata child, feature children, lookalike ordinary name, missing versus unexpected error; parsed SQL shows same ordered targets and 0/1/B/B+1 groups, quoted identifiers, same txn/flags |
| Public success/control | Small DROP DATABASE case with stored/indexed/auto-increment plus view/sequence as useful; independent catalog/data/metadata postconditions, recreate same name and same-client connection use; ordinary DROP TABLE controls unchanged |

The two mixed temporary-order failures and the explicit-transaction COMMIT control are distinct contracts; do not merge them into a generic “rollback passes” assertion. Existing mocked missing-name and unexpected-error checks also remain distinct. Delete only obsolete per-table gate-count assumptions and duplicate setup; preserve semantic assertions. No timing thresholds, sleeps, skipped cases or retry-to-pass logic. Real transaction fixtures must restore variables/state and capture terminal status.

## 10. Private verification exceeds submitted tests

White-box: profile exact base/candidate, count lifecycle SQL including disabled-log caveats, inspect workspace/allocation slope; record all per-table hook/reclaim ordering and transaction options. Inject admission, branch, partition, engine-tail and metadata errors; test both whole transaction rollback and statement-only rollback followed by later COMMIT. Check no new stale callback/fence/alias mutation for tables beyond the failing execution prefix. Candidate evidence must be run, not inferred from source or a unit spy.

Real black-box on a test-owned service: mixed objects, quoted Unicode/backticks, hidden indexes/partitions; both direct-client temp failure orders and internal executor rollback; foreign keys in both cross-database directions, enabled rejection and disabled cleanup; grants/logical IDs and same names in a second tenant; auto-increment/plugin/ISCP service usability after errors and later COMMIT; outside branch/PITR/snapshot owners; existing ALTER/TRUNCATE SkipDataBranchReclaim controls. Use deterministic barriers/observable lock state for concurrent DML and cross-CN creator/clone visibility. If a mode disallows a surrounding transaction for a DDL, preserve that public rejection and use supported DROP TABLE to prove statement-only rollback behavior.

Focused named UTs then owning packages once, applicable vet/build, repository Go/native/CGo wrapper; no empty test selection. BVT uses clean owned SQL+ISCP-ready instance, normal result comparison, explicit teardown postconditions and a second case run. Relevant race/concurrency evidence must exercise actual shared-state boundaries; do not pad confidence with unrelated suites.

Scale is separate from UT/BVT: mandatory known live counts 100, 1,000 and 2,000; larger 5,000/10,000 and equal-live-count churn challenge when affordable. Record build/toolchain/native/config/topology, setup separately, raw elapsed times, real relation counts, CN profile/memory, catalog absence by saved DB/table IDs, and `DROP DATABASE; SELECT 1` on the **same connection with reconnect disabled**. E4's new-client query proves only later service health. Compare fresh and churn histories separately.

## 11. Acceptance and delivery

All functional/ordering/rollback/metadata gates must pass. Require **at least 25% lower median end-to-end DROP DATABASE time at both 1,000 and 2,000 plain tables**, from at least three comparable paired/recreated base/candidate runs with raw samples. E4's initial single samples are not the paired baseline; its later three-sample medians can be reused if the candidate comparison preserves the relevant setup and mode. This is an engineering gate, not a public SLA.

The incremental 1,000→2,000 cost must materially decrease and the T(2000)/T(1000) ratio must not worsen outside variance. Check profiles if a total gain hides steeper growth. No meaningful small/single-table regression, new retained resource leak or all-N plan allocation. Reduced lifecycle write count alone is insufficient; branch/CCPR counts are expected to remain linear as today.

If the remaining cleanup dominates and this candidate fails, do not label it the completed issue fix. Revisit the measured dominant owner with an independently reviewed design. In particular, do not restore grouped reclaim without statement-aware callback proof, and do not silently add a database-only bypass. The user authorized robust optimization, not a cosmetic benchmark result.

A standalone speedup does not establish the two-hour disconnect cause or claim its resolution. The deliverable claim is measured reduction of table-count-driven CN database cleanup with the original table-by-table failure boundaries preserved.

## 12. Handoff

Revision 2 failed independent design review. Revision 3 incorporates both confirmed blockers, removes unsafe regrouping, and retains the smallest measurable candidate. Critical review points: exact lazy admission location, no admission state beyond one invocation, unchanged per-table branch/partition/hook order, both mixed temporary outcomes, and statement rollback followed by COMMIT.

Current status: revision-3 design approval and P3 implementation commit are complete. The independent commit review requested real failed-statement→COMMIT and executor-temporary rollback evidence; the task ledger now records both public SQL probes and the first-member BVT. A later focused change moves the existing incoming-FK check before table retirement, closing the demonstrated E12 external-FK/auto-ID failure path on a real service; its separate independent design review approved the move. Measured quiet improvement still misses the 1,000-table gate, and E25's unrelated-DDL wait persists under one slow table. General statement-effect recovery, merge-event outcome and scoped busy isolation remain unresolved. See the task ledger for exact binaries, raw samples, reviews and withdrawn experiments; no PR readiness is claimed here.
