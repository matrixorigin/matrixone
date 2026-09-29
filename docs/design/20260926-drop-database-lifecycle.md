# DROP DATABASE: reuse lifecycle admission without reordering cleanup

The subsequent pessimistic-RC component protocol in [revision 1 of the RC lifecycle design](20260929-rc-lifecycle-protocol.md) supersedes this document's earlier no-new-wire/wait/upgrade assumption for PR #29457. This document retains #29393's ordered per-table cleanup and separate performance acceptance.

Design revision: **4**, 2026-09-27, with a focused performance addendum dated 2026-09-28. **Supersedes revision 3. The rejected grouped SQL path is removed; DROP DATABASE now reuses the existing per-table primitive directly after resolving live descriptors.** Revision 3's admission and callback constraints remain. Current review and operational limits are recorded below.

Issue: https://github.com/matrixorigin/matrixone/issues/27575. PR: https://github.com/matrixorigin/matrixone/pull/29393. This document records the direct-execution implementation and its evidence; inherited busy-system and callback-scope limits remain explicit below.

Inspected worktree/base: `/tmp/mo-issue-27575`, `9c6fcb5b501d040ab1bcb5ada139939770468638`, branch `codex/issue-27575-drop-database-serial`. Designer owns only this artifact and changed no tracked source. Source/GitHub reads are this agent's evidence; SQL measurements were supplied by the parent and require its raw evidence bundle.

Requested model/effort: GPT-6 Astra, xhigh. Actual invocation identity is supplied by the parent spawn record; this agent has no independent model-introspection API and requested no substitution. Skills applied: mo-dev and its design, validation, testing and index-plugin references.

## 1. Revised decision

Use one ordered DROP TABLE coordinator for single-table, public multi-table and internal database-drop invocations. It owns one **call-local admission boolean**, initially false. The per-table primitive requests lifecycle admission at its exact existing location after live temporary/no-op resolution and before persistent database/relation locks. The first successful request executes the existing barrier and marks admission; later members reuse it. The primitive retains every other operation, including branch reclaim/compaction **at its current per-table location before partition cleanup and before the next member**.

DROP DATABASE keeps its earlier lifecycle gate because that protects database/FK/view work before table dispatch. After the exclusive database lock and FK check, it resolves each live relation once into the existing `plan.DropTable` descriptor, preserves enumeration order, and calls `dropTableSingle` directly with that physical relation. This removes the nested parser/planner/executor loop and the repeated per-member lifecycle write while keeping the same per-table cleanup boundary. Physical temporary relations also run through the direct per-table cleanup with their resolved physical identity, so transaction-owned allocator/storage retirement is preserved; the outer `RemoveTempTablesByDatabase` still journals their session aliases. A temporary alias may shadow a persistent name, but the persistent relation is still dispatched through its resolved physical descriptor so its allocator, branch, PITR, merge and partition cleanup runs. Replace the database's quadratic hidden-name membership scan with an exact-name set. No new execution option, database-only fast path, fallback, ancestor-gate cache, transaction-global state, worker, service API or persistent format is introduced.

The hypothesis is that eliminating N repeated lifecycle writes plus nested SQL planning materially reduces growing workspace cost. Existing branch probes, CCPR probes and other per-table cleanup remain. The direct path must meet the unchanged 1,000/2,000-table end-to-end gate or it is not a delivered issue fix.

This is the minimum coherent ownership refactor after review rejected broader regrouping. G-FEATURE-DESIGN remains applicable through material catalog hot-path/lifecycle ownership impact; the independent reviewer must approve this exact version before implementation. The direct path must preserve the planner-owned validation that matters for DROP DATABASE (live relation resolution, hidden-child selection, view/sequence exclusion and table metadata) without retaining a second execution path. Make no unrelated CLONE, timeout, callback-framework or engine/workspace changes.

## 2. Evidence and limits

E1: The [issue](https://github.com/matrixorigin/matrixone/issues/27575) reports one nightly occurrence on `4.2-dev` `92db870eecaccc6ceea6b42f004170d14daaa6ab`: DROP DATABASE after five minutes of table churn lasted about two hours and ended with ERROR 2013. The 10,000-name workload bounds potential live tables but statement-attempt counters do not measure them. Original cluster logs/profiles are gone. Neither regression status nor the disconnecting component is proven.

E2: [The production comment](https://github.com/matrixorigin/matrixone/issues/27575#issuecomment-5836653456) reports visible TN relation drops in about 24 ms, preceded by CN latency. It has no end-to-end duration/live count. It supports CN investigation, not a quantitative attribution of the incident.

E3: At the exact design base, source proves per-name nested DROP TABLE dispatch, repeated per-table lifecycle barriers/metadata probes, a nested linear hidden-name membership scan, and final sequential engine deletion. The candidate replaces only the nested dispatch: it builds live descriptors under the already-held database lock and invokes the same `dropTableSingle` primitive with the outer transaction. These are source facts, not an executed incident reproduction.

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
| `pkg/sql/compile/ddl.go:137-410` | DB gate, exclusive lock, RC snapshot advancement, FK/view preparation, descriptor selection, direct table drops, engine deletion and tail |
| `ddl.go:252-299` | O(R×H) hidden-name membership; legacy IndexDef children must remain recognized |
| `ddl.go:4139-4160` | Existing single/multi executor split; multi deep-copies each member |
| `ddl.go:4162-4531` | Live temporary/noop resolution, lifecycle SQL at 4205, then table-specific hooks, branch reclaim at 4508, then partitions |
| `pkg/sql/compile/alter.go:278` | Barrier executes stable feature-registry SQL in caller's transaction |
| `pkg/sql/compile/compile.go:10835`; `sql_executor.go:383,565` | Outer TxnOperator/options/context propagation; direct execution no longer creates a nested SQL executor |
| `pkg/sql/plan/build_ddl.go:5510-5689` | Existing multi-table plan construction/validation and view/sequence IF EXISTS noops |
| `pkg/vm/engine/disttae/engine.go:709`; `txn_database.go:195` | Residual relation filtering/deletion, table/column rowid work, ordered catalog entries and workspace caches |

E6 proves repeated lifecycle barrier execution in the target workload. A successful barrier already protects the invocation through its transaction; repeating the same write before every member does not acquire a new owner. The optimization retains the first barrier at the same execution boundary and changes no transaction/snapshot ownership. Its timing/scale benefit remains a hypothesis requiring profiling and measured acceptance; no source argument proves the two-hour incident or disconnect cause.

## 5. Invariants and exact API

1. Preserve the ordered temporary/noop prefix and every table's existing cleanup-before-next-table boundary. Gate failure after an independent temporary prefix does not undo that prefix. Branch failure before a later table prevents all later hooks, aliases and callbacks.
2. Every active persistent member executes only after lifecycle admission, at the same transaction/snapshot ownership boundary. Pure temporary/noop invocations do not acquire that gate. Admission is remembered only after success and never across calls/retries/transactions.
3. Use the same per-table primitive, planner validation, temporary owner, lock order, error classes and side effects for all callers. Preserve existing SkipDataBranchReclaim semantics for ALTER/TRUNCATE replacements; do not overload IgnoreForeignKey.
4. DROP DATABASE descriptors and all per-table effects use the outer TxnOperator and statement context. There is no nested SQL executor, group commit/savepoint/local retry or fallback execution. Both statement-only and whole-transaction rollback remain the existing frontend's responsibility.
5. Hidden children are parent-owned, selected names retain enumeration order, ErrNoSuchTable collection exceptions remain narrow, and tenant/physical/logical IDs keep their original meanings.
6. The descriptor slice contains one pointer per enumerated relation, reusing live `TableDef` objects; only the current member is deep-copied before execution. No generated all-N SQL, background owner, goroutine or retry state is introduced.

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

An invocation/retry receives a fresh false boolean. A successful barrier is reused only within that ordered call, while the same outer transaction retains its existing lifecycle ownership. DROP DATABASE already admits the lifecycle before descriptor execution, so it passes `true` to the primitive; a standalone or public multi-DROP call owns its own local boolean. This deliberately avoids an ancestor/transaction cache and invalidation protocol.

## 6. DROP DATABASE descriptor execution and related cleanup

Keep its prologue and tail verbatim in behavior: database lifecycle and optional View gates, exclusive DB lock, pessimistic RC AdvanceSnapshot without rewind, subscription handling, FK rewrites, database privilege/View metadata cleanup, external-FK check, engine deletion, database PITR/jobs/aliases and affected rows.

Replace `ignoreTables []string` and its nested membership loop with a `map[string]struct{}`. Insert exactly the same feature-flagged index/partition names and all legacy IndexDef child names; retain TableDefs reads/errors. Preserve two-pass selection because a child may precede its parent. Iterate the relation-ordered descriptor slice, never the map. Do not classify hidden tables by name prefix.

Each selected relation becomes one `plan.DropTable` descriptor carrying the live table ID/definition, index-child names, and a narrow child-side FK cleanup SQL. The descriptor pointer reuses the relation's `TableDef`; execution deep-copies only the current member so the prepass cannot be mutated, while the already-resolved relation and database handle are reused for the same transaction snapshot. Views and system sequences are intentionally skipped by the per-table primitive and remain covered by final engine database deletion.

The direct loop sets the same `IgnoreForeignKey` and `ignorePublish` controls used by the old internal path, passes the already-successful database lifecycle admission and database lock, and stops at the first member error. It keeps the existing outer DDL INFO log and avoids manufacturing nested DDL log records; per-table cleanup SQL retains its current disabled-log behavior. Physical temporary descriptors stay in the loop and are passed by resolved physical identity, avoiding a second session-alias lookup while retaining allocator, shard, merge and branch cleanup. A persistent descriptor shadowed by a session alias is likewise executed by the resolved relation pointer, so it cannot be redirected to the temporary generation. `RemoveTempTablesByDatabase` journals aliases so statement rollback and transaction rollback restore the original physical identities.

Production ownership is `ddl.go` for descriptor collection/direct execution and `build_dml_util.go` for the narrow deferred incoming-FK predicate. The change keeps private descriptor collection, the direct loop and set replacement local to the DDL path, with the old nested SQL helper removed. No return-ID conversion, branch-reclaim rewrite or per-algorithm handling remains. Existing direct primitive tests use a fresh local boolean or move lifecycle assertions to `Scope.DropTable`; do not preserve obsolete wrappers or delete distinct oracles.

## 7. Side effects, concurrency and unhappy paths

All per-table work remains before the next member: CCPR enforcement; per-plan FK SQL/constraint changes; ISCP unregister/drain/fences; idxcron unregister; every plugin drop/cache hook; mo_indexes and external mappings; parent/hidden relation deletion; privilege and View handling; journaled session alias cleanup; auto-increment/shard callbacks; PITR/merge settings; branch reclaim/compaction; partition deletion. Their exact existing intra-table order is retained. Database-level publication/subscription/View/FK/PITR/privilege/job handling stays intact. disttae still owns table-column catalog adjacency, logical-ID indexing and workspace caches.

This is essential, not merely a small-diff preference. Current plugin hooks may evict reloadable caches, temporary retirement can be independent, and service callbacks can outlive statement rollback. Revision 3 does not reorder a later table's hooks before a preceding table's fallible cleanup. It makes no blanket claim that those effects roll back together.

| Boundary | Required behavior |
| --- | --- |
| Temporary/noop prefix, then first persistent gate failure | Preserve earlier independent temporary retirement; current persistent lookup/locks and later work do not run |
| Temporary internal-executor prefix, then failure | Existing parent transaction/statement journal owns physical and alias rollback; never convert it to independent retirement |
| Per-table lookup/check/hook/reclaim/partition failure | Return its error immediately before the next table |
| Explicit-transaction statement failure then COMMIT | No callback/service deletion for a later untouched table; test this separately from full ROLLBACK |
| Descriptor collection or planning failure | No per-table member runs; the outer statement owns rollback and no temporary cleanup is independently committed |
| Engine.Delete or database-tail failure | Preserve original outer statement/transaction rollback behavior and no partial per-table commit |
| Cancellation/lock timeout/retry | Existing contexts/lock service/outer retry own it; no new timeout, background work or local retry; a fresh call starts unadmitted |
| Concurrent CREATE/CLONE before DB lock grant | Keep RC AdvanceSnapshot visibility and retained SnapshotTS; no orphan catalog rows |
| Concurrent DML | Existing per-table storage lock/order; barrier is already held from first admission |
| Crash/restart/unknown commit | Existing transaction recovery, no new progress persistence or auto-retry assumption |

Descriptor collection happens before execution, as the old database path also collected all relation names before dispatch. The database exclusive-lock/RC snapshot protocol and outer FK cleanup/IgnoreForeignKey protect member definitions; hidden children must be excluded before execution. A descriptor error prevents all per-table members, while a later live lookup or cleanup error stops at the already-visited prefix. Do not bypass validation to force equivalence.

No new wait graph edge or worker is introduced. Sequential table lock and cleanup ordering remain; repeated admission SQL is removed only after the first successful barrier. There is no catalog/wire/config/format change. Upgrade/mixed-version and restart use existing contracts; separate restart testing is not required solely for this local optimization. Actual statement/full rollback and concurrency evidence are required.

## 8. Resource/performance model and rejected alternatives

For N ordinary plain tables in DROP DATABASE, successful lifecycle writes change from N+1 to 1 because the database prologue already admits the lifecycle owner. **Branch participation probes remain N; CCPR table probes remain N.** All required per-table cleanup remains. Nested SQL parsing/planning/executor setup is removed. This explicit count is the white-box hypothesis, not proof of elapsed improvement. Ordinary public multi-DROP uses at most one successful lifecycle barrier; pure temporary/noop invocations use zero.

Membership changes from O(R×H) to expected O(R+H), with O(H) local map space. Added invocation state is one boolean and one pointer descriptor per relation; descriptors reuse live definitions and do not copy full schemas. Only the current descriptor is deep-copied. Existing relation lists, transaction catalog writes/locks/service callbacks and per-table branch DAG reads retain their existing bounds/costs. The patch is not a cap on total transaction memory.

Hypothesis: fewer writes to the identical feature-registry row reduce growing workspace processing between the logged SQL calls in E6. Profiles must check the unexplained elapsed gap, allocations and scale slope. Remaining branch/PITR/merge/index/rowid SQL may dominate enough to fail acceptance.

Rejected: pure multi-DROP (E5), nested bounded SQL groups, unbounded all-N statement, parallel shared-workspace drops, per-table transactions, direct Engine.Delete, database-only SkipMetadata, transaction-wide admission cache, and broad bulk metadata rewrite. Also rejected by confirmed review: upfront admission (B1), grouped/deferred branch reclaim (B2), and the proposed temporary-only flush workaround (does not protect later persistent service callbacks). Fixing callbacks to be statement-aware is out of scope for this candidate.

## 9. Orthogonal submitted tests

Reuse existing compile ddl tests, alter lifecycle-admission tests, temporary_ddl_test.go ownership fixture, frontend session journal controls, plugin-drop tests and public multi-DROP/FK/grant/PITR/branch cases. Do not add another embedded cluster for a local loop/boolean. An options-capable spy is needed because NewMemExecutor discards options; restore service globals immediately with t.Cleanup.

| Cell | Required oracle |
| --- | --- |
| Lazy common admission | Two persistent members execute one gate before the first member lookup/locks; pure temporary/noops execute zero; a new invocation/retry executes a fresh gate. Verify table order, not only SQL count |
| Direct-client temp-first gate failure | Planned `[temporary_t,persistent_t]`; inject gate error. Exactly one prior retirement, alias absent with physical retirement ownership preserved; persistent table and later members untouched |
| Direct-client persistent-first reclaim failure | `[persistent_t,temporary_t]`; fail persistent branch probe. Temporary lookup/retirement never occurs and alias remains; parent rollback restores persistent catalog/data as applicable |
| Internal-executor temporary rollback | Same originating session, existing temporaryDDLInExecutorTxn/internal ownership; no RetireTemporaryTable callback. Later failure followed by parent statement/transaction rollback restores temp table/data/alias. Keep the existing direct-delete failure preserves alias control. A nontransactional stub alone cannot prove rollback |
| Persistent callback prefix | `BEGIN; DROP TABLE p1_plain,p2_autoinc` with p1 branch error and default statement rollback, then COMMIT. p2 Delete callback is never registered, allocator/data/table remain usable. Distinct ISCP later-target control proves no later drain/fence was installed |
| Selection and descriptor safety | Hidden child before parent, legacy metadata child, feature children, lookalike ordinary name, temporary physical relation, session-alias shadow, missing versus unexpected error; same ordered IDs/definitions and outer txn/context |
| Public success/control | Small DROP DATABASE case with stored/indexed/auto-increment plus view/sequence as useful; independent catalog/data/metadata postconditions, recreate same name and same-client connection use; ordinary DROP TABLE controls unchanged |

The two mixed temporary-order failures and the explicit-transaction COMMIT control are distinct contracts; do not merge them into a generic “rollback passes” assertion. Existing mocked missing-name and unexpected-error checks also remain distinct. Delete only obsolete per-table gate-count assumptions and duplicate setup; preserve semantic assertions. No timing thresholds, sleeps, skipped cases or retry-to-pass logic. Real transaction fixtures must restore variables/state and capture terminal status.

## 10. Private verification exceeds submitted tests

White-box: profile exact base/candidate, count lifecycle SQL including disabled-log caveats, inspect workspace/allocation slope; record all per-table hook/reclaim ordering and transaction options. Inject admission, branch, partition, engine-tail and metadata errors; test both whole transaction rollback and statement-only rollback followed by later COMMIT. Check no new stale callback/fence/alias mutation for tables beyond the failing execution prefix. Candidate evidence must be run, not inferred from source or a unit spy.

Real black-box on a test-owned service: mixed objects, quoted Unicode/backticks, hidden indexes/partitions; both direct-client temp failure orders and internal executor rollback; foreign keys in both cross-database directions, enabled rejection and disabled cleanup; grants/logical IDs and same names in a second tenant; auto-increment/plugin/ISCP service usability after errors and later COMMIT; outside branch/PITR/snapshot owners; existing ALTER/TRUNCATE SkipDataBranchReclaim controls. Use deterministic barriers/observable lock state for concurrent DML and cross-CN creator/clone visibility. If a mode disallows a surrounding transaction for a DDL, preserve that public rejection and use supported DROP TABLE to prove statement-only rollback behavior.

Focused named UTs then owning packages once, applicable vet/build, repository Go/native/CGo wrapper; no empty test selection. BVT uses clean owned SQL+ISCP-ready instance, normal result comparison, explicit teardown postconditions and a second case run. Relevant race/concurrency evidence must exercise actual shared-state boundaries; do not pad confidence with unrelated suites.

Scale is separate from UT/BVT: mandatory known live counts 100, 1,000 and 2,000; larger 5,000/10,000 and equal-live-count churn challenge when affordable. Record build/toolchain/native/config/topology, setup separately, raw elapsed times, real relation counts, CN profile/memory, catalog absence by saved DB/table IDs, and `DROP DATABASE; SELECT 1` on the **same connection with reconnect disabled**. E4's new-client query proves only later service health. Compare fresh and churn histories separately.

## 11. Acceptance and delivery

All functional/ordering/rollback/metadata gates must pass. The original revision-4 engineering target was **at least 25% lower median end-to-end DROP DATABASE time at both 1,000 and 2,000 plain tables**, from at least three comparable paired/recreated base/candidate runs with raw samples. E4's initial single samples are not the paired baseline; its later three-sample medians can be reused if the candidate comparison preserves the relevant setup and mode. The integrated RC delivery's measured result and user-approved acceptance boundary are recorded in §15. This target is not a public SLA.

The incremental 1,000→2,000 cost should materially decrease and the T(2000)/T(1000) ratio must not worsen outside variance. Check profiles if a total gain hides steeper growth. No meaningful small/single-table regression, new retained resource leak or generated all-N SQL/plan statement allocation. The bounded descriptor slice is intentional; reduced lifecycle write count alone is insufficient; branch/CCPR counts are expected to remain linear as today.

If the remaining cleanup dominates and a candidate fails its applicable acceptance criteria, do not label it the completed issue fix. Revisit the measured dominant owner with an independently reviewed design. In particular, do not restore grouped reclaim without statement-aware callback proof, and do not silently add a database-only bypass. The user authorized robust optimization, not a cosmetic benchmark result.

A standalone speedup does not establish the two-hour disconnect cause or claim its resolution. The deliverable claim is measured reduction of table-count-driven CN database cleanup with the original table-by-table failure boundaries preserved.

## 12. Handoff

Revision 2 failed independent design review. Revision 3 incorporated the temporary and callback blockers by removing unsafe regrouping. Revision 4 removes the remaining nested SQL path and keeps the same per-table cleanup boundary for persistent and physical temporary relations while retaining resolved-relation execution for a persistent relation shadowed by a session alias. Critical review points: exact lazy admission location, no admission state beyond one invocation, unchanged per-table branch/partition/hook order, descriptor/live-relation agreement, physical temporary allocator retirement, and statement rollback followed by COMMIT.

Revision-4 status: revision-4 was published as PR #29393. Its late-table-lock `KILL QUERY`→COMMIT probe exposed a statement-rollback defect in allocator deletion (#29395); this follow-up reconciles terminal allocator deletion intents with the workspace's surviving physical table deletions. Focused tests and public rollback/COMMIT probes pass, including an earlier successful deletion and a rejected deletion with an active cache builder. A public race also committed an in-place external FK ADD while DROP waited on a different table: the same DROP transaction then retried its lifecycle admission and rejected the new FK. A separate first-member branch-reclaim wait was cancelled, followed by statement rollback and COMMIT; the untouched later table kept its allocator and its existing ISCP job advanced its CDC tail after another write. A fresh three-sample comparison of those binaries measured 1,000-table medians of 5.997s (base) and 4.930s (candidate), and 2,000-table medians of 16.079s and 12.699s. These 17.8% and 21.0% gains did not meet the 25% gate above. The single-slow-table global SNAPSHOT registry convoy remains, so #27575 is not resolved by this PR. The two-hour disconnect cause and broader merge outcomes remain unproven. See the task ledger for binaries, raw samples, and reviews.

## 13. Focused performance addendum, 2026-09-28

The revision-4 restriction against engine/workspace changes is superseded only for this measured hot path. A CPU profile of a coverage-instrumented binary at the exact PR head's 1,000-table DROP attributed 5.17s cumulative time to `FastApplyDeletesByRowIds`, reached through catalog point reads that repeatedly applied transaction-local deletes. That binary was used for hotspot attribution only, not the timed comparison below. This path affects ordinary reads too, so the optimization remains conditional: a sorted list of at least 32 deleted Rowids and exactly one candidate row uses binary search; other calls retain their existing path. For at least 128 delete batches targeting one block and at most 256 Rowids, the current data source records the first full singleton miss and builds a 1 KiB row-offset bitmap after the second. Subsequent singleton probes use that bitmap until `txnOffset` changes. Bitmap and multirow scans and blocks with out-of-range row offsets keep the existing batch path; early hits never initiate indexing. The 256 bound applies only to this new index, not to the existing global merge or total transaction memory. No DROP-specific engine bypass is added.

Same non-coverage build flags (`GOAMD64=v3`, `GOEXPERIMENT=simd`), native inputs, launch topology, SQL setup and one owned persistent service data directory were used for exact merge-base `a99db843c7b4630a86883addc9424433d93fc2d2`, original PR head `94a0cde08a4680e5003969674faa37e2b2b68230`, and the final local candidate. Each sample recreated the database and the stated number of live plain tables; setup was excluded. The DROP command selected `CONNECTION_ID()` before and after DROP with reconnect disabled, and a final catalog check found zero matching databases/tables. Raw DROP seconds:

| Binary | 1,000 tables | 2,000 tables | Median 1,000 / 2,000 |
| --- | --- | --- | --- |
| Merge-base | 16.199, 14.632, 14.945 | 49.235, 31.827, 48.827 | 14.945 / 48.827 |
| Original PR head | 12.924, 13.009, 13.729 | 30.702, 42.706, 34.343 | 13.009 / 34.343 |
| Final candidate | 9.963, 10.105, 11.000 | 26.355, 26.627, 27.232 | 10.105 / 26.627 |
| Merge-base reverse-order check | — | 48.300, 33.514, 56.599 | — / 48.300 |

That historical candidate, before the rebase and subsequent cold-read changes, was 32.4% and 45.5% below the first base medians; the 1,000→2,000 ratio fell from 3.27 to 2.64. A later reverse-order base run reproduced the high 2,000-table median, but its wide 31.827–56.599s spread limits precision. These sequential runs recreated schema in one data directory rather than resetting catalog history between binaries. They cannot establish the gate for the current source. The separate global SNAPSHOT row convoy remains outside this PR's scope; #27575 remains open.

Review follow-up: a cold-read probe exposed that the original 4,096-Rowid merge also ran for bitmap and multirow scans. With 128 batches of 32 Rowids, the first bitmap filter cost 35.8 µs without that merge and 500.2 µs with it, adding 98,336 bytes of allocation. Limiting the merge to singleton reads with at most 256 Rowids restored that bitmap path to 36.1 µs with zero added allocation. A second probe then found that the cap still made a *first* singleton read copy and sort 128 two-row batches: a cold miss cost about 19.6 µs and an early hit about 20.1 µs, versus 1.8 µs and 35 ns when consuming the original batches.

Counting 16 full misses before sorting fixed that first-read spike in a microbenchmark, but failed the whole-SQL cost check. On this machine, six runs of each version under the same launch configuration, with separate fresh data directories because the older 4.0.8 binary cannot join a 4.0.9 catalog, gave medians of **10.717/25.124 seconds** for the earlier eager-merge binary versus **14.309/34.197 seconds** for the 16-miss version at 1,000/2,000 tables. All runs verified the same connection and absence by saved database ID. The host had sustained high I/O pressure, and the versions/data histories differ, so these are not a formal paired acceptance measurement; nevertheless, consistent 34%/36% slower medians at both sizes make a long admission delay unacceptable. That revision was rejected.

The bounded offset bitmap replaced the eager full-Rowid sort. It waits for a second full miss before building, so a one-shot point read does not pay bitmap construction and an early hit does not initiate it. The bitmap handles duplicate offsets and consumes at most 1 KiB per indexed block, versus up to 6 KiB for the sorted Rowid copy. Source offsets outside the physical 8,192-row block bound disable the index for that block. `txnOffset` invalidates both the block grouping and bitmap. An independent source oracle passed for 127/128/256/257/512 batches, duplicate offsets, bitmap, multirow, source immutability and repeated point reads; the owning `disttae` package also passed. On 128 batches of two Rowids, two 200 ms microbenchmark samples measured first misses at 3.14–3.16 µs (464 B) versus 1.83 µs for direct streaming and about 19.6 µs for the rejected eager full-Rowid sort. First-batch hits measured 39.7–39.9 ns versus 33.3–34.3 ns for direct streaming; after index construction, misses measured 23.5–23.6 ns versus 1.82–1.84 µs for direct streaming. These are isolated costs, not current DROP timings.

Review challenge on `eda7c24d82`: aptend correctly noted that the published 32.4%/45.5% table preceded the final cold-read change. A new exact-version 4.0.9 comparison used the then-current merge-base `239fe81c82`, unchanged PR head `eda7c24d82`, and the post-rebase bitmap candidate, each with the same build flags/native libraries/topology/SQL, separate fresh service data directories, three DROP samples per scale, same-connection IDs with reconnect disabled, and zero catalog rows by saved database ID. Setup was excluded. Raw seconds:

| Build | 1,000 tables | 2,000 tables | Medians |
| --- | --- | --- | --- |
| Measured merge-base `239fe81c82` | 16.170, 16.813, 17.026 | 35.074, 49.928, 27.647 | 16.813 / 35.074 |
| Unchanged reviewed head `eda7c24d82` | 19.980, 16.185, 15.491 | 29.366, 34.726, 35.783 | 16.185 / 34.726 |
| Bounded bitmap, before direct offset search | 15.492, 15.044, 15.190 | 32.514, 35.187, 36.381 | 15.190 / 35.187 |
| Direct offset search plus bounded bitmap | 11.578, 13.493, 11.909 | 39.236, 35.344, 42.131 | 11.909 / 39.236 |

The exact reviewed head therefore did **not** meet the 25% gate on this current-base setup. The direct offset path is 29.2% below base at 1,000 tables but its first 2,000-table median is 11.9% above base. These samples coincided with substantial shared-host I/O pressure and another service/linker workload; base's 2,000-table range alone is 27.647–49.928 seconds. The result is an observed setup result, not a causal regression attribution.

The post-rebase candidate's 1,000-table CPU profile attributed 5.26s cumulative to workspace-delete filtering, with 3.51s in repeated generic per-batch Rowid filtering and 0.52s validating bitmap eligibility. The direct offset change uses the existing block grouping: valid singleton probes compare only row offsets, using binary search in sorted batches of at least 32 Rowids; bitmap and multirow scans retain their original path. Offset values outside a Rowid's 32-bit range cannot match; source offsets outside the physical 8,192-row block range still use direct row-offset matching and never build a bitmap. No new owner, lock or global cache is introduced. The independent oracle, 31/32/33-row boundary, 8,191/8,192 source boundary, 32-bit wraparound, cap and invalidation tests passed. The current direct-offset service run above also verified connection continuity and catalog absence at all six points.

Because the 2,000-table first run was noisy, three additional 2,000-table comparisons alternated exact base/current candidate using their existing equally aged persistent data directories. All six checked the same connection and saved-ID catalog absence. Base: **61.116, 50.654, 49.919s**; candidate: **55.991, 31.445, 31.162s**. Their medians differ by 37.9%, but one matched pair differs by only 8.4%; both ranges are wide. This history-rich comparison is useful evidence of possible benefit under accumulated catalog history and cannot erase the fresh-directory result. The design's robust two-scale end-to-end acceptance remains **open**; the PR must not claim the old table proves current-head completion or close #27575.

The branch was subsequently rebased onto `d99187d7b3` without conflicts. That newer base changes compile/plan code, so the performance tables above are identified by their measured `239fe81c82` base; no new exact-base end-to-end performance claim is made for the rebased head. After rebase, the focused `pkg/sql/compile` DROP lifecycle tests and full `pkg/vm/engine/disttae` package passed. Aptend's 128-batch/257-Rowid counterexample remains a linear scan in the direct offset path: each comparison is cheaper, but this source fact does not close the 2,000-table end-to-end gate.

## 14. Current-base validation, 2026-09-29

The measured base was `087000434fed`; the candidate was rebased onto it before the final measurements. Independent QA found and fixed a false incoming-FK rejection caused by a same-name View, a nil index-metadata panic, and redundant per-table FK catalog deletes. A focused CPU profile then attributed 630 ms of a 1,000-table DROP to success logs for the routine per-table branch/CCPR probes. Suppressing only those two probe success logs left the 25% gate open (1,000: 6.342→4.795 s, 24.392%; 2,000: 17.363→13.256 s, 23.654%). A new profile of that candidate attributed 590 ms to two auto-increment Delete INFO records per table. The final candidate retains the post-registration record and removes the pre-registration duplicate. DDL/TN operation and failed internal SQL logs remain.

For the final candidate, base and head used identical Go 1.26.4/native inputs, separate **fresh** one-CN data directories, matching catalog history per sample, and alternating order. Every sample recreated exactly the stated live table count, excluded setup time, checked `CONNECTION_ID()` before and after DROP with reconnect disabled, and found no database/table/column/index rows by saved database ID afterward. Raw DROP plus same-connection check seconds:

| Build | 1,000 tables | 2,000 tables | Medians |
| --- | --- | --- | --- |
| Base `087000434fed` | 6.380, 7.560, 6.893 | 19.999, 18.615, 16.535 | 6.893 / 18.615 s |
| Final candidate | 5.025, 4.943, 4.863 | 11.952, 12.490, 12.789 | 4.943 / 12.490 s |

Median improvement is **28.297% / 32.904%**, satisfying the declared two-scale 25% gate in this controlled run. The 2,000/1,000 median growth ratio improves from 2.701 to 2.527. Individual pairs still vary, so this is a measured acceptance result for the stated setup, not a production latency guarantee. Earlier nonpassing rounds remain in the QA ledger. The global SNAPSHOT-row convoy, historical two-hour disconnect, and #29400/#29457 integration are not established by this performance result; #27575 remains open.

After that measurement, `main` advanced to `466eb8ff4f6` with only Sirius substrait binding code/tests and its design document (#29209). The final branch was rebased onto it without conflicts. The complete production/test diff has the same SHA256 before and after rebase; compile/plan/incrservice package tests passed again. The table above retains its exact measured `087000434fed` base provenance rather than relabeling it as a fresh `466eb8ff4f6` benchmark.

## 15. Integrated RC delivery, 2026-09-29

The RC protocol from #29457 was integrated into #29393 and rebased onto exact base `c69187e0e3`. Fresh one-CN runs used Go 1.26.4, `GOAMD64=v3`, `GOEXPERIMENT=simd`, matching native inputs and temporary probe source, alternating base/candidate order, and three recreated databases per scale. Setup was excluded. Every sample checked `SELECT 1` and connection identity on the same pinned connection and found no database/table/column/index rows by saved database ID.

| Build | 1,000-table raw seconds | 2,000-table raw seconds | Median 1,000 / 2,000 |
| --- | --- | --- | --- |
| Base `c69187e0e3` | 6.698, 6.713, 6.804 | 16.418, 16.497, 16.529 | 6.713 / 16.497 |
| Integrated RC | 5.414, 5.360, 5.312 | 12.657, 12.665, 12.418 | 5.360 / 12.657 |

The integrated medians improve **20.2% / 23.3%**; the 2,000/1,000 growth ratio improves from 2.458 to 2.361. They do not reach the original 25% target. The user accepted the measured improvement together with the stronger slow-table concurrency behavior as the delivery criterion. In the public three-actor SQL regression, a `DROP TABLE` in an unrelated database finishes within 2 seconds while the target `DROP DATABASE` remains blocked behind a retained UPDATE table lock. That demonstrates independent progress, not a quantified production speedup for an arbitrarily slow table. The historical two-hour disconnect and ALTER-history global G(X) scope limit remain unresolved; #27575 stays open.

To isolate the RC integration cost, a separate alternating 1,000-table comparison on the same base measured the parent-only median at 5.490 s and the integrated median at 5.363 s (three fresh samples each). The integrated path was 2.3% faster in that bounded comparison; this is a regression control, not an additional gate or a claim about every workload.
