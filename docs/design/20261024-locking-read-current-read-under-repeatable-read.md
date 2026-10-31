# Locking Reads Perform Current Reads Under REPEATABLE READ

## Document metadata

| Field | Value |
|---|---|
| Owning issue | https://github.com/matrixorigin/matrixone/issues/30955 |
| Revision | 15 (2026-10-29) — approved and delivered; the review-and-delivery record below the decision log cites the exact reviewed revision and the implementation PR. The approved decision set is the inherited mechanism set of revisions 12-14 (rev-12 passed design review; rev-13 added the staged-delivery contract D-10 after its review blocked a partial rollout; rev-14 moved D-4's transfer mask into stage-1, completed D-1's explicit-role representation and closed the D-8 window) plus exactly two new decisions: **R15-F1** — the rev-3–14 cluster capability gate contradicted the repository's shutdown-only upgrade contract (`upgrade-compatibility.md`), so the allocated `MORPCVersion135` design and every below-gate/legacy/transition mechanism are DELETED; D-7 is rewritten as the single-version deployment contract, D-8 loses its capability-transition layers, and D-9/D-10/section 6-8 clauses are amended accordingly; **R15-F2** — D-5 gains the marked-retry persistence invariant: an already-recorded marked floor must surface its retry on every terminal path of the lock batch, so a later SKIP LOCKED direct conflict cannot erase it. Revisions 1-11 were each rejected by pre-PR design review; the complete decision chronology of every earlier revision is preserved in this file's git history, and this document now retains only the current state of each decision. |
| Status | **Approved and delivered** — the design-first phase is closed. Revision 15 was approved as a design BEFORE its implementation was delivered, per the requested design-first order: the independent pre-PR design review (the same read-only local design-review process that rejected revisions 1-11 and approved revisions 12 and 14; separate from this implementation and from the PR's implementation reviewers) reviewed exactly this revision's content on 2026-10-29 while the implementation was still the previously reviewed head `5415d839824` and returned PASS — verdict `approved=true`, verified Revision 15 at commit `6243db858e` with content SHA-256 `e2f7e37135778c60fe7f3604bd5d44974ff2e76fe1c186864247db5f8b3ae4c9`, recorded at `state/artifacts/issue-30955/pre-pr-review-1793297991115.json`, which states its own scope: "Approval covers this exact design, not implementation readiness or complete issue delivery." Both of its decisions are implemented, together with the stage-1 primary relation-read surface, in PR #31587 below: R15-F1's gate removal (the entire mixed-version/capability machinery is absent from both design and implementation at the delivered head) and R15-F2's marked-retry persistence. After delivery, the implementation received its own separate PR approvals at its pre-rebase head `8a705621ec` (iamlinjunhong, 2026-10-29: no remaining findings, including "Design revision 15's single-version deployment contract and retry-persistence changes are acceptable for its documented stage-1 scope") and at the rebased heads `8fc3801e8e` and `2b6dd91192` (the recorded mo-bot reviews, 2026-10-30). No approval-requiring decision in this document remains open. |
| Implementation PR / PR series | **PR #31587 — "fix: REPEATABLE READ locking reads current-read the latest committed value"** (https://github.com/matrixorigin/matrixone/pull/31587). One PR delivers the complete approved contract: the stage-1 primary relation-read surface plus R15-F1's gate removal and R15-F2's marked-retry persistence. Exact reviewed revision-15 content: preserved verbatim in this file's git history at commit `6243db858e` (pre-rebase) / `cec189c19e` (the rebased PR branch), content SHA-256 `e2f7e37135778c60fe7f3604bd5d44974ff2e76fe1c186864247db5f8b3ae4c9` — this exact content was design-approved before delivery (independent pre-PR design review, PASS 2026-10-29, `approved=true`, recorded verdict `state/artifacts/issue-30955/pre-pr-review-1793297991115.json`; full record in the review-and-delivery record below the decision log); delivered implementation heads `8a705621ec` → rebased `8fc3801e8e` (base main `97b2261005`), plus the design-record reconciliation head `2b6dd91192`. |
| Base revision | `d93d5d66c83d2927c233de17f21fe7f513bc7d32` |
| Design gate trigger | complexity trigger: adds a per-read visibility mode inside a fixed SI read view, materially changes retry/idempotency behavior, crosses plan/executor/compile/engine/moerr boundaries (≥2 subsystems), and adds additive plan/pipeline wire fields (rev-15 note: the change introduces no cluster capability version, so rev-14's gate-design clause is removed). Not an ordinary focused bug fix. |

Decision log (findings mapped to decisions, evidence verified at base
`d93d5d66c8`; the decision cell records the latest revision that owns the
finding — R5 rows supersede the specific mechanisms rev-4 chose where they
say so):

| Finding | Verified evidence at base | Decision (owning revision) |
|---|---|---|
| Rev1-F1/F2/F3/F4, Rev2-F1..F4, Rev3-F1..F4 (accumulated, resolved earlier) | see the corresponding sections; all re-verified or preserved | Retained resolutions: discovery-inclusive frontier, per-occurrence visibility, no SnapshotTS mutation, transfer-mask without workspace rewrite, wire-verified retry token |
| **R4-F1 frontier freshness** | `WaitLogTailAppliedAt(empty)` delegates to `timestampWaiter.GetTimestamp(empty)` (client.go:937-960), which immediately returns `latestTS.Next()` when the local latest satisfies the (empty) floor (timestamp_waiter.go:83-85) — no newer publication is acquired; a peer commit completed before statement admission but not yet published/applied to this CN is outside F₀, so initially-empty discovery misses it with no retry to recover. The alternative contract exists: `PushClient.AcquireLogtailReadBarrier` "obtains a TN-ordered publication frontier and waits until the normal CN logtail apply pipeline reaches it" (disttae/logtail_consumer.go:238-269; `acquireAppliedLogtailReadBarrier` 272-293 waits `GetTimestamp(frontier)` and validates `applied ≥ frontier`); engine interface + compile helper already in place (`engine.LogtailReadBarrier`, compile:`getLogtailReadBarrier`, used by lifecycle admission and CDC guards) | D-2: F₀ is acquired through `AcquireLogtailReadBarrier` at the start of compile source assembly — a publication frontier ordered after every commit that completed before statement admission, whose application this CN waits for before marked reads assemble sources; the retry admission floor stays a concrete timestamp (`WaitLogTailAppliedAt(max(requiredTS, Fₖ))`), where a non-empty floor genuinely waits. Deterministic delayed-application regression added |
| **R4-F2 scan-occurrence identity** | Right-oriented SEMI/ANTI/SINGLE joins are admitted (stats.go:2486-2500 sets `IsRightJoin` for SEMI/ANTI/SINGLE/LEFT/OUTER by stats) and `swapJoinChildren` swaps children afterwards (opt_misc.go:544-548), so final-plan child positions no longer express the block structure a post-optimization walk would rely on; scalar subqueries lower to LEFT/SINGLE joins reusing both subtrees (flatten_subquery.go:780-799, 861-867), so two occurrences of the same table (outer locked + subquery) share one `TableDef.TblId` — a final-plan table-identity pass would mark both or neither | D-1 rewritten: read-view identity is the **bind-time scan-node occurrence marker**, not a post-optimization structural walk; the bind tree (subquery subtrees distinct under SEMI/ANTI/SINGLE/MARK children, no orientation normalization yet) is where the collectLockTargets exclusion rules are meaningful. Marks travel with node objects through re-parent/swap rewrites automatically. The enumerated fabrication sites that create NEW scan nodes (apply_indices' two idx-table-node sites) must propagate the mark and set the additive `IndexScanInfo.ParentTableId`; other potential fabrication sites are enumerated and covered by a typed-plan occurrence corpus test. Right-join-swap and same-table-scalar-subquery controls added. **Capture mechanism superseded by R5-F1** — the walk-adjacent capture point was found to run after WHERE subquery lowering; the retained identity conclusion is one role per node object surviving all rewrites, now captured at construction time |
| **R4-F3 ordinary local readers ignore RelData tombstones** | `NewLocalDataSource` receives `relData.GetTombstones()` (txn_table.go:2668-2679 buildLocalDataSource) but `ApplyTombstones` and `GetTombstones` consume `extraTombstones` only when `ls.category == engine.ShardingRemoteDataSource` (local_disttae_datasource.go:1086-1094 and :1147-1158); ordinary local sources apply tombstones they compute themselves (workspace stages + pState stages at `ls.snapshotTS`). A mask appended only to the RelData set would therefore resurrect compaction-transferred rows on the ordinary local path | D-4 output becomes a **separate mask-only tombstone input** to the data source with two consumers: (a) the shipped/remote form appends to the relData set (applied by the existing ShardingRemote consumer, unchanged), and (b) the ordinary local source gets an additive mask input consumed in both `ApplyTombstones` and `GetTombstones` under explicit policy and ownership rules; Regressions through the ordinary local path (in-memory and S3-dumped deletes across compaction) mandated |
| **R5-F1 occurrence ownership must be established before WHERE subquery lowering** | `bindWhere` is invoked at query_builder.go:9222 and calls `flattenFilterSubqueriesGuarded` at :9369, which lowers WHERE scalar subqueries into the join tree *before* `collectLockTargets` runs at :9236. A scalar subquery with LIMIT 1 sets `ctx.hasSingleRow` (:10986), so :797-798 of flatten_subquery.go emits a plain `Node_LEFT` join (not SINGLE). `collectLockTargets` (:15027) excludes only SEMI/ANTI/SINGLE/MARK right children (:15038) and visits **both** children of every other join (:15074-15075), so the lowered subquery occurrence is reachable and would be marked by revision 4's capture-at-walk rule; JOIN-condition subqueries flatten at :14265 and HAVING subqueries at :11247/11272/11362/11552. Result: for `SELECT o.id FROM f o WHERE o.v > (SELECT i.v FROM f i WHERE i.id=1 LIMIT 1) FOR UPDATE` after f.v moved 100→999, marking both occurrences compares current 999 with current 999 instead of current 999 with snapshot 100 — G2 and MySQL violated. The binder's own block discipline is the correct pre-lowering ownership source: `baseBindSubquery` binds every subquery in a child `BindContext` (`subqueryNestingDepth+1`, base_binder.go:963-973) with `builder.isForUpdate = false` for the whole subtree (base_binder.go:981-1015); the same discipline guards CTE bodies (query_builder.go:5628-5635), view bodies (:12687-12705), and grouping/UNION arms (:4695-4704); a subquery's own locking clause re-enters through bindSelect's lock parsing (:6272-6288) | D-1 rewritten (again): ownership is a **construction-time per-occurrence role** captured at the single ordinary table-scan construction site `buildTable` (query_builder.go:13117, node built at :13400) from the live per-block locking state — role `OuterLocking` iff the scan is constructed under the outer locking statement's own block (`builder.isForUpdate` true at construction), role `NestedBlock` otherwise, with one explicit carve-out: view bodies inherit `OuterLocking` along the transparent FROM-expansion path (views must keep the base's nested-LOCK_OP suppression at :12701 while their scans stay marked, because `collectLockTargets` locks view-expanded scans today). `collectLockTargets` remains the LOCK authority, its walk and target set unchanged; it stops deciding marks entirely. The role rides the node object through re-parenting, `IsRightJoin` swaps, and scalar-subquery/LEFT lowering exactly as verified in R4-F2, and all fabrication rewrites propagate it (apply_indices sites; retained audit). Typed-plan and SQL controls mandated for WHERE and JOIN-condition scalar subqueries lowered to LEFT joins, including the same-table occurrence, plus lock-target-set conservation controls asserting the collected target list is byte-identical to base for every corpus plan |
| **R5-F2 cached and prepared executions need a per-execution admission and reset contract** | `Compile.Reset` (compile.go:287-368) reuses the retained physical generation verbatim ("Reset reuses an existing logical/physical generation"; no rebinding), so a cached prepared plan carries bind-time marks and any per-attempt state frozen from the compiling execution; `resetForReuse` drops the closed reader per execution (`DataSource.R = nil`, scope.go:304-307) and `Scope.Run` rebuilds readers through `getRelData`/`buildReaders` on each execution (scope.go:397-405, getRelData :1180 → `handleDbRelContext` re-opens the relation per execution), but `DataSource.Timestamp` stays compiled state — nothing in Reset re-stamps it, so an obsolete frontier from execution k would silently serve execution k+1 (a row committed between executions that matches the predicate is then missed by discovery *and* by lock acquisition, which produces no retry signal). Capability handling already has three verified seams that revision 4 didn't connect: (1) prepared EXECUTEs record `prepareStmt.protocolVersion` at bind (computation_wrapper.go:1610, mysql_cmd_executor.go:3253), compare with the live `currentProtocolVersion` on every EXECUTE and rebuild on ANY change in EITHER direction (`protocolMismatch` :1481-1490 → `preparePlanNeedsRebuild` :938-940 → `needRebuild` :1485, `cwft.planGenerationReused = !needRebuild` :1502; reused path calls `retComp.Reset` :626-631); (2) every Run re-validates cached plans against live negotiated capability ("Cached plans can outlive the negotiated cluster capability." compile2.go:451-459; cold twin :169-181) with validators that derive requirements from the plan object and reject with NotSupported "requires rebinding" errors (remoterun.go:2362-2387, e.g. the `MORPCVersion129` check :2383); (3) the session plan cache publishes per-entry `protocolVersion` (mysql_cmd_executor.go:6852-6868, skip on wrapper mismatch :6856-6863) and already eagerly evicts entries when a lower gate fails (cachedPlanForInput :4574-4583: "Evict eagerly while the rollout gate is closed … prevents an entry from surviving an observed protocol rollback and becoming eligible again after a later upgrade") | New **D-8 per-execution admission and reset contract** (below), integrated with D-2 and D-7: eligibility (does this plan express current-read marks) is a bind-time, immutable property of the cached plan; the **admitted read mode** is re-resolved per execution alone through D-8's effective-role resolution (rev 15, R15-F1: no capability input exists; this cell's rev-5 capability layer — the new Run-admission validator, the session-cache publish/evict guards and the capability-transition matrix — is deleted with the gate); F₀ is acquired fresh at **every** execution at the marked-read source-refresh boundary (cold: compile source assembly; reused: the per-execution reader rebuild driven by Reset) — never inherited across executions — and the prior execution's frontier, mask and retry-admission floors are retired in Reset alongside the existing per-execution resets (`c.retryTimes = 0` :335, `UpdateScopeTxnOffset` sweep :352-361). Mandated tests (rev-15 form): repeated prepared RR executions with intervening commits (the retained R-8 freshness members); the deleted transition-matrix members are not tests of this design. |
| **R6-F1 generated search scans escape the marked surfaces** | A Boolean MATCH predicate reaches `rewriteFullTextBooleanConsumers` / `applyFullTextBooleanComposition` (apply_indices.go:516-535), which fabricates separate search streams per leaf (apply_indices_fulltext_boolean.go:131 — LEFT-joined onto the surviving base scan), and the fulltext2 path builds a `Node_INDEX_SEARCH_SCAN` (buildFullText2SearchScan, apply_indices_fulltext2.go:181-223, node at :211) — with a covered variant that REMOVES the base scan and reads INCLUDE columns straight from the stream (apply_indices_fulltext.go:1303; `resolveFullTextIndexPath`/project-anchored descent and the SORT-anchored vector entry at apply_indices.go:542-548 contain no LOCK_OP awareness). At execution, `Scope.buildReaders` routes `Node_INDEX_SEARCH_SCAN` away from the relation path to `buildVectorIndexReaders` (scope.go:1904-1905), whose identity uses the unchanged transaction SnapshotTS (`currentSnapshot = s.Proc.GetTxnOperator().Txn().SnapshotTS`, scope.go:2127-2134); `compileIndexSearchScanDataSource` likewise stamps `DataSource.Timestamp = txnOp.Txn().SnapshotTS` (compile.go:5873); plugin reads borrow the same transaction (`txnForRun` clones only for named snapshots, vectorindex/sqlexec/sqlexec.go:206-219), and the fulltext2 plugin runs its index-load SQL via `sqlexec.NewSqlProcess(proc)` with the shared `VectorIndexCache`, keying the cache by the named-snapshot TS only (`sp.ApplyScanSnapshot` → `veccache.SnapshotKey`, fulltext2/plugin/search/search.go:714-718) and pinning the tail read at the txn SnapshotTS when no named snapshot exists (:763-767). **Empirical scope at base (probe3/3b/3c/4, this round):** direct `MATCH … FOR UPDATE` shapes are REFUSED — the project-anchored rewrite's descent stops at the LOCK_OP node, the raw `fulltext_match` survives (EXPLAIN: `Project → Lock → Table Scan` with the raw filter) and execution errors 20105 across all five tested shapes — while the same projection rewrites fine without FOR UPDATE; BUT a locking statement OVER a MATCH-bearing view rewrites below the Lock and EXECUTES with the search stream inside the locking plan (EXPLAIN: `Project → Lock → Sort → Join(Table Scan, Fulltext Index Scan)`), and deterministically returns the STALE match set after a peer commit (locking read returned 3|300 while 4|400 was committed and searchable; plain RR control 3|300; post-commit 3|300+4|400 — probe4-base.log). The review's exclusion scenario is live at base through this shape, and the SORT-anchored vector entry is equally Lock-invisible | D-1/D-3 extended: the fabrication audit and propagation helper cover the generated search scan family — classic fulltext streams (apply_indices_fulltext.go:458), Boolean-composition streams (:131), fulltext2 `Node_INDEX_SEARCH_SCAN` (:181-223, with `IndexSearchScan.SourceTable` identifying the base occurrence) including the covered path (role travels to the covering scan; its base-occurrence-less shape is conserved, see N6) and the vector project/SORT-with-order paths. D-3 gains surface D: marked search scans stamp `DataSource.Timestamp := F` per execution, `buildVectorIndexReaders` derives identity/coverage from that F, and the plugin executes with a TS pin at F through the EXISTING named-snapshot cohort (`ApplyScanSnapshot`/`EffectiveSnapshotTS` → `CloneSnapshotOp`), so index-load SQL runs at F AND the cache key becomes `SnapshotKey(IndexTable, F)` (frontier reads never reuse or pollute snapshot-keyed entries) with the tail read point at F. The review's demanded deterministic locking-MATCH regression is captured red on base through the live view-wrapped shape (probe4), with the direct-shape refusals conserved as regression assertions (20105 before AND after), and typed assertions that BOTH the base consumer and the search consumer receive F mandated. (**Pin route corrected by R7-F1/R7-F2, view mechanics further corrected by R8-F1/R8-F2: the named-snapshot cohort accepts only historical TS and a snapshot clone drops the transaction's own writes — F now has its own execution carrier, an ISOLATED execution view that shares write data only (not the complete workspace), and a distinct ExecutionKey cache family; marked ASYNC searches additionally require the per-execution completion contract**) |
| **R6-F2 table-sharding reads have a separate RPC boundary lacking F transport** | `txnTableDelegate.Ranges` splits local discovery (`rangesOnePart` + local uncommitted objects) and remote-shard discovery via `forwardRead` (txn_table_delegate.go:293-327), whose `shardservice.ReadOptions.ReadAt(...)` is fixed to the transaction SnapshotTS (:1377-1386); `getReadRequest` serializes ProcessInfo plus the TxnTable identity into `shard.ReadParam` (:1218-1245); the receiver `getTxnTable` decodes the Process payload, builds a fresh workspace and a `txnDatabase{op: proc.GetTxnOperator()}` (txn_table_sharding_handle.go:642-697), and builds its reader with `tbl.db.op.SnapshotTS()` (`readutil.NewReader`, :319-325) over a `ShardingRemoteDataSource` — so coordinator admission at F never reaches either the remote discovery or the receiver's reader, and the receiver has no way to recover the per-execution frontier. Activation verified: non-local when the table is sharded, the shard service is enabled and not all replicas are local (`isLocalFunc`, txn_table_delegate.go:1200-1209); existing harnesses: txn_table_delegate_test.go, txn_table_sharding_handle_test.go, txn_table_sharding_test.go | New **D-9 sharded-read transport contract (below)**: for marked occurrences the coordinator's `forwardRead` calls `ReadAt(F)` and the `ReadParam` carries the execution frontier additively; the receiver reconstructs its transaction as today but takes the request-scoped read view through the same clone/pin mechanism used by named snapshots (request-scoped `CloneSnapshotOp(F)` — the sqlexec cohort of R6-F1), so `tbl.db.op.SnapshotTS()` at reader build resolves to F for that request only (unmarked requests unchanged, byte-identical decode). The coordinator's D-4 mask for the marked relation rides the sharding request (additive param field) and is consumed through the EXISTING `ShardingRemoteDataSource` extraTombstones channel; local-part discovery applies the ordinary local mask input (D-4(b)). Capability: additive fields + ReadAt(F) appear only above the D-7 gate, receiver ignores absent fields below it; cancellation is statement-context bounded and propagates with the existing request teardown. Validation extends the three existing harness tests plus the public multi-CN sharded current-read witness. (**Ownership corrected by R7-F4: F and the mask attach to the constructed reader stream — streamReaders lease — not the request context; the receiver pin follows the R7-F1 execution cohort**) |
| **R7-F1 the named-snapshot pin rejects F ≥ SnapshotTS** | `SqlProcess.EffectiveSnapshotTS` returns nil for any TS that is empty or NOT strictly less than the transaction SnapshotTS (`if ts.IsEmpty() || !ts.Less(txnOp.Txn().SnapshotTS) { return nil }`, sqlexec.go:147-152, doc comment "or a TS not earlier than the current one"), so `txnForRun` keeps the original txn — a frontier from the barrier is by construction ≥ SnapshotTS and would be DISCARDED; `TestTxnForRunNonHistoricalSnapshotUsesCurrent` (snapshot_test.go:60-71) pins that behavior ("a non-earlier SnapshotTS must not clone"), and `vectorscan.Identity` admits only `spec.ScanSnapshot.TS.Less(currentSnapshot)` as historical (execution.go:265-267). Rev-6's route of F through the `ApplyScanSnapshot`/`EffectiveSnapshotTS`/`SnapshotKey` cohort is therefore unusable for this feature's F | D-3 surface D rewritten around a **separate execution-read carrier**: O-D7 is RESOLVED in this revision — F rides its own additive field on the search scan (plan `IndexSearchScan` + the Scope-wire DataSource), consumed by a NEW helper cohort `ApplyExecutionReadTS(F)`/`EffectiveExecutionTS()` that accepts F in every relation to SnapshotTS (typically after barrier admission F > SnapshotTS) and never rewrites `SnapshotTS`; the named-snapshot cohort keeps sole ownership of historical semantics and stays byte-identical (the admission that rejects non-earlier TS is untouched); `vectorscan.Identity` takes the execution TS as the read point for marked scopes beside the preserved historical case; cache keys split: `SnapshotKey` stays for historical reads and a NEW `ExecutionKey(IndexTable, F)` family serves marked executions, so current, historical and frontier entries can never collide; the tail/probe read point is `EffectiveExecutionTS()` for marked executions. Mandated UT: drive the ACTUAL pin helpers with F > SnapshotTS — the execution cohort pins while the named-snapshot cohort still refuses the same value (both behaviors asserted), plus key-distinctness and identity read-point assertions |
| **R7-F2 a snapshot clone drops the transaction's own writes from the search view** | `CloneSnapshotOp` binds `op.reset.workspace = tc.reset.workspace.CloneSnapshotWS()` (operator.go:792-806), and `CloneSnapshotWS` builds a fresh EMPTY read-only workspace — `tableCache`, `tableOps`, `deletedBlocks`, `batchSelectList` all newly allocated and empty, `readOnly.Store(true)` (txn.go:3414-3438) — the parent's rows, deletes and pending S3 tombstones are not carried. Classic fulltext maintenance writes index rows inside the SAME user transaction (`INSERT INTO index_table SELECT … FROM sink_scan CROSS APPLY fulltext_index_tokenize`, build_dml_util.go:7274 onward). Rev-6's snapshot-clone search read would therefore lose a transaction's own matching INSERT/UPDATE index entries (D-4's mask restores deletes only; it cannot restore missing inserts) | The frontier search read NEVER runs on a bare snapshot clone. New contract: the execution view = committed state at F (R7-F1 cohort) **plus the owning transaction's workspace overlay applied per execution as a read-only delta** (**mechanism superseded by R8-F1/R8-F2: sharing the COMPLETE workspace was found to retain the parent's snapshot-bound interned relations and require mutating the shared `Transaction.op` — the view is now an ISOLATED workspace/operator pair that shares only WRITE DATA read-only, and the "own index rows" overlay applies to the SYNC family only; the ASYNC family's overlay is per-execution SOURCE-row evaluation — see the R8-F1/R8-F2 rows and D-3(D)**). The shared `VectorIndexCache` stores COMMITTED-ONLY state (the `ExecutionKey` entries carry no transaction-private rows; own overlays are applied on top per execution and never stored or keyed into shared entries). Mandated locking-MATCH own-write matrix: own INSERT (new matching doc), own UPDATE (doc text rewritten to match), own DELETE (existing match removed) — each with plain-read controls and commit/rollback controls; plus a cache-pollution UT asserting no workspace row ever appears in a shared cache entry (**matrix restated by R8-F2 for the two families**) |
| **R7-F3 upstream already consumes version 130** | The integration target's tip defines `MORPCVersion130 = 130 // incomplete temporal string overloads and rejection diagnostics` with `MORPCLatestVersion = MORPCVersion130` on origin/main (commit 5a813a324d, #30311; const.go:169), while the design's own base still has `MORPCLatestVersion = 129` (const.go:168). An upstream peer at 130 implements temporal-string semantics and NOTHING of this design, so rev-6's fixed gate admits unsupported peers | **Superseded (rev 15, R15-F1):** the finding's evidence — an upstream-130 peer implements none of this design — motivated the rev-7 integration-time allocation `MORPCVersionLockingCurrentRead` ≥ 131 that revisions 3-14 adopted as the capability gate. Revision 15 deletes that entire resolution with the gate (D-7): MatrixOne is shutdown-upgrade-only, and additive proto fields ride the wire with no negotiation, so the current design consumes no version allocation and no peer-capability input |
| **R7-F4 shardingBuildReader hands the reader to a stream that outlives the request** | `HandleShardingReadBuildReader` registers `shardingRemoteReader{streamID, rd, release, deadline: now+StreamReaderLease}` into `streamHandler.streamReaders` (:336-347, `transferred = true`); `HandleShardingReadNext` consumes the retained `sr.rd.Read` (:404-412); cleanup runs through the lease-expiry sweep (:95-97) or explicit release in ReadClose (:58-59). A request-context-scoped pin/mask (rev-6 D-9 wording) would be torn down at build end while the stream continues | D-9 ownership rewritten to the READER-STREAM lifetime: F and the mask become immutable attributes of the constructed `shardingRemoteReader` record, created at BuildReader from the transported request values, owned by the stream under the EXISTING lease/deadline mechanics, consumed by every ReadNext, and released only on the stream's existing terminal paths (ReadClose, lease sweep); a lost BuildReader response (coordinator gave up after the receiver built) leaves the stream to the existing lease sweep exactly as today; coordinator cancellation drops its side and the receiver stream terminates by the existing lease. Mandated lifecycle UTs crossing actual RPC boundaries: build → next(s) → close; build → abandoned → sweep; next-after-cancel — asserting pin immutability, per-ReadNext mask usage and zero leaked state after terminal cleanup |
| **R7-F5 the retry-token fallback weakens the guaranteed frontier** | Rev-6 D-5 said "Degraded-unparseable transport falls back to the local applied frontier with bounded extra retries": the coordinator's local applied frontier can LAG the remote commit that caused the retry, contradicting D-2's `max(requiredTS, Fₖ)` admission; and `timestampWaiter.GetTimestamp` returns `latestTS.Next()` whenever the (satisfied) floor permits with no application-progress condition (timestamp_waiter.go:83-85), so repeated calls need not advance and the claimed "bounded extra retries" has no independent bound | D-5 fallback REMOVED. A malformed or missing retry-frontier token is TERMINAL for the statement: tokens are validated before admission (structure parse and typed-TS sanity), and an invalid token fails with an explicit moerr ("invalid locking-read retry frontier"), never a silently weaker frontier; spin-freedom is unconditional: each admitted retry's floor must be STRICTLY greater than the previous frontier (`F_{k+1} = max(requiredTS, Fₖ) > Fₖ`), and when the computed floor is not strictly greater the statement terminates with an explicit error (no re-run, independent of waiter progress); `SnapshotTS` is never mutated on any exit path. Mandated fault UTs with a lagging coordinator/waiter stub: invalid token → terminal error; non-progressing validated tokens → terminal after the first non-progress admission; no mutation and no spin in any case |
| **R8-F1 sharing the complete workspace retains snapshot-bound readers and forces parent mutation** | `txnDatabase.getTxn` is `db.op.GetWorkspace().(*Transaction)` (txn_database.go:47-48), so every database obtained through a workspace resolves THAT workspace's `*Transaction`; `getRelation` returns the workspace-INTERNED relation (`txn.getCachedTable`, txn_database.go:133-136), and under SI `getCachedTableByKey` returns the cached delegate unchanged (`if txn.op.IsSnapOp() || !txn.op.Txn().IsRCIsolation() { return tbl }` — "keep it as it is", txn.go:2968-2978); `NewLocalDataSource` pins its read point from the RELATION's own operator — `source.snapshotTS = types.TimestampToTS(table.db.op.SnapshotTS())` (local_disttae_datasource.go:83) — not from any operator clone the execution holds; and `BindTxnOp` is a plain write of `txn.op = op` on the shared `Transaction` (txn.go:3441-3442). Rev-7's view ("CloneExecutionReadOp(F) … SHARES the ORIGINAL workspace read-only") would (a) hand the marked execution the parent's interned relations — snapshot-bound at the parent SnapshotTS, so the correctly pinned clone is never consulted by the datasource — and (b) to make shared relations read at F it would have to rebind the shared op, mutating the parent's operator for every other parent reader | The execution view becomes an **ISOLATED pair, sharing only WRITE DATA** (rev-8, D-3(D)): `CloneExecutionReadOp(op, F)` constructs a NEW operator + NEW `Transaction` (the `CloneSnapshotOp`/`CloneSnapshotWS` seam, operator.go:789/:798, txn.go:3414-3438, the `client.Workspace` interface types.go:303+). PRIVATE to the view: its `op` (the execution cohort accessor, `EffectiveExecutionTS() == F`, `SnapshotTS()` = parent's, never rewritten), its `tableCache` (NEW EMPTY — relations used by a marked execution are constructed through the VIEW's database so `db.op` resolves F and `local_disttae_datasource.go:83` pin follows; the parent's warmed cache and its snapshot-bound relations are never consulted, accepted bounded per-execution relation-reconstruction cost), its derived containers (`batchSelectList`, `cnObjsSummary`) and `readOnly = true` (writes rejected, as today's read-only clone). SHARED READ-ONLY from the parent, bounded at view construction (the parent's append-only `writes` prefix `[0:endOffset)` captured like `snapshotWriteOffset`, `deletedBlocks` and the `tableOps`/`databaseOps` chains): own writes remain visible at F without any copy. `BindTxnOp` is NEVER used by the view: the pair is wired at construction (field assignment), so the parent's `Transaction.op` and the parent's cached relations are identity-stable across the execution. Cleanup: the view is created after the barrier at the marked execution's source assembly (cold compile / the reused-path per-execution rebuild, D-2/D-8) and destroyed at that execution's retirement (reader teardown / Reset sweep); borrowed containers are owned by the parent and never mutated; nothing of the view survives the execution. Mandated REAL disttae consumer UT (`CreateEngines` harness, not helper-only pins): warm the parent's relation cache, construct the view with F > SnapshotTS, read through the view's relation — the newer committed row AND the transaction's own writes are returned and the view datasource's read point equals F — then assert the parent's subsequent ordinary RR read is byte-identical to before (same interned relations, unchanged `SnapshotTS`, unchanged `Transaction.op` identity, no parent `tableCache` growth). (**Borrow-read discipline refined by R9-F1: the actual readers lazily MUTATE borrowed entries and charge through the receiving transaction, so the "read-only prefix" borrow requires entry-clone-on-first-mutation, view-side accounting and a pin-ledger teardown — see the R9-F1 row; and by R11-F1: the shared `tableOps`/`databaseOps` chains carry parent-owned HANDLES whose accessors return and proc-mutate the payloads — the view consults them as predicates only and constructs view-local handles — see the R11-F1 row; and by R12-F1: that shared enumeration (writes prefix, `deletedBlocks`, chains) still missed the flushed-tombstone registry — a DISTINCT visibility input whose consumers resolve it through the relation's owning transaction while `CloneSnapshotWS` starts the view's copy empty (txn.go:3430) — see the R12-F1 row**) |
| **R8-F2 marked async searches are incomplete at F (committed sources trail the index; own writes have no postings)** | fulltext2 is ALWAYS-async: "an ISCP consumer tails the base table and appends segments. The index therefore trails the base table by some lag" and the watermark advances in the SAME transaction as the segment INSERTs (fulltext2/plugin/coverage/coverage.go:16-27); a loaded search = base segments + `LoadTailSegmentsWithin(…)` — only CDC-tail rows ALREADY appended are visible (search_cache.go:174-192), so a source row committed before F can have NO posting in any loaded generation; async DML maintenance exits before building any in-transaction index rows (`IsAsync … if async { return nil }`, build_dml_util.go:7237-7244 — the transactional index-row branch at :7274+ is sync-only), so the transaction's own INSERT/UPDATE/DELETES have no transaction-local postings either; the self-completing tail runs only for injected JSON probes (`u.probeTail = q.JSONProbe && u.tblcfg.ProbeTail`, search/search.go:763), and user MATCH is freshness-tolerant (plugin.go:75-79). The async family is broader than fulltext2: `indexplugin.IsAsync` = always-async algorithms (hnsw/bm25/cagra/ivfpq/fulltext2) OR `async=true` on a normally-synchronous engine (indexplugin/async.go:31-56). Rev-7's "pinning index-load SQL and covered output to F supplies current visibility" therefore holds for synchronously maintained indexes only; for async indexes the marked locking MATCH misses rows through F (G2 violated) | Two-family contract (rev-8, D-3(D)): SYNC-maintained indexes keep rev-7's mechanism (the ISOLATED execution view alone is sufficient — index rows are commit-atomic and own index rows ride the shared write data). ASYNC (`IsAsync`) marked searches additionally run a **mandatory commit-covered completion per execution**: generation capture unchanged (searched build_ts under the cache lock); coverage is evaluated at the execution's F through the coverage plugin's bar/watermark protocol (the coverage.go ordering property — a persisted watermark never ahead of durable index data — keeps a conservative decline sound); then, mirroring the verified probeTail cases (search.go:476-519): (a) generation NEWER than the F-physical bound (shared-cache pollution guard) or gap crossing a DDL → CURRENT-READ FALLBACK: the match set is emitted from a source scan AT F through the execution cohort with the match/distance predicate evaluated at runtime on source rows (complete over source state ≤ F; replaces index selection; rare); (b) generation BEHIND → the gap `(searched, F]` completes by source-change evaluation: `table_changes(searched, F]` inserts evaluated against the index's match predicate at runtime, deletes suppress pks; (c) generation COVERED at F → no completion. The transaction-local overlay (both cases) reuses the view's shared write window: the transaction's own SOURCE rows are evaluated by the same runtime match predicate (INSERT add, UPDATE delete-then-add, DELETE suppress) — async own-write visibility comes from SOURCE rows, since no own postings exist. Covered output (INCLUDE columns) for completion/fallback/own-write rows resolves at F through the same view. Per-execution renewal: coverage/completion re-evaluate on every execution against that execution's F; completions/overlays are per-execution private (shared cache stays committed-only); UNMARKED searches — plain MATCH, JSON probes, plan reuse, `PreparedPlanDependsOnIndexCoverage` — keep their existing freshness-tolerant contract byte-identically. Mandated: controlled-CDC-lag UTs (harness-constructed generations with build_ts < F and known gap rows, cf. search_cache/cdc test patterns) asserting completed marked results and unchanged unmarked results, plus the own-write locking-MATCH matrix for BOTH families with commit/rollback controls and ordinary-read controls. (**Case (b) incremental completion superseded for RANKING-SENSITIVE shapes by R9-F2: scored/ranged/limited marked searches complete by full re-evaluation at F; only rank-insensitive shapes keep incremental gap completion — see the R9-F2 row**) |
| **R8-F3 an occurrence can carry an explicit historical snapshot inside a locking statement — no precedence defined** | `buildTable` appends the scan with `ScanSnapshot: snapshot` (query_builder.go:13666), `collectLockTargets` has NO historical-snapshot exclusion (every TABLE_SCAN with a Pkey becomes a lock target, :15046-15069), and compilation routes such scans through the historical clone cohort when the TS is non-zero and `Less(SnapshotTS)` (compile.go:5609-5622) — so an `AS OF` table source under `FOR UPDATE` is simultaneously a lock target and a historical reader today, while rev-7 D-1 marked EVERY scan constructed under `isForUpdate` with no snapshot condition. The design's marked carrier and the named-snapshot cohort are orthogonal transports, so an unruled implementation could override the explicit snapshot with F at the relation hop but not the search hop (or vice versa), or drop the lock | Eligibility-and-precedence rule (rev-8, D-1/D-3): an occurrence whose table source carries an explicit historical snapshot that ENGAGES the historical cohort (TS non-zero and strictly earlier than the transaction SnapshotTS — exactly the compile.go:5613-5618 admission) is assigned the explicit role `HistoricalSnapshot` at construction time — INELIGIBLE for current read: it is never marked `OuterLocking`, never receives F (no execution cohort, no `ExecutionKey` entry, no mask, no sharding execution fields, no async completion), and reads at its named snapshot through the EXISTING historical cohort (relation reads via the clone, `SnapshotKey` cache family, search sources with a named snapshot stay on `sp.EffectiveSnapshotTS` byte-identically), uniformly across relation, search, cache and wire consumers; the LOCK is conserved exactly as today (`collectLockTargets` unchanged — the target for the snapshot-bearing scan is still formed; LockOp still executes over the rows read at the named snapshot). A snapshot clause that does NOT engage the historical cohort (equal/later TS) leaves the occurrence eligible; the two cohorts stay disjoint by the same admission rule both already use. Rationale: the explicit user snapshot is the more specific visibility request; MatrixOne conserves its existing combined behavior instead of redefining or refusing it. Mandated: typed-plan corpus members (AS OF source under locking → role `HistoricalSnapshot`, no F delivered to any consumer, lock target conserved; snapshot-bearing VIEW body; a named-snapshot search source — the execution cohort NOT engaged) and public SQL controls (locking `AS OF` returns the historical value while a subsequent eligible locking read returns current, view-wrapped `AS OF`, lock still held). (**Evaluation TIMING corrected by R9-F3: the SnapshotTS comparison is per-execution against the EXECUTING transaction (compile.go:10854-10866); the bind-time artifact records presence facts only and the effective role resolves at the per-execution admission boundary — see the R9-F3 row**) |
| **R9-F1 actual workspace readers lazily MUTATE borrowed entries across owner boundaries** | The exact-PK point probe calls `ls.table.getTxn().ensureInsertKeyOffsetsLocked(&writes[ls.wsCursor])` (local_disttae_datasource.go:757) — the receiver is the relation-owning workspace (the VIEW for view-built relations), the entry is the BORROWED parent one; `ensureInsertKeyOffsetsLocked` writes `entry.keyOffsets` into the entry and, via `accountInsertKeyOffsetsLocked`, sets `entry.keyIndexAccountedSize` and charges `txn.workspaceSize`/`txn.approximateInMemInsertSize` of the RECEIVING transaction (txn.go:242-275); `unaccountInsertKeyOffsetsLocked` (txn.go:310-323) is the single refund choke point for batching mutations, entry release, workspace truncation, ALTER transfer and transaction teardown — so a view probe mutates the parent's ENTRY while charging the VIEW's balances, and the parent's later invalidation/teardown refunds bytes it never charged; `readOnly` is consulted only by Commit (txn.go:80/:3003) and does not guard this path. O-D8 had left the borrow representation open | The borrow becomes **immutable under the view by construction**: at the view's reader boundary every borrowed `Entry` that a reader would lazily mutate is FIRST CLONED (shallow clone sharing the batch vectors read-only, private `keyOffsets`/`keyIndexAccountedSize` tail) into view-private storage — hydration then writes the CLONE and the charge lands in the VIEW's balances through its own `accountInsertKeyOffsetsLocked` receiver; clones are unreachable from the parent's writes slice, so no parent refund path ever touches them and all view charges die at retirement. Clone scope is bounded to the entries the view actually point-probes (not the whole prefix). Shared batch data: the parent's `writes` slice is append-only and the captured prefix `[0:endOffset)` keeps a stable backing array (Go growth reallocates only the parent's slice header); batch VECTORS are never written by view readers; every pin/retain a view reader takes on shared batches/data objects is recorded in a VIEW-PRIVATE PIN LEDGER and unpinned exactly there at retirement — the view never releases a pin it did not take and never drops one the parent owns (non-owning teardown). Mandated cold exact-PK read through the view (own INSERT with an unhydrated index entry at construction): the read returns the row correctly through the clone, and the parent's entry `keyOffsets`/`keyIndexAccountedSize`/`workspaceSize`/`approximateInMemInsertSize` are unchanged through view retirement AND through the parent's subsequent commit and rollback. O-D8's borrow-representation half is RESOLVED to this discipline. (**Synchronization refined by R10-F3: parent-side readers can lazily hydrate the same borrowed entries under the PARENT mutex while the view borrows under its own — the borrow now executes under the parent transaction mutex with copy-then-release and explicit lock ordering; see the R10-F3 row and D-3(D)**) |
| **R9-F2 incremental gap completion cannot repair scored/limited marked searches** | fulltext2 scores are corpus-GLOBAL — `globalStats` carries N, average doc length and per-term document frequency (fulltext2/index.go:355-376; the comment: "appending a CDC tail shifted a doc's score and could drop a globally-top-k doc at the merge"); the engine applies a pushed `ScoreRange` INSIDE the search ("out-of-range rows never cross into the join above", search/search.go:733-747) and a pushed LIMIT runs WAND top-K candidate limiting during the search (search/search.go:810-825, "SearchInto"), discarding runners-up before any completion could see them; the JSON-probe self-completion appends score-zero candidate rows for a later base recheck (search/search.go:421-470) and therefore never establishes ranked MATCH correctness. Rev-8's case (b) ("changed rows evaluated against the match predicate at runtime; inserts emit, deletes suppress") is sound only for statistics-free shapes: a gap commit shifts UNCHANGED documents' scores (a score-range rejection at generation G cannot be repaired by evaluating only changed rows), and suppressing a deleted bulk top-1 cannot recover the already-discarded runner-up | The marked-ASYNC completion splits by **ranking sensitivity, resolved at plan build** (the same execution that builds the search stream knows its pushed range/LIMIT/score-outputs): (RANK-INSENSITIVE — no pushed ScoreRange, no pushed LIMIT/top-K, no score column in the output and no score-bearing ORDER BY) all matching candidates emit and the emitted SET is statistics-free, so rev-8's incremental gap completion + own-SOURCE overlay stands (gap matches appended, own INSERT add / UPDATE delete-then-add / DELETE suppress, dedup/replacement by pk); (RANKING-SENSITIVE — any of the above present) a lagging generation is completed by **RE-EVALUATION AT F**: one full ranked evaluation over the F-complete source view with F-consistent corpus statistics — text indexes two passes (pass 1 computes N/df/avgDocLen over the F-visible corpus; pass 2 matches, scores, re-ranks), vector indexes a single pass (distances need only the query and doc vectors) — and score range, ordering, top-K truncation and pk dedup are applied ONCE at the end of the merged candidate set, so no downstream stage ever sees an intermediate set; candidate replenishment is unnecessary because re-evaluation considers every F-visible row before any limiting. Covered-at-F generations keep the shared bulk (statistics consistent). The re-evaluation path is the SAME mechanism as rev-8's current-read fallback, so case (a) (newer-generation/DDL-span) and case (b)-ranking-sensitive unify: correctness by re-evaluation, and the incremental path remains only for the rank-insensitive family. Mandated completed-reference corpus controls: a gap commit flipping a doc across the score range; a gap delete removing the bulk top-1 with an unchanged runner-up that must be ranked in; own UPDATE replacing a bulk winner; covered-path INCLUDE controls — each asserted EQUAL to a reference computed over the explicitly-built F-complete corpus (scores, ordering and truncation included); plus a mode-elision member (rank-insensitive stays incremental and correct) and unchanged UNMARKED searches against the same lagging generations  (**Covered shortcut and qualification superseded by R10-F1/R10-F2: case (c) now additionally requires an EMPTY own-write window for the source, the SourceCommitTS fail-closed outcome routes to complete evaluation, and the complete pushed search request — IncludePredsJSON and FilterBytes, not just the pattern — is re-applied on every candidate source before final ranking/truncation; see the R10-F1/R10-F2 rows and D-3(D)**) |
| **R9-F3 SnapshotTS-crossing reuse makes the frozen HistoricalSnapshot role stale** | The existing reader decides historical-cohort engagement at DATASOURCE BUILD against the EXECUTING transaction's snapshot (`node.ScanSnapshot.TS.Less(c.proc.GetTxnOperator().Txn().SnapshotTS)` → clone operator — compile.go:10854-10866 and the twin site :5609-5622); `shouldRebuildPreparePlan` (computation_wrapper.go:2834-2853) and the per-EXECUTE rebuild rules (:1481-1502: `needsRebuild`, mode mismatch, `rebuildEveryExecute`, `protocolMismatch`) react to schema, FK, UDF, subscription, JSON-probe coverage and protocol — NONE to the executing transaction's SnapshotTS. Rev-8 captured the role at CONSTRUCTION against the BINDING transaction's SnapshotTS and D-8 froze it, so a plan bound at S0 <= H (eligible, marked) reused at S1 > H carries the stale eligible role while the reader now takes the historical clone — historical relation reads could coexist with execution-frontier search/cache/wire state | Provenance and eligibility are **SPLIT**: bind-time, immutable in the cached plan — the capture helper records ONLY presence facts (`locked block provenance` + `explicit snapshot clause present with TS H`; no comparison, no mode); execution-time, per-execution — at the per-execution admission boundary (D-8 layer-3 position, BEFORE any marked-consumer setup and before the statement's D-2 barrier acquisition) each candidate resolves its EFFECTIVE mode with the SAME predicate the reader uses, against the EXECUTING transaction's SnapshotTS: engaging iff H is non-zero and Less(execSnapshotTS) → effective `HistoricalSnapshot` (ineligible: historical-cohort consumers, lock target unchanged); else effective `OuterLocking` (eligible: marked consumers at this execution's F). BOTH transition directions (S0 <= H → S1 > H: eligible → ineligible; S0 > H → S1 <= H: ineligible → eligible) resolve per execution by the same rule. The effective mode lands on the EXECUTION's materialization (the per-execution scope/DataSource attributes created in the source-refresh sweep) and dies with the execution under D-8's retirement discipline — the cached plan's provenance is never rewritten, no rebuild is forced, no schema/protocol field changes. The statement acquires the D-2 barrier iff at least one occurrence resolves eligible (ineligible-only executions stay on today's path with no barrier and no F). Mandated cached/prepared matrix crossing S0 <= H < S1 and the reverse: per execution, eligible delivers F to relation AND search consumers; ineligible delivers F to NONE while the historical clone engages exactly as today; lock-target list byte-identical across both executions; view-body and named-snapshot search-source members included; the bind-time corpus asserts presence facts only |
| **R9-F4 per-frontier ExecutionKey turns every F into a cold full load with its own mapping and can refuse under budget** | An exact-key miss runs `Preload` → governor `makeRoom` → full `Load` (pkg/vectorindex/cache/cache.go:1246-1290, "Refuse before Load allocates anything" under overload); fulltext2 entries hold their own whole mapping per key ("The mapping is not shared between cache entries: LoadFromStorage spills to a fresh LOCAL file per load and mmaps it whole, so a second named-snapshot key of the same index maps its own copy … N generations could pin N files", search_cache.go:220-236; base postings are per-entry mmap views :63-70); the governor REFUSES a load that cannot fit with live entries un-reclaimable (governor.go:438-493 "index cache is full … retry, or raise the … budget"). Rev-8 keyed marked entries by (IndexTable, F) while D-2 renews F per execution — N concurrent executions on one unchanged index could pin N resident copies, and a budget-constrained second load would FAIL a locking read that succeeds today | The marked-cache family is **re-keyed by the LOADED GENERATION, not by F**: entries carry (generation identity, captured coverage bound — the highest source commit the load covered, captured atomically with the load — and the committed-only payload), all under the ALLOCATED gate. An execution at F: (1) accepts any resident entry whose captured bound covers F per the coverage protocol; (2) entries are SHARED by every execution whose F they cover, so consecutive/reused-plan executions of an unchanged index share ONE resident entry (no O(F) key space, no per-frontier copies); (3) an F beyond the resident bound triggers ONE fresh load at the execution read point (the isolated view makes the load land at F) under the existing LoadOrStore coalescing — concurrent F's beyond the old bound share that load; (4) retirement: an execution holds its entry under the EXISTING cache read-lock discipline for the stream duration and releases at read close; generation replacement happens only through the existing idle-reclaim/TTL policy with pinned entries un-evictable, so resident multiplicity is bounded by (index table, live-generation) — one per index in steady state, +1 transiently only while a pinned older generation coexists with a fresh load. **Refusal degradation (availability rule):** if the governor refuses the fresh load, a MARKED execution never fails for cache capacity — it degrades to the R9-F2 F-source re-evaluation, which needs no index entry (correct results at fallback cost); UNMARKED executions keep today's refusal error unchanged. D-6 gains the accounting (one shared generation entry per index; one full load per generation whose coverage first reaches an admitting F; no per-frontier copy cost remains). Mandated: load-count UTs (repeated AND concurrent marked executions on one unchanged index → exactly one load/one resident entry shared; F advance within coverage → no reload; F advance beyond → exactly one reload, old entry retiring through idle-eviction once unreferenced), a constrained-budget concurrency test (two marked executions needing distinct generations with a budget admitting one → second degrades to source re-evaluation with correct results, no statement failure), and a recorded performance comparison (resident bytes, load counts, latency) against today's shared bare-key model in the same harness scenario |
| **R10-F1 the covered-at-F shortcut loses ranked candidates under transaction-local writes; the coverage bar itself fails closed on them** | `SourceCommitTS` FAILS CLOSED on transaction-local writes: "Transaction workspace writes intentionally fail closed: they are visible to the query but cannot have reached ISCP yet" — `if tbl.hasTransactionLocalWrites() { return types.TS{}, moerr.NewInternalErrorNoCtx("source commit ts is unavailable with transaction-local writes") }` (source_commit_ts.go:28-40, scanning `txn.writes` under the lock :64-74); with a pushed LIMIT the reader truncates inside `SearchInto` (search/search.go:810-825 — discarded runner-ups unreachable by any later overlay); scoring is corpus-GLOBAL (fulltext2/index.go:355-376 — own corpus members shift unchanged documents' scores). Rev-9 case (c) said "generation COVERED at F → … no completion" and item 3 applied own INSERT/UPDATE/DELETE as a set overlay: with committed matches A and B and pushed LIMIT 1 the bulk emits only A; an own DELETE of A suppresses it and B is UNRECOVERABLE; own INSERT/UPDATE invalidate the bulk's corpus statistics; rev-9 had no own-write condition on the covered shortcut and no route for the SourceCommitTS refusal | Ranked admission qualifies against the **COMPLETE transaction-visible corpus** — committed state at F ∪ the transaction's own source writes — INDEPENDENT of committed coverage (rev-10, D-3(D)): the covered shortcut in case (c) applies ONLY when BOTH (a) committed coverage reaches F AND (b) the view's borrowed write window contains NO source-affecting writes for that index (an instant empty-check over the bounded prefix; `IsAsync` indexes have no in-txn postings, so the check is on SOURCE-table rows); when own writes are present: RANK-SENSITIVE shapes run the COMPLETE evaluation of R9-F2 with the corpus statistics and candidate set INCLUDING the own writes (pass 1 counts own docs into N/df/avgDocLen; pass 2 scores the union — an own DELETE removes the bulk winner so the truncated runner-up re-emerges; own INSERT/UPDATE participate in scoring and ranking), RANK-INSENSITIVE total-set shapes keep the incremental overlay (own add/remove is exact for sets). The `SourceCommitTS` fail-closed outcome is routed as "coverage UNDECIDED", never a user error and never a silently weaker covered decision: the execution falls to the same complete-evaluation (ranking-sensitive) or incremental-with-overlay (rank-insensitive) path. Mandated reference members: covered-generation own DELETE of the (LIMIT 1) winner with the runner-up recovered; winner updated to a nonmatch; own-write score-range changes (an unchanged doc crossing the range through own-corpus growth); the SourceCommitTS-refusal member (a transaction-local write makes the bar unavailable yet the marked result is still completed-correct) |
| **R10-F2 pushed qualifications vanish above the search — completion must re-apply them** | The fulltext2 INCLUDE/pk prefilter pushdown PEELS those predicates out of `scanNode.FilterList` into the predicate JSON "which the fulltext2 search reader evaluates against the stored per-doc values inside the WAND walk" (`scanNode.FilterList = residual`, apply_indices_fulltext.go:652-667), and the covered plan runs "All qualification … inside the search" with the MATCH filter removed from the base scan (:1287-1309); the search request carries them separately from the pattern — `Fulltext2Query{…, FilterBytes: u.filterBytes (:753), IncludePredsJSON: includePreds (:754)}` (search/search.go:742-755). Rev-9's completion contract evaluated only the "match/distance predicate" on gap/replacement/own rows and resolved INCLUDE VALUES at F for OUTPUT — a token-matching gap or own INSERT whose include_value=0 under a query requiring include_value>10 would emit as a FALSE POSITIVE (nothing above re-checks) | One **execution qualification contract** (rev-10, D-3(D)): the COMPLETE pushed search request — the pattern AND the peeled INCLUDE/pk predicates (`IncludePredsJSON`) AND the runtime membership filter (`FilterBytes`) — is applied UNIFORMLY to every candidate source: bulk emissions (already qualified in-search), gap `(searched, F]` rows, current-read replacement rows, and own-write rows, each evaluated against its INCLUDE values resolved AT F through the isolated view; qualification runs BEFORE the final one-shot ranking/truncation (R9-F2) and before the score range, so no false positives can cross into the join; the completion evaluator consumes the request's `IncludePreds`/`FilterBytes` fields directly (no re-derivation from residual FilterLists, which may be empty). Mandated reference members: a token-matching gap/own INSERT failing the INCLUDE predicate (include_value=0 under a query requiring >10 — the false-positive counterexample); an UPDATE crossing the predicate boundary (INCLUDE value moving across); runtime-membership failures on BOTH the join path and the covered path |
| **R10-F3 parent-side lazy mutation races the view's borrow — no shared lock** | The workspace reader path takes the OWNING transaction's mutex (`ls.table.getTxn().Lock(); ls.rc.WorkspaceLocked = true …` deferred unlock, local_disttae_datasource.go:647-655), reads and caches `writes` entries under it (:717), and hydrates under it (:757), with `accountInsertKeyOffsetsLocked` writing `entry.keyIndexAccountedSize`/`entry.keyOffsets` under that lock (txn.go:269-274); the view's relations resolve the VIEW's `*Transaction` and its DIFFERENT mutex (txn.go:3414-3438), and parallel pipeline fragments of one statement give real concurrency between parent-side readers (unmarked occurrences of the same source table, other readers of the same workspace) and the view's borrow/probe. Rev-9 blocked VIEW-side mutation of parent state but left borrowed entries as parent-mutable data unsynchronized against view reads; stable backing arrays and the pin ledger do not close it; the cold-probe test was sequential only | The view's borrowed-entry access runs **under the PARENT transaction's mutex** (rev-10, D-3(D)/R9-F1 refinement): snapshot/clone/probe of any borrowed entry executes inside `parent.Lock()` exactly as ordinary parent readers take it, COPYING the needed entry state (the entry's stable fields, plus hydrated `keyOffsets` if present, plus the clone target) under that lock, releasing BEFORE any view-private state is exposed or mutated (`WorkspaceLocked`-per-source semantics preserved for the view side). Lock ordering: the parent mutex is strictly OUTERMOST whenever both could be taken; the view never holds its own mutex while acquiring the parent's, and parent readers never touch view locks — so no inversion exists. The pin ledger records pins taken on shared batches under the parent mutex; retirement unpins through the objects' atomic refcounts (no parent wait) and disposes clones/ledger privately; view-side accounting still lands in the VIEW's balances after the copy (conservation unchanged). Mandated: a barrier-controlled CONCURRENT member — a parent-side reader hydrating the same entry in a parallel goroutine (the shape parallel fragments produce) while the view borrows/probes — run under the race detector, asserting no race, correct results, and accounting conservation across the concurrent window and retirement |
| **R11-F1 the shared create/drop chains carry parent-owned HANDLES, not just visibility — the private tableCache does not isolate them** | `dbOpsChain.existAndActive` returns `x[len(x)-1].payload` — the chain's stored `*txnDatabase` OBJECT (txn.go:3479-3487) — and the twin `tableOpsChain.existAndActive` returns the stored `*txnTable` (txn.go:3552-3559); `Engine.Database` returns that parent database directly after the deleted check (`if v := txn.databaseOps.existAndActive(key); v != nil { return v, nil }`, engine.go:502-518); `txnDatabase.relation`'s created-table branch returns the PARENT `*txnTable` AND MUTATES its proc pointer (`v.proc.Store(p); return v, nil`, txn_database.go:125-129) BEFORE the `getCachedTable` lookup — so rev-10's private EMPTY tableCache never intercepts this branch: a transaction-created table's relation would resolve `db.op` to the PARENT, bypassing the view's read point (local_disttae_datasource.go:83) and the view-owned hydration/accounting of R9-F1, and the view would mutate the parent handle. The planned consumer tests covered warmed caches and own DML only | The view resolves transaction-local metadata through **VIEW-LOCAL handles** (rev-11, D-3(D)): the shared `databaseOps`/`tableOps` chains are consulted as PREDICATES ONLY (existAndDeleted/existAndActive answered without taking the payload), preserving create/drop visibility exactly; when a chain marks a database or table created in this transaction, the view CONSTRUCTS its own handle from the created object's definition — a view-side `txnDatabase` bound to the VIEW operator and a view-side relation built through the view's own object graph and interned in the VIEW's tableCache — so the handle's `db.op`/`getTxn` resolve the VIEW (read point F; view-owned entry-clone hydration and accounting); the view NEVER executes `proc.Store` on a parent-owned handle (that mutation belongs to the parent path; view handle construction stores the proc on the view-local copy); the stored payloads are read-only inputs to handle construction. Mandated consumer cases (item 5): a transaction-created DATABASE and a transaction-created TABLE under a marked execution — the read resolves F through the view-local handle with own-write rows returned; the parent's stored payload objects are NEVER returned to the view and are not proc-mutated (pointer-identity assertions); hydration charges on the view-local handle die at view retirement; parent commit and rollback after the execution behave identically to today |
| **R11-F2 the full-source re-evaluation has no resource contract — while the existing paths are explicitly bounded** | Today's no-LIMIT reader "STREAM[s] every matching doc in bounded batches (no top-K heap, no materialization of the whole result set)" with a producer goroutine, `streamCh`, and cancel/ctx cleanup (search/search.go:773-788), the engine's streamed boolean shape keeps "peak Go heap … O(one segment), NOT O(globalN)" and "Ranking is the upstream ORDER BY score node's job" (fulltext2/index.go:502-515), and `globalStats`' df counters are "memoized per query" (per-QUERY-term df, never a corpus-wide term map, index.go:355-376). Rev-10's re-evaluation (case (b) RANK-SENSITIVE and the newer/DDL-span fallback) and its selection under cache-governor refusal (the R9-F4 degradation) carried NO retained-memory bound, accounting/admission owner, streaming or spill contract — D-6 counted scans only, so the replacement could be read as an O(globalN)-materializing scan failing the budget exactly when the cache already refused it | The re-evaluation gains an explicit **resource contract** (rev-11, D-3(D) item 7): (1) pass-1 statistics run as a STREAMING aggregation — scalar N and avgDocLen accumulators plus per-QUERY-term df/phrase counters (bounded by the query's distinct terms, the globalStats dfCache discipline), never a corpus-wide term map; (2) pass-2 evaluation runs in BOUNDED BATCHES mirroring the streamed reader — the evaluator itself retains no whole result set; (3) NO-LIMIT shapes (including score-output and score-ORDER queries) delegate ranking to the EXISTING pipeline Sort/Limit operators, exactly the streamed reader's contract ("Ranking is the upstream ORDER BY score node's job") — their existing spill/accounting contracts govern any result-set materialization that the pipeline itself performs; (4) pushed-LIMIT shapes use a K-BOUNDED top-K heap (the SearchInto reusable-ColumnBuffers discipline, search.go:810-825), retained rows O(K); (5) score-range-only shapes stream-filter; (6) candidate suppression/dedup retentions are bounded by the workspace own-write window (already workspace-accounted) and — on the incremental rank-insensitive path only — the gap window; the re-evaluation scan itself needs no merge-dedup (the F-source view resolves one live row per pk); (7) ALL evaluator retentions (counters, heap, suppression sets) are registered EXECUTION-SCOPED on the statement's memory pipeline and released at the per-execution retirement — no new budget/governor admission is introduced because the retained orders match the existing paths (O(query terms)+O(K)+O(workspace window)); (8) exhaustion and cancellation follow the existing reader discipline (upstream close/cancel stops the pass through the statement context; close releases pins, clones and ledgered state) — and the R9-F4 governor-refusal degraded run executes the SAME contract, so the fallback cannot be the unbounded path it was readable as. Mandated validation (4b): deterministic constrained-budget cases (a governor-refused run completing with bounded memory and the completed-correct result) and no-LIMIT cases (a large harness corpus with score output asserting batch-wise streaming and bounded evaluator retentions), plus the K-bounded heap member |
| **R12-F1 the cloned view loses S3-flushed workspace DELETEs — `CloneSnapshotWS` starts the registry empty** | The S3 flush path (`writeFileLockedWithAutoIncrEpochKnown`) stashes each flushed tombstone file's ObjectStats into the OWNING transaction's registry (`StashFlushedTombstones` → `Store(stats, nil)`, txn.go:1931-1935, types.go:786-788); the reader's in-memory workspace-delete gate EXCLUDES file-backed entries (`(entry.typ == DELETE) && (entry.fileName == "")`, local_disttae_datasource.go:634), and both suppression surfaces resolve the registry through the RELATION's owning transaction — `applyWorkspaceFlushedS3Deletes` reads `ls.table.getTxn().cn_flushed_s3_tombstone_object_stats_list` (:1386/:1394) and is consumed by `ApplyTombstones` (:1069/:1108) and `GetTombstones` (:1142/:1164), while `txnTable.CollectTombstones` reaches `getUncommittedS3Tombstone` (txn_table.go:855/:863/:925, txn.go:2846-2858) — but `CloneSnapshotWS` gives the view a FRESH EMPTY registry (txn.go:3430) and rev-8..11's borrowed enumeration covered only the writes prefix, `deletedBlocks` and the chains; D-4 cannot substitute (its mapping domain is rows whose ORIGINAL object died inside the frontier window — the still-alive uncompacted flushed case is outside it), so the marked read RESURRECTS rows the transaction deleted and the re-evaluation corpus/statistics count them (demonstrated as a distinct visibility input by TestLocalDatasource_ApplyWorkspaceFlushedS3Deletes, local_disttae_datasource_test.go:179) | **Flushed-tombstone capture (rev-12, D-3(D) view block):** the view captures the parent's flushed tombstones AT CONSTRUCTION — a parent-mutex-guarded VALUE-COPY of every registry ObjectStats entry into the VIEW's own fresh registry, copied-and-released under the R10-F3 outermost-lock rule before any view-private state is exposed; the capture is NON-OWNING (no Delete on the parent's registry, no file operation, no lifetime claim: the tombstone FILES keep the parent's workspace-entry ownership — commit transfers them to TN, rollback/GC removes them — and the parent's registry keeps its own retirement paths, commit-GC entry removal types.go:1147 and `delTransaction` teardown txn.go:3295/:3321), and the view's copy dies at the per-execution retirement sweep; bounds: one capture per view construction (a flush landing later belongs to the NEXT execution's view, D-2/D-8), retained size bounded by the transaction's own flushed-file count; the carried snapshot feeds the SAME three read surfaces (ApplyTombstones, GetTombstones, CollectTombstones) so flushed-deleted rows stay suppressed in every view consumer INCLUDING the re-evaluation candidate scan and pass-1 corpus/statistics. Mandated validation (item 5 (h)): the flushed own-DELETE member without compaction asserting ApplyTombstones/GetTombstones/CollectTombstones suppression, re-evaluation corpus statistics, and unchanged parent commit/rollback behavior |
| **R13-F1 Compile pool generations keep execution-read state** | Verified on the delivered slice at base `d93d5d66c8`: `compileService.go` cycles Compile objects through the reuse pool (:24 `reuse.Alloc[Compile]`, :31 `reuse.Free[Compile]`), so successive statements — including successive statements of one still-open SI transaction — receive recycled Compile objects; `resetExecutionReadState` is called only from `Compile.Reset` (compile.go:314), while `Compile.clear` (:477) resets `executionGeneration` (:513) but contains zero execRead resets, and fresh construction has none; the delivered carrier cache therefore re-serves a previous execution's frontier and carrier whenever `execReadOpOwner` equals the current still-open transaction operator (the delivered owner guard passes), with the disable flag and the bound-scope ledger surviving equally. Demonstrated on the delivered fixed binary (evidence/probe5-prepr.log scenario F1a): a locking read on row X, then a peer commit on row Y, then the SAME transaction's locking read on Y returns the stale snapshot value (100 where 999 is committed) — exactly D-8/R5-F2's own consequence bullet (a stale frontier excludes the row from discovery and lock acquisition, and empty discovery never reaches the retry machinery); the review's empty-first shape (F1b) returned the correct value in the same run, showing the SQL-visible manifestation depends on which recycled object the allocator returns, so closure must be deterministic rather than probabilistic; the retained instrumented trace `/tmp/mo30955/logs-fixed-dbg/stdout.log` (26 [EXECR-DBG] lines) and `evidence/impl-debug/genrs-fixed3.log` record the same-session Compile cycling found at implementation time | **Pool-generation retirement (rev-13, D-8 closure):** per-execution execution-read state — frontier value, owner pin, carrier, bound-scope ledger, disable flag and reason — is retired at EVERY Compile object lifecycle boundary that precedes reuse: `Reset`, `clear` (the pool-release path) and fresh construction, idempotently; additionally a monotonic per-Compile execution-read EPOCH is stamped at every retirement, and the cached carrier is honored only when the owner operator matches AND the epoch matches the one captured at this statement's first admission, so any missed retirement path degrades to a fresh frontier re-acquisition (fail-loud on mismatch, never silent reuse); the delivered owner guard is retained but no longer load-bearing alone. Deterministic UT ladder (item 8 (i)) covers successive statements in ONE transaction (the same-operator case), successive transactions, pool recycling through the reuse package, and generations that previously disabled binding or bound scopes |
| **R13-F2 discovery, the remote single-scope reader and the persisted-tombstone surface resolve at the session snapshot** | Verified on the delivered slice: `Scope.getRelData` calls `c.handleDbRelContext(s.DataSource.node, s.IsRemote)` (scope.go:1181) which opens the discovery relation through the SESSION operator (compile.go:10998), and `expandRanges` enumerates table objects through that relation (`ForeachSnapshotObjects` filters at its `db.op.SnapshotTS()`, txn_table.go :1272/:1430/:1507) — objects persisted between the session snapshot and F are excluded from the locked read's own source assembly; on the remote single-scope path the runtime REPLACES the already-bound `s.DataSource.Rel` with a freshly opened snapshot-bound handle (scope.go:1215), dropping the carrier bind after assembly; the stamped `DataSource.Timestamp` (compile.go:5973) carries F but cannot re-enumerate missing ranges or repair a replaced reader; `txnTable.CollectTombstones` reads the `db.op.Txn().SnapshotTS` META FIELD (txn_table.go:972) bypassing the carrier while the delivered read surfaces resolve `db.op.SnapshotTS()`. Demonstrated on the delivered fixed binary (evidence/probe5-prepr.log scenario F2): peer bulk-inserted PERSISTED rows committed inside the RR window are invisible to the SAME transaction's locking reads at both point and range shapes (empty where the committed state holds 3601 rows; plain reads correctly stay at the 1-row snapshot) | **Discovery-consistent carrier (rev-13, D-3 closure):** the admitted execution read view binds EVERY relation surface through which this statement's locked occurrence resolves data, not only the scan's `DataSource.Rel`: (a) `handleDbRelContext` resolves marked scopes' discovery relation and database through the execution carrier, so `expandRanges` enumerates at F and aligns with the already carrier-aware innermost filters; (b) the remote single-scope replacement keeps a per-scope carrier clone — OR, until D-9's transport lands (stage-2), a marked scope that would execute remote makes the whole statement decline (G4) rather than silently mixing a replaced snapshot-bound reader; (c) `CollectTombstones`'s persisted path resolves through the carrier's `SnapshotTS()`; (d) a carrier-consistency AUDIT is mandated as a static check: every snapshot resolution on a carrier-bound relation reads `db.op.SnapshotTS()`, never the `Txn().SnapshotTS` meta field. Validation (item 8 (ii)): SQL regressions with peer-persisted bulk commits inside the RR window (the probe's standing red), a real disttae consumer UT reading live committed data and own writes/flushed deletes through a live carrier, and the remote-reader case on the 2-CN harness |
| **R13-F3 fabricated and copied scan nodes lose the locking role** | Verified on the delivered slice: every scan-node fabrication site copies `ScanSnapshot` while dropping `scan_read_role` — apply_indices.go :1550/:1588/:1630/:1692/:2383/:4158/:4230/:4705/:4779/:4998, the two idx-table-node sites R4-F2 named among them — and `DeepCopyNode` copies `ScanSnapshot` at deepcopy.go :378/:538 while dropping `ScanReadRole`; `IndexScanInfo` has no `ParentTableId` (plan.proto:1386 carries only `scan_read_role = 96`); an unmarked covered-plan index surface reads the index at the SESSION snapshot while the locked base occurrence current-reads at F, so a newly matching row is excluded before locking and NOTHING errors — `abandonExecutionRead` never fires and the statement silently mixes reads, violating G4. Demonstrated on the delivered fixed binary (evidence/probe5-prepr.log scenario F3): with the secondary index chosen (EXPLAIN shows `Index Table Scan on f3p.idx_f3p_v` under Lock), the locking read for the peer-committed value returns empty where the design returns the row | **Typed-provenance preservation (rev-13; R4-F2 completion as approved):** role propagation is CONTRACT at every site that fabricates or copies a scan node of a locked occurrence — the apply_indices sites propagate the SOURCE node's role and set the additive `IndexScanInfo.ParentTableId` linking the idx-scan occurrence to its owning base table; `DeepCopyNode` preserves the field verbatim (no provenance-losing copy); the fulltext2 covered-plan strip (apply_indices_fulltext.go:1293) remains the single ENUMERATED allowlist with recorded rationale; a typed-plan occurrence-corpus UT enumerates every fabrication/copy site against the locked-statement corpus asserting expected roles; and the statement-level closure check (D-10, G4 completion) treats any locked-occurrence scan left at `ScanNestedBlock` after rewriting — outside the approved nested-block sets — as a whole-statement decline, so a mark can never silently vanish. Forced-index red/green members (item 1.8) and a covered-plan mixed-surface case are mandated (the probe's index case is the standing red) |
| **R13-F4 marked-SI revalidation at lock acquisition was deferred without a sanctioned contract** | Verified on the delivered slice: lock_op.go still gates the whole refresh family on RC — `setPlanSnapshotForLock(..., txn.IsRCIsolation(), ...)` (:973), the `!txnOp.Txn().IsRCIsolation()` early exits (:1107, :1135, :1155), the extra-check gate (:1214), and the wait-resolved conflict branch returning `NewTxnWWConflict(ctx, tableID, "SI not support retry")` (:1251-1252, observed as 20619 on the delivered fixed binary, evidence/probe3-fixed2.log); approved D-5's marked-SI branches (revalidate against the executing generation's frontier at lock acquisition; the ~1152 normal refresh and the ~1214 extra-check refresh with a concrete floor; the ~1251 wait-conflict returning the refresh outcome for marked non-RC) are therefore UNDELIVERED for marked SI — a peer commit between frontier acquisition and lock acquisition, or while the statement waits on a held lock, leaves the marked statement serving the pre-commit value with no refresh path, and the delivered BVT/UT matrix covered none of these members | **D-5 delivered as approved (rev-13; no mechanism change):** marked-SI locking reads revalidate visibility at lock acquisition against the attempt's frontier and surface through the approved tokenized `ErrTxnNeedRetryLockingRead` retry path (O-D1 allocation; `Compile.isRetryErr` acceptance only for marked locking SELECTs under SI; token validity REQUIRED; spin-free floors `F_{k+1} = max(requiredTS, F_k)` admitted only when strictly greater); UNMARKED SI behavior stays byte-identical to base, including the 20619 wait corner. Stage-1 closure (item 8 (iv)) requires the deterministic commit-between-acquisition and commit-while-waiting SQL members on the issue's own shape plus the item-3 lockop matrix green; these are the selector's explicitly named wait case |
| **R13-F5 ordinary locking views inherit nothing** | Verified on the delivered slice: view expansion clears `builder.isForUpdate` for the view body (query_builder.go :12701-12704 — the base's nested-LOCK_OP suppression, correct and retained) and the delivered role helper derives the role from `isForUpdate` alone (:13136-13143), so every ordinary view body scan constructs `ScanNestedBlock` while `collectLockTargets` still walks and LOCKS view-expanded scans (approved R5-F1 evidence) — the view occurrence takes the lock yet reads the snapshot. Demonstrated on the delivered fixed binary (evidence/probe5-prepr.log scenario F5): after a peer commit, `SELECT v FROM v_f5 WHERE id=1 FOR UPDATE` returns the STALE snapshot value while the direct-table form returns the committed value, and plain reads through the view correctly keep the snapshot; joining such a view with a directly locked table mixes current and snapshot occurrence reads within one statement without tripping any decline | **R5-F1's approved carve-out delivered (rev-13 closing item):** the view-expansion path carries an explicit inheritance state SEPARATE from `isForUpdate`, so transparent view bodies construct `OuterLocking` for the outer locking statement while LOCK_OP suppression stays bound to `isForUpdate` exactly as approved; nested blocks inside the view stay `NestedBlock`; CTE bodies stay excluded exactly as today. Item 1.6's view/CTE members plus a direct-table x locking-view JOIN control (per-occurrence roles asserted: every occurrence `collectLockTargets` locks reads current, nested occurrences read snapshot) and the G4 declinature rules for unservable shapes are the mandated validators (probe scenario F5 is the standing red) |
| **R13-F6 the partial rollout ships without any sanctioned delivery contract** | Verified on the delivered slice and the approved document: the diff enables current-read semantics for admitted ordinary relation scans with ZERO capability gating — no `defines.MORPCVersionLockingCurrentRead`, no validate-family member, no prepared-statement/session-cache protocolVersion guards (approved D-8 layers 2/3 undelivered) — while `BindExecutionReadOp` soft-refuses combined-partition, sharded, index-parent and transaction-created kinds; the approved rev-12 document contains NO staged-delivery or phasing text (read in full this round), so nothing defined which shapes are enabled, what happens to every other shape, or why that split is safe, and the delivered `relation-read slice` claimed the approval of a complete mechanism set it only partially implemented; the pre-PR review therefore blocked it: deferring approved requirements does not establish compatibility or own-delete correctness for the enabled mode | **D-10 staged delivery contract (rev-13; the genuinely NEW material):** see D-10. Stage-1 is the COMPLETE approved contract for the primary relation-read surface — D-1 provenance completion (R13-F3, R13-F5), D-2, D-3 carrier-consistent surfaces (R13-F2), D-4's ordinary-local transfer mask (R14-F1: moved into stage-1), D-5 revalidation (R13-F4, with rev-15's marked-retry persistence invariant, R15-F2), D-7's single-version deployment contract (R15-F1: this cell's rev-13 gate members — the allocated capability constant, bind-time gate, per-Run validator and the two protocolVersion cache guards — are deleted with the whole gate design), D-8's per-execution admission/reset plus pool-generation retirement with the epoch backstop (R13-F1), and statement-wide G4 closure; every locked shape outside the admission set DETERMINISTICALLY conserves base semantics via a whole-statement decline (locks exactly as today, reads exactly as today, no silent mixing, no new failure classes); stage-2 owns the already-approved deferred subsystems — the isolated execution view and re-evaluation family with R12-F1's flushed-tombstone capture, D-9 sharding transport, the marked-async completion and generation-keyed cache families (R-14/R-15/R-16), and the multi-CN wire regression items — each still under the same conservation discipline until its own stage lands |
| **R13-F7 executable proof leaves demonstrated paths uncovered** | Verified on the delivered slice: the dual-role BVT member animates only the OUTER occurrence's row while its scalar subquery watches an UNCHANGING row, so a hypothetically all-current or all-snapshot execution still satisfies the member (no oracle power over which occurrence current-reads); the disttae UT asserts handle/pin/clone mechanics on synthetic workspaces and never reads real committed data, persisted objects, own writes or tombstones through a live carrier — the persisted-tombstone meta-field path (txn_table.go:972) was never exercised; the archived first post-boot replay produced one wrong-result dual-role miss (73/74) that six later clean replays never explained, and the exact failing assertion text was NOT retained (re-checked this round: `bvt-replays-flake-bound.log` and `motester/flake3.log` hold only the three 100% summaries; `motester/report/report.txt` was overwritten by the belt run), so a post-hoc root cause from existing artifacts is impossible; the delivered matrix also lacks pool-reuse, discovery/persisted-object, revalidation-wait and view/index members — every one of which this round demonstrated as a real wrong-result path (probe scenarios F1a/F2/F3/F5) | **Closure evidence ladder (rev-13, item 8):** stage-1 closure is defined as item 8 (i)-(ix) — deterministic pool-reuse members; discovery/remote-reader regressions with peer-persisted bulk commits plus the carrier-consistency audit; the typed-provenance corpus + forced-index + covered-plan members; marked-SI revalidation and commit-while-waiting members; view/CTE/JOIN mixing members; real disttae consumer members reading live committed data with own INSERT/UPDATE/DELETE visibility and own/flushed-delete suppression equivalence through the carrier (rev 15, R15-F1: the ladder's former capability-gate conservation members — below-gate uniform legacy, above-gate enable and EXECUTE-time transitions — are deleted with the gate and are not part of the ladder, which is why no version-dependent matrix member appears here); an ANIMATED dual-role oracle whose watched row receives DISTINCT committed values per re-read so all-current and all-snapshot executions both FAIL (replacing the stable-value oracle); and a boot-fresh instrumented replay suite recording the frontier-acquisition vs peer-commit ordering for the dual-role member so any recurrence of the archived miss FAILS LOUD with evidence. This round's probe matrix is the standing red side; repetition of previously-flaky members without root cause does not close any member |
| **R14-F1 stage-1 as scoped would admit delete-carrying marked reads without D-4's transfer mask — own-deleted rows can resurrect across a compaction inside [SnapshotTS, F]** | Verified at the delivered base and revision: D-10.2 admitted ordinary local relations with no exclusion for transactions carrying deletes while D-10.4 deferred D-4 to stage-2; the ordinary local reader keys its workspace-delete suppression by block and rowid — `workspaceDeleteEntriesForBlockLocked(bid)` with offset matching (local_disttae_datasource.go:1204-1241), `GetTombstonesByBlockId(ls.snapshotTS, bid, ...)` (:1425-1429) and `FastApplyDeletesByRowOffsets(..., rawRowIdDeletes.offsets[*bid])` (:1453-1457) — so an own DELETE against object O followed by a compaction O→P inside the RR window leaves the admitted reader enumerating P at F (rows rebuilt under P's identity) while the transaction's workspace tombstones identify O: the deleted row SURFACES through P on the marked read; the delivered carrier delegates the SAME workspace (no old-to-new mapping), and R12-F1's flushed-tombstone capture supplies suppression surface identity, not mapping; approved D-4 exists exactly for this domain — per attempt over [txn.SnapshotTS, F], every workspace DELETE of marked tables (in-memory entries and S3-dumped objects' rowid+PK) whose rowid's object died inside the window (`GetChangedObjsBetweenForTombstoneTransfer`, comment 'includes deleted intermediate', logtailreplay/blocks_iter.go:225-230) is mapped by PK to the alive-generation rowid (`doTransferRowids`, transfer.go:495, consumed at transfer.go:60), covering late-added deletes and O→P→P2 chains, consumed as the mask-only `maskTombstones` input at `ApplyTombstones`/`GetTombstones`, with zero mask work for delete-free executions (D-6) and no commit-path effect | **D-4's ordinary-local form moved into stage-1 (rev-14, D-10 re-scoped; supersedes R13-F6's stage-2 listing of D-4 — the isolated-execution-view/re-evaluation family stays stage-2):** the mask is a MANDATORY stage-1 input for every admitted marked execution over the ordinary local surface; delete-free executions compute zero mask work per D-6; a delete-carrying marked read never ships without the mask, so the resurrection shape cannot occur inside stage-1's admitted set (remaining stress lives inside D-4 itself, whose approved design stands unchanged). Mandatory stage-1 evidence (item 8 (vi)): D-4's own regression set moved into the ladder as deterministic CreateEngines-harness members — in-memory workspace deletes across compaction, S3-dumped (flushed) workspace deletes across compaction, late-added deletes, double/repeated compaction (O→P→P2), the dual-view control (frontier reads suppress the deleted rows; snapshot reads keep them hidden via original rowids) and commit-behavior invariance, all through the ordinary local path; no SQL-level compaction forcing is claimed — the hazard is code-verified and the validator is the deterministic harness member |
| **R14-F2 the promised vanished-mark decline is not computable from the specified representation** | Verified: D-1's artifact wording gives the role enum's zero value (default `NestedBlock`) BOTH meanings — 'an unset field means "no current read"' — while D-10.3 required post-optimization detection of 'a locked-occurrence scan left ... at `ScanNestedBlock` outside the approved nested-block sets'; distinguishing those sets after optimization needs structural or table-identity inference, which same-table scalar subqueries lowered to LEFT joins defeat (R5-F1's own evidence; `collectLockTargets` recurses into BOTH LEFT children, query_builder.go:15070-15109), and O-D4 permits a presence boolean OR a role walk while O-D9 deferred the mechanism — so with one lost outer mark beside a valid nested occurrence the summary stays true, the statement mixes reads, and nothing in the specified inputs can institute the promised whole-statement decline | **Explicit role representation (rev-14, D-1 amendment; O-D9 RESOLVED):** construction (the `buildTable` role helper) emits ONLY the explicit set — `OuterLocking`, `OuterLockingWithExplicitSnapshot`, the NEW `NestedBlockExplicit` (nested by query-block provenance: subqueries, CTE bodies, grouping/UNION arms, nested blocks inside view bodies), or the covered-plan strip value set by the fulltext2 covered-plan mutation (its single allowlist now representationally distinguishable); the zero value means 'unset', is never produced for a scan of a completed plan of this feature, and carries no representation meaning beyond the plain-proto wire default — absent fields decode as today's unmarked plan exactly (rev 15, R15-F1: the former above-gate/below-gate and legacy-wire qualifiers are deleted with the gate, D-7; the enum extension stays additive for wire-decode safety only). The declinature check is thereby a pure role walk over the bound plan: in a locking-presence plan, ANY unset-role scan is a vanished mark and declines the WHOLE statement BEFORE source publication; `NestedBlockExplicit` and covered-strip occurrences are exempt BY REPRESENTATION, so no join-shape or table-identity inference is needed and the same-table scalar-subquery plan stays decidable. Mandated (item 8 (iii)): the ablation member — a same-table scalar-subquery plan with exactly one outer role dropped beside a valid `NestedBlockExplicit` occurrence asserts whole-statement decline before any marked source binds/publishes, with both-marked and both-explicit-nested controls executing per contract; the typed corpus additionally asserts construction never emits the zero role |
| **R14-F3 D-10's below-gate promise contradicted D-8's Run-time rejection** | Verified: D-10.1 and D-10.3 promised 'no new error classes' and 'below the capability gate, every marked plan executes today's semantics uniformly', while approved D-8 layer-3 mandates the validate-family member `validateLockingCurrentReadCapability` REJECTING a marked plan at Run admission below the gate with the NotSupported 'locking current-read execution requires rebinding (MORPC protocol version <MORPCVersionLockingCurrentRead>)' error at both call sites (cold compile compile2.go:169-181; every Run including reused prepared pipelines compile2.go:451-459); the window is REACHABLE because the frontend prepare-execute check (computation_wrapper.go:1482-1487: `protocolMismatch := prepareStmt.protocolVersion != 0 && prepareStmt.protocolVersion != protocolVersion` feeding `preparePlanNeedsRebuild`) runs before Run admission, so the negotiated capability can drop between the two; the two contracts could not both hold | **Superseded (rev 15, R15-F1):** the contradiction this finding proved — D-10's below-gate uniform-conservation promise vs the per-Run validator's NotSupported rejection — had been resolved in rev-14 by a rebuild-or-reject transition contract (prepared-statement protocol-version rebuild on capability drop, session-cache eviction, one recorded new-error exception). Revision 15 deletes the gate and with it EVERY transition class: a marked plan stays marked for its cache lifetime and re-executes under D-8's per-execution admission (D-8 note 2), no rebuild-or-reject path and no new-error exception exist, and item 8 (vii)'s capability-drop transition members are replaced by the marked-retry persistence members of R15-F2 |
| **R15-F1 the rev-3–14 cluster capability gate violates the repository's shutdown-only upgrade contract** | Verified at the repository contract, the merge base and the delivered head. Contract: `.agents/skills/mo-dev/references/upgrade-compatibility.md` (identical at merge base `9e29011e73`) — "MatrixOne supports shutdown upgrade only", "Do not add a per-feature `MORPCVersion` constant, bump `MORPCLatestVersion`, or gate a feature on … any other 'all peers support X' capability check", "Do not add legacy encode/decode paths, neutral fallbacks, or downgraded plans for an older peer", "Adding features, fields, operators, or functions is never by itself a reason to bump", and a new or changed proto field "needs no version gate". Delivered head: `pkg/defines/const.go:197-198` adds `MORPCVersion135` and raises `MORPCLatestVersion` over the base high-water 134; `pkg/sql/plan/locking_current_read.go:25-46` adds `ClusterHasLockingCurrentRead`; `pkg/sql/plan/query_builder.go:13374-13388` conditions mark emission on it at bind time; `pkg/sql/compile/locking_current_read_capability.go:38-59` adds the `validateLockingCurrentReadCapability` Run-admission member with a NotSupported "requires rebinding" terminal error wired at `compile2.go:188/:474`; `pkg/frontend/mysql_cmd_executor.go:4545-4548/:7020-7021` adds gate-down session-cache eviction and publish guards; 9 test files carry gate/transition members. PR review blocked the design on this contradiction (aunjgr 2026-10-29, mo-bot rounds). | **D-7 rewritten as the single-version deployment contract (rev 15):** every gate/legacy/transition mechanism is DELETED from design and implementation — the allocated constant and `MORPCLatestVersion` bump, `ClusterHasLockingCurrentRead`, the bind-time gate, the validator member and both call sites, the session-cache publish/evict guards, and the gate/transition test members (full deletion list in D-7's rev-15 notes; the `PlanHasLockingCurrentRead` presence summary and its feature consumers stay). The feature is unconditional for every shape D-10/G4 admits; additive proto fields ride the wire with no negotiation; absents decode by plain-proto default to no-role/no-frontier. D-8's per-execution reset/freshness and effective-role resolution stay verbatim; the prepared-statement `protocolVersion` machinery stays owned by its existing consumers and is no longer consumed for this feature. Persisted state, backup/restore and the MySQL client protocol are untouched by this feature; the release boundary of the semantic change is the binary version itself, exactly like every other semantic fix. Supersedes: the gate halves of R5-F2/R7-F3/R13-F6/R14-F3, D-7 rev-3–14, D-8 layers 2-3, D-9 item 4's gate clause, D-10.1's new-error exception, R-6, and the gate members of R-8/R-11/item 4/item 5/item 6/item 8 (vii). |
| **R15-F2 a marked retry recorded on an earlier target can be lost to a later target's direct SKIP LOCKED conflict** | Verified at the delivered head (PR-review P1, confirmed by trace): the marked-SI branch of `doLock` records its observation only through `opts.markingFloor` (`lock_op.go:524`) and returns an EMPTY refreshTS (:1377-1395 `recordMarkingFloor(newSnapshotTS)` then `return true, false, timestamp.Timestamp{}, nil`); `performLock` persists an RC-style retry inside the target loop only when refreshTS is non-empty (:575-580) and defers the marked conversion to after the loop (:598-606, `ctr.lockingReadFloor` + `ErrTxnNeedRetryLockingRead` only `if ctr.retryError == nil`); a later target returning `lockservice.ErrLockConflict` exits the loop first at :548-549; `callSkipLocked` then sees `ctr.retryError == nil` and converts the returned direct conflict into an ordinary skip (:242-252). Counterexample: RR `SELECT a.id,b.id FROM a JOIN b ON b.id=a.k WHERE a.id=1 FOR SHARE SKIP LOCKED`, plan locking `a` before `b`; at frontier F, a(1).k=1; after F a writer commits a(1).k=2; another transaction holds b(1) exclusively while b(2) is free. The a-grant records the newer-version floor with an empty refreshTS; the b(1) conflict then silently skips the candidate, and the statement can return an empty result instead of retrying at the floor to the current pair (1,2) — no receipt the current-read contract was ever honored for the pair. | **D-5 marked-retry persistence invariant (rev 15):** once any target of a `performLock` call has recorded a marked floor, the call must surface the locking-read retry on EVERY terminal path — a later skippable conflict may shrink the accepted row set but never erases an already-recorded retry obligation. The conversion runs at the point of recording, before the next target is inspected; `ctr.lockingReadFloor` max-aggregates across targets and the tokenized retry is (re)generated from the CURRENT max; definition-retry precedence is retained (`retryWithDefChangedError` dominates whenever set, before or after a marked floor); fatal `doLock` errors still return immediately; `callSkipLocked`'s existing conflict branch then surfaces the retry with `result.Batch = nil` (no accepted prefix published, so a downstream LIMIT cannot consume partial output). Mandated item 8 (vii) members: the RR marked-SI counterpart of `TestLockOpSkipLockedPreservesRefreshBeforeLaterTargetConflict` (floor + empty refreshTS on the first target, direct conflict on the second), the two-floor max-aggregation member, the both-order definition-precedence members, and the LIMIT/no-output assertion including the all-candidates-skipped shape. |

Revision 15 review and delivery record. Revision 15 answers the two
findings of PR #31587's first review round, was approved as a design
before its implementation was delivered, and is the approved revision:
R15-F1 removes the rev-3–14 cluster capability gate (raised by aunjgr,
2026-10-29: a per-feature MORPCVersion gate conflicts with the
repository's shutdown-only upgrade contract) and R15-F2 adds the
marked-retry persistence invariant (raised as P1 by iamlinjunhong,
2026-10-29).

Design-first approval of exactly this content — reviewer identity and
reviewed revision. The independent pre-PR design review (a separate,
read-only, controller-dispatched design-review session; the same
review process that recorded the rejections of revisions 1-11 and the
approvals of revisions 12 and 14, and distinct from both the
implementation work and the PR's implementation reviewers) reviewed
revision 15 on 2026-10-29, BEFORE the implementation revision existed:
its reviewed tree was commit
`6243db858e82a01c8c89ee8bc0d33d7f02274772` sitting directly above the
previously reviewed implementation head `5415d839824` with NO
implementation change (worktree clean, merge-base `9e29011e73`).
The verdict is PASS, `approved=true`, recorded at
`state/artifacts/issue-30955/pre-pr-review-1793297991115.json`:
"PASS — design-only review for #30955 on main. Verified
docs/design/20261024-locking-read-current-read-under-repeatable-read.md
Revision 15 at commit `6243db858e82a01c8c89ee8bc0d33d7f02274772`,
SHA-256 `e2f7e37135778c60fe7f3604bd5d44974ff2e76fe1c186864247db5f8b3ae4c9`.
… R15-F1's gate-removal closure matches the repository's shutdown-only
upgrade contract. R15-F2's immediate retry persistence, maximum-floor
aggregation, definition-error precedence and fatal-error preservation
address the verified later-target conflict path. … Approval covers
this exact design, not implementation readiness or complete issue
delivery. The planned repairs require red/green proof on
implementation parent 5415d83982 and a separate full implementation
review." The exact reviewed revision-15 content is preserved verbatim
in this file's git history at commit `6243db858e` (equivalently
`cec189c19e` after the PR's rebase onto main); the decision table and
sections below carry the same decisions in their approved
post-revision-15 form — deleted mechanism text (the allocated gate
constant, bind-time gate, per-Run validator, below-gate legacy plans,
capability transitions and their test members) is removed rather than
silently retained.

Implementation followed that design approval and then received its
own separate reviews: the complete approved set (stage-1 plus both
revision-15 decisions) is PR #31587, heads `8a705621ec` → `8fc3801e8e`
(this document's record reconciliation is head `2b6dd91192`); the
implementation approvals recorded on that PR are iamlinjunhong's
APPROVED review of `8a705621ec` (2026-10-29: no remaining findings —
"Design revision 15's single-version deployment contract and
retry-persistence changes are acceptable for its documented stage-1
scope") and the mo-bot reviews of `8fc3801e8e` and of
`2b6dd91192` (2026-10-30), which reconfirm the same decisions after
the rebase and the record reconciliation.

## 1. Problem and evidence

### Observed defect (reproduced on base `d93d5d66c8`)

Environment: locally built `mo-service` from the assigned base, single-CN launch
(`LOG`+`TN`+`CN`, isolated cluster on `127.0.0.1:39001`, runtime `/tmp/mo30955`,
outside the tracked diff). Session A uses
`SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ`; session B autocommits.
Two deterministic probes (explicit ordering, no sleep windows), archived at
`state/artifacts/issue-30955/evidence/probe-base.log` and `probe2-base.log`.

Probe 1 (values bug, the issue's exact reproduction): A reads 100 after BEGIN;
B autocommits `UPDATE f SET v=999` (verified on B); A's
`SELECT v FROM f WHERE id=1 FOR UPDATE` returns **100** (InnoDB: 999); A's plain
read still returns 100 (correct RR); after COMMIT the read is 999. Controls:
FOR SHARE returned 100 where InnoDB returns the committed 333; LOCK IN SHARE MODE
returned 333 where InnoDB returns 444; the wait-resolved conflict errors
`20619 w-w conflict: SI not support retry` where InnoDB returns 555; the same
interleaving under READ COMMITTED returns 777 (RC machinery works); a plain
UPDATE in the same committed window succeeds as a statement and its COMMIT
aborts with 20619 (SI write semantics enforced at commit — out of scope).

Probe 2 (candidate-discovery gap): `SELECT COUNT(*) FROM f WHERE v=999 FOR
UPDATE` returns **0** (InnoDB: 1) after B commits v=999; the analogous count for
a peer-committed INSERT valued 1000 returns **0** (InnoDB would see it); plain
snapshot reads stay correct (1 row, v=100) and the post-commit read sees 2 rows.

Probe 3/4 (rev-6 round: the search-stream and sharding boundaries, archived
at `state/artifacts/issue-30955/evidence/`{probe3-base.log, probe3b-base.log,
probe3c-base.log, probe4-base.log} with sources `probe3-viewmatch.go`,
`probe3-shapes.go`):

- Every DIRECT `MATCH(doc) AGAINST(…) FOR UPDATE` shape tested (plain filter,
  id-only covered candidate, `INBOOLEAN MODE` composition, ORDER BY, residual
  AND-filter) errors **20105** at base: the fulltext rewrite's project-anchored
  descent stops at the `Lock` node (EXPLAIN shows `Project → Lock → Table Scan`
  with the raw `fulltext_match` filter), and the raw function is un-executable.
- A locking statement OVER a MATCH-bearing view rewrites BELOW the Lock and
  executes: EXPLAIN `Project → Lock → Sort → Join(Table Scan on f, Fulltext
  Index Scan)`. In the deterministic two-session scenario (A RR/BEGIN, B
  autocommits a new matching row, B's committed reader confirms the new match
  viewable), A's `SELECT id, v FROM v_ft3 FOR UPDATE` returns **3|300** while
  the committed new match is 4|400 (InnoDB: 4|400 must be current-read and
  locked); A's plain RR read through the same view correctly returns 3|300
  (snapshot), and A's post-commit read returns 3|300+4|400. The snapshot-bound
  search stream therefore excludes peer-committed matches from the LOCKING
  read before `LockOp` — the review's asserted exclusion, live at base.

### Affected users/workflows

Any `SELECT ... FOR UPDATE / FOR SHARE / LOCK IN SHARE MODE` used to re-check
current state before acting (OCC re-validation, queue/lease acquisition,
version probing): stale values for discovered rows, and missing rows whose only
the current state matches — silently, on single-CN and multi-CN alike.

### Invariant to restore (the contract)

Under every isolation level, a locking read of the outer query block's tables
reads the **latest committed state at a frontier that provably covers every
commit completed before the admission of that execution** (execution = one
run of the statement, including each EXECUTE of a cached or prepared plan; and,
after retry admission, every commit observed by lock detection), through the
access path actually chosen
— full scans, forced secondary-index access, index-join lookups, ordinary local
readers and remote execution — including candidate discovery, and with the
reader's own uncommitted deletes honored across compaction. Every **unmarked
read** — plain statements and ordinary nested query blocks, including subquery
occurrences of the same table inside the locking statement (WHERE, HAVING and
JOIN-condition scalar subqueries, including their LEFT-lowered forms, as well
as EXISTS/IN subqueries and CTE bodies) — keeps the
transaction snapshot; and (R8-F3, engagement resolved per execution by
R9-F3) every locked occurrence whose explicit historical snapshot ENGAGES
the historical cohort for that execution (H non-zero and strictly earlier
than the EXECUTING SnapshotTS, the reader's own admission) keeps ITS NAMED
snapshot instead of the
current state (the lock is still taken on the rows it read). Concretely for
the issue's sequence: `r1_snap = 100`,
`r2_forupd = 999`, `r3_snap = 100`; and for
`SELECT o.id FROM f o WHERE o.v > (SELECT i.v FROM f i WHERE i.id=1 LIMIT 1)
FOR UPDATE` after f.v moved 100→999: the outer occurrence reads 999, the
subquery occurrence reads the snapshot 100, and the predicate compares 999
against 100.

Negation: any locking-row value or row set differing from that committed state
— through any access path, fragment placement, or read form — or any change to
unmarked reads' snapshot results inside the same transaction, violates the
contract.

### Standard / precedent

MySQL 8.0 InnoDB semantics (MySQL Reference Manual, "Locking Reads" and
"Consistent Nonlocking Reads"): the locking clause sets current-read mode for
its tables; consistent reads elsewhere keep the RR read view. Mandatory
interoperability for the OCC pattern the issue cites.

## 2. Root cause (owner paths, verified at base)

Locking-read chain: `pkg/sql/plan/query_builder.go` (bind-time
`collectLockTargets` 15027 invoked at 9236; LOCK_OP appended above WHERE at
9236-9250/6598-6608; optimizer rewrites — including index application at
4458-4461, right-join orientation and scalar-subquery lowering — run after
bind) → `pkg/sql/compile/compile.go` (`compileLock` 9666, `constructLockOp`
operator.go:967, scan data production at `getCompileTableScanDataSource`
5750-5823) → block-reader assembly (scope.go:1955 `BuildBlockReaders`,
engine.go:1326) → `pkg/sql/colexec/lockop/lock_op.go` (`doLock`).

All row data flows through readers resolving the shared SI snapshot, on both
read paths: relation-side (txn_table.go:1430 fast filter, 1272/1507 object
walks, 2833 partition admission, 2778 reader construction,
local_disttae_datasource.go:83 source visibility, committed-tombstone
collection at txn_table.go:946/972) and shipped-scope
(`DataSource.Timestamp` from `txnOp.Txn().SnapshotTS`,
compile.go:5757/:5810, consumed at scope.go:1955 → engine.go:1326). Local
readers apply tombstones they compute at `ls.snapshotTS`
(local_disttae_datasource.go stages at 1099-1137 and 1159+), ignoring
`relData` tombstones outside the sharding-remote category (:1086-1094,
:1147-1158).

In `doLock`, every new-version refresh branch is RC-gated (normal ~1152,
extra-check ~1214; the wait-resolved conflict ~1251 errors ErrTxnWWConflict
for non-RC); empty batches never lock (lock_op.go:312), so predicate-current
discovery triggers nothing (probe 2); `Compile.isRetryErr` (compile.go:717)
accepts retries only under RC. The plan has no marker separating user
locking-read SELECT targets from DML/FK targets; neither a shared-snapshot
advance nor a final-plan table-identity walk can fix it (nested readers share
the snapshot; RC's transfer rewrites workspace rowids in place —
transfer.go:473-481 — unsound once a view returns to an older TS; occurrence
identity does not survive structural rewalks after child swaps
(opt_misc.go:544-548) or distinguish same-table occurrences
(flatten_subquery.go:780-799/861-867)); cursor-based transfers miss
late-added deletes and later compactions (transfer.go:265-269,
blocks_iter.go:243-252); and `WaitLogTailAppliedAt(empty)` provides no fresh
commit frontier (client.go:937-960 + timestamp_waiter.go:83-85).

Two further facts verified at base (rev-5):

- The `collectLockTargets` walk (query_builder.go:15027, invoked at :9236)
  runs on a tree whose WHERE subqueries have **already been lowered into
  joins**: `bindWhere` (:9222) calls `flattenFilterSubqueriesGuarded` (:9369)
  first; scalar subqueries carrying LIMIT 1 set `hasSingleRow` (:10986) and
  lower to plain `Node_LEFT` joins (flatten_subquery.go:797-798), which the
  walk visits on both children (only SEMI/ANTI/SINGLE/MARK right children are
  excluded, :15038). Any marking decided by this walk — revision 4's included
  — therefore cannot separate a lowered scalar subquery's occurrence from the
  outer block's occurrence of the same table. The only structure that still
  expresses the original query-block provenance at that point is the
  construction-time nesting discipline the binder already maintains
  (base_binder.go:981-1015; query_builder.go:5628-5635, :12687-12705,
  :4695-4704).
- The reused-execution path has no per-execution read-freshness state to build
  on: `Compile.Reset` (compile.go:287-368) re-provisions the retained
  generation without rebinding, the engine reader is rebuilt per execution
  (scope.go:304-307, :397-405) but `DataSource.Timestamp` is never re-stamped,
  so a plan-level frontier or mask snapshot would silently outlive its
  execution. Capability transitions during a cached plan's lifetime are
  already handled by per-EXECUTE protocol-mismatch rebuild
  (computation_wrapper.go:1481-1490), per-Run cached-plan capability
  validators (compile2.go:451-459) and session-cache eager eviction
  (mysql_cmd_executor.go:4574-4583) — none of which revision 4 connected into
  a per-execution admission contract for the marks. (Rev-15, R15-F1: these
  seams belong to other features' existing gates; this feature no longer
  consumes them for capability purposes.)

Two further facts verified at base (rev-6 — the fifth review's two
read-source boundaries):

- **Generated search scans are outside every marked surface.** A MATCH
  predicate is served by fabricated search streams (Boolean composition LEFT
  joins at apply_indices_fulltext_boolean.go:131; fulltext2
  `Node_INDEX_SEARCH_SCAN` at apply_indices_fulltext2.go:211; a covered
  variant that removes the base scan entirely at apply_indices_fulltext.go
  :1303, with no LOCK_OP awareness in the project-anchored descent or the
  SORT-anchored vector entry). At execution
  the search scan router bypasses relation readers (scope.go:1904-1905):
  `buildVectorIndexReaders` builds the search identity from the transaction
  SnapshotTS (scope.go:2127-2134), `compileIndexSearchScanDataSource` stamps
  `DataSource.Timestamp` from it (compile.go:5873), plugin execution borrows
  the same txn with no named snapshot (vectorindex/sqlexec/sqlexec.go:206-219)
  and the shared VectorIndexCache keys entries by the named-snapshot TS only
  (fulltext2/plugin/search/search.go:714-718, tail read :763-767). Empirical
  scope (probes 3b/3c/4): direct `MATCH … FOR UPDATE` shapes cannot even form
  a pre-error search plan (the descent stops at Lock; 20105), but a locking
  statement over a MATCH-bearing view forms `Lock` above
  `Join(Table Scan, Fulltext Index Scan)` and deterministically drops
  peer-committed matches from the LOCKING read (probe4), so the search stream
  of an EXECUTING locking plan is snapshot-bound at base.
- **Table-sharding uses a separate RPC boundary with no frontier transport.**
  Remote-shard discovery and reads are forwarded by `txnTableDelegate` with
  `ReadAt` fixed to the transaction SnapshotTS
  (txn_table_delegate.go:1377-1386); the receiver reconstructs a transaction
  from the serialized ProcessInfo (txn_table_sharding_handle.go:642-697) and
  reads at that reconstructed txn's SnapshotTS
  (`readutil.NewReader(..., tbl.db.op.SnapshotTS(), ...)`, :319-325). Neither
  the Scope DataSource frontier nor the local mask input reaches this
  boundary, so a sharded marked occurrence can keep the old RR view even
  though the coordinator admitted the statement at F.

Three further facts verified at base (rev-7 — the sixth review's mechanism
findings):

- The named-snapshot pin cohort cannot carry F: `EffectiveSnapshotTS` admits
  only TS strictly earlier than the transaction SnapshotTS (sqlexec.go:147
  -152, pinned by snapshot_test.go:60-71) and `vectorscan.Identity` only
  recognizes `TS.Less(currentSnapshot)` (execution.go:265-267) — a barrier
  frontier is by construction not earlier and would be silently discarded.
- A snapshot clone loses the transaction's own writes: `CloneSnapshotOp`
  binds an EMPTY read-only workspace (`CloneSnapshotWS`, operator.go:792
  -806 + txn.go:3414-3438), while classic fulltext maintenance writes index
  rows inside the same user transaction (build_dml_util.go :7274+:
  `INSERT INTO index_table … fulltext_index_tokenize`) — so a clone-based
  frontier search read drops the transaction's own matching writes before
  the join.
- Upstream already consumes the version rev-6 gated on: origin/main defines
  `MORPCVersion130` (incomplete temporal string semantics, commit 5a813a324d)
  with `MORPCLatestVersion = 130`, while the design base stays at 129 — a
  fixed "130" gate admits peers that implement temporal-string behavior and
  none of this design.

Three further facts verified at base (rev-8 — the seventh review's mechanism
findings):

- Sharing the parent WORKSPACE cannot carry the execution view: the workspace
  interns relations and hands them back unchanged under SI
  (txn_database.go:47-48/:133-136, txn.go:2968-2978), those relations pin their
  read point from their own operator (local_disttae_datasource.go:83), and
  the only way to make shared relations read at F — `BindTxnOp` — writes the
  shared `Transaction.op` (txn.go:3441-3442), so rev-7's
  "workspace-sharing" view either reads at the parent snapshot or mutates
  the parent. The view must be an isolated operator/workspace pair.
- ASYNC indexes are structurally incomplete at any read point: fulltext2 is
  always-async with the ISCP consumer trailing the base table
  (coverage.go:16-25), loaded generations contain only appended CDC tails
  (search_cache.go:174-192), and async DML maintenance writes no in-txn
  postings (build_dml_util.go:7237-7244 — `indexplugin.IsAsync` covers
  fulltext2/hnsw/bm25/cagra/ivfpq plus `async=true` opt-ins) — so pinning
  index reads to F completes marked searches only for synchronously
  maintained indexes; the marked async search needs a per-execution
  completion (the self-completing probeTail decision tree exists but runs
  only for JSON probes, search.go:763).
- An explicit historical snapshot coexists with a locking clause in one plan:
  `buildTable` carries `ScanSnapshot` on the scan (query_builder.go:13666),
  `collectLockTargets` still forms that scan's lock target
  (:15046-15069) and compile routes its reads through the historical clone
  (compile.go:5609-5622) — so a locking statement over an `AS OF` source is
  today a locked historical reader, and the design's marking rule needs an
  explicit eligibility partition instead of a blanket `isForUpdate` mark.

Four further facts verified at base (rev-9 — the eighth review's mechanism
findings):

- The borrowed writes prefix is NOT inert readers' input: the exact-PK probe
  hydrates the very entry it reads and charges the RECEIVING transaction
  (`ensureInsertKeyOffsetsLocked(&writes[wsCursor])`,
  local_disttae_datasource.go:757; charge txn.go:242-275; refund
  txn.go:310-323), and `readOnly` guards only Commit (txn.go:80/:3003) — a
  view read of an own INSERT with an unhydrated entry would mutate the
  parent's Entry while charging the view, leaving the parent to refund bytes
  it never charged.
- fulltext2's ranking is corpus-GLOBAL and applied INSIDE the search
  (globalStats N/df/avgDocLen, fulltext2/index.go:355-376; pushed ScoreRange
  and LIMIT top-K discard rows during the search, search.go:733-747/:810-825;
  the JSON-probe tail appends score-zero rows, :421-470) — a flag added to
  the probeTail mechanism cannot make incremental completion sufficient for
  scored/ranged/limited MATCH shapes.
- The engine's snapshot-cohort engagement compares against the EXECUTING
  transaction at datasource build (compile.go:10854-10866, twin :5609-5622)
  while no prepared-rebuild rule reacts to SnapshotTS
  (computation_wrapper.go:2834-2853/:1481-1502) — eligibility frozen at bind
  time would go stale under SnapshotTS-crossing reuse in either direction.
- The shared index cache is exact-key loaded with per-entry whole mappings
  (cache.go:1246-1290; search_cache.go:63-70/:220-236) and the governor
  refuses a concurrent second load under budget (governor.go:438-493) —
  keying marked entries by the per-execution frontier would multiply resident
  copies and turn budget pressure into locking-read failures that today
  cannot occur.

Three further facts verified at base (rev-10 — the ninth review's mechanism
findings):

- `SourceCommitTS` FAILS CLOSED on transaction-local writes: "Transaction
  workspace writes intentionally fail closed: they are visible to the query
  but cannot have reached ISCP yet" — the coverage bar machinery the marked
  completion routes through cannot produce a covered answer while the
  transaction wrote the source table (source_commit_ts.go:28-40), and a
  pushed LIMIT truncates inside SearchInto (search.go:810-825) so a
  covered-generation own DELETE of the winner leaves the discarded
  runner-up forever unrecoverable under rev-9's overlay.
- The pushed qualifications do not survive above the search: the planner
  peels INCLUDE/pk predicates out of the scan's FilterList into the
  request's predicate JSON (apply_indices_fulltext.go:652-667), the covered
  plan runs ALL qualification inside the search (:1287-1309), and the
  request carries the runtime membership filter and the INCLUDE/pk
  predicates beside the pattern (search.go:742-755) — a completion
  evaluator that tests only the match pattern emits false positives for
  gap/own rows failing the qualifications.
- Workspace reading is synchronized on the OWNING transaction's mutex
  (local_disttae_datasource.go:647-655 holds it while reading `writes`
  :717 and hydrating :757; txn.go:269-274 writes the entry's index and
  accounting fields under it) — the isolated view is a different
  Transaction with a different mutex, so rev-9's clone-on-view-mutation
  discipline did not close the parent-hydration-versus-view-read race that
  parallel pipeline fragments of one statement make real.

Two further facts verified at base (rev-11 — the tenth review's mechanism
findings):

- The shared transaction-local op chains carry parent-owned HANDLES, not
  just visibility metadata: `dbOpsChain.existAndActive` returns the
  chain's stored `*txnDatabase` payload (`x[len(x)-1].payload`,
  txn.go:3479-3487) and the twin `tableOpsChain.existAndActive` returns
  the stored `*txnTable` (txn.go:3552-3559); `Engine.Database` returns
  that PARENT database before any cache is consulted (`if v :=
  txn.databaseOps.existAndActive(key); v != nil { return v, nil }`,
  engine.go:502-518), and the created-table relation branch returns the
  PARENT `*txnTable` AND mutates its proc pointer BEFORE the tableCache
  lookup (`v.proc.Store(p); return v, nil` then `rel :=
  txn.getCachedTable(ctx, key)`, txn_database.go:125-129) — so rev-10's
  private EMPTY tableCache never intercepts transaction-created objects:
  an empty cache isolates nothing when the chain accessor hands the
  parent handle back, and the view's read point and view-owned
  hydration/accounting would be bypassed by the parent-bound handle.
- The existing reader family already carries explicit resource contracts
  the rev-10 re-evaluation lacked: the no-LIMIT reader STREAMS "every
  matching doc in bounded batches (no top-K heap, no materialization of
  the whole result set)" through a producer goroutine and a capped
  channel with cancel cleanup (search.go:773-788), and `streamBoolean`
  keeps the "peak Go heap … O(one segment), NOT O(globalN)" bound with
  "Ranking … the upstream ORDER BY score node's job"
  (index.go:502-515) while `globalStats` needs only N, avgDocLen and
  per-QUERY-term df counters ("memoized per query", index.go:355-376) —
  rev-10's replacement scan gave D-6 scan counts but NO retained-memory
  bound, an accounting/admission owner, streaming or spill contract, so
  the replacement could exceed today's per-query footprint precisely
  when the cache governor had already refused it.

One further fact verified at base (rev-12 — the eleventh review's mechanism
finding): the S3-flushed workspace DELETEs are a DISTINCT visibility input
whose registry is per-Transaction and relation-resolved — the flush path
stashes each tombstone file's ObjectStats into the OWNING transaction's
registry (`writeFileLockedWithAutoIncrEpochKnown` →
`StashFlushedTombstones`, txn.go:1931-1935/types.go:786-788), the in-memory
workspace-delete gate EXCLUDES file-backed entries (`(entry.typ ==
DELETE) && (entry.fileName == "")`, local_disttae_datasource.go:634), and
all three suppression surfaces resolve the registry through the relation's
owning transaction (applyWorkspaceFlushedS3Deletes :1386/:1394 from
ApplyTombstones :1069/:1108 and GetTombstones :1142/:1164;
CollectTombstones → getUncommittedS3Tombstone, txn_table.go:855/:863/:925,
txn.go:2846-2858) while `CloneSnapshotWS` starts the view's copy EMPTY
(txn.go:3430) — so rev-8..11's borrowed enumeration (writes prefix,
deletedBlocks, chains) lost the flushed tombstones, RESURRECTING
still-alive-object rows the transaction deleted and polluting the
re-evaluation corpus/statistics, with D-4's mask unable to restore them
(its mapping domain is rows whose original object died inside the frontier
window).

## 3. Goals / non-goals

G1 Locking reads under SI/RR read the latest committed state of the outer
block's tables through the access path and reader form actually chosen, with a
frontier ordered after the current execution's pre-admission commits, covering
all three SQL locking forms, both commit timings, candidate discovery, every
execution of a cached or prepared statement, the generated search/function
scans a marked plan uses (MATCH/vector search streams and their covered forms,
which are the plan's read view when they replace the base scan — including
async-maintained indexes, whose marked results are COMPLETED to the execution
frontier per execution, rev-8), and reads
forwarded across the table-sharding boundary.

G2 Unmarked reads — statements, ordinary nested blocks, subquery occurrences
inside the locking statement (WHERE/HAVING/JOIN-condition scalar subqueries in
both their pre-lowering form and their LEFT-lowered post-flattening form,
EXISTS/IN closed blocks, CTE bodies) —
keep the SI snapshot; the transaction's
`SnapshotTS` is never mutated by this feature. Locked occurrences whose
explicit historical snapshot ENGAGES the cohort for the executing
transaction keep that NAMED snapshot in THOSE executions (R8-F3 with the
R9-F3 per-execution engagement rule): they read through the historical
cohort exactly as today, with their lock target conserved; the current-read
machinery (F carrier, execution-family cache entry, mask, completion, wire
fields) is withheld from them in those executions and re-attaches on later
executions where the comparison no longer engages.

G3 RC behavior, DML lock targets, FK validation, table/DDL fences, SI write
w-w aborts, and the RC transfer machinery stay byte-identical. Lock
acquisition is unchanged for every occurrence class: `collectLockTargets`'
target list is byte-identical to base for every plan, including the
LEFT-lowered scalar-subquery occurrence (locked today as a pre-existing
MO/MySQL locking divergence and still locked; only its **read view** is
restored to the snapshot per MySQL's query-block rule) and including MATCH
plans in all their shapes (the covered-path variant has NO base target today
— a pre-existing coverage gap recorded by N6, conserved unchanged).

G4 A locking statement executes with current-read semantics on every fragment
it uses, or with today's semantics on every fragment — never a mixture,
across the lifetime of a cached or prepared plan (rev-15: no in-cluster
version mix exists — shutdown-only upgrade, R15-F1): per-execution admission
renews frontiers, masks and retry floors at every execution and retires them
at Reset.

Non-goals: N1 DML current-read visibility under SI; N2 locking PK-less tables
(pre-existing: no targets, no locks); N3 `refreshTableSnapshot`/DDL fences
(locked-table DDL keeps today's error); N4 RC retry/transfer internals; N5 the
dormant refresh-expression column path; N6 covered-path and direct-shape MATCH
lock coverage — the fulltext2 covered variant serving a MATCH removes the base
scan (apply_indices_fulltext.go:1303), and direct `MATCH … FOR UPDATE` shapes
cannot plan at all at base (20105; the project-anchored descent stops at Lock —
probes 3b/3c). This revision CONSERVES both exactly: the covered variant keeps
its (today empty/absent) base lock target, and direct shapes keep their
refusal; restoring lock coverage or enabling those statements is a separate
problem; N7 sharding internals: shard placement, replication and the shard
RPC lifecycle themselves (only the read view, mask transport, admission and
cancellation AT this boundary are in scope).

## 4. Alternatives evaluated

| Option | Evaluation |
|---|---|
| A1 (rev 1) shared snapshot advance/restore | rejected: nested readers share the snapshot; in-place RC transfer unsound under view returns |
| **A2 (chosen)** per-read frontier | retained; refined each revision. Rev-4: barrier-based frontier (R4-F1), node-occurrence identity (R4-F2), ordinary-local mask consumption (R4-F3). Rev-5: construction-time occurrence ownership (R5-F1), per-execution admission/reset (R5-F2). Rev-6: generated-search-scan and plugin read/cache boundaries (R6-F1), sharded-read transport (R6-F2). Rev-7: separate execution-read carrier + `ExecutionKey` family (R7-F1), own-write visibility through the execution view (R7-F2, view mechanics corrected by R8-F1), allocated capability version (R7-F3), stream-lifetime sharding ownership (R7-F4), terminal retry errors (R7-F5). Rev-8: ISOLATED execution view sharing write data only (R8-F1), mandatory marked-async completion contract (R8-F2), historical-snapshot eligibility/precedence (R8-F3). Rev-9: entry-clone-on-first-mutation borrow discipline with view-side accounting and pin-ledger teardown (R9-F1), rank-insensitive/ranking-sensitive completion split with re-evaluation at F (R9-F2), per-execution effective eligibility against the executing SnapshotTS with bind-time presence provenance (R9-F3), generation-keyed shared cache family with captured coverage bounds and refusal degradation (R9-F4). Rev-10: complete transaction-visible-corpus ranked admission with the closed covered shortcut and SourceCommitTS-refusal routing (R10-F1), uniform pushed-qualification re-application on every candidate source (R10-F2), parent-mutex-synchronized borrowed-entry access with copy-then-release and race validation (R10-F3). Rev-11: view-local metadata handles for transaction-created database/table lookups (chains consulted as predicates only, view-bound handles interned in the view's tableCache, parent payloads never returned or proc-mutated) (R11-F1); item-7 re-evaluation resource contract (streaming pass-1 statistics with per-query-term df counters, bounded-batch emission, K-bounded heap under a pushed LIMIT, no-LIMIT ranking delegated to the existing pipeline operators, execution-scoped retentions) inherited by the governor-refusal degraded run (R11-F2). Rev-12: parent-mutex-guarded non-owning value-copy capture of the parent's flushed-tombstone registry into the isolated view, keeping file lifetime with the parent's workspace entries and feeding ApplyTombstones/GetTombstones/CollectTombstones suppression plus re-evaluation corpus/statistics exclusion of flushed-deleted rows (R12-F1) |
| A3 in-operator row patching | cannot fix discovery or re-evaluate predicates |
| A4 permanent SI snapshot advance | breaks plain RR reads |
| A5 error-only | unavailability, not a fix |
| Register-node execution | new operator model; rejected |
| Rev-3 post-optimization table-identity re-derivation | rejected by R4-F2: child positions swap (opt_misc.go:544-548) and same-table occurrences merge under one TblId (flatten_subquery.go:861-867) |
| Rev-4 marking decided inside the `collectLockTargets` walk | rejected by R5-F1: the walk runs after `bindWhere`'s lowering (:9222→:9369→:9236) on a tree where LEFT-lowered scalar subqueries are indistinguishable from outer-block joins; provenance must be captured at construction time from the binder's nesting discipline |
| Plan-lifetime frontier/mask state (sticky per cached plan) | rejected by R5-F2: `Compile.Reset` reuses generations without rebinding (compile.go:287-368) and `DataSource.Timestamp` is never re-stamped, so a sticky frontier would silently miss commits made between EXECUTEs (no lock conflict, no retry); F₀ and masks must be per-execution |
| TABLE_SCAN-only marking (leave generated search/function scans on the snapshot) | rejected by R6-F1: the search stream IS a read source of the locking plan (it filters candidates before LOCK_OP and even replaces the base scan on the covered path), so snapshot-bound search would keep the reported defect reachable through `MATCH ... FOR UPDATE` and cache-confuse frontier vs snapshot reads in the shared VectorIndexCache |
| Receiver-side re-derivation of F (let the sharding receiver find its own frontier) | rejected by R6-F2: the receiver cannot recover the coordinator execution's admitted frontier from any local state (it reconstructs only the serialized ProcessInfo txn); the frontier and mask are execution-scoped properties that must be transported with the read request and pinned by the receiver through the existing clone mechanism |
| Error or conserved-stale result for marked ASYNC searches (fulltext2 and the always-async vector algorithms) | rejected by R8-F2: an error would regress today's EXECUTABLE locking-MATCH statements (the view-wrapped family runs at base, probe4), and conserving the stale result keeps the reported defect live through ISCP lag (a source commit ≤ F with no posting anywhere — no bulk segment, no in-txn row, build_dml_util.go:7237-7244); the existing self-completing probeTail decision tree (search.go:476-519) demonstrates the sound per-execution completion (covered / gap `(searched, F]` / current-read fallback), so completion with an own-SOURCE overlay and unchanged unmarked freshness-tolerance is the only contract satisfying both the current-read invariant and ordinary-read invariance |
| Per-frontier cache keying (`ExecutionKey(IndexTable, F)`) | rejected by R9-F4: each renewed F (D-2) would be a cold full load with its own whole mapping (cache.go:1246-1290; search_cache.go:63-70/:220-236) and the governor would refuse under budget (governor.go:438-493), failing locking reads that succeed today; the loaded generation with a captured coverage bound shares one entry across every F it covers, keeping the key space bounded |
| Incremental gap completion applied to scored/ranged/limited marked searches | rejected by R9-F2: corpus-global statistics move with every gap commit (fulltext2/index.go:355-376) and pushed ranges/top-K discard rows during the search (search.go:733-747/:810-825), so incremental emission cannot repair an unchanged document's score-range rejection nor recover a discarded runner-up; only rank-insensitive full-set shapes tolerate incremental completion; ranking-sensitive shapes re-evaluate over the F-complete source view with F-consistent statistics |
| Current-read overriding an explicit historical snapshot (`AS OF` under a locking clause) | rejected by R8-F3: the explicit user snapshot is the more specific visibility request and the historical cohort already engages for such scans (compile.go:5613-5622) while `collectLockTargets` still locks them today; overriding would silently redefine a documented read mode, refusing would regress executable statements — the eligible-ineligible partition with conserved lock targets preserves both behaviors |

## 5. Chosen design — components, owners, invariants

### D-1 Read-view identity is the construction-time occurrence role, determined by query-block provenance, preserved through rewrites

`proto/plan.proto`: `bool locking_read` on `plan.LockTarget` (field 20); the
scan-occurrence role on the scan node (additive field — a role enum with
default `NestedBlock`, so an unset field means "no current read"; rev-9: the
stored plan value for a locked explicit-snapshot source is the PRESENCE
provenance `OuterLockingWithExplicitSnapshot(H)` — the effective
`HistoricalSnapshot` exists ONLY as per-execution state, resolved at the
admission boundary and never serialized in the plan — see eligibility
below; bounded by
the invariant below); additive `IndexScanInfo.ParentTableId`.
`proto/pipeline.proto`: `locking_read`
(+ per-statement frontier, D-2/D-3) on shipped `pipeline.LockTarget` and
shipped `pipeline.Scope`-carried DataSource.

Marking (rev-5 core change, resolving R5-F1): the role is captured where the
scan node is **constructed**, from the live per-block locking state — not
where the lock targets are collected:

- Role source: the binder's own nesting discipline, which the base already
  maintains at every semantic block boundary — `baseBindSubquery` binds each
  subquery in a child `BindContext` with
  `subqueryNestingDepth+1` and `builder.isForUpdate = false` for the whole
  subtree (base_binder.go:963-1015; the comment there already states the
  MySQL rule that an outer FOR UPDATE does not lock subquery rows); the same
  discipline guards CTE bodies (query_builder.go:5628-5635), view bodies
  (:12687-12705) and grouping/UNION arms (:4695-4704); a subquery's own
  locking clause re-enters through bindSelect's lock parsing (:6272-6288),
  so a `FOR UPDATE` written inside a subquery still marks that subquery's
  own outer block.
- Capture site: the ordinary user-table scan construction point,
  `QueryBuilder.buildTable` (query_builder.go:13117; the `Node_TABLE_SCAN`
  node is built at :13400 with its `BindContext`), plus the shared capture
  helper: role = `OuterLocking` iff the scan is constructed while
  `builder.isForUpdate` is true; else `NestedBlock`. Because capture happens
  at construction, it is by definition **before** every lowering — WHERE
  (:9369), HAVING (:11247+), JOIN-condition (:14265) — that revision 4
  misplaced; the WHERE-clause scalar subquery in
  `SELECT o.id FROM f o WHERE o.v > (SELECT … LIMIT 1) FOR UPDATE` has its
  scans constructed inside `baseBindSubquery` with `isForUpdate = false` and
  is therefore `NestedBlock`, while the outer `f o` scan is `OuterLocking`.
- View carve-out (transparent expansion inheritance): `collectLockTargets`
  locks view-expanded scans today (their `BindContext` is not a
  CTE-binding context), so those scans read inconsistent
  lock-vs-view state if left nested. View bodies keep the existing
  `isForUpdate = false` LOCK_OP suppression (:12701: a view body must not
  emit its own LOCK_OP) but the capture helper assigns their FROM-level
  scans the outer statement's `OuterLocking` role via an explicit
  inheritance flag set along the view-expansion path; nested blocks inside
  the view (its own WHERE subqueries, etc.) stay `NestedBlock`. CTE bodies
  stay `NestedBlock`, consistent with `collectLockTargets`' existing CTE
  exclusion (their scans are neither locked nor current-read).
- Eligibility precedence (rev-8, resolving R8-F3; **timing corrected by
  R9-F3**): PROVENANCE, captured at construction where `ScanSnapshot` is
  known (buildTable, query_builder.go:13666), records ONLY presence facts —
  the scan is constructed in the locked block AND its table source carries
  an explicit snapshot clause with TS H (no comparison, no mode; stored as
  plan data `OuterLockingWithExplicitSnapshot(H)`). The EFFECTIVE role is
  resolved PER EXECUTION at the admission boundary (D-8 layer-3 position,
  before any marked-consumer setup and before the statement's D-2 barrier):
  engaging iff H is non-zero and strictly earlier than the EXECUTING
  transaction's SnapshotTS — the SAME predicate the reader applies at
  datasource build (compile.go:10854-10866 and its twin :5609-5622) —
  yields effective `HistoricalSnapshot` (ineligible: no F, no execution
  cohort, no execution-family cache entry, no mask, no sharding execution fields,
  no async completion; reads at the named snapshot through the EXISTING
  historical cohort byte-identically; lock target conserved), else effective
  `OuterLocking` (eligible). Both directions of SnapshotTS-crossing reuse
  resolve per execution by the same rule; the effective mode installs on the
  execution's materialization and dies with the execution (D-8 retirement),
  never rewriting the cached plan.
- `collectLockTargets` unchanged in role: it remains the LOCK authority; its
  walk (:15027), its SEMI/ANTI/SINGLE/MARK exclusion (:15038) and its CTE
  boundary guard are preserved verbatim. It no longer decides marks. The
  plan-level eligibility summary for D-8 is computed at bind end from
  whether any node carries locked-block provenance (plain `OuterLocking` or
  the presence provenance `OuterLockingWithExplicitSnapshot(H)`; the
  effective step happens per execution at admission, D-8 layer 3).

Preservation rules (retained from rev 4):

- Node-preserving rewrites need no work: the role rides the node object
  through re-parenting, join-type changes, `IsRightJoin` child swaps
  (opt_misc.go:544-548), projections removal, and scalar-subquery lowering
  to LEFT/SINGLE joins.
- Rewrites that FABRICATE new scan nodes must propagate: `apply_indices`
  constructs the index-table scan node at two sites (apply_indices.go:4136-4153,
  4690-4706) — those sites set the role from the source node and set
  `IndexScanInfo.ParentTableId` (the original base scan survives and keeps its
  role; the `Node_INDEX` join reuses it at :4721). **Generated search scans
  (rev-6, R6-F1) are covered by the same audit and helper**: the classic
  fulltext search stream (`buildFullTextMatchSearchScan`,
  apply_indices_fulltext.go:458, its Boolean-composition per-leaf streams at
  apply_indices_fulltext_boolean.go:131 and the vector project-with-order
  entry), the fulltext2 `Node_INDEX_SEARCH_SCAN`
  (`buildFullText2SearchScan`, apply_indices_fulltext2.go:181-223 —
  `IndexSearchScan.SourceTable`/`SourceTableDef` identify the base occurrence
  the site copies from), and its covered variant (apply_indices_fulltext
  .go:1291-1330, which REMOVES the base scan): in the covered variant the role
  travels to the covering scan so the statement's read view (now emitted by
  the search stream alone) is still admitted at F; the lock-target set is
  conserved unchanged (no base target exists today — N6). Fabrication sites
  found in other rewrites are enumerated at implementation through a one-time
  audit of `Node_TABLE_SCAN`, `Node_FUNCTION_SCAN` and
  `Node_INDEX_SEARCH_SCAN` construction, and handled by one shared
  propagation helper.
- There is no post-optimization structural re-walk; block-provenance rules are
  evaluated exactly once, at construction time, where they are valid.

Invariant R-D1 (rev-9 restated): **bind time** — a scan node's stored
provenance records, exactly once at construction, whether its occurrence was
built inside the outermost locking block (per the nesting discipline; view
bodies inherit along the transparent expansion), plus the PRESENCE of an
explicit snapshot clause and its TS H (no mode comparison; lock-target set
byte-identical to base for every corpus plan). **Execution time** — each
execution resolves the EFFECTIVE role as a pure function of (stored
provenance, executing SnapshotTS) at the admission boundary: presence + H
engaging (H non-zero, `Less(execSnapshotTS)`) → `HistoricalSnapshot`
(ineligible, historical cohort, lock target conserved); otherwise
`OuterLocking` (eligible). Typed-plan occurrence-corpus UT: forced-index plans mark the
index scan and the base lookup; right-oriented SEMI/SINGLE swaps mark the
outer occurrence regardless of final child order; a same-table
**WHERE**-scalar subquery (LIMIT 1 form) marks only the outer occurrence
while its lowered LEFT-join occurrence of the same table stays unmarked;
JOIN-condition and HAVING scalar subqueries stay unmarked; EXISTS/IN bodies
stay unmarked regardless of swap orientation; view-body scans are marked;
CTE-body scans are unmarked; DML appendLockNode plans mark nothing; unmarked
plans are byte-identical to today. MATCH-family corpus (rev-6): a JOIN-path
fulltext MATCH locking plan marks BOTH the surviving base scan and its
fabricated search stream (role inherited through `IndexSearchScan.SourceTable`),
and the covered-path variant marks the covering search scan while its base
target list stays exactly as today (empty — N6). Eligibility corpus (rev-8
R8-F3, rev-9 timing per R9-F3): an `AS OF` source under a locking statement
records the presence-provenance fact `OuterLockingWithExplicitSnapshot(H)`
— the corpus asserts presence and the stored H only, since the effective
role is execution-derived; a snapshot-bearing VIEW body behaves the same;
a named-snapshot search source keeps `sp.EffectiveSnapshotTS`/`SnapshotKey`
semantics byte-identically (the execution cohort is NOT engaged); a
snapshot clause is present regardless of engagement. The per-execution
effective-mode matrix (rev-9: cached/prepared plans crossing S0 <= H < S1
and the reverse) asserts BOTH modes per execution: eligible delivers F to
both consumers; ineligible delivers F to NONE while the historical clone
still engages (compile.go:10854-10866 twin :5609-5622); lock-target list
byte-identical across both executions. **The corpus additionally asserts that
`collectLockTargets`' output list is byte-identical to base for every
corpus plan** (the lock set never changes; only the read role does).

Rev-13 audit note (provenance preservation): "preserved through rewrites" is
CONTRACT over every copy and fabrication site, not only the identity rule —
`DeepCopyNode` copies `scan_read_role` verbatim, the apply_indices fabrication
sites propagate the source role and set the additive `IndexScanInfo.ParentTableId`,
and the fulltext2 covered-plan strip remains the single enumerated allowlist with
recorded rationale. The delivered slice missed exactly these forms (R13-F3;
demonstrated on the index path this round), and the statement-level closure check
added in D-10/G4 makes a vanished mark a whole-statement decline rather than a
silent snapshot co-occurrence.

Rev-14 representation closure (R14-F2; rev-15 wording: the gate is deleted so
"above-gate plan" reads "any completed plan of this feature"): the zero value's meaning is hereby
narrowed — construction emits ONLY the explicit set
(`OuterLocking`, `OuterLockingWithExplicitSnapshot`, the new
`NestedBlockExplicit`, or the covered-plan strip value; the fulltext2 mutation
sets the strip value instead of plain nested). An unset (zero) role is then
never produced for the scans of a completed plan and can only be a
mark lost through a rewrite; the G4 declinature check treats any unset-role
scan inside a locking-presence plan as a vanished mark (whole-statement
decline before source publication), while intentional nested blocks are exempt
by representation. Rev-15 amendment (R15-F1): the legacy/below-gate clause is
deleted — zero stays the plain-proto wire default (it carries no gating
meaning inside a cluster, where every service runs one binary), and the enum
extension stays additive for wire-decode safety only.
O-D9 is resolved by this mechanism; O-D4's summary storage shape stays
bounded.

### D-2 Frontier acquisition: publication barrier before source assembly

Owner: `Compile`. State: per-execution frontier on the execution's process
(plus its wire copy on shipped scopes, D-3); retry generations evolve it per
D-5's floors.

Acquisition of F₀ (revision-4 core change, carried forward; rev-9
eligibility condition from R9-F3): for a SI locking-read SELECT whose
occurrences resolve at least one ELIGIBLE
`OuterLocking` execution-mode at the admission boundary, the execution acquires
`F₀ := getLogtailReadBarrier(engine).AcquireLogtailReadBarrier(statementCtx)`
before assembling marked scans' sources. The barrier contract (verified at
disttae/logtail_consumer.go:238-269 and 272-293): it "obtains a TN-ordered
publication frontier and waits until the normal CN logtail apply pipeline
reaches it" — so every commit that completed before this execution's
admission is
inside F₀ (publication ordering), and this CN's apply pipeline has reached ≥ F₀
before the call returns. This supersedes `WaitLogTailAppliedAt(empty)`, which
returns the local latest without acquiring a newer publication frontier
(timestamp_waiter.go:83-85) and therefore cannot be trusted for discovery.

Ordering (rev-5, extending to every execution — R5-F2): F₀ is acquired exactly
once per **execution** at the marked-read source-refresh boundary, before
any marked source starts producing data:

- Cold executions (fresh compile, and every session plan-cache hit, which
  rebuilds scopes per execution): at the start of compile source assembly, so
  marked Ranges, `DataSource.Timestamp` and the mask are computed with F₀
  (as in rev 4).
- Reused executions (a retained prepared `Compile` re-entered through
  `Reset` + Run, compile.go:287-368/:626-631): at the per-execution reader
  rebuild — `resetForReuse` drops the previous reader (`DataSource.R = nil`,
  scope.go:304-307) and `Scope.Run` rebuilds it via `getRelData`/`buildReaders`
  (scope.go:397-405) with the relation re-opened per execution
  (`handleDbRelContext`) — the fresh F₀ is acquired in the same
  per-execution sweep that recalculates the marked scope's source state
  (alongside `UpdateScopeTxnOffset`, compile.go:352-361), the marked scopes'
  `DataSource.Timestamp` is re-stamped to this execution's F₀ there, and the
  barrier's bounded wait completes before the first marked reader builds.
  A frontier may never be inherited from a previous execution (see D-8).

Remote fragments receive the per-execution F₀ on the wire and read at exactly
that F₀.

Retry admission (unchanged floor semantics): on accepting the retry signal,
after the failed generation quiesces,
`F_{k+1} := WaitLogTailAppliedAt(max(requiredTS, F_k))`. With a concrete floor,
the waiter genuinely waits for the applied pipeline to cover the floor
(the empty-floor shortcut does not apply); `requiredTS` comes from
lock-service-observed commits (D-5), so every observed commit is admitted
before the next attempt. `F_k` is always the **current execution's** frontier
(D-8): retry counters and floors are execution-scoped and reset with the
execution (`c.retryTimes = 0`, compile.go:335) rather than the cached plan.

Failure/liveness: the barrier and the admission wait are bounded by the
execution's statement context; cancellation aborts the statement cleanly (no
spin). Each
retry strictly raises the frontier; locks held prevent new versions of locked
rows. Cost: one barrier per locking statement execution — the same primitive
lifecycle
admission and CDC guards already pay per operation
(compile/lifecycle_admission.go:283-295; frontend/session.go:101-124).

### D-3 Read surfaces and transports

Marked relations resolve readTS from the admitted frontier of the current
execution (lazily; default
everywhere = `op.SnapshotTS()`); only occurrences resolving ELIGIBLE for THIS
execution do this — occurrences whose effective role is `HistoricalSnapshot`
(D-1, per-execution engagement resolution R9-F3) ride the historical cohort
unchanged and receive no readTS parameterization, mask, or wire frontier on
any surface below:

A. Relation-side reads: candidate/object discovery (txn_table.go:1430,
1272/1507); partition admission (2833 → readTS, statement-context bounded);
reader construction (2778); local source visibility
(local_disttae_datasource.go:83); committed-tombstone collection for the
shipped tombstone view (txn_table.go:946, :972 inside
`CollectTombstones`/`collectTombstones` 855/863 — at readTS, else frontier
reads resurrect peer-deleted rows). The mask (D-4) attaches to the collected
set for marked reads.

B. Shipped-scope reads: `getCompileTableScanDataSource` (compile.go:5757,
:5810) — marked scans set `DataSource.Timestamp := F` (the frontier from D-2),
so shipped blocks read at F via `BuildBlockReaders(ctx, proc,
s.DataSource.Timestamp, ...)` (scope.go:1955) → readers (engine.go:1326),
unchanged consumers. Marker + frontier ride the shipped Scope-wire DataSource
so remote-`Rel`-form fragments install the same wrapper at decode time
(remoterunServer.go:576).

C. Ordinary local readers: consume the mask through the new mask-only input
(D-4) in both `ApplyTombstones` and `GetTombstones`; the RelData
extraTombstones gate stays sharding-remote-only (unchanged legacy behavior).

D. **Generated search scans and their plugin execution (rev-6/rev-7/rev-8, resolving
R6-F1 + R7-F1/R7-F2 + R8-F1/R8-F2/R8-F3).** Marked search scans (MATCH/vector streams carrying
the inherited role, D-1) read at the same per-execution F on every hop of
their execution, WITHOUT reusing the named-snapshot cohort (which rejects
F ≥ SnapshotTS) and WITHOUT dropping the transaction's own writes:

- Compile: `compileIndexSearchScanDataSource` stamps the marked search
  scan's `DataSource.Timestamp := F` (compile.go:5873 for marked scans,
  SnapshotTS for everything else); on reused executions this re-stamp joins
  the D-2/D-8 per-execution source-refresh sweep so a cached prepared MATCH
  plan cannot pin an old frontier.
- Execution-read carrier (O-D7, RESOLVED in rev-7): F rides its own additive
  field on the search scan (plan `IndexSearchScan` + the Scope-wire
  DataSource), consumed by a NEW helper cohort
  `ApplyExecutionReadTS(F)`/`EffectiveExecutionTS()` that accepts F in every
  relation to SnapshotTS (typically F > SnapshotTS after barrier admission)
  and never rewrites `SnapshotTS`. The named-snapshot cohort
  (`ApplyScanSnapshot`/`EffectiveSnapshotTS`/`ScanSnapshot.Less`) keeps sole
  ownership of historical semantics untouched, including its refusal of
  non-earlier TS (sqlexec.go:147-152, snapshot_test.go:60-71). Rev-9
  partition: a search source whose explicit historical snapshot ENGAGES for
  THIS EXECUTION resolves `HistoricalSnapshot` (D-1 per-execution rule) and
  NEVER receives the execution cohort in that execution — it rides the
  named-snapshot cohort byte-identically (R8-F3 + R9-F3).
- Reader assembly: `buildVectorIndexReaders` derives the search identity
  and coverage from the marked scope's F (`currentSnapshot`, scope.go-2127;
  `vectorscan.Identity`, :2131-2134 — the identity takes the execution TS as
  its read point for marked scopes beside the preserved historical case)
  instead of the transaction SnapshotTS when the scope is marked.
- **Execution view — an ISOLATED pair sharing write data only (rev-8,
  resolving R8-F1).** The plugin's index-load SQL runs under
  `CloneExecutionReadOp(op, F)`, which constructs a NEW operator wired to a
  NEW private `Transaction` on the verified `CloneSnapshotOp`/`CloneSnapshotWS`
  seam (operator.go:789/:798, txn.go:3414-3438) — it never shares the parent
  workspace object, because `getTxn` resolves the workspace's own
  `*Transaction` (txn_database.go:47-48), the workspace's interned relations
  are returned unchanged under SI (txn.go:2968-2978) and pin their read point
  from THEIR operator (local_disttae_datasource.go:83), and rebinding would
  write the shared `Transaction.op` (txn.go:3441-3442). PRIVATE to the view
  (never visible to the parent): the execution operator and the paired
  workspace it points to, a NEW EMPTY `tableCache` (relations used by the
  marked execution are constructed through the VIEW's database, so
  `db.op` resolves the execution cohort and the datasource read point is F;
  the parent's warmed cache and its snapshot-bound relations are never
  consulted, never mutated), fresh `batchSelectList`/`cnObjsSummary`, and
  `readOnly = true` (write admission rejected — no DML, no
  `IncrStatementID`/`UpdateSnapshotWriteOffset` side effects beyond the
  interface's safe read-only implementations). SHARED READ-ONLY from the
  parent, bounded at view construction (the parent's append-only `writes`
  prefix captured as a fixed offset, its `deletedBlocks` state, its
  `tableOps`/`databaseOps` chains for created/dropped-table visibility):
  this is what keeps the transaction's own writes visible at F. **Borrow
  immutability against actual readers (rev-9, resolving R9-F1):** the
  disttae exact-PK probe lazily mutates the entry it reads —
  `ls.table.getTxn().ensureInsertKeyOffsetsLocked(&writes[ls.wsCursor])`
  (local_disttae_datasource.go:757) writes `entry.keyOffsets` and charges
  `keyIndexAccountedSize`/`workspaceSize`/`approximateInMemInsertSize`
  through the RECEIVING transaction (txn.go:242-275), with refunds flowing
  back through the owning transaction's single choke point
  (txn.go:310-323) and `readOnly` guarding only `Commit` (txn.go:80/:3003).
  The view therefore supplies an ENTRY-CLONE-ON-FIRST-MUTATION indirection
  at its reader boundary: a borrowed entry that a reader would lazily
  mutate is first cloned (shallow clone sharing the batch vectors
  read-only, private `keyOffsets`/`keyIndexAccountedSize` tail) into
  view-private storage; hydration writes the CLONE and the charge lands in
  the VIEW's balances; clones are unreachable from the parent's writes
  slice, so every view charge dies at retirement and no parent refund path
  ever sees them. Clone scope is bounded to the entries the view actually
  point-probes (its own INSERT lookups), not the whole prefix. Shared
  batch data: the captured prefix keeps a stable backing array (Go growth
  reallocates only the parent's slice header), view readers never write
  batch vectors, and every pin a view reader takes on shared batches is
  recorded in a VIEW-PRIVATE PIN LEDGER balanced at retirement — the view
  never releases a pin it did not take and never drops one the parent
  owns (non-owning teardown). **Synchronization with parent-side
  mutation (rev-10, resolving R10-F3):** the view is a SEPARATE
  `*Transaction` with its own mutex, but workspace readers take the
  OWNING transaction's mutex (`ls.table.getTxn().Lock();
  ls.rc.WorkspaceLocked = true`, local_disttae_datasource.go:647-655),
  read and cache entries under it (:717) and hydrate under it (:757), with
  `accountInsertKeyOffsetsLocked` writing the entry fields under that
  same lock (txn.go:269-274) — and parallel pipeline fragments of one
  statement give real concurrency between parent-side readers and the
  view's borrow (rev-9 blocked view→parent mutation; it did not
  synchronize parent-side lazy hydration against view reads). The view's
  BORROWED-ENTRY ACCESS therefore executes under the PARENT transaction's
  mutex, exactly as ordinary parent readers take it: snapshot, clone and
  probe of a borrowed entry run inside `parent.Lock()`, COPYING the needed
  entry state (stable fields, hydrated `keyOffsets` if present, clone
  target) under the lock and releasing BEFORE any view-private state is
  exposed or mutated. Lock ordering: the parent mutex is strictly
  OUTERMOST whenever both could be involved; the view never holds its own
  mutex while acquiring the parent's and parent readers never touch view
  locks — no inversion exists. Pin-ledger entries are recorded under the
  parent mutex; retirement unpins through the objects' atomic refcounts
  (no parent wait) and disposes clones/ledger privately; view-side
  accounting still lands in the view's balances after the copy
  (conservation unchanged). **View-local metadata handles (rev-11, resolving
  R11-F1):** the shared `databaseOps`/`tableOps` chains are consulted as
  PREDICATES ONLY (existAndDeleted/existAndActive answered WITHOUT taking the
  payload) because their accessors return parent-owned HANDLE OBJECTS
  (txn.go:3479-3487/:3552-3559) — `Engine.Database` returns the parent
  database (engine.go:502-518) and the created-table relation branch returns
  the parent `*txnTable` while proc-storing it (txn_database.go:125-129),
  bypassing the private tableCache; when a chain marks a database or table
  created in this transaction, the view CONSTRUCTS a view-local handle from
  the created object's definition (view-side `txnDatabase` bound to the VIEW
  operator; view-side relation interned in the VIEW's tableCache), so the
  handle's `db.op`/`getTxn` resolve the VIEW (read point F; view-owned
  hydration/accounting) and NO parent handle is returned to or proc-mutated
  by the view; create/drop visibility is unchanged (predicate parity with
  the parent path). **Flushed-tombstone capture (rev-12, resolving
  R12-F1):** the S3-flushed workspace DELETEs are a DISTINCT visibility
  input — the flush stashes each tombstone file's ObjectStats into the
  OWNING transaction's registry (`writeFileLockedWithAutoIncrEpochKnown`
  → `StashFlushedTombstones`, txn.go:1931-1935/types.go:786-788), the
  reader's in-memory gate EXCLUDES file-backed entries
  (`(entry.typ == DELETE) && (entry.fileName == "")`,
  local_disttae_datasource.go:634) and the suppression reads the registry
  through the RELATION's owning transaction (applyWorkspaceFlushedS3Deletes
  :1386/:1394 from ApplyTombstones :1069/:1108 and GetTombstones
  :1142/:1164; CollectTombstones → getUncommittedS3Tombstone,
  txn_table.go:855/:863/:925 with txn.go:2846-2858) — so the cloned view's
  FRESH EMPTY registry (txn.go:3430) would RESURRECT flushed-deleted rows.
  The view therefore CAPTURES the parent's flushed tombstones at
  construction: a parent-mutex-guarded VALUE-COPY of every registry entry
  (ObjectStats is a value type) into the view's own fresh registry, copied
  under `parent.Lock()` per the R10-F3 outermost-lock rule and released
  BEFORE any view-private state is exposed. The capture is NON-OWNING: no
  Delete on the parent's registry, no file operation, no lifetime claim —
  the tombstone FILES keep the parent's workspace-entry ownership (commit
  transfers them to TN; rollback/GC removes them), the parent's registry
  keeps its own retirement paths (commit-GC entry removal types.go:1147;
  `delTransaction` teardown txn.go:3295/:3321), and the view's copy DIES
  at the per-execution retirement sweep with the rest of the view.
  Bounds: one capture per view construction (a flush landing after
  construction belongs to the NEXT execution's view, D-2/D-8), retained
  size bounded by the transaction's own flushed-file count. Consumed: the
  carried snapshot feeds the SAME three read surfaces — ApplyTombstones,
  GetTombstones and CollectTombstones — so a flushed-deleted row stays
  suppressed in EVERY view consumer, including the re-evaluation candidate
  scan and its pass-1 corpus/statistics (no resurrection, no stale
  counts); still-alive target objects need no D-4 remapping (D-4's mapping
  domain is rows whose ORIGINAL object died inside the frontier window).
  `BindTxnOp`
  is NEVER invoked (the pair is wired by field assignment at construction);
  the parent's operator, cached relations, entries, accounting and
  SnapshotTS are
  identity-stable across every marked execution. Lifecycle/cleanup: created
  after the barrier at the marked execution's source assembly, destroyed at
  that execution's retirement (reader teardown / the Reset sweep) with
  exactly zero state surviving into the parent or the next execution;
  borrowed containers remain owned by the parent; cloned entries and
  ledgered pins die with the view.
- Own-write visibility, two families (rev-8, resolving R8-F2's overlay
  direction): for SYNC-maintained indexes (`!IsAsync` — classic fulltext,
  ivfflat), own writes ARE index-table rows committed in-transaction
  (build_dml_util.go:7274+), and the isolated view's shared write window
  makes them visible at F as rev-7 required; for ASYNC indexes
  (`indexplugin.IsAsync` — fulltext2/hnsw/bm25/cagra/ivfpq, or `async=true`,
  indexplugin/async.go:31-56), async DML maintenance writes NO in-txn
  postings (build_dml_util.go:7237-7244), so the marked search overlays the
  transaction's own SOURCE rows directly, per the completion contract below.
- **Marked ASYNC-search completion contract (rev-8, resolving R8-F2).** A
  plain execution-view pin is INSUFFICIENT for a marked search over an
  async index: the ISCP consumer trails the base table (coverage.go:16-27)
  and only already-appended CDC-tail segments load (search_cache.go:174-192),
  so a source commit ≤ F can be absent from every loaded generation. The
  marked async search therefore MANDATORILY completes to a per-execution
  COMPLETED-COVERED result through F:
  1. Generation capture unchanged: the searched generation's build_ts is
     captured under the cache-entry lock (the existing
     `tailSearchedBuildTS` discipline), and coverage is decided against the
     EXECUTION's F using the coverage plugin's bar/watermark protocol (the
     coverage.go ordering property — a persisted watermark never ahead of
     durable index data — keeps a conservative decline sound). **Workspace
     fail-closed routing (rev-10, resolving R10-F1):** `SourceCommitTS`
     ERRORS when the source table has transaction-local writes ("they are
     visible to the query but cannot have reached ISCP yet",
     source_commit_ts.go:28-40) — the marked execution treats that refusal
     as COVERAGE UNDECIDED and falls to the same completion paths below
     (never a user error, never a silently weaker covered decision); the
     coverage decision therefore never depends on a value the workspace
     cannot produce while own writes exist.
  2. Cases (mirroring the verified self-completing probeTail decision tree,
     search.go:476-519, generalized from JSON probes to marked MATCH/vector
     searches): (a) generation NEWER than the F-physical bound (a
     shared cache entry built past F by a concurrent load)
     or the gap `(searched, F]` crosses a source DDL → CURRENT-READ
     FALLBACK: the emitted set comes from a source scan AT F through the
     isolated view with the match/distance predicate evaluated at runtime on
     source rows (complete over source state ≤ F; the emitted pk set replaces
     index selection — with the item-3 qualifications applied to every
     emitted row); (b) generation BEHIND F → completion by
     **RE-EVALUATION AT F** (rev-9, resolving R9-F2: scores are corpus-GLOBAL
     — `globalStats` N/df/avgDocLen, fulltext2/index.go:355-376 — and the
     engine applies pushed score ranges and LIMIT top-K DURING the search
     (search.go:733-747, :810-825), so incremental changed-row evaluation
     cannot repair an unchanged document's score-range rejection nor
     recover a top-K runner-up already discarded): the marked search first
     classifies its RANKING SENSITIVITY at plan build (presence of a pushed
     ScoreRange, a pushed LIMIT/top-K, a score output column, or a
     score-bearing ORDER BY). RANK-SENSITIVE: the whole ranked evaluation is
     recomputed over the F-complete source view with F-consistent corpus
     statistics — text indexes in two passes (pass 1 computes N/df/avgDocLen
     over the F-visible corpus INCLUDING the transaction's own source rows;
     pass 2 matches, scores and orders the union committed-at-F ∪ own
     writes), vector
     indexes in one pass (distances need only the query and doc vectors) —
     and score range, ordering, top-K truncation and pk-dedup are applied
     ONCE at the end of the merged candidate set, so no downstream stage
     ever sees an intermediate or statistically-mixed set; candidate
     replenishment is unnecessary because re-evaluation considers every
     F-visible row (own writes included — an own DELETE of the bulk winner
     makes the truncated runner-up re-emerge) before any limiting.
     RANK-INSENSITIVE (no range, no
     LIMIT, no score output/order — every matching candidate emits, the
     emitted set is statistics-free): the cheaper incremental gap completion
     stands — changed rows in `(searched, F]` are
     evaluated against the index's match predicate at runtime (inserts emit,
     deletes suppress; same tokenization/parser configuration as the index's
     build); (c) generation COVERED at F AND the transaction's own-write
     window for that source table is EMPTY → the bulk index result is
     **complete against the transaction-visible corpus** at F (statistics
     consistent, no corpus member suppressed), no completion; with own
     source writes present the covered generation is NOT sufficient for
     RANK-SENSITIVE shapes (the bulk truncation and corpus statistics were
     computed without them) — the execution routes to the same re-evaluation
     at F treating the union as the corpus, while RANK-INSENSITIVE total-set
     shapes keep the covered bulk plus the item-4 overlay (rev-10, resolving
     R10-F1: the covered shortcut now requires committed coverage AND an
     empty own-write window). Runtime predicate
     evaluation uses the engine's
     existing tokenizer/search-predicate entry points on source rows (the
     same parsers the index build uses — classic `fulltext_index_tokenize`
     and the fulltext2 term pipeline); it is an internal path, never a
     recursive public-SQL re-probe (O-D8 bounds the exact entry points).
  3. **Execution qualification (rev-10, resolving R10-F2):** the COMPLETE
     pushed search request is re-applied UNIFORMLY to every candidate
     source — bulk emissions (already qualified in-search), gap rows,
     current-read replacement rows and own-write rows. The planner peels
     the INCLUDE/pk predicates out of `scanNode.FilterList` into the
     request's `IncludePredsJSON` (apply_indices_fulltext.go:652-667) and
     the covered plan runs ALL qualification inside the search
     (:1287-1309 — "All qualification now runs inside the search", the
     MATCH filter removed from the base scan), with runtime membership
     carried separately as `FilterBytes` (search.go:742-755) — nothing
     above re-checks, so the completion evaluator consumes
     `IncludePreds`/`FilterBytes` DIRECTLY from the request and applies
     them (pattern ∧ INCLUDE/pk predicate ∧ membership) against INCLUDE
     values resolved AT F through the isolated view, BEFORE the final
     one-shot ranking/truncation and score range of item 2 — a
     token-matching gap or own row failing the INCLUDE predicate or
     membership is a false positive that this contract eliminates.
  4. Transaction-local overlay (applied in every case): the isolated view's
     shared write window supplies the transaction's own SOURCE rows — own
     INSERT (runtime-evaluated: add on match), own UPDATE (suppress the old
     pk, evaluate the new doc), own DELETE (suppress) — the ONLY own-write
     carrier that exists for async indexes; item 2's RANK-SENSITIVE
     re-evaluation consumes the SAME rows as corpus members.
  5. Covered output: INCLUDE columns for completion, fallback and
     own-write rows resolve at F through the same isolated view
     (covered-path output, apply_indices_fulltext.go:1303, inherits F
     because the stream is the read view).
  6. Per-execution renewal, unchanged unmarked contract: coverage and
     completion re-evaluate on EVERY execution against that execution's F;
     completion/overlay state is per-execution private (the shared cache
     stays COMMITTED-ONLY). UNMARKED searches keep today's contract
     BYTE-IDENTICALLY — user MATCH stays freshness-tolerant, JSON-probe
     `probeTail`/`ProbeTail` machinery and `PreparedPlanDependsOnIndexCoverage`
     are untouched, plan reuse unchanged; the completion engages only when
     the marked role AND an `IsAsync` index coincide. Cost is bounded by the
     changed-row window `(searched, F]` (case b) or a predicate-filtered
     source scan (rare fallback), per execution, on top of the one D-2
     barrier.
  7. **Re-evaluation resource contract (rev-11, resolving R11-F2):** the
     full-source re-evaluation (case (b) RANK-SENSITIVE, the newer/DDL-span
     fallback, and the R9-F4 governor-refusal degraded run) executes under
     the same bounded disciplines the existing paths already obey
     (search/search.go:773-788 "STREAM every matching doc in bounded
     batches (no top-K heap, no materialization of the whole result set)";
     fulltext2/index.go:502-515 "peak Go heap … O(one segment)" with
     "Ranking … the upstream ORDER BY score node's job"): pass-1 statistics
     are a STREAMING aggregation (scalar N/avgDocLen accumulators plus
     per-QUERY-term df/phrase counters — the globalStats dfCache discipline,
     never a corpus-wide term map); pass-2 emits in bounded batches with no
     evaluator-side whole-result retention; NO-LIMIT shapes (including
     score-output and score-ORDER queries) delegate ranking to the EXISTING
     pipeline Sort/Limit operators (whose spill/accounting contracts govern
     any pipeline-side materialization); pushed-LIMIT shapes use a K-BOUNDED
     top-K heap (the SearchInto reusable-ColumnBuffers discipline) with
     retained rows O(K); score-range-only shapes stream-filter; suppression
     retentions are O(workspace own-write window) — plus, on the incremental
     rank-insensitive path only, O(gap window) — and the re-evaluation scan
     needs no merge-dedup (the F-source view yields one live row per pk).
     ALL evaluator retentions (counters, heap, suppression sets) are
     EXECUTION-SCOPED on the statement's memory pipeline, released at the
     per-execution retirement — no new budget/governor admission, because
     the retained orders match the existing paths; exhaustion and
     cancellation follow the existing reader discipline (upstream
     close/cancel stops the pass through the statement context; close
     releases pins, clones and ledgered state).
- Coverage and cache (rev-9 re-keying, resolving R9-F4): the shared
  `VectorIndexCache` stores COMMITTED-ONLY entries. Marked executions use a
  NEW **generation-keyed execution family** — an entry is keyed by the
  (IndexTable, LOADED GENERATION) and carries (generation identity; the
  CAPTURED COVERAGE BOUND — the highest source commit the load covered,
  captured atomically with the load; the committed-only payload) — where
  rev-7/rev-8's `ExecutionKey(IndexTable, F)` keyed by the frontier itself,
  and the per-execution renewal of F (D-2) would have turned every distinct
  F into a cold full load with its own whole mapping
  (pkg/vectorindex/cache/cache.go:1246-1290 exact-key miss → Preload →
  makeRoom → full Load; "The mapping is not shared between cache entries …
  N generations could pin N files", search_cache.go:63-70/:220-236) and the
  governor refuses under budget (governor.go:438-493) — failing a locking
  read that succeeds today. Sharing rules: an execution at F accepts any
  resident entry whose captured bound COVERS F per the coverage protocol
  (all F's an entry covers share it — consecutive and reused-plan executions
  of an unchanged index share ONE resident entry; no O(F) key space
  remains); an F beyond every resident bound triggers ONE fresh load at the
  execution read point (the isolated view lands the load at F) under the
  existing LoadOrStore coalescing; the execution holds its entry under the
  existing cache read-lock discipline for the stream duration and releases
  at read close; generation replacement happens only through the existing
  idle-reclaim/TTL policy with pinned entries un-evictable, so resident
  multiplicity is bounded by (index table, live-generation) — one per index
  in steady state, +1 transiently only while a pinned older generation
  coexists with a fresh load. **Refusal degradation:** if the governor
  refuses the fresh load, a marked execution never fails for cache capacity
  — it degrades to the R9-F2 re-evaluation path EXECUTING THE ITEM-7
  RESOURCE CONTRACT (correct results at fallback
  cost, no index entry needed, bounded retentions — the degraded run is not
  an unbounded scan); unmarked executions keep today's refusal
  behavior unchanged. Own-workspace
  overlays and per-execution completions are applied per execution on top of
  the accepted entry and are never stored into or keyed into shared entries;
  the tail/probe read point is `EffectiveExecutionTS()` for marked
  executions (:763-767 pattern). A named-snapshot search source keeps the
  `SnapshotKey` family (R8-F3: the families stay partitioned by role, not
  just by TS).
- Typed assertion: compile-level UT asserts marked MATCH plans deliver F to
  BOTH consumers — the surviving base scan (or the covering search scan on
  the covered path) and the search stream's identity/TS pin — and that
  unmarked MATCH plans keep every hop at SnapshotTS byte-identically; the
  pin-helper UT drives the ACTUAL helpers with F > SnapshotTS (execution
  cohort pins; named-snapshot cohort still refuses the same value); eligible
  vs `HistoricalSnapshot` PER-EXECUTION partition asserted (no F on any
  consumer of an occurrence resolving ineligible in that execution).

E. **Forwarded sharded reads (rev-6/rev-7, resolving R6-F2/R7-F4) — the
contract lives in D-9; listed here because it is a read surface, not a
lifecycle rule.**

Not parameterized (unchanged): statistics/estimates (285/450/1240),
`MaxAndMinValues` (651/709), the meta property walk (828), `getCommittedRows`
(3625), catalog/engine resolution (engine.go:460/518/524),
`PrimaryKeysMayBeBlocked` explicit from/to. Classification checklist test
mandated over remaining SnapshotTS consumers.

Rev-13 closure (carrier-consistent surfaces — R13-F2): the delivered slice
bound the carrier at the scan's `DataSource.Rel` only. Contract now: a marked
scope's DISCOVERY relation opens through the execution carrier (the
`handleDbRelContext` marked-scope form; `expandRanges` then enumerates at F,
aligned with the carrier-aware innermost filters at txn_table.go :1272/:1430/
:1507); the remote single-scope `DataSource.Rel` replacement RETAINS a
per-scope carrier clone — or the whole statement declines until D-9's transport
lands (stage-2; never a silent snapshot-bound replacement);
`CollectTombstones`'s persisted path resolves the carrier's `SnapshotTS()`
instead of the `Txn().SnapshotTS` meta field; and a carrier-consistency AUDIT
(static) requires every snapshot resolution on a carrier-bound relation to read
`db.op.SnapshotTS()`. The delivered carrier form (an operator wrapper delegating
the session workspace, lock identity and metadata unchanged) is sound for the
relation-read surface exactly under this every-consumer rule — the item 8 (vi)
real-consumer members read live committed data and own writes through it and
assert own/flushed-delete suppression equivalence; the ISOLATED execution-view
form remains the approved mechanism for the re-evaluation surfaces that need it
(D-3(D), D-4, R-13).

### D-4 Read-scoped mask: cursorless recompute and explicit consumption boundaries

Computation (revision-3 shape, unchanged; "attempt" = one execution attempt,
including its retry generations): per attempt over the full SI window
`[txn.SnapshotTS, F]`: for every current workspace DELETE of marked tables
(in-memory entries and S3-dumped tombstone objects' rowid+PK), if the rowid's
object died inside the window (`GetChangedObjsBetweenForTombstoneTransfer`,
blocks_iter.go:243-252), map by PK to the alive-generation rowid (the existing
`doTransferRowids` PK search restricted to objects alive at F). Covers
late-added deletes and O→P→P2 chains by construction; nothing persists after
the execution; storage bounded by mapped entries per attempt; PK-mapping safety
follows from the deleter's pessimistic locks.

Consumption (revision-4 core change, per R4-F3): the computed mask is a
**mask-only tombstone input**, distinct from legacy extraTombstones:

- Shipped/remote form: mask rowids/files are appended to the marked relation's
  collected RelData tombstone set and travel to remote block readers through
  the existing ShardingRemote consumer (unchanged gate,
  local_disttae_datasource.go:1086-1094/:1147-1158).
- Ordinary local form: `NewLocalDataSource`/`LocalDisttaeDataSource` gains a
  dedicated `maskTombstones` input (populated only for marked relations). Both
  consumer boundaries apply it: `ApplyTombstones` (the rowsOffset filter — a
  mask stage placed with the uncommitted-delete stages, gated by the same
  `Policy_SkipUncommitedInMemory`/`Policy_SkipUncommitedS3` families the
  workspace delete stages use, since mask entries re-express this
  transaction's own uncommitted deletes for the F view) and `GetTombstones`
  (the bitmap path used by persisted-block reads). Application is rowid/bitmap
  matching, idempotent.

Ownership rules (R-D4): mask produced only for marked relations by D-4 at the
admitted frontier of the current execution; consumed only when the source's
readTS equals that frontier
(the source is per-attempt); never visible to unmarked readers (nil input);
never persisted; never used by the commit path (workspace entries are never
rewritten, `lastTransferred`/`pendingTransfer` untouched under SI, RC's
in-place transfer untouched).

Mandated regressions (R4-F3 demand): through the ORDINARY local path —
in-memory workspace deletes across compaction, S3-dumped workspace deletes
across compaction — asserting frontier reads suppress the deleted rows,
snapshot reads keep them hidden via original rowids, and commit behavior is
unchanged. This is in addition to the earlier dual-view, commit-invariance,
late-delete and double-compaction cases.

Rev-14 (R14-F1): this ordinary-local form is STAGE-1 scope — every admitted
marked execution over the ordinary local surface computes and consumes the
mask for its own deletes (zero work for delete-free executions per D-6), so a
delete-carrying marked read never ships without it; the stage-2 reserved
material becomes the isolated execution view and re-evaluation family, which
this form does not require. The regression list above moves into section 8
(item (vi)) as mandatory stage-1 evidence.

### D-5 Retry signaling, transport, aggregation (unchanged substance)

doLock marked-SI branches compare against the current attempt frontier (the
executing generation's frontier, D-2/D-8) and return the
refresh outcome without `UpdateSnapshot` (normal ~1152 shape and extra-check
~1214 shape gated by `hasNewVersionInRange(frontier, newTS)` where
`newTS = WaitLogTailAppliedAt(lockedTS)` — a concrete floor, so the waiter
genuinely waits; wait-resolved conflict ~1251 shape returns the refresh outcome
for marked non-RC, RC parity). `performLock` aggregates
`ctr.lockingReadRequiredTS = max(refreshTS)` and surfaces once as the new
`moerr.ErrTxnNeedRetryLockingRead` (next free code; base high-water 20907)
carrying the required TS in the message token — the wire-transported moerr
field — with the single-consumer accessor. `Compile.isRetryErr` accepts it only
for marked locking-read SELECTs under SI. Coordinator admission per D-2.
Token validity is REQUIRED (rev-7, replacing the removed fallback): the
retry token is validated on receipt before any admission decision
(structural parse of the wire token, typed-TS sanity against the waiter's
applied watermark); a malformed or missing token fails the statement with an
explicit terminal moerr ("invalid locking-read retry frontier") — the
statement never substitutes a weaker frontier (the coordinator-local
applied value can lag the remote commit that caused the retry, and
`timestampWaiter.GetTimestamp` can return the same `latestTS.Next()` on
repeat calls without new application, timestamp_waiter.go:83-85, so neither
local value is a valid floor source). Spin-freedom is unconditional: retry
admission requires the computed floor to be STRICTLY greater than the
previous frontier — `F_{k+1} = max(requiredTS, F_k)` is admitted only when
it exceeds `F_k`; a validated token that yields no strict progress
terminates the statement with an explicit error instead of re-running
(independent of waiter progress), and `SnapshotTS` is never mutated on any
exit path.

Rev-13 reaffirmation (stage-1 scope): the delivered slice deferred the marked-SI
branches; this revision restates D-5 as stage-1-mandatory (R13-F4). Nothing in
the mechanism changes: marked-SI revalidation at lock acquisition against the
executing generation's frontier, refresh through the tokenized retry path, the
wait-resolved conflict returning the refresh outcome for marked non-RC, and
unmarked SI byte-identical conservation (including today's 20619 corner). The
item 8 (iv) members — deterministic commit-between-acquisition and
commit-while-waiting SQL cases on the issue's own shape plus the item-3 lockop
matrix — gate stage-1 closure.

Rev-15 marked-retry persistence (R15-F2, PR-review P1): persistence of the
marked retry must not depend on how LATER targets of the same lock batch
terminate. The stage-1 delivery records a marked floor only in the
call-local accumulator (`opts.markingFloor` wiring, lock_op.go:524) and
returns an EMPTY refreshTS from the marked-SI branch (:1377-1395);
`performLock` converts it to `ctr.lockingReadFloor` + the tokenized retry
only AFTER the target loop (:598-606), while a later target returning a
direct `lockservice.ErrLockConflict` exits at :548-549 first, and
`callSkipLocked` converts a returned conflict with `ctr.retryError == nil`
into an ordinary skip (:242-252) — so a join whose first target discovered a
newer version and whose second target conflicts on a stale candidate can
silently drop the retry (the R15-F2 counterexample in the decision table).
INVARIANT: once any target of a `performLock` call has recorded a marked
floor, the call must surface the locking-read retry on EVERY terminal path;
a later skippable conflict may shrink the accepted row set but never erases
an already-recorded retry obligation. The conversion runs at the point of
recording — before the next target is inspected — with `ctr.lockingReadFloor`
max-aggregated across targets and the tokenized retry (re)generated from the
CURRENT max; definition-retry precedence is retained (`retryWithDefChangedError`
dominates whenever set, before or after a marked floor), and fatal `doLock`
errors still return immediately. `callSkipLocked`'s existing conflict branch
then surfaces the retry with `result.Batch = nil` — the already-published
prefix/row-count rules keep a downstream LIMIT from consuming partial
output. This changes WHEN the single D-5 retry is materialized into operator
state, not its transport, aggregation-precedence or admission rules; the
mandated item 8 (vii) members are restated below.

### D-7 Deployment: single-version cluster (rev 15, R15-F1; supersedes the cluster capability gate of revisions 3-14)

MatrixOne supports shutdown upgrade only (the repository contract,
`upgrade-compatibility.md`): peers inside one cluster always run the same
binary, a new or changed proto field, plan node, operator or function needs
no version gate, and adding features is never by itself a reason to bump
`MORPCVersion`. The rev-3–14 gate design (the allocated
`defines.MORPCVersionLockingCurrentRead`/`MORPCVersion135`, the bind-time
gate, the below-gate legacy plans, the per-Run validate-family rejection,
the prepared-statement protocolVersion rebuild consumption and the
session-cache publish/evict guards) contradicted that contract and is
DELETED from design and implementation: locking-read current-read is
UNCONDITIONAL for every statement shape D-10/G4 admits. There is no
eligibility-by-capability step; the D-1 roles and the plan-level presence
summary are emitted at bind time for every plan, absent fields decode to
no-role/no-frontier as a plain-proto property, and the additive
plan/pipeline/sharding wire fields ride the existing RPC wire with no
negotiation. The rev-7 allocation rule (R7-F3), the below-gate/legacy
clauses, the mixed-version matrix and the gate UT mandates are superseded.

What stays: every shutdown-compatibility line the contract itself names —
persisted state, upgrade steps, backup/restore, and external boundaries
(the MySQL client protocol; cross-cluster publication/CDC). This feature
persists nothing new, mutates no on-disk format, adds no catalog upgrade
step and does not change the client protocol; its cached plans are
process-local. The release boundary of the semantic change is therefore
the binary version itself, exactly like every other semantic fix.

Implementation removal plan (post-approval, enumerated as the contract for
the implementation revision): delete `MORPCVersion135` and restore
`MORPCLatestVersion = MORPCVersion134` (pkg/defines/const.go); delete
`ClusterHasLockingCurrentRead` (pkg/sql/plan/locking_current_read.go) and
the bind-time gate consumption (query_builder.go); delete the whole
validator file `pkg/sql/compile/locking_current_read_capability.go` and
both call sites (compile2.go cold compile and every Run); delete the
session-cache publish-guard and gate-down eviction pair
(mysql_cmd_executor.go); drop this feature's consumption of the
prepared-statement protocolVersion machinery (the machinery stays owned by
its existing consumers); delete the gate/transition test members while
keeping every non-gate member (the cache-transition tests keep their
publication/freshness members in gate-free form). `PlanHasLockingCurrentRead`
and every feature consumer of the presence summary (compile.go,
dependent.go admission/declinature/barrier paths) stay unchanged, as do
all other features' existing gates (which belong to their own cleanup PRs
per the contract's cleanup precedent rule).

Whether each execution runs the current-read mode for a given occurrence
is still decided per execution by D-8 — solely through the effective-role
resolution (bind provenance against the executing SnapshotTS); no
capability input exists anywhere in that decision.

### D-8 Per-execution admission and reset contract for cached and prepared executions (rev 5; capability-transition layers deleted in rev 15 by R15-F1)

Rev-15 scope: with the D-7 gate deleted, the layers below contract to ONE
admission concern — a cached or prepared plan re-executes the current-read
contract on EVERY execution with fresh per-execution state; there is no
capability layer because there is no capability input:

1. **Eligibility (bind time, immutable in the cached plan; effective mode per
   execution — rev-9, resolving R9-F3).** D-1's PROVENANCE marks plus one
   plan-level summary ("this plan expresses locking-current-read presence",
   derived from the outer-block marks; presence of an explicit snapshot
   clause with its TS H is stored as plan data, never resolved to a mode) are
   captured when the plan is bound. The
   summary is derived from the plan object itself (zero cost for unmarked
   plans) and is what every later check reads; provenance facts never appear
   or disappear after bind. The EXECUTING transaction's SnapshotTS is not a
   bind input, so the EFFECTIVE role (eligible `OuterLocking` vs
   `HistoricalSnapshot`) is re-resolved from the provenance on EVERY
   execution at the per-Run admission boundary — the first step of every Run
   admission —
   with the reader's own predicate (H non-zero and Less(execSnapshotTS)),
   before any marked-consumer setup and before the D-2 barrier: an execution in which
   every candidate resolves ineligible skips barrier, F and every marked
   consumer exactly as today (the historical cohort engages on its own);
   an execution with at least one eligible candidate runs the marked path
   with only those occurrences. SnapshotTS-crossing reuse of a cached or
   prepared plan is sound in BOTH directions because nothing was frozen
   (the cache/history mismatch the review demonstrated — a plan bound at
   S0 <= H cannot carry a stale eligible mode into an S1 > H execution —
   cannot occur).
2. **Cache reuse (rev 15, R15-F1 — the capability-transition layer is
   deleted).** Prepared statements keep the existing protocolVersion
   record/rebuild machinery owned by its other-feature consumers; this
   feature adds no comparison of its own (a marked plan is not
   capability-dependent, so no rebuild class of this feature exists).
   Session plan cache: marked entries are published exactly as unmarked
   entries are today; there is no publish-side capability guard and no
   gate-down eviction. A cached or prepared marked plan stays marked for
   its cache lifetime and re-executes the current-read contract per
   execution under D-8's freshness rules.
3. **Per-Run admission (crossed on EVERY execution path — first step:
   effective-role resolution only).** The admission re-resolves each
   provenance candidate's EFFECTIVE role against the EXECUTING transaction's
   SnapshotTS (D-1, R9-F3), so the mode is consistent before any consumer
   setup. Rev-15: the validate-family capability member
   `validateLockingCurrentReadCapability` and its NotSupported
   "requires rebinding" rejection at both call sites (cold compile
   compile2.go:169-181; every Run including reused prepared pipelines
   compile2.go:451-459) are DELETED (R15-F1); no capability validator
   exists for this feature, and no plan of this feature can reach Run in a
   mode requiring one.

Freshness and retirement (per execution — the R5-F2 core):

- A fresh F₀ is acquired at every execution of an eligible, admitted plan, at
  the marked-read source-refresh boundary (D-2: cold at compile source
  assembly; reused at the Reset-driven per-execution reader rebuild). It is
  never read from the plan or from previous executions.
- Retirement happens at execution end and in `Reset` (reused path): the
  previous execution's frontier value, per-scope stamped
  `DataSource.Timestamp` marks (reopened readers are already dropped —
  scope.go:304-307, :367-371), mask inputs and retry-admission floors are
  cleared alongside the existing per-execution resets (`c.retryTimes = 0`,
  compile.go:335; `TxnOffset`/`UpdateScopeTxnOffset` :352-361). The mask is
  computed lazily for THIS execution's F₀ only (D-4 ownership rules).
- Consequence (why sticky state is forbidden): if B commits a row matching
  the predicate between A's EXECUTE k and EXECUTE k+1, a stale frontier from
  k would exclude the row from both discovery and lock acquisition, and
  discovery producing no candidates never triggers the retry machinery — the
  wrong read is invisible to the lock layer. Per-execution F₀ is therefore
  the only sound contract; the delayed-application regression for cold runs
  (R-4) is joined by a between-executions commit regression for the reused
  path (R-8 tests).
- `Compile.Reset`'s contract itself gains the eligibility check: the Reset
  path is valid for every plan of this feature (rev-15: no capability
  precondition exists; effective-role resolution runs per execution as the
  first Run step). This preserves the call order of the existing machinery:
  admission
  runs before runC proceeds (compile2.go:451-459 position).

Rev-13 closure (pool generations — R13-F1): the retirement rule above executes
at EVERY Compile-object recycle boundary, not only in `Reset` — the reuse-pool
release path (`Compile.clear`, which today resets `executionGeneration` but none
of the execution-read fields) and fresh construction retire the same state
idempotently, because the pool hands recycled Compile objects to SUCCESSIVE
STATEMENTS OF THE SAME still-open transaction (demonstrated this round: the
same-transaction second locking read re-served the previous frontier and missed
the committed row). Backstop: a MONOTONIC per-Compile execution-read EPOCH is
stamped at every retirement, and the cached carrier additionally requires the
epoch captured at this statement's first admission — any missed retirement path
degrades to a fresh acquisition, never to reuse. The delivered owner-operator
guard remains but is no longer load-bearing alone. Item 8 (i)'s deterministic
pool-reuse ladder (same-operator successive statements, cross-operator,
previously disabled generations, reuse-package recycling) is the closure
evidence; SQL-level reproducibility is explicitly NOT required for closure
because which recycled object the allocator returns is not deterministic from
SQL.

Rev-14 (R14-F3): SUPERSEDED in rev 15 by R15-F1. The rebuild-or-reject
contract, its frontend-to-Run transition window and its item 8 (vii)
transition members are deleted with the gate: inside a cluster no capability
transition exists, so there is no transition class and no new-error
exception at all; item 8 (vii) is restated as the marked-retry persistence
members of R15-F2.

### D-9 Sharded-read visibility transport (rev 6/7, resolving R6-F2 + R7-F4)

Scope: reads of a marked relation forwarded through the table-sharding RPC
boundary (active when the table is sharded, the shard service is enabled and
not all replicas are local — `isLocalFunc`, txn_table_delegate.go:1200-1209);
N7 keeps shard placement/replication/lifecycle out of scope.

1. **Discovery transport.** `txnTableDelegate.Ranges` (:293-327) already
   splits local discovery (`rangesOnePart` + local uncommitted objects) from
   the forwarded part; the marked relation's readTS parameterization (D-3 A)
   drives the local part at F, and `forwardRead` passes
   `shardservice.ReadOptions.ReadAt(F)` instead of the transaction SnapshotTS
   (:1377-1386) so remote-shard discovery covers every commit through F.
2. **Receiver pinning — STREAM lifetime (rev-7, resolving R7-F4; rev-8 view
   mechanics).**
   `getReadRequest` (:1218-1245) serializes ProcessInfo
   and the table identity into `shard.ReadParam`; the additive execution
   field carries the admitted F. The receiver (`getTxnTable`,
   txn_table_sharding_handle.go:642-697) reconstructs its transaction exactly
   as today, then takes the read view at F through the execution cohort of
   D-3(D) — built as the SAME ISOLATED pair over the receiver's
   reconstruction (new private operator + new private workspace sharing the
   reconstructed workspace's write data read-only; the receiver side has no
   parent-isolation hazard since its reconstruction is request-owned, but the
   same private-empty-`tableCache`/no-`BindTxnOp` rules apply so its
   relations resolve F at `local_disttae_datasource.go:83`) — no SnapshotTS
   mutation; the coordinator's own-delete overlay for the remote portion is
   the transported mask of item 3, since the receiver's freshly constructed
   workspace carries none of the coordinator's writes by construction), so
   reader construction (
   `readutil.NewReader(..., tbl.db.op.SnapshotTS(), ...)`, :319-325) resolves
   to F. The workspace and the reconstructed
   transaction's SnapshotTS are never mutated; unmarked requests carry no
   field and decode byte-identically. **Ownership:** F and the mask are
   attributes of the CONSTRUCTED READER, not the request: `HandleShardingRead
   BuildReader` transfers the reader and its release callback into
   `streamHandler.streamReaders` under the EXISTING lease mechanics
   (`deadline: now+StreamReaderLease`, :336-347), every subsequent
   `HandleShardingReadNext` consumes the same pinned reader (:404-412), and
   release happens ONLY on the stream's existing terminal paths — explicit
   ReadClose (`sr.release()`, :58-59) or the lease-expiry sweep (:95-97).
   A lost BuildReader response (coordinator gave up after the receiver built)
   leaves the stream to that same lease sweep, exactly as today's readers do;
   coordinator cancellation drops its side and the receiver stream terminates
   by lease. No request-scoped teardown destroys a live stream, and no stream
   state outlives its terminal cleanup.
3. **Mask transport.** The coordinator's D-4 mask for the marked relation
   travels in the sharding request as an additive mask field of `ReadParam`
   and becomes an immutable attribute of the constructed reader (same
   ownership as F, item 2): consumed at the receiver through the EXISTING
   `ShardingRemoteDataSource` extraTombstones channel — the receiver already
   builds its local source in exactly that category
   (buildLocalDataSource(..., engine.ShardingRemoteDataSource, :308-315), the
   consumer boundary D-4(b) verified at local_disttae_datasource.go-1086-1094
   /-1147-1158) — applied by every ReadNext of the stream and released only
   with it. The coordinator's local part keeps the ordinary local mask
   input (D-4). Never persisted at the receiver; nil on unmarked requests.
4. **Field safety and cancellation (rev-15 retitle; no capability negotiation —
   R15-F1).** The additive `ReadParam` fields and the
   `ReadAt(F)` semantics need no version gate: both ends of every in-cluster
   exchange run one binary, the fields carry plain-proto-safe zero values,
   and absent fields decode to an unmarked reader — a plain-proto property
   pinned by the existing decode members. Forwarded waits are
   bounded by the execution's statement context at the coordinator; the
   receiver side is bounded by the existing stream lease; no reader build
   starts before the barrier completes (D-2), so no
   new deadlock window exists at this boundary. Mandated lifecycle UTs cross
   actual RPC boundaries: build → next(s) → close; build → abandoned → lease
   sweep; next-after-cancel — asserting pin immutability, per-ReadNext mask
   usage and zero leaked state after terminal cleanup.

### D-10 Staged delivery contract (rev 13, amended in rev 14 per R14-F1/F2/F3 and in rev 15 per R15-F1/R15-F2; the partial-rollout gate rev-12 did not contain)

The implementation round delivered a working subset of this document's mechanism
set and the review correctly blocked it: a partial rollout without a contract.
D-10 is that contract. Nothing about D-10 weakens an approved mechanism; it fixes
what is IN a deliverable stage, what happens to everything else, who owns the
remainder, and what evidence closes a stage.

1. **Non-mixing policy.** A locking statement under REPEATABLE READ executes the
   FULL approved current-read contract for every locked occurrence, or the WHOLE
   statement conserves today's snapshot semantics (locks exactly as today on the
   same rows, reads exactly as today). Rev-15 (R15-F1): the formerly recorded
   exception to "no new error classes" is DELETED with the gate — this design
   introduces no new terminal error class, and the reserved NotSupported
   "requires rebinding" rejection path is removed with the validator that
   produced it. No statement mixes current-read and snapshot occurrences of the locked
   targets; no statement survives a partially failing bind.

2. **Stage-1 admission set (the primary relation-read surface, complete):**
   `SELECT ... FOR UPDATE / FOR SHARE / LOCK IN SHARE MODE` over ordinary,
   exclusive-handle-bindable, non-partitioned, non-sharded,
   not-transaction-created user relations executing node-local scopes —
   including transparent view-expanded occurrences (the R5-F1 carve-out,
   R13-F5), covered and forced secondary-index surfaces with propagated
   provenance and `ParentTableId` admission (R13-F3), predicate discovery
   through carrier-bound relations (R13-F2), own-delete compaction safety
   through D-4's ordinary-local transfer mask consumed by every admitted
   marked execution over this surface (R14-F1: delete-free executions compute
   zero mask work per D-6; delete-carrying executions ALWAYS compute it), and
   marked-SI revalidation with the retry contract (D-5, R13-F4) — admission is
   UNCONDITIONAL for this set (rev-15, R15-F1: no capability constant, no
   bind-time gate, no per-Run validator, no per-feature protocolVersion
   rebuild consumption and no session-cache publish/evict guard exists;
   D-8's per-execution admission/reset stays).

3. **Refusal/conservation closure (G4 completion).** Any locked occurrence the
   admission predicate cannot fully serve — refused relation kinds (mock,
   combined-partition, sharded, index-parent, unbound, transaction-created), a
   marked scope that would execute remote until D-9 lands, search-generated
   scans, DDL-spanning shapes, or a mark that VANISHED through rewriting —
   makes the WHOLE statement decline to conserving base semantics BEFORE any
   marked source is published. The declinature check is the explicit-role walk
   of R14-F2: in a locking-presence plan, an UNSET (zero-value) scan role can
   only be a lost mark — construction never emits the zero value for a
   completed plan — so any unset-role scan declines the statement,
   while `NestedBlockExplicit` and covered-strip occurrences are exempt BY
   REPRESENTATION; no post-optimization structural inference over join shapes
   or table identity is needed, so same-table scalar-subquery/LEFT-lowered
   plans stay decidable. Rev-15 amendment (R15-F1): the closed-gate
   conservation clause and the run-time transition sentence are DELETED —
   byte-identical conservation is asserted for unmarked statements everywhere
   and for G4-declined statements, a bound plan never changes admission
   mid-life (no capability input exists), so there is no transition class
   and no residual past-the-guards case.

4. **Ownership (stage boundary).** Stage-1 owns D-1 complete (with rev-14's
   explicit-role representation), D-2, D-3's carrier-consistent surfaces,
   D-4's ordinary-local transfer mask with its full regression set (R14-F1),
   D-5 (with rev-15's marked-retry persistence invariant, R15-F2), the rev-15
   single-version deployment contract (D-7, R15-F1), D-8's per-execution
   admission/reset with pool-generation retirement (R13-F1), G4 statement closure via the
   explicit-role walk, and item 8's ladder (i)-(ix). Stage-2 owns the
   ALREADY-APPROVED remaining mechanisms, unchanged and not redesigned here:
   the isolated execution view and re-evaluation family with R12-F1's
   flushed-tombstone capture (the R-13 family, which D-4's stage-1 form does
   not need), D-9's sharding transport, the marked-async completion and
   re-evaluation families, the generation-keyed cache families
   (R-14/R-15/R-16), the search/vector consumer suites, and the multi-CN
   wire regression items. Every stage-2 shape conserves base semantics while
   its stage is undelivered — the remaining-scope table in the PR description
   lists each shape with its owning stage, and no stage-2 item is dropped from
   this document.

5. **Validation and closure.** A stage is deliverable only when item 8's ladder
   is green for the admitted set AND the conservation members prove the
   declinature set byte-identical to base. The probe matrix archived this
   round (evidence/probe5-prepr.log) fixes the CURRENT RED for the ladder:
   the same-transaction stale-frontier locked read, the missing
   peer-persisted objects, the stale locking view and the stale index surface
   are demonstrated wrong-result paths of the delivered slice that stage-1
   must turn green — or decline the whole statement where admission does not
   extend. Rev-14 additions to the ladder: D-4's stage-1 regression set
   (in-memory and S3-dumped own deletes across compaction, late-added deletes,
   repeated compaction — R14-F1; deterministic CreateEngines-harness members,
   no SQL-level compaction forcing claimed), the vanished-mark ablation member
   (R14-F2) and the marked-retry persistence members (R15-F2; rev-15: the
   capability-drop transition members are deleted with the gate, R15-F1). No
   repetition-based or narrative-only closure counts.

### D-6 Bounds summary

One barrier wait per marked statement execution (addition to the cost
statement); zero
shared-state mutation; zero mask work for delete-free transactions; mask
bounded per execution; retries monotone and bounded by the statement deadline;
per-execution admission is O(1) on unmarked plans and (rev-15: no capability
read exists — R15-F1) one barrier acquisition + one ISOLATED execution-view
construction per marked execution (bounded relation reconstruction in the
view's fresh private `tableCache` — the parent's warm cache is reused by
unmarked reads untouched; entry clones and pin-ledger entries only for
point-probed entries, per R9-F1); marked ASYNC searches add per execution
either the incremental gap window `(searched, F]` (RANK-INSENSITIVE shapes) or
one F-source re-evaluation (RANKING-SENSITIVE shapes while the resident
generation lags at F OR the transaction carries source-local writes — the
covered shortcut requires committed coverage AND an empty own-write window,
R10-F1 — the two-pass text form and one-pass vector form,
R9-F2 — and the rare newer-generation/DDL-span case), whose retained state is
BOUNDED by the re-evaluation resource contract (O(query-terms) statistics
counters, O(K) heap with a pushed LIMIT, O(workspace-window) suppression — no
evaluator-side whole-result materialization; execution-scoped retentions
released at retirement; the same contract governs the governor-refusal
degraded path), plus the rev-12 read-only FLUSHED-TOMBSTONE capture (one
parent-mutex value-copy of the parent registry's ObjectStats entries per
marked execution, bounded by the transaction's own flushed-file count,
non-owning, freed at the per-execution retirement), plus a fresh
generation load ONLY when F exceeds every resident coverage bound (R9-F4:
resident multiplicity bounded by (index, live-generation); no per-frontier
cache copies; a governor refusal degrades to re-evaluation instead of
failing); unmarked searches and occurrences resolving ineligible in the
execution add zero work.

## 6. State, lifecycle, compatibility

Transaction `SnapshotTS` never written by this feature (unchanged); new state
is the process-local per-execution frontier, the wire role marker and frontier,
the marked-relation wrappers, the per-execution mask, and (rev-8) the
ISOLATED execution views — private operator/workspace pairs created after the
barrier at each marked execution's source assembly and destroyed at that
execution's retirement, leaving the parent transaction (operator identity,
cached relations, write data) and every following execution untouched — all
execution-scoped,
acquired at execution admission (D-2/D-8) and retired at Reset/execution end;
locks unchanged; workspace untouched for commit purposes; additive proto
fields default to no-role/no-frontier (rev-9; rev-15: the wire carries only
presence provenance and per-execution state, absence of the execution fields
decodes identity to "no role" as a plain-proto property — both ends of every
in-cluster exchange run one binary under the shutdown-only upgrade contract,
so no below-gate decode contract exists; the per-execution effective role
needs NO wire field at all — it resolves locally on both ends from the same
plan data plus each side's executing SnapshotTS). Rev-15 plan-cache contract
(R15-F1): prepared statement plans keep the existing protocolVersion
record/rebuild machinery owned by its other-feature consumers; a marked
plan is a permanent property of its generation — there is no
capability publish-guard, no gate-side eviction and no per-Run capability
validator (all deleted). A cached or prepared marked plan re-executes the
current-read contract on every EXECUTE with a fresh frontier (D-8).
Unmarked plans remain byte-identical in behavior to today.

Rev-13 additions (R13-F1, D-10): per-execution state is retired at the
pool-recycle boundary (`clear`) as well as in `Reset`, each retirement stamping
a monotonic execution-read epoch that gates the cached carrier (any missed
boundary degrades to re-acquisition, never reuse); statement-wide decline under
D-10 replaces silent partial binding for shapes outside the live stage's
admission set (rev-15: the uniform-legacy capability clause is deleted with
the gate, R15-F1). All other
state-lifecycle properties are unchanged.

## 7. Residual risks and implementation-time verification

- R-1 surface completeness (D-3 lists): classification checklist test over
  disttae SnapshotTS consumers; discovery-shape UTs (fast filter, slow path,
  shipped blocks, remote-Rel form, ordinary local source).
- R-2 mask isolation and consumption: mask-only input applied per the R-D4
  ownership rules in both local consumer boundaries; dual-view,
  commit-invariance, statement-failure cleanliness UTs; ordinary-local-path
  compaction regressions (review-mandated).
- R-3a mask semantics: late-delete and double-compaction UTs (retained);
  R-3b S3-dumped entry PK-column format verified at implementation with a
  dedicated UT.
- R-4 frontier freshness: barrier acquisition with a deterministic
  delayed-application regression: an initially-empty discovery must wait at the
  barrier, then return the committed match, while subsequent plain RR reads
  stay unchanged; admission-wait monotonicity and cancellation without spin
  pinned by waiter-stub UTs.
- R-5 occurrence identity (rev-5 restated): construction-time role capture at
  `buildTable` with the verified nesting discipline; fabrication-site audit
  with the shared propagation helper; occurrence-corpus UT including the
  WHERE/JOIN-condition/HAVING scalar-subquery controls (LEFT-lowered, same-table),
  EXISTS/IN, view-inheritance and CTE controls, plus the lock-target-set
  conservation assertion (target lists byte-identical to base for every corpus
  plan).
- R-6 deployment (rev-15 restated, R15-F1 — supersedes the rev-7
  capability-gate row): MatrixOne is shutdown-upgrade-only, so no per-feature
  gate exists; the gate constant, bind-time gate, per-Run validator and their
  mandated UTs (legacy-decode, gate-pinning, validator rejection) are deleted
  with the mechanism. Residual risk retained: the additive wire/pipeline
  fields keep plain-proto-safe zero values ("no role/no frontier"), exercised
  by the existing decode members; the deletion is pinned by the change closure
  staying green and the real 2-CN regression of R-11/multi-CN members
  (retained, gate-free).
- R-7 scope: added barrier plumbing and the local-mask input atop revision 3;
  owner paths enumerated per component; same design-gated classification.
- R-8 per-execution freshness and reuse (R5-F2; rev-15: the
  capability-transition members are deleted with the gate, R15-F1): repeated
  prepared RR
  EXECUTEs with peer commits between executions (row must be discovered and
  returned at each execution — stale-frontier counterexample); stubbed-barrier
  acquisition count == one per execution; per-execution retirement of frontier/
  mask/floors in Reset.
- R-9 provenance class coverage (R5-F1 residual): the construction-capture
  predicate must classify every bind site the corpus exercises; sites
  discovered during implementation that bypass `buildTable` (e.g. table
  functions, external scans) are enumerated in the same fabrication audit
  and assigned NestedBlock by default (safe: unmarked).
- R-10 search-boundary completeness (R6-F1, rev-8 restated): the plugin
  executes outside the relation path, so F must reach EVERY hop through the
  SEPARATE execution cohort (identity, `EffectiveExecutionTS()` pin,
  generation-keyed execution cache family, tail read, covered output, ranked-correct async completion) —
  pinned by the probe4-shaped corpus, the pin-helper UT with F > SnapshotTS,
  the stubbed-searcher UT and the real-disttae consumer UT of R-13; own-write
  visibility flows through the ISOLATED view's shared write window (SYNC
  family: index-table rows; ASYNC family: SOURCE-row overlay per R-14) with
  shared cache entries staying COMMITTED-ONLY (own-write locking-MATCH matrix
  for BOTH families plus the cache-pollution UT); named-snapshot semantics
  stay byte-identical (admission refusal included), and the
  `HistoricalSnapshot` partition keeps ineligible occurrences off every
  current-read surface (R8-F3 corpus + public SQL controls); scope
  conservation is asserted (direct
  `MATCH … FOR UPDATE` refusals stay 20105 before AND after; covered-path
  target sets stay as today), and the vector-SORT-anchor reachability check
  prevents a silent new live path appearing without the both-consumer
  assertion.
- R-11 sharding transport (R6-F2/R7-F4, rev-7 restated): the receiver is a
  reconstructed transaction the coordinator does not own — F and the mask
  attach to the constructed reader STREAM (the shardingRemoteReader record),
  immutable for the stream, owned under the EXISTING lease/deadline
  mechanics and released only by ReadClose or the lease sweep (a lost
  BuildReader response terminates by the same lease as today); the
  reconstructed txn's SnapshotTS is never mutated; the receiver's pin is the
  same ISOLATED-pair construction over its reconstruction (R8-F1 rules); the
  mask is consumed only
  through the existing ShardingRemote channel; absent additive fields decode
  to an unmarked reader by plain-proto default (rev-15: both ends run one
  binary, R15-F1); coordinator cancellation drops its side and the stream ends by lease; delegate/handle harness UTs
  crossing actual RPC boundaries (build→next→close; build→abandoned→sweep;
  next-after-cancel) plus the multi-CN witness pin the boundary.
- R-12 retry-token integrity (R7-F5): tokens are validated before any
  admission; invalid/missing tokens terminate the statement with an explicit
  error (never a weaker frontier, never spin — strict-progress admission
  independent of waiter behavior, timestamp_waiter.go:83-85); fault UTs with a
  lagging coordinator assert terminal behavior, no mutation and no spin.
- R-13 execution-view isolation (R8-F1; borrow discipline added by R9-F1;
  metadata handles added by R11-F1; flushed-tombstone capture added by
  R12-F1):
  sharing the parent workspace would
  return the parent's snapshot-bound interned relations
  (txn_database.go:47-48/:133-136, txn.go:2968-2978, then
  local_disttae_datasource.go:83 pins from the RELATION's operator) and
  rebinding would write the parent's `Transaction.op` (txn.go:3441-3442) — so
  the view is an ISOLATED operator/workspace pair sharing write data
  read-only, with a private empty `tableCache` and zero `BindTxnOp` use; the
  read-only borrow survives ACTUAL readers by entry-clone-on-first-mutation
  (R9-F1: the exact-PK probe hydrates borrowed entries and charges the
  receiving transaction — local_disttae_datasource.go:757 + txn.go:242-323 —
  so clones carry the mutation, the view pays the accounting, and clones die
  at retirement unreachable from the parent's slice) with a view-private pin
  ledger for every pin taken on shared batches, and borrowed-entry access is
  SYNCHRONIZED with parent-side lazy hydration under the PARENT transaction
  mutex (copy-then-release; parent mutex strictly outermost; the view never
  holds its own mutex while acquiring the parent's; pin ledger recorded
  under it, retired via atomic refcounts) (R10-F3);
  TRANSACTION-CREATED databases and tables never resolve through the
  chains' payloads — the shared chains are consulted as PREDICATES only
  (existence/visibility answers) and the view constructs its own
  view-operator-bound `txnDatabase`/relation interned in the VIEW's
  tableCache, so no parent `*txnDatabase`/`*txnTable` payload is returned
  to the view and the base created-table branch's `v.proc.Store(p)`
  (txn_database.go:125-129) is never executed against a parent handle
  (R11-F1); the FLUSHED workspace tombstones are CARRIED by a
  parent-mutex-guarded VALUE-COPY of the parent's flushed-tombstone
  registry into the view's fresh registry (ObjectStats copied by value
  under the R10-F3 outermost-lock rule, released before exposure;
  NON-OWNING: no parent Delete, no file operation — the tombstone files
  keep the parent's workspace-entry ownership and the parent's registry
  keeps its own retirement paths, commit-GC removal types.go:1147 and
  `delTransaction` teardown txn.go:3295/:3321 — while the view's copy
  dies at the per-execution retirement), so ApplyTombstones/GetTombstones/
  CollectTombstones suppress flushed-deleted rows through the VIEW's
  carried copy exactly as the parent path does (R12-F1); the
  REAL disttae consumer UT (warmed parent cache; F > SnapshotTS; own writes
  through the view; cold exact-PK probe of an unhydrated entry; parent
  entries, accounting and counters unchanged through view retirement AND
  parent commit/rollback; parent untouched before/after; subsequent ordinary
  RR read byte-identical — extended rev-10 with a barrier-controlled
  CONCURRENT parent-hydration member run under the race detector asserting
  no race, correct results and accounting conservation — extended rev-11
  with the transaction-created DATABASE and transaction-created TABLE
  members asserting the read point through the view-local handle, the
  parent's handle isolation (no parent `*txnDatabase`/`*txnTable` payload
  returned to or proc-mutated by the view, pointer-identity asserted),
  hydration accounting and retirement — extended rev-12 with the FLUSHED
  own-DELETE member: an own DELETE flushed to S3 without compaction
  (target object still alive) is suppressed through ApplyTombstones,
  GetTombstones and CollectTombstones via the carried copy, the
  re-evaluation corpus/statistics exclude the flushed-deleted row (no
  resurrection, no stale counts), and the parent's registry, tombstone
  files and subsequent COMMIT/ROLLBACK behavior are byte-identical
  through the view's lifetime, retirement and the parent's own teardown)
  is the boundary's
  proof — helper-only pin
  assertions cannot prove it; teardown at the per-execution retirement sweep
  leaves no view state in the parent.
- R-14 marked-async completion (R8-F2; ranked semantics added by R9-F2): for
  `IsAsync` indexes the bulk
  result alone is incomplete at F (ISCP lag, coverage.go:16-25; no own in-txn
  postings, build_dml_util.go:7237-7244), so marked executions MANDATORILY
  complete through per-execution coverage at F (covered / behind / newer or
  DDL-span, mirroring search.go:476-519) plus the own-SOURCE overlay; the
  BEHIND case splits by RANKING SENSITIVITY because fulltext2 scores are
  corpus-GLOBAL (fulltext2/index.go:355-376) and pushed score ranges/top-K
  discard rows DURING the search (search.go:733-747/:810-825) —
  RANK-SENSITIVE shapes re-evaluate the whole ranked search over the
  F-complete source view with F-consistent statistics INCLUDING the
  transaction's own source rows (the covered shortcut needs committed
  coverage AND an empty own-write window; the SourceCommitTS fail-closed
  outcome routes to the same paths — never an error, never silently weaker —
  R10-F1); the COMPLETE pushed search request (pattern, IncludePredsJSON,
  FilterBytes membership) is re-applied
  uniformly on every candidate source BEFORE the final one-shot range,
  order, truncation, dedup step (R10-F2) — applied once after the merged
  set; no candidate limiting
  before the complete set — the ENTIRE re-evaluation runs under the rev-11
  ITEM-7 RESOURCE CONTRACT (pass-1 streaming scalar statistics with
  per-QUERY-term df counters, pass-2 bounded-batch emission, a K-bounded
  top-K heap under a pushed LIMIT, no-LIMIT shapes delegating rank/order
  to the EXISTING pipeline Sort/Limit operators, NO evaluator-side
  whole-result retention, execution-scoped retentions released at the
  per-execution retirement; the governor-refusal degraded run inherits
  the SAME contract) (R11-F2), RANK-INSENSITIVE full-set shapes keep the
  incremental gap window; the runtime match predicate
  must use the index's build-time parser configuration (O-D8); controlled
  CDC-lag UTs (harness-built generations with build_ts < F and known gap
  rows) with completed-REFERENCE corpus controls (score-range flip; deleted
  bulk top-1 with runner-up recovery; own UPDATE replacing a winner;
  covered-path INCLUDE; mode elision) and the two-family own-write matrix are
  the discriminating proof — extended rev-11 with the deterministic
  BOUNDED-EXECUTION members (a governor-refused load degrading to the
  item-7 contract and completing with bounded retentions and a
  completed-correct result; a large-corpus no-LIMIT score-output member
  asserting batch-wise streaming with no evaluator-side whole-result
  retention; a pushed-LIMIT member asserting the K-bounded heap);
  UNMARKED searches keep the freshness-tolerant contract byte-identically
  (ordinary-MATCH controls against the same lagging generations), and
  completions/overlays never enter shared cache entries.
- R-15 historical-snapshot precedence (R8-F3; per-execution resolution by
  R9-F3): `buildTable` carries
  `ScanSnapshot` (query_builder.go:13666) and `collectLockTargets` locks such
  scans today (:15046-15069) while compile decides cohort engagement against
  the EXECUTING transaction (compile.go:10854-10866 and twin :5609-5622) —
  the bind-time artifact therefore stores PRESENCE PROVENANCE only (locked
  block + explicit clause + H) and each execution resolves the EFFECTIVE role
  at the admission boundary against its own SnapshotTS in BOTH directions
  (a plan bound at S0 <= H must not carry a stale eligible mode into an S1 >
  H execution, and vice versa); an engaging explicit snapshot
  wins over the locking frontier for READS in that execution (no F
  on any surface) while the lock target is conserved; the partition's
  consistency is pinned by the per-execution effective-mode matrix
  (cached/prepared crossing S0 <= H < S1 and reverse, view and search
  consumers, lock targets byte-identical), the presence-fact eligibility
  corpus (all four consumer classes: relation/search/cache/wire), the
  both-behavior pin-helper UT extended with an ineligible scope, and public
  SQL controls (locking `AS OF` historical + lock held; snapshot-bearing
  view; subsequent eligible locking read current).
- R-16 marked-cache capacity (R9-F4): per-frontier `ExecutionKey` keying with
  per-execution F renewal would give every distinct F a cold full load with
  its own whole mapping (cache.go:1246-1290; search_cache.go:63-70/:220-236)
  and could refuse under budget where today's shared entry serves concurrent
  readers (governor.go:438-493) — so the execution family is keyed by the
  LOADED GENERATION with a captured coverage bound, sharing one resident
  entry across every F it covers (consecutive and reused-plan executions
  share; one fresh load only when F exceeds every resident bound; retirement
  through the existing read-lock/idle-reclaim/TTL policy with pinned entries
  un-evictable, so multiplicity is bounded by (index, live-generation)); a
  governor refusal NEVER fails a marked execution — it degrades to the
  R9-F2 F-source re-evaluation while unmarked keeps today's refusal; pinned by
  load-count UTs, the constrained-budget concurrency degradation test and a
  recorded performance comparison against the shared bare-key model.

- R-17 staged-delivery residual (rev-13, D-10): until stage-2 mechanics land,
  their locked shapes (search/MATCH paths, sharded or partitioned forwards,
  transaction-created relations, remote-marked scopes, DDL-spanning corners)
  still exhibit the REPORTED defect by design — stage-1 conserves today's
  snapshot semantics there and D-10's ownership table records who unblocks
  each shape (rev-15: the below-gate uniform clause is deleted with the
  gate — R15-F1). The archived
  first-replay dual-role miss (73/74, implementation round) remains
  unexplained by existing artifacts (the failing assertion text was
  overwritten before archival; re-verified this round), so stage-1 closure
  REQUIRES the item 8 (ix) instrumented boot-fresh replay: a recurrence
  fails loud with the recorded frontier-vs-commit watermark ordering instead
  of passing by later repetition; the epoch backstop and the carrier-consistency
  audit similarly fail loud (statement decline or fresh acquisition) on
  unrecognized future paths rather than silently degrading. Rev-14: D-4's
  ordinary-local mask moved INTO stage-1 (R14-F1), so own-delete compaction
  safety is closed with stage-1 itself; D-10.4 records the updated boundary.

Note on rev-3 vs rev-4 citation: the rev-3 assumption that appended RelData
tombstones would be applied by ordinary local readers was wrong
(local_disttae_datasource.go:1086-1094/:1147-1158); rev-4 corrects it with the
explicit mask-only input. The rev-3 "post-optimization re-derivation" pass is
removed as unsound under orientation swaps and same-table occurrences.

Note on rev-4 vs rev-5 citation: the rev-4 assumption that the
`collectLockTargets` walk runs "on the bind tree where subquery subtrees are
distinct" was wrong for WHERE/HAVING/JOIN-condition scalar subqueries —
`bindWhere` (:9222) lowers them through `flattenFilterSubqueriesGuarded`
(:9369) into LEFT joins before the walk (:9236), and the walk enters both
children of a LEFT join (:15074-15075, exclusion only for SEMI/ANTI/SINGLE/
MARK at :15038). Rev-5 moves ownership capture to construction time
(`buildTable`, :13117/:13400, under the nesting discipline verified at
base_binder.go:963-1015). The rev-4 "plan cache unaffected" claim is likewise
corrected: rev-5 defines per-execution admission, retirement and the bounded
cache invalidation behaviors (D-8).

Note on rev-5 vs rev-6 citation: rev-5's surface lists stopped at the
relation/ordinary-local/shipped-reader classes; the fifth review identified
two read-source boundaries they did not cover. Rev-6 verifies both and extends
D-1 (fabrication audit + role propagation for the generated search scan family)
and D-3 (frontier stamp and plugin TS pin/cache key on the search execution
path), and adds D-9 (the sharded-read transport contract anchored on
`forwardRead`'s pluggable `ReadAt`, the receiver's existing txn-reconstruction
and the existing ShardingRemoteDataSource tombstone channel). The empirical
addition this round matters for scoping: direct `MATCH … FOR UPDATE` shapes
cannot plan at base (20105; probes 3b/3c), while the view-wrapped locking MATCH
form rewrites below the Lock and is live-demonstrated stale (probe4) — the
design's transport covers the family, and the refusals are conserved as
assertions.

Note on rev-6 vs rev-7 citation: three rev-6 mechanisms were unsound and are
corrected here. (1) Rev-6 routed F through the named-snapshot cohort — but
`EffectiveSnapshotTS` admits only TS strictly earlier than the transaction
SnapshotTS (sqlexec.go:147-152, pinned by snapshot_test.go:60-71) and
`vectorscan.Identity` likewise (execution.go:265-267), so the barrier
frontier would have been DISCARDED; rev-7 gives F its own execution carrier
and a distinct `ExecutionKey` family. (2) Rev-6's search read at F would
have run on a snapshot clone — but `CloneSnapshotWS` builds an empty
read-only workspace (txn.go:3414-3438) and classic fulltext index writes
are transactional (build_dml_util.go:7274+), so the transaction's own
matching writes would have disappeared; rev-7 shares the original workspace
read-only for the execution view and keeps shared cache entries
committed-only (**that sharing mechanism is itself corrected by R8-F1 — see
the next note: the view is an isolated pair sharing write data only**) (3) Rev-6 gated on a fixed `MORPCVersion130` — but
origin/main already consumes 130 for temporal-string semantics (commit
5a813a32d), so the gate admitted peers implementing nothing of this design;
rev-7 requires a newly allocated constant above the integration base's
high-water, revalidated at integration. Two rev-6 contract wordings are also
corrected: the sharding pin/mask ownership follows the READER STREAM
lifetime (BuildReader hands the reader to `streamReaders` under a lease;
ReadNext consumes it later — txn_table_sharding_handle.go:336-347/:404-412),
and D-5's "degraded unparseable → local applied frontier with bounded extra
retries" fallback is REMOVED (the local frontier can lag the triggering
remote commit and the waiter has no application-progress bound,
timestamp_waiter.go:83-85) — invalid tokens and non-progressing floors now
terminate with explicit errors.

Note on rev-7 vs rev-8 citation: three rev-7 mechanisms were unsound or
incomplete and are corrected here. (1) Rev-7's execution view "SHARES the
original workspace read-only" — but a shared workspace hands back its own
interned relations (getTxn asserts the workspace's `*Transaction`,
txn_database.go:47-48; SI `getCachedTableByKey` returns them unchanged,
txn.go:2968-2978), they pin their read from THEIR operator
(local_disttae_datasource.go:83), and making them read at F would require
`BindTxnOp`, which writes the shared `Transaction.op` (txn.go:3441-3442) — rev-8
makes the view an ISOLATED operator/workspace pair sharing only the parent's
WRITE DATA read-only (R8-F1), proven by a real disttae consumer UT instead of
helper-only pins. (2) Rev-7 claimed pinning index-load SQL/tail/covered output
to F supplies current visibility for marked searches — true only for
synchronously maintained indexes; for `IsAsync` indexes (fulltext2 and the
always-async vector algorithms plus `async=true`) the ISCP trails the source
(coverage.go:16-25), loaded generations contain only appended CDC tails
(search_cache.go:174-192), async DML maintenance writes no in-txn postings
(build_dml_util.go:7237-7244) and the self-completing tail runs only for JSON
probes (search.go:763) — rev-8 adds the mandatory per-execution
commit-covered completion contract (covered / gap `(searched, F]` /
current-read fallback) and re-aims the own-write overlay at the async family's
SOURCE rows (R8-F2), with unmarked freshness tolerance unchanged. (3) Rev-7
marked every `isForUpdate` construction with no snapshot condition, while
`buildTable` carries `ScanSnapshot` (query_builder.go:13666), such scans are
lock targets today (collectLockTargets :15046-15069) and compile routes them
through the historical clone (compile.go:5609-5622) — rev-8 defines the
`HistoricalSnapshot` eligibility role with uniform precedence and conserved
lock targets (R8-F3).

Note on rev-8 vs rev-9 citation: four rev-8 mechanisms were unsound or
incomplete and are corrected here. (1) Rev-8 borrowed the parent's writes
prefix as a plain read-only slice — but the actual disttae exact-PK reader
lazily mutates the entry it reads and charges the receiving transaction
(`ensureInsertKeyOffsetsLocked(&writes[wsCursor])`,
local_disttae_datasource.go:757; charge txn.go:242-275; refund txn.go:310-323;
readOnly guards only Commit, txn.go:80/:3003), which would mutate the
parent's Entry while charging the view and leave the parent refunding bytes
it never charged — rev-9 makes the borrow immutable by construction
(entry-clone-on-first-mutation, view-side accounting, pin-ledger
non-owning teardown; R9-F1, resolving O-D8's borrow half). (2) Rev-8's
behind-at-F case completed the gap incrementally — but fulltext2 scores are
corpus-GLOBAL (fulltext2/index.go:355-376) and pushed score ranges and
LIMIT top-K discard rows DURING the bulk search (search.go:733-747/:810-825),
so incremental changed-row evaluation cannot repair an unchanged document's
score-range rejection nor recover a discarded runner-up; and the JSON-probe
tail that motivated the mechanism emits score-zero candidate rows
(search.go:421-470), which never establishes ranked correctness — rev-9
splits marked-async completion by ranking sensitivity, re-evaluating
ranking-sensitive shapes over the F-complete source view with F-consistent
statistics and keeping incremental completion only for rank-insensitive
full-set shapes (R9-F2). (3) Rev-8 resolved snapshot eligibility at BIND
time against the binding transaction's SnapshotTS and froze the role — but
the existing reader compares against the EXECUTING transaction at datasource
build (compile.go:10854-10866, twin :5609-5622) and no rebuild rule reacts to
SnapshotTS (computation_wrapper.go:2834-2853/:1481-1502), so a plan bound
at S0 <= H could carry a stale eligible mode into an S1 > H execution —
rev-9 stores only presence provenance at bind and re-resolves the effective
role per execution at the admission boundary in both directions (R9-F3).
(4) Rev-8 keyed marked cache entries by the frontier
(`ExecutionKey(IndexTable, F)`) — but each distinct F is a cold full load
with its own whole mapping (cache.go:1246-1290; search_cache.go:63-70/:220-236)
and the governor refuses a second concurrent load under budget
(governor.go:438-493), which would fail locking reads that succeed today —
rev-9 re-keys the family by the LOADED GENERATION with a captured coverage
bound (one shared resident entry per generation covering every F it
covers; one fresh load only beyond the resident bound; existing read-lock/
idle-reclaim retirement; governor refusal degrades to F-source re-evaluation
instead of failing) (R9-F4).

Note on rev-9 vs rev-10 citation: three rev-9 gaps are corrected here.
(1) Rev-9's case (c) declared a COVERED generation complete with no
completion regardless of own writes — but a pushed LIMIT truncates inside
SearchInto (search.go:810-825) so an own DELETE of the bulk winner abandons
the discarded runner-up forever, own corpus members shift unchanged
documents' corpus-global scores (fulltext2/index.go:355-376), and
SourceCommitTS itself FAILS CLOSED on transaction-local writes
("source commit ts is unavailable with transaction-local writes",
source_commit_ts.go:28-40) — rev-10 requires the covered shortcut to hold
committed coverage AND an empty own-write window for the source, includes
own rows in the re-evaluation corpus and statistics, and routes the
SourceCommitTS refusal to the same completion paths (R10-F1). (2) Rev-9's
completion evaluator applied only the match/distance predicate to gap and
own rows — but the planner peels INCLUDE/pk predicates out of the
FilterList into the request (apply_indices_fulltext.go:652-667), the covered
plan runs all qualification inside the search (:1287-1309) and the request
carries FilterBytes/IncludePredsJSON beside the pattern (search.go:742-755)
— rev-10 re-applies the complete pushed search request uniformly on every
candidate source before the final ranking/truncation, eliminating the
include_value=0 false positive (R10-F2). (3) Rev-9's entry-clone borrow
blocked view-side mutation of parent state but not parent-side lazy
hydration racing the view's read (parent readers take the OWNING
transaction's mutex at local_disttae_datasource.go:647-655/:717/:757 with
entry writes at txn.go:269-274, while the view is a separate Transaction
with a different mutex and parallel fragments give real concurrency) —
rev-10 runs borrowed-entry access under the parent transaction mutex with
copy-then-release, an explicit outermost-lock rule and race-detector
validation (R10-F3).

Note on rev-10 vs rev-11 citation: two rev-10 gaps are corrected here.
(1) Rev-10 treated sharing `tableOps`/`databaseOps` as exposing only
created/dropped VISIBILITY that the view's private tableCache would
isolate — but the chains' accessors return the parent-owned payloads
themselves: `Engine.Database` hands back the parent `*txnDatabase`
(engine.go:502-518) and the created-table branch hands back the parent
`*txnTable` AFTER proc-mutating it, both before any cache lookup
(txn_database.go:125-129) — rev-11 consults the chains as PREDICATES only
(existence/visibility answers, never the stored payloads) and
constructs view-operator-bound handles interned in the VIEW's
tableCache, never returning or proc-mutating a parent payload (R11-F1).
(2) Rev-10's re-evaluation contract fixed correctness but no resource
bounds — the no-LIMIT reader streams by design (search.go:773-788) and
the async reader keeps an O(one-segment) bound (index.go:502-515) while
the replacement scanned source tables with only D-6 scan counts — rev-11
adds the D-3(D) item-7 resource contract component-wise (pass-1 streaming
scalar statistics with per-QUERY-term df counters, pass-2 bounded-batch
emission, a K-bounded heap under a pushed LIMIT, no-LIMIT shapes
delegating rank/order to the EXISTING pipeline Sort/Limit operators, NO
evaluator-side whole-result retention, execution-scoped retentions
released at the per-execution retirement, no new budget/governor
admission) and the governor-refusal degraded run inherits the SAME
contract (R11-F2).

Note on rev-11 vs rev-12 citation: one rev-8..11 gap is corrected here.
rev-8's borrowed enumeration ("the parent's append-only writes prefix …,
`deletedBlocks` and the `tableOps`/`databaseOps` chains") treated DELETE
visibility as writes + deletedBlocks, but S3-flushed DELETE entries are
EXCLUDED from the reader's in-memory gate (local_disttae_datasource.go:634)
and their suppression lives in a separate per-Transaction registry consumed
through the relation's owning transaction (:1386/:1394,
txn_table.go:855/:863/:925, txn.go:2846-2858) — rev-8..11, including
rev-11's view-local handles, never carried it, and `CloneSnapshotWS` starts
the copy empty (txn.go:3430); D-4's mask cannot substitute because its
mapping domain is rows whose ORIGINAL object died inside the frontier
window (the uncompacted still-alive case is outside it) — revision 12 adds
the parent-mutex-guarded, value-copy, non-owning capture supplying the
missing input to every view consumer including the re-evaluation
corpus/statistics (R12-F1).

Note on rev-12 vs rev-13 citation: no rev-12 mechanism was found unsound; the
thirteenth round is a DELIVERY-CONTRACT revision. The implemented slice reused
rev-12's D-2 barrier and D-1 construction marks partially (no view carve-out, no
fabrication propagation, no copy preservation — R13-F3/R13-F5 restate approved
R4-F2/R5-F1 items as closure items), so R13-F1's pool-boundary retirement plus
the epoch backstop completes D-8 rather than replacing it, and R13-F2's
carrier-consistent discovery/remote/tombstone surfaces complete D-3's
every-consumer claim (as the rev-11 note once corrected borrowed enumeration,
this note corrects discovery enumeration, the remote relation replacement and
the persisted-tombstone meta field). R13-F4 reaffirms D-5 as stage-1 (approved
earlier; deferred by the slice without any sanctioned contract — precisely
what D-10 now makes impossible). R13-F6's D-10 is the genuinely NEW material of
this revision: the twelfth-round approval covered the complete mechanism set
and did not authorize a partial rollout, so the revision-13 approval had to
cover D-10 explicitly — D-10 is inside the approved revision-15 set that
PR #31587 delivers.

Note on rev-13 vs rev-14 citation: rev-13's staging mechanism was kept
directionally and three gaps closed. (1) D-4's ordinary-local transfer mask
moves from stage-2 into stage-1 (R14-F1) — the delivered carrier form shares
the workspace, which performs no old-to-new rowid mapping (suppression is
keyed by block/rowid), so stage-1 could not ship delete-carrying marked reads
without resurrecting own-deleted rows across a compaction inside
[SnapshotTS, F]; R13-F6's decision cell superseded where it listed D-4 among
stage-2 items, the isolated-execution-view family remaining there. (2) The
vanished-mark promise becomes computable through the explicit-role
representation (R14-F2), amending D-1's artifact wording only in that the
zero value means "unset" for plans of this feature rather than "no current read"
(the gate qualifier is gone — rev-15);
the enum extension is additive. (3) Superseded in rev-15 (R15-F1): the
below-gate promise and the mid-flight rebuild-or-reject are deleted with the
gate. No
rev-13 mechanism was unsound; these rows supersede the corresponding D-10
sentences only.

## 8. Verification plan (cheapest discriminating evidence first)

Environment: isolated local cluster (39xxx plan, runtime `/tmp/mo30955`); a
second CN on an extended non-overlapping port plan for the multi-CN item;
shared mo-tester repository and shared config untouched; isolated report dirs.

1. **Red/green BVT** on
   `test/distributed/cases/pessimistic_transaction/select_for_update.sql`
   (plus a fulltext-focused file nearby if reviewers prefer separation):
   1. the issue's sequence + FOR SHARE / LOCK IN SHARE MODE forms;
   2. F1 discovery blocks (value-change, peer-committed matching INSERT),
      unchanged plain RR reads after;
   3. F2 scalar-subquery block: current `f` beside snapshot `g`;
   4. NEW (R4-F2) same-table scalar subquery control: the subquery over the
      SAME table `f` must return the snapshot value while the locked outer
      occurrence returns current;
   5. NEW (R5-F1) WHERE-scalar lowering control, the review's exact example:
      `SELECT o.id FROM f o WHERE o.v > (SELECT i.v FROM f i WHERE i.id=1
      LIMIT 1) FOR UPDATE` after f.v moved 100→999 — outer reads 999,
      the subquery reads snapshot 100, the predicate row is returned
      (999-vs-100 comparison); red on base (base reads both at 100),
      green after (outer occurrence current, subquery snapshot); a
      JOIN-condition scalar-subquery control and a HAVING scalar-subquery
      control of the same shape;
   6. NEW (R5-F1) view control: a simple view over `f` under
      `SELECT … FOR UPDATE` returns the committed value (view scans inherit
      the outer role); CTE control: the CTE body occurrence keeps the
      snapshot and stays lock-free (matching base's CTE exclusion);
   7. NEW (R6-F1) **locking MATCH regression, captured red on base this round
      (probe4)**: a fulltext index on `f.doc`, a view `v_ft` whose body holds
      the MATCH predicate, and the deterministic sequence — A RR/BEGIN, B
      autocommits a new matching row (witnessed searchable from a committed
      reader), A's `SELECT id, v FROM v_ft FOR UPDATE` must then return the
      new match (current read through the search stream); base returns the
      stale match set; plain RR read through the view keeps the snapshot;
      post-commit read returns both. Direct `MATCH … FOR UPDATE` shapes stay
      refused (20105 before AND after — conserved, asserted), plus the
      plain-shape MATCH rows are asserted unchanged; NEW (R8-F2) the same
      two-session shape on a FULLTEXT2 index (`CREATE FULLTEXT2 INDEX` —
      the always-async family): base returns the stale set, green-after
      returns the committed match through the mandatory completion; NEW
      (R7-F2, restated by R8-F1/R8-F2) the own-write locking-MATCH matrix
      through the view — own INSERT (a new matching doc in A), own UPDATE
      (A rewrites a doc to match), own DELETE (A removes an existing match)
      — each asserted visible in the locking read, each with a plain
      in-transaction MATCH control and commit/rollback controls (sync index:
      own INDEX rows; fulltext2: the SOURCE-row overlay);
   8. forced-secondary-index discovery regressions (red on base, green after);
   9. lock release on COMMIT/ROLLBACK; RC regression blocks;
   10. NEW (R5-F2) prepared-statement block: PREPARE a FOR UPDATE SELECT under
      RR; EXECUTE it repeatedly with a peer commit between executions; every
      EXECUTE returns the newly committed current value while plain reads
      inside the transaction keep the snapshot (per-execution F₀ is the
      discriminating property — a sticky frontier returns the stale value);
      NEW (R6-F2) sharded current-read witness: the multi-CN environment
      hosts a sharded table (per the review, a public SQL-visible witness —
      see item 6) with a peer commit inside the frontier window asserting
      the forwarded shard read observes the committed row for the locking
      statement while ordinary reads keep the snapshot.
   11. NEW (R8-F3) historical-snapshot controls (conservation members —
      they pass on base and MUST STAY passing; the current-read change must
      not touch them): after B commits `100→999`,
      `SELECT … FROM f AS OF TIMESTAMP <pre-commit> WHERE id=1 FOR UPDATE`
      returns the HISTORICAL 100 and still takes the lock (a peer UPDATE of
      the read row blocks/errs against A's lock), a snapshot-bearing VIEW
      (`CREATE VIEW v_asof AS SELECT id,v FROM f AS OF TIMESTAMP
      '<pre-commit>'`) under `SELECT … FOR UPDATE` returns the historical
      value the same way, and a subsequent eligible locking read of the same
      table (`SELECT … WHERE id=1 FOR UPDATE`, no snapshot clause) returns
      the CURRENT 999 while A's plain RR read stays 100; a `{snapshot=…}`
      search-source member (the named-snapshot search form) stays
      historical byte-identically (asserted by controls, not new machinery);
      NEW (R9-F3) a PREPARED conservation member: PREPARE the locking `AS OF`
      statement, then EXECUTE it in a LATER transaction whose SnapshotTS is
      beyond H while the bind transaction's was not — the execution still
      returns the historical value with the lock (per-execution engagement
      resolution; a bind-time-frozen mode would wrongly current-read it, so
      this member must stay passing).
2. **UT plan (occurrence corpus)**: construction-time capture + fabrication
   propagation; right-oriented SEMI/SINGLE swap control (role follows the
   occurrence through `IsRightJoin` swaps); the WHERE/JOIN-ON/HAVING
   scalar-subquery controls in typed-plan form (LEFT-lowered, same-table —
   only the outer occurrence marked); EXISTS/IN bodies unmarked; view-body
   scans marked along the FROM-expansion path; CTE-body scans unmarked;
   forced-index; DML plans unmarked; unmarked plans byte-identical; NEW
   (R6-F1) MATCH-family members: the view-wrapped locking MATCH plan (the
   probe4 shape) marks BOTH the surviving base scan and the fabricated
   fulltext stream (which inherits the view body's inherited outer role),
   with the typed assertion that compile delivers `DataSource.Timestamp == F`
   to the base scan scope AND the search scan scope, and that
   `vectorscan.Identity`/`ApplyScanSnapshot` derive their read point from
   that F — the review's "both base and search consumers" assertion; the
   covered-path member's (today absent) base target list conserved; a
   vector-SORT-anchor reachability check recording whether any locking
   vector form rewrites at base and, if so, the same both-consumer
   assertion on it; and the collectLockTargets output-list conservation
   assertion (byte-identical to base for every corpus plan, MATCH family
   included — direct shapes keep their 20105 refusal). NEW (R8-F3, rev-9
   provenance split per R9-F3)
   eligibility members: a locking plan over an `AS OF` source records the
   PRESENCE PROVENANCE (locked-block mark + explicit-clause fact + H) — never
   a mode — and CONSERVES
   its lock target; a snapshot-bearing VIEW body behaves the same;
   a named-snapshot search source carries no execution cohort involvement
   and keeps `SnapshotKey` behavior; every member's
   `collectLockTargets` output stays byte-identical to base. NEW (R9-F3) the
   PER-EXECUTION effective-mode matrix on the same cached/prepared plans:
   crossing S0 <= H < S1 (eligible at bind, engaging when reused) and the
   reverse direction — per execution, the eligible execution delivers F to
   the relation and search consumers while the engaging execution delivers
   F to NONE (the historical clone cohort engaging exactly as today,
   compile.go:10854-10866 and twin :5609-5622), with lock targets
   byte-identical across both executions.
3. **UT lockop**: the D-5 matrix (marked/unmarked × RC/SI, token round-trip,
   max aggregation, no UpdateSnapshot) with frontier-based comparisons.
4. **UT compile/retry**: barrier-based F₀ acquisition (stubbed barrier:
   initially-lagging application must make acquisition wait and release at the
   ordered frontier); `DataSource.Timestamp == admitted frontier` at scope
   assembly for marked scans (ordinary and INDEX_SEARCH_SCAN scopes); retry
   admission monotone with concrete floors;
   `SnapshotTS` never mutated on any exit path; rev-15 (R15-F1): the
   gate-pinning UTs, the Run-admission-validator UTs and the
   capability-transition/mixed-version matrix members are DELETED with the
   mechanism; retained NEW (R5-F2) per-execution
   UTs: a repeated-Reset execution harness asserting stubbed-barrier
   acquisitions == one per execution and per-execution retirement of frontier/
   mask/floors; plus the marked-retry persistence members of item 8 (vii)
   (R15-F2) through the lockop matrix.
4b. **UT search-plugin boundary (R6-F1, rev-8 restated)**: with a stubbed
   searcher — the pin-helper UT FIRST drives the ACTUAL helpers with
   F > SnapshotTS: the execution cohort
   (`ApplyExecutionReadTS`/`EffectiveExecutionTS`) pins while the
   named-snapshot cohort still refuses the same value (both asserted,
   snapshot_test.go:60-71 semantics preserved; rev-8 adds the
   `HistoricalSnapshot` scope: an ineligible occurrence engages no
   execution-cohort path and keeps the named-snapshot/historical route); then the stubbed
   searcher asserts the plugin's index-load SQL runs under the execution pin
   at F through the ISOLATED pair (its relation's datasource read point is
   F, its `tableCache` private and empty, its write window shared read-only),
   marked executions use the GENERATION-KEYED execution family with captured
   coverage bounds (a resident entry covering F is shared; an F beyond every
   bound triggers exactly one fresh load; no per-F keys) while snapshot-keyed
   and bare keys are untouched, and NEW (R9-F4) load-count and capacity
   members run here too: repeated AND concurrent marked executions on one
   unchanged index → exactly one load and one shared resident entry; F
   advance within coverage → no reload; F advance beyond → one reload with
   the old entry retiring through idle-eviction once unreferenced; a
   constrained-budget run (two executions needing distinct generations with
   a budget admitting one) degrades the second to F-source re-evaluation
   with the correct completed result and no statement failure; a recorded
   performance comparison (resident bytes, load counts, latency) against
   today's shared bare-key model in the same harness scenario, the tail/probe read point is
   F, covered-path INCLUDE values arrive at F, a named-snapshot MATCH read
   keeps `SnapshotKey` behavior byte-identically, and unmarked executions
   keep every hop at SnapshotTS byte-identically. NEW (R7-F2, rev-8
   two-family restatement) the locking-MATCH own-write matrix: SYNC index —
   own INSERT/UPDATE index entries and own deletes visible through the
   isolated view's shared write window; ASYNC index — the transaction's own
   SOURCE row evaluated by the runtime match predicate (add on INSERT,
   delete-then-add on UPDATE, suppress on DELETE) — each with plain-read
   and commit/rollback controls; own rows NEVER appear in a shared cache
   entry (cache-pollution assertion, both families). NEW (R8-F2)
   **controlled-CDC-lag completion UTs (R8-F2 + the R9-F2 ranked split)**:
   harness-constructed generations
   with build_ts < F, known gap rows AND an explicitly-built completed
   REFERENCE corpus (the search_cache/cdc test
   patterns); the three coverage cases are driven
   independently (covered-at-F → no completion; BEHIND → the R9-F2
   ranking split: rank-insensitive shapes complete the gap
   `(searched, F]` incrementally — runtime tokenization, gap-delete
   suppression, the own-SOURCE overlay, INCLUDE values at F — while
   RANKING-SENSITIVE shapes (pushed ScoreRange, LIMIT/top-K, score output
   or score ORDER) re-evaluate at F with F-consistent corpus statistics;
   newer-than-F / DDL-span → current-read re-evaluation complete at F).
   Completed-reference members assert EQUALITY of the emitted set with
   the reference INCLUDING scores, ordering and truncation: a gap commit
   flipping a doc across the score range; a gap delete removing the bulk
   top-1 with the unchanged runner-up ranked in; own UPDATE replacing a
   bulk winner; covered-path INCLUDE controls; plus a mode-elision
   member (rank-insensitive stays incremental and correct). NEW (R10-F1)
   own-write-versus-covered members: a COVERED generation with an own
   DELETE of the pushed-LIMIT-1 winner (the runner-up must be returned —
   the truncated bulk cannot restore it) for the ranking-sensitive class;
   a winner updated to a NONMATCH under coverage; own-inserts shifting the
   corpus statistics so an UNCHANGED doc crosses the score range; and the
   SourceCommitTS fail-closed member (a transaction-local write makes the
   bar unavailable, source_commit_ts.go:28-40, yet the marked result is
   still completed-correct through the routed path). NEW (R10-F2)
   qualification members: a token-matching gap or own INSERT FAILING the
   peeled INCLUDE/pk predicate (include_value=0 under a query requiring
   >10 — the false-positive counterexample, evaluated against the
   request's IncludePredsJSON); an UPDATE moving a row's INCLUDE value
   ACROSS the predicate boundary; a runtime-membership failure
   (FilterBytes) on BOTH the join path and the covered path. NEW (R11-F2)
   **bounded-execution members** (the item-7 resource contract): a
   deterministic constrained-budget run whose governor refuses the
   second fresh load degrades to the re-evaluation path EXECUTING the
   item-7 contract and completes with bounded retentions and a
   completed-correct result (no statement failure, no unbounded scan);
   a no-LIMIT score-output member over a large corpus asserting
   batch-wise streaming emission (bounded per-batch retentions, no
   evaluator-side whole-result materialization, rank/order delegated to
   the pipeline's EXISTING Sort/Limit operators with correct final order
   and scores); a pushed-LIMIT member asserting the K-bounded top-K heap
   (retention grows with K, never with the corpus) with reference
   equality including scores, ordering and truncation; and a
   cancellation member releasing the streaming channel/producer and its
   retentions per the existing reader close/cancel discipline. The SAME
   lagging generation under an UNMARKED MATCH keeps today's
   freshness-tolerant result
   byte-identically (ordinary-contract control); repeated prepared EXECUTEs
   renew coverage per execution (no completion state inherited). NEW
   (R7-F5) retry-token fault UTs with a lagging
   coordinator/waiter stub: invalid/missing token → explicit terminal
   error; validated token with no strict progress → terminal after the first
   non-progress admission; `SnapshotTS` never mutated on any exit.
5. **UT disttae (`CreateEngines` harness)**: discovery through each shape;
   same-table dual role; committed-tombstone view at readTS; mask via the
   ORDINARY local path (in-memory and S3-dumped deletes across compaction —
   R4-F3 demand), plus shipped-set form; late-delete and double-compaction;
   dual-view and commit invariance; statement-failure cleanliness; DDL fence
   unchanged. NEW (R8-F1) **the real execution-view consumer UT** (the
   boundary's proof — helper pins cannot substitute): with a warmed
   workspace relation cache (an ordinary RR read through the parent
   transaction first), construct the isolated execution view at F >
   SnapshotTS (harness-advanced committed state), then (a) read through
   the VIEW's relation: the newer committed row AND the transaction's own
   writes (own INSERT/DELETE) are returned, and the view datasource's read
   point equals F; (b) the parent is untouched in every observable: the same
   interned relation objects are returned to the parent afterwards, its
   `SnapshotTS` and `Transaction.op` are identity-stable, its `tableCache`
   did not grow, its writes list length/contents are unchanged; (c) the
   parent's subsequent ordinary RR read is byte-identical to the pre-view
   read (snapshot semantics preserved; nothing leaked through the borrowed
   window); (d) after the view's retirement sweep no view state remains;
   (e) NEW (R9-F1) the cold exact-PK member: with an own INSERT whose index
   entry is UNHYDRATED at view construction, the view point-probes it, the
   read returns the row through the view-private ENTRY CLONE, and the
   parent's entry `keyOffsets`/`keyIndexAccountedSize` plus the parent's
   `workspaceSize`/`approximateInMemInsertSize` are UNCHANGED through the
   view's lifetime, its retirement AND the parent's subsequent COMMIT and
   ROLLBACK (the view-side charge appears in the view balance and
   disappears at retirement; the pin ledger balances); and (f) NEW (R10-F3)
   the CONCURRENT race member: with a barrier-controlled parent-side reader
   hydrating the same entry in a parallel goroutine (the shape parallel
   pipeline fragments of one statement produce) while the view borrows,
   snapshots, clones and probes under the parent transaction mutex, run
   under the race detector asserting no race, correct results, and
   accounting conservation across the concurrent window and retirement; and (g) NEW (R11-F1) the
   TRANSACTION-CREATED-object members: a table created inside the
   transaction, and a database created inside the transaction hosting a
   second table, are looked up through the view — the chain answer is
   consulted as a PREDICATE only, the view constructs its own
   view-operator-bound handles interned in the VIEW's tableCache, the
   read point is F, NO parent-owned `*txnDatabase`/`*txnTable` payload is
   returned to the view (pointer-identity asserted against the parent's
   handles) and the base created-table branch's `v.proc.Store(p)`
   (txn_database.go:125-129) is never executed against a parent handle,
   hydration accounting lands on the view and is released at
   retirement, and the parent's subsequent COMMIT and ROLLBACK behave
   exactly as today; and (h) NEW (R12-F1) the FLUSHED own-DELETE member:
   an own DELETE flushed to S3 WITHOUT compaction (its target object
   still alive at F) is read through the view — suppression asserted
   through all three read surfaces (ApplyTombstones and GetTombstones on
   the datasource, CollectTombstones on the relation: the view's
   carried parent-mutex value-copy of the registry answers them; red
   without rev-12: the empty cloned registry resurrects the row),
   correct re-evaluation corpus/statistics (the flushed-deleted row
   absent from pass-1 N/df/avgDocLen and pass-2 candidates, with
   reference-equality results), and unmodified parent behavior: the
   parent's registry entries are byte-identical through the view's
   lifetime and retirement (no entry added or deleted by the view), the
   tombstone files stay owned by the parent's workspace entries, and the
   parent's subsequent COMMIT and ROLLBACK behave exactly as today.
   NEW (R6-F2/R7-F4) **sharding-harness extensions** on the
   existing
   delegate/handle seams (txn_table_delegate_test.go,
   txn_table_sharding_handle_test.go, txn_table_sharding_test.go):
   a captured `forwardRead` for a marked occurrence passes `ReadAt(F)` and
   the additive frontier/mask fields (unmarked requests byte-identical);
   the forged receiver request reconstructs the txn as today and derives the
   pinned read at F for reader build through the receiver-side isolated
   pair; the reader STREAM (streamReaders
   record) carries F and the mask immutably across every ReadNext until
   ReadClose or the lease sweep; the shipped
   mask is consumed through the existing ShardingRemoteDataSource
   extraTombstones
   channel on each ReadNext; a lost/abandoned build (coordinator gone) frees
   the stream by the existing lease; absent additive fields decode to an
   unmarked reader by plain-proto default (rev-15: the codec gate-pinning
   clause is deleted with the gate, R15-F1); and
   lifecycle cases cross actual RPC boundaries: build → next(s) → close,
   build → abandoned → lease sweep, next-after-cancel — asserting pin
   immutability, per-ReadNext mask usage, and zero leaked state after
   terminal cleanup.
6. **Multi-CN real regression**: 2-CN isolated launch; persisted-data join of
   two locked outer tables with peer commits to both across the window; all
   fragments current; subsequent plain RR reads unchanged (rev-15, R15-F1:
   the legacy-decode tests, the gate-forced-low uniform-legacy run and the
   high→low capability-drop member are deleted with the gate); NEW (R6-F2) the public sharded current-read witness —
   with the shard service enabled and a sharded table whose shard is not
   local to the executing CN, the two-session locking scenario asserts the
   FORWARDED shard read observes the peer commit at the coordinator's
   admitted F (locking read returns the committed row; plain RR reads stay
   snapshot; shard-forwarding observed in the trace or by the harness-held
   replica set), plus the cancellation proof that a coordinator cancel
   aborts the forwarded wait without leaking receiver state.
7. Static: incremental mo-dev closure (gofmt/vet/lint incl. regenerated pb);
   classification documented; `make static-check` left to CI.

8. **Stage-1 closure ladder (rev-13, R13-F7/R13-F6; the gating evidence for
   D-10):** every member must be RED on the delivered slice (the archived
   probe-proven matrix `evidence/probe5-prepr.log` is the standing red for the
   wrong-result members) and GREEN only on the completed stage-1 build, and
   the conservation members must prove the declinature set byte-identical to
   base:
   (i) deterministic pool-reuse UT members — successive statements in one
   still-open transaction with the SAME operator, successive transactions, an
   execution after a previously DISABLED binding generation and after a
   previously BOUND-ledger generation, recycled through the reuse package;
   (ii) discovery/remote regressions — peer-persisted BULK commits inside the
   RR window at both point and range shapes, the carrier-consistency audit as
   a static member, and the remote-reader case on the 2-CN harness;
   (iii) typed-provenance corpus members — every fabrication/copy site's
   expected roles, a `DeepCopyNode` round-trip case, the forced-index
   red/green member (item 1.8) and a covered-plan mixed-surface case — plus
   the R14-F2 ablation member: a same-table scalar-subquery plan with exactly
   one outer role dropped beside a valid `NestedBlockExplicit` occurrence
   asserts whole-statement decline BEFORE source publication, with both-marked
   and both-explicit-nested controls executing per contract, and the corpus
   asserts construction never emits the zero role;
   (iv) marked-SI revalidation members — the deterministic
   commit-between-acquisition and commit-while-waiting cases on the issue's
   own shape plus the item-3 lockop matrix;
   (v) view/CTE/JOIN mixing members — item 1.6's members plus the
   locking-view JOIN control asserting per-occurrence roles and the G4
   declinature rules;
   (vi) real disttae consumer members — live workspace, real committed data
   read through the carrier (persisted objects, own INSERT/UPDATE/DELETE
   visibility), own flushed-S3-delete suppression equivalence, and the
   persisted-tombstone meta-field conversion asserted at the txn_table.go:972
   surface — plus D-4's mandatory stage-1 regression set (R14-F1) as
   deterministic CreateEngines-harness members through the ordinary local
   path: in-memory and S3-dumped workspace deletes across compaction
   (frontier reads suppress the deleted rows while snapshot reads keep them
   hidden via original rowids), late-added deletes, double/repeated compaction
   (O→P→P2) and commit-behavior invariance;
   (vii) marked-retry persistence members (rev-15, R15-F2 — replacing the
   deleted capability-gate/drop members of R14-F3, whose members are
   removed with the gate per R15-F1): the RR marked-SI counterpart of
   `TestLockOpSkipLockedPreservesRefreshBeforeLaterTargetConflict` — two
   marked FOR SHARE targets in one batch where the first target's grant
   records a marking floor and returns an EMPTY refreshTS and a later target
   returns a direct `lockservice.ErrLockConflict` — asserting
   performLock/callSkipLocked surface the locking-read retry instead of
   skipping the candidate (ctr.retryError carries the tokenized floor);
   a two-floor max-aggregation member (both targets record floors; the
   surfaced floor/error carries the MAX, and re-persisting a lower floor
   never regresses the state); both-order definition-precedence members
   (definition change recorded before, and after, a marked floor —
   `retryWithDefChangedError` dominates in both orders); and the
   LIMIT/no-output assertions — the retry returns before any accepted
   prefix is published (`result.Batch = nil`, no partial rows consumed by
   a downstream LIMIT), including the all-candidates-skipped shape;
   (viii) an ANIMATED dual-role oracle — the watched row receives DISTINCT
   committed values per re-read so all-current and all-snapshot executions
   both FAIL, replacing the stable-value oracle;
   (ix) a boot-fresh instrumented replay suite for the locking-read BVT that
   records, per replay, the frontier-acquisition vs peer-commit watermark
   ordering for the dual-role member — the archived implementation-round miss
   either root-causes with recorded evidence or never recurs; repetition of a
   previously-flaky member without root cause does not close any member.

## 9. Open decisions

- O-D1 moerr code number: next free at implementation (base high-water 20907).
- O-D2 proto placement of the scan-node occurrence role (Node vs ScanNode):
  bounded by the R-D1 invariant (set exactly once, at construction).
- O-D3 local mask input shape (dedicated Tombstoner vs unified input with a
  category bit): bounded by the R-D4 ownership rules; no legacy behavior may
  change for unmarked readers.
- O-D4 shape of the plan-level eligibility summary (one bool on `plan.Query`
  derived at bind vs walking node roles on demand): either is sound because
  both are derived from the same immutable bind-time marks; cost prefers the
  derived summary; correctness requires only that it is computable from the
  cached plan object alone.
- O-D5 interaction of D-6 bounds with `Reset`'s TP-only admission
  (`cantCompileForPrepareErr`, returned at compile.go:295 for non-TP plans):
  FOR UPDATE plans are TP; the implementation adds a placement check to the
  Reset-time admission so the per-execution contract cannot be violated by
  an unexpected AP-placed marked plan.
- O-D6 (rev-6, rev-7 restated) exact additive placements: the sharding execution frontier/
  mask fields on `shard.ReadParam` (one additive TS + one additive mask blob
  vs a wrapped sub-message), and the receiver pin's exact carrier over its
  reconstruction (a read-only view op at F): all variants
  are sound under D-9's rules (stream-lifetime ownership under the existing
  lease, plain-proto-safe zero values with no
  capability admission — rev-15, R15-F1); chosen at implementation with the
  harness UTs pinning the pick.
- O-D7 (rev-6) plugin TS-pin carrier — **RESOLVED in rev-7 (R7-F1)**: the
  frontier rides its own execution-TS field on the search scan (plan
  `IndexSearchScan` + the Scope-wire DataSource), NOT the named-snapshot
  `ScanSnapshot` path, because `EffectiveSnapshotTS` and `vectorscan.Identity`
  admit only historical TS (sqlexec.go:147-152, execution.go:265-267) and the
  rev-6 "nameable snapshot" reuse would discard F; the named-snapshot cohort
  remains solely responsible for historical semantics, byte-identical. What
  stays bounded for implementation is the concrete Go/protobuf shape of the
  additive field and the helper names, pinned by the F > SnapshotTS
  pin-helper UT. Rev-8 refines the carrier's CONSUMER, not this decision: the
  pin executes through the ISOLATED operator/workspace pair (R8-F1) and is
  withheld from `HistoricalSnapshot` occurrences (R8-F3); before rev-8 the
  execution view was described as a workspace share — the isolated-pair
  mechanics are part of the resolved carrier.
- O-D8 (rev-8; borrow half RESOLVED by rev-9/R9-F1) runtime match-predicate
  entry for async completion, own-write overlay and the ranked
  re-evaluation path (R8-F2 + R9-F2): the exact internal entry point that
  evaluates a source row against an index's match/distance predicate at
  execution time (the classic tokenizer — `fulltext_index_tokenize`'s
  engine — the fulltext2 term pipeline, or a vector distance over the
  source vector column, plus the F-corpus statistics pass the two-pass text
  form composes). **RESOLVED (R9-F1): the borrow representation** — the
  bounded write-window borrow on the isolated workspace is the
  ENTRY-CLONE-ON-FIRST-MUTATION discipline plus the view-private pin ledger
  specified in D-3(D)/the R9-F1 decision row (stable prefix array;
  vector-readonly sharing; clones unreachable from the parent's slice;
  non-owning teardown). What stays bounded: the concrete runtime-predicate
  entry only — decision bounds: it
  must use the same parser/index configuration the index build uses (so
  runtime evaluation agrees with bulk-search semantics), it must never
  re-enter a public SQL probe path (internal evaluation only), and no
  borrowed entry, batch vector or parent counter may ever be mutated
  (asserted by the cold exact-PK and completed-reference UTs). Chosen at
  implementation with the controlled-lag
  and consumer UTs pinning the pick.

- O-D9 (rev-13) stage-1 declinature surface — **RESOLVED in rev-14
  (R14-F2)**: the check is the explicit-role walk over the bound plan — an
  unset (zero-value) scan role inside a locking-presence plan declines the
  whole statement before source publication, while `NestedBlockExplicit` and
  the covered-plan strip value are exempt by representation; computed from the
  bound plan alone, unmarked statements short-circuit untouched, decline
  conserves locks-as-today. What stays bounded: only the concrete enum
  naming/packing for the additive values and O-D4's summary storage shape,
  pinned by the item 8 (iii) ablation and corpus members and the conservation
  byte-identity blocks.
