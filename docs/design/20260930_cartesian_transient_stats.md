# Transient optimizer statistics for Cartesian DML

Design revision: v6, reviewed by GPT-6.1-sol / xhigh before implementation.
Owning issues: [#29497](https://github.com/matrixorigin/matrixone/issues/29497),
[#29533](https://github.com/matrixorigin/matrixone/issues/29533),
[#29534](https://github.com/matrixorigin/matrixone/issues/29534).
Implementation: [PR #29527](https://github.com/matrixorigin/matrixone/pull/29527).
Evidence baseline: `f93762ed90e618f477a37ecee640ce8395c72d8e`;
verified main/merge-base: `c2abd6a54b7cd3e13c1b1494388cd7a81b8369d4`.

**Design decision: APPROVED FOR IMPLEMENTATION.** Delivery remains blocked on
the listed correctness, capacity and performance evidence. Preserve Cartesian
estimated-child multiplication and half-query-cap RIGHT DEDUP guard. No new
cache, registry, counter, protocol/confidence flag, per-SQL switch or executor.

The SEMI correction is an ordinary local fix. Stats/cache expands the estimate
and reusable-generation contract across owners and hot paths; the design gate
applies. Publish this selected design in a versioned reviewer-accessible document
in the existing PR, record its exact revision and approval before implementation.
Provider/metadata is R2; snapshot/cache/mutex admission is the bounded R3 closure.

## Cause and invariant

Actual 5-by-8 memory sources lack object statistics and default to 1,000 each.
Correct multiplication produces 1M estimated rows, selecting existing object
writing (>10MiB at 300 estimated bytes/row) and ordinary DEDUP shuffle (>320k).
The two-round matrix proves 3.61x no-PK, 5.53x regular-index, 2.25x PK and 3.07x
multi-UNIQUE regressions; accurate small statistics remove the plan changes.
RIGHT SEMI independently uses the wrong preserved side after physical swap.

A new transient row observation is a conservative estimate, never exact ANALYZE.
It must not stay small after growth through compiler or full-plan/compile reuse.
Stable inputs retain plan/compile reuse. Resource admission/spill remains the
existing execution owner; estimates are not a universal memory/accuracy guarantee.

## Provider and snapshot owner

- Extract original txnTable.Stats published/engine lookup into one private
  getPublishedStats/getEngineStats. Rows/getCommittedRows and Size use that owner,
  preserving original maps/counts and avoiding double-counting memory estimates.
- Add optional observation in txnTable, not GlobalStats: the relation knows the
  transaction/snapshot. Do not publish/store anonymous estimates in GlobalStats.
- Share the existing latest-subscription + waitCanServeTableSnapshot phase with
  getPartitionState; optional statistics stop if latest cannot serve, without
  checkpoint replay. Use one fenced immutable state and current transaction TS.
- Missing stats/no objects: positive ApproxInMemRows is existing O(1) row-tree Len,
  including versions/deletes; zero remains nil/default unless published evidence
  already proves empty. Do not fabricate completed TableName/object counts.
- Missing stats with objects: sum retained object metadata rows plus memory Len,
  O(objects), no row scan/I/O. Close iterator locally. Growing/unknown object
  counts use the existing structural object-capacity bound, not zero/partial rows.
  Include obsolete/history objects conservatively rather than promising exactness.
- Preserve ordinary published statistics on the read-only path. For own writes,
  derive the committed bound and add the workspace estimate; returning only old
  published 5 after 1.2M own INSERTs is not acceptable. Copy shared maps/metadata
  immutably when useful; clear the transient copy's completion marker only.

## Precise workspace metadata and fail-closed unknown

- Existing Workspace.Readonly() fast path has no lock/scan and no own writes.
  Snapshot-clone workspaces are empty/read-only. A created table may use a positive
  workspace-only bound; no observed rows remains unknown, not a fabricated zero.
- Otherwise TryLock the existing transaction mutex. Under it, inspect the
  complete existing workspace write log for this database/table. This is a
  conservative planning upper bound; execution visibility remains its existing
  prefix owner. NewCompile advances that prefix after planning, so using it here
  omits the preceding statement's writes (public own-growth red: 40 vs 80).
  Including any tail beyond the execution prefix can only increase the bound. Sum INSERT memory batch.RowCount and persisted INSERT
  ObjectStats.Rows at the actual metadata attribute. Ignore DELETE conservatively.
  Malformed/incomplete metadata must not yield a partial low sum. Do not call
  ForEachTableWrites while locked; reuse/extract its private locked iteration if
  appropriate. No engine/subscription/internal SQL/I/O under the workspace lock.
- This is O(existing entries + matching object metadata), not O(rows). Read-only
  autocommit stays O(1). The exceptional writing-transaction cost must be measured.
  The global memory counter is rejected: 500 unrelated inserts would estimate
  sources as 505/508 and reintroduce object writing for 40 actual output rows.
  No new per-table aggregate/index/cache is justified to avoid this metadata scan.
- TryLock avoids reentrancy: internal SQL can compile with txn.Lock already held.
  On contention, unavailable serving snapshot, or incomplete nonempty observation,
  **do not return nil/default1000** while other sources receive new small counts.
  Return an anonymous conservative maximum of the existing uint64 row-count
  domain. It is an upper planning bound, not an exact count or new protocol flag.
  Source-domain overflow/unknown rejects RIGHT DEDUP through the existing guard
  and keeps no-ON incoming estimates above ordinary shuffle admission.
- Bound the integer consumers in the same closure: row-derived int32 block hints
  (AGG and scan/filter recalculation) saturate at maxInt32 before conversion/add;
  int64 shuffle row hints cannot wrap negative. Estimated auto-ID prefetch must be
  zero/disabled when source/derived cardinality is outside its integer domain,
  including a derived AGG over an unknown leaf; retain actual-batch demand
  allocation. Do not prefetch maxInt64 IDs. Float Cartesian output is not capped.
  Prove ordinary finite estimates unchanged and unknown->AGG/PROJECT/DEDUP/writer
  consumers with typed tests. This is required reachability, not a new issue.
- Delegate/combined observations cannot claim a partial global sum. Preserve
  published values where valid; unavailable shard/workspace ownership must not
  serialize an anonymous local low bound as global. No sharding wire changes.

## Compiler cache and existing generation owners

- One StatsInfoUsableForCache helper serves frontend and internal compiler.
  Anonymous positive TableCnt remains usable for planning but not a three-second
  fast hit. Existing completed TableName is the cache marker; accurate object
  counts alone cannot qualify a transient workspace overlay. Preserve published
  originals and explicit named empty observations; keep wrappers for NDV consumers.
- Use existing Workspace.Readonly() eligibility so a writing transaction cannot
  hit only old published counts and skip workspace admission. Valid historical
  ScanSnapshot cannot hit the normal table-ID cache or contaminate its subsequent
  fast eligibility; use the existing wrapper without an extra cache, keeping any
  snapshot-only cached copy anonymous if needed. Preserve tenant/schema binding.
- One shared comparison checks normal TABLE_SCAN Stats.TableCnt against current
  StatsWithTableDef, passing ObjRef/TableDef/ScanSnapshot intact. Compare unfiltered
  TableCnt, not runtime-filter-mutated Outcnt. nil maps to existing default1000:
  cached1000 stays stable, cached5->nil rebuilds once. Same-count object transition
  alone does not rebuild; existing ranges/Reset refresh execution dependencies.
- Ordinary SQL uses transaction-admitted dispatchStmt/checkModify and existing
  rebuildStaleCachedStatements. SQL/binary EXECUTE ORs the comparison into existing
  needRebuild before cached compile admission/binding. Existing rebuild metadata,
  dirty-on-rejection and exactly-once old compile release remain the owners.
  Do not patch cached node stats in place or blanket-disable plan/compile caching.
- Reuse/factor established COUNT(*)/LIMIT0 skipStats and execType override model
  predicates, including final-plan forms; skip internal/external scan models.
  Avoid endless rebuilds for intentional model differences. No arbitrary-number
  provenance inference, new captured dependency registry or per-commit invalidator.

## RIGHT SEMI and validation

Shared estimateJoinSelectivity(preserved, matching) retains the ordinary formula
and one calculation. Ordinary SEMI/INDEX still preserve logical left before swap.
Only the after-swap RIGHT SEMI owner uses child1 preserved / child0 matching for
Outcnt and BlockNum. Keep physical-right hashmap size, sum cost, existing combined
selectivity and LIMIT tail; no unrelated join heuristics change. Reuse the SINGLE
fixture for pre/post orientation, asymmetric filters, zero, LIMIT0/1/7, repeated
recalc, PROJECT and downstream build. Recorded red and owning-package/public
SEMI green prove this independent closure, not the future provider implementation.

Focused existing-fixture UT: memory/object/zero/published maps; Rows/Size ownership;
workspace memory versus multi-object batches, unrelated500 rows, log rollback/
compaction and already-held mutex; huge anonymous bounds through integer consumers;
both compiler caches within3s; stable generation/compile identity and rapid growth,
nil/default/forced-model controls, snapshot/tenant/version and rebuild rejection.

Public BVT: natural no-patch tiny DML/index/SEMI content, 1062 atomicity, rollback/
follow-up, stable prepared and rapid source growth; same-instance rerun and clean
catalog. Separate actual1.2M/64MiB/default-spill success remains mandatory capacity
evidence. Keep published-stale accuracy limitations explicit; 300M is unrun.

Rerun the identical two-round default/accurate/stale-high performance matrix.
Proved tiny writer/shuffle regressions must disappear; plain scans/conditioned
joins/VALUES/plain DML/repeated SELECT/stable binary prepared, COUNT/LIMIT0, writes
in a transaction and unrelated500 rows are acceptance controls. Measure metadata
scan cost at representative/large existing workspace prefixes; no new production
diagnostics. Report absolute cost plus ratios for sub-ms cases. An unacceptable
control regression requires refinement/review, not a nominal delivery PASS.

Changed packages need focused red-green, owning-package tests, gofmt/vet/lint;
race only the actual shared-admission closure. Cover cancellation of snapshot wait,
TryLock fail-closed return, iterator cleanup, errors before stale cached execution,
and failed metadata publication/compile cleanup. Existing untouched spill lifecycle
evidence can be reused. Old-head CI cannot certify unimplemented remediation.

Revision v6: GPT-6.1-sol / xhigh independently approved the complete-log
workspace bound on 2026-09-30 after the public transaction-growth counterexample.
Do not advance execution snapshotWriteOffset during planning or add admission
hooks. Preserve readonly O(1), target-only INSERT metadata, TryLock fail-closed,
rollback log removal, and persisted ObjectStats.Rows validation. The public red
and expected 80-row green are required delivery evidence.
