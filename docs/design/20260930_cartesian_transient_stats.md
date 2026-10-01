# Transient optimizer statistics for Cartesian DML

Current implementation contract: v9. Owning issues:
[#29497](https://github.com/matrixorigin/matrixone/issues/29497),
[#29533](https://github.com/matrixorigin/matrixone/issues/29533),
[#29534](https://github.com/matrixorigin/matrixone/issues/29534).
Implementation: [PR #29527](https://github.com/matrixorigin/matrixone/pull/29527).
[Validation and measured limits](20260930_cartesian_transient_stats_validation.md)
record source/binary provenance, completed checks and unverified environments.

Preserve Cartesian estimated-child multiplication and the half-query-cap RIGHT
DEDUP guard. Estimates do not guarantee runtime capacity; existing allocation
and spill paths remain responsible for actual resource admission.

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
  getPublishedStats. Rows/getCommittedRows and Size use that owner,
  preserving original maps/counts and avoiding double-counting memory estimates.
- Add optional observation in txnTable, not GlobalStats: the relation knows the
  transaction/snapshot. Do not publish/store anonymous estimates in GlobalStats.
- Share the existing latest-subscription + waitCanServeTableSnapshot phase with
  getPartitionState; optional statistics stop if latest cannot serve, without
  checkpoint replay. Use one fenced immutable state and current transaction TS.
- Missing stats/no objects: positive ApproxInMemRows is existing O(1) row-tree Len,
  including versions/deletes; zero remains nil/default unless published evidence
  already proves empty. Do not fabricate completed TableName/object counts.
- Missing stats with objects: sum snapshot-visible object metadata rows plus memory Len,
  O(retained object metadata), no row scan/I/O. Use the existing iterator visibility
  predicate and close it locally. Deleted appendable objects are sealed: use their
  recorded rows for snapshots before deletion and exclude them at/after deletion.
  Only truly growing or zero-row unknown metadata uses structural object capacity.
  Future objects are excluded; memory Len remains a conservative version bound.
- Preserve ordinary published statistics on the read-only path. For own writes,
  derive the committed bound and add the workspace estimate; returning only old
  published 5 after 1.2M own INSERTs is not acceptable. Borrow immutable published maps/metadata where valid; clear the transient
  completion marker. SizeMap holds total bytes: growing TableCnt must copy/scale
  it to preserve observed average widths. Invalid counts, unrepresentable totals,
  rounded width decrease or summed-byte overflow clear the entire map, using
  existing incomplete-width fallbacks. Same/decreasing valid counts borrow it.

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
  Malformed/incomplete metadata must not yield a partial low sum. The observation reads the locked log directly; it does not call
  ForEachTableWrites while locked. No engine/subscription/internal SQL/I/O under the workspace lock.
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
  int64 shuffle row hints cannot wrap negative. AUTO_INCREMENT speculative prefetch must be bounded at its persistent
  consumer: tableCache may trigger only the existing CountPerAllocate range,
  regardless of a finite or saturated planner hint. Keep actual-batch prefetch
  and synchronous demand allocation unchanged. Delete the obsolete compiler
  source-graph DFS; safe signed hints need only the existing conversion helper. Float Cartesian output is not capped.
  Prove ordinary finite estimates unchanged and unknown->AGG/PROJECT/DEDUP/writer
  consumers with typed tests. This is required reachability, not a new issue.
- Delegate/combined observations cannot claim a partial global sum. Preserve
  published values where valid; unavailable shard/workspace ownership must not
  serialize an anonymous local low bound as global. Positive partition children
  must cover the same byte-map columns, and each column sum must be representable;
  otherwise discard the aggregate SizeMap after all merges. Empty children add
  metadata but no bytes. Byte coverage is independent of the TableName cache
  marker. Remote anonymous row promotion reuses transientTableStats so observed
  width is scaled or falls back when bytes are unrepresentable. Final cross-column
  overflow remains the existing planner completeStatsSizeMap responsibility.
  No sharding wire changes.

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
  cached1000 stays stable, cached5->nil rebuilds once. The comparison shares the
  DefaultStats cardinality constant without allocating a full default object. Same-count object transition
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
recalc, PROJECT and downstream build. Directional SEMI estimates have separate typed and public result oracles.

## BOOL metadata and vector mode changes

- The BOOL min/max producer computes minimum with AND and maximum with OR.
  Historical mixed and valid all-false bounds share false/false serialized bytes;
  the three existing objectio metadata getters return a private conservative
  BOOL view. The two writer serialization paths retain raw bounds. The format
  and non-BOOL metadata views are unchanged. Legacy false-only blocks may need
  extra reads; read compatibility cannot recover their lost provenance.
- Successful changes to the existing vector AUTO/PRE defaults clear ordinary
  cached plans and mark prepared statements for the existing EXECUTE rebuild.
  Equivalent normalized values and rejected SET preserve caches. Prepared
  handles, parameter buffers and cursors retain their original owners.

## Validation contract

- Verify real producer-to-planner cardinality and width, immutable published
  inputs, unknown/overflow bounds, snapshot visibility and Rows/Size ownership.
- Verify stable ordinary/prepared reuse, source growth and rollback, statistics
  errors before cached execution, historical cache isolation and configuration
  transitions. Preserve COUNT/LIMIT0/internal/execution-hint model controls.
- Verify durable AUTO_INCREMENT offsets with a fresh allocator and actual
  batches exceeding their configured range; planner hints cannot reserve an
  unbounded persistent range.
- Preserve public content, duplicate-key atomicity, rollback/follow-up writes,
  real spill and metadata compatibility oracles. Complete initial publication
  before injecting fixture costs; do not weaken execution assertions.
- Compare small writers and ordinary cached reads with matched controls. Report
  workspace metadata cost and stale-statistics regressions, including absolute
  timings. A known limitation is not a performance pass.

Measurements, test commands, platform gaps and historical revision evidence
belong in the linked validation record or PR, not this implementation contract.
