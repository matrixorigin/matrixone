# Issue #31046 — preserve logical predicates until access selection

**PASS for implementation**, superseding the original design's assumption that all component equalities still reach regular-index selection. Author: **gpt-6.1-sol / xhigh**, selected-model session `/root/issue31037_design`, 2026-10-24. Bases remain main `ac13078b9872fe9ca84e0eeb4d1571312f99382a` and4.2-dev `0da0d693d7c7e54e57fd37f31ab8cd3fa71b7941`. No production edits by this reviewer.

## New evidence and first owner

The corrected full-planner fixture still fails after the two veto repairs. E3's real main service likewise retains10004-row base scans for all four original-schema composite queries. This is independent evidence that the first design is incomplete.

Both branches call `mergeFiltersOnCompositeKey` inside `optimizeFilters`, before secondary-index selection. It consumes `a=1` into `prefix_eq(hidden_compound_key,serial(1))`; the later point matcher cannot see `a`. Covering access also loses its logical input shape. `BlockFilterList` is optional derived pruning state, explicitly absent with `blockFilter=2`; it is not an authoritative source of logical facts.

Repair the phase boundary at the existing composite-filter owner: **keep semantic `FilterList` predicates logical until the regular access path has been selected, then lower surviving base scans with the existing composite rewriter.** Keep the original complete-PK/FORCE and sorted-preference corrections from `design.md`.

## Implementable phase API on both branches

1. Give the existing recursive `mergeFiltersOnCompositeKey` owner a small explicit phase argument, e.g. `mergeFiltersOnCompositeKey(nodeID int32, beforeIndexSelection bool)`. Reuse its traversal and `doMergeFiltersOnCompositeKey`; do not add a second general tree visitor.
2. Add one bounded structural class check: the scan has a composite physical primary/cluster key (reuse `compositeKeyPartColumns`) and at least one materialized ordinary regular secondary index. Use existing catalog algorithm/`TableExist` metadata; exclude spatial/nonregular algorithms from this deferral class. Do not match tenant names, SQL text, generated-column syntax or the particular index definition in the issue.
3. In the early phase (`optimizeFilters`), skip destructive merging for that class. Other scans retain their current early behavior, including its existing scan-stat calculation/reset. The semantic predicates stay solely in their existing `FilterList`; no saved predicate map, extra plan field, block-filter promotion or serialized-expression decoder is introduced.
4. In the late phase, process **only that deferred class** among surviving TABLE_SCAN nodes. Index-only replacements no longer have the source table's secondary-index metadata and need no composite lowering. Base scans under INDEX backfill still lower their exact residuals. Use the current branch's existing lowering/encoding guards unchanged.
5. The late phase must not call `resetHashMapStats` or otherwise clear already chosen distribution state. It changes filters only; let the existing following `reCalcNodeStatsAfterSwap(..., needResetHashMapStats=false)` refresh estimates. Do not reuse the early method's unconditional `calcScanStats`+reset in this phase.

## Exact late call-site closure

In `createQuery`, immediately after successful `applyForceIndexHints` and `applyIndices`, before runtime-filter generation and final remapping:

- Collect component block-filter candidates through existing `collectCompositePartBlockFilters` before lowering.
- Run the late composite merge on deferred scans, then existing `retainConsumedCompositePartBlockFilters`.
- Fold the resulting scan predicates with the existing `foldTableScanFilters` owner, including serialized constants/IN representation as required by the branch.
- Use the already following non-resetting stats recalculation. Append compound and consumed-component block filters with existing `appendCompoundKeyBlockFilters` / `appendCompositePartBlockFilters`; their disable-hint and deduplication behavior remains authoritative. Keep the existing filter-ordering owner after newly generated predicates are ready.
- **Clear and recompute the full `colRefCnt` from the resulting root** before any subsequent consumer of that map. Logical component refs may disappear and hidden compound refs may appear. Do not patch counts by guessed deltas. Final `remapAllColRefs` still derives its own physical scan projection/remap.

Apply the same phase contract using each branch's existing call shape;4.2's `optimizeFilters` return signature need not change. Update direct composite-merge UT callers to state the phase they exercise.

## Invariants, tradeoffs and scope

Access matchers see original typed logical predicates, including complete/prefix PK, secondary leading parts, parameters, ranges and covering inputs. This remains true when statistics are absent or block pruning is disabled. Exact residual evaluation, snapshot/locking, index hints, physical encoding and backfill identity remain with existing owners. No extra row predicate is duplicated merely to preserve facts.

There is one normal late index-selection/finalization phase per root. Repeated **pre-selection** filter optimization must preserve the logical input, and late finalization must be idempotent; test both. Do not recover predicates from an already finalized physical plan to rerun access selection. Any reachable caller requiring that reverse phase transition is a blocker to resolve explicitly, not grounds for a block-filter decoder.

No statistics formula or ranking system is changed. Nevertheless, deferred scans retain logical rather than serialized predicates during earlier estimate/join/distribution passes, so some estimates and plans can differ. This is an explicit bounded indexed-composite-class risk, not a claim of identical global statistics. Nonindexed/nonregular scans keep their prior phase. Preserve existing distribution decisions at late lowering and validate the affected class with public controls.

The implementation remains a focused planner phase correction if it uses this one owner and existing helpers. If the implementation grows into new persistent predicate state or a general lowering/cost framework, stop and redesign before that expansion. Version this concise amendment in the repository before the new phase edits if the repository's architectural gate is applied.

## Added validation required before final review

- The real SQL-to-full-planner witness, covering and noncovering, automatic/FORCE and plain/FOR UPDATE, must pass on **both branches**. Direct-selector tests alone do not close this issue. Retain E3 as the negative control for the two-veto-only implementation.
- Assert phase behavior: indexed composite logical equalities survive early optimization; nonindexed composite scans still lower early; surviving deferred base scans lower late; repeated early optimization and repeated late finalization are stable. Include composite cluster-key and full-PK controls, a range/no-secondary-point control, absent stats and `blockFilter=2`.
- Assert late lowering preserves a seeded chosen shuffle/hash-map strategy and that the refreshed reference counts/remapped scan columns contain the needed hidden key without retaining unused logical row columns. Covering replacement must not acquire an unbound hidden-base-key residual.
- Reuse the two branch baseline public artifacts. Fresh final public checks must cover original generated schema, plain-column witness, SQL PREPARE/EXECUTE reuse/NULL/no-match, exact results/metadata, actual lock conflict/release, actual rows read and matched timing controls. Add nonindexed composite, covering, sorted-range, unselective secondary and a join/distribution control within the deferred class to detect material estimate/phase regressions.
- Recompute final affected test/SCA scope to include `expr_opt.go`, `opt_misc.go`, `query_builder.go` and their direct consumers. Run owning planner normal once, affected nonempty race selection once, and final incremental static checks per branch with supported native/toolchain prerequisites. No repeated baseline discovery is required.

Final overall selected-model review remains mandatory before the authorized pushes/two PRs; no merge is authorized.
