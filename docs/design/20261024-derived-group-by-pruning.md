# Derived GROUP BY pruning (issue #31037)

- Status: PASS for implementation; final implementation acceptance remains open.
- Author/reviewer: gpt-6.1-sol, xhigh; 2026-10-24.
- Frozen base: `6f773c556d4f3177f5373eb193cd576e080b662f`.
- Scope: ordinary aggregates whose redundant keys are exact, total integer expressions of explicitly retained direct grouped columns.

## Problem and measured evidence

E01 at `/private/tmp/mo-q36-main-20261024` is reusable same-base public evidence: Q36 groups INT32 ClientIP together with ClientIP-1/-2/-3; balanced 50M medians were 1.953s versus 0.962s for the equivalent one-key query. Both returned the exact row oracle and did not spill. Operator memory was 1.84GiB versus 799MiB; output was 858MiB versus 286MiB. This establishes redundant grouping cost, not a code-change improvement. Do not rerun this discovery baseline. A controlled candidate measurement is still required.

The public baseline `baseline-public.json` additionally establishes negative contracts: INT32 x+y can overflow; BIGINT+1 errors even when HAVING later rejects every group; grouping 1/x over two duplicate zero rows emits two warning 1365 diagnostics; invalid string-to-integer casting errors. Pure-looking or deterministic expressions therefore cannot automatically move above aggregation.

## Options and decision

| Option | Assessment |
| --- | --- |
| Extend GroupByHashKey alone | Smaller hash tuples, but group/exec2.go still prepares/evaluates every GroupBy expression and retains every logical group vector. Misses the measured storage/materialization opportunity. |
| Prune logical keys and reconstruct outputs | Selected. Existing projection/HAVING/window expressions can reconstruct values over fewer group outputs. Existing local/merge group, shuffle, spill, and admission paths remain owners. |
| Runtime special path | Reject. Adds expression semantics, output layout, spill/reset/free, distribution, and prepared-reuse obligations to execution for a proof available in the planner. |

Do not remove an arbitrary mathematically invertible key or infer dependencies from primary keys here. Keep direct grouped columns as anchors and remove only proven expressions of those anchors. This handles key order, multiple source columns, nesting, and operators generically without Q36 or SQL-text matching.

## Actual owners and integration

1. `group_binder.go:elideStableLiteralGroupBy` already compacts groups and the three AST registries before consumers bind. Reuse its registry remapping helpers, but perform this new rewrite after consumers bind: rebinding AST after elision can change target-aware numeric typing (`numericProjectionTypes`).
2. In `bindSelectClause`, use the existing all-consumers-bound boundary immediately after `rewriteAffineSumFamilies`, before group/aggregate subquery flattening and `appendAggNode`. Pass the same expression lists and ORDER BY list already assembled there. No new optimizer tree pass, binding state, protobuf field, runtime hint, or operator is needed.
3. Consumer closure is `ctx.projects` (including hidden ORDER BY/distinct outputs), `ctx.windows`, `ctx.times`, `boundHavingList`, `fillVals`, `fillCols`, `boundOrderBys`, and `boundTimeWindowOrderBy` when present. Initial scope skips time windows, SAMPLE, recursive blocks, and all grouping extensions, so their paths remain untouched. Ordinary window consumers are supported through the existing complete window traversal.
4. `preWindowHavingList` is split earlier, but is consumed only by the later non-aggregate HAVING branch. A rewritten block retains at least one group, hence takes `appendAggNode`, which splits the current rewritten `boundHavingList` afresh. Do not use or mutate the stale pre-split list in that branch.
5. `appendAggNode` builds GroupBy, padding keys, tags and SpillMem; later `determineGroupByHashKeys` may add its independent table-key proof. `remapAllColRefs` computes the new group-size/aggregate offsets and demanded output projection. Never manually patch compiled offsets or GroupByHashKey.

## Proof and eligibility

Build a map from source (RelPos, ColPos) to retained direct group position. Eligible anchors have ordinary signed integer or UINT8/16/32 types accepted by `affineIntegerTypeRange`; direct UINT64, BIT, floats, decimals, strings/collations and temporal domains stay outside this optimization. Conflicting complete type metadata for the same source identity must fail closed. Every source-column leaf in a removable candidate must match an explicit anchor, including complete type/nullability metadata; an ungrouped source leaf rejects that candidate.

Call existing `exactAffineIntegerRange(context, candidate)`. This is already documented as the reusable proof boundary for deterministic exact integer bases in `affine_sum_rewrite.go`. It admits canonical resolved signed-INT64 +, -, *, unary +/- and exact integer casts with checked full-domain bounds, source-free integer literals, and supported integer columns. It rejects unresolved parameters, PreparedNumeric, Literal.Src, unsupported functions/types, invalid overload/result contracts, and overflow. Nullable integer columns remain eligible: existing strict integer operators/casts propagate NULL, so equal retained values including NULL determine exactly equal candidate values.

Reuse that proof unchanged; do not copy its range calculations or add a generic deterministic/purity flag framework. In particular, `GetFunctionValueUnstableByID`, `IsVolatile`, `CannotFold`, and `GetFunctionIsProvenPureFoldableById` are not substitutes for totality: the curated purity permission is for constant folding and does not establish absence of errors. The accepted canonical integer operations have the audited in-process executor behavior and no row diagnostic/effect path. Validate the actual resolved IDs/types, not function names alone; the existing canonical proof does this.

Initial scope also requires at least two groups, consistent all-active grouping metadata, valid AST registry positions, and at least one retained anchor. Explicitly reject GROUPING SETS/ROLLUP/CUBE (including their single-set forms), SAMPLE/time-window and recursive contexts. Do not infer “ordinary” merely from all flags being true: use `ctx.groupingFuncAllowed/isGroupingSet` and existing context guards.

## Rewrite algorithm and exact reconstruction

1. Preflight before mutation. Collect anchors and eligible candidates independently of textual order; retain every direct anchor and every ineligible key. Candidate dependencies may reference retained direct anchors only, never another removable candidate. There is no dependency closure, fixpoint or cycle.
2. Compute old-to-new group positions. For each retained group create a reference with its original complete output type and new group slot. For each removed group clone its original bound tree and replace only its source-column leaves with references to the matching retained group slots. Preserve original function IDs, casts, literal values, widths/scales, nullability, collation and provenance; do not rebind or algebraically simplify.
3. Reuse/generalize `cloneAndRewriteAffineAggregateRef(s)` and its existing validation/rewrite traversal to a neutrally named tag-slot replacement utility. It already visits functions, lists, complete window/frame expressions and fails closed on unknown variants or target-tag correlation. Avoid a second general expression visitor or wrappers around another full walker. Keep existing affine callers on the shared utility and existing tests.
4. Before using it here, fail closed for consumer subquery/dependent expressions and runtime reconstruction provenance (`PreparedNumeric` or Literal.Src); their referenced query/provenance expressions can own old group slots outside the executable-child walk. Add these checks within the shared preflight policy or the same traversal, not a duplicate visitor. Group candidate proof already rejects dynamic provenance. Prepared statements with ordinary fixed integer expressions may optimize; any parameter-dependent group candidate stays intact.
5. Rewrite copies of all consumer lists/order expressions. Require each target-tag reference to have a valid old position and the expected complete original type. Source-scope GroupBy/AggList inputs must never acquire new post-aggregate references; reject unexpected group-tag refs there. Any unsupported/malformed consumer aborts the entire group rewrite with the original context intact.
6. Install compacted groups, flags, rewritten consumers and remapped `groupByAst/groupByCanonicalAst/groupByParamAst` together. Use `validGroupByPositions/remapElidedGroupByPositions`; never leave partially remapped state. Other bindings/aggregate slots/headings remain unchanged.

This is logical pruning plus existing output projections, without adding a PROJECT node. On empty input a nonempty retained key list still produces no groups. On nonempty input two rows are equivalent under retained keys iff they were equivalent under the complete original keys: each removed value is a total exact function of retained values. Reconstructed outputs therefore preserve values and types. Moving/removing these pure total evaluations cannot change SQL errors, warnings, effects, or which fallible/volatile retained keys execute.

## Planner cost and runtime ownership

Use a small named optimization work/depth budget before calling the recursively canonical range proof or cloning reconstructed outputs; exceeding it keeps the original plan. A suggested ceiling is depth 64 and a shared 4096 expression-node allowance per query block, charging candidate validation and inserted reconstructions. This guards the existing proof's nested copy/rebind work and repeated-reference expansion; no unbounded pairwise comparison/fixpoint is introduced. Anchor lookup is by source identity; candidate and consumer traversal is bounded by expression size and the cap. Include a deep/wide fail-closed test.

Runtime uses fewer ordinary GroupBy vectors through existing local/merge group compilation and serialization. Spill thresholds, reservation accounting, memory admission, recovery, cancellation, close/free and prepared reset remain with the generic operators. No new resource owner or goroutine is introduced. Forced spill and DOP checks must prove the existing path actually executes; default E01 did not spill. Do not claim this fixes the residual single-key hash-kernel gap to DuckDB.

## Acceptance evidence map

| Obligation | Efficient evidence |
| --- | --- |
| Key reduction + exact tree/type preservation | New focused planner tests: Q36, derived-before-anchor, multiple anchors, nested safe +,-,*, casts/unary, nullable anchors, unselected derived keys; assert reachable AGG key count and output function/type contracts. |
| Consumer/remap closure | HAVING filtering/alias, hidden ORDER BY, ordinal/qualified spellings, DISTINCT, ordinary window partition/order/frame; altered group positions; mixed safe/unsafe keys; existing affine and constant-group tests. |
| Negative semantic controls | Public SQL compare against baseline-public.json: INT32 overflow, BIGINT overflow under rejecting HAVING, duplicate-zero warning count/code, bad cast; planner preservation for rand/UUID/sequence/service-call functions, floats ±0/NaN, collated/CHAR text, decimals, missing anchors, unknown/malformed expressions, dynamic parameters/Literal.Src/PreparedNumeric, grouping extensions/SAMPLE/time window, subquery/correlation. Never execute service/effect functions merely to test planner rejection. |
| Empty/reuse/unhappy paths | Empty/all-NULL/duplicate/boundary rows; same prepared statement executed with changing parameters where candidates stay intact; deep/wide cap; malformed registry/type metadata atomic no-op. Existing group spill/reset/free/cancel fixtures remain reusable because no runtime code changes. |
| Distribution/spill | Same deterministic small public oracle under DOP 1/4/8, plus a forced-spill data/cardinality/threshold setup with EXPLAIN ANALYZE spill bytes/rows > 0 and exact outputs. Validate grouped-only and count-bearing shapes. |
| Regression/scope | Focused planner normal + race, directly affected affine/constant/group-hash tests, planner package normal, required repository SCA; existing group tests only where validation exposes a new concern. |
| Performance | Reuse E01 data/runtime configuration and baseline artifacts. Candidate same-input balanced runs; report median, exact results, reachable plan, operator memory/output and spill status. Compare candidate Q36 to original baseline and one-key counterfactual; a fresh interleaved base binary run is useful only to control host drift, not to repeat discovery. Add compact planner benchmark only if the bounded proof adds material compile time. |

Implementation acceptance requires the versioned design committed first, these applicable results recorded with commands/raw outputs and source/config identities, and final independent overall review on the actual implementation diff. No production/test source has been changed in this design task.

