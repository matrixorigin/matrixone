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



# Budget-aware aggregate distribution — PR #31040 review amendment

- Status: PASS for implementation; final code/performance review remains required.
- Model/reviewer: gpt-6.1-sol, xhigh; 2026-10-24.
- Reviewed head: d27daa42a50c8fca50d348454b8f744f245d6cc6. Original design/proof and validated logical pruning remain unchanged.
- Trigger: mo-bot review5637939378 and independently reproduced baseline/before-fix artifacts under review-fix/.

## Failure and correct owner

For one million INT rows with100000 groups, DOP4 and agg_spill_mem4194304, pruning redundant keys corrects the estimated group count. The existing cardinality-only shuffle test consequently stops redistributing input. Four local partial groups then retain overlapping states, followed by a merge group. The reviewer measured19.1ms→131.2ms and11.12MiB spilled; the task host reproduced15.79ms→88.09ms and11.24MiB spilled, with exact outputs. The unpruned single-key query also has the regression, establishing a generic distribution-cost omission.

Fix determineShuffleForGroupBy in shuffle.go. Do not weaken the total-expression proof, restore redundant keys, inflate cardinality, add a pruning marker, or introduce a runtime controller. Preserve the ordinary cardinality and COUNT(DISTINCT) decisions; add a bounded memory-pressure reason to select existing complete group ownership.

## Existing contracts reused

- Stats.HashmapStats.HashmapSize estimates pre-HAVING groups; child.Stats.Outcnt estimates rows actually reaching grouping. A rejecting HAVING must not reduce retained input/group state.
- calcDOP in stats.go owns the existing block/CPU estimate. Query.MaxDop caps it; system.GoMaxProcs supplies the available local CPU limit. Actual compile-time placement and the existing single-owner fallback remain authoritative.
- fixedTypeRetainedBytes in stats.go safely resolves fixed-width payload sizes and declines unsupported protobuf IDs.
- hashtable.EstimateInt64HashMapSize / EstimateStringHashMapSize own the runtime cell growth/load-factor model. Reuse them rather than an invented bytes-per-group multiplier.
- Ordinary COUNT/STARCOUNT retain one INT64 state vector; grouped-only retains none. Audit/resolve their actual function IDs, DISTINCT bit, arity and result type before using this state model. Other aggregate families have additional state contracts and decline this optional estimate.
- node.SpillMem is the actual operator setting. colexec.ResolveSpillThreshold(0) matches group.container.setSpillMem's existing auto formula including its128MiB floor. Reuse it without opening a query/CN memory budget or allocation generation.
- Group runtime uses values<10000 as forced group-count thresholds and values>=10000 as bytes. Do not use colexec.ShouldSpill here: its<=100000 row-count convention is a different operator contract.

## Bounded estimate and decision

Let N be finite positive child input rows, G=min(HashmapSize,N) be finite positive pre-HAVING groups, and W be the existing local worker estimate:

    cpuLimit = min(system.GoMaxProcs(), Query.MaxDop when positive)
    W = calcDOP(cpuLimit, child.Stats, builder.isPrepareStatement)
    P = ceil(min(G, N/W))       # possible groups per local partial owner
    S = ceil(G/W)              # groups per complete shuffle owner
    payload = sum(retained logical group-key fixed widths)
              + sum(ordinary COUNT/STARCOUNT INT64 state widths)
    M(k) = runtimeHashCellEstimate(k) + k*payload
    B = colexec.ResolveSpillThreshold(node.SpillMem)

Use finite/overflow-safe conversions/additions. M is an optional cost estimate, not allocation admission or a claim that every byte of vector/chunk/NULL metadata is modeled. Hash cells follow the runtime codec: sum physical equality-key fixed widths plus the runtime's per-component NULL-marker rule; H8 when the complete fixed key fits eight bytes, otherwise HStr. Count payload for every retained logical grouping slot, even when GroupByHashKey omits it physically. Validate physical-key positions/uniqueness and flags rather than indexing malformed metadata.

Select the additional memory-pressure reason only when:

    W > 1
    P > S
    B >= 10000
    M(P) > B
    M(S) <= B

This compares a replicated partial owner expected to cross its byte spill threshold with complete ownership expected to fit. It also reduces replicated-state work: local-plus-merge retains approximately W*M(P)+M(G), versus W*M(S) for shuffled ownership. P is a cost-model capacity estimate; it is not a proof that every physical scan owner observes every group. Existing statistics/placement and actual-plan validation bound that uncertainty.

In the reproduced INT+COUNT case, P=100000 and S=25000. Runtime H8 cell estimates are4MiB versus1MiB; logical key plus count payload contributes1.2MB versus0.3MB. Thus the explicit4MiB byte budget exposes a concrete partitioning opportunity without calibrated magic bytes.

## Eligibility and integration

1. Existing early topology exclusions remain first: special local DISTINCT preaggregation, explicitly prescribed DISTINCT shuffle, nonmergeable DISTINCT, coarse split grouping sets, and dontShuffle(child). The new reason must never bypass them.
2. The new optional estimate accepts ordinary all-active grouping only, fixed-width complete logical/physical keys, and grouped-only or ordinary COUNT/STARCOUNT state lists. Decline dynamic/grouping-set expansion, variable-width keys, DISTINCT or other aggregates, missing/default/invalid stats/types/positions and unknown worker estimates. This is a bounded supported cost domain, not a claim that unsupported plans cannot benefit.
3. Compute the new reason beside standardShuffle/distinctStateShuffle. Only return early when all three reasons are false. Reuse the existing highest-NDV active-column selection, GetHashColumn/type support and determineShuffleType/reuse/range owners.
4. For the memory reason, require the chosen key's estimateNDVAfterSelection to meet the existing ordinary ShuffleThreshHoldOfNDV50000. This prevents raw table NDV from promoting a selective or low-owner distribution. Preserve current standard/distinct thresholds/behavior.
5. The existing final two-million-group hash-shuffle veto must also recognize the positive memory reason, as it already recognizes DISTINCT state. Otherwise a proven pressure opportunity would be silently undone when range metadata is unavailable. Known selected NDV and supported fixed key provide the existing ownership/skew floor; range/hash/reuse strategy selection remains unchanged.
6. DOP1 produces no new reason. Multi-CN-only additional ownership is intentionally not predicted from absent scheduler state; compilation retains its existing placement/version/single-owner guards. No planner fields, protobuf data or runtime state are added.

Runtime instrumentation, spill/recovery, cancellation, reset/free, resource admission and stage scheduling stay with existing operators. New work is O(group slots + aggregate slots) plus logarithmic bounded hash-capacity arithmetic; no plan-tree walk/fixpoint or function execution is needed.

## Explicit tradeoffs

Pressure-only admission deliberately leaves the resident auto/16MiB moderate-group topology unchanged: its latency ranges overlap and forcing extra dispatch merely to lower aggregate memory needs a broader cost tradeoff. The confirmed4MiB latency/spill regression is addressed at the common owner, including a handwritten single-key COUNT. Auto mode can select the addition only when its resolved per-owner budget is actually crossed. Forced group-count modes below10000 remain unchanged, preserving deterministic spill testing.

The model omits variable-size aggregate states and variable-width key payloads instead of undercounting them. The existing policies continue to own those cases. No universal no-regression or exact resident-memory prediction is claimed from an estimate; matched negative controls and actual plans are required below.

## Validation and acceptance

- Focused shuffle-owner UT: the100k/1m/DOP4/4MiB opportunity, ordinary single-key/grouped-only and multiple COUNTs, retained logical versus physical keys, H8/HStr fixed-width codecs, HAVING pre-filter group estimate, budget boundaries9999/10000 and auto resolution, DOP1, selected/unknown/NaN/Inf/default stats, selective/low-NDV keys, malformed metadata, variable-width/unsupported aggregate/DISTINCT exclusions, and pressure hash fallback below2m. Existing standard/DISTINCT/reuse/topology tests remain green.
- Public small SQL semantics can reuse original unchanged row/type/error/warning/prepared evidence; add the common-owner plan-shape counterexample with a deterministic source/NDV fixture. A BVT should assert functional results with minimal data, not wall-clock latency or a million-row load.
- Reuse the already retained exact one-million-row reproduction setup/oracle. Measure matched baseline/current/fixed runs for Q36 and handwritten single-key at4MiB; capture actual worker topology, spill rows/bytes and memory. Require the new reason to recover complete group ownership and remove the observed spill. Recheck resident auto/16MiB controls, DOP1, low NDV, selective/skew, variable-width and DISTINCT controls. Do not rerun original discovery or all unchanged proofs.
- Revalidate10M/50M positive timing and one-key/retained-composite/low-NDV controls only where new distribution decisions can differ; preserve original evidence when the chosen plans and relevant inputs remain identical. Existing forced-spill DOP controls below10000 should keep their prior topology.
- Run new/direct shuffle tests normal and focused race, planner package normal once, scoped required SCA, then final exact-fingerprint overall review. No executor changes imply no new cleanup/restart/fault harness requirement; actual spill and generic topology execution remain mandatory.
- Commit this design amendment before production/test edits. Final acceptance requires measured resolution of the reproduced regression and explicit control results, not only the analytical estimate.

