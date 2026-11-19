# Exact non-NULL multi-column COUNT DISTINCT through existing pair Groups

Owning issue: https://github.com/matrixorigin/matrixone/issues/30995

Implementation PR: https://github.com/matrixorigin/matrixone/pull/30998

## Decision

Extend the existing single COUNT DISTINCT pair-Group optimization to **tuples whose every argument is proven effectively non-NULL at the aggregate input**. Group by all canonical tuple components, distribute locally reduced rows using one sufficiently high-NDV canonical component, deduplicate the complete key at the destination, and finish with ordinary unary COUNT. Reuse existing Group, shuffle, spill and count owners.

Any nullable or unproven tuple stays on the exact existing aggregate path. This includes declared NOT NULL columns made nullable by an outer join and nullable columns with an IS NOT NULL filter unless the established proof owner already proves their effective contract. Do not invent a new predicate/nullability inference framework. Original grouping keys themselves may be nullable; this eligibility restriction applies to the DISTINCT arguments.

## Problem and ownership

Multiargument COUNT DISTINCT keeps exact argument sets and merges them serially. For a one-million-row non-NULL tuple workload this takes about 446 ms, versus about 22 ms for unary COUNT DISTINCT. The existing scalar pair-Group topology already owns local reduction, exchange, complete-key deduplication and ordinary counting; extending its planner admission avoids another execution or storage path.

Currently, HavingBinder expands COUNT DISTINCT tuple spelling into separate arguments and establishes existing string comparison normalization. `canOptimizeDistinctAgg` accepts only one aggregate argument. Multiarg COUNT therefore retains exact argument-state admission and serial exact-state merging rather than the existing pair-key topology. The first owner is `pkg/sql/plan/distinct_agg.go`; the executor correctly follows that slower plan.

## Invariant and eligibility

For grouping identity G and tuple A=(A1,...,Ak), SQL requires:

`count_G = |{ canonical(A(r)) : G(r)=G and every Ai(r) is non-NULL }|`.

The new path may use ordinary COUNT(A1) only when **all** Ai are proven non-NULL at its input. Equal complete keys from all workers/CNs must meet one exact deduplication owner. Different tuples sharing a selected component/hash bucket must remain distinct. Empty global input must return zero; empty grouped input must return no groups. A nullable/all-invalid original group must retain its existing exact result through fallback.

Admit only:

- one ordinary COUNT DISTINCT with at least two arguments and no inactive grouping flags;
- supported `int16/int32/int64`, `uint16/uint32/uint64`, `char/varchar/text` source columns, or the existing narrow comparison-cast/direct-column form;
- **every original argument** passes `builder.exprEffectivelyNotNullableBeforeRemap(arg, originalChildID)`;
- existing `shouldUseDistinctKeyPreAggregation` cost/final-group gate passes;
- a supported canonical component has finite selection-adjusted NDV at least the existing `shuffleDistinctGroupMinNDV`; choose the highest, not the first.

The effective proof is mandatory before selecting the new topology. Bind-time `arg.Typ.NotNullable` alone is unsafe: remapping refreshes it only later, and a LEFT/RIGHT/FULL/SINGLE join or materialization can null-extend a declared NOT NULL source. A LEFT JOIN can preserve all left keys while introducing NULL right keys; counting the first component alone would overcount unmatched rows. Both argument orders must use fallback whenever any argument is unproven. A conservative false proof is acceptable, including an unproven comparison wrapper; do not weaken the common proof helper to increase this task's coverage.

Unsupported types/expressions, nullable/unproven tuples, small/unknown NDV, missing/high grouping NDV, mixed/multiple aggregates and inactive grouping sets retain their old path. Unary COUNT/SUM/AVG and generic aggregate-shuffle costing remain unchanged.

## Canonical components and physical keys

Normalize admitted tuple components with existing `appendPadSpaceWindowKeyCastIfNeeded`. Direct CHAR becomes comparison VARCHAR; promoted PAD SPACE keys retain established normalization. Clone normalized expressions at the consumer boundary so remapping or metadata updates cannot mutate shared expression leaves. The accepted effective proof establishes non-NULL output for these null-preserving comparison forms; preserve charset/collation/width and correct non-NULL metadata.

Tuple values are not user-visible outputs, so canonicalization may replace their representation. Keep ordinary VARCHAR trailing spaces significant unless PAD SPACE provenance requires otherwise. Group and exchange use their existing typed Unicode collation owners. Do not change public SERIAL, ordinary casts, global string hashing or visible grouping values.

Direct CHAR normalization is needed even though Group understands CHAR equality: the legacy non-Unicode string exchange hashes its supplied bytes. Exchange the normalized output column, never raw CHAR bytes with different padding.

Let m be original GroupBy length, including hidden physical keys. The semantic pair key is:

`oldPhysicalGroupPositions + [m,...,m+k-1]`.

Old positions are a clone of nonempty original GroupByHashKey, or all original group positions otherwise. Extend them with **every** tuple axis, including a nonselected shuffle component. Assign the same identity to both pair nodes using the existing wire convention: nil means all GroupBy columns; a nonempty list must be sorted, unique and a strict subset. Use nil when the complete list spans all columns. The Group validator rejects a full explicit list.

Do not run `determineGroupByHashKey` on either new tuple pair. Nil deliberately represents complete identity, not permission for new FD pruning. The current global key annotation precedes this rewrite. Preserve original outer GroupByHashKey positions while remapping original GroupBy expressions to final pair outputs. Old PAD SPACE physical keys are equality proofs, not proof that new tuple axes are determined. Do not infer an undeclared fixture relationship such as g8=k%8.

## Existing ownership topology

`source -> local Group(G,canonical A) -> hash exchange(Aj) -> final Group(G,canonical A) -> ordinary COUNT(A1) grouped by G -> ordinary partial-count merge`.

Both pair nodes are aggregate-free, have identical full key shape and inherit SpillMem. Final pair expressions are direct references to local outputs in order, preserving the existing adjacent-pair compile contract. Mark the first with the established local-preaggregation marker and the second with the established distinct-key-shuffle marker at m+j. Keep the existing hash strategy reset; do not use stale raw-CHAR range bounds. Preserve source NDV/selectivity for costing without multiplying marginal NDVs into a claimed joint NDV.

The complete final pair Group owns exactness. Equal tuples have equal selected canonical components and therefore co-locate; collisions or common selected values affect work distribution only. The final COUNT can retain a single MergeGroup which adds compact counts, rather than merging large exact argument states. No compile exception is added or weakened.

No new resource-owner implementation, goroutine, cache, projection, temporary-file controller, queue, retry or generation rule is introduced. The additional pair Groups use existing hash/spill accounts for retained valid keys; query/statement memory and disk limits remain applicable. Error/cancel/timeout/Free/Reset and transport backpressure stay on existing operators. Binding failures abort before the original node publishes the rewritten child. Unsupported remote Group/expression/shuffle capabilities retain existing complete fallback; do not bypass capability fences.

There is no catalog/on-disk/wire/config change, persistent migration or new security/tenant boundary. Rollback is code rollback. Prepared reuse remains under current plan/operator ownership.

## Alternatives and decision log

| Alternative | Decision |
| --- | --- |
| Existing exact-state merge | Keep as correct fallback, especially for nullable/unproven tuples; it skips invalid rows cheaply and preserves zero-valued groups. |
| Component Groups plus nullable P1/P2 masks | Rejected: per-row masks add measurable CPU overhead compared with the old executor’s early NULL skip. |
| Marginal NULL-statistics cost discount | Rejected: global NULL marginals do not predict filtered NULL correlation; changing shared costing could regress existing plans. |
| Generic SERIAL tuple | Not selected: it preserves original encodings and does not itself implement canonical CHAR/PAD SPACE, float, JSON/vector or Unicode equality. Correct integration needs canonicalization/materialization/NDV work and per-row packed-byte copies. Do not change its public semantics. |
| Complete-tuple shuffle codec | Deferred: existing scalar exchange plus complete Group identity solves this issue without another canonicalization/protocol consumer. Low marginal/high joint NDV remains fallback. |
| Sum local DISTINCT counts | Rejected: cross-worker duplicate tuples would be counted repeatedly. |

Accepted limitation is semantic and reusable: optimize only tuples already proven non-NULL, independent of table/query names, schema relationships, predicates or issue IDs. A future nullable optimization needs a general mechanism with measured benefit and acceptable invalid-row overhead; this task does not layer another estimator or runtime state machine to obtain it.

## Implementation and validation acceptance

Risk-to-proof map:

| Risk | Required proof |
| --- | --- |
| Nullable/unproven admission | Typed fallback for each independent nullable argument and both orders, declared NOT NULL outer-join columns and an unproven materialized/cast source. Effective proof must guard the new branch before lowering. |
| Complete tuple identity/FD | Local/final physical lists preserve all tuple axes, nil/all-column contract, PK-minimized/PAD SPACE old keys, and differences only in a nonselected component. |
| Canonical strings | CHAR/PAD SPACE, ordinary VARCHAR trailing spaces, available Unicode equivalences and normalized exchange key; preserve visible group values. |
| Consumer/public reachability | Existing real parse/bind/optimize/remap compile fixture for unary/grouped/global tuple spellings; inspect typed finalized pair contract plus public exact results. |
| Cross-worker exactness | DOP1/8 and two-CN duplicate/grouped/CHAR/prepared/spill execution; prove no local-count sum shortcut. |
| NULL/empty/fallback | Public nullable/mixed/all-invalid/empty/grouped and both outer-join argument orders produce exact prior outputs and show fallback. No mask/projection nodes from this task remain. |
| Spill/cancel/resource lifecycle | Real-spill, observed-active cancellation, concurrent survivor, prepared reuse and zero owned-spill-file cleanup proof where admitted plans/owners are unchanged. |
| Performance | Preserve demonstrated non-NULL tuple gains with final plan/content reconciliation. Controlled all-invalid comparison must return to baseline variation/accepted <=10% boundary; add no competing builds/analyzers. Reuse unchanged unary/mixed/low-NDV controls when final plan proof supports it. |

Validation covers planner/compile UT and race, incremental static analysis, public exact results at DOP1/8, two CNs, prepared execution, forced spill and concurrent cancellation. Controlled clean-main ABBA comparisons use identical data and budgets with no competing builds. The eligible tuple must improve by at least 25%; unary, mixed, low-NDV and all-invalid controls must avoid material regression. Nullable optimization remains deferred because masks added measurable CPU overhead on invalid rows and marginal statistics could not reliably predict filtered NULL correlation.

## Separate bounded logtail test correction

A separate test correction covers `TestReadBarrierResponseFollowsEarlierLogtailUpdate` in logtail/service. Receiving a response does not establish completion of the publisher's deferred admission release. Initialize readBarrierSlots capacity 1 before Start using the shared fixture bootstrap. Keep event order, barrier-ID and timestamp assertions. After response checks, use a context-timeout-bounded send to reacquire the sole slot, then receive to undo the test acquisition. Successful send synchronizes with release; timeout retains leak detection. No sleeps/polling or production behavior change.

The owning package normal tests and exact-target race count 20 validate the fixture and synchronization change.
