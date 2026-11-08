# Decimal multiplication: expression domain vs declared storage domain

- Status: Review required
- Tracking issue: [#30938](https://github.com/matrixorigin/matrixone/issues/30938)
- Implementation: branch `agent/issue-30938-main-e4fcaa57` (PR or PR series to be created after design PASS; this document is reviewed as the distinct design-first phase of that implementation review, per `references/feature-design-review.md` §2)
- Design revision: 44
- Last updated: 2026-11-04
- Revision history: revision 1 (2026-10-29, sha256 `ed94cab579352b1ddd035d181c4fa18d66ea737ffad2bd032b767986f23a1983`) received design review REQUEST_CHANGES with four findings; revision 2 (2026-10-31, sha256 `e6e93931b66a06d82c67af97bf930df080c8af64c9eb02e310be0cf6223605e1`) resolved them (F-AVG, F-FALLBACK, F-TRAVERSAL, F-VIEW) and recorded the non-blocking PR #31034 overlap (§8). Revision 2 then received a second design review with six findings; revision 3 (2026-10-31, sha256 `421f6cbce67dd78d94cafdd55ca20415ecd008a6462cf4dc44627a044f93e9f4`) resolved them (below). Revision 3 then received a third design review with four findings; revision 4 resolves them: F-AVG-BASE-CAPACITY (§2 I6, §5 — the widened publication changes the argument type the pre-existing AVG formula consumes, so a pure function of the published type newly rejects previously successful averages and changes previously successful rounding and persisted AVG schemas; the aggregate argument is now bound in its base-equivalent domain by an ordinary plan-expression cast inserted exactly where the argument's envelope differs from its published type, so the single registered formula — planner and executor — reads the parent domain verbatim), F-BOUNDARY-TRIGGER (§4.3, §9 — the boundary triggers on envelope ≠ published type, not on published width >65, so width-reducing wrappers over widened products persist the parent's own below-65/≤65 schemas such as `DECIMAL(54,0)`/`DECIMAL(53,0)`; the TRUNCATE oracle at §9 is corrected to its own arithmetic `DECIMAL(63,2)`, separate from ROUND's `(64,2)`), F-RECURSIVE-ADMISSION (§4.3, §8, §9 — the parent's member-to-anchor conversion (`query_builder.go:5904–5927`) is modeled instead of a shape-based planning reject: union slots persist the anchor envelope, recursive members are admitted per value through the parent's own cast coercion, and an unused wide branch no longer rejects a query every produced value fits), F-VIEW-OUTER-OPERATORS (§6 M2/M5, §9 — each outer operator over a view column keeps its own pre-existing result-domain contract: width-preserving consumers such as `abs` evaluate carrier-exactly, while domain-narrowing operators such as addition keep their unchanged 65-cap and checked rejection). Revision 3 resolved the second-round findings: F-AVG2-EXEC (§2 I6, §5 — the pre-existing AVG registration is the single planner↔executor channel; the round-10 aggexec deviation and revision 2's binder Rule-A are both withdrawn entirely), F-AVG2-DERIVE (§5 — AVG result inference is a pure function of the argument's published type; no provenance classification of AVG inputs exists anywhere), F-DATAFLOW (§4.3 — envelope evaluation is a demand-driven memoized one-shot walk with anchor-seeded recursion: no fixpoint iteration, so the lossy per-shape projections cannot be iterated against themselves and every slot is evaluated at most once), F-BOUNDARY-FLOOR (§4.3 boundary rule — the persisted type is exactly the envelope-derived declared type, removing the `max(65, ·)` floor and restoring base schemas below 65, e.g. `DECIMAL(64,2)` for `ROUND(product,2)`), F-VIEW-PRESENT (§6 M2 — the decimal presentation owner is the executable projection type at the view-reference binding seam; catalog surfaces read the persisted declared records; no narrowing cast and no decimal change in the ENUM/SET-only restore seam), F-SCALE-OVERFLOW (§1, §9 — the product scale rule is stated exactly and the physical-overflow oracle is the kernel's 256-bit coefficient range, not a 76-digit count). Revision 4 then received a fourth design review with four findings; revision 5 resolves them: F-FALLBACK-DOMAIN (§1, §9, decision log 22 — the multiply kernel's scaled fallback path (`d256MulScaledFallback`/`d256MulResultFits`, `pkg/sql/plan/function/arith_decimal_fast.go:2193–2218`) still admitted against the `DECIMAL(65, s)` declared domain instead of the physical carrier, so carrier-fitting high-scale products such as `(65,40)×(65,40)` holding `10^15` were rejected contrary to I1; the fallback admission is replaced with the inline path's exact physical criterion — rounded coefficient magnitude below `2^255` — strictly widening acceptance, keeping the pre-existing `Decimal256 Mul overflow` family for genuine physical overflow and changing no previously accepted value), F-CAPPED-CONSUMERS (§4.1, §4.2, §5, §9, decision log 23 — capped consumers were treated as 'capped, base-identical', but `decimalAddSubReturnType`, `decimalDivisionReturnType` and `SumReturnType` below argument width 43 are width-sensitive below their caps: `TRUNCATE((38,10)×(38,10),0)+z(DECIMAL(39,0))` publishes `(65,0)` where the parent authored `(54,0)`, an invisible divergence to both the boundary and the revision-4 aggregate binding; §4.1 gains add/subtract and divide envelope mirrors over reconstructed operand envelopes, §4.2's machine-checked exhaustiveness extends to seam-sensitive (not only >65) outputs, and §5's envelope-bound aggregate argument generalizes to every decimal aggregate whose registered result differs between envelope-bound and published argument types), F-BINDING-PHASE (§2 I6, §3 step 4, §5, §9, decision log 24 — revision 4 inserted the aggregate-argument cast 'at aggregate binding', but no binding-phase positional envelope resolver exists: pending group/window outputs live in BindContext binding tags and scalar subqueries stay `Expr_Sub` until `flattenSubqueries`, defeating §4.3 positional addressing at that phase; the reconciliation moves to a single final-plan pass at the end of `createQuery` — post-flatten (`query_builder.go:6700–6719`, `:11511`), post-remap (`:4538`, `:4540`) — where every AGG `AggList` and WINDOW `WinSpecs` argument resolves positionally through the same §4.3 evaluator), F-PERF-PREMISE (§2 I8, §9, §10, decision log 25 — 'inference-only, hot SELECT unchanged, no benchmark required' was false for the inserted aggregate-argument cast: a same-scale `(76,s)→(65,s)` decimal cast bypasses the duplication fast path (`pkg/sql/plan/function/func_cast.go:1456–1468`) and formats and re-parses every non-NULL coefficient (`decimal256ToDecimal256`, `:6980–7030`); I8 is replaced by an explicit cost model with a stated acceptance budget and a parent-versus-candidate performance validation plan added to §9, executed during the implementation phase). Revision 5 then received a fifth design review with three findings; revision 6 resolves them: F-DOWNSTREAM-PROPAGATION (§2 I6, §3 step 4, §5, §9, decision log 26 — the final-plan pass mutated aggregate argument and result types but the planner copies types by value at aggregate/window pull-up (`query_builder.go:2332–2344`, `:2948–2960`), at binder references (`projection_binder.go:80–100`) and at TIME_WINDOW's inner-aggregate deep copy (`having_binder.go:979–990`, executed from at `timewin.go:559–572`), and result columns read the final projection's copied types (`build.go:999–1003`): `AVG(a*b)` bound `(65,4)` at bind time, so `AVG(a*b)/3` bound division scale 8, and mutating only the AVG to `(65,0)` left the division at scale 8 where the parent used 4 — and `arithmetic.go:668–671` executes at the retained result scale; the pass now runs ordered sweeps P1 (reconcile, child-to-parent, TIME_WINDOW copies included), P2 (re-sync copied reference types from their producer slots), P3 (re-derive downstream consumer types post-order through the same registered rules — the parent's types by construction — covering scalar chains, derived/nested aggregates and final result metadata before execution)), F-ADMISSION-CONTRACT (§2 I6, §5, §9, decision log 27 — revision 5's rebound trigger fired only where envelope and registered result BOTH differed, but `avgDecimal256ReturnType` derives the identical `(65,4)` for `(76,0)` and `(66,0)`, so the mixed-origin matrix row could never trigger and a `COALESCE` arm holding `+10^70`/`-10^70` accumulated without the parent's admission and could cancel to zero, and bare-product SUM was cast-free with the same admission hole; result-type agreement is not domain-admission equivalence, so the trigger reverts to uniform envelope divergence for every decimal aggregate — bare-product SUM binds `(65,0)` and pays the same documented per-row capacity cast, and §9 pins independent cancellation oracles for mixed-origin AVG and bare-product SUM), F-MIXED-VERSION (§2 triggers, §3 step 6, §8, §9, decision log 28 — the 'no format change, normal binary replacement' claim ignored that revision 5's kernel repair changes EXECUTABLE semantics: an old-CN worker rejects the `(65,40)`-product `10^70` coefficient at `arith_decimal_fast.go:2193–2218` where the repaired kernel accepts it, and the same pipeline stays decodable by old CNs; the plan now fences the change with the repository's established remote-expression admission machinery — a new `RemoteExpressionFeatures` condition set for widened-width or fallback-reachable DECIMAL multiplication, a minted protocol floor (`defines.MORPCVersion132`), one-CN scope demotion and checked destination validation (`expression_protocol.go:60–154`, `remoterun.go` validation family, the `DecimalDivisionSemantics`-v97 precedent at `string_literal_form.go:380–382`) — so affected pipelines never reach an incompatible worker, while persisted, catalog, wire and plan formats stay unchanged and downgrade rebind reads only parent-verbatim declared ≤65 records). Revision 6 then received a sixth design review with five findings; revision 7 resolves them: F-COERCION (§2 I6, §3 step 4, §4.1, §5, §9, decision log 29 — implicit binder alignment casts bake the binder-computed target into a `TargetType` argument at binding time (`base_binder.go:8987–9050`, `:12079–12096`), so reconciling a producer underneath them (AVG `(65,4)`→`(65,0)`) makes a retained `COALESCE` coercion reconstruct the wrong common (`(69,4)` over the stale `(65,4)` cast instead of the parent's `(65,0)`; the merge formula is `setSafeDecimalWidthAndScaleFromSource`, `type_check.go:1074–1123`); the pass now replays exactly the binder's implicit coercion decision with the final branch types — provenance is the plain overload-0 `cast` without `SyntaxExplicitCast` (`base_binder.go:13035–13041`) — and SQL-authored casts stay immutable), F-SYNC-ORDER (§3 step 4, §5, §9, decision log 30 — revision 6's P2 copy re-sync ran before P3 re-derivation, so a shared producer whose scalar type P3 later finalizes (the `(65,4)`→scale-8 division on a shared CTE `AVG(a*b)/3`) re-synced stale types into SINK/SINK_SCAN projection copies (`query_builder.go:3310–3313`, `:3231–3243`, SourceStep edges `:3247–3251`); finalization is now dependency-ordered — reconcile and re-derive first, then re-sync every copy last — and the §9 `AVG(a*b)/3` witness operands switch from `10^31` to `1` because the parent's own `10^62/3` overflows `(65,4)`'s 61 integer digits, making that oracle unrunnable on the parent; the scale-4 vs stale scale-8 discrimination now reads `0.3333` against `0.33333333`), F-DEPENDENT-EXIT (§3 step 4, §5, §9, decision log 31 — `createQuery` returns through `createDependentQuery` for correlation-bearing statements (`query_builder.go:4318–4320`, returning at `correlation_binding.go:148`), bypassing any pass placed only at the main body's end (`:4593`), and correlation programs hold independent type copies (`OutputTypes`, `correlation_binding.go:208–210`; dependent scalar-call result types, `:215–217`); the reconciliation now lives in one shared owner invoked at both exits and the correlation copies join the re-sync enumeration, with the `correlation_binding_test.go:57–73` depth-two route as the §9 route oracle), F-SELECTOR-AGGREGATES (§2 I6, §4.2, §5, §6, §9, decision log 32 — revision 6's uniform trigger cast every divergent decimal aggregate argument, but value-selecting aggregates (`min`, `max` via `ReturnFirstArgType`, `list_agg.go:103`/`:120`; `any_value` `:370`; `max_by` `:717`; `percentile_disc` via `args[0]`, `aggexec/ordered_percentile.go:151–156`) return one of the input rows unchanged at the argument type, so casting their arguments narrowed carrier-fitting selected values and rejected them before selection — contradicting §4.1's transparent-selector inventory, §6 M5's exact-rows/checked-at-CTAS percentile contract, and MySQL's legitimate selected values; the trigger subject is now exactly §4.2 class (ii)'s aggregate membership `{avg, sum}` — the only aggregates whose registered result differs across the envelope/published seam, machine-checked by check two — while selecting and non-decimal-result aggregates keep the carrier argument, and §9 pins wide-value SELECT/window controls plus separate CTAS rejection cases), F-PRODUCT-ADMISSION (§2 I6, §5, §8, §9, decision log 33 — revision 6 equated the merged-envelope argument cast with the parent's product admission: the parent's multiply validator checks the multiplication's own published width (`checkDecimal256ResultPrecision`, `arithmetic.go:121–150`, limit `10^65` for `(38,0)²`), while the merged argument cast checks the wider merged domain (`ParseDecimal256` against `(66,0)`, `func_cast.go:7019–7021`), so canceling `+10^65`/`−10^65` product values — 66 digits, rejected at the parent's multiply — fit the merged envelope and could average to a silent `0` that the ±`10^70` oracle misses; inside every admitted AVG/SUM argument the pass now casts **every divergent decimal seam** to its own envelope post-order (inner-first), so the widened multiply's own seam reproduces the parent's multiply-time admission exactly and genuine 66-digit literal branches remain admitted, both pinned by §9 cancellation-boundary and genuine-literal controls). Revision 7 then received a seventh design review with six findings; revision 8 resolves them: F-INPUT-REFRESH (§2 I6, §3 step 4, §5 facts 7/10, §5 finalization protocol, §9 — revision 7's three global sweeps treated copies as outputs to be re-synchronized LAST, but copies are INPUTS to consumer decisions: the division in `AVG(a*b)/3` reads the pull-up projection's value-copied Col (`query_builder.go:2332–2344`), and the division executor runs the retained result scale (`arithmetic.go:668–671`), so S2 re-derived the division from the stale `(65,4)` copy and S3 repaired the copy afterwards with no further re-derivation — executing `0.33333333` at scale 8 where the parent executes `0.3333` at scale 4; the S1/S2/S3 sweep structure is replaced by a single dependency-ordered finalization over the producer/consumer/copy DAG that refreshes every referencing copy the moment its producer slot finalizes, before any consumer of that slot re-derives), F-HEAD-GATE (§5 admission spec, §2 I6, §9 — revision 7 conditioned seam discovery on the argument HEAD's envelope differing from its published type, but a `COALESCE(a*b, <76-digit literal>)` argument over nullable `(38,0)` columns merges to published `(76,0)` with envelope `(76,0)` (product envelope `(65,0)` combined with the genuine literal's `(76,0)`), so the gate detected no divergence, inserted no seam cast, and silently averaged cancelling `+10^65`/`-10^65` product rows the parent multiply rejected (`arithmetic.go:121–150`); seam discovery is now head-independent — every decimal subexpression of every decimal AVG/SUM argument whose OWN envelope differs from its OWN published type receives the seam cast, inner-first, with the head treated as one subexpression among them), F-PRODUCER-BOUNDARY (§5 fact 11, §5 boundary rule, §2 I8, §9 — revision 7's per-source casts cannot survive a materialized producer boundary: shared-CTE reuse materializes one producer with one output column read by N `SINK_SCAN` consumers (`cte_reuse.go:41–64`), so casting the seam inside the producer narrows every reader (a `MAX` of the same column would lose its carrier-exact row) while casting only at the consumer is impossible once the arms are merged into one column — the arm provenance is gone; the finalization pass now emits one statement-local **companion projection** per divergent seam inside a materialized producer — the seam's own uncast value — leaves the original column carrier-exact for value-selecting readers, and rebuilds the AVG/SUM consumer's argument replaying the reference-resolved tree with each divergent seam replaced by a seam cast over its companion column, so admission is enforced consumer-side per source without narrowing any unrelated reader), F-CAST-OWNERSHIP (§4.1 casts bullet, §5 fact 12, §5 replay rule, §9 — revision 7 classified replay targets by flag predicate (overload-0 `cast` without `SyntaxExplicitCast`), but that profile also matches the pass's own seam/head casts — plain cast machinery with no annotation channel — and the recursive-CTE member→anchor casts (`makePlan2CastExpr`, `query_builder.go:5922`, constructed `make.go:1060–1086`) and `appendCastBeforeExprWithOverload` alignment (`base_binder.go:12079–12096`), so the replay would have recomputed a `(76,0)` target over a seam cast's final child and silently destroyed admission, and retargeted anchor-domain coercions; the replay unit is now the enclosing coercive binding DECISION — the pass re-invokes the decision's own registered check with the final child types and the check regenerates exactly the alignment-cast layer it owns at its argument positions (checks are pure in argument types and session state), so decision-foreign fixed-target casts (seam/head admission casts, anchor coercions) are preserved bitwise without any plan-format annotation), F-DEPENDENT-ENVELOPE (§4.1 new rules, §5 fact 13, §5 channel protocol, §9 — the §4.3 evaluator had no `Expr_Dependent`/`Expr_Slot` cases (`build_ddl.go:1822–1958` dispatches only `Expr_Lit`/`Expr_Col`/`Expr_F`), so a dependent scalar product retained its widened authored type, defeated divergence detection, and a fitting `10^62` correlated average was newly rejected by the widened `(65,4)` result domain; and revision 7's copy enumeration covered `OutputTypes` and the dependent-call result `Typ` but omitted the `InputTypes` and `CorrelationSlot` types whose elementwise equalities `ValidateCorrelation` enforces (`correlation.go:216`/`:242`); the evaluator gains dependent/slot envelope rules through the capture chain, and the correlation channel finalizes in the producer order of `correlation_prepare.go:25–50` — caller captures, program `InputTypes`, callee slot types and callee internal finalization, `OutputTypes`, the caller's dependent-call result, then the caller's consumers), and F-TRANSPARENT-CONSUMERS (§4.2 check two, §2 I6, §5 fact 14, §9 — revision 7's exhaustiveness oracle whitelisted only class (ii), but value-selecting aggregates copy their input type verbatim (`ReturnFirstArgType`, `list_agg.go:103`/`:120`/`:370`/`:717` returning `typs[0]`, `:876–877`; `list_window.go:209–214`/`:234–239`/`:260–265`; `ordered_percentile.go:151–156`), so their result NECESSARILY differs across an envelope/published pair and the machine oracle would have rejected the design's own selector exemptions; check two now classifies every divergent consumer as transparent propagation (§4.1 copy/merge rules over value branches — accepting the selector families explicitly) or domain arithmetic (class (ii), §5-admitted), with unclassified divergences failing deterministically, and the "only aggregates whose result differs" prose narrows to domain arithmetic). Revision 8 then received an eighth design review with four findings; revision 9 resolves them: F-COMPANION-DEMAND (§5 facts 15/18, §5 admission spec, §2 I6, §9 I6/I10 — the companion projection captured a short-circuited seam subexpression as an unconditional producer column, but COALESCE/CASE/IF arms evaluate on shrinking row masks (`EvalCoalesce`, `pkg/sql/colexec/evalExpression.go:1628–1658`, pinned by `TestFlowControlShortCircuitInvalidCast`, `evalExpression_test.go:2994`) and the CTE-sharing code itself vetoes demand expansion for fallible outputs (`cteOutputDemandPreservesEvaluation`, `cte_reuse.go:412–443`); the companion now computes the seam's own envelope cast under the arm-selection guard chain of the seam's short-circuit ancestors — an ordinary guarded CASE reusing the executor's masks — so nothing evaluates on rows the base engine never touched, an unselected overflowing branch leaves the statement successful, and a selected violating row raises the parent's multiply-time family at the parent's producer demand), F-ROW-CHANGING-BOUNDARY (§5 fact 16, §5 admission spec, §9 I6/I10 — a per-row companion value can cross only row-preserving boundaries: pruning renumbers aggregate output slots (`query_builder.go:2310–2344`, resolved positionally `build_ddl.go:2095–2107`), a dedup step would fold the companion into the dedup key, and grouped/window aggregation and set operations emit rows with no source-row correspondence; placement now follows the row shape between the seam and its reader — guarded companions on row-preserving materialization paths, an ordinary in-tree seam-locus cast below the FIRST interior row-changing operator otherwise, gated on an admitting AVG/SUM reader), F-REPLAY-RECOVERY (§5 fact 17, §4.1 casts bullet, §5 replay rule, §9 I6/I10 — `plan.Function` carries no decision identity (`plan.pb.go:3180–3190`), and re-invoking a check over its RETAINED cast tree is not ownership-correct (a retained `(65,4)` target merges to `(69,4)` through `setSafeDecimalWidthAndScaleFromSource`, `type_check.go:1074–1123`, where a fresh rebind of the original operands produces `(65,0)`); the replay recovers each decision's pre-coercion operands — pass-inserted casts skipped by a pass-local identity set, the single binder-inserted alignment cast per position unwrapped with regeneration-verified ownership (`base_binder.go:8987–9054`), fold-decision positions rebound from the `Literal.Src` channel the binder sets only for implicit overload-0 casts (`plan.pb.go:1880`, `base_binder.go:4865–4875`) — and re-invokes the decision's registered check over the recovered operands at their final envelope types, rebuilding only changed positions), and F-MATERIALIZATION-WITNESS (§5 fact 18, §9 I6/I8/I10 — `reusableCTEProducer` declines sharing below two occurrences (`cte_reuse.go:73`, occurrences appended per table binding, `query_builder.go:5670–5686`), so the revision-8 witnesses' single-reference `SELECT AVG(v), MAX(v) FROM c` never materialized and never crossed the materialized boundary, and one statement-level rejection cannot also demonstrate a sibling reader's value; the witnesses switch to genuine two-reference forms with a one-SINK, two-SINK_SCAN plan-shape assertion and separated error and value statements, and the (a‴) benchmark is re-based on the same two-reference shape). Revision 9 then received a ninth design review with four findings; revision 10 resolves them: F-GUARD-SEMANTICS (§5 fact 15, §5 admission spec, §2 I6, §9 I6/I10, decision log 44 — a conjunctive–guarded companion does NOT reproduce the executor’s masks: `EvalCase` keeps a row in `remaining` when its predicate is NULL (`pkg/sql/colexec/evalExpression.go:1605–1614`), so negating earlier CASE predicates deviates from three–valued selection, and ordinary AND evaluates both operands eagerly with no flow-control special case (`:1886–1892`), so a guard conjunction evaluates fallible seam subexpressions on rows whose arm the parent never selects – `COALESCE(x, a*b, c*d)` with non–NULL x skips both products, but the companion guard for the third arm evaluates `a*b` and can raise the kernel overflow on a previously successful statement; the companion is replaced by a single statement–local **envelope–shadow column**: a structural replica of the whole admitted argument expression with each divergent seam carrying its in–place envelope cast and its coercive binding decisions replayed, evaluated by the executor’s own short–circuit machinery (the same `EvalCoalesce`/`EvalCase`/`EvalWhenThen` code runs the replica’s unmodified predicates and arms), so evaluation demand and three–valued selection equal the base engine’s BY CONSTRUCTION), F-PRUNED-PROJECTION-DEPS (§5 fact 16, §5 admission spec, §5 finalization materialized-boundary bullet, §9 I6/I8/I10, decision log 45 — revision 9’s consumer rebuilt its argument tree consumer-side reading the ORIGINAL columns, but a `SINK_SCAN` exposes only the producer’s exported columns (`cte_reuse.go:1877–1897`) and pruning compacts producer outputs (`query_builder.go:3226–3250`), so a producer exporting only the merged column leaves the replay with no column to read; the shadow is constructed producer-side at the node where the argument’s own expression is computed — its inputs are exactly the argument’s own inputs, already in scope there and already preserved by pruning for the original column — exported as one ordinary slot, and the admitting consumer’s argument becomes a direct slot reference with no cross-boundary replay), F-POST-FOLD-RECOVERY (§5 fact 17, §4.1 casts bullet, §5 admission spec, §9 I6/I10, decision log 46 — regeneration-equality cannot be both the ownership test and the regeneration when the target legitimately changes (verification over final operands degrades an owned retained layer to foreign), and `Literal.Src` is not a general fold-provenance channel: its population is gated on the FLOAT/string prepared-peer machinery (`base_binder.go:4861–4875`) while ordinary folding creates decimal literals and typed NULLs without `Src` (`rule/constant_fold.go:320–388`, `:661–662`, `utils.go:2099–2132`) and can materialize Decimal256 constants as `Expr_Vec`; recovery is now snapshot-first — the pass records each in-scope decision’s recovered operands with their pass-entry (binding-moment) types before any mutation, verifies ownership by re-invoking the registered check over the recovered operands at those snapshot types against the retained alignment layer, and only then regenerates targets over the final envelope types — and folded positions recover through an explicit three-channel table (valued decimal literal folds re-typed from their own digits by the binder’s literal typing, check-equivalent by the pad-only lemma; NULL folds recovered as untyped NULLs; vector folds recovered through the vector’s element type), total in scope because the fold-decline keeps widened-multiplication trees unfolded), and F-PROFITABLE-MATERIALIZATION (§5 fact 18, §5 finalization materialized-boundary bullet, §9 I6/I8/I10, decision log 47 — two table bindings are necessary but not sufficient: `cte_reuse.go:172–174` also requires the profitability comparison (`:826–837`, safety factor 1 on the two-occurrence path, `:812–813`, the 2× spill margin applying only at 3+ occurrences, `:150–151`), the consumer drain requirements (`:104–113`), the demand-preservation veto (`cteOutputDemandPreservesEvaluation`, `:412–443`) and the storage admission reservation (`reserveSharedMaterialization`); an unfiltered scan/projection producer costs `outcnt = N` (`stats.go:2266–2268` for the scan, `:1812–1816` for the projection), so two references inline at 2N where sharing costs N + 3N and the revision-9 witnesses silently inlined and proved nothing; the materialization witnesses and the (a‴) benchmark are re-based on a deterministically profitable filtered producer (selectivity below one half makes 2·outcnt < producerCost on the repository’s own cost model) with the same one-SINK/two-SINK_SCAN plan-shape assertions and separated error/value statements, and the public contract’s single-reference AVG/MAX simultaneity claim is replaced by separated statements). Revision 10 then received a tenth design review with three findings; revision 11 resolves them: F-SCOPE-SPANNING-ARGUMENTS (§2 I6, §5 facts 16/18, §5 admission spec, §5 finalization materialized-boundary bullet, §6, §9 I6/I8/I10, decision log 48 — revision 10 replicated the argument's COMPLETE original expression inside the materialized producer, but a consumer-join argument such as `AVG(COALESCE(c.v, u.d))` reads `u.d`, which exists only at the consumer join — eligible pinned-build consumer joins are explicitly supported (`cte_reuse.go:1208–1246`, `:1684–1695`) and a `SINK_SCAN` exposes only the producer's exported columns (`:1877–1897`) — so no producer-side replica can compute it and no wholesale slot replacement can stand in for it; the shadow unit is therefore the exported SLOT's own producing expression: ONE shadow per (producer, referenced exported slot) covers every producer-representable divergent seam, while consumer-side seams — those reading another relation, another producer's export or a captured input — keep their ordinary in-tree envelope casts in the consumer's recomposed argument, and the consumer rebinds each shadowed slot reference directly to the exported shadow slot, a partition whose demand and three-valued selection equal the base engine's BY CONSTRUCTION on both sides, pinned by a materialized-CTE join witness with an independent MAX reader), F-FOLD-DESTROYS-DOMAINS (§4.1 casts bullet, §5 fact 17, §5 replay rule, §9 I6/I10, decision log 49 — the revision-10 fold table recovered folded operand domains from folded ARTIFACTS, but the merge consumes each operand's DECLARED width and scale (`setSafeDecimalWidthAndScaleFromSource`, `type_check.go:1074–1123`): `COALESCE(avg_out, CAST(1 AS DECIMAL(64,0)))` and `COALESCE(avg_out, CAST(1 AS DECIMAL(65,1)))` with `avg_out` `(65,4)` both merge to `(68,4)` and constant-fold to the same artifact, yet after `avg_out` finalizes `(65,0)` their fresh parent targets differ — `(65,0)` versus `(66,1)` — a distinction no property of the folded artifact retains; the binder now records a statement-local DECISION LEDGER at the alignment decision site (`base_binder.go:8987–9054`) — per (decision node, argument position) the pre-coercion operand type exactly as the merge consumed it — the decision node's identity survives every fold shape (`constant_fold.go:262–264`, `:320–330`, `:323–327`, `:530–574` mutate the operand subtree in place), final regeneration consumes ledger domains for folded positions, a ledger miss degrades safely to decision-foreign preservation, §9's coverage probe asserts totality, and paired folded-constant and typed-NULL controls compare final types, values and persisted metadata against fresh parent binds), and F-SHADOW-ADMISSION-ACCOUNTING (§5 fact 18, §5 finalization materialized-boundary bullet, §9 I6/I8/I10, decision log 50 — revision 10 appended shadow columns during finalization AFTER the materialization admission reserved from the original output schema (`cte_reuse.go:152–179`, `:712–768`, `:771–808`), so a shadowed producer's actual row bytes and spill framing exceed the reservation it was admitted on and runtime reservations (`pkg/sql/internal/materialized/source.go:484–518`) can fail near a cap; the admission now computes the reservation on the SHADOW-EXPANDED output schema — the exported storage types plus one fixed 32-byte `Decimal256` column per exported slot whose producing expression carries widened multiplication provenance, a conservative occurrence-independent upper bound on per-statement shadows — before the share is committed, all-or-nothing with unchanged cumulative counters on rejection, and a producer that cannot fit the expanded reservation declines sharing and inlines with the base-identical contract). Revision 11 then received an eleventh design review with four findings; revision 12 resolves them: F-COMMON-DOMAIN-OVERFLOW (§2 I4, §3 step 1, §4.1, §9 I4, §10, decision log 51 — the widened publication retypes the operands the value-merge families' common-domain computation consumes, so a merge the parent computed successfully can now overflow the 76-digit carrier — `(76,0)` + `(65,1)` needs `requiredWidth` 77 where the parent's `(65,0)` + `(65,1)` needed 66 (`setSafeDecimalWidthAndScaleFromSource`, `type_check.go:1074–1123`) — and each caller's failure branch regresses previously successful statements before the §5 pass ever runs: COALESCE/IFNULL bind-fail (`func_binary.go:883`, `:1136–1157`), CASE and IFF exhaust their support loops into bind failure (`operatorSet.go:559` → `:571–572`, `:896`), and LEAST/GREATEST silently degrade to FLOAT64 (`func_builtin_leastgreatest.go:376`, `:681`); on the merge's failure, and only where an operand carries widened-multiplication provenance, the call site retries the identical merge over each operand's parent-published type, so the outcome — the parent's success target with parent-domain alignment casts, or the family's own failure branch — is the parent's by construction, and comparisons keep the base engine's existing saturated fallback (`list_operator.go:272`) and need no change), F-LEDGER-LINEAGE (§4.1 casts bullet, §5 facts 16/17, §9 I6/I10, §10, decision log 52 — entry 49 keyed the ledger by (decision-node identity, argument position), but plan trees are copied: projection inlining deep-copies decision subtrees with fresh `Expr`/`Function` objects (`opt_misc.go:469–479`, `deepcopy.go:1179–1184`, `:1284–1296`) and the §5 shadow replicas are newly constructed copies, so an identity key cannot serve exactly the trees the replay must reach; the ledger is now keyed by CONTENT — function overload identity, arity, and per position the pre-coercion operand type exactly as the merge consumed it together with the post-merge aligned type — copy- and fold-stable, captured at pass entry, replayed identically by copies and shadow replicas, with an observable-state ambiguity (two entries matching one observable key while differing in a folded position's recorded domain — a deliberately constructed same-value cross-domain twin pair) surfacing as a checked planning failure asserted empty by the coverage probe, never a silent parent divergence), F-FOLDED-LAYER-WITNESS (§5 fact 17, §9 I6/I10, decision log 53 — revision 11's ownership verification re-derived the original alignment layer and compared it bitwise against the retained layer, which is unsatisfiable at a folded position because folding replaced the layer with a scalar artifact (`rule/constant_fold.go:319–330`, `:390–402`); folded-position ownership is now positional — a folded artifact is owned exactly when its type equals the entry's recorded post-merge aligned type, the fold preserving the folded node's type — unfolded positions keep the bitwise structural test, and folded constants re-normalize their value into the recorded pre-coercion domain — exact, because the alignment cast is a fixed-point scale lift that only appends zeros — before the regenerated alignment re-folds to the parent's fresh artifact), and F-NULL-FOLD-PROVENANCE (§2 I7, §3 step 2, §9 I7, §10, decision log 54 — the folding decline lived inside `MaterializeDecimal256Constant` (`rule/constant_fold.go:530`), which a NULL-producing fold never reaches: `GetConstantValue` returns the `Isnull` literal at `:661–662` with no decimal256 dispatch, so a NULL-producing widened tree folded to NULL keeping its widened type and destroyed the expression-syntax provenance the boundary reads; the decline now guards the fold walk's recursive ENTRY (`foldExpr`, `:138`) across every destructive fold shape — valued, NULL-producing and enveloping — with the materialization check retained as inner defense and untyped evaluation unchanged). Revision 12 then received a twelfth design review with two findings; revision 13 resolves them: F-LEDGER-COLLISION (§2 I7, §3 step 2/step 4, §4.1 casts bullet, §5 fact 17, §9 I7/I10, §10, decision log 55 — revision 12 accepted a deliberately constructed folded-twin pair — `CAST(1 AS DECIMAL(64,0))` versus `CAST(1 AS DECIMAL(65,1))` aligning to and folding as the identical `(68,4)` artifact inside decisions whose sibling operand later finalizes differently — as an unresolvable collision rejected by a checked planning failure, but the twin statement is parent-SUCCESSFUL (the parent binds two distinct results `(65,0)` and `(66,1)` and never fails), so the deliberate rejection corner violated the design's own compatibility contract; the failure's premise was that folding destroys the distinguishing operand cast syntax before the pass's capture, so instead of failing, the binder now marks each recorded decimal coercive decision that the source-domain resolver classifies provenance-bearing and enters its content signature in the statement-local DECISION BARRIER carried on the statement's compile-time `process.Process`, and BOTH fold engines — the rule engine's `foldExpr` (`rule/constant_fold.go:138`) and the sibling engine `constantFoldWithPreparedExactSource` (`pkg/sql/plan/utils.go:1888`), each receiving the same `proc` — decline their recursive entry at any node whose signature matches a barrier entry, preserving the recorded pre-coercion structure through every post-recording fold and speculative `DeepCopyExpr` fold alike; capture now distinguishes the twins structurally, each binds its own entry and replays the parent's decision, the twin statement executes with parent-equivalent values, result types and persisted metadata (§9 I10's single-statement regression), the residual ambiguity check becomes mechanism-bug defense in depth — unreachable for recorded decisions while the barrier stands and probe-asserted empty so a future ungated fold path fails loudly in CI — and entry 52's corpus-absence rationale is superseded), and F-RECONVERGENCE-CHANNEL (§2 I4, §3 step 1, §4.1 reconvergence bullet, §9 I4, §10, decision log 56 — revision 12 placed the reconvergence retry inside the four merge check call sites, but a merge checker consumes a pure `[]types.Type` vector (`function/function.go:1161`, reached through `function.GetFunctionByName` at `base_binder.go:8323–8327`): a widened product and a genuine wide literal both supply `(76,0)` while needing different retry decisions, and behind a producer reference the surface type alone cannot recover the producing expression's operand widths `bindColRef` resolves only a copied `Binding` type, `base_binder.go:653–657`); the retry therefore moves into the binder's source-domain funnel gating exactly the reconvergence families' function lookups, resolving each operand's (provenance flag, parent-published type) through the statement-local decision ledger for direct expression trees, through a planner-local per-column channel on producer `Binding`s — populated at producer-binding construction from the producing expression's recorded signature, the `headingProvenance` precedent at `bind_context.go:106–134`/`:510–562`, never serialized — for derived-table, view, CTE and alias references — and through the retained subquery projection for scalar subqueries; non-widened operands resolve identity views (genuine wide literals keep their natural width, so a no-provenance never-retries is automatic even where the surface types coincide), the LEAST/GREATEST silent-degrade detector retries exactly when the first lookup returns FLOAT64 over decimal-family inputs with multiplication provenance, a retry failure re-arms the family's own original outcome untouched, the four family files leave the planned production set as the retry's only owners become `base_binder.go` plus the registry carrier, and §9 gains the direct / derived-table / CTE (inlined and materialized) / scalar-subquery reconvergence table with identical-type genuine-wide controls and the deferral-set-empty resolver-coverage assertion). Revision 13 then received a thirteenth design review with two findings; revision 14 resolves them: F-AGG-WINDOW-CHANNEL (§2 I4, §3 step 1/step 4, §4.1, §5 fact 17, §9 I4/I7/I10, §10, decision log 57 — the revision-13 resolver channels never reach the current query block's own pending aggregate and window producers, which bind `ColRef`s into `BindContext.aggregates` at `aggregateTag` (`having_binder.go:480–499`) and into `ctx.windows` at `windowTag` (`window_binder.go:1187–1195`/`:1444–1453`) with no `Binding` column and no `Expr_Sub` head, so `COALESCE(AVG(a*b), L76)` — whose parent merges `(65,0)` with `(76,0)` while the provisional `(65,4)` needs width 80 — could neither retry nor flag the barrier; the resolver gains channel (d), the binder's tag/ordinal pending-producer channel, descending the retained producing call and deriving the producer output's parent type through the parent's registered result rule over the resolved parent argument domains, used for both reconvergence retries and barrier provenance) and F-REGISTRY-LIFECYCLE (§2 I7/I8, §3 step 2, §9 I10, §10, decision log 58 — the registry rode the statement's compile-time `process.Process` with no install/restore/teardown owner although one process is reused across statements (`mysql_cmd_executor.go:6220`, `:6453–6472`) and retries (`computation_wrapper.go:3011–3070`); one carrier-provided enter/release pair now installs a fresh empty registry at both outermost planner entries — `BuildPlan` (`build.go:721`) and `(*BaseOptimizer).Optimize` (`optimize.go:54`) — shares it with nested entries through a depth marker (required because the optimizer's fold rules at `optimize.go:29`/`:32` run inside the same generation after the nested `BuildPlan` returns), and a defer-registered release restores the exact prior field value when the outermost entry returns on success, error and panic unwind alike, establishing statement isolation, retry and reprepare freshness and bounded retention; `pkg/sql/plan/build.go` and `pkg/sql/plan/optimize.go` join the planned production set, i.e. 17 production files). Revision 14 then received a fourteenth design review with two findings; revision 15 resolves them: F-GROUPED-CHANNEL (§2 I4, §3 step 1/step 4, §4.1 reconvergence bullet, §5 fact 17, §9 I4/I7/I10, §10, decision log 59 — entry 57's tag/ordinal channel covers only the block's pending aggregate and window producers, but a grouped expression reaches the same merges as a `ColRef{groupTag, colPos}` reference minted by the projection and HAVING binders (`projection_binder.go:56–65`, `having_binder.go:55–66`) with the retained bound key in `BindContext.groups` (`group_binder.go:272–290`; output read through `groupOutputType`, `types.go:1093–1098`): `SELECT COALESCE(a*b, CAST(1 AS DECIMAL(65,1))) FROM t GROUP BY a*b` merges `(65,0)`+`(65,1)` into `(66,1)` on the parent while the widened `(76,0)` needs `requiredWidth` 77 and fails before reconciliation with no channel to retry; the resolver's tag inventory now descends grouped outputs through `ctx.groups[colPos]` — the group output IS the retained producing expression, its parent-published type the parent's own bind read through the ledger with the grouping-set nullability adjustment on top — sampled outputs through `ctx.sampleFunc.columns[colPos]` (`build_sample.go:76`/`:96`, pair at `:137`) and projected outputs through `ctx.projects[colPos]` (`order_binder.go:380–390`/`:450–460`, `base_binder.go:666–680`/`:682–692`), while `timeTag` (timestamp boundary columns only, `base_binder.go:625–644`) and `resultTag`/`sinkTag` (never minted by expression binders; plan-construction accessors `bind_context.go:181–198`) resolve irrelevant by construction — used for both reconvergence retries and barrier provenance, with grouped, HAVING, projection-fallback, ordinal and sampled corpus cells, genuine-wide grouped controls and the extended deferral-set-empty assertions) and F-VIEW-DESCRIPTION-LIFECYCLE (§2 tradeoffs, §3 step 2, §9 I10, §10, decision log 60 — entry 58 installs the registry only at `BuildPlan`/`(*BaseOptimizer).Optimize`, but the standalone view-description generations — the runtime view-columns table function's `DescribeViewColumns` (`view_columns.go:124–160` → `view_regeneration.go:142–155`), SHOW view metadata (`view_description.go:109–175`), standalone schema inference (`view_schema_inference.go:385`) and persisted-environment regeneration (`compile/view_metadata_recovery.go:1314–1340` → `view_regeneration.go:262` → `genViewTableDef` `:271`) — converge on `inferViewColumns`'s direct call of `bindAndOptimizeSelectQueryWithValidatorAndCapture` (`build.go:67`) at `build_ddl.go:301`/`:308` with no enclosing lifecycle entry: the enter/release pair gains its third invocation at that shared funnel, nested callers (`build.go:741`/`:748`, `build_dcl.go:394`, `build_ddl.go:408`) sharing through the depth marker, the defer-registered release covering every terminal path including the publication-probe bind-only return (`build.go:145–151`), and every standalone path carrying a non-nil process (the isolated contexts' `NewViewBindingProcess`, `process2.go:121–152`, used at `sql_executor_context.go:72–100` and `frontend/compiler_context.go:92–106`; the recovery path's explicit process reused across pending views, `view_metadata_recovery.go:1314–1340`; the borrowed-generation precedent `BorrowViewSchemaResources`, `pkg/vm/process/view_schema_resource.go:19–48`); the entry lands inside the already-owned `pkg/sql/plan/build.go`, so the planned production set stays 17 production files). Revision 15 received a further design review with two findings; revision 16 resolves them: F-TIMEWINDOW-CHANNEL (§2 I4, §3 step 1, §4.1, §9 I4/I7/I10; decision log 61) and F-BOUNDARY-ALIGNMENT-DOMAINS (§4.1 casts, §4.3/§5, §9 I2/I10; decision log 62). Revision 16 then received a further design review with one finding; revision 17 resolves it: F-DECISION-EVIDENCE-LIFECYCLE (§2, §5, §9 I2/I10; decision log 63) — the standalone view-generation owner moves from the shared funnel one level out to `inferViewColumns` so completed decision evidence survives through the post-funnel column-record assembly, and prepared-CTAS refresh re-derives the parent's schema by reading the retained decision ledger carried from prepare to execute. Revision 17 then received a further design review with two findings; revision 18 resolves them: F-EVIDENCE-PUBLICATION-PATH (§2, §5, §9 I2/I10, §10, decision log 64 (i)-(iii) — revision 17's DecimalFloatProofs-bundle routing never runs for a prepared CTAS, so capture and publication move to the real PREPARE/DDL path: the authoring generation's outermost release stashes the completed ledger in a generation-keyed, read-once process-side claim slot, `createPrepareStmtInSession` claims it late into a new `PrepareStmt` field, both retry paths claim their fresh generation's stash, and release pairs with the statement's specialization-cache lifetime) and F-PREPARED-CTAS-WITNESS (§9 I2/I10, decision log 64 (iv) — the refresh witness parameterized as `COALESCE(TRUNCATE(a*b, ?), d)` with EXECUTE `?=0`, so specialization necessarily changes the inferred output type, defeating the `reflect.DeepEqual` skip; a typed oracle proves the boundary evaluation runs and consumes retained evidence on each entry path, beside the explicit-cast control and the no-parameter no-op control; `pkg/frontend/mysql_cmd_executor.go` joins and `pkg/sql/plan/prepared_binding.go` leaves the planned production set, staying 20 production files). Revision 18 then received a further design review with two findings; revision 19 resolved them: F-REBUILD-EVIDENCE-PUBLICATION (§2, §5, §9 I2/I10, §10, decision log 65 (i)-(ii) — entry 64 left ordinary EXECUTE rebuilding unspecified, `rebuildPreparePlan` building a fresh generation at `computation_wrapper.go:1537` whose replacement commit at `:1565` bypasses `createPrepareStmtInSession` and both retry claims, and the same path clearing the specialization cache at `:1640–1642` — revision 18's paired release owner — before the CTAS specialization consumer at `:1922`–`:1926`; the rebuild channel gains its claim ordered after the replacement commit and before the clearing, and the carrier release decouples from the clearing to the seed-plan generation's own lifetime) and F-SIBLING-CAST-RETENTION (§9 I2/I10, §10, decision log 65 (iii)-(iv) — entry 64 (iv)'s fixture still kept output types equal, the retained non-provisional sibling alignment cast holding the COALESCE head at its prepare-stored type so the DeepEqual skip still fired; the witness moves the parameter marker into the TRUNCATE head against the complete prepared cast tree, with a typed probe oracle on every entry path and the schema-invalidation rebuild scenario). Revision 19 then received a further design review with two findings; revision 20 resolves them: F-MISS-ORACLE-COLLAPSE (§9 I10 (xiv), §10, decision log 66 (i)-(iii) — revision 19's sibling cast targets (76,12), and casts wider than 65 are looked through independently of the retained ledger (§4.1, `build_ddl.go:1870–1881`), so an empty ledger reconstructs the same (53,0) as consumption and the claimed (64,0) miss oracle was unreachable; the re-parameterized fixture `TRUNCATE(COALESCE(TRUNCATE(a*b, 0), d), ?)` embeds a decision-owned ≤65 alignment cast, separating consumption (the parent's (53,0)), the empty-ledger ablation ((64,0)) and refresh-never-ran ((54,1)) on all four entry paths) and F-REUSE-WITNESS-REACHABILITY (§9 I10 (xiv), §10, decision log 66 (iv)-(v) — at the minted protocol floor every ordinary-database CTAS plan carries a non-nil `DatabaseDefaults`, so `shouldRebuildPreparePlan` forces per-execute rebuilding (`computation_wrapper.go:2834–2837`, `:1444`, `:1477–1480`) and revision 19's cached-generation reuse scenarios were unreachable; the rebuild validation splits into a per-execute-rebuild witness under the preserved pre-existing CTAS rebuild policy and a cached-generation-reuse witness asserting its `DatabaseDefaults`-absence and no-invalidation preconditions before its no-rebuild probe, nonce identity and repeated consumption). Revision 20 passed independent design review and implementation began; implementing it verified at code and by an instrumented planner probe that its execute-route premises were unreachable, and revision 21 resolves this (F-EXECUTE-CHANNEL-UNREACHABLE, §9 I10 (xiv), §10, decision log 67): no execute route re-derives a prepared DDL plan's inferred output types — every fill is substitution-only and the ordinary rebuild builds the PREPARE statement with markers again — and the pre-existing per-column refresh skip compares the filled query's output type against the original query's output type (equal for every function-headed column), so the execute-route refresh evaluation is unreachable for widened-multiplication fixtures and the parent's own EXECUTE persists its PREPARE-snapshot tag; the contract's observable behavior is corrected to the parent's (54,1) at every execute locus with the empty-ledger ablation relocated to a labeled harness probe ((54,1) with retained evidence, (65,1) without), the explicit-cast control persisting (65,1) on both engines, Witnesses A/B keeping their formations with corrected value oracles and the retained carrier honestly scoped as read-only insurance, no production file joining or leaving the planned set. Revision 21 then received a further design review with one finding; revision 22 resolves it: F-VALUE-ARM-REBIND (§9 I10 (xiv)–(xv), §10, decision log 68) — revision 21 generalized its digits-marker fixture's skip conclusion to every function-headed column, but a parameter marker in a provisional common-VALUE arm is rebound at EXECUTE through `ResetParamRefRule` (which replaces the provisional casts and rebinds the enclosing functions with a nil process, recording nothing), changing the output column's inferred type, defeating the per-column refresh skip and leaving the reaching boundary evaluation without decision evidence — on the value-arm witness `COALESCE(TRUNCATE(a*b, 0), ?)` over `(38,10)` operands, EXECUTE `?=0.5` persists `(65,1)` where the parent's own execute derives `(54,1)`; revision 22 scopes the skip conclusion to type-preserving fills (the digits-marker fixture retained as the skip control), threads the execute process through `ResetParamRefRule` so the pre-existing binder recording hook captures the rebound coercive decisions content-keyed by their rebound types into an execute-scope registry merged over the retained carrier, pins the parent-parity oracles `(59,6)`/`(54,1)` against an independently derived parent channel, adds the missing-evidence ablation (`(65,1)`), and `pkg/sql/plan/visit_plan_rule.go` joins the planned production set (21 production files). Revision 22 then received a further design review with one finding; revision 23 resolves it: F-SUPPLY-PRODUCER-DOMAINS (§2, §9 I10 (xvi), §10, decision log 69) — revision 22's execute-rebind evidence lifecycle covered the refresh boundary but left the earlier fill-time rebind without producer lineage, so a rebound merge consumer referencing a remapped aggregate producer — `CAST(COALESCE(MAX(a*b), ?) AS DECIMAL(65,1))` over `DECIMAL(38,0)` operands, `?=0.5` typed `DECIMAL(20,1)` — failed its first lookup on the widened `(76,0)` operand and saw the retry decline with no binder context or channel entry, the pre-existing merge error propagating before the refresh could consume the merged evidence; revision 23 gives the resolution scope a plan-producer lineage view reusing the projection-refresh envelope owner's per-node inventories, attached around the same fill-and-refresh window with retained evidence read-only, restoring the parent's `(65,1)`/1.0 parity with an independent parent oracle. Revision 23 then received a further design review with two findings; revision 24 resolves them: F-SOURCESTEP-LINEAGE (§2, §9 I10 (xvii), §10, decision log 70) — entry 69 (iv)'s lineage-view closure indexes only PHYSICAL children (`childProjectLists`, `utils.go:8143`–`:8146`) with no SINK_SCAN/SourceStep resolution, so a prepared merge consumer over a MATERIALIZED shared CTE exporting a widened product still lost its producer lineage at the scan edge (probe-confirmed: two SINK_SCAN readers of one producer step, the fill erroring pre-refresh with no parent rebind); the view now delegates producer resolution to the complete existing storage-domain owner (`resolveChildRelationalOutput`, `build_ddl.go:2173`–`:2395`, including its SourceStep case `:2296`–`:2338` and path-bounded cycle handling `:2319`–`:2328`), and §9 I10 gains the genuinely materialized CTE witness (xvii); F-VALUE-ARM-PROJECTION — the round-37 handler projection replaced the VALUE-arm paragraph instead of appending the aggregate-producer one; §9 I10 (xv) and entry 68 (vi)–(vii) were verified intact in this document and the projection restores the VALUE-arm contract verbatim alongside the aggregate-producer and materialized-CTE clauses. Revision 24 then received a further design review with two findings; revision 25 resolves them: F-MULTIPRODUCER-DOMAINS (§9 I10 (xviii), §10, decision log 71) — entry 70 (iv)'s delegation declined EVERY multi-producer result, but the delegated resolver's set-operation case returns ALL branch contributors by design (`build_ddl.go:2367`–`:2383`), so a prepared merge consumer over a UNION ALL of two widened-product branches still lost its producer lineage (probe-confirmed: one `Node_UNION_ALL` with two PROJECT branches each exporting the widened `(76,0)` multiply, the storage domain resolving BOTH branch contributors for the output slot, the fill erroring pre-refresh under the multi-producer decline, the pinned twin filling OK and refreshing `(34:66:1)` on both ledger channels); the lineage view now resolves each contributor's parent domain and provenance branch-wise and combines them through the existing set-operation typing contract (the coalesce-family common resolution, `query_builder.go:4819`–`:4833`), reserving decline for genuinely unresolved cases, and §9 I10 gains the set-operation witness (xviii); F-SCHEMA-WIDTH-PROJECTION — the Challenger projection's universal 65-digit persisted-schema claims contradicted I2's preservation of the parent's genuinely inferred above-65 domains and the recorded `(66,1)` witnesses; the projection is re-aligned so the widening never authors a wider schema than the parent's own inference would have authored for the same statement while parent-genuine wider inferred tags persist exactly as the parent's records do, retaining the `DECIMAL(66,1)` witnesses explicitly. Revision 25 then received a further design review with one finding; revision 26 resolves it: F-SETOP-ALIGNMENT-RECOVERY (sections 9 I10 (xix) and 10, decision log 72) — entry 71 (iii)'s contributor evaluation retains a set-operation-OWNED alignment cast as the branch lineage wherever branch widths differ (`query_builder.go:4968`–`:4996` inserts it, `build_ddl.go:2381` returns it unchanged, `base_binder.go:9803`–`:9809` treats it as a fixed target), so a prepared merge consumer over a UNION ALL of UNEQUAL widened-product branches (`(38,0)²` and `(35,0)²`) still failed to bind the parent's converging domain (probe-confirmed in both branch orders, the narrower branch exporting the engine-inserted cast to the published `(76,0)`); the lineage view now recovers each set-operation-owned coercion's SOURCE through the engine's own ownership predicate (`unwrapPreparedSetOperationCoercion`, `utils.go:7512`–`:7529`) before deriving the contributor's parent domain — SQL-authored casts (recorded overload 1 after the prepared rebind, probe-confirmed) keeping their fixed targets — combining the per-branch parent domains through the unchanged set-operation typing contract, and section 9 I10 gains the unequal-width witness (xix). Revision 26 then received a design review with one finding; revision 27 resolves it: F-SETOP-COMMON-RECONVERGENCE (sections 9 I10 (xx) and 10, decision log 73): the two set-operation common-type owners (`query_builder.go:4828`–`:4830` initial, `utils.go:7588`–`:7590` execute) retrying their own FAILED coalesce lookups over resolver-computed parent-published branch domains — binding the parent's own common type (e.g. `(66,1)`), re-arming the identical rejection on retry failure and for every no-provenance branch set, SQL-authored cast arms keeping their authored targets. Revision 27 then received an independent pre-PR implementation review of the complete diff with five findings; revision 28 resolves them: F-MOD-PARENT-DOMAIN (sections 9 I10 (xviii)/(xxi) and 10, decision log 74 (i)/(vii)-(viii) — the parent-domain resolver gains the `mod`/`%` rule through the shared `DecimalModReturnType` typing owner, the out-of-case decline witness is withdrawn into the success family with parent/fixed success, value and persisted-tag assertions, and the decline family is re-scoped to evidence-reconstruction failure), F-SEAM-RESOLUTION (producer seams resolved through post-remap node/slot identity and SourceStep ownership, head-independently, with materialized two-reader regressions — decision log 74 (ii)), F-ENVELOPE-COVERAGE (section 4.1's addition/subtraction/division mirrors and PERCENTILE_DISC rule plus section 4.2's registry-wide inventory landed — decision log 74 (iii)), F-TIME-WINDOW-FINALIZATION (TIME_WINDOW producers, output copies and consumers finalized in dependency order through `BuildTimeWindowLayout` with parent metadata assertions — decision log 74 (iv)), and F-MEMOIZED-WALK (section 4.3's statement-scoped memo and anchor-only recursive seeding with deterministic count tests — decision log 74 (v)). Revision 28 then received an independent design-only review with one finding; revision 29 resolves it: F-MOD-STORAGE-ENVELOPE (§4.1, §4.2, section 9 I10 (xxi) (c), decision log 75) — revision 28's `mod`/`%` rule reached only the parent-domain resolver, leaving width-sensitive MOD chains over widened products to persist the widened published head at the CTAS/view storage boundary (live this round on owned isolated services: `TRUNCATE(a*b,0) % d` with `DECIMAL(38,10)` operands and `DECIMAL(39,0)` d persisted `(64,0)` where the parent persists `(53,0)`, CTAS and view alike; `(a*b) % d` with `DECIMAL(38,0)` operands persisted `(76,0)` where the parent persists `(65,0)`); section 4.1 gains the `mod`/`%` envelope mirror over reconstructed operand envelopes (`DecimalModReturnType` rule-for-rule, maximum operand width and scale), section 4.2's item-(ii) classification and the registry-wide inventory check extend to `mod`, cell (xxi) (c) gains the below-65 wrapper CTAS/view witnesses and the genuine-wide MOD control with parent-derived oracles live-confirmed this round, and entry 74 (i) is annotated kept-as-history. Revision 29 then received an independent pre-PR implementation review of the complete diff with two findings (artifact `pre-pr-review-1793553775104.json`); revision 30 resolves them: F-UNSTABLE-SHADOW-IDENTITY (§3, §5, facts 15/16/18, §9 I10 (xxii), decision log 76 (i)) — the shadow replica of a value-unstable producing expression owns an independent executor and evaluates the unstable call a second time, so the admitting reader consumes values no sibling reader of the same export observes, breaking the single-publication contract the materialization enforces; the admission channel is now selected per slot by the same value-stability classification that chose materialization — a value-stable slot keeps the shadow replica, a value-unstable slot takes each divergent seam's envelope cast in place inside the original export with its published type unchanged, so nothing evaluates twice and every consumer reads the same produced values — and F-SELECTOR-PARENT-DOMAIN (§9 I10 (xxiii) and the value-selecting row, decision log 76 (ii)) — the parent-domain resolver is completed for the six supported value-selecting selectors (max_by_non_null, percentile_disc, first_value/last_value/nth_value, lag/lead), whose pending-output and prepared-lineage merges declined where the parent converged, with the lag/lead with-default reconcile completed at both of its loci; quantileexactweighted is verified excluded (identical pre-existing admission rejection on both engines). Revision 30 then received an independent design-only review with three findings (artifact `pre-pr-review-1793556020172.json`); revision 31 resolves them: F-REPLAY-SAFETY-POSITIVE-PROOF (§3 step 4, §5, facts 15/16/18, §9 I10 (xxii), decision log 77 (i)) — a value-stability flag is not a replay-safety proof: the nonvolatile `llm_chat` performs one HTTP request per row, so a shadow channel gated on `GetFunctionValueUnstableByID` alone would duplicate external effects; the shadow channel now requires POSITIVE per-node replay-safety proof (decimal arithmetic operators or plain `cast`, or membership in the curated `provenPureValueFunctions` set through `GetFunctionIsProvenPureFoldableById`, over Lit/Col leaves), every unproven slot taking the in-place single-evaluation channel. F-LAGLEAD-STORAGE-ENVELOPE (§4.1, §9 I10 (xxiii), decision log 77 (ii)) — section 4.1's lag/lead storage envelope clipped widened default merges to precision 65, persisting `(65,1)` where the parent's reconcile derives and persists `(66,1)` (and `(65,10)` where the parent persists `(75,10)`); the envelope now reproduces the parent's `reconcileLagLeadDefaultType` coalesce-common rule with no 65-digit clip. F-SEEDED-RAND-ORACLE (§9 I10 (xxii), decision log 77 (iii)) — the seeded-`RAND(42)` witness was unrunnable (the only registered RANDOM overload is zero-argument) and non-discriminating (identical-seed executors draw identical values); it is replaced by a plan-shape oracle, a deterministic invocation-count oracle at the executor-test boundary over `llm_chat`'s HTTP requests, and a within-engine value-consistency check. Revision 31 then received an independent design-only review with two findings (artifact `pre-pr-review-1793557659411.json`); revision 32 resolves them: F-DIAGNOSTIC-REPLAY (§3 step 4, §5, facts 15/16/18, §9 I10 (xxii), decision log 78 (i) — equal returned values do not establish replay safety: the curated set contains `unhex`, which appends warning 1411 on malformed input, and the blanket plain-`cast` exemption reaches warning-appending conversions, so a shadow replica could duplicate client-visible diagnostics; the positive proof gains a diagnostic-freedom tier — decimal arithmetic error-only, casts restricted to the decimal→decimal warning-free conversion family, curated membership narrowed to per-overload audited overloads with `coalesce` audited and `unhex` the canonical excluded counterexample — plus deterministic parent/candidate/repaired warning-count oracles with masked-arm controls) and F-PLAN-SHAPE-SPLIT (§9 I10 (xxii) (a1), decision log 78 (ii) — the plan-shape oracle's assertions separated into version-shared, parent-specific, repaired-specific and failing-candidate-specific). Revision 32 then received an independent design-only review with one finding (artifact `pre-pr-review-1793558552709.json`); revision 33 resolves it: F-LAGLEAD-PARENT-REJECTION (§4.1 lag/lead bullet, §9 I10 (xxiii), the §6 observability clause, decision log 79, kept-as-history annotations on entries 76 (ii), 77 (ii)/(iv)/(vii) and 78 (v)) — the parent's `reconcileLagLeadDefaultType` does not persist `(66,1)`/`(75,10)`: it derives the coalesce-family common and then rejects every derived decimal width >65 at planning (HEAD `base_binder.go:9208`/`:9239`/`:9285`, fallback `:9261`; live receipt `/tmp/30938_bvt_dtype_base.log` row 188), so the repaired engine's 66–76 admission is recorded as an intentional provenance-blind expansion with an explicit storage policy (persist the reconcile-derived tag) and the with-default oracles are re-planned three-way (parent checked rejection, repaired success-and-persist, candidate clip-red). Revision 33 then received an independent design-only review with two findings (artifact `pre-pr-review-1793559592252.json`); revision 34 resolves them: F-LAGLEAD-PERSISTED-FLOOR (§2 I2/I4/I8, §3 step 6, §4.1 lag/lead bullet, §6 M4, §8, §9 I2/I9/View-surfaces rows and I10 (xxiii), decision log 80, kept-as-history annotations on entries 77 and 79) — the expansion band's persisted tags mint the pre-existing `RequiredProtocolVersion` marker at v132 through a new `DecimalWindowDefaultCarrierDomain` feature signal (a `lag`/`lead` decimal256 result wider than 65, reachable only through the repaired reconcile), closing authoring/replay/mixed-version/rollback for the band while `DecimalMultiplicationCarrierDomain` stays execution-only; and F-CAST-DESCRIPTOR-LEAVES (§5 predicate, §9 I10 (xxiii) (e)) — a binder-produced cast's `Expr_T` target descriptor is validated as metadata with the leaf rule scoping to value arguments, and the unreachable `DECIMAL(76,0)` positive control is replaced by the SQL-reachable `CAST(a*b AS DECIMAL(65,0))`. Revision 34 then received an independent design-only review with one finding (artifact `pre-pr-review-1793561068798.json`); revision 35 resolves it: F-LAGLEAD-SIGNAL-NEWLY-ADMITTED (§2, §4.1 signal definition, §6 M4, §8, §9 I9/I10 (xxiii), decision log 81, kept-as-history annotation on entry 80) — revision 34's width-alone detector fenced parent-supported lag/lead shapes whose wide decimal256 results arise with no reconcile: an absent default publishing the first argument's type (`list_window.go:142–193`), a NULL default short-circuiting `reconcileLagLeadDefaultType` at HEAD `base_binder.go:9148–9150` into the `:8980–8983` fallback, a matching default never entering the `:8898` gate, and no-default lag/lead over widened products or genuinely wide parent-inferred columns — unnecessarily rejecting their view authoring during upgrade skew and their view reads after rollback; the signal is redefined as recorded evidence of newly admitted default reconciliation carried by the statement-scoped decision ledger, negative feature/authoring/replay oracles are planned for those shapes plus a no-default multiplication view, and the provenance-free scale-70 positive coverage is retained. Revision 35 then received an independent design-only review with two findings (artifact `pre-pr-review-1793561911159.json`); revision 36 resolves them: F-LAGLEAD-PARENT-GATE-ADMISSION (§4.1 signal definition, §9 I9/I10 (xxiii), decision log 82 (a), kept-as-history annotation on entry 81 (a)) — newly admitted behavior is determined from the PARENT's operand domains and the PARENT's `base_binder.go:8898` gate at the lag/lead binding locus (the value argument's parent-published domain recovered through the decision ledger's recorded widening decisions), covering both gate transitions: widening can make the repaired gate DISAPPEAR (the review's `LAG(a*b,0,N76)` counterexample over `(38,0)` columns with a 76-digit literal default — the parent's `(65,0)` publication enters the gate and rejects the derived `(76,0)` at HEAD `:9284–9285` while the changed engine's equal types skip it, so revision 35's changed-gate trigger recorded nothing and the view went unfenced) or APPEAR (`LAG(a*b,0,CAST(0 AS DECIMAL(65,0)))` — the parent's gate is skipped and the parent binds `(65,0)` with no rejection, a byte-identical parent-supported record although the changed engine's gate runs and derives `(76,0)`); and F-LAGLEAD-PERSISTED-CHANNEL (§3 step 6, §4.1 publication, §6 M4, §8, §9 I9, decision log 82 (b), kept-as-history annotations on entries 80 (b)/(c) and 81 (b)) — no ledger-to-walk handoff exists (`RequiredRemoteExpressionFeatures` at `string_literal_form.go:945` receives no ledger, `BuildPlan` releases the registry at `build.go:728–745` retaining a claim only for prepared-CTAS generations, and every execution gate reconstructs features from trees), so the band rides the PRE-EXISTING `BindContext.persistedExpressionProtocolRequirement` channel (`types.go:945–949`; installed for view-definition binding at `bind_context.go:64–67`, inherited by nested blocks at `:70`; raised through an `observeExpressionProtocol` sibling at `persisted_ip.go:190–208`; captured at `build_ddl.go:287–291`, maxed post-optimize at `:326–331`, gated at `:332–341`, minted at `:451–453`/`:467`) to the view-authoring marker path — the band's only consumer — while compilation and pipeline gates need no band evidence (multiplication-provenance band statements keep the tree-derived floor and provenance-free band statements execute value-identically on the width-generic lag/lead and decimal256 cast kernels), withdrawing revision 34's three execution-site band mappings and adding the post-planning-release, cached-execution, send-time and isolation oracles plus the GATE-DISAPPEARANCE parent-rejection/new-success view witness. Revision 36 then received an independent design-only review with two findings (artifact `pre-pr-review-1793563339549.json`); revision 37 resolves them: F-LAGLEAD-VIEW-EXPANSION-PROPAGATION (§3 step 6, §4.1 publication, §6 M4/M5, §8, §9 I9/I10, decision log 83 (i), a kept-as-history annotation on entry 82 (ii) (a)) — `bindView`'s isolated expansion context (HEAD `query_builder.go:12532–12534`) shares only the ctas accumulator, so a band raise fired while expanding a band-marked inner view inside an outer CREATE VIEW never reaches the outer authoring root the capture reads (`build_ddl.go:288–290`) and the outer view loses its contracted v132 marker and skew-window authoring rejection; the expansion context now joins the generation's accumulator through one explicit share line beside the ctas precedent (transitive across nesting, nil/no-op on non-view statements, inert for CTAS, the remaining isolated sites classified nil-or-dead by construction), with planned nested-view marker, split-floor authoring-rejection, replay and isolation oracles; and F-M4-GATE-TRANSITIONS (§6 M4/M5, §8, decision log 83 (ii)) — M4's every-transition no-marker claim wrongly swept in the parent-REJECTED GATE-DISAPPEARANCE case, so the no-marker statement is limited to parent-supported cases (GATE-APPEARANCE included) with GATE-DISAPPEARANCE retained as a marked positive. Revision 37 then received the independent design-only PASS (artifact `pre-pr-review-1793564492063.json`, one P3 advisory honored in the projection). The implemented revision was then reviewed in the round-70 implementation-phase pre-PR review (artifact `pre-pr-review-1793570718605.json`, REQUEST_CHANGES, five P2 findings); revision 38 (2026-11-02, sha256 recorded at submission) resolves the two design-material findings — F-TARGET-PROTOCOL-COLLISION (§1, §3 step 6, §8, §9 I9, decision log 84 (iv): after the pinned base, origin/main allocated protocol versions 132 (decimal float rounding and strict textual float conversions, #31191), 133 (typed JSON MIN/MAX) and 134 (ODKU) with `MORPCLatestVersion = 134`, colliding with the revision-6 floor — those binaries keep the 65-digit multiplication cap yet would pass a 132-keyed gate; the base refreshes to `be8eb40ad8` (merge-base = the pinned base) and both features re-mint `defines.MORPCVersion135`, with version-132/133/134 rejection oracles joining §9 I9) and F-NARROW-CARRIER-ENVELOPE (§4.1, §9 I2, decision log 84 (iii): the multiplication envelope mirror `min(w1+w2, 65)` reproduced only the decimal256-carrier publication and narrowed narrow-carrier persisted schemas below the parent's own; the mirror now reproduces the parent's per-carrier-family `decimalMultiplyReturnType` publication over the parent's `decimalMultiplyDomain` operand domains) — and records the three verified-valid implementation-phase obligations (shadow replay-safety predicate, selector parent-domain completion, execution-cost evidence) with no design-contract change, plus the F-BAND-MESSAGE-ORACLE reconciliation of the (85,20) control's identical message routing (§9 I10, decision log 84 (vi)). Revision 39 (2026-11-02, sha256 recorded at submission) corrects, on round-71 empirical evidence, the revision-38 narrow-carrier example values and adds the F-UNSEAMED-ENVELOPE-PROVENANCE decision (§4.1, §4 boundary application, §9 I2, decision log 85): declared DECIMAL(17/18,·) columns are DECIMAL128-typed, so the refreshed parent persists (35,0)/(36,0) for the declared (18,0) pairs (parent-parity pins; revision 2's formula coincides there), the declared-pair discriminating RED control is the coefficient-promotion family DECIMAL(38,0) × DECIMAL(1,0) persisting revision-2's (39,0) where the parent authors the promoted (40,0), and the legacy-D64 coefficient-adjustment family moves to intermediate-shaped witnesses ((a+b)*a and (a+b)*(a+b) over DECIMAL(10,0) columns persist the parent's (29,0)/(38,0), the unprovenanced reconstruction's (21,0)/(22,0) being the RED controls); the envelope evaluator threads widened-multiplication provenance so unseamed trees contribute and persist their published (parent-identical) domains and the storage boundary replaces a tag only on seamed columns; §1's overview formula is qualified to the adjusted operand domains and carrier-specific caps, honoring the revision-38 approval's P3 advisory (artifact `pre-pr-review-1793572148230.json`) — the advisory's own example value (36 for DECIMAL(18,0) × DECIMAL(17,0)) assumed the legacy-D64 adjustment applies to declared columns and is superseded by the round-71 empirical evidence in decision log 85 (i) (the refreshed parent publishes (35,0) for that declared pair). Revision 40 (2026-11-02): resolves the round-72 pre-PR design review's two P2 findings — (i) F-OUTPUT-DIVERGENCE-PROVENANCE: contained widening is distinguished from output-domain divergence, parent-side types are computed node-wise by re-invoking each node's own unchanged publication rule (the parent's fixed-DECIMAL64(18,s) d64+d64 shortcut included) with fixed-domain barriers (SQL-authored casts, admission casts, member→anchor coercions) pinning parent-side := published, and the cast-plus-addition witness `CAST(a*b AS DECIMAL(10,0)) + d` persists the parent's (18,0); (ii) F-SHUTDOWN-UPGRADE-COMPATIBILITY: the mixed-version apparatus (the minted `MORPCVersion135` floor, the `DecimalMultiplicationCarrierDomain` feature, demotion, the destination guard) and the band's `RequiredProtocolVersion` marker chain are withdrawn under the repository's shutdown-only upgrade contract, band merges persisting plain catalog records in the parent's own above-65 form with forward compatibility for base-authored schemas and views inside the supported boundary; the P3 disposition pins separate RED baselines in the validation plan - the current pre-provenance candidate for the intermediate and cast-barrier witnesses, the revision-2-envelope revert for the coefficient-promotion control (decision log 86). Revision 41 (2026-11-02): resolves the round-73 pre-PR design review (artifact `state/artifacts/issue-30938/pre-pr-review-1793577123632.json`) — (i) F-PRESERVE-EXISTING-MARKERS (P2): the marker withdrawal is scoped to adding no ISSUE-SPECIFIC marker or floor; every requirement the unchanged pre-existing channel computes for the same statement persists verbatim — the v89 wide-literal requirement included (`make.go:309–311` → `string_literal_form.go:1027–1032` → `persisted_ip.go:154–155`/`:249–254` → `build_ddl.go:462–478`) — with marker-preservation assertions for inherited nonzero markers and mixed-feature views and absence assertions only where no existing feature requires a marker; (ii) F-VALIDATION-MATRIX-ALIGNMENT (P3): the I10 row's active v135/below-floor sentences are restated as plain-record persistence with inherited channel-computed requirements, and the `(40,0)` coefficient-promotion case is excluded from variant B's passing-control list (decision log 87). Revision 42 (2026-11-03, sha256 recorded at submission): F-INTERNAL-FOLLOWUP-FILL-LINEAGE — discovered while constructing the round-18 pre-PR implementation review's F1 live witnesses (artifact `state/artifacts/issue-30938/pre-pr-review-1793698927931.json`): a prepared CREATE TABLE AS SELECT runs a SECOND, independent execute-time fill inside the internal executor's follow-up reparse (`populateCreatedTable`, `pkg/sql/compile/ddl.go:2638`-`:2740` → `specializeInternalPreparedPlan`, `pkg/sql/compile/sql_executor.go:903`-`:922`), which fills through the NO-PROCESS variant and therefore declines the rebound merge with the planner error where the parent commits only its own checked evaluation rejection; the internal route joins entries 68 (iv)/69 (v)'s execute-route discipline (entered generation, process-threaded preserving fill, attached-session increment recovery), `pkg/sql/compile/sql_executor.go` joining the production set (22 files) (decision log 88). Revision 43 (2026-11-04, sha256 recorded at submission) resolves the pre-PR implementation review's P2 finding (artifact `state/artifacts/issue-30938/pre-pr-review-1793727196297.json`) — F-PREPARED-CTAS-VALUE-CONTRACT: revision 42's compatibility criterion (§1) and entry 88 (vii)'s no-other-contract-change claim are contradicted by demonstrated numeric behavior on the entry-88 route — the fixture's prepared UNION CTAS at `div_precision_increment=10` materializes `0.3333000000` on the repaired engine where the pinned base materializes `0.0000003333` (retained execution logs `r85_base_classify_r13.log`/`r85_cand_classify_r13.log`), the ordinary frontend CTAS materializing `0.3333333333` at the same setting (`r85_base_frontend_r13.log`); entry 88 is reopened and the intended prepared-CTAS numeric behavior and compatibility scope are decided explicitly — the base's value is the pre-existing internal-route append-path coefficient-passthrough defect's output, not a compatibility oracle, the intended value is the base-evaluated expression value correctly scale-aligned into the recovered persisted domain, and the internal route's division-evaluation increment stays the pre-existing build-context behavior (unchanged, out of scope) — with the validation plan gaining independently calculated value assertions for prepared and ordinary CTAS at default and non-default increments (§1, decision log 89). Revision 44 (2026-11-04, sha256 recorded at submission) resolves the pre-PR design-only review's P2 finding (artifact `state/artifacts/issue-30938/pre-pr-review-1793728646262.json`) — F-PREPARED-CTAS-EXEMPTION-SCOPE: the projection's blanket live-validation exemption for prepared set-operation CTAS contradicted the demonstrated base success of the bigint-bound division witness (`r85_base_classify_r13.log`) and entry 89 (v)'s live value comparisons; the panic exemption is restricted to the crash-evidenced shapes and bindings of entry 74 (vi) (b), live value/schema validation is explicitly retained for the successful bigint-bound witness, and §8's behavioral-compatibility claim gains §1's revision-43 named exclusion (§8, decision log 90). Applicable revision-42 decisions remain reusable; the upstream PR #31034 overlap stays partial and non-blocking (§8).

## 1. Problem and observed contract

On base `5fe08a3307` — and unchanged on the revision-38 refreshed base
`be8eb40ad8` (F-TARGET-PROTOCOL-COLLISION, decision log 84 (iv)) — SQL
infers the result type of DECIMAL multiplication as
`DECIMAL(min(dw1+dw2, 65), scale)` over the operands' adjusted coefficient
domains (`decimalMultiplyDomain`: a DECIMAL64-OID width-18 operand counts 19
for the legacy signed-int64 coefficient, a DECIMAL128-OID width-38 operand
counts 39 — §4.1's multiplication bullet; the 65 cap is the decimal256-carrier
ceiling, the narrow-route ceiling being 38) and the evaluation layer then
enforces that published width row by row. Any product whose value needs 66–76
digits is
rejected with `ERROR 1690 ... exceeds DECIMAL(65,0)` even though the product
fits MatrixOne's `Decimal256` (256-bit, ≈76 decimal digits) physical carrier:

```sql
SELECT 99999999999999999999999999999999999999 * 99999999999999999999999999999999999999;
-- base: ERROR 1690; MySQL 8.0.45 returns the exact 76-digit product.
SELECT CAST(REPEAT('9',65) AS DECIMAL(65,0)) * 2;
-- base: ERROR 1690; MySQL returns the exact 66-digit product.
```

MySQL 8 evaluates `my_decimal_mul` into a fixed 9×32-bit buffer (wider than
the declaration limit 65), returns the computed value, and clamps only the
*declaration metadata* of the result. Intermediate products genuinely wider
than the buffer fail inside the kernel
([`item_func.cc`](https://github.com/mysql/mysql-server/blob/mysql-8.0.45/sql/item_func.cc#L2193)).

The violated contract: **a value domain that the physical carrier can hold
exact must be computable and returnable as an expression result; persist-
ing a schema for these widened expression results remains governed by the
declared SQL decimal domain (`DECIMAL(M,S)`, `M ≤ 65`)**. Two real domains
exist in the engine, and the base conflates them:

1. **Expression (carrier) domain** — what `Decimal256` arithmetic can hold:
   a signed 256-bit coefficient (magnitude below `2^255` ≈ `5.79×10^76`,
   i.e. every value with ≤76 digits and the carrier-fitting half of the
   77-digit range). A product is accepted iff its rounded coefficient fits
   that range; anything larger is the kernel's own `Decimal256 Mul
   overflow`/`Div overflow` error (`arith_decimal_fast.go:3267`). **This
   acceptance rule is the design's target state, not the base reality**
   (revision 5, F-FALLBACK-DOMAIN): the inline multiply path already
   decides physical overflow alone (`d256MulInline`,
   `arith_decimal_fast.go:3207–3267`), but the scaled fallback path —
   `d256MulScaledFallback` admitting via `d256MulResultFits`
   (`arith_decimal_fast.go:2193–2218`) — still enforces the
   `DECIMAL(65, s)` digit domain inside the kernel, e.g. rejecting the
   `10^70` rounded coefficient of `(65,40)×(65,40)` operands holding
   `10^15` although the value fits the carrier and its published
   `(76,40)` domain. The design replaces that one admission comparison
   with the inline path's exact physical criterion (rounded coefficient
   magnitude below `2^255`: upper limbs zero, sign-limb high bit clear).
   Acceptance strictly widens — every coefficient admitted by the old
   comparison (magnitude below `10^65`, plus its inclusive negative
   boundary) stays admitted — round-half-up, raw-overflow recovery and
   the pre-existing `Decimal256 Mul overflow` error family for genuine
   physical overflow are unchanged, and the inline and validator code
   paths stay byte-identical. The 76
   digit count is the *published metadata* cap (`min(w1+w2, 76)`), not the
   acceptance oracle: a 77-digit product whose coefficient stays below
   `2^255` — e.g. `(10^65−1) × 500000000000` ≈ `5.0×10^76` — publishes
   `(76,0)`, the shared post-kernel validator defers (width 76) and the
   value is returned.
   The product **scale rule is pre-existing and unchanged** by this design:
   `min(s1+s2, max(12, s1, s2))` in both the publication
   (`decimalMultiplyReturnType`, `pkg/sql/plan/function/list_operator.go`)
   and the batch kernel (`d256Mul`,
   `pkg/sql/plan/function/arith_decimal_fast.go`) — two scale-10 operands
   publish and compute scale 12, not 20, and `(65,65)×(65,65)` publishes
   `(76,65)` with the value rounded to scale 65.
2. **Declared storage domain** — what SQL DDL can declare and the catalog can
   persist or print: `DECIMAL(M,S)` with `M ≤ 65` (hence `S ≤ 65`), checked
   casts admitted on insert/update.

The repository already encodes this split in execution: the shared post-kernel
validator `checkDecimal256ResultPrecision` (`pkg/sql/plan/function/arithmetic.go:124`)
skips its per-row precision scan whenever the published result width is ≥76,
deferring to the kernel (pre-existing guard from the TPCH Decimal256 dispatch
designs `docs/design/tpch-decimal-multiply-dispatch-fix.md` and
`tpch-decimal-multiply-range-proofs.md`). The base defect is that *result-type
inference* never published ≥76, so the declared-domain check still fired on
expression results. The design therefore changes inference ownership **plus one focused
kernel repair** (revision 5, F-FALLBACK-DOMAIN — revision 4's "changes
inference ownership, not arithmetic" premise missed the scaled fallback's
declared-domain admission, which this design removes): **expression
domain at binding/publication, declared domain at the storage boundary,
kernel as the sole physical-overflow authority on every multiplication
path (inline, scaled-generic and scaled-fallback alike).**

Success criteria (measurable):

- Both issue statements return exact 76- and 66-digit products.
- No query that succeeded on base returns a different value or loses a
  previously persistable schema/value domain. One named exclusion
  (revision 43, decision log 89): a value the base materialized through
  the pre-existing internal-route append-path coefficient-passthrough
  defect — the raw coefficient written under the target column's scale
  without rescaling — on a prepared set-operation fill whose session
  `div_precision_increment` differs from the internal plan build's
  increment is defect output, not a compatibility oracle; the intended
  value there is the base-evaluated expression value correctly
  scale-aligned into the recovered persisted domain (the base's own
  `0.3333` stored as `0.3333000000` in the `DECIMAL(75,10)` column
  instead of the passthrough `0.0000003333`), with the persisted
  schema tag and the statement's success identical on both engines.
- Persisted schemas for widened multiplication stay declarable (SHOW
  CREATE round-trips); genuine non-carrier wide domains keep their
  parent-identical persisted form; and every out-of-domain value
  admission is a checked error, never silent loss.
- Only multiplication publishes above the declared domain; the boundary is
  provably the kernel's own overflow for expression results and a checked
  `20301`/1690-family admission for storage.

## 2. Invariants and first owners

- **I1 — kernel authority.** For expression results, physical overflow is
  decided only by the Decimal256 kernel (and its documented post-kernel
  validator's ≥76 fast path). SQL-layer inference never rejects a value the
  carrier holds — and neither does any admission check inside the multiply
  kernel itself: the scaled fallback's declared-domain comparison
  (`d256MulResultFits`, `arith_decimal_fast.go:2206–2218`) is replaced with
  the inline path's exact physical criterion (revision 5,
  F-FALLBACK-DOMAIN), so high-scale and raw-overflow-recovery products
  face the same sole authority as inline products.
  Owners: `decimalMultiplyReturnType`,
  `pkg/sql/plan/function/list_operator.go:228`; the fallback admission,
  `pkg/sql/plan/function/arith_decimal_fast.go:2193–2218`.
- **I2 — no carrier expansion in persisted schemas or catalog-visible
  metadata.** The declaration-width expansion that widened multiplication
  introduces for expression results never reaches ANY persisted or
  catalog-visible surface: CTAS table columns, prepared-CTAS columns, and
  **view column records** (CREATE VIEW / ALTER VIEW / DESCRIBE / replay).
  After the boundary, any column whose persisted width still exceeds 65 has
  a non-carrier envelope that provably requires it — the identical domains
  the parent's pre-existing inference persisted with the same values and
  admission profile (see I4). The ONE
  deliberate exception is the §4.1 LAG/LEAD default-merge expansion band
  (admitted in revision 34; persistence restated in revision 40,
  F-SHUTDOWN-UPGRADE-COMPATIBILITY, decision log 86 (ii)): an admitted
  band merge persists its reconcile-derived tag — the parent derives no
  schema for these shapes, rejecting the derivation at planning — as a
  plain catalog record in the parent's own above-65 form carrying
  exactly the persisted-protocol requirement the unchanged
  pre-existing channel computes for the same statement — this change
  mints no issue-specific marker (§6 M4, §8; revision 41, decision
  log 87 (i)). Every
  other inferred column persists with
  exactly the declared tag the parent's own inference would have authored —
  including widths below 65 (e.g. `DECIMAL(64,2)` from
  `ROUND(carrier_product, 2)`) — which parses and round-trips.
  Owners: `decimalMulStorageBoundary`, `pkg/sql/plan/build_ddl.go:2334`,
  called from CTAS DDL authoring (`:2535`) and prepared-CTAS realignment
  (`:2470`, `RefreshPreparedCTASInferredColumns`, reading the retained
  decision ledger carried from prepare to execute — revision 17,
  F-DECISION-EVIDENCE-LIFECYCLE, decision log 63(ii), publication revised to the real prepared-DDL path in revision 18 (F-EVIDENCE-PUBLICATION-PATH, decision log 64 (i)-(iii)), its channels completed through ordinary prepared-plan rebuilding in revision 19 (F-REBUILD-EVIDENCE-PUBLICATION, decision log 65 (i)-(ii)), its witness oracles and rebuild validation repaired in revision 20 (F-MISS-ORACLE-COLLAPSE/F-REUSE-WITNESS-REACHABILITY, decision log 66), its execute-route oracles and entry-path probe corrected in revision 21 (F-EXECUTE-CHANNEL-UNREACHABLE, decision log 67), its skip claims scoped to type-preserving fills with the provisional common-VALUE execute rebind given its decision-evidence lifecycle in revision 22 (F-VALUE-ARM-REBIND, decision log 68), the execute rebind's producer lineage completed in revision 23 (F-SUPPLY-PRODUCER-DOMAINS, decision log 69) with its shared-producer SourceStep edge coverage completed in revision 24 (F-SOURCESTEP-LINEAGE, decision log 70) with its set-operation branch-domain combination completed in revision 25 (F-MULTIPRODUCER-DOMAINS, decision log 71) and its set-operation coercion-source recovery completed in revision 26 (F-SETOP-ALIGNMENT-RECOVERY, decision log 72, its set-operation common-domain reconvergence completed in revision 27, F-SETOP-COMMON-RECONVERGENCE, decision log 73), and the view
  column record assembly inside `inferViewColumns` (`build_ddl.go:240`)
  — itself the standalone view-generation owner since revision 17, the
  generation spanning its funnel call and this assembly (decision log
  63(i)) — the one shared assembly serving CREATE VIEW, ALTER VIEW,
  description and replay (§6).
- **I3 — checked admission.** A value outside the persisted column's declared
  domain fails admission with a checked error (cast `20301` family / 1690 at
  evaluation); no truncation, no silent width escape into storage.
  Owner: pre-existing typed cast/insert evaluation of the declared column
  type; the boundary guarantees the stored tag is declarable so this check
  is both well-defined and sufficient. Views own no storage and admit no
  values, so no admission exists at the view seam (§6); CTAS owns admission
  for its columns.
- **I4 — wide-inference preservation.** Any domain the base could return or
  persist (genuine wide literals, binder alignment casts, mixed-scale
  merges, stored wide columns, pre-existing inference above 65 created by
  branches dominated by an already-wide operand) keeps its pre-existing
  value domain and declared type exactly. The boundary never narrows below
  the declared domain or below the expression's proven non-carrier envelope.
  The §4.1 expansion band is outside this invariant: its persisted tags
  are reconcile-derived admissions the base never authored (I2's single
  exception), fenced per §6 M4; every domain this invariant names keeps
  its parent form exactly.
  Where a shape's provenance is provably unresolvable the design chooses a
  documented default tier (§4.2), never an ad-hoc guess. The value-merge
  families converge a widened operand's common domain exactly as the
  parent converged it: a merge that overflows the carrier retries over
  the operands' parent-published types obtained through the
  binder-owned source-domain channel (revision 12,
  F-COMMON-DOMAIN-OVERFLOW; re-placed in revision 13,
  F-RECONVERGENCE-CHANNEL; §3 step 1, §4.1). The same channel
  additionally resolves the current query block's own pending
  producers — a `ColRef` bound against `aggregateTag` descends
  `BindContext.aggregates[colPos]` and one bound against `windowTag`
  descends `ctx.windows[colPos].WindowFunc` (revision 14,
  F-AGG-WINDOW-CHANNEL; decision log 57). The revision-15
  inventory completes the block's pending producers: one bound
  against `groupTag` descends `BindContext.groups[colPos]` — the
  retained bound group key (`group_binder.go:272–290`), its output
  type the parent's own bind read with `groupOutputType`'s
  grouping-set nullability adjustment (`types.go:1093–1098`) — one
  bound against `sampleTag` descends
  `ctx.sampleFunc.columns[colPos]`, and one bound against
  `projectTag` descends `ctx.projects[colPos]` (revision 15,
  F-GROUPED-CHANNEL; decision log 59).  One bound against `timeTag` descends `ctx.times[colPos]` — an `Expr_Col`
  entry is a timestamp boundary self-reference resolved irrelevant by
  construction; an `Expr_F` entry is the retained TIME_WINDOW-routed
  aggregate output (`BindTimeWindowFunc`, `having_binder.go:979–1003`),
  recursing uniformly with its `ColRef{aggregateTag, aggColPos}` arguments
  descending `ctx.aggregates[aggColPos]` and the parent's registered result
  rule deriving the output's parent type (revision 16, F-TIMEWINDOW-CHANNEL;
  decision log 61).
  Owner: `ctasStorageDomain` envelope rules, `pkg/sql/plan/build_ddl.go:1795`,
  plus the binder-owned source-domain resolver and retry funnel in
  `pkg/sql/plan/base_binder.go` gating the reconvergence families'
  `GetFunctionByName` lookups (`base_binder.go:8323–8327`; the
  revision-12 placement at the merge check call sites —
  `pkg/sql/plan/function/type_check.go:1074` with `func_binary.go:883`,
  `operatorSet.go:559`/`:896`,
  `func_builtin_leastgreatest.go:376`/`:681` — is SUPERSEDED,
  decision log 56: the check contract receives a pure `[]types.Type`
  vector, `function/function.go:1161`, and a reference operand's
  producing expression is unrecoverable from its surface type).
- **I5 — sound, bounded evaluation.** Storage-envelope evaluation is a
  deterministic, request-scoped, demand-driven memoized walk over
  (producer node, slot) keys: every slot's envelope is computed at most
  once and reused from the memo afterwards, recursive-CTE union slots are
  seeded with their anchor-member envelope (the parent's own typing rule:
  recursive CTE columns are typed from the anchor), and no
  recursive-member slot is envelope-evaluated for the union's persisted
  schema — member admission stays value-based through the parent's own
  member-to-anchor cast coercion, and back-references close on the seed
  whenever a demanded tree reads one (§4.3). There is no
  iteration and no fixpoint, so per-shape base-projections (e.g. the
  LAG/LEAD reconcile clamp) are applied exactly once per expression, never
  compounded against themselves; total per-slot work is one local
  expression walk, and the total bound is the sum of local tree sizes.
  An evaluation counter is exposed for deterministic traversal-count
  tests (each slot exactly once; a shared diamond adds no second walk).
  Owner: `ctasStorageDomain` memo/evaluator,
  `pkg/sql/plan/build_ddl.go:1716` area.
- **I6 — decimal aggregate args in the base-equivalent domain, zero
  executor deviation.**
  Decimal aggregate result metadata and capacity come from the
  pre-existing registrations (`aggexec.AvgReturnType`,
  `pkg/sql/plan/function/list_agg.go:158`, formula
  `avgDecimal256ReturnType`; `aggexec.SumReturnType`'s `min(w+22, 65)`;
  both `pkg/sql/colexec/aggexec/sumavg2.go`) applied to the argument's
  **base-equivalent domain (the §4.3 envelope)**, with **no change to the
  executor, no hand-authored result type (the pass re-derives the
  authored type through the same registration the executor re-derives
  from), and no width- or
  provenance-based classification of aggregate inputs** — the aggregate formula
  itself stays the parent's single function of argument type
  (`pkg/sql/colexec/aggexec/sumavg2.go` and its test are reverted
  byte-identically to base; the round-10 Rule A and the revision-1/2/3
  binder result-type seams are withdrawn — decision log 7/13/18).
  What revision 3 missed: widening changed the *argument type the formula
  consumes*, so a pure function of the published type newly rejected
  previously successful averages (a `(76,0)` argument yields `(65,4)`
  where the parent's `(65,0)` argument yielded `(65,0)` — a `10^62`
  average the parent returned no longer fits the derived result domain)
  and changed previously successful rounding and persisted AVG schemas.
  The shared planner↔executor contract is therefore the argument
  expression's bound type itself: in the post-flatten final-plan
  reconciliation pass at the end of `createQuery` (§5), every decimal
  aggregate argument whose §4.3 envelope differs from its published type
  is wrapped in an ordinary plan-expression cast to the envelope domain
  (the same cast machinery COALESCE/recursive-member alignment already
  uses); planner authoring and every executor construction path then read
  the same argument type — the parent domain — verbatim. The pass
  placement is where §4.3's positional (NodeId, slot) addressing is
  defined (revision 5, F-BINDING-PHASE): binding-time BindContext tags,
  pending grouped/window outputs and unflattened scalar subqueries have
  no positional form inside the envelope evaluator. The trigger subject
  is the **domain-arithmetic decimal aggregates `avg` and `sum`**
  (revision 7, F-SELECTOR-AGGREGATES narrowing the revision-6 uniform
  trigger): they are exactly §4.2 class (ii)'s aggregate membership —
  the only aggregates whose registered result is **domain arithmetic**
  over the argument type across the envelope/published seam (the AVG
  formula consumes argument width; SUM derives `min(w+22, 65)`) —
  machine-checked by check two (revision 8, F-TRANSPARENT-CONSUMERS:
  the value-selecting aggregates' results differ across the seam too,
  but only by transparent propagation, and the two families are
  classified separately there). Revision 6
  cast every divergent decimal aggregate argument uniformly, but
  value-selecting aggregates — `min`, `max` (`ReturnFirstArgType`,
  `list_agg.go:103`/`:120`), `any_value` (`:370`), `max_by` (`:717`),
  `percentile_disc` (`args[0]`, `aggexec/ordered_percentile.go:151–156`)
  — return one of the input rows unchanged at the argument type, so the
  cast narrowed selected carrier values and rejected carrier-fitting
  ones before selection, contradicting §4.1's transparent-selector
  inventory, §6 M5's exact-rows/checked-at-CTAS contract and MySQL's
  legitimate selected values; selecting and non-decimal-result
  aggregates therefore keep the carrier argument cast-free (their
  base-equivalence holds without a cast: identical scale between
  published and envelope, so previously successful queries keep
  identical values and types — the abs/§4.1 precedent covers the
  widened SELECT metadata). The revision-5 gate (fire only where the
  registered results also differ) stays withdrawn for the admitted
  class: registered result-type agreement between two bound domains is
  NOT admission equivalence — `avgDecimal256ReturnType` derives `(65,4)`
  for both `(76,0)` and `(66,0)`, yet the domains admit different value
  sets, and only the envelope domain is the one the parent's own
  multiplication publication admitted (revision 6,
  F-ADMISSION-CONTRACT). Inside every admitted AVG/SUM argument the
  pass casts **every divergent decimal seam to its own envelope,
  post-order inner-first** (revision 7, F-PRODUCT-ADMISSION): the
  widened multiply's own seam reproduces the parent's multiply-time
  admission (`checkDecimal256ResultPrecision` checks the
  multiplication's own published width, `arithmetic.go:121–150`), which
  the merged argument-root cast alone could not (`(66,0)` admits the
  ±`10^65` products the parent rejected). Seam discovery is
  **head-independent** (revision 8, F-HEAD-GATE): every decimal-typed
  subexpression of every decimal AVG/SUM argument whose own envelope
  differs from its own published type receives the cast, whether or not
  the argument head diverges (a `COALESCE(a*b, <76-digit literal>)`
  argument merges to published `(76,0)` = envelope `(76,0)` — no head
  divergence — yet its product seam still diverges and casts). Where
  reference resolution places a divergent seam inside a **materialized
  relational producer** (shared-CTE reuse, `cte_reuse.go:41–64`),
  admission crosses the boundary per the row-shape-and-scope taxonomy
  (revision 8, F-PRODUCER-BOUNDARY; revised in revisions 9–10,
  F-COMPANION-DEMAND/F-ROW-CHANGING-BOUNDARY then
  F-GUARD-SEMANTICS/F-PRUNED-PROJECTION-DEPS; scope-decomposed with
  consumer composition in revision 11, F-SCOPE-SPANNING-ARGUMENTS):
  the boundary exists only past the complete materialization admission
  gate (fact 18 — occurrence, profitability, drain, demand and
  storage, the last charged on the shadow-expanded schema), and on a
  row-preserving path
  the producer admits each (producer, referenced exported slot) whose
  producing expression carries a divergent seam through ONE OF TWO
  channels, selected per slot by POSITIVE replay-safety proof over every node
  of the slot's producing expression (fact 15; revision 31,
  F-REPLAY-SAFETY-POSITIVE-PROOF, completed with the
  diagnostic-freedom tier in revision 32, F-DIAGNOSTIC-REPLAY).
  (PROVEN) a slot whose producing
  expression is proven replay-safe emits ONE
  statement-local **envelope-shadow column** — a structural replica of
  THAT SLOT'S OWN producing expression with each producer-representable
  divergent seam cast in place and its decisions replayed from the
  decision ledger, evaluated by the executor's own short-circuit
  machinery with the base engine's masks and three-valued selection
  (nothing evaluates on rows the base engine excludes, an unselected
  overflowing branch leaves the statement successful, and a selected
  violating row raises the parent's multiply-time family at the
  parent's producer demand — fact 15); the replica's deterministic
  re-evaluation is value-identical to the original export row for row,
  and the original column stays carrier-exact for sibling readers.
  (UNPROVEN) a slot whose producing expression is NOT proven
  replay-safe emits NO shadow: a replica would own an independent
  executor (`evalExpression.go:269–275`) and evaluate the expression
  a second time — re-drawing a volatile `rand()`/`uuid()` and
  re-issuing an effectful unlisted call's effects (`llm_chat`'s
  per-row HTTP request) — publishing values no sibling reader of the
  same export observes and duplicating observable effects, breaking
  the single-publication contract (fact 15). Admission instead moves
  INTO the original
  export: each producer-representable divergent seam of that slot's
  producing expression takes its envelope cast IN PLACE, the export is
  evaluated exactly once by the producer's own machinery with the base
  engine's masks (demand equals the parent's producer demand exactly
  as fact 15 states — an unselected overflowing arm admits nothing),
  and the slot's published type is unchanged. Every consumer of that
  export — the admitting AVG/SUM readers and the value-selecting
  siblings alike — reads the SAME produced values: fitting rows read
  carrier-exact admitted values identical to the parent's; a selected
  violating row raises the parent's multiply-time family at the
  in-place cast while the producer materializes, so the whole
  statement fails on either engine (the parent rejects the same
  statement at the multiply); a statement with no admitting AVG/SUM
  reader of the slot splices nothing and evaluates exactly as before.
  The in-place casts carry the design-owned cast identity (fact 17),
  so the section-4.1 envelope mirrors recover through them and the
  recomposing consumer recognizes the admission instead of recasting.
  In BOTH channels the admitting AVG/SUM consumer RECOMPOSES its
  argument over the exports: each slot admitted through a shadow
  rebinds as a direct reference to the exported shadow slot, each slot
  admitted in place is read directly from the original export with its
  seams already admitted, while argument structure reading another
  relation, another producer's export or a captured input stays in
  tree at the consumer with its consumer-side seams cast in place
  (fact 16 — every input of a slot's producing expression is
  producer-side by construction, so no replay dependency crosses the
  boundary, and a scope-spanning argument such as
  `AVG(COALESCE(c.v, u.d))` is admitted in both scopes with no
  producer-side replica of consumer-side structure);
  where a row-changing operator intervenes inside the producer (a dedup
  step, grouped or window aggregation, a set operation — fact 16), no
  per-row shadow can cross, and the seam instead takes its ordinary
  in-tree envelope cast at its locus below the FIRST such operator,
  gated on an admitting AVG/SUM reader downstream. The same
  pass also propagates
  its mutations through the plan's copied types by a single
  **dependency-ordered finalization** over the producer/consumer/copy
  DAG (revision 6,
  F-DOWNSTREAM-PROPAGATION; revision 7, F-COERCION/F-SYNC-ORDER/
  F-DEPENDENT-EXIT; revision 8, F-INPUT-REFRESH/F-DEPENDENT-ENVELOPE):
  each producer slot finalizes exactly once, every referencing copy
  (projection/reference/TIME_WINDOW/fill/SINK/SINK_SCAN types and the
  correlation channel's `InputTypes`, slot types, `OutputTypes` and
  dependent-call result type) refreshes immediately at its producer's
  finalization — before any consumer that reads it re-derives — and
  each enclosing consumer re-derives through its registered rule,
  replaying its coercive binding decisions over recovered pre-coercion
  operands at their final envelope types (decision-owned alignment
  layer regenerated, revision 9 F-REPLAY-RECOVERY; SQL-authored and
  fixed-target casts preserved), all before executor construction, and
  the whole owner runs at both
  `createQuery` exits — the dependent route included (§5).
  Because a widened publication differs from its envelope only in the
  width the multiplication seam added, the cast is value- and
  scale-preserving for every value the parent admitted: every
  base-successful AVG keeps its exact parent metadata and value (a
  fitting `10^62` product still averages `10^62` typed `(65,0)`;
  `AVG(2*3)` stays `6`, not `6.0000`), and genuine wide arguments keep
  their parent results with no cast at all (envelope == typed:
  `(66,0)` → `(65,4)`, `1.5000`). Inside the admitted argument, values
  outside a widened multiply's parent publication fail that inner seam
  cast with the parent's multiply-time admission family — reproducing
  the parent's rejection per source, before any branch merge (§5);
  remaining values outside the rebound head domain raise the parent's
  declared-domain checked cast family at the head — the admission the
  parent's own
  multiplication publication performed for those same values; an average
  that still does not fit its `(65, s)` result domain raises the
  pre-existing checked division/result-capacity failure family retained
  byte-identically by the executor.
  Owners: the single registration sites (§5) — planner registry and every
  executor construction path (group, window, reconstruction) — and the
  aggregate-argument envelope rewrite in the final-plan reconciliation
  pass (§5), sharing the §4.3 evaluator.
- **I7 — folding provenance.** Constant folding never erases the
  multiplication provenance the storage boundary needs: trees carrying a
  >65-width decimal multiplication keep their executable expression shape;
  folds of genuine non-multiplication domains keep materializing. The
  decline guards every destructive fold shape — valued, NULL-producing
  and enveloping — not only scalar-vector materialization (revision 12,
  F-NULL-FOLD-PROVENANCE). The statement-local DECISION BARRIER
  additionally keeps every provenance-flagged recorded decision's
  operand cast syntax unfolded until pass-entry capture (revision 13,
  F-LEDGER-COLLISION; decision log 55), consulted at BOTH fold engines'
  recursive entries through one registry carried on the statement's
  compile-time `process.Process`, so a copy and a shadow replica find
  the same preserved structure and the parent-successful folded-twin
  statement never degrades into a planning failure. The registry's
  planning-generation lifecycle is owned by one carrier-provided
  enter/release pair at both outermost planner entries — `BuildPlan`
  (`pkg/sql/plan/build.go:721`) and `(*BaseOptimizer).Optimize`
  (`pkg/sql/plan/optimize.go:54`): nested entries share the active
  registry through a depth marker, and a defer-registered release
  restores the exact prior field value when the outermost entry
  returns, on success, error and panic unwind alike, so the reused
  session process never carries one statement's decisions into the
  next (revision 14, F-REGISTRY-LIFECYCLE; decision log 58).
  Owner: the recursive fold walk entries `foldExpr`
  (`pkg/sql/plan/rule/constant_fold.go:138`) and the sibling engine
  `constantFoldWithPreparedExactSource` (`pkg/sql/plan/utils.go:1888`),
  with `MaterializeDecimal256Constant` (`:530`) retained as inner
  defense, the registry carrier in `pkg/vm/process`, and the
  generation install/restore owner at the two outermost planner
  entries `BuildPlan` (`pkg/sql/plan/build.go:721`) and
  `(*BaseOptimizer).Optimize` (`pkg/sql/plan/optimize.go:54`).
- **I8 — no format/protocol/state change; bounded, documented execution
  cost.** No catalog, plan-protobuf, wire, or on-disk format change; no
  background worker, cache, queue, scheduler, or other long-lived state.
  No new expression feature, protocol floor or issue-specific marker is
  minted (revision 40, F-SHUTDOWN-UPGRADE-COMPATIBILITY, decision log
  86 (ii); scoped in revision 41, decision log 87 (i)):
  the pre-existing `ViewData.RequiredProtocolVersion` field, its gates
  and the v72–v134 floor allocations keep their base behavior untouched —
  the unchanged channel recording exactly what it computes on base for
  each statement, the v89 wide-literal requirement included (revision
  41, decision log 87 (i)) — and revision 6's mixed-version apparatus
  and revision 34's band marker are withdrawn, so no new format, field
  or gate is introduced.
  `ctasStorageDomain` (including its memo) is built per DDL statement and
  discarded; the aggregate reconciliation pass holds only statement-local
  memo state (§5). Cost model (revision 5, F-PERF-PREMISE — the earlier
  "inference only, hot SELECT unchanged" premise was false): plans without
  widened decimal multiplication provenance are skipped structurally — no
  envelope walk, no cast, no measurable SELECT delta. Plans that carry it
  pay (a) one statement-scoped envelope walk at planning (linear in the
  plan DAG, §4.3, including the finalization pass's per-plan
  coercion replay, copy refresh, shadow-or-locus-cast
  construction and shadow-expanded admission charging (fact 18)), and (b) at
  execution, one same-scale
  decimal capacity
  cast per non-NULL input row of each divergent seam inside a rebound
  AVG/SUM argument (bare-product arguments pay one cast per row — seam
  and head coincide; mixed-origin arguments pay one cast per divergent
  arm; value-selecting aggregates pay nothing, revision 7), plus — only
  where a divergent seam sits inside a materialized producer with an
  admitting AVG/SUM reader — either one extra statement-local
  envelope-shadow projection column per admitted exported slot at the
  producer on a
  row-preserving path (an ordinary replica of that slot's producing
  expression whose per-row demand replicates the slot evaluation plus
  the in-place seam casts, evaluated under the executor's own masks by
  construction —
  already
  counted in (b); sibling readers pay nothing) or, where a row-changing
  operator intervenes inside the producer, the same per-row cast moved
  in tree to the seam locus below the first such operator with no extra
  column at all (revision 10; per-slot granularity in revision 11) — the
  ordinary same-scale cast path formats and re-parses each coefficient
  (`decimal256ToDecimal256`,
  `pkg/sql/plan/function/func_cast.go:6980–7030`; the `(76,s)→(65,s)`
  domain change bypasses the duplication fast path at `:1456–1468`; a
  behavior-identical limb-comparison capacity kernel may substitute as a
  semantics-neutral optimization at implementation time). Queries the
  base rejected (the issue contract) have no parent baseline; previously
  successful aggregate-over-product queries gain only (b). The §9
  performance row states the acceptance budget and the
  parent-versus-candidate benchmark plan (run at implementation time).
  The constant-fold decline still affects only constant trees that carry
  multiplication provenance — from revision 12 at the fold walk's
  recursive entry, covering NULL-producing folds too (the unchanged
  planner micro-optimization trade of §10). The revision-13 decision
  barrier consults one membership registry at both fold engines'
  entries: a statement whose recorded decisions carry no
  provenance flag pays an O(1) empty-set check per fold entry and
  folds base-identically, and a statement with flagged decisions
  trades the fold micro-optimization on exactly those recorded
  operand subtrees (values unchanged; §10; decision log 55). The
  revision-14 generation lifecycle (F-REGISTRY-LIFECYCLE; decision
  log 58) adds one field save-and-install and one defer-registered
  restore at each of the two outermost planner entries plus one
  depth-counter update per nested entry — O(1) per planning
  generation with no per-expression or per-row component, paid by
  every statement on the reused session process.

Design triggers (required by `references/feature-design-review.md` §1):
size — the complete change edits ≈700 nonblank, non-comment production lines
across 12 production files (`build_ddl.go` +585 alone, against the review's
≥500 default trigger); complexity — it restructures precision ownership
across binder (`base_binder.go`), function publication (`list_operator.go`),
constant folding (`rule/constant_fold.go`), DDL schema authoring
(`build_ddl.go`), the aggregate domain contract and its final-plan
reconciliation pass (§5, `query_builder.go`), the multiply kernel's
scaled fallback admission (`arith_decimal_fast.go`);
revision 40 withdraws the remote execution admission surface (feature
analysis `string_literal_form.go`, floor and scope demotion
`expression_protocol.go`, destination validation `remoterun.go`, the
minted constant `defines/const.go`) from the planned set, i.e.
two or more ownership boundaries; revision 7 adds the dependent-query
exit owner (`correlation_binding.go`), making 13 production files. Revision 3 withdraws the AVG authoring
rule entirely (aggexec stays byte-identical to base, §5), replaces
revision 2's least-fixpoint dataflow with the memoized one-shot walk
(§4.3) and removes the boundary floor; revision 4 keeps those choices
while binding aggregate decimal arguments in their base-equivalent domain
(§5), widening the boundary trigger to every affected expression domain
(§4.3) and aligning recursive-member admission and outer-view expression
contracts with the parent's own seams (§4.3/§6); revision 5 keeps all of
these and adds the kernel fallback admission repair (§1/§2 I1), the
capped-consumer envelope mirrors and generalized aggregate binding
(§4.1/§4.2/§5), the final-plan pass placement (§5), and the corrected
execution-cost model (§2 I8/§9); revision 6 keeps all of these and adds
the reconciliation propagation sweeps (§5 P1–P3), the uniform
aggregate-admission trigger (§5) and the remote execution admission
floor (§3 step 6/§8); revision 7 keeps all of these and narrows the
aggregate-admission trigger to the domain-arithmetic class `{avg, sum}`
with per-source seam admission and selector-exact exemption (§5),
replays implicit binder coercions inside rebuilt trees (§5), reorders
the propagation sweeps dependency-first (§5 S1–S3, superseding the
revision-6 P1–P3 order) and covers both `createQuery` exits
(§5/§3 step 4); revision 8 keeps all of these and replaces the three
global sweeps with one dependency-ordered finalization protocol with
just-in-time copy refresh and correlation-channel closure (§5),
moves seam discovery off the argument head gate (§5), adds the materialized-producer envelope-shadow admission seam (§5, new
planning-side code in `query_builder.go`'s finalization owner — the
same production file), scopes coercion replay to decision-owned
alignment layers (§5/§4.1), adds dependent/slot envelope rules
(§4.1/§4.3 — same `build_ddl.go` owner) and reclassifies the
exhaustiveness oracle into transparent-versus-domain-arithmetic
families (§4.2); revision 9 keeps all of these and makes
companion capture demand-exact under the executor's short-circuit arm
masks, splits materialized-boundary admission by row shape into guarded
companion columns and in-tree seam-locus casts, and recovers
coercive-decision operands for replay — all inside the same
`query_builder.go` finalization owner and the `build_ddl.go` evaluator
(the 13-file set is unchanged); revision 10 keeps all of these and
replaces the guarded companion with one structural envelope-shadow
replica column per admitted argument at the producer (demand and
three-valued selection inherited from the executor's own evaluator;
the admitting consumer rebinds to the exported shadow slot), orders
coercion-decision recovery snapshot-first with original-layer
verification distinct from final-target regeneration plus an explicit
fold-recovery table, and re-bases the materialized-boundary witnesses
and their benchmark on deterministically profitable filtered producers
— all inside the same two owners (the 13-file set is unchanged); revision 11 keeps all of these and scope-decomposes the shadow per referenced exported slot with consumer-side composition for scope-spanning arguments, replaces fold-artifact recovery with the bind-time decision ledger, and charges the shadow-expanded schema to the materialization admission reservation before the share is committed — all inside the same owners (the 13-file set is unchanged); revision 12 keeps all of these and adds the value-merge common-domain reconvergence at the merge check call sites — `function/type_check.go` (the retry beside `setSafeDecimalWidthAndScaleFromSource`) with `func_binary.go`, `operatorSet.go` and `func_builtin_leastgreatest.go` joining the production set (17 production files) — makes the decision ledger copy-stable by content keying with positional folded-position ownership (the same `base_binder.go` recorder and `query_builder.go` finalization owner), and moves the fold decline to the fold walk's recursive entry (the same `rule/constant_fold.go` owner); revision 13 keeps all of these revision-12 choices except two placements it supersedes (decision log 55–56): the reconvergence retry leaves the merge check call sites — `function/type_check.go`, `func_binary.go`, `operatorSet.go` and `func_builtin_leastgreatest.go` exit the planned production set — for the binder-owned source-domain-channel resolver and retry funnel inside the already-owned `base_binder.go`, while the fold decline grows into the statement-scoped decision barrier gated at BOTH fold engines' recursive entries — the same `rule/constant_fold.go` owner and one new gate owner `pkg/sql/plan/utils.go` — fed by the planner-owned statement registry carried on the statement's `process.Process` (one new `pkg/vm/process` carrier file), i.e. 15 production files; revision 14 keeps all of these and adds the binder's tag/ordinal pending-producer resolution inside the already-owned `base_binder.go` resolver/channel corpus (no new reconvergence file) plus the planning-generation lifecycle owner for the statement registry invoked at the two outermost planner entries — `pkg/sql/plan/build.go` (`BuildPlan`, `:721`) and `pkg/sql/plan/optimize.go` (`(*BaseOptimizer).Optimize`, `:54`) join the planned production set, i.e. 17 production files; revision 15 keeps all of these and completes the binder's tag/ordinal pending-producer resolution to the full same-block result-tag inventory — grouped, sampled and projected producers — inside the already-owned `base_binder.go` resolver (no new reconvergence file), and the planning-generation lifecycle owner gains its third invocation at the shared SELECT funnel `bindAndOptimizeSelectQueryWithValidatorAndCapture` (`build.go:67`) inside the already-owned `pkg/sql/plan/build.go`, so the planned production set stays 17 production files. Revision 16 keeps all of these and resolves decimal TIME_WINDOW pending-producer references through the binder's `timeTag` channel and recovers decision-owned pre-alignment operand domains for storage-envelope evaluation — both inside the already-owned `base_binder.go` resolver and the already-owned `build_ddl.go` envelope evaluator over the already-carried statement registry — so the planned production set stays 17 production files; revision 17 keeps all of these and moves the standalone view-generation owner one level out from the shared funnel to `inferViewColumns` (`build_ddl.go:240`), the generation now holding through the post-funnel view column record assembly (`:345–387`) that evaluates storage envelopes — a placement change inside the already-owned `build_ddl.go`, the funnel keeping decision log 60's invocation as the defensive standalone entry — and retains completed decision evidence from prepare to execute for prepared-CTAS refresh following the repository's `DecimalFloatProofs` carrier pattern, `pkg/sql/plan/prepared_binding.go`, `pkg/frontend/types.go` and `pkg/frontend/computation_wrapper.go` joining the planned production set (20 production files). Revision 18 keeps all of these except the one placement it supersedes (decision log 64): revision 17's bundle-and-cache routing never runs for a prepared CTAS (entry 64's verified PREPARE/DDL path), so the retained-evidence publication moves to that real lifecycle — `pkg/sql/plan/prepared_binding.go` exits the planned production set (its `BuildPlan`/`Optimize` invocations remain ordinary decision-log-58 generation entries) while `pkg/frontend/mysql_cmd_executor.go` joins it (`createPrepareStmtInSession`'s late claim into the new `PrepareStmt` retained-evidence field), `pkg/frontend/types.go` and `pkg/frontend/computation_wrapper.go` remaining — so the planned production set stays 20 production files. Revision 19 keeps all of these and completes the retained-evidence publication channels and the parameterized refresh witness (decision log 65): the ordinary prepared-plan rebuild channel (`rebuildPreparePlan`, `computation_wrapper.go:1537`) gains a claim ordered after the `:1563–1566` replacement commit and before the `:1640–1642` specialization-cache clearing, the carrier release decoupling from that clearing to the seed-plan generation's own lifetime, and the witness placing its parameter marker in the TRUNCATE head so specialization changes the inferred output type against the complete prepared cast tree — all inside the already-owned `pkg/frontend/computation_wrapper.go`, `pkg/frontend/types.go` and `pkg/frontend/mysql_cmd_executor.go`, so the planned production set stays 20 production files. Revision 20 keeps all of these and repairs the witness oracle and the rebuild validation (F-MISS-ORACLE-COLLAPSE and F-REUSE-WITNESS-REACHABILITY, decision log 66): the parameterized refresh witness gains a decision-owned width-≤65 alignment-cast fixture whose empty-ledger ablation genuinely differs from consumption, and the rebuild validation splits into a reachable per-execute-rebuild witness under the preserved pre-existing CTAS rebuild policy and a cached-generation-reuse witness whose preconditions are asserted before its no-rebuild probe — §9 fixtures, oracles and UT/BVT witnesses only, so the planned production set stays 20 production files. Revision 21 keeps all of these and corrects the witness's execute-route oracles (F-EXECUTE-CHANNEL-UNREACHABLE, decision log 67): implementing revision 20 verified that no execute route re-derives a prepared DDL plan's inferred output types and that the pre-existing per-column refresh skip compares the filled query's output type against the original query's output type, so the execute-route refresh evaluation is unreachable for widened-multiplication fixtures and the parent's own EXECUTE persists its PREPARE snapshot tag — §9 oracles, witnesses and the public-contract projection only, so the planned production set stays 20 production files. Revision 22 keeps all of these and wires the provisional common-VALUE execute-rebind channel into the retained-evidence lifecycle (F-VALUE-ARM-REBIND, decision log 68): `ResetParamRefRule` carries the execute scope's process (a new field set at its `utils.go:6678` construction) so `bindPreparedFuncExprImplByPlanExpr` passes it instead of nil (`base_binder.go:7448`–`:7449`), the pre-existing recording hook (`base_binder.go:8295`–`:8299`) capturing the rebound decisions into an execute-scope registry installed for the fill-and-refresh window, and the refresh argument merging retained-carrier entries under the execute registry's — all inside the already-owned `pkg/sql/plan/utils.go`, `pkg/sql/plan/base_binder.go` and `pkg/frontend/computation_wrapper.go`, with `pkg/sql/plan/visit_plan_rule.go` joining the planned production set (21 production files). Revision 23 keeps all of these and completes the execute-time rebind's producer lineage (F-SUPPLY-PRODUCER-DOMAINS, decision log 69): the resolution scope gains a plan-producer lineage view mapping consumer-side `(nodeID, relPos, colPos)` triples to retained producing expressions by the exact per-node indexing the projection-refresh envelope owner `resolveColumnType` already owns (`utils.go:8161`–`:8232`), attached around the same fill-and-refresh window on the three execute routes, the operand resolver gaining one view step before its surface fallback with out-of-view triples conserving today's decline — all inside the already-owned `pkg/sql/plan/utils.go`, `pkg/sql/plan/base_binder.go` and `pkg/frontend/computation_wrapper.go`, so the planned production set stays 21 production files. Revision 24 keeps that tally while delegating the execute-time lineage view's producer resolution to the already-owned storage-domain resolver in `pkg/sql/plan/build_ddl.go` (F-SOURCESTEP-LINEAGE, decision log 70), retracting the entry-69 closure as the resolution substrate with the funnel, read-only and decline contracts standing, so the planned production set stays 21 production files. Revision 25 keeps that tally while the lineage view's branch-wise multi-producer combination reuses the already-owned storage-domain resolver and the already-owned set-operation typing resolution inside `pkg/sql/plan/build_ddl.go` and `pkg/sql/plan/query_builder.go` (F-MULTIPRODUCER-DOMAINS, decision log 71) — no new owner, no new file — so the planned production set stays 21 production files. Revision 26 keeps that tally while the lineage view recovers each set-operation-owned alignment coercion's source through the already-owned coercion ownership predicate inside `pkg/sql/plan/utils.go` before the branch-wise combination (F-SETOP-ALIGNMENT-RECOVERY, decision log 72, its set-operation common-domain reconvergence completed in revision 27 through the same two owners, F-SETOP-COMMON-RECONVERGENCE, decision log 73) — no new owner, no new file — so the planned production set stays 21 production files. The
measured line count
shifts in both directions without changing either trigger's
verdict. Earlier rounds of this fix recorded an
ordinary-fix exemption for the query-local inference core; that exemption
does not cover the complete accumulated scope, and design review of the
complete change is required. Implementation review resumes only after this
document's PASS.

## 3. End-to-end data and control flow

1. **Binding/publication.** Decimal multiplication publishes
   `DECIMAL(min(w1+w2, 76), s1+s2 (clamped by the pre-existing scale rule))`
   for expression results. SQL cannot author a decimal width >65, so plan
   widths above 65 arise from exactly three sources: multiplication
   publication (carriers), binder-generated alignment casts (mirroring
   `setSafeDecimalWidthAndScaleFromSource` merges), and genuinely wide
   literal/branch-dominated value domains (a 66-digit literal types itself
   (66,0)). The LAG/LEAD
   default-type reconciliation (`reconcileLagLeadDefaultType`,
   `pkg/sql/plan/base_binder.go:9147`) caps the shared expression result at
   the carrier limit 76 instead of the declared 65; persisted admission of
   its values still happens at the storage boundary.
   The value-merge families (`coalesce`/`ifnull`, `case`, `iff`,
   `least`/`greatest`) converge a widened operand's common domain
   exactly as the parent converged it (revision 12,
   F-COMMON-DOMAIN-OVERFLOW): their shared merge
   (`setSafeDecimalWidthAndScaleFromSource`,
   `type_check.go:1074–1123`, called from `func_binary.go:883`,
   `operatorSet.go:559`/`:896`, `func_builtin_leastgreatest.go:376`/`:681`)
   computes `requiredWidth = maxIntegralWidth + maxScale` over the
   operand types, and a widened publication can push that beyond the
   76-digit carrier although the parent — whose product published
   `(65, s)` — merged successfully. On that failure, and only where an
   operand carries widened-multiplication provenance, the binder's
   source-domain funnel retries the family's own function lookup over
   each operand's parent-published type (revision 13,
   F-RECONVERGENCE-CHANNEL — the revision-12 call-site placement is
   superseded: a merge checker consumes only `[]types.Type`,
   `function/function.go:1161`, so provenance and parent domains are
   resolved where argument expressions are still visible, `base_binder.go`):
   per operand the resolver walks DIRECT expression trees against the
   statement-local decision ledger (a recorded widened multiplication
   publication yields its parent type from its entry's recorded
   pre-coercion operand types via the §4.1 mirror — parent-unique for
   scale-0 surfaces since `min(76, 65)` is unconditional), threads a
   producer REFERENCE (derived-table and view columns, CTE columns
   including their materialized scans, query-level aliases) through a
   planner-local per-column channel on the producer's `Binding`
   (populated at producer-binding construction from the producing
   expression's recorded signature — the `headingProvenance`
   precedent, `bind_context.go:106–134`/`:510–562`, cloned and
   merged by the same rules, never serialized), and descends into a
   retained scalar-subquery projection head (`Expr_Sub`,
   `base_binder.go:1010–1031`, structural until `flattenSubqueries`),
   and resolves the current query block's own PENDING producers: a
   `ColRef` bound against the block's `aggregateTag` descends
   `BindContext.aggregates[colPos]` — the retained aggregate call
   (`having_binder.go:480–499`) — and a `ColRef` bound against
   `windowTag` descends `ctx.windows[colPos].WindowFunc` — the
   retained window call (`window_binder.go:1187–1195`,
   `:1444–1453`) — recursing uniformly into the producing
   expression's argument trees and deriving the producer output's
   parent type by applying the parent's registered result rule over
   the resolved parent argument domains (revision 14,
   F-AGG-WINDOW-CHANNEL; decision log 57), and the revision-15
   inventory completes the block's pending producers (revision 15,
   F-GROUPED-CHANNEL; decision log 59): a `ColRef` bound against
   `groupTag` — minted by the projection and HAVING binders
   (`projection_binder.go:56–65`, `having_binder.go:55–66`) —
   descends `BindContext.groups[colPos]`, the retained bound group
   key (`group_binder.go:272–290`): the group output IS the
   retained producing expression, so the descent recurses uniformly
   with channels (a)–(c), no registered result rule applies, and
   the parent-published group output type is the parent's own bind
   of that retained expression, read through the ledger, with
   `groupOutputType`'s grouping-set nullability adjustment
   (`types.go:1093–1098`) applied on top — an adjustment depending
   only on the retained expression and `groupingFlag`, both
   parent-invariant; a `ColRef` bound against `sampleTag`
   (`build_sample.go:76`/`:96`, beside the `{groupTag, sampleTag}`
   binding pair at `:137`) descends
   `ctx.sampleFunc.columns[colPos]`, the retained bound sample
   column, recursing identically; and a `ColRef` bound against
   `projectTag` (ORDER BY ordinal and alias references,
   `order_binder.go:380–390`/`:450–460`, and the UNION-alias
   fallback for a non-column-reference projection,
   `base_binder.go:666–680`/`:682–692`) descends
   `ctx.projects[colPos]`, the retained bound projection
   expression. A `ColRef` bound against `timeTag` descends `ctx.times[colPos]` with the
   entry shape distinguishing the two mints (revision 16,
   F-TIMEWINDOW-CHANNEL; decision log 61): an `Expr_Col` entry is a
   timestamp boundary self-reference (`TimeWindowStart`/`TimeWindowEnd`,
   `base_binder.go:625–644`), never a decimal producer, and an `Expr_F`
   entry is the retained TIME_WINDOW-routed aggregate output
   (`BindTimeWindowFunc`, `having_binder.go:979–1003`, published as
   `ColRef{timeTag, colPos}` at `having_binder.go:721–736`), the descent
   recursing uniformly with channels (a)–(c) and the entry's top-level
   `ColRef{aggregateTag, aggColPos}` arguments descending
   `ctx.aggregates[aggColPos]` per channel (d), the producer output's parent
   type derived by the parent's registered result rule over the resolved
   parent argument domains (sliding and `MAX_BY` remaps vary only ordinals
   and window configuration). The remaining result-tag families resolve
   nothing by construction: `resultTag`/`sinkTag` are never minted by
   expression binders —
   they serve only the `rootTag()`/`topTag()` plan-construction
   accessors (`bind_context.go:181–198`) and the recursive-CTE
   plumbing that rewrites already-bound trees — so no operand
   reaching the funnel can carry them;
   non-widened operands resolve identity views (a genuine wide
   literal keeps its natural width on both engines, so identical
   surface types never conflate a product with a literal), an
   unresolvable operand defers to the family's original outcome with
   §9 asserting the deferral set empty, and the LEAST/GREATEST
   silent-degrade detector retries exactly when the first lookup
   returns FLOAT64 over decimal-family inputs with multiplication
   provenance (`func_builtin_leastgreatest.go:376`/`:681`). The
   retry's outcome is the parent's by construction — its success target (e.g.
   `COALESCE((38,0)×(38,0), CAST(1 AS DECIMAL(65,1)))` binds the
   parent's `(66,1)` with parent-domain alignment casts; channel (d)
   serves the same outcome where the widened operand is the block's
   own pending producer: `COALESCE(AVG(a*b), L76)` over `(38,0)`
   columns with a genuine 76-digit literal `L76` binds the parent's
   `(76,0)` — the provisional `AVG(a*b)` `(65,4)` against `(76,0)`
   needs `requiredWidth` 80 where the parent's `(65,0)` merges to
   `(76,0)` — through the retained aggregate call and the parent's
   registered AVG rule; the grouped form `SELECT COALESCE(a*b,
   CAST(1 AS DECIMAL(65,1))) FROM t GROUP BY a*b`, reaching the merge
   through the `ColRef{groupTag, colPos}` reference, needs
   `requiredWidth` 77 over the provisional `(76,0)` group output
   where the parent's `(65,0)` merges to `(66,1)`, and retries
   through `ctx.groups[colPos]` to the parent's `(66,1)` with the
   parent-domain alignment cast on the group reference; the TIME_WINDOW
   form `SELECT COALESCE(MAX(a*b), CAST(1 AS DECIMAL(65,1)))
   FROM t INTERVAL(ts, 1, minute)` reaches the merge through
   `ColRef{timeTag, colPos}` — the provisional widened retained
   output `(76,0)` against `(65,1)` needs `requiredWidth` 77
   where the parent's `(65,0)` merges to `(66,1)` — and retries
   through `ctx.times[colPos]` and its `ColRef{aggregateTag,
   aggColPos}` argument to the parent's `(66,1)` with the
   parent-domain alignment cast on the TIME_WINDOW reference) or, where
   the
   parent merge also fails, the family's own failure branch unchanged
   (COALESCE/CASE/IFF bind failure, LEAST/GREATEST FLOAT64 fallback); a
   value admitted by the widened domain but beyond the parent target's
   capacity raises the parent's own checked cast family at the
   parent-domain alignment cast, exactly where the parent rejected the
   statement. Every statement whose first merge succeeds — every
   narrow-provenance statement — pays nothing. Comparisons
   (`list_operator.go:272`) keep the base engine's existing saturated
   common-type fallback (largest representable scale at width 76,
   checked casts rejecting only values that overflow) and need no
   change.
2. **Constant folding.** The folder declines folding a tree that
   carries a >65-width decimal multiplication at the recursive fold
   entry (`foldExpr`, `rule/constant_fold.go:138`, via
   `CarriesWideDecimalMultiplicationProvenance`) — every destructive
   fold shape: valued scalar-vector materialization, NULL-producing
   folds and the enveloping wrapper folds that contain them (revision
   12, F-NULL-FOLD-PROVENANCE — revision 11 declined only the
   scalar-vector materialization inside `MaterializeDecimal256Constant`,
   which a NULL fold never passes through: `GetConstantValue` returns
   the `Isnull` literal at `:661–662` with no decimal256 dispatch);
   the tree still constant-evaluates identically at runtime — only the
   fold micro-optimization is skipped — while provenance
   for the boundary survives in expression syntax. The inner
   `MaterializeDecimal256Constant` check stays as defense in depth.
   Beyond provenance trees, every recorded decimal coercive decision
   the source-domain resolver marks provenance-bearing enters the
   statement-local DECISION BARRIER (revision 13, F-LEDGER-COLLISION;
   decision log 55) carried on the statement's compile-time
   `process.Process` (planner-owned opaque field, `WarningSink
   any`/`planSnapshotTS` precedent, whose planning-generation
   lifecycle is owned by one carrier-provided enter/release pair at
   the outermost planner entries — `BuildPlan` (`build.go:721`) and
   `(*BaseOptimizer).Optimize` (`optimize.go:54`) since revision 14
   and, since revision 15, the shared SELECT funnel
   `bindAndOptimizeSelectQueryWithValidatorAndCapture`
   (`build.go:67`, F-VIEW-DESCRIPTION-LIFECYCLE; decision log 60),
   through which every plan generation funnels — and, since revision
   17, the standalone view-generation owner `inferViewColumns`
   (`build_ddl.go:240`, F-DECISION-EVIDENCE-LIFECYCLE; decision log
   63(i)), whose generation holds through the funnel call AND the
   post-funnel column-record assembly — including initial
   builds (`mysql_cmd_executor.go:4241`/`:4263`/`:4275` — the main
   SELECT build calls the funnel at `build.go:741`/`:748`), compile
   retries
   (`buildPlanForCompileRetry`, `computation_wrapper.go:3011–3070`),
   prepared rebuilds (`BuildPreparedExecutionPlan`,
   `prepared_binding.go:1022`, with its `Optimize` calls at
   `:1037`/`:1048`), EXPLAIN recursion (`buildExplainPlan`,
   `build.go:595` → `:602`) and the standalone view-description
   generations the first two entries never reach — the runtime
   view-columns table function (`view_columns.go:124–160` →
   `DescribeViewColumns`, `view_regeneration.go:142–155`), SHOW
   view metadata (`view_description.go:109–175`, describing at
   `:161`), standalone schema inference
   (`view_schema_inference.go:385`) and persisted-environment
   regeneration (`regenerateViewUsingPersistedEnvironment`,
   `compile/view_metadata_recovery.go:1314–1340` →
   `RegenerateViewDefinition`, `view_regeneration.go:262` →
   `genViewTableDef` at `:271`) — all converging on
   `inferViewColumns` (`build_ddl.go:240`) itself — the
   standalone view-generation owner since revision 17
   (F-DECISION-EVIDENCE-LIFECYCLE, decision log 63(i)) — whose entry
   installs BEFORE its funnel call at `build_ddl.go:301`/`:308` and
   holds through `builder.bindSelect` (`build.go:99`),
   `builder.createQuery()` (`build.go:153`) AND the post-funnel view
   column record assembly at `:345–387` whose storage-envelope
   evaluation persists those records — the funnel keeping decision
   log 60's invocation as the defensive standalone entry, joining
   nested through the depth marker whenever `inferViewColumns`
   installed first; the lifecycle itself is unchanged: an entry with
   no active generation saves
   the field's exact prior value, installs a fresh empty registry and
   marks depth one; a nested entry increments the depth and shares
   the active registry — required, because the optimizer's fold rules
   at `optimize.go:29`/`:32` run inside the same generation after the
   nested `BuildPlan` returns and must still consult the barrier the
   nested binding recorded; and a defer-registered release (the
   `warning_attempt.go:119–185` LIFO-restore precedent) decrements on
   every return path and, at depth zero, restores the saved prior
   value on success, error and panic unwind alike, establishing
   statement isolation on the reused session process
   (`mysql_cmd_executor.go:6220`, `:6453–6472`), freshness across
   retries and prepared rebuilds, and bounded retention; the outermost release of a generation that
   authored a prepared CTAS additionally stashes a copy of the
   completed statement ledger in the generation-keyed, read-once
   process-side claim slot — publication and release per decision
   log 64 (revision 18, F-EVIDENCE-PUBLICATION-PATH), the claim
   channels completed by the ordinary prepared-plan rebuild claim and
   the release decoupled from the specialization-cache clearing per
   decision log 65 (revision 19, F-REBUILD-EVIDENCE-PUBLICATION), its
   validation re-scoped to reachable witnesses in revision 20
   (F-REUSE-WITNESS-REACHABILITY, decision log 66 (iv)-(v), its execute-channel reachability verified unreachable and its oracles corrected in revision 21 (F-EXECUTE-CHANNEL-UNREACHABLE, decision log 67), its skip conclusion scoped to type-preserving fills and the provisional common-VALUE rebind's evidence lifecycle defined in revision 22 (F-VALUE-ARM-REBIND, decision log 68, its execute rebind's producer lineage completed in revision 23, F-SUPPLY-PRODUCER-DOMAINS, decision log 69, its shared-producer SourceStep edge coverage completed in revision 24, F-SOURCESTEP-LINEAGE, decision log 70, its set-operation branch-domain combination completed in revision 25, F-MULTIPRODUCER-DOMAINS, decision log 71, its set-operation coercion-source recovery completed in revision 26, F-SETOP-ALIGNMENT-RECOVERY, decision log 72, its set-operation common-domain reconvergence completed in revision 27, F-SETOP-COMMON-RECONVERGENCE, decision log 73)); a nil
   `proc` (test mocks) is a no-op; revision 14,
   F-REGISTRY-LIFECYCLE, decision log 58, extended to the shared
   funnel's third invocation in revision 15,
   F-VIEW-DESCRIPTION-LIFECYCLE, decision log 60, and moved one
   level out to `inferViewColumns`'s fourth invocation in revision
   17 (F-DECISION-EVIDENCE-LIFECYCLE, decision log 63(i)) — every standalone
   view-description path carries a non-nil process (the isolated
   compiler contexts build on `NewViewBindingProcess`,
   `process2.go:121–152`, at `sql_executor_context.go:72–100` and
   `frontend/compiler_context.go:92–106`; the regeneration path
   carries an explicit process reused across pending views,
   `view_metadata_recovery.go:1314–1340`, the reuse this entry's
   per-call freshness closes; the borrowed-generation precedent
   `BorrowViewSchemaResources`,
   `pkg/vm/process/view_schema_resource.go:19–48`); consulted during
   planning
   only, never serialized and never transmitted — remote pipelines
   build their own processes from the shipped finished plan); at any
   node whose content signature matches a barrier
   entry BOTH fold engines decline their recursive entry — the rule
   engine's `foldExpr` and the sibling engine's recursive entry alike,
   each already receiving the same `proc` at every call site, so the
   enumeration of mutating and speculative fold applications
   (`optimize.go:29`/`:32`, `partition_topn.go:42`, and the sibling
   engine's in-build and copy-based callers) needs no per-site edits
   — skipping the argument recursion and the destructive rewrite, so
   the pre-coercion operand syntax each ledger entry's pass-entry
   capture reads survives every post-recording fold; positions folded
   during their own binding before any decision inside them was
   recorded remain decision log 53's positional-ownership domain.
   Folded genuine
   non-multiplication scalars (`Expr_Vec`) materialize unchanged and keep
   their authored (genuine) value domain at the boundary.
3. **Expression evaluation.** Kernels compute into the Decimal256 carrier;
   the shared validator defers at width 76; products beyond the carrier
   error inside the kernel — on every path: the scaled fallback admits by
   the inline path's physical criterion after the §1 repair (I1), and its
   pre-existing `Decimal256 Mul overflow` family now fires exactly for
   genuine physical overflow. SELECT protocol and result metadata are
   otherwise unchanged.
4. **Consumers of published metadata.** After subquery flattening, the
   per-step optimizer rewrites and column pruning have produced the final
   statement plan, a single reconciliation owner invoked at **both**
   `createQuery` exits (§5; revision 7, F-DEPENDENT-EXIT — the main
   body's end and the dependent route through `createDependentQuery`,
   `query_builder.go:4318–4320`) binds every divergent argument of the
   domain-arithmetic decimal aggregates **AVG and SUM** in its
   base-equivalent domain (I6; revision 7, F-SELECTOR-AGGREGATES — the
   revision-6 blanket over every decimal aggregate is narrowed to §4.2
   class (ii)'s aggregate membership, machine-checked by check two;
   value-selecting and non-decimal-result aggregates stay cast-free):
   inside the argument **every** decimal subexpression whose own envelope
   differs from its own published type takes an ordinary cast to that
   envelope, inner-first — head-independent seam discovery (revision 7,
   F-PRODUCT-ADMISSION, made head-independent in revision 8,
   F-HEAD-GATE — the widened multiply's seam reproduces the parent's
   multiply-time admission; a genuine 76-digit COALESCE branch no longer
   masks the product seam's divergence), with seams that resolve into a
   materialized producer admitted per the row-shape-and-scope taxonomy (revision 11)
   — on row-preserving paths, ONE admission channel per (producer,
   referenced exported slot), selected by positive replay-safety
   proof over every node of the slot's producing expression
   (revision 31, F-REPLAY-SAFETY-POSITIVE-PROOF, completed in
   revision 32 with the diagnostic-freedom tier, F-DIAGNOSTIC-REPLAY):
   a proven slot
   emits ONE envelope-shadow replica column, each shadow replicating
   its slot's own producing expression with producer-representable
   seams cast in place, consumer-side seams cast in tree in the
   consumer's recomposed argument, and each shadowed slot reference
   rebinding directly to the exported shadow slot; an unproven slot
   emits NO shadow — a replica would evaluate a volatile or
   effectful call twice, publishing divergent values and duplicating
   observable effects to the admitting reader — and
   instead takes each producer-representable divergent seam's
   envelope cast in place inside the original export, the export
   evaluated exactly once with its published type unchanged and every
   consumer reading the same produced values, the recomposing
   consumer reading that export directly with the admission
   recognized through the design-owned cast identity; an
   in-tree seam-locus cast below the first interior row-changing
   operator otherwise (revision 8, F-PRODUCER-BOUNDARY; revised in
   revisions 9–10,
   F-COMPANION-DEMAND/F-ROW-CHANGING-BOUNDARY then
   F-GUARD-SEMANTICS/F-PRUNED-PROJECTION-DEPS; scope decomposition
   and ledger replay in revision 11,
   F-SCOPE-SPANNING-ARGUMENTS/F-FOLD-DESTROYS-DOMAINS) — the coercive
   binding decisions in the argument are then replayed over their
   recovered pre-coercion operands at the final branch types,
   ownership snapshot-verified first (revision
   7, F-COERCION; decision-owned replay in revision 8,
   F-CAST-OWNERSHIP; recovered operands in revision 9,
   F-REPLAY-RECOVERY; snapshot-first verification and the
   fold-recovery table in revision 10, F-POST-FOLD-RECOVERY; the
   bind-time decision ledger replaces fold-artifact recovery in
   revision 11, F-FOLD-DESTROYS-DOMAINS; content-keyed copy-stable
   ledger with positional folded-position ownership in revision 12,
   F-LEDGER-LINEAGE/F-FOLDED-LAYER-WITNESS; the statement-scoped
   decision barrier preserving recorded decisions' operand syntax at
   both fold engines' entries in revision 13, F-LEDGER-COLLISION; the
   binder's tag/ordinal pending-producer channel resolving the
   current query block's own pending aggregate and window producers
   for reconvergence and barrier provenance in revision 14,
   F-AGG-WINDOW-CHANNEL; the channel corpus completed to the full
   same-block result-tag inventory — grouped, sampled and projected
   pending producers — in revision 15, F-GROUPED-CHANNEL), the
   argument head binds the envelope,
   and the call's
   authored result type is re-derived for the rebound argument through
   the same parent registration the executor uses, so planning and
   execution consume the parent's own argument domain. The pass then
   propagates its mutations before executor construction as one
   **dependency-ordered finalization** over the plan's
   producer/consumer/copy DAG (§5; revision 7, F-SYNC-ORDER; revision 8,
   F-INPUT-REFRESH — the revision-7 S1/S2/S3 global sweeps are
   withdrawn, because copies are INPUTS to consumer decisions and a
   copies-last re-sync repairs them only after consumers have already
   re-derived from them): plan nodes finalize child-to-parent and
   producer steps before their consumer steps; AVG/SUM calls reconcile
   in place, including TIME_WINDOW's copied inner aggregate; the moment
   a producer slot reaches its final type, every referencing copy
   refreshes from it — pull-up projection columns
   (`query_builder.go:2332–2344`, `:2948–2960`), referencing expressions
   (`projection_binder.go:80–100`), TIME_WINDOW copies, fill
   references, SINK and SINK_SCAN projection schemas across their
   `SourceStep` edges, and the correlation channel in producer order
   (caller captures → program `InputTypes` → callee `CorrelationSlot`
   types → the callee's own internal finalization → `OutputTypes` →
   the caller's dependent-call result `Typ`; revision 8,
   F-DEPENDENT-ENVELOPE, reusing the ownership order of
   `correlation_prepare.go:25–50`) — and only then does each enclosing
   consumer re-derive its authored type through its registered rule,
   replaying its own coercive decisions. Every element finalizes
   exactly once from already-final inputs, so no copy is ever read in a
   mid-mutation state and no consumer re-derives twice. Every other
   consumer is
   enumerated in the §4.2
   inventory, which is pinned by a machine-checked exhaustiveness test, so
   no consumer keys off width>65 reachability without a rule.
5. **Storage boundary (CTAS / CREATE VIEW / ALTER VIEW / prepared CTAS).**
   For each inferred DECIMAL column record whose memoized envelope differs
   from its published type — the affected-domain trigger: the widening
   seam changed the expression's typing somewhere below this column, with
   published width >65 as one special case — the boundary computes the
   expression's exact non-carrier storage envelope (§4) using the §4.3
   evaluator and persists the **envelope-derived declared type verbatim**
   `DECIMAL(integral+scale, scale)` — never the widened published width
   and never floored at 65, so base-authored schemas below 65 (e.g.
   `DECIMAL(64,2)` for `ROUND(product,2)`) are reproduced exactly (I4) and
   carrier expansions never persist (I2). Envelope scale merges beyond
   the declarable 65-scale declaration are a checked planning error.
   Inserted values are admitted by the existing declared-type cast
   evaluation (I3). The view application of the same transformation and
   the read-side contract are in §6.
6. **Remote execution and version compatibility (restated in revision
   40, F-SHUTDOWN-UPGRADE-COMPATIBILITY, decision log 86 (ii)).** This
   change adds NO remote-execution admission: no expression feature, no
   protocol floor, no MULTICN→ONECN demotion and no destination guard.
   Revision 6's F-MIXED-VERSION apparatus — the
   `DecimalMultiplicationCarrierDomain` feature, its compile-time
   plan→features analysis (`plan.RequiredRemoteExpressionFeatures`,
   `pkg/pb/plan/string_literal_form.go`), the minted
   `defines.MORPCVersion135` floor and the `expression_protocol.go` /
   `remoterun.go` gates — is WITHDRAWN (kept-as-history): the repository
   contract makes upgrade shutdown-only, with in-cluster mixed-version
   operation, downgrade and per-feature protocol gates out of scope
   (`.agents/skills/mo-dev/references/feature-design-review.md:147–150`;
   `upgrade-compatibility.md:3–7`, `:54–65` — "an old CN would not
   understand this" is not a finding). After a supported upgrade every
   CN runs the changed binary, so the repaired scaled-fallback kernel
   (§1) executes uniformly; the change adds no plan-protobuf content
   (I8), and the executable-semantics divergence between binaries (an
   old binary rejects the `(65,40)` `10^70` coefficient the repaired
   kernel accepts) never coexists in one cluster. The PRE-EXISTING
   remote-expression floor machinery — the refreshed base's v132/v133/v134
   allocations, their features, floors and gates — is untouched: this
   change mints no constant, does not bump `MORPCLatestVersion` and
   extends no gate. The band adds no issue-specific marker either (§4.1):
   the pre-existing `ViewData.RequiredProtocolVersion` field and its
   established gates keep their base behavior — written only through
   the pre-existing unchanged channel, which keeps recording exactly
   what the base machinery computes for the same statement (the v89
   wide-literal requirement included; revision 41, decision log
   87 (i)).

## 4. Storage-domain inference (committed design)

`decimalStorageEnvelope{integral, scale}` models exact-numeric value domains
the way the type system already combines decimal branches — maximum
integral capacity plus maximum scale — mirroring
`setSafeDecimalWidthAndScaleFromSource` so the boundary recomputes exactly
the envelope a base plan (publication capped at 65) would have carried.
The **zero envelope `{0,0}` is combine-neutral**; every combination keeps
the other side's domain. It is the neutral element for every combine and
for "no proven domain contribution" generally; recursion is resolved in
§4.3 by anchor seeding, not by growing domains from the neutral element.

### 4.1 Envelope rules (per expression shape)

Envelope evaluation (`envelopeOwned`) dispatches per expression shape, each
rule citing its single source of truth so planner semantics cannot drift
from evaluator semantics:

Parent-side typing and provenance are defined NODE-WISE (revision 40,
F-OUTPUT-DIVERGENCE-PROVENANCE, decision log 86 (i)). A leaf's parent-side
type is its published type. A non-multiplication internal node's
parent-side type RE-INVOKES the node's own unchanged publication rule over
the operands' parent-side types — the authoritative typing source, so every
publication shortcut (the parent's fixed-`DECIMAL64(18, s)` d64+d64
add/subtract route, `list_operator.go:296–299`) and every cap fires exactly
as the parent's, and no re-implemented mirror remains to drift (each
"mirror … rule-for-rule" bullet below names the publication its node
re-invokes, content unchanged; the revision-39 add mirror is the one that
drifted — it lacked the shortcut and reconstructed `(11,0)` for
`CAST(a*b AS DECIMAL(10,0)) + d` over `(38,0)` columns with `d
DECIMAL(10,0)` where the parent publishes `(18,0)`). A multiplication
node's parent-side type is the parent's pre-change
`decimalMultiplyReturnType` — the one publication this change alters —
retained and evaluated pure in types over the operands' parent-side types
with the OID-keyed `decimalMultiplyDomain` adjustments (revision 38,
F-NARROW-CARRIER-ENVELOPE, unchanged). A FIXED-DOMAIN BARRIER — a
SQL-authored cast to a declared DECIMAL target, a declared-domain
admission cast, or a recursive member→anchor coercion — pins its
parent-side type to its published type by construction (both engines
evaluate the identical authored target per row), so widening below a
barrier is CONTAINED and changes no type at or above it. A node carries
the SEAM exactly on OUTPUT-DOMAIN DIVERGENCE (published ≠ parent-side); a
wrapper whose own publication maps the divergent operand to the parent's
output — a shortcut or cap absorbing the divergence, as an addition's own
65 cap absorbs `(38,0)×(38,0) + d` into the parent's `(65,0)` — is
seam-free and terminates propagation. The §4.3 storage boundary persists
the parent-side type only on a column whose expression ROOT carries the
seam; contained widening below a barrier or an absorbing wrapper keeps the
column's published type verbatim (I4). The §5 admission sources are the
seam points not dominated by a containing barrier, each re-receiving its
parent-side type exactly as before.

- **Literals** derive from their own digits (text/int), never from a
  binder-widened cast target (e.g. `coalesce(a*b, 0)` aligns `0` to the
  carrier; the `0` branch still contributes one digit).
- **Integer-family typed operands** contribute their exact digit domain
  (mirroring `preparedIntegerIntegralWidth`) so a looked-through alignment
  cast never discards integer capacity.
- **Multiplication** contributes its base-publication envelope per carrier
  family — the parent's own `decimalMultiplyReturnType` evaluated pure in
  types over the parent's operand domains. The parent's
  `decimalMultiplyDomain` consumes the operand's own type keyed on its OID
  (`list_operator.go:157–209`, #31034): the declared width, a
  DECIMAL64-OID operand of width 18 counting 19 for the legacy signed-int64
  coefficient, a DECIMAL128-OID operand of width 38 counting 39 under the
  released checked-D128 MUL coefficient bound, a width-unspecified operand
  counting its carrier maximum. Because the plan-side declaration mapping
  (`build_util.go:395–401`, refreshed-base code) assigns precision ≤16 to
  DECIMAL64 and precision 17–38 to DECIMAL128, a declared `DECIMAL(18,0)`
  or `DECIMAL(17,0)` column is DECIMAL128-typed and counts its declared
  width — the legacy-D64 adjustment applies only to DECIMAL64-OID
  intermediates such as the parent's fixed-`DECIMAL64(18, s)` d64+d64
  add/subtract publication (`list_operator.go:297–299`). The
  decimal256-carrier family (a decimal256 operand, or a narrow pair the
  parent promotes because its nominal domain sum exceeds 38) publishes
  `DECIMAL256(min(dw1+dw2, 65), s)` and the decimal128-carrier family (the
  parent's non-promoted narrow route) publishes
  `DECIMAL128(min(dw1+dw2, 38), s)`, with
  `s = min(s1+s2, max(12,s1,s2))` on both: the expansion attributable to
  the carrier is by construction everything above that parent publication
  (revision 38, F-NARROW-CARRIER-ENVELOPE — revision 2's `min(w1+w2, 65)`
  formula reproduced only the decimal256-carrier family, persisting
  `(39,0)` where the parent authors the promoted `(40,0)` for
  `DECIMAL(38,0) × DECIMAL(1,0)`; revision 39,
  F-UNSEAMED-ENVELOPE-PROVENANCE — and an unprovenanced reconstruction
  persists `(21,0)`/`(22,0)` where the parent authors `(29,0)`/`(38,0)`
  for `(a+b)*a` / `(a+b)*(a+b)` over `DECIMAL(10,0)` columns through the
  parent's legacy-D64 coefficient adjustment over the DECIMAL64(18,0)
  add-shortcut intermediates, while revision 2's `(35,0)`/`(36,0)` for the
  declared `DECIMAL(18,0) × DECIMAL(17,0)` / `DECIMAL(18,0)²` pairs
  coincide with the refreshed parent's own tags, decision log 85). Each
  operand contributes its PARENT-SIDE type — published for a leaf, the
  re-invoked publication for a wrapper, the retained pre-change
  multiplication publication for a `*` node — and a node carries the seam
  exactly when its published type diverges from its parent-side type
  (revision 40, F-OUTPUT-DIVERGENCE-PROVENANCE).
- **Casts**: a `>65` target is binder-generated and is looked through to the
  argument (literal arguments derive from their digits) — SQL cannot author
  such a cast, so pre-existing behavior is unchanged; `≤65` targets keep
  their declared typed envelope. An implicit binder alignment cast (branch
  or argument alignment inside `coalesce`/`case`/greatest/least families or
  aggregate argument alignment) carries the target the binder computed at
  its binding moment — the parent's type exactly when the parent's rules
  produced the same tree, but potentially widened when the producer below
  it is a widened publication; the §5 pass rebuilds exactly the alignment
  casts owned by a replayed coercive binding decision (revision 8,
  F-CAST-OWNERSHIP — the replay unit is the decision, not the flag
  profile of a cast node; §5 facts 8/12) with ownership VERIFIED by re-invoking the
  decision's own registered check over the decision's recovered
  pre-coercion operands at their pass-entry snapshot types against the
  retained alignment layer, and targets then REGENERATED over the
  recovered operands at their final envelope types (revision 10,
  F-POST-FOLD-RECOVERY — the pass skips its own inserted casts by
  identity and takes the single binder-inserted alignment cast per
  position as the unwrap candidate; revision 11,
  F-FOLD-DESTROYS-DOMAINS; content-keyed in revision 12,
  F-LEDGER-LINEAGE — folded positions recover their original
  operand domains from the statement-local DECISION LEDGER the binder
  records at the alignment decision site: per entry, keyed by CONTENT —
  function overload identity, arity, and per position the pre-coercion
  operand type exactly as the merge consumed it WITH that position's
  post-merge aligned type — surviving expression copies and every fold
  shape (a DeepCopyExpr preserves types and payload; a fold preserves
  the folded node's type), with a pass-entry capture binding each
  in-scope decision to its entry, a miss degrading to decision-foreign
  preservation and an observable-state ambiguity surfacing as a checked
  planning failure (§5 fact 17) — with the revision-13 decision
  barrier (F-LEDGER-COLLISION) preserving flagged decisions' operand
  syntax to capture, the parent-successful folded-twin collision is
  unreachable for recorded decisions and the failure stands as
  mechanism-bug defense in depth asserted empty by §9's probes; the revision-10 fold
  table (pad-only lemma, untyped-NULL recovery, element-type recovery)
  is WITHDRAWN — declared width and scale are unrecoverable from a
  folded artifact; the revision-9 `Literal.Src` channel stays
  withdrawn — its population is the prepared-peer FLOAT/string
  provenance machinery only; §5 fact 17),
  and never touches
  a SQL-authored cast or a decision-foreign
  fixed-target cast (the pass's seam/head admission casts; recursive
  member→anchor coercions). The look-through rule
  above is unchanged: §4.1/§4.3 envelopes still see through any cast
  whose target exceeds 65, whether binder-authored at binding time or
  reconstructed by the pass.  The STORAGE-envelope evaluator applies one further look-through (revision
  16, F-BOUNDARY-ALIGNMENT-DOMAINS; decision log 62): a ≤65-target cast
  verified decision-owned through the statement-local ledger — the same
  content-keyed identity and registered-check re-invocation §5's ownership
  verification uses — contributes its operand's reconstructed parent
  envelope rather than its own widened target, so the widened binder's real
  scale- or width-raising alignment casts below width 66 (the merge
  families' `newCheckResultWithCast` layers, `func_binary.go:1125–1160`,
  applied at `base_binder.go:9050`) never shadow the producer's parent
  envelope at any storage boundary, independently of whether §5's
  aggregate-driven replay fired for the statement; SQL-authored casts and
  decision-foreign fixed-target casts (the pass's seam/head admission casts,
  recursive member→anchor coercions) keep their declared typed envelope
  exactly, a ledger miss preserves it, and an observable-state ambiguity
  stays the checked planning failure of §5 fact 17.
- **round/truncate/floor/ceil** mirror `refineDecimalRoundingReturnType`
  rule-for-rule evaluated against the non-carrier input envelope (same
  int64 constant-digits comparison, same carry rules, floor/ceil family
  capped at the declared domain).
- **add/subtract** re-invoke `decimalAddSubReturnType`
  (`pkg/sql/plan/function/list_operator.go:296`) over the operands'
  parent-side types (revision 40, F-OUTPUT-DIVERGENCE-PROVENANCE): the
  publication itself is the typing source — the fixed-`DECIMAL64(18, s)`
  d64+d64 shortcut at `:297–299`, the general
  `min(maxIntegerDigits + scale + 1, 65)` rule and the oid promotion all
  fire exactly as the parent's — so a widened operand derives the
  parent's own below-cap type instead of inheriting the clamp of the
  published type: `TRUNCATE((38,10)×(38,10), 0) + z` (z declared
  `DECIMAL(39,0)`): parent-side operands `(53,0)` and `(39,0)` combine to
  the parent's `(54,0)`, where the widened published chain computes
  `(65,0)`. Above a fixed-domain barrier the shortcut preserves the
  parent's authoritative typing: `CAST(a*b AS DECIMAL(10,0)) + d` over
  `(38,0)` columns with `d DECIMAL(10,0)` — the barrier pins
  `DECIMAL64(10,0)`, the d64+d64 shortcut fires exactly as the parent's
  and the tree persists the parent's `(18,0)`, where revision 39's
  re-implemented mirror reconstructed `(11,0)` (the round-72 review
  witness, decision log 86 (i)).
- **divide** mirror `decimalDivisionReturnType`
  (`pkg/sql/plan/function/decimal_division.go:104–134`) rule-for-rule
  over the operand envelopes (revision 5, F-CAPPED-CONSUMERS): precision
  `leftPrecision + rightScale + increment`, scale `leftScale + increment`,
  the two caps (`maxDecimalDivisionPrecision 65`,
  `maxDecimalDivisionScale 30`) and the `precision < scale` lift applied
  verbatim, with `increment` the same session `div_precision_increment`
  value the publication consumed
  (`applyDivPrecisionIncrement`, `decimal_division.go:146–159`; the
  binder default at `pkg/sql/plan/function/numeric_resolver.go`) — e.g.
  `TRUNCATE((38,10)×(38,10), 0) / DECIMAL(38,2)`: envelope operands
  `(53,0)`, `(38,2)` give the parent's `(59,4)` where the widened
  published chain computes `(65,4)`.
- **mod (%)** mirror `DecimalModReturnType`
  (`pkg/sql/plan/function/decimal_mod.go:22`–`:54`) rule-for-rule over
  the operand envelopes (revision 29, F-MOD-STORAGE-ENVELOPE; completes
  decision 74 (i) at the storage boundary): result scale is
  `max(s1, s2)` and result precision is `max(w1, w2)` with each operand
  width taken as its envelope width `integral + scale` — the same
  maximum-of-operand-width/scale derivation the typing owner applies to
  the original operands, evaluated here over the reconstructed parent
  domains; the owner's integer-operand digit recovery
  (`integerDecimalDigits`) and non-decimal pass-through (temporal,
  approximate, string, BIT and unknown operands keep the selected type)
  apply verbatim, the latter keeping their authored typed envelopes. A
  widened operand therefore reconstructs the parent's own below-cap or
  genuine-wide tag instead of persisting the widened published head:
  `TRUNCATE((38,10)×(38,10), 0) % d` (d declared `DECIMAL(39,0)`) has
  operand envelopes `(53,0)` and `(39,0)` combining to the parent's
  `(53,0)` where the widened head computes `(64,0)`; `(38,0)×(38,0) % d`
  has the `*` envelope `(65,0)` with `(39,0)` giving the parent's
  `(65,0)` where the widened head publishes `(76,0)`.
- **lag/lead** combine the value and default branch envelopes through
  the reconcile's coalesce-common rule, restated in revision 33
  (F-LAGLEAD-PARENT-REJECTION) against the assigned parent. THE
  PARENT'S COMPLETE RULE: `reconcileLagLeadDefaultType` (`git show
  5fe08a3307:pkg/sql/plan/base_binder.go:9147–9288`, called at
  `:8902`) derives the merge's common domain — for a decimal value
  with a decimal default, through the `coalesce` registry lookup
  (`:9258`) and the common rule `setSafeDecimalWidthAndScaleFromSource`
  (`pkg/sql/plan/function/type_check.go:1113–1122`: maximum integral
  width, maximum scale, width = integral + scale) — and then REJECTS
  any derived decimal common wider than 65 with the checked
  `moerr.NewOutOfRange(ctx, "DECIMAL", "LAG/LEAD common precision
  exceeds SQL limit 65")` at three width-check sites: bool-numeric
  `:9208`; decimal×integer/bit/year `:9239`, whose comment records
  the intent ("The shared exact-arithmetic helper permits 76
  internal digits, but this public result must remain expressible in
  persisted SQL DDL"); and the final post-lookup check `:9285`. A
  fourth site (`:9261`) re-arms the same rejection when the coalesce
  lookup itself fails on a decimal default. The parent therefore
  persists NOTHING for either witness shape: the `CAST(0 AS
  DECIMAL(65,1))` default pair over `(38,0)` operands derives common
  `(66,1)` — 66 > 65, a checked planning rejection — and the
  `DECIMAL(30,10)` default pair derives `(75,10)`, likewise rejected
  (live parent receipt `/tmp/30938_bvt_dtype_base.log` row 188:
  `lag(1, 0, <66-digit literal>)` fails with this exact family).
  Ordinary coalesce and set-operation common typing carry NO such
  width check (the committed UNION-family `(66,1)` rows are parent
  receipts), so this rejection is the reconcile's own. THE EXPANSION
  (intentional, sanctioned by the issue's removal of the 65-digit
  expression cap): the frozen diff widens the three width checks to
  the carrier bound with the message "LAG/LEAD common precision
  exceeds the Decimal256 limit 76" (worktree `:9272`/`:9304`/`:9353`),
  and the completed reconcile of entry 76 (ii) binds the derived
  common over reconstructed parent domains (`(66,1)`, `(75,10)`).
  The admission is PROVENANCE-BLIND — every decimal default merge
  whose derived width fits 76 binds, multiplication present or not
  (the committed scale-70 `f4_sel`/`f4_unsel` shapes bind `(70,70)`;
  `lag_def_lit` binds `(66,0)`) — because the width check is the
  same 65-cap the issue removes and because the sibling selectors
  already converge `(66,1)` through the check-free ordinary coalesce
  common; the lookup-failure fallback (worktree `:9326`) retains the
  parent's message text and is re-armed identically on retry failure.
  STORAGE POLICY (explicit): an admitted merge persists the
  reconcile-derived declared tag at every storage boundary — the
  `CAST(0 AS DECIMAL(65,1))` default pair persists `(66,1)`, the
  `DECIMAL(30,10)` default pair persists `(75,10)`, the scale-70
  pair persists `(70,70)` — with no clip and no scale rejection.
  The revision-31 clip removal stands with the corrected
  justification that `combineDecimalStorageEnvelope`
  (`build_ddl.go:1656`) reproduces the REPAIRED reconcile's
  computation rule-for-rule for the expanded band (maximum integral
  width, maximum scale), there being no parent persisted schema to
  reproduce; a derived width above 76 keeps the checked rejection
  with the updated message as defense, and the dead scale-rejection
  branch (`build_ddl.go:2069–2072`, the constant at `:1744`) stays
  removed — under reconstructed parent domains a merged scale never
  exceeds the declaration's own bound. The frozen implementation
  still carries the revision-30 envelope (clip `:2073–2075`, scale
  rejection `:2069–2072`), so the committed `.result` rows it
  produced — `lag_def_lit` persisting `(65,0)` (lines 321–324) and
  the `f4_sel`/`f4_unsel` scale rejections (lines 489/491) — are
  restated to the repaired tags `(66,0)` and `(70,70)` when the
  envelope repair lands at the implementation turn. With no default
  branch (or a NULL default) the published domain passes through
  unchanged. Under the §4.3 one-shot evaluator the rule is applied
  exactly once per expression to fixed inputs, so it cannot compound
  against itself and owes no monotonicity property.
   PERSISTED FORM AND COMPATIBILITY (restated in revision 40,
  F-SHUTDOWN-UPGRADE-COMPATIBILITY, decision log 86 (ii)): an admitted
  band merge persists its reconcile-derived tag as a PLAIN catalog
  record in the parent's own above-65 form (the parent's prepared-CTAS
  `(66,1)` receipts establish the form). A band CTAS TABLE stores no
  expression to replay and reads in-domain data with unchanged
  declared-domain admission; a band VIEW stores its definition SQL
  with the same reconcile-derived column records and no issue-specific
  `ViewData.RequiredProtocolVersion` mint — the pre-existing field
  carries exactly the requirement its unchanged channel computes for
  the same statement (the v89 wide-literal requirement where the
  definition carries a wide literal, the gate-disappearance witness's
  N76 included; revision 41, decision log 87 (i)). No new protocol
  floor, issue-specific marker, demotion, destination guard or
  rollback fence is introduced for the band:
  revision 34's F-LAGLEAD-PERSISTED-FLOOR, revision 36's
  F-LAGLEAD-PERSISTED-CHANNEL and revision 37's
  F-LAGLEAD-VIEW-EXPANSION-PROPAGATION are WITHDRAWN (kept-as-history)
  under the repository's shutdown-only upgrade contract
  (`feature-design-review.md:147–150`,
  `upgrade-compatibility.md:3–7`/`:54–65`), which admits no
  mixed-version execution to fence and no supported downgrade. The
  parent-gate band DETERMINATION (revision 36's
  `DecimalWindowDefaultCarrierDomain` signal over the decision ledger)
  had the marker as its only consumer and is withdrawn with it; the
  parent-supported wide `lag`/`lead` enumeration (an absent default
  publishing the first argument's type, `list_window.go:142–193`; a
  NULL default short-circuiting `reconcileLagLeadDefaultType`,
  `base_binder.go:9148–9150`, into the `:8980–8983` fallback; a
  default matching the value domain; no-default forms over widened
  products or genuinely wide columns; every parent-domain reconcile
  deriving ≤65) is unchanged behavior persisting parent-identical
  records, needing no determination. The band's only behavioral
  content is the ADMISSION itself: the changed engine's reconcile
  admits a derived decimal common of width 66–76 — the
  gate-disappearance witness `LAG(a*b,0,N76)` over `(38,0)` columns
  with a 76-digit all-nine literal default (rejected by the parent's
  gate) returns exact values and persists `(76,0)` — and rejects a
  derived width above 76 through the same retained lookup-failure
  fallback, both engines emitting the identical `LAG/LEAD common
  precision exceeds SQL limit 65` text on the decimal coalesce family
  and the changed engine's `… exceeds the Decimal256 limit 76`
  confined to the non-coalesce numeric routings (revision 35). Forward
  compatibility inside the supported boundary: the changed binary
  reads, replays, describes and schema-infers every base-authored
  view and schema byte-identically (I2/I4), and band records authored
  on the changed binary are plain catalog records any binary of this
  version reads with unchanged declared-domain admission. Ordinary
  and cached prepared statements: a prepared CREATE VIEW (preparable
  through `getPreparePlan`'s default `BuildPlan` route,
  `build_dcl.go:29–102`) stores its reconcile-derived records at
  PREPARE and each execute-time rebuild rebinds the definition and
  re-derives the same records — no bind-time evidence crosses a
  plan-cache lifetime.
- **Transparent value-selecting functions**
  (`abs, coalesce, case, if, ifnull, greatest, least, max, min`) combine
  exactly their value-producing branches — control-flow predicates (CASE
  WHEN/IF condition) never produce result values and are excluded;
  coalesce lists every argument as a value branch and is unchanged.
- **Value-merge common-domain reconvergence** (new in revision 12,
  F-COMMON-DOMAIN-OVERFLOW): the common-domain merge
  `setSafeDecimalWidthAndScaleFromSource` (`type_check.go:1074–1123`)
  merges DECLARED operand types — `maxScale` over scales,
  `requiredWidth = maxIntegralWidth + maxScale` over `Width − Scale` —
  and rejects a `requiredWidth` above the 76-digit carrier
  (`decimalTypeForRequiredWidth`, `:1125–1151`; the carrier width itself,
  `types.T_decimal256.ToType().Width == 76`, `types.go:869–871`). A
  widened publication can push that total beyond 76 although the
  parent's publication merged successfully — `(76,0)` + `(65,1)` needs
  77 where the parent's `(65,0)` + `(65,1)` needed 66 — and each
  caller's failure branch changes previously successful statements:
  COALESCE/IFNULL reject at binding (`func_binary.go:883`, `:1136–1157`),
  CASE and IFF exhaust their support loops into bind failure
  (`operatorSet.go:559` → `:571–572`, `:896`), and LEAST/GREATEST
  silently degrade to FLOAT64 (`func_builtin_leastgreatest.go:376`,
  `:681`). On that failure, and only where an operand subtree carries
  widened-multiplication provenance, the call site retries the
  identical merge over each operand's PARENT-published type — the
  operand's own §4.1 mirror evaluated purely in types (multiplication
  contributes `min(w1+w2, 65)` with its result scale) — SUPERSEDED in
  revision 13 (F-RECONVERGENCE-CHANNEL, decision log 56): the merge
  check contract is a pure `[]types.Type` vector
  (`function/function.go:1161`, reached through
  `function.GetFunctionByName` at `base_binder.go:8323–8327`), so the
  retry moves into the binder's source-domain funnel for exactly the
  reconvergence families, with each operand's parent-published type
  resolved structurally — direct expression trees against the
  statement-local ledger, producer references (derived-table and view
  columns, CTE columns, aliases) through the planner-local per-column
  `Binding` channel populated at producer-binding construction
  (`headingProvenance` precedent, `bind_context.go:106–134`), scalar
  subqueries through their retained projections (`Expr_Sub`,
  `base_binder.go:1010–1031`), and the current query block's pending
  aggregate and window outputs through the binder's tag/ordinal
  channel — `BindContext.aggregates[colPos]` for a
  `ColRef{aggregateTag, colPos}` (`having_binder.go:480–499`) and
  `ctx.windows[colPos].WindowFunc` for a `ColRef{windowTag, colPos}`
  (`window_binder.go:1187–1195`/`:1444–1453`), the producer output's
  parent type derived by the parent's registered result rule over
  the resolved parent argument domains and the same resolution
  feeding the barrier's provenance flags (revision 14,
  F-AGG-WINDOW-CHANNEL; decision log 57), and the block's pending
  grouped, sampled and projected outputs through the same channel's
  completed inventory — `BindContext.groups[colPos]` for a
  `ColRef{groupTag, colPos}` (`projection_binder.go:56–65`,
  `having_binder.go:55–66`; the retained bound key,
  `group_binder.go:272–290`; its output with `groupOutputType`'s
  grouping-set nullability adjustment, `types.go:1093–1098`),
  `ctx.sampleFunc.columns[colPos]` for a `ColRef{sampleTag, colPos}`
  (`build_sample.go:76`/`:96`) and `ctx.projects[colPos]` for a
  `ColRef{projectTag, colPos}` (`order_binder.go:380–390`/
  `:450–460`, `base_binder.go:682–692`) and `ctx.times[colPos]` for a
  `ColRef{timeTag, colPos}` — an `Expr_F` entry
  descending the retained TIME_WINDOW-routed aggregate
  (`having_binder.go:979–1003`), its `Expr_Col` timestamp-boundary
  self-references (`base_binder.go:625–644`) resolved irrelevant
  by construction (revision 16, F-TIMEWINDOW-CHANNEL; decision
  log 61) — with `resultTag`/`sinkTag` resolved irrelevant by
  construction
  (revision 15, F-GROUPED-CHANNEL; decision log 59) — the four
  family files exit the
  planned production set, and the retry's trigger additionally covers
  the LEAST/GREATEST silent FLOAT64 degrade over decimal-family
  inputs: on success the
  retry authors the parent's own target with the binder's ordinary
  parent-domain alignment casts (a value admitted by the widened domain
  but beyond the parent target's capacity raises the parent's checked
  cast family at execution — e.g. a 76-integral-digit product value
  merged with a scale-1 sibling binds `(66,1)`, and the product's
  `10^76`-scale value is rejected there by the family the parent
  applied to the product itself); on failure the family's own branch
  stands unchanged (the parent produces the same failure over genuinely
  >76-merged genuine-wide types, so a no-provenance failure never
  retries). The result scale and oid promotion come from the retried
  merge verbatim. The retried decision is an ordinary coercive binding
  decision: the §5 ledger records it with its content key and replays
  it like any other (revision 12, F-LEDGER-LINEAGE). Comparison
  operators (`list_operator.go:272`) are NOT in scope: the base engine
  already saturates their common type at `T_decimal256` width with the
  largest representable scale and lets the checked cast reject only
  values that actually overflow — the parent's behavior for widened
  operands verbatim.
- **first_value/last_value/nth_value** select their single value argument.
- **percentile_disc (aggregate and window form) selects its ordered
  argument's domain** (new in revision 2): its return type is the input
  type verbatim (`PercentileDiscReturnType`,
  `pkg/sql/colexec/aggexec/ordered_percentile.go:151`), its planner admission (`orderedSetPercentileCheck`,
  `pkg/sql/plan/function/ordered_set.go:64`) and executor support
  (`PercentileDiscSupportedType`, `ordered_percentile.go:165`) both admit
  Decimal256, and the value it returns is one of the input rows — the exact
  envelope of the ORDER BY argument. Both the AGG slot and the WINDOW
  win-spec resolution paths reach the same rule, so e.g.
  `percentile_disc(0.5) WITHIN GROUP (ORDER BY a*b)` persists the declared
  (65, s) schema with fitting values and checked admission of oversized
  values (I2/I3), exactly the base outcome.
- **Aggregates surface via relational resolution** (below); a scalar
  wrapper around an aggregate (`abs(max(p))`) resolves through the
  aggregate's producer expression rather than trusting the published
  column width.
- **Window expressions** resolve through their window function argument in
  the owning WINDOW node's context.
- **Dependent scalar calls (`Expr_Dependent`) and correlation slots
  (`Expr_Slot`)** (new in revision 8, F-DEPENDENT-ENVELOPE): revision 7's
  evaluator dispatched only `Expr_Lit`/`Expr_Col`/`Expr_F`
  (`build_ddl.go:1822–1958`), so a dependent scalar multiplication
  product retained its widened authored type, envelope == published held
  spuriously, and neither the boundary nor the §5 admission ever fired on
  the correlated path. A dependent call's envelope resolves through the
  CALLEE program's root output: the dependent program's root-node
  producer slot is envelope-evaluated by the same rules (recursively,
  memoized in the same statement-local memo keyed on the program's own
  `(NodeId, slot)` space), and the call takes that envelope. A
  correlation slot's envelope resolves through the caller-side CAPTURED
  expression for that slot ordinal — the capture is a positional edge
  from the slot to the caller expression the binder captured, readable in
  the dependent program's capture list — evaluated by the same rules.
  Both rules read only already-bound expressions, so the memoized
  one-shot walk treats them as additional producer edges; the slot and
  the call each evaluate exactly once. `ValidateCorrelation`'s equalities
  (`correlation.go:216`/`:242`) constrain the channel's copied types,
  which the §5 finalization refreshes in producer order (§5 fact 13).

Relational producer resolution (`resolveRelationalOutput`) follows a column
reference through the plan DAG below the main projection node:

- PROJECT/SORT/PARTITION/FILTER/FILL are positional mirrors of their child
  projections.
- AGG output slots resolve through the node's own projection
  (self-references RelPos −1 grouping keys, −2 logical
  grouping-then-aggregate slots), with positional
  `GroupBy`-then-`AggList` fallback for layouts without a projection list.
- WINDOW resolves its single win-spec result slot to the window function;
  passthrough slots mirror the child.
- TIME_WINDOW resolves through **the same `BuildTimeWindowLayout` the
  operator is built from** (aggregate slots in `AggList` order, boundary
  `_wstart/_wend` slots typed, partition-key slots to
  `TimeWindowPartitionBy`), so slot semantics cannot drift from execution.
- SAMPLE mirrors its self-referencing slots and forwarded child columns.
- JOIN resolves the side-selected child projection (RelPos picks left/right
  child); a MARK marker or unexpected shape keeps the authored type.
- UNION/INTERSECT/MINUS resolve each branch's slot and union the
  contributors.
- CTE readers (SINK_SCAN/RECURSIVE_SCAN/RECURSIVE_CTE) publish positional
  projections whose producers live in `query.Steps`: `SourceStep →
  steps[step] → SINK child → produced root`.

### 4.2 Consumer inventory and fallback semantics (revision 2, extended in revision 5)

Revision 1's claim "unrecognized shapes keep the authored type, exactly the
base behavior" is **withdrawn**: publication changed that authored type, and
a width-preserving consumer outside the rule set (the review's
`percentile_disc` witness) then persisted the carrier width — an I2
violation even with fitting values. Replacement:

1. **Complete width-preserving consumer inventory.** A consumer is
   width-preserving if its published result type copies an input type or
   derives monotonically from it without a cap. The inventory over the live
   registries (`pkg/sql/plan/function` overloads + `aggexec` return
   types) is:
   - scalar selectors: `abs, unary_plus, unary_minus, coalesce, case, if,
     iff, ifnull, greatest, least, max, min, name_const` (§4.1 rules);
   - value-arg aggregates/window functions: `any_value, max_by,
     max_by_non_null, first_value, last_value, nth_value` (single value
     argument; ordering arguments never produce result values) and
     **`percentile_disc`** (new rule, §4.1);
   - `lag/lead` (dedicated merge rule, §4.1);
   - casts (look-through/declared rules, §4.1);
   - round family (mirror rule, §4.1).
   Consumers verified **not** width-preserving for our domain divide into
   two width classes (revision 5, F-CAPPED-CONSUMERS — revision 2's
   single "capped, base-identical" bucket missed class (ii)):
   (i) **capped and width-insensitive across every seam divergence**:
   `percentile_cont` returns `T_decimal128(38, s+1)` for decimal input
   (`ordered_percentile.go:134`) and `median` returns
   `T_decimal128(38, s+1)` (`median2.go:34`) — pure functions of scale,
   so envelope-bound and published inputs yield identical results;
   `quantile_exact_weighted` returns `args[0]` verbatim
   (`quantile_exact_weighted.go:87`) but its value admission excludes
   Decimal256 (`:57` — decimal only as ≤(38)-wide Decimal128), so no >65
   output is constructible; `approx_percentile`/bit aggregates/var-family
   return float/integer types.
   (ii) **capped but width-sensitive below the cap**: `round`/`truncate`/
   `floor`/`ceil` (`refineDecimalRoundingReturnType`), `add`/`subtract`
   (`decimalAddSubReturnType`) and `divide` (`decimalDivisionReturnType`)
   are width-sensitive at every argument width and mirrored rule-for-rule
   `mod` (`DecimalModReturnType`, `decimal_mod.go:22`–`:54`) joins the
   mirrored set as its one member NOT 65-capped — its
   maximum-of-operand-precisions derivation publishes the widened
   `(76,0)` head across the seam — and is mirrored rule-for-rule in
   §4.1 (revision 29, F-MOD-STORAGE-ENVELOPE; decision 74 (iii)'s
   registry-wide inventory check extends to `mod`);
   `avg` (`avgDecimal256ReturnType`, width-sensitive at every
   width) and `sum` (`SumReturnType` `min(w+22, 65)`, width-sensitive
   below argument width 43 — e.g. `min(34+22, 65)` ≠ `min(42+22, 65)`)
   are covered by the §5 envelope-bound aggregate argument. No other
   decimal consumer is width-sensitive across the seam (machine-checked
   below). Multiplication stays the only scalar operator publishing >65
   (`CarriesWideDecimalMultiplicationProvenance`'s premise —
   `decimalAddSub`/`decimalDivision` cap at 65); being 65-capped no
   longer exempts a consumer from a rule.
2. **Machine-checked exhaustiveness (extended in revision 5,
   F-CAPPED-CONSUMERS).** A planner UT walks every registered
   decimal-accepting overload and aggregate return type. Check one
   (unchanged): for the synthesized carrier inputs ((76,0) and (76,38)),
   any result wider than 65 must name a §4.1 rule; the whitelist IS the
   inventory above, failing deterministically on any forgotten or newly
   added consumer. Check two (new in revision 5; reclassified into two
   disjoint divergence families in revision 8,
   F-TRANSPARENT-CONSUMERS): for every sampled carrier expression
   the UT pairs its published type with the same expression's §4.3
   envelope type across the seam-sensitive families — widened products
   ((66,0)–(76,·)) against their base publications, the mid-scale wrapper
   pairs (64,0)/(53,0) and (65,0)/(54,0), and the division pair
   (65,4)/(59,4) — and every decimal consumer whose result type differs
   between a pair must classify into exactly one of two owned families.
   **Family T (transparent propagation)**: the consumer's registered
   result is a verbatim copy of, or a binder-merge over, the TYPES of its
   value-producing inputs — scalar selectors (`abs, unary±, coalesce,
   case, if, iff, ifnull, greatest, least, max, min, name_const`),
   value-arg aggregates (`any_value, max_by, max_by_non_null`),
   window `first_value`/`last_value`/`nth_value`, `percentile_disc`,
   `lag`/`lead`, and casts — each naming its §4.1 transparent rule
   (verbatim-copy returns such as `ReturnFirstArgType`, or the
   branch-merge rules re-evaluated over branch envelopes):
   propagation across the seam is sound without admission because the
   result IS an input's domain, and these consumers stay outside the §5
   pass's subject (the revision-7 selector exemption, now justified by
   classification rather than enumeration). **Family D (domain
   arithmetic)**: the consumer derives its result by a width/scale
   computing rule over the input types — exactly class (ii) above
   (`round/truncate/floor/ceil`, `add/subtract`, `divide`, `avg`,
   `sum`) — each naming its §4.1 mirror or the §5 aggregate rule; these
   are the consumers whose published type must be reconciled or whose
   argument must be admitted. A divergence that names neither family
   fails the test deterministically at review/CI time instead of silently
   persisting a wrong domain; revision 7's whitelist of class (ii) alone
   is withdrawn because family-T consumers (e.g. aggregate `max`
   copying `(76,0)` verbatim) necessarily diverge across the pairs and
   would have tripped the oracle they are contractually exempt from.
3. **Fallback tiers (explicit, not ad-hoc).**
   - *Enumerable function shapes not in the inventory*: the defended
     **declared-domain fallback** clamps the typed width into the declared
     domain (integral capacity ≤ 65 − scale). I2 dominates I4 here: with
     the inventory machine-checked, a truly genuine >65 domain reaching
     this tier is a defect to fix with a rule, not a default to bless; the
     clamp guarantees no carrier ever persists even if the inventory has a
     hole.
   - *Non-enumerable shapes with no producer to trace* (runtime parameter
     markers `Expr_P`, folded scalar vectors `Expr_Vec`, unresolved
     relational slots): keep the authored type (I4 dominates): these carry
     genuine domains by construction (a bound parameter supplies its own
     declared domain; a folded vector survived I7's provenance decline and
     therefore has no multiplication carrier; an unresolved relational slot
     derives from a catalog/table-def type or a typed layout slot).
4. **Relational fallback justification.** Node-level unresolved shapes keep
   authored types because their types originate from catalog records (≤
   declared, or genuine-wide columns already persisted in parent form) or
   from producer expressions the switch above covers. Node kinds are
   enumerable plan shapes; the §9 plan UT families pin each covered kind
   and the residual set (table/function/external scans, value scans) is
   typed directly from definitions.

### 4.3 Evaluation algorithm: memoized one-shot walk with anchored recursion (revision 4)

Revision 1 claimed `O(|nodes| × max expression size)` from path-scoped
visited sets alone; revision 2 replaced it with a least fixpoint over
§4.1 rules. Design review showed both unsound (rev 1: path-scoped
visitation bounds depth, not work — factorial exploration on recursive
`greatest(p1..pk, i)` members and repeated work on shared diamonds; rev 2:
the LAG/LEAD rule embeds the parent's own lossy integral cap
(build_ddl.go's `over := merged.integral + merged.scale − 65` reduction),
which is not monotone on the componentwise lattice — with value envelope
(65,0), a default growing (0,0) → (0,1) moves the output (65,0) → (64,1) —
so iterating it inside a fixpoint can compound a per-shape projection
against itself; and bounding growth per slot did not bound evaluation
visits — a reverse dependency chain costs quadratic visits under ordered
sweeps while each slot grows once). Revision 3 removes iteration
entirely: the evaluation is a single memoized demand walk whose results
are final, and recursion is resolved against the anchor domain rather
than by growing domains. Revision 4 keeps that walk and narrows
recursion's role to the persisted schema alone: the anchor seed *is* the
union slot's envelope, and recursive members are admitted value-wise by
the parent's own member-to-anchor conversion rather than by a
member-envelope planning check (review F-RECURSIVE-ADMISSION).

- **Domain keys and ownership** (unchanged). Envelopes are computed for
  `(NodeId, slot)` keys — the output slots of plan-DAG producers reachable
  from the boundary's target columns — in one request-scoped memo map on
  `ctasStorageDomain` (statement-scoped, discarded after; I8). No
  cross-request sharing, no global state.
- **Semantics: demand-driven memoized evaluation.** Each slot's envelope
  is computed at most once: the boundary walks a target column's
  producer expression, and a column reference inside it resolves its
  referenced `(NodeId, slot)` from the memo — evaluating that slot on
  demand if needed, then reusing the completed value for every later
  reference. Once complete, a memo entry never changes. Acyclic plans are
  DAGs: every referenced slot is evaluated exactly once; a slot used by
  two descendants (shared producer diamond) or twice in one output list
  (`SELECT x, x`) is walked once total. Every §4.1 rule consumes only
  already-final envelopes of its inputs, so base projections (the
  LAG/LEAD integral cap, the ROUND/TRUNCATE mirror, declared cast
  targets) apply exactly once per expression to fixed inputs — no rule is
  ever re-run against its own projected output, and no monotonicity
  property is owed. References that cannot resolve read their authored
  typed envelope or the §4.2 tier fallback, as before.
- **Recursive cycles: anchor-seeded schema, value-based member admission
  (revision 4).** The parent types recursive CTE columns from the anchor
  member and **coerces every recursive member to the anchor's projection
  types with an ordinary per-column cast** — `makePlan2CastExpr` /
  `forceAssignmentCastExpr` at `query_builder.go:5904–5927` — so member
  admission is per value at cast evaluation, never a shape judgment.
  The envelope algebra mirrors exactly that: each union slot's memo is
  seeded with the anchor member's envelope for that slot — the union's
  persisted schema — and **no recursive-member slot is envelope-evaluated
  for the persisted schema at all**: member envelopes already contribute
  nothing to the parent's column typing, and revision 3's
  admission-envelope planning reject (decision log 20) is withdrawn
  because it rejected queries whose every produced value fits (a member
  `CASE WHEN n=0 THEN <66-digit literal> ELSE p END` whose unused branch
  merely contributes a wide merge domain). Member-side behavior is the
  parent's own coercion unchanged: the member cast targets the anchor
  slot's plan type (the carrier domain for a widened anchor), so fitting
  produced values — including values selected past unused wide branches
  — flow through recursion exactly, a produced value outside the
  persisted anchor envelope raises the pre-existing I3 checked admission
  when it enters declared storage, and a value overflowing the physical
  carrier errors inside the kernel (I1). Back-references from inside a
  recursive member — including cross-column feedback such as k members
  `greatest(p1, .., pk, i)` — close on the anchor seed in one step
  whenever a demanded tree reads one; physical recursion depth/iteration
  limits remain the pre-existing executor contract. A genuine-wide
  anchor (a 66-digit literal with no multiplication) keeps its parent
  form end-to-end: envelope == typed domain, the member cast is the
  parent's identical coercion, and the persistent surface is exactly the
  parent's own inference (I4).
- **Work bound (valid).** Total work = one seed copy per union slot plus
  one local expression walk per `(NodeId, slot)`: `O(Σ over evaluated
  slots of local expression tree size)` — linear in the plan it reads,
  with no re-evaluation, revision, or propagation pass. Intermediate
  contributor lists are allocated per visit and bounded by one node's
  projection length. The bound counts every visit; there are no
  unsuccessful or repeated visits to bound separately.
- **Determinism and test hook.** Evaluation order is the demand order of
  the boundary's fixed column iteration (deterministic per plan), the
  memo is statement-local, and the evaluator counts slot evaluations;
  §9 pins exact counts: shared producer diamonds (the producer slot is
  counted once regardless of fan-out), k-member recursive
  `greatest(p1..pk, i)` CTEs (each union slot seeded once and **zero**
  recursive-member slot evaluations — member envelopes contribute nothing
  to the persisted schema, revision 4), and cross-column recursive
  feedback (closed by the seed, count exact) — the determinism
  revision 1's termination-only abs/coalesce cases could not establish.

**Boundary application (revision 4; trigger restated in revision 40).**
`decimalMulStorageBoundary` runs on every DECIMAL column whose expression
carries **widened-multiplication provenance** — the affected-domain trigger
(review F-BOUNDARY-TRIGGER), observable as the memoized envelope differing
from the published type at a seam point: the widening changed the
expression's typing somewhere below the column. Provenance enters only
through a multiplication whose widened 76-cap publication exceeds its
parent-side type and propagates through consumers until a wrapper absorbs
it or a fixed-domain barrier contains it (§4.1, revision 40); a column
whose expression carries no seam keeps its published type verbatim — the
parent's publication shortcuts (its fixed-`DECIMAL64(18, s)` d64+d64
add/subtract route, `list_operator.go:296–299`) are part of its published
type, and the parent-side computation re-invokes them rather than
reconstructing them (revision 40,
F-OUTPUT-DIVERGENCE-PROVENANCE, decision log 86 (i)). Published width >65 is one special case; the trigger also fires
when a rule downstream of a widened multiplication published a width ≤65
that differs from the parent's — e.g. `ROUND(product, 0)` /
`TRUNCATE(product, 0)` over `DECIMAL(38,10) × DECIMAL(38,10)` publish
`(65,0)`/`(64,0)` where the parent authored `(54,0)`/`(53,0)` (via
`refineDecimalRoundingReturnType`: integer digits + digits, ROUND adding
one carry digit; `base_binder.go`). A column untouched by the widening seam — every
tree without widened-multiplication provenance, including genuine-wide
domains — keeps its authored type exactly as the parent did (I4; enforced
by the revision-40 output-divergence gate, not merely by
parent-side-equals-published coincidence). The boundary reads the memoized envelopes, turns `rejectReason`
into a checked planning error, and otherwise persists
**`DECIMAL(envelope.integral + envelope.scale, envelope.scale)` verbatim**
— revision 2's `max(65, ·)` floor and keep-published branch are removed
(review F-BOUNDARY-FLOOR). The envelope is by construction the
base-typed domain for every shape: every node's parent-side type is its
own pre-change publication rule re-invoked over the operands' parent-side
types — the multiplication rule contributing the parent's per-carrier-family
publication (`min(dw1+dw2, 38)` on the parent's decimal128 narrow route and
`min(dw1+dw2, 65)` on the decimal256 carrier, over the parent's
`decimalMultiplyDomain` operand domains, revision 38
F-NARROW-CARRIER-ENVELOPE) and every other node re-invoking its own
unchanged typing function, shortcuts included (revision 40,
F-OUTPUT-DIVERGENCE-PROVENANCE) — so persisting it reproduces the parent's
schema exactly, including widths below 65.

Consequences, by shape:

- **Multiplication carriers** (envelope ≤ 65): the column persists the
  parent's own publication — `DECIMAL(65, scale)` for a pair whose
  decimal256-carrier derivation reaches the parent's 65 cap, and the
  parent's own narrow-route tag below it (`DECIMAL(20,0)` for
  `DECIMAL(10,0) × DECIMAL(10,0)`, `DECIMAL(36,0)` for declared
  `DECIMAL(18,0) × DECIMAL(18,0)` — declared precision 17/18 maps to
  DECIMAL128, `build_util.go:395–401` — and `DECIMAL(38,0)` for
  `(a+b) × (a+b)` over `DECIMAL(10,0)` columns through the parent's
  legacy-D64 coefficient adjustment over the DECIMAL64(18,0) add-shortcut
  intermediates, revision 38 F-NARROW-CARRIER-ENVELOPE corrected by
  revision 39 F-UNSEAMED-ENVELOPE-PROVENANCE) —
  exactly the schema the base would have authored:
  on base the multiplication published that tag and evaluation
  enforced it, so base CTAS stored fitting values under it and
  failed on oversized ones; the new tag is the same declared tag, and the
  oversized failure becomes a checked admission of the inserted values
  (I3).
- **Width-reduced wrappers** (`ROUND`/`TRUNCATE` mirrors, declared casts):
  the persisted width is the envelope's own — e.g.
  `ROUND(DECIMAL(38,2) × DECIMAL(38,2), 2)` persists `DECIMAL(64,2)`,
  base-identical (`refineDecimalRoundingReturnType`: 61 integer digits +
  2 digits + 1 carry digit via `pkg/sql/plan/base_binder.go`), never the
  revision-2 65-floored (65,2), and `TRUNCATE` follows its own, narrower
  arithmetic (61 + 2, no carry digit → `DECIMAL(63,2)`); legacy CTAS/view
  schemas over such shapes are byte-identical (M4 real, §6).
- **Boundary-crossing wrappers over mid-scale products** (revision 4,
  F-BOUNDARY-TRIGGER): wrappers whose *published* result width is ≤65 still
  persist the parent's schema. `DECIMAL(38,10) × DECIMAL(38,10)` publishes
  `(76,12)` (base `(65,12)`); `ROUND(product, 0)` and `TRUNCATE(product, 0)`
  therefore publish `(65,0)` and `(64,0)` — both at or below the declared
  cap, invisible to a width-only rule — while the parent authored `(54,0)`
  and `(53,0)`. The affected-domain trigger fires (envelope ≠ published)
  and the persisted records are `(54,0)`/`(53,0)`, parent-verbatim, in
  CTAS, prepared CTAS and view replay. Admission of the wrapper's values
  into these parent schemas follows I3 exactly as for carriers: values
  within the parent's domain store exactly, wider values raise the
  checked admission.
- **Capped width-sensitive consumer chains** (revision 5,
  F-CAPPED-CONSUMERS): consumers capped at 65 but width-sensitive below
  the cap route through the §4.1 mirrors at every step, so the
  affected-domain trigger catches their chains too:
  `TRUNCATE((38,10)×(38,10), 0) + z` (z `DECIMAL(39,0)`) publishes
  `(65,0)` yet persists the parent's `(54,0)` (add/subtract mirror over
  `(53,0)` and `(39,0)`), and `TRUNCATE((38,10)×(38,10), 0) /
  DECIMAL(38,2)` publishes `(65,4)` yet persists the parent's `(59,4)`
  (division mirror). Aggregates keep their own §5 argument-side rule:
  their results cap at the declared 65 in every branch, so the boundary
  never re-classes them.
- **Genuine non-carrier wide domains** (wide literals, alignment merges,
  dominated branches whose envelope needs >65): the persisted form is
  exactly the pre-existing inference's own — e.g. a genuine 66-digit
  literal domain persists (66,0), storing the exact values as it did on
  base — with the same admission profile and the parent's pre-existing
  SHOW CREATE printing limitation for M>65 (parser-level, unchanged by
  this design, see §7). I4 dominates the printability axis: the boundary
  never invents or removes a domain the parent had.
- **Nothing in I2/I4 breaks**: carrier-originated expansions never persist
  (inventory + defended fallback + boundary rule), genuine domains keep
  their parent form exactly, and width-reduced wrappers keep the parent's
  narrower form rather than a 65-floor.

## 5. Decimal aggregate base-equivalent argument-domain contract (revision 9)

Review F-AVG2-EXEC and F-AVG2-DERIVE (round 14) replaced revision 2's
provenance-gated Rule A; review F-AVG-BASE-CAPACITY (round 15) showed
revision 3's "pure function of the published type" resolution wrong in the
other direction; reviews F-CAPPED-CONSUMERS and F-BINDING-PHASE (round 16)
showed revision 4's scope too narrow (AVG-only) and its placement wrong
(binding-time); reviews F-DOWNSTREAM-PROPAGATION and F-ADMISSION-CONTRACT
(round 17) showed revision 5's pass incomplete (no downstream type
propagation) and its trigger under-admitting (result-type agreement used
as a proxy for domain equivalence); reviews F-COERCION, F-SYNC-ORDER,
F-DEPENDENT-EXIT, F-SELECTOR-AGGREGATES and F-PRODUCT-ADMISSION (round 18)
showed revision 6's pass still incomplete (implicit binder coercions baked
with binding-moment targets left stale), its sweep order wrong (copies
re-synchronized before their producers finished retyping), its coverage
incomplete (the dependent-query `createQuery` exit unreconciled), its
trigger over-broad (value-selecting aggregates admitted through a domain
cast although they return input rows carrier-exact) and its admission
coarse (one head cast equated merged-envelope admission with the parent
multiply's per-source admission); reviews F-INPUT-REFRESH,
F-HEAD-GATE, F-PRODUCER-BOUNDARY, F-CAST-OWNERSHIP,
F-DEPENDENT-ENVELOPE and F-TRANSPARENT-CONSUMERS (round 19) showed
revision 7's propagation order still wrong in the other direction
(global copies-last sweeps re-derived consumers from stale copied
inputs, though copies are inputs to consumer decisions), its seam
discovery conditioned on head divergence (a genuine 76-digit COALESCE
branch masks the product seam), its per-source admission undefined
across materialized relational producer boundaries (shared-CTE reuse
merges the arms into one column read by N consumers), its replay
classifier over-broad (a flag predicate matching the pass's own fixed
admission casts and the recursive member→anchor coercions), its
dependent/slot envelopes absent and its correlation copy enumeration
incomplete (`InputTypes` and slot types omitted, though
`ValidateCorrelation` constrains them), and its exhaustiveness oracle
mis-classifying transparent consumers (verbatim-copy selectors
necessarily diverge across the seam pairs); and review
F-COMPANION-DEMAND, F-ROW-CHANGING-BOUNDARY, F-REPLAY-RECOVERY and
F-MATERIALIZATION-WITNESS (round 20) showed revision 8's companion
capture demand-expanding against the executor's short-circuit arm
masks, its per-row companion transport undefined across pruning and
row-changing operators, its decision replay without a recoverable
operand list (plan functions carry no decision identity), and its
materialized-boundary witnesses never crossing the boundary
(single-reference CTEs decline sharing below two occurrences); and
review
F-GUARD-SEMANTICS, F-PRUNED-PROJECTION-DEPS, F-POST-FOLD-RECOVERY and
F-PROFITABLE-MATERIALIZATION (round 21) showed revision 9's conjunctive
companion guards STILL mis-evaluating demand and three-valued truth
(NULL CASE predicates keep the row selected where the negated-predicate
guard treats it as excluded, and eager AND evaluates fallible sibling
arms the base masks skip), its consumer-side argument rebuild reading
columns a pruned materialized producer never exported, its
regeneration-equality ownership test inverting exactly when the
target legitimately changes and its fold recovery reading a
prepared-peer-only provenance channel, and its two-reference witnesses
silently declined by the profitability gate (unfiltered producers
never share); and review F-SCOPE-SPANNING-ARGUMENTS,
F-FOLD-DESTROYS-DOMAINS and F-SHADOW-ADMISSION-ACCOUNTING (round 22)
showed revision 10's whole-argument shadow replication unbuildable
for arguments spanning producer scopes (an input joined at the
consumer exists only consumer-side), its fold recovery unable to
reconstruct domains folding already destroyed (distinct declared
domains align identically, fold to the same artifact and diverge at
their fresh targets), and its admission reservation charging the
original output schema although finalization appends shadow columns
(an admitted plan can exceed its own budget). The final contract binds every divergent
argument of the domain-arithmetic decimal aggregates **AVG and SUM** at the
final plan — admitting each divergent source seam per source inside the
argument, discovered head-independently and admitted across materialized
boundaries by structural envelope-shadow replication per referenced
exported slot on row-preserving paths — the consumer recomposing
scope-spanning arguments over the exports — or in-tree seam-locus
casts on row-changing interiors, per the row-shape-and-scope taxonomy
(revision 11) — and propagates the reconciliation
by one dependency-ordered finalization over the producer/consumer/copy
DAG with decision-owned coercion replay over snapshot-verified
recovered operands whose folded domains the bind-time decision ledger
records, at
both `createQuery` exits, before execution. Facts 1–3 anchor the
channel argument (unchanged), facts
4–5 anchor revision 5, fact 6 anchors revision 6, facts 7–9 anchor
revision 7, facts 10–14 anchor revision 8, and facts 15–18 anchor
revisions 9–11:

1. **The executor derives the AVG result type from argument types, not from
   the plan's authored result type** (round-14 verified, unchanged).
   `newSumAvgDecExec` calls `AvgReturnType(param)` independently
   (`pkg/sql/colexec/aggexec/sumavg2.go`, constructor);
   `AggFuncExecExpression` carries only argument expressions and
   configuration (`pkg/sql/colexec/aggexec/types.go`); the grouped
   executor builds aggregates from `GetArgExpressions` types
   (`pkg/sql/colexec/group/helper.go`) and the window executor from
   argument types (`pkg/sql/colexec/window/window.go`). A binder-authored
   result type the executor does not re-derive splits plan metadata from
   execution — unsound regardless of which type is preferable.
2. **No classification inside the aggregate can select a product-specific
   domain honestly** (round-14 verified, unchanged). Mixed arguments
   (`AVG(COALESCE(d DECIMAL(65,0), a*b, <66-digit literal>))`) carry a
   base argument domain (66,0) and a widened published type (76,0) in one
   expression; wrapped products need per-wrapper reconstruction; a
   derived-table ColRef hides the multiplication from any syntax
   predicate. The executor receives one argument type; nothing in it
   separates a widened product from a genuine wide domain.
3. **The published argument type changed out from under the formula**
   (round-15, F-AVG-BASE-CAPACITY). The pre-existing formula is
   width-sensitive: `precision = min(width+4, 65)`,
   `scale = min(scale+4, 38)` floored at the input scale, integer-capacity
   clamp applied only when the argument width ≤ 65. For
   `DECIMAL(38,0) × DECIMAL(38,0)` the parent's argument was its
   publication `(65,0)` and the parent's AVG derived `(65,0)` — returning
   a fitting `10^62` average successfully. Revision 3 fed the widened
   `(76,0)` argument into the same formula, deriving `(65,4)`: the same
   `10^62` average then needs coefficient `10^66` at scale 4 and fails
   the pre-existing result-capacity check (`decimal256FitsPrecision`,
   `sumavg2.go` — "beyond the range, can't be converted to
   Decimal256(65,4)"), newly rejecting a previously successful query.
   Feeding `(76,0)` also changes previously successful rounding
   (`AVG(2*3)`: `(65,0)` → `(65,4)`, `6` → `6.0000`), changes a CTAS's
   persisted AVG column from `(65,0)` to `(65,4)`, and re-derives an
   existing AVG view's record to `(65,4)` ≠ the persisted `(65,0)` —
   contradicting I4 and M4.
4. **Domino width-sensitivity extends to capped scalar consumers and to
   SUM** (round-16, F-CAPPED-CONSUMERS). The §4.1 mirrors reconstruct the
   parent's argument domain at every step of a capped-consumer chain, and
   the aggregate formulas consume it: `AVG(TRUNCATE((38,10)×(38,10),0)+z)`
   (z `DECIMAL(39,0)`) must see the parent's `(54,0)` argument — deriving
   `(58,4)` and returning `1.5000` over `{1,2}` — not the published
   `(65,0)` (deriving `(65,0)`, value `2`); `SUM(ROUND((33,32)×(40,32),0))`
   must see the parent's `(34,0)` argument — `min(34+22, 65)=(56,0)` — not
   the published `(42,0)` (`(64,0)`, a wider result and capacity domain).
   Revision 4's "SUM needs no counterpart" held only for bare-product
   arguments (widths ≥43 after the cap), which are only one family.
5. **The reconciliation placement must be final-plan** (round-16,
   F-BINDING-PHASE). Revision 4 said "at aggregate binding", but the §4.3
   evaluator addresses producers positionally `(NodeId, slot)`, and at
   that phase pending grouped/window outputs still live in BindContext
   binding tags (`pendingColumnSource`, `base_binder.go:3613–3635`) and
   scalar subqueries stay `Expr_Sub` typed from their projection
   (`base_binder.go:1010–1031`) until `flattenSubqueries`
   (`query_builder.go:6700–6719` for groups/aggregates, `:11511` for
   window specs, `:11619`/`:11644` for projections). No binding-phase
   positional resolver exists, so binder-time insertion leaves
   `AVG((SELECT a*b FROM t LIMIT 1))` and pending `AVG(...) OVER ()`
   windows uncovered — the authored widened type survives into the
   executor.
6. **Result-type agreement is not domain-admission equivalence**
   (round-17, F-ADMISSION-CONTRACT). Revision 5 fired the rebound only
   where the envelope and the published argument ALSO derived different
   registered aggregate results. But `avgDecimal256ReturnType` derives
   `(65,4)` for both `(76,0)` and `(66,0)` (precision `min(w+4, 65)`,
   clamp only at width ≤ 65), so the mixed-origin
   `COALESCE(d (65,0), a*b, <66-digit literal>)` argument — published
   `(76,0)`, envelope `(66,0)` — never received its cast, admitting
   product values such as `+10^70`/`-10^70` that the parent's own
   multiplication publication rejected, and a canceling pair averages
   silently to `0`. Bare-product SUM was likewise cast-free, admitting
   `>65`-digit product values the parent rejected at the multiply. The
   public contract keeps every AVG/SUM argument value inside the
   domain the parent's typing admitted; uniform envelope divergence
   across the width-sensitive class delivers it — and revision 7
   (fact 9's per-source decomposition) delivers it per source, because
   a single head cast against the merged envelope is weaker than the
   parent's multiply-time admission (±10^65 products fit a `(66,0)`
   head yet the parent rejected them at the multiply).
7. **The planner copies expression types by value, so reconciliation
   must propagate** (round-17, F-DOWNSTREAM-PROPAGATION). Aggregate and
   window pull-up copy `expr.Typ` into projection columns
   (`query_builder.go:2332–2344` and `:2948–2960`), binder references
   copy them into referencing expressions
   (`projection_binder.go:80–100`), TIME_WINDOW deep-copies the inner
   aggregate and its argument types (`having_binder.go:979–990`) whose
   executor constructs from the copies (`timewin.go:559–572`), and the
   result columns read the final projection's copied types
   (`build.go:999–1003`). Mutating an aggregate's argument and result
   type alone leaves the copies stale: `AVG(a*b)` over `(38,0)²` binds
   `(65,4)` from the published `(76,0)`, so `AVG(a*b)/3` binds division
   scale `4 + 4 = 8`; after the pass rebinds the argument to `(65,0)`
   and re-derives `(65,0)`, the parent's division scale is `0 + 4 = 4`
   — and the division executor runs at whatever result scale the plan
   retains (`d256DivKernelAtScale`, `arithmetic.go:668–671`), silently
   changing the rounded value (`1/3 = 0.3333` at the parent's scale 4
   versus `0.33333333` at a retained scale 8 — the revision-7 oracle
   uses operands whose division stays in range at both scales).
8. **Implicit binder coercions bake their targets at binding time, so
   re-deriving expression types leaves stale cast nodes** (round 18,
   F-COERCION). COALESCE/CASE/IF-family branch alignment and decimal
   argument alignment are materialized as ordinary overload-0 `cast`
   plan nodes whose `TargetType` the binder computed from the branch
   types at binding time (`appendCastBeforeExpr`,
   `base_binder.go:8987–9050`, decimal width/scale merge at
   `type_check.go:1074–1123`; the cast is built with
   `newCheckResultWithCast`, `func_binary.go:1136–1158`), and implicit
   casts are exactly those without `SyntaxExplicitCast`
   (the binder's own implicitness test,
   `base_binder.go:13035–13041`, flag set at `:11894–11910`).
   Re-deriving a producer's `Typ` after reconciliation does not revisit
   those materialized targets: `AVG(d2 e)` bound with argument `d2`
   aligned by an implicit cast to a stale `(65,4)` still applies the
   stale `(65,4)` cast after the producer becomes `(65,0)`, where the
   parent — binding the same SQL over final branch types — computes a
   different target (`setSafeDecimalWidthAndScaleFromSource`'s maxInt
   65/scale-4 arithmetic over the stale branch types yields `(69,4)`
   for one revision-6 witness family, versus the parent's `(65,0)`).
   SQL-authored casts are user semantics and are immutable; exactly the
   implicit casts are rebuilt.
9. **The final plan has more copy seams than fact 7's enumeration, and
   `createQuery` has two exits** (round 18, F-SYNC-ORDER,
   F-DEPENDENT-EXIT). SINK and SINK_SCAN nodes carry their own
   projection-column schemas (`query_builder.go:3310–3313`,
   `:3231–3243`) populated across `SourceStep` edges (`:3247–3251`),
   and the dependent-query route returns from `createQuery` before the
   main body's end (`query_builder.go:4318–4320`,
   `if builder.qry.Correlation != nil { return builder.createDependentQuery() }`),
   whose correlation `OutputTypes` are deep copies
   (`correlation_binding.go:208–210`) and whose dependent scalar call
   carries its own result `Typ` (`:215–217`) — the route already
   exercised by `TestDependentPublicPlan`
   (`correlation_binding_test.go:57–73`). A pass hard-wired to the main
   body's end (`:4593`) never reconciles a correlated statement's
   final plan, and a copy synchronized before its producer's complete
   retyping finalizes from a mid-mutation producer.
10. **Copies are inputs to consumer decisions, so refresh must be
    just-in-time** (round 19, F-INPUT-REFRESH). Revision 7's S3 treated
    the copied types as pure outputs to be synchronized after all
    re-derivation. But the division executor runs the result scale its
    plan type retains (`d256DivKernelAtScale`, `arithmetic.go:668–671`),
    and the division in `AVG(a*b)/3` reads the pull-up projection's Col
    whose `Typ` was value-copied from the AGG slot at pull-up
    (`query_builder.go:2332–2344`): S2 re-derived the division while
    that Col still carried the binding-moment `(65,4)`, producing the
    stale scale 8 (`0.33333333`), and S3 then repaired the Col with no
    further re-derivation. A sweep scheme reorders repairs; it cannot
    make an input final at the moment it is read. The correct structure
    is a topological finalization over the dependency DAG whose nodes
    are producer slots, copies and consumers and whose edges are
    slot→copy (reads), slot→consumer (direct use) and copy→consumer
    (use through a reference): each producer slot finalizes exactly
    once; every referencing copy refreshes from it immediately as part
    of that finalization; and every consumer re-derives only when all
    its input slots and referenced copies are final. The plan DAG is
    acyclic (recursive-CTE cycles close on the anchor seed, §4.3;
    TIME_WINDOW copies and step edges point from already-built
    producers to later consumers), so the order exists; induction over
    it shows every assigned type equals the type the parent bound, and
    no element is computed twice.
11. **Materialized relational producer boundaries erase arm
    provenance** (round 19, F-PRODUCER-BOUNDARY). Shared-CTE reuse
    rewrites a multi-reference CTE into ONE producer step plus one
    `SINK_SCAN` per consumer (`reuseMultiReferenceCTEs`,
    `cte_reuse.go:41–64`: one producer materializes, N readers scan;
    the producer "cannot be narrowed by the consumers"). A per-source
    seam cast cannot be placed inside such a producer without
    narrowing every reader — a value-selecting `MAX` over the same
    column would newly lose its carrier-exact selected row, violating
    the revision-7 selector contract — yet after materialization the
    argument's arms are merged into one column and the consumer cannot
    reconstruct which value came from which arm. Any mechanism that
    only edits the consumer's argument tree therefore works on
    flattened/inline producers and silently loses admission on
    materialized ones (or vice versa). Fact 16 refines the boundary by row shape — a shadow column crosses only row-preserving edges — and
    fact 18 pins when the boundary exists at all.
12. **Cast ownership is per coercive binding decision, not per node
    flag** (round 19, F-CAST-OWNERSHIP). Revision 7's replay classifier
    — every overload-0 `cast` lacking `SyntaxExplicitCast` — matches
    (a) the pass's own seam/head admission casts, which are ordinary
    cast nodes with no annotation channel (revision 7 explicitly chose
    "no plan-format change"), whose target recomputed from the final
    child type is the child's own widened type (an identity recomputation
    that would silently destroy admission), and (b) fixed-target casts
    built outside any alignment decision: the recursive member→anchor
    coercions (`makePlan2CastExpr`, `query_builder.go:5922`, constructed
    at `make.go:1060–1086` without setting the flag) and
    `appendCastBeforeExprWithOverload` alignment
    (`base_binder.go:12079–12096`, likewise flagless). What a coercive
    check decision owns is exactly the alignment-cast layer it inserted
    at ITS argument positions; decisions are pure in argument types and
    session state, so re-invoking a decision's registered check with the
    final child types regenerates that layer exactly, and any cast the
    re-invoked decision does not regenerate — SQL-authored,
    fixed-target admission, anchor coercions — is foreign to it and is
    preserved bitwise. No plan annotation is needed: ownership is
    delimited by the check's own regeneration surface — provided the
    re-invocation starts from the decision's ORIGINAL operand list
    rather than the retained cast tree; fact 17's recovery supplies it.
13. **Dependent calls and correlation slots are opaque to the envelope
    evaluator, and the correlation channel's equalities span more
    copies than revision 7 enumerated** (round 19,
    F-DEPENDENT-ENVELOPE). The §4.3 evaluator dispatches only
    `Expr_Lit`/`Expr_Col`/`Expr_F` (`build_ddl.go:1822–1958`) — an
    `Expr_Dependent` scalar product kept its widened authored type, so
    envelope == published held spuriously and a correlated `AVG((SELECT
    a*b ...))` derived the widened `(65,4)` result, newly rejecting the
    fitting `10^62` average the parent returned. And
    `ValidateCorrelation` requires the slot's `Typ` to equal
    `InputTypes[ordinal]` (`correlation.go:216`) and each call input's
    type to equal `InputTypes` elementwise (`:242`), so revision 7's
    copy list (`OutputTypes`, dependent-call result `Typ`) left
    `InputTypes` and `CorrelationSlot` types stale — inviolable on any
    plan the validator runs. The execute-time plan copy already owns a
    producer-order channel refresh ("caller captures, callee slots and
    result, then the caller's comparison",
    `refreshPreparedCorrelationExpr`, `correlation_prepare.go:25–50`),
    the ownership model the finalization reuses.
14. **A result difference across the seam splits into transparent
    propagation and domain arithmetic** (round 19,
    F-TRANSPARENT-CONSUMERS). `min`/`max`/`any_value`/`max_by` return
    `typs[0]` verbatim (`ReturnFirstArgType`,
    `list_agg.go:103`/`:120`/`:370`/`:717`, body `:876–877`),
    `first_value`/`last_value`/`nth_value` copy their value argument
    (`list_window.go:209–214`/`:234–239`/`:260–265`) and
    `percentile_disc` copies its ordered argument
    (`ordered_percentile.go:151–156`): across a `(65,0)`/`(76,0)` pair
    their results necessarily differ, so check two's revision-7
    whitelist (class (ii) alone) would deterministically reject the
    design's own selector exemptions. The divergence is sound precisely
    because it is transparent — the result domain is an input's domain,
    propagated by a §4.1 rule — while class (ii) divergences are domain
    arithmetic and must be reconciled (mirrors) or admitted (§5).
15. **Short-circuit demand and three-valued selection are observable
    semantics, and no guard arithmetic reproduces them** (round 20,
    F-COMPANION-DEMAND; round 21, F-GUARD-SEMANTICS). The executor
    evaluates COALESCE arms on a shrinking row mask — each arm runs only
    where every earlier arm yielded NULL (`EvalCoalesce`,
    `pkg/sql/colexec/evalExpression.go:1628–1658`; CASE/IF
    `:1600–1616`), a property pinned by
    `TestFlowControlShortCircuitInvalidCast`
    (`evalExpression_test.go:2994`: an invalid cast on a masked-out arm
    must not fire) — and the CTE-sharing code itself vetoes any share
    that would evaluate a fallible output on an expanded row domain
    (`cteOutputDemandPreservesEvaluation`, `cte_reuse.go:412–443`: "a
    cast can be deterministic and still fail on rows excluded by one
    consumer's predicate"). Revision 8's companion captured the seam's
    value as an unconditional producer column and cast it
    consumer-side: the capture evaluated short-circuited arms on rows
    the base masks exclude, and the consumer-side cast checked a
    narrower row domain than the parent's producer-side multiply
    admission for a materialized producer. Revision 9's guarded
    companion narrowed the capture but STILL mis-evaluated demand and
    truth: (a) a CASE branch whose earlier predicate is NULL is NOT
    deselected — `EvalCase` keeps the row in `remaining`
    (`if remaining[j] && !null && b`,
    `evalExpression.go:1605–1614`) — while the revision-9 guard's
    negated predicates treat NULL as excluded (SQL `NOT NULL` stays
    NULL), diverging from three-valued selection; (b) ordinary AND has
    no flow-control special case — the flow-control dispatch names only
    case/elt/coalesce (`evalExpression.go:1866–1884`) and every other
    function, AND included, evaluates ALL operands eagerly on the
    incoming mask (`:1886–1892`) — so a guard conjunction `x IS NULL
    AND a*b IS NULL` evaluates the fallible `a*b` on rows whose
    `COALESCE(x, a*b, c*d)` arm the base engine never selects
    (non-NULL x short-circuits both products), and the guard itself can
    raises the kernel's physical-overflow family on a statement the
    parent executes successfully. The revision-11 shadow is
    therefore not a guarded expression at all but one statement-local
    **envelope-shadow column** per (producer, referenced exported
    slot): a structural REPLICA of that slot's own producing expression
    with each producer-representable divergent seam carrying its
    in-place envelope cast and its coercive binding decisions replayed
    (§5 admission spec), placed at the producer node where the slot's
    own expression is computed (fact 16). The executor's own short-circuit machinery runs
    the replica's unmodified predicates and arms — the same
    `EvalCoalesce`/`EvalCase` code, the same masks, the same
    three-valued selection — so evaluation demand equals the base
    engine's BY CONSTRUCTION: an unselected overflowing branch leaves
    the statement successful, and a violating value on a SELECTED arm
    raises the parent's multiply-time admission family at the parent's
    producer demand. No guard exists to mis-evaluate.
    Revision 30 added the single-evaluation condition
    (F-UNSTABLE-SHADOW-IDENTITY): the shadow replica is a deep copy of
    the slot's producing expression (`build_ddl.go:11257`, appended at
    `:11278`), and every expression copy owns an independent executor
    (`evalExpression.go:269–275`). Revision 31 strengthens the channel
    gate from a value-stability flag to POSITIVE replay-safety proof
    (F-REPLAY-SAFETY-POSITIVE-PROOF): value stability is not
    replay-safety — the nonvolatile `llm_chat` registration
    (`list_builtIn.go:2925–2943`, STRICT, no volatile flag, reported
    stable by the materialization classification) performs one HTTP
    request per row (`func_builtin_llm.go:69–101`, the per-row `Chat`
    at `:90`; one request per `Chat`, `pkg/monlp/llm/ollama.go:99`
    → `ChatMsg` `:77–96`), so a shadow gated on flag absence alone
    would issue every external request twice; revision 32 adds the
    diagnostic half of the same lesson (F-DIAGNOSTIC-REPLAY): equal
    returned values do not establish replay safety — a replica of a
    diagnostic-appending expression duplicates the client-visible
    warning stream. A slot's producing expression is proven
    replay-safe only when EVERY `Expr_F` node inside it is one of
    three POSITIVELY evidenced kinds — (a) a decimal arithmetic
    operator (`*`, `+`, `-`, `/`): error-only kernels, the
    whole-file warning-sink sweep of `arithmetic.go` and
    `arith_decimal_fast.go` finding zero `GetWarningSink`/
    `AppendWarningDiagnostic` references, so evaluation appends no
    diagnostic and an error aborts both evaluations identically;
    (b) a `cast` whose argument and result types are BOTH decimal
    (`T_decimal64`/`T_decimal128`/`T_decimal256`): the one conversion
    family whose `func_cast.go` paths carry no diagnostic append —
    the whole-file enumeration places every append on string→integer
    (`:7396`/`:7409`/`:7428`/`:8471`/`:8482`/`:8499`), string→decimal
    inside `castModeAssignmentIgnore` (`:8645`/`:8986`/`:9082`, a
    mode explicit CAST never selects) or the time/json/mediumint
    families (`:1152`/`:3269`/`:5170`/`:5207`/`:9573`) — the
    revision-31 blanket plain-`cast` exemption is WITHDRAWN because
    `NewExplicitCast` (`list_operator.go:2638–2645`) reaches
    warning-appending conversions outside this family; or (c) an
    overload that `function.GetFunctionIsProvenPureFoldableById`
    accepts (`function.go:337–350`) AND that carries a recorded
    per-overload diagnostic audit establishing its executor appends
    no warning or note for the admitted argument domains — the audit
    table currently admits exactly `coalesce` (`CoalesceGeneral`,
    `func_binary.go:1198–1226`, a pure selector over
    already-evaluated arms whose process never reaches a warning
    sink; `CoalesceStr`, `:1227`, takes `_ *process.Process`).
    Curated membership alone is NOT diagnostic-freedom proof:
    `unhex` is a member (`function.go:216`) with none of the gate's
    exclusion flags (`list_builtIn.go:9276–9294`), yet malformed
    input appends warning 1411 (`unhexToBytes`, `func_unary.go:9277`,
    the append at `:9294`; asserted by
    `TestUnhexWarningForInvalidHex`, `func_unary_test.go:6789–6807`)
    — the canonical counterexample, UNPROVEN. And every VALUE-POSITION
    leaf is a Lit or Col: a binder-produced cast carries its target
    type as an `Expr_T` descriptor second argument
    (`make.go:1073–1083`), which the predicate validates as METADATA —
    the descriptor's `Typ` decides the decimal→decimal family test of
    (b) — while only the cast's VALUE argument (its first) recurses
    into the leaf rule. Every other expression — one containing a volatile
    call, a real-time call, an INTERNAL-class registration, an
    unlisted function such as `llm_chat`, a curated-but-unaudited
    overload such as `unhex`, a cast whose conversion leaves the
    decimal→decimal family, or any non-Lit/Col value leaf — is UNPROVEN
    and takes the in-place single-evaluation channel of fact 16, so
    no replica ever duplicates an observable effect or diagnostic.
    For a proven expression the replica's second evaluation is
    deterministic, value-identical row for row, effect-free and
    diagnostic-free. Eagerly factoring an
    unproven call into its own shared slot stays REJECTED: the base
    engine evaluates such calls lazily under arm masks, and hoisting
    one to an eager slot reassigns a volatile generator's draws from
    selected rows in evaluation order to all producer rows in column
    order. The replica channel is therefore confined to positively
    proven slots; an unproven slot takes the in-place admission
    channel (§5, fact 16), inside the producer's own single
    evaluation, and 'no double evaluation' reads literally for every
    statement. The revision-28/29 shadow witnesses — `COALESCE(a*b,
    <literal>)` over Col leaves, `coalesce` audited diagnostic-free,
    `*` error-only and every arm-alignment cast decimal→decimal —
    classify PROVEN under this predicate and keep the
    shadow channel byte-identically.
16. **A per-row shadow column can cross only row-preserving
    boundaries, and a shadow unit needs no input outside its
    producer** (round 20, F-ROW-CHANGING-BOUNDARY; round 21,
    F-PRUNED-PROJECTION-DEPS; round 22, F-SCOPE-SPANNING-ARGUMENTS).
    Revision 10's per-argument unit claimed the argument's own inputs
    were all producer-side, which a consumer-join argument such as
    `AVG(COALESCE(c.v, u.d))` refutes; revision 11's unit is the
    referenced exported slot's own producing expression, whose inputs
    are producer-side by construction (the producer evaluated the
    slot from them).
    Aggregate outputs renumber under pruning and map to positional
    group+aggregate slots (`query_builder.go:2310–2344`, the
    prune/`aggPos` remap), which the §4.3 resolver addresses through
    the final projection/group/agg layouts
    (`build_ddl.go:2095–2107`); across a dedup step an added
    shadow column would join the dedup key (splitting value-equal rows
    whose shadow values differ), and grouped/window aggregation and
    set operations emit rows with no source-row correspondence at all.
    Shadow transport is therefore confined to row-preserving edges
    (projection/filter chains and SINK/SINK_SCAN materialization, with
    emission on the post-prune, post-remap final plan so positions are
    settled and nothing renumbers them afterwards); wherever the
    seam-to-reader path crosses a row-changing operator inside the
    producer, admission moves in tree to the seam locus below the
    FIRST such operator, inside the argument subtree at the seam's own
    position, demand-exact because it inherits the arm's own mask.
    Admission channel selection is per (producer, referenced exported
    slot) under the POSITIVE replay-safety proof of fact 15
    (revision 31, F-REPLAY-SAFETY-POSITIVE-PROOF, completed in
    revision 32 with the diagnostic-freedom tier, F-DIAGNOSTIC-
    REPLAY): a proven slot
    takes the shadow replica; an unproven slot emits NO shadow and
    instead takes each producer-representable divergent seam's
    envelope cast IN PLACE inside the original export — evaluated
    exactly once by the
    producer's own machinery with the base engine's masks, on the same
    row-preserving paths by the same construction, demand-exact
    because it inherits the arm's own mask — with the export's
    published type unchanged and every consumer reading the same
    produced values. The in-place casts carry the design-owned cast
    identity (fact 17), so the section-4.1 envelope mirrors recover
    through them and the recomposing consumer recognizes the admission
    instead of recasting. The
    shadow's COVERAGE is the producer-representable part of the
    argument (revision 11, F-SCOPE-SPANNING-ARGUMENTS): each divergent
    seam of an admitted argument is classified by input scope — a
    seam whose every input is computed inside an exported slot's own
    producing expression is PRODUCER-REPRESENTABLE and is admitted
    inside that slot's shadow; a seam reading any other input — a
    column of another relation joined at the consumer (eligible
    pinned-build consumer joins are explicitly supported,
    `cte_reuse.go:1208–1246`, `:1684–1695`), a column of another
    materialized producer's export, or a captured/correlated value —
    is CONSUMER-SIDE and keeps its ordinary in-tree envelope cast in
    the consumer's recomposed argument (taxonomy (A) in place at the
    seam's own position, taxonomy (B) below the first consumer-path
    row-changing operator). The shadow replicates ONE exported slot's
    own producing expression, so its inputs are exactly that slot's
    own inputs — already in scope at the producer BY CONSTRUCTION
    (the slot itself needs them), already preserved by pruning for the
    original column; revision 10's wider claim — a whole-argument
    replica whose inputs were 'the argument's own inputs at the
    producer' — is WITHDRAWN: for `AVG(COALESCE(c.v, u.d))` the
    input `u.d` exists only at the consumer join and no producer-side
    replica can read it, and a `SINK_SCAN` consumer never reads an
    unexported column either (revision 9's consumer-side replay read
    the ORIGINAL columns across the boundary and failed exactly here
    — `SINK_SCAN` exposes only the producer's exported columns,
    `cte_reuse.go:1877–1897`, and pruning compacts producer outputs,
    `query_builder.go:3226–3250`). Each shadow crosses the boundary
    as one ordinary exported slot per (producer, referenced exported
    slot) — one slot shadows at most once per statement regardless of
    how many admitted consumers reference it — and the admitting
    consumer RECOMPOSES its argument over the exports: each shadowed
    slot reference rebinds directly to the shadow slot, consumer-side
    structure (the outer `COALESCE` over `u.d`, joins, another
    producer's exports, captured inputs) stays in the consumer tree
    with its decisions replayed from the decision ledger (fact 17),
    and no replay reads a column the producer did not export. Demand
    partitions exactly: the executor's own machinery evaluates each
    slot replica with its unmodified masks on exactly the producer
    rows the base engine evaluates that slot's expression on
    (row-preserving path), and the consumer-side remainder evaluates
    with the base engine's consumer masks — jointly the base
    engine's evaluation BY CONSTRUCTION, with no scope gap; a
    positively proven replica's deterministic re-evaluation is
    value-identical to the original export row for row and duplicates
    no observable effect, and an unproven slot is never replicated
    (revision 31's in-place channel), so no observable double
    evaluation exists (fact 15).
17. **A coercive binding decision has no machine identity; ownership
    is verified over a snapshot taken before any mutation, and folded
    positions read the bind-time decision ledger** (round 20,
    F-REPLAY-RECOVERY; round 21, F-POST-FOLD-RECOVERY; round 22,
    F-FOLD-DESTROYS-DOMAINS). Revision 10's folded-artifact recovery
    table is withdrawn below — folding destroys the declared domains
    the merge consumed before any pass-entry snapshot can read them. `plan.Function`
    carries only the overload reference, the argument list and flag
    fields (`plan.pb.go:3180–3190`) — nothing names which check placed
    a retained alignment cast — and re-invoking the check over the
    RETAINED cast tree reconstructs the wrong answer: a retained
    `(65,4)` baked target re-merged through
    `setSafeDecimalWidthAndScaleFromSource`
    (`type_check.go:1074–1123`) yields `(69,4)` where the parent's
    fresh rebind of the original operands yields `(65,0)`. Revision 9
    regenerated targets over FINAL operands and called
    regeneration-equality the ownership test — unsound twice over:
    when the target legitimately changes (the decision's own check
    over final operands must NOT regenerate the retained layer, e.g. a
    `COALESCE(avg_out, d)` whose `avg_out` retypes `(65,4)→(65,0)`
    with `d` `(61,0)` regenerates `(65,0)`, never the retained
    `(65,4)`), equality fails and the decision's OWN alignment is
    misjudged foreign; and its fold recovery read `Literal.Src`, whose
    population is gated on the FLOAT/string prepared-peer provenance
    machinery (`base_binder.go:4861–4875`) — ordinary folding creates
    decimal literals and typed NULLs with no `Src`
    (`rule/constant_fold.go:320–388`, `:661–662`,
    `utils.go:2099–2132`) and can materialize Decimal256 constants as
    `Expr_Vec` (`constant_fold.go:323–327`). Revision 10 separates
    verification from regeneration, in this order: (i) SNAPSHOT AND CAPTURE — at
    pass entry, before any mutation, every in-scope decision's operand
    list is recovered and recorded WITH its pass-entry
    (binding-moment) types, and the pass-entry CAPTURE binds each
    in-scope decision node to its ledger entry by CONTENT (the
    copy-stable key below — a copy resolves its origin's entry) — casts this pass inserts are skipped via a
    pass-local identity set, the single binder-inserted alignment cast
    per position (`base_binder.go:8987–9054`) is the unwrap
    candidate, and a folded position's original domain comes from the
    decision ledger recorded at binding (below), resolved through the
    captured handle; (ii)
    ORIGINAL-LAYER VERIFICATION, per position — an UNFOLDED position
    keeps the revision-10 bitwise test: the decision's registered check
    is re-invoked over the recovered operands AT THE SNAPSHOT TYPES (the
    check is pure in argument types and session state) and equality with
    the RETAINED alignment layer, structure and targets, is the
    ownership test — legitimate final-target differences play no role
    because the check input equals the input the binder saw; a FOLDED
    position has no retained layer to compare (revision 12,
    F-FOLDED-LAYER-WITNESS — folding replaced the alignment subtree
    with a scalar artifact, `rule/constant_fold.go:319–330`, `:390–402`,
    so revision 11's bitwise claim was unsatisfiable exactly there),
    and ownership is POSITIONAL instead: the artifact is owned exactly
    when its type equals the entry's recorded post-merge aligned type —
    the fold preserves the folded node's type, and only a decision-owned
    alignment fold materializes with that recorded target (SQL cannot
    author a >65 cast target, pass-inserted casts carry ≤65 envelope
    targets and are skipped by the identity set, and I7's entry-level
    decline keeps provenance-carrying trees unfolded), so the witness
    cannot false-positive; (iii)
    FINAL REGENERATION — for verified-owned positions only, the check
    is re-invoked over the recovered operands at their FINAL §4.3
    envelope types and the regenerated layer replaces the retained one
    (the changed-target case regenerates `(65,0)`, never the stale
    `(69,4)` family a re-check over retained casts would produce);
    decision-foreign fixed-target casts (skip-reject at (ii)) are
    preserved bitwise. Folded-position recovery (revision 11, F-FOLD-DESTROYS-DOMAINS
    — the revision-10 fold table is WITHDRAWN): in-scope folds
    destroy operand domains, because the merge consumes each operand's
    DECLARED width and scale
    (`setSafeDecimalWidthAndScaleFromSource` merges with
    scale = maxScale and width = maxIntegralWidth + maxScale,
    `type_check.go:1074–1123` — maxScale over source scales,
    maxIntegralWidth over `Width − Scale`):
    `COALESCE(avg_out, CAST(1 AS DECIMAL(64,0)))` and
    `COALESCE(avg_out, CAST(1 AS DECIMAL(65,1)))` with `avg_out`
    `(65,4)` both align to `(68,4)` and constant-fold to the same
    artifact (`rule/constant_fold.go:262–264` folds the argument
    recursively, `:320–330` replaces the subtree with a scalar
    literal, `:323–327`/`:530–574` materialize a Decimal256
    constant as `Expr_Vec`, and a typed NULL fold discards its cast
    source), yet after `avg_out` finalizes `(65,0)` the parent's fresh
    targets differ — the `(64,0)` twin regenerates `(65,0)`, the
    `(65,1)` twin regenerates `(66,1)` — a distinction no property
    of the folded artifact retains. The binder therefore records a
    statement-local **decision ledger** at the alignment decision site
    (`base_binder.go:8987–9054`): for every coercive binding
    decision with a decimal operand it stores one entry keyed by
    CONTENT (revision 12, F-LEDGER-LINEAGE — node identity does not
    survive projection inlining's `DeepCopyExpr`, `opt_misc.go:469–479`
    / `deepcopy.go:1179–1184`, `:1284–1296`, nor the §5 shadow
    replicas' construction): the function overload identity, the
    argument count, and per argument position both the operand's
    PRE-COERCION bound type exactly as the merge consumed it and the
    position's post-merge aligned type. Content is copy-stable —
    `DeepCopyExpr` preserves every type and payload the key reads —
    and fold-stable: at the pass-entry capture each in-scope decision
    node computes its observable signature from the recovered operands
    (an alignment cast retained at a position unwraps to the operand's
    observable type; a folded position contributes its artifact's type,
    the entry's recorded domain adopted unseen until regeneration) and
    binds the unique entry whose recorded key agrees on every
    observable position. A copy computes the identical observable
    signature — an inlined COPY of an admitted decision is
    semantically the same decision at the same operand domains, so
    resolving its origin's entry is correct — and a §5 shadow replica
    resolves each replicated decision's entry by its own observable
    signature; §9 pins the projection-inlining control asserting copies
    replay parent-verbatim across explicit-cast boundary destruction,
    typed-NULL folds and vector materialization alike. A
    folded-artifact COLLISION — two recorded entries agreeing with one
    node on every observable position while differing in a folded
    position's recorded domain, a deliberately constructed same-value
    cross-domain twin such as `CAST(1 AS DECIMAL(64,0))` against
    `CAST(1 AS DECIMAL(65,1))` folded to the identical `(68,4)` artifact
    inside admitted arguments — admitted no sound plan-local resolution in revision 12, which
    rejected the parent-successful statement at planning (the finding
    this revision repairs, F-LEDGER-COLLISION): the revision-13
    DECISION BARRIER (decision log 55) keeps every flagged decision's
    operand cast syntax unfolded, so the twins' capture-time
    observable signatures stay distinct and each twin binds its own
    entry; the replays regenerate the parent's respective
    `(65,0)`/`(66,1)` decisions and the twin statement executes with
    parent-equivalent values, result types and persisted metadata
    (§9 I10's single-statement regression). The same protection
    reaches twins whose admitted producer is the current query
    block's own pending aggregate: `SELECT COALESCE(AVG(a*b),
    CAST(1 AS DECIMAL(64,0))), COALESCE(AVG(a*b), CAST(1 AS
    DECIMAL(65,1))) FROM t` resolves the pending `AVG(a*b)` output
    through the revision-14 tag/ordinal channel, flags both
    decisions into the barrier before their operand syntax folds,
    and executes parent-equivalent, its two result types the
    parent's `(65,0)` and `(66,1)` (revision 14,
    F-AGG-WINDOW-CHANNEL; §9 I10's same-block regression). The
    grouped pending producer is protected identically: `SELECT
    COALESCE(a*b, CAST(1 AS DECIMAL(64,0))), COALESCE(a*b, CAST(1
    AS DECIMAL(65,1))) FROM t GROUP BY a*b` — both SELECT-list
    references minted `ColRef{groupTag, colPos}` — resolves each
    decision's produced operand through the revision-15 grouped
    descent into `BindContext.groups[colPos]`, flags both decisions
    into the barrier before their operand syntax folds, and
    executes parent-equivalent, its two result types the parent's
    `(65,0)` and `(66,1)` (revision 15, F-GROUPED-CHANNEL; §9 I10's
    grouped twin regression). A
    residual collision is
    still raised as a checked PLANNING failure — loud, before
    execution — but it now classifies an ungated-fold MECHANISM BUG
    (I8 still forbids a plan-format annotation channel, and degrading
    a sibling to decision-foreign preservation would silently retain
    its stale artifact): the §9 barrier-coverage probe asserts every
    flagged decision's operand syntax survived to capture, and the
    ambiguity probe asserts the residual set is empty.
    Phase (ii) verification is per-position by construction: an unfolded
    position's bitwise test regenerates its retained layer exactly (the
    check input equals the input the binder saw), and a folded
    position's positional test compares the artifact's type with the
    recorded aligned type it materialized. Phase (iii) FINAL
    regeneration consumes, per argument position, the position's final
    operand domain: an UNFOLDED operand contributes its §4.3 final
    envelope type; a FOLDED operand contributes its ledger-recorded
    pre-coercion type, envelope-finalized — identity for folded
    operands, because the fold-decline
    (`CarriesWideDecimalMultiplicationProvenance`, §3 step 2) keeps
    every tree carrying a widened multiplication unfolded and no other
    envelope-divergent expression reaches a fold — so the `(64,0)`
    twin regenerates `(65,0)` and the `(65,1)` twin regenerates
    `(66,1)`, each the parent's fresh rebind, and a typed NULL's
    recorded type contributes its genuine scale to the merge, never an
    'untyped' approximation. The folded operand's VALUE re-normalizes
    into the recorded pre-coercion domain before the regenerated
    alignment applies: the binder's alignment cast is an exact
    fixed-point scale lift (its scale is the merge maximum, so it only
    appends zeros), the artifact's coefficient therefore re-bases into
    the operand's own declared domain loss-free (`1.0000` at `(68,4)`
    re-bases to `1` at `(64,0)` or `1.0` at `(65,1)`), and the
    regenerated alignment layer builds over the re-based operand and
    folds again by the ordinary rules to the parent's fresh artifact
    (`1` at `(65,0)` versus `1.0` at `(66,1)`). A position with NO ledger entry — a
    decision synthesized after binding by a rule pass or a manual
    rebuild — is decision-foreign at that position: preserved
    bitwise (safe degradation), and §9 pins a coverage probe
    asserting every in-scope decimal coercive decision holds an entry,
    so a missed channel fails loudly in testing instead of silently
    skipping a replay, and the same probe asserts the observable-state
    ambiguity set is empty. The `Literal.Src` channel stays WITHDRAWN (its
    provenance serves only the prepared-peer FLOAT/string machinery).
18. **CTE sharing materializes only past the complete admission
    gate: occurrence, profitability, drain, demand and storage — the
    storage charge computed on the shadow-expanded schema** (round 20,
    F-MATERIALIZATION-WITNESS; round 21, F-PROFITABLE-MATERIALIZATION;
    round 22, F-SHADOW-ADMISSION-ACCOUNTING). `reusableCTEProducer` returns no
    share when `len(cteRef.occurrences) < 2` (`cte_reuse.go:73`), an
    occurrence appended per table binding of the CTE name
    (`query_builder.go:5670–5686`): a statement binding the name once
    never materializes. But two bindings are necessary, NOT sufficient:
    the same admission also requires the profitability comparison
    (`cte_reuse.go:172–174`; `cteReuseIsProfitable`, `:826–837` —
    inline cost `refs × producerCost` against sharing cost
    `producerCost + (refs + 1) × outcnt`, accepted only when sharing is
    strictly cheaper at safety factor 1 on the two-occurrence path
    (`:812–813`; the 2× spill safety factor applies only at three or
    more occurrences, `:150–151`)), the full-drain consumer
    requirements (`cteConsumerDrainRequirements`/`cteHasDrainWitness`,
    `:104–113`), the demand-preservation veto
    (`cteOutputDemandPreservesEvaluation`, `:412–443`, fact 15) and the
    storage admission reservation (`reserveSharedMaterialization`,
    pinned by `cte_lazy_binding_test.go:1737–1756`) — computed, from
    revision 11 (F-SHADOW-ADMISSION-ACCOUNTING), on the
    SHADOW-EXPANDED output schema: the reservation's storage types are
    the exported types PLUS one fixed 32-byte `Decimal256` column per
    exported slot whose producing expression carries widened
    multiplication provenance — the conservative,
    occurrence-independent upper bound on the shadows any statement
    can emit into that producer (fact 16: a shadow is per slot per
    statement, never per consumer, and from revision 31 only a
    POSITIVELY PROVEN replay-safe slot can emit one — an unproven
    slot takes the in-place channel with no schema expansion — so the
    bound
    charges a superset of the slots that can actually shadow, and no
    consumer-side admission decision can exceed it) — charged through the same byte
    estimate, per-column spill framing and cumulative process
    accounting (`cte_reuse.go:152–179`, `:712–768`, `:771–808`)
    BEFORE the share is committed, all-or-nothing: a declined share
    leaves the cumulative counters untouched and every occurrence
    inlines with the base-identical in-tree contract, so a producer
    that cannot fit its expanded reservation never starts sharing and
    runtime reservations
    (`pkg/sql/internal/materialized/source.go:484–518`) always match
    a schema the planner already charged. An unfiltered
    scan/projection producer costs `outcnt = N` (scan cost N,
    `stats.go:2266–2268`; projection cost = outcnt, `:1812–1816`), so
    at two references: inline = 2N, shared = N + 3N — the
    revision-9 witnesses' unfiltered producers were DECLINED and
    silently exercised the inline path, proving nothing about the
    boundary. A witness of materialized-boundary admission must clear
    every gate element BY CONSTRUCTION: at least two full-draining
    aggregate readers of the name (scalar-subquery `AVG`/`MAX`
    consumers drain fully), a deterministically profitable producer —
    a row-preserving FILTERED producer with estimated selectivity
    below one half, e.g. `WHERE k % 4 = 0` over N rows (scan cost N,
    projected outcnt N/4: shared = (N + N/4) + 3·N/4 = 2N < inline =
    2(N + N/4) = 2.5N; profitable ⇔ 2·outcnt < producerCost on this
    cost model) — with the filter preserving row correspondence (fact
    16, path (A)), the demand veto satisfied because sharing evaluates
    no fallible output on an expanded domain, and a bounded materialization
    that `reserveSharedMaterialization` admits. The plan-shape oracle
    then asserts exactly one SINK producer and exactly two SINK_SCAN
    readers before any value assertion — so a cost-model drift that
    silently declines sharing fails the witness loudly instead of
    vacuously passing it — error and value expectations are separated
    statements, and a statement-level rejection cannot double as proof
    of a sibling reader's value.

**Reconciled contract: the parent formula, the parent's argument domain,
bound at the final plan.** §4.3's envelope already computes, for every
expression shape, the exact type the parent would have bound. A single
**aggregate-argument reconciliation pass** runs at **both** `createQuery`
exits (revision 7, F-DEPENDENT-EXIT): the main body's end
(`query_builder.go:4593`) and the dependent route
(`query_builder.go:4318–4320` → `createDependentQuery`,
`correlation_binding.go:148`), invoked immediately before each return,
after subquery flattening, the
per-step optimizer rewrites, and `remapAllColRefs` /
`removeUnnecessaryProjections` (`:4538`/`:4540`) — the phase where every
aggregate call lives in a defined final-plan position. The pass's subject
is exactly the §4.2 class (ii) domain-arithmetic decimal aggregates
`avg` and `sum` — the width-sensitive membership the §4.2 inventory and
its machine check (§4.2 check two) already pin, so a future
width-sensitive decimal aggregate must join this class or fail the UT;
value-selecting aggregates are excluded by construction (fact 2's
channel argument does not apply: they return input rows unchanged, so
no domain cast is inserted and widened values flow carrier-exact to the
consumer, with storage admission CTAS-side as for every widened
publication; §4.1/§6 enumerate them). The pass:

- walks every AGG node's `AggList` and every WINDOW node's `WinSpecs` of
  the final plan for `avg`/`sum` calls whose argument is decimal-typed
  (a statement without widened decimal multiplication
  provenance skips structurally — §2 I8);
- evaluates each such argument's §4.3 envelope **on the
  final plan** with the same evaluator and positional resolution the
  storage boundary uses (statement-local memo; the deterministic
  evaluator gives this consumer and the boundary consumer identical
  results — flattened scalar subqueries resolve through their expanded
  producer slots, derived-table references are plain column references
  into their producer nodes, and previously pending grouped/window
  outputs are the rewritten `AggList`/`WinSpecs` expressions themselves);
- for each argument, uniformly across the class with no result-agreement
  gate (revision 6,
  F-ADMISSION-CONTRACT; fact 6) — **admits every divergent decimal seam
  inside the argument per source, discovered head-independently, cast
  inner-first, then rebinds the argument head** (revision 7,
  F-PRODUCT-ADMISSION; discovery made head-independent in revision 8,
  F-HEAD-GATE): **every** decimal-typed subexpression of the argument
  whose own §4.3 envelope differs from its own published type receives an
  ordinary plan-expression **cast to its own envelope domain** (the same
  cast machinery COALESCE alignment and the
  parent's
  recursive-member coercion use — no annotation channel, no new
  expression kind, no plan-format change), innermost first, so a widened
  multiply `(76,0)` inside any wrapper or merge re-receives its parent's
  publication `(65,0)` — the cast's target-domain decimal admission
  (`func_cast.go:7019–7021`, enforcing the `(65,0)` target's `10^65`
  limit) reproduces exactly the parent multiply's own width-keyed
  publication check (`checkDecimal256ResultPrecision`,
  `arithmetic.go:121–150`, the same `10^65` limit for `(38,0)²`
  operands), rejecting exactly the values the parent's
  multiply-time publication rejected, before any branch merge. Discovery
  does NOT condition on the argument head's divergence (revision 7's
  gate, withdrawn): a `COALESCE(a*b, <76-digit literal>)` argument over
  nullable `(38,0)` columns publishes `(76,0)` with envelope `(76,0)` —
  the product seam's `(65,0)` envelope combined with the genuine
  literal's `(76,0)` — so the head matches, revision 7 detected nothing,
  and cancelling `+10^65`/`-10^65` product rows flowed into the
  aggregate although the parent multiply rejected them; the product
  seam's own divergence drives its cast regardless of what the head
  happens to merge to. Where a seam's
  subexpression resolves INTO a materialized relational producer (fact
  11) — one producer column read by N consumers through
  `SINK`/`SINK_SCAN`, a boundary that exists only where the complete
  admission gate of fact 18 passes (at least two occurrences,
  profitability, drain, demand and storage) — placement follows the
  row shape between the seam locus and the admitting consumer (fact
  16). On a
  row-preserving path the finalization emits ONE statement-local
  **envelope-shadow column per (producer, referenced exported slot)**
  whose producing expression carries an admitted producer-representable
  seam (an ordinary projection column — no new expression kind, no
  plan-format change): a structural REPLICA of that slot's own
  producing expression, computed at the producer node where the slot's
  own expression is computed and reading exactly that expression's own
  source columns (fact 16 — every input is already in scope, already
  preserved by pruning for the original column), with each
  producer-representable divergent seam inside the replica carrying
  its ordinary in-place envelope cast — inner-first, the same cast
  machinery COALESCE alignment and the parent's recursive-member
  coercion use — and its coercive binding decisions replayed from the
  decision ledger at the replica's final types (fact 17): the ledger is
  content-keyed (revision 12, F-LEDGER-LINEAGE), so the replica — a
  newly constructed copy with fresh node identities — resolves each
  replicated decision's entry by its own observable signature exactly
  as the original would, needing no identity channel; the
  replica binds its slot's base-equivalent head type. The shadow's
  evaluation demand is the slot expression's own: the executor's own
  `EvalCoalesce`/`EvalCase` machinery evaluates the replica's
  unmodified predicates and arms with the base engine's row masks and
  three-valued selection (fact 15) — nothing computes on rows the
  base engine excludes, and a violating value on a SELECTED arm raises
  the parent's multiply-time admission family at the parent's producer
  demand, so the error surface equals the base engine's on this
  producer. The
  original column stays carrier-exact for every other reader; the
  admitting consumer's shadow-slot reference REBINDS as a direct
  reference to the exported shadow slot and its remaining argument
  structure recomposes in tree — no cross-boundary replay, no read of
  any column the producer did not export (fact 16)
  — and the consumer's result re-derives through the registration from
  the recomposed argument's head type exactly as from any in-tree
  rebound argument. The admitting consumer RECOMPOSES its argument
  over the exports (revision 11, F-SCOPE-SPANNING-ARGUMENTS): each
  shadowed slot reference rebinds directly to the exported shadow
  slot, while argument structure the producer cannot compute —
  columns of other relations joined at the consumer, another
  materialized producer's exports, captured/correlated inputs — stays
  in the consumer's own tree, where consumer-side divergent seams take
  their ordinary in-tree envelope casts at their own positions
  (demand-exact through the same arm masks) and the argument's
  coercive decisions replay from the ledger; no replay dependency
  crosses the boundary in either direction.
  Where a row-changing operator
  intervenes between the seam locus and the producer's output (a dedup
  or DISTINCT step — a shadow would join the dedup key — a grouped
  or window aggregation, or a set operation, whose output rows carry no
  source-row correspondence), no per-row shadow can cross: the seam
  instead takes its ordinary in-tree envelope cast at its locus below
  the FIRST such operator, inside the argument subtree at the seam's
  own position —
  demand-exact like every other in-tree seam cast (it inherits the
  arm's own mask), value-identical on
  the parent's admitted domain, and gated like the shadow on an
  admitting AVG/SUM reader downstream. Shadow columns are
  statement-local and plan-internal: they never appear in result
  columns or on the wire, they flow through the producer's
  SINK/SINK_SCAN schemas as ordinary slots (the finalization's copy
  refresh covers them), they are created only when at least one AVG/SUM
  consumer of that producer actually admits a producer-representable
  seam in the slot's expression on a row-preserving path, ONE shadow
  serves every producer-representable divergent seam of ONE exported
  slot (a multi-seam slot still exports exactly one shadow column, and
  one slot shadows at most once per statement for any number of
  admitted consumers), and
  flattened/inline producers need none — the seam is present in the
  consumer's own tree and is cast in place. Every
  coercive binding decision
  inside the argument — in-tree and inside a shadow replica alike — is
  then replayed over its recovered pre-coercion operands in
  snapshot-first order (facts 8/12/17): at pass entry the owner records
  each in-scope decision's recovered operands WITH their pass-entry
  binding-moment types (recovery skips pass-inserted casts by the
  identity set, takes the single binder-inserted alignment cast per
  position as the unwrap candidate, and reads each folded position's
  original operand domain from fact 17's decision ledger); ownership is VERIFIED by re-invoking the decision's
  registered check over the recovered operands AT THE SNAPSHOT TYPES
  and requiring the regenerated alignment layer to equal the retained
  one bitwise — legitimate final-target differences play no role,
  because the check input equals the input the binder saw; the check is
  then re-invoked over the recovered operands at their FINAL envelope
  types and regenerates exactly its owned alignment layer, so the
  argument head re-types to the common type of its final branches and a
  legitimately changed target regenerates instead of misjudging its own
  alignment foreign; SQL-authored casts and decision-foreign
  fixed-target casts (the seam/head
  admission casts themselves, recursive member→anchor coercions — any
  candidate failing verification) are
  preserved bitwise. The head
  receives the envelope cast only where it still differs from the
  envelope after the replay
  (a mixed `coalesce` head re-types to the envelope `(66,0)` itself and
  needs no root cast; a bare product's head stays `(76,0)` and receives
  the `(65,0)` root cast — the head is simply the outermost subexpression
  subject to the same divergence test; inside a materialized producer
  the shadow replica carries the head and every seam cast, so the
  exported shadow slot IS the rebound argument and the consumer adds
  nothing). The argument's rebound head
  type is by
  construction the envelope, and the call's
  authored result type is re-derived for the rebound argument through
  the same parent
  registration the executor re-derives from.

**Finalization before execution (revision 9, facts 7–18; the
revision-7 S1/S2/S3 global sweeps are withdrawn).** The mutations above
change authored types that the planner has already copied by value, and
revision 7 showed the sweep structure could not repair those copies
correctly: S3's copies-last re-sync read rightly feared mid-mutation
producers, but fact 10 shows copies are also INPUTS to consumer
decisions — a global copy sweep in either direction re-derives at least
one consumer from a stale copied input (the `AVG(a*b)/3` division
reading the pulled-up Col at binding-moment `(65,4)` and executing the
retained scale 8). The pass therefore runs ONE **dependency-ordered
finalization** over the plan's producer/consumer/copy DAG — inside the
same statement-local lifetime and the same structural skip — whose
single rule is: a producer slot finalizes exactly once; every
referencing copy refreshes from it immediately as part of that
finalization; and a consumer re-derives only when every input it reads
(direct child expressions and referenced copies alike) is final.

- **Order and interleaving.** Plan nodes finalize child-to-parent, and
  producer steps before their consumer steps (`query.Steps` exec
  order); within a node, expression trees finalize post-order
  (children before enclosing calls). An AGG/WINDOW node's AVG/SUM
  calls reconcile at their node's position (seam casts inner-first,
  decision replay, head bind, result re-derivation), including the
  TIME_WINDOW node's deep-copied inner aggregate
  (`having_binder.go:979–990`), whose copied argument refreshes from
  the already-finalized real aggregate output and whose result
  re-derives through the registration, so the timewin executor
  (`timewin.go:559–572`) constructs from parent types. The moment a
  slot in the finalized node's output reaches its final type, every
  copy referencing that slot is re-set from it — pull-up projection
  columns (`query_builder.go:2332–2344`, `:2948–2960`), binder
  aggregate/window references (`projection_binder.go:80–100`),
  TIME_WINDOW copies, fill references, and SINK/SINK_SCAN projection
  schemas (`query_builder.go:3310–3313`, `:3231–3243`) across their
  `SourceStep` edges (`:3247–3251`) — BEFORE any consumer tree that
  reads one of those copies is finalized. Each enclosing consumer then
  re-derives its authored type through its registered rule — pure in
  argument types and session state, the same functions the §4.1 mirrors
  use — and replays its coercive binding decisions (facts 12/17): the
  decision's pre-coercion operands are recovered and verified against
  the pass-entry snapshot (pass-inserted casts skipped by identity,
  the single binder-inserted alignment cast per position unwrapped and
  accepted only when the checked regeneration at the SNAPSHOT types
  equals the retained layer bitwise, folded positions recovered per
  fact 17's table), its registered
  check is then re-invoked over them at their final envelope types and
  regenerates exactly its owned alignment layer, so a merge over a
  reconciled `(65,0)` producer aligns its branches to the parent's
  `(65,0)` target, never the stale retained-target `(69,4)` family,
  while SQL-authored casts, the pass's seam/head admission casts and
  recursive anchor coercions are preserved bitwise. Scalar chains
  (`AVG(1)/3` → the parent's `(65,4)` division type, `1/3 = 0.3333` —
  its division now reads the just-refreshed `(65,0)` Col, and never a
  retained scale 8's `0.33333333`), nested and derived aggregate
  consumers, and join/filter/having expressions over reconciled outputs
  all finalize parent-verbatim, each exactly once.
- **Correlation channels finalize in producer order** (revision 8,
  facts 12–13; reusing the execute-time ownership model of
  `refreshPreparedCorrelationExpr`, `correlation_prepare.go:25–50):
  caller-side captured expressions finalize at their own position in
  the caller's trees → the program's `InputTypes` refresh from the
  finalized captures → the callee's `CorrelationSlot` types refresh
  from `InputTypes` → the callee program finalizes internally by this
  same protocol, recursively (its own AVG/SUM reconciliations, its own
  copies; a dependent scalar product carries its §4.1 dependent-rule
  envelope) → `OutputTypes` refresh from the callee's finalized root
  output → the caller's dependent-call result `Typ` refreshes from
  `OutputTypes` (`correlation_binding.go:208–210`, `:215–217`) → the
  caller's consumers of the call (comparisons, aggregates, projections)
  re-derive. `ValidateCorrelation`'s equalities (`correlation.go:216`,
  `:242`) hold on the finalized plan by construction, and the
  `TestDependentPublicPlan` route remains the route oracle.
- **Materialized-boundary admission** (revision 10, facts 11, 15, 16,
  18; scope decomposition and admission accounting in revision 11): when a divergent seam inside an AVG/SUM argument resolves into a
  materialized relational producer (shared-CTE reuse,
  `cte_reuse.go:41–64`, materialized only when the complete admission
  gate of fact 18 passes — two or more occurrences, the profitability
  comparison, the drain requirements, the demand veto and the
  shadow-expanded storage reservation), placement follows the row
  shape between the seam
  locus and the admitting consumer. (i) On a row-preserving path the
  producer gains, at the producer's own finalization, ONE
  statement-local envelope-shadow column per exported slot whose
  producing expression carries an admitted producer-representable
  seam — the structural replica of that slot's own expression with
  each such seam cast in place and its decisions replayed from the
  decision ledger, evaluated by the executor's own short-circuit
  machinery with the base engine's masks and three-valued selection
  (fact 15 — unselected arms evaluate nothing; a selected violating
  row raises the parent's multiply-time family at the parent's
  producer demand); the
  original column stays carrier-exact, every shadow appends strictly
  within the shadow-expanded schema the storage admission already
  charged (fact 18), the shadow slots flow through
  the SINK/SINK_SCAN schemas as ordinary columns covered by the copy
  refresh, and the admitting consumer RECOMPOSES its argument at the
  consumer's own finalization position: shadowed slot references
  rebind directly to the exported shadow slots, and the remaining
  argument structure — other relations' columns at consumer joins,
  other producers' exports, captured inputs — stays in the consumer
  tree with consumer-side seams cast in tree and decisions replayed
  from the ledger (§5 bullet three) — the shadow's inputs are its
  slot's own inputs at the producer (already exported or preservably
  adjacent for the original column), so no replay dependency crosses
  the boundary and no scope-spanning argument asks the producer to
  compute consumer-side structure (facts 11/16; revision 11,
  F-SCOPE-SPANNING-ARGUMENTS). (ii)
  Where a row-changing operator intervenes inside the
  producer (dedup/DISTINCT, grouped or window aggregation, set
  operations — fact 16), no per-row shadow can cross: the seam takes
  its ordinary in-tree envelope cast at its locus below the FIRST such
  operator, value-identical on the admitted domain and demand-exact.
  Both placements are gated on an admitting AVG/SUM reader; sibling
  readers of every statement the parent admitted are untouched and
  producers with no admitting reader gain nothing.
- **Completeness in one traversal.** Every Col expression whose `Typ`
  copies a producer slot is re-set exactly once, at that slot's
  finalization; every consumer re-derives exactly once, after all its
  inputs; the final projection's types — the
  `GetResultColumnsFromPlan` source (`build.go:999–1003`) — carry the
  reconciled metadata to the wire. Producer slots finalize before their
  readers, expression inputs before their enclosing calls, every copy
  reads only finalized slots and every consumer reads only finalized
  inputs, and a second pass can never change a type.
- **Memo validity across mutations.** The §4.3 envelope reads only
  pass-invariant inputs — declared column types, constant literal types,
  registered rules, session parameters and the completed
  statement-local decision ledger — immutable once binding
  completes and released with the generation (revision 16,
  F-BOUNDARY-ALIGNMENT-DOMAINS; a generation that authored a
  prepared CTAS additionally retains a copy claimed onto its
  `PrepareStmt` at publication — or the retry's fresh-generation
  claim or the ordinary prepared-plan rebuild claim — and releases it
  only with the prepared statement's own lifetime (a replacing claim, or
  `PrepareStmt.Close` on DEALLOCATE and eviction — never the specialization-cache
  clearing at `types.go:850`) — revision 17,
  F-DECISION-EVIDENCE-LIFECYCLE; publication revised to the real
  prepared-DDL path in revision 18, F-EVIDENCE-PUBLICATION-PATH,
  decision log 64 (i)-(iii); rebuild publication and the decoupled
  release in revision 19, F-REBUILD-EVIDENCE-PUBLICATION, decision log
  65 (i)-(ii)) — keyed on producer identity
  `(NodeId, slot)` (and the dependent programs' own slot spaces); the
  finalization mutates consumer-position authored types, never those
  inputs. Every memo entry therefore stays valid for the pass's
  lifetime, and induction over the dependency order shows the type the
  protocol assigns each affected consumer equals the evaluator's
  envelope for it.

Only chains that transitively consumed a reconciled aggregate output
change; the deliberate widened-publication expression semantics (§6 M2
per-operator contracts over unreconciled wide operands) are untouched;
the revision-16 ledger-owned pre-alignment look-through (§4.1 casts,
decision log 62) changes no chain at all — it is a storage-envelope
evaluation rule over completed binding evidence, independent of this
pass's aggregate-driven replay — and executor construction reads
reconciled types because it follows
`createQuery`.

Planner AggList/WinSpec authoring and every executor construction path
then read the same argument expression type — the parent's own domain —
and the single registered formula returns the parent's exact result on
it:

- **Fitting products keep parent capacity and rounding verbatim.** A
  `(76,0)`-published product argument binds as `(65,0)` — the parent's
  publication for the same multiply — so `AVG` derives `(65,0)`, a
  fitting `10^62` product averages `10^62` as it did on base, `AVG(2*3)`
  stays `6`, CTAS persists the parent's `(65,0)` AVG column, and
  base-authored AVG view records re-derive identically (M4 restored for
  aggregates).
- **Every other input needs no distinction.** The trigger is *envelope ≠
  published*, a semantic type-divergence predicate computed by the shared
  evaluator — never syntax keyed on multiplication. Genuine wide inputs
  (wide literals, dominated branches, alignment merges) have envelope ==
  typed domain (I4): no cast is inserted, and the formula reproduces the
  parent's exact result — `(66,0)` → `(65,4)`, value `1.5000` over
  `{1,2}` (parent-verified). Declared-domain inputs are already
  parent-typed: no cast. Mixed, wrapped, and derived-table arguments all
  receive merely the domain the parent itself would have bound.
- **Capacity failures keep the parent's checked families, per source and
  at the head.** A value outside any admitted seam's envelope domain
  fails that seam's cast with the declared-domain checked family — the
  same per-value admission the parent's own multiplication publication
  enforced for those values, at the same source boundary the parent
  enforced it (the parent rejected a `10^70` product — and a `10^65`
  product from `(38,0)²` operands — at the multiply with 1690; the
  design rejects both at the product arm's seam cast inside the AVG
  argument with the parent's invalid-input "beyond the range" family,
  even when a merged `(66,0)` head domain would have admitted the
  `±10^65` pair). Any residual value outside the rebound head domain
  fails the head cast with the same family. Sums keep accumulating in
  the raw Decimal256 carrier, and an average that does not fit its
  `(65, s)`
  result domain raises the pre-existing checked division/result-capacity
  failure (`decAvg`: `%s beyond the range, can't be converted to
  Decimal256(65,·)`; `Decimal256 Div overflow`) — byte-identical executor
  behavior.
- **The executor and its tests stay byte-identical to base.** Round 10's
  `avgDecimal256ReturnType` clamp (returning (65,0) for >65 arguments)
  remains withdrawn — it rewrote the genuine (66,0) domain. Revision 2's
  binder Rule-A seam and revision 3's "no seam at all" are both
  superseded: the aggregate consumes a typed argument, and the binder's
  job is to hand it the domain the parent would have. No rule keyed on
  multiplication exists inside the aggregate anywhere in this design.
- **SUM binds at every divergence, including bare products** (revision
  6, F-ADMISSION-CONTRACT — revision 5's "bare-product SUM stays
  cast-free" and revision 4's "SUM needs no counterpart" are both
  withdrawn; admission decomposed per source in revision 7,
  F-PRODUCT-ADMISSION). `SumReturnType` publishes `min(w+22, 65)`
  (`sumavg2.go`):
  identical for `(65,·)` and `(76,·)` bare-product arguments, which
  revision 5 took as license to skip the cast — but the published
  `(76,0)` domain admits product values (e.g. `10^70`, or a canceling
  `+10^70`/`-10^70` pair summing silently to `0`) that the parent's own
  multiplication publication rejected, contradicting the admission
  contract. The uniform divergence trigger wraps the bare-product SUM
  argument head to `(65,0)` exactly as to any envelope (a bare widened
  multiply is simultaneously its own argument's only divergent seam, so
  its seam cast and head cast are the same `(65,0)` cast): fitting sums
  keep the
  parent's `(65,0)` result type, value and capacity verbatim, an
  out-of-domain value raises the parent's checked invalid-input family
  at the argument cast (the parent's own multiply-time admission for
  that value), and the per-row capacity cast is the same documented I8
  cost the AVG families pay — the §9 performance row measures AVG and
  SUM against the same 1.5× budget. A SUM argument that merges a widened
  product with genuinely wide sources (e.g. `COALESCE(d, a*b, <66-digit
  literal>)`) admits per source instead: the product arm's values are
  checked against the multiply's own parent publication at the arm's
  seam cast (±10^65 rejected — the parent's multiply limit — although
  they fit the merged `(66,0)` head), the genuine 66-digit arm flows
  through its identity envelope, and the head keeps the merged (66,0)
  domain — so genuine wide values sum exactly as the parent did while
  no arm admits what its own source rejected. Wrapper-reduced arguments
  such as
  `ROUND((33,32)×(40,32), 0)` published `(42,0)` vs the parent's
  `(34,0)` bind as before, re-deriving `(56,0)`. SUM's pre-existing
  result-publication capacity check (`validateDecimal256SumResult`) is
  unchanged and still governs result-side overflow. The §4.2
  check-two inventory continues to machine-pin which aggregates are
  domain-arithmetic (`avg`, `sum` — family D; the transparent selectors
  are family T) — also as the machine-pinned
  definition of this pass's subject class (§5 header; revisions 7–8).
- **Storage consequence.** AVG and SUM results both cap at the declared
  65 in every branch, so their result columns never invoke the storage
  boundary (§4); the §4 envelope mirror for aggregate slots applies the
  same registered return-type function to the argument envelope — which
  is now literally the argument's bound type after reconciliation —
  keeping downstream metadata
  parent-derived. The reconciliation pass reuses
  the §4.3 evaluator (same memo semantics, statement-scoped); statements
  whose plans contain no decimal multiplication subtree skip it
  structurally and pay nothing, and statements carrying provenance pay
  the §2 I8 cost model (pinned by the §9 performance row).

Result-type cells under the final contract (every parent-computable query
is parent-identical; carrier rows bind the argument in the envelope
domain):

| argument bound at the aggregate | formula result | value contract |
|---|---|---|
| product `(76,0)`-published, envelope `(65,0)` → bound `(65,0)` | `(65,0)` | fitting products average exactly (`10^62` → `10^62`, `2*3` → `6`); a value outside `(65,0)` is checked at the argument |
| product `(76,12)`-published ((38,10)×(38,10)), envelope `(65,12)` → bound `(65,12)` | `(65,12)` | scale-12 values round exactly as the parent |
| product `(76,4)`-published ((38,2)×(38,2)), envelope `(65,4)` → bound `(65,4)` | `(65,4)` | parent capacity/rounding |
| `ROUND((38,2)×(38,2), 2)` published `(75,2)`, envelope `(64,2)` → bound `(64,2)` | `(65,3)` | parent formula on the parent's own wrapper domain |
| mixed `coalesce(d (65,0), a*b, <66-digit literal>)` published `(76,0)`, envelope `(66,0)` → bound `(66,0)` | `(65,4)` | parent-verified: `1.5000` over `{1,2}`; admission is per source (revision 7): the product arm carries its own `(65,0)` seam cast (rejecting `±10^65` products the parent multiply rejected), the genuine 66-digit arm is identity, the rebuilt head is `(66,0)` |
| genuine `(66,0)` (wide literal, no multiplication seam) — no cast | `(65,4)` | `1.5000` for {1,2} — parent-verified, unchanged |
| declared `(65,0)` — no cast | `(65,0)` | unchanged |
| declared `(64,2)` — no cast | `(65,3)` | unchanged |
| `AVG(TRUNCATE((38,10)×(38,10),0)+z(39,0))`, published `(65,0)`, envelope `(54,0)` → bound `(54,0)` | `(58,4)` | parent-verified: `1.5000` over `{1,2}` (round-16 counterexample; §4.1 add/subtract mirror) |
| `SUM(ROUND((33,32)×(40,32),0))`, published `(42,0)`, envelope `(34,0)` → bound `(34,0)` | `(56,0)` | parent result type and checked capacity domain verbatim |
| `SUM(a*b)` bare product, published `(76,0)`, envelope `(65,0)` → bound `(65,0)` (revisions 6–7: uniform class trigger; the bare product is its own only seam, so seam cast = head cast) | `(65,0)` | fitting sums keep parent value/metadata verbatim; an out-of-domain product value is rejected at the argument cast with the parent's multiply-time admission family |

§9 keeps the machine check that makes the single-channel claim auditable:
a UT asserts the planner-authored aggregate result type equals the
registered `aggexec` return type of the argument types as bound across
the matrix above, for plain and window calls, plus plan-shape oracles
(the seam cast is present exactly on each divergent decimal seam inside
an `avg`/`sum` argument plus the head where it still diverges after the
decision replay — discovered head-independently: a genuine 76-digit
COALESCE branch does not suppress the product seam's cast; seams resolving into materialized producers appear per the revision-11 row-shape-and-scope taxonomy — ONE envelope-shadow replica column per referenced exported slot on row-preserving paths covering producer-representable seams (the admitting consumer rebinding each shadowed slot reference to the exported shadow slot and composing the remainder in tree), consumer-side seams cast in tree, and an in-tree seam-locus cast below the first interior row-changing operator where row correspondence breaks — with the producer's original column carrier-exact, no shadow when no admitting reader exists, and the shadow evaluating nothing on arms the base engine's masks exclude because the executor's own machinery evaluates the replica — and present on every
carrier and divergent-wrapper row including bare-product SUM, absent on
the genuine-wide and declared rows, and absent on every value-selecting
decimal aggregate argument even when its envelope diverges from its
published type — revision 9 taxonomy), placement
oracles (the pass runs on the post-flatten, post-remap final plan at
both `createQuery` exits — the main body's end and the dependent route,
the latter exercised through the `TestDependentPublicPlan`-shaped
correlation UT, `correlation_binding_test.go:57–73` — and
rewrites arguments inside flattened scalar subqueries
(`AVG((SELECT a*b FROM t LIMIT 1))`), derived-table column references
(`SELECT AVG(x) FROM (SELECT a*b AS x FROM t) d`), previously pending
grouped outputs (`AVG(a*b) ... GROUP BY k`) and window specifications
(`AVG(a*b) OVER ()`) alike, including a dependent-plan
AVG/SUM reconciliation), finalization oracles (dependency order: every
copy refreshes exactly once at its producer slot's finalization —
asserted by instrumented counters over pull-up columns
`query_builder.go:2332–2344`/`:2948–2960`, references
`projection_binder.go:80–100`, TIME_WINDOW copies, fill references,
SINK/SINK_SCAN schemas across `SourceStep` edges, and the full
correlation channel `InputTypes`/slot types/`OutputTypes`/dependent
result — and every consumer re-derives after all its inputs are final,
so the `AVG(1)/3` division re-derives from the refreshed `(65,0)` Col
and carries the parent's `(65,4)` division type end-to-end, returning
`0.3333`, never a retained scale 8's
`0.33333333`; a shared-CTE witness on the genuine two-reference form `WITH c AS
(SELECT AVG(a*b) AS v FROM t) SELECT (SELECT v/3 FROM c) q, (SELECT v
FROM c) w` asserts the same value/type through a real SINK/SINK_SCAN boundary (fact 18's complete admission gate — two occurrences, and the aggregated producer costs the N-row scan while emitting one row, so the profitability comparison shares it and both readers drain fully); a nested-consumer witness `SUM(a*b)/AVG(a*b)`
and a derived-aggregate witness `SELECT SUM(v) FROM (SELECT AVG(a*b) AS
v FROM t) d` assert parent-verbatim types; the TIME_WINDOW
copy carries the rebound argument and re-derived result type; and final
result-column metadata equals the parent's), decision-ownership oracles
(a replayed decision verifies ownership over its pass-entry snapshot and regenerates its owned alignment layer over the recovered operands at final types (fact 17) — grouped `AVG(COALESCE(d, a*b))` finalizes parent-verbatim `(65,0)` argument and head types, never the stale `(69,4)` family, and the changed-target case (`COALESCE(avg_out, d)`, snapshot `(65,4)` → final `(65,0)` with `d` `(61,0)`) keeps ownership of the retained `(65,4)` layer while regenerating `(65,0)` — while the
pass's inserted seam cast keeps its fixed `(65,0)` target bitwise after
the full finalization (no identity recomputation), a SQL-authored
`CAST(d AS DECIMAL(65,4))` inside the same argument is bitwise
preserved, and a recursive member→anchor coercion keeps its anchor
target bitwise even with a widened product in the member tree),
admission oracles (per source, head-independent: mixed-origin AVG over
arms selecting `+10^65`/`-10^65` products and `+10^70`/`-10^70`
products raises the parent's multiply-time admission family at the
product arm's seam cast — never a silent `0` — including the argument
shape `COALESCE(a*b, <76-digit literal>)` whose head merges to
`(76,0)` == published `(76,0)` (revision 7's masked case); the genuine
76-digit and 66-digit literal controls and the fitting `+10^31`/`-10^31`
control average exactly; the shared-producer cases run on the PROFITABLE two-reference form (fact 18's complete gate) `WITH c AS (SELECT COALESCE(a*b, <66-digit literal>) AS v FROM t WHERE k % 4 = 0) SELECT (SELECT AVG(v) FROM c) av, (SELECT MAX(v) FROM c) mx` — a row-preserving filter at selectivity 1/4 makes two references share profitably on the repository's own cost model — the value statement over fitting rows materializes exactly one SINK producer read by two SINK_SCAN readers and returns AVG parent-verbatim from the exported shadow slot while MAX returns the exact widened row from the original carrier-exact column, and the error statement — the same shape over rows whose product arm selects ±10^65 — is a SEPARATE rejected statement raising the parent's multiply-time family at the shadow's in-place seam cast (a statement-level rejection cannot also demonstrate the sibling's value); demand controls assert that a physically overflowing product on an UNSELECTED COALESCE/CASE arm leaves the statement successful while the same value on the SELECTED arm raises the pre-existing checked overflow family — extended by a NULL earlier CASE-predicate control (the replica keeps the row selected exactly as `EvalCase` does) and a fallible skipped intermediate COALESCE-arm control (`COALESCE(x, a*b, c*d)` with non-NULL x on the eligible materialized producer: neither product evaluates); the no-admitting-reader control (two MAX references) shares the producer with no shadow; and a dedup-, grouped- or set-op-interior variant takes the in-tree seam-locus cast with no shadow column at all;
bare-product SUM cancellation cases; dependent-path cases — outer AVG
of a correlated scalar product returns parent-verbatim values and
metadata, and a reconciled AVG result captured by a dependent program
keeps `InputTypes`/slot/`OutputTypes` equalities with
`ValidateCorrelation` green), and
parent-versus-design value oracles
(fitting `10^62`, `6`, `1.5000`) for ordinary and window aggregates,
CTAS persisted metadata, and base-authored view replay.

## 6. View catalog and expression-result contract (revision 4)

Revision 1 listed CREATE VIEW inside "storage boundary" step 5 but named no
owner, no read contract and no replay story (review F-VIEW witness:
`inferViewColumns`, `build_ddl.go:240–397`, persists inferred expression
types through a separate path; views have no inserted-value admission).
This section is the complete view contract.

**Where view types live.** A view's column records are authored once by
`inferViewColumns` from the optimized plan's head projection and stored in
the catalog (CREATE VIEW, `genViewTableDef` assembly; ALTER VIEW,
`buildAlterView`); the definition SQL is stored in the ViewData JSON beside the
pre-existing `RequiredProtocolVersion` field (base machinery this change
never extends: its unchanged channel keeps writing exactly what it
computes on base — the v89 wide-literal requirement included —
revisions 40/41, decision log 86 (ii)/87 (i)). Reads rebind the stored SQL through
`builder.bindView` (`pkg/sql/plan/query_builder.go:12510`) and description
rebinds through `DescribeViewColumns` → the same `inferViewColumns`
assembly (`pkg/sql/plan/view_regeneration.go`), which is also the
regeneration/replay entry.

**M1 — catalog contract.** View column records follow the identical
storage-boundary transformation as CTAS (envelope → persisted declared
domain): carrier expansions never persist; genuine non-carrier wide domains
keep their parent form. Owner: the single `inferViewColumns` record
assembly, applied identically for authoring (`forAuthoring=true`) and
description (`false`), so catalog, DESCRIBE and replay agree byte-for-byte
and replay is deterministic.

**M2 — expression-result and presentation contract (reads; revision 3,
outer-expression clause revised in revision 4).**
Design review (F-VIEW-PRESENT) verified that revision 2's named mechanism
does not exist for decimals — `mysqlSpecialTypeFromProvenance` returns nil
for every non-ENUM/SET plan type (`mysql_special_types.go:843–848`), so
`restoreViewMySQLSpecialTypes` never restores a decimal view column, and
`metadataViewColumnsSQL` (`view_columns_history.go:27`) only adapts
`information_schema.COLUMNS`; the DIV-only `appendDecimalDivViewBoundary`
(`query_builder.go:12745–12749`) is likewise irrelevant. Revision 3
removes that claim and names the two presentation owners directly. No
reconciliation mechanism is added, because none is needed:

- **Executable values and result-set metadata — the rebound plan is the
  owner.** A view reference rebinds the stored definition, and the
  parent's output binding reads the rebound projection types verbatim
  (`query_builder.go:14050`, `types[i] = &projects[i].Typ`), so
  MySQL-protocol result-set column metadata is the executed head
  projection's own metadata — the carrier domain for a carrier-derived
  view column, exactly as `SELECT a*b` without any view presents its
  published type. Values evaluate with full carrier arithmetic (a
  76-digit product returns 76 digits; the kernel errors only on physical
  overflow, I1), and no narrowing cast exists anywhere at the view seam —
  views own no storage and admit no values. Outer expressions over the
  view column consume the same rebound carrier type and **keep each
  operator's own pre-existing result-domain contract** (revision 4,
  F-VIEW-OUTER-OPERATORS): width-preserving consumers (the §4.2 tier, e.g.
  `abs`, whose decimal256 overload returns the argument type verbatim,
  `list_builtIn.go`) evaluate carrier-exactly with executable metadata —
  `SELECT abs(v.c) FROM v` over a 76-digit product returns 76 digits —
  while domain-narrowing operators keep their unchanged caps and checked
  rejections: addition publishes `min(integerDigits+scale+1, 65)`
  (`decimalAddSubReturnType`, `list_operator.go`), and a sum that leaves
  the declared domain fails the pre-existing shared validator
  (`checkDecimal256ResultPrecision`, `arithmetic.go`) with the 1690
  family — exactly the behavior the parent applied to every >65-typed
  operand, e.g. a genuine wide `(66,0)` view column. Rebinding preserves
  the input type; it never alters what a consumer operator publishes.
  Nested views chain identically: the inner view's rebound plan is
  expanded inside the outer definition and the outer reference again
  reads executable types, per consumer operator. (The ENUM/SET-only
  `restoreViewMySQLSpecialTypes`
  seam remains the parent's prior art for a deliberate
  catalog-versus-executable divergence — set by `bindView`,
  `query_builder.go:12540`, consumed at `:5648/:6517/:13139/:14011` —
  and stays byte-unchanged.)
- **Catalog surfaces — the persisted declared records are the owner.**
  `DESCRIBE v`, `SHOW COLUMNS FROM v` and `information_schema` present
  the view column records authored under M1 — the declared domain,
  including the base-derived widths below 65 that §4.3's verbatim
  boundary rule restores. A declared `(65,0)` view column over a carrier
  expression therefore presents `(65,0)` on every catalog surface and the
  carrier domain on the result set, with values computed exactly up to
  the carrier — the same declared/executable separation M1 gives CTAS,
  carried to its only storage-free consumer. The declared record remains
  honest: it states the domain admission would enforce if the view were
  materialized (M3 admits fitting values, rejects oversized ones), while
  result-set metadata describes the values the statement actually
  returns. Both surfaces are pinned by independent §9 oracles.

**M3 — CTAS from a view.** The CTAS boundary sees the same expanded plan
the executor runs (view expansion is part of the rebound plan), so the
envelope resolves through the view wrapper mirrors to the true producers;
outcomes equal the inline-expression form: declared (65, s) persisted,
fitting values stored, oversized rejected by I3 admission.

**M4 — replay, regeneration, upgrade.** All view definitions authored under
base stored ≤65 column records (base publication cap) and their rebound
types under this design re-derive identical records: every shape base
publishing produced satisfies parent-side == published, so M1's
transformation is the identity on legacy view SQL. No regeneration,
rewrite, or protocol bump is triggered for existing views. Views authored
carrying the §4.1 expansion band persist their reconcile-derived above-65
tags as PLAIN catalog records with no issue-specific
`ViewData.RequiredProtocolVersion` mint (revision 40,
F-SHUTDOWN-UPGRADE-COMPATIBILITY, decision log 86 (ii); scoped in
revision 41, F-PRESERVE-EXISTING-MARKERS, decision log 87 (i)): the
field — pre-existing base machinery whose established gates serve the
base's own features — carries on every record this change authors
exactly the requirement its unchanged channel computes for the same
statement (the v89 wide-literal requirement where the definition
carries a wide literal — `make.go:309–311` marks it,
`string_literal_form.go:1027–1032` discovers it,
`persisted_ip.go:154–155`/`:249–254` records the v89 floor and
`build_ddl.go:462–478` persists it), and revision 34's marker mint,
revision 36's raise channel and revision 37's expansion share are
withdrawn (kept-as-history). Under the
repository's supported shutdown-only upgrade boundary
(`feature-design-review.md:147–150`,
`upgrade-compatibility.md:3–7`/`:54–65`) no mixed-version read path
exists to fence and downgrade is unsupported; inside the boundary the
changed binary replays, regenerates, describes and schema-infers every
base-authored definition byte-identically (I2/I4), and a nested view over
a band inner definition persists the same plain records — the outer
definition's records follow M1 over the expanded plan, with no issue-specific marker at either
level — each level's record carrying exactly the requirement the
unchanged channel computes for its own expanded plan (the outer
expansion visiting the inner's wide literals) — and byte-identical
replay, regeneration, describe and schema inference at both.

**M5 — outcome map** (each row pinned by §9 validation):

| Surface | Contracted outcome |
|---|---|
| CREATE VIEW over `a*b` ((38,0)²) | column record DECIMAL(65,0); definition SQL stored as written |
| SELECT * FROM the view | exact 76-digit values; result-set metadata is the executable carrier domain (M2 value owner) |
| DESCRIBE / SHOW COLUMNS / information_schema | DECIMAL(65,0), identical to the persisted record (M2 catalog owner) |
| Outer expression over the view column — width-preserving consumer (`SELECT abs(v.c)`) | carrier-exact evaluation and executable metadata; no narrowing cast |
| Outer expression over the view column — 65-capped consumer (`SELECT v.c + 1`) | the addition's own unchanged result domain `(65, ·)`; a result leaving that domain fails the pre-existing checked validator (1690 family), exactly as over genuine wide columns |
| Nested view over the same definition | inner record (65,0); outer record follows M1 over the expanded plan; values carrier-exact through both |
| Nested view over a BAND inner definition (restated in revision 40) | the outer definition's records follow M1 over the expanded plan — the reconcile-derived above-65 tags persist as PLAIN catalog records with NO marker at either level; replay, regeneration, describe and schema inference are byte-identical at both levels (M4) |
| CREATE VIEW over `ROUND(a*b, 2)` with (38,2) operands | column record DECIMAL(64,2) — base-derived below-65 width preserved verbatim (§4.3) |
| ALTER VIEW redefining to `a*b` | re-authored records follow M1 |
| CTAS FROM the view | (65,0) persisted via §4; exact fitting values; checked `20301` admission beyond — schema and value oracles independent of the view's result-set metadata |
| Definition replay / `DescribeViewColumns` / regeneration | deterministic, byte-identical to authoring; legacy viewdefs unchanged (M4) |
| View over genuine wide literal domain | keeps parent form (66,0) exactly (I4) |
| View over `percentile_disc(... ORDER BY a*b)` (agg/window) | declared 65 for the carrier; exact returned rows; oversized rows checked at CTAS (none at the view seam) |
| View over `max(a*b)` / `min(a*b)` and every other value-selecting decimal aggregate (revision 7) | no §5 admission cast by construction — the aggregate returns an input row, so SELECT returns the exact carrier rows the widened publication produces; the view record is declared (65,0); oversized rows are checked at CTAS/INSERT only, exactly as for any widened publication |

## 7. Alternatives

Criteria: correctness (exact domains, no lost pre-existing capacity),
complexity, performance, compatibility, operability, testability, migration
cost.

- **A0 — status quo (clamp publication at 65).** Rejected: it is the
  reported bug; fails MySQL-compatibility and the I1 contract for every
  66–76-digit product.
- **A1 — tag-and-propagate provenance at a shared owner.** Attach a
  declared-domain/carrier marker to planner expressions or nodes during
  binding and propagate it through optimizer rewrites into DDL authoring.
  Rejected unchanged, and both review rounds confirm the premise:
  revision 2's AVG Rule-A binder seam was withdrawn in revision 3 exactly
  because the executor needs no plan-annotation channel it cannot have
  (§5): `plan.Expr` has no such field; adding one changes the plan
  protobuf wire format (I8 violation) and makes every rewrite rule that
  rebuilds expressions a provenance consumer that must re-derive or
  preserve it — a strictly larger ownership surface spanning all optimizer
  passes. Provenance would still have to be reconstructed wherever rules
  synthesize new expressions (folding, CTE inlining, set-op rewrites).
  Note the predicate used by I7 (constant folding only) is *structural
  recognition of the one operator that can publish >65*, not a propagated
  tag: multiplication is the only >65 publisher, so spotting it
  syntax-first is complete and rewrite-safe — any rewrite either preserves
  the widened multiplication subtree (carrier intact) or eliminates it
  (no carrier), never fabricates one. No other invariant consumes this
  predicate.
- **A2 — kernel/insert-time admission only, no planner reconstruction.**
  Publish carrier widths everywhere, persist either the (76, *) tag or a
  blind (65, *) clamp, and let insert casts enforce admission. Rejected:
  persisting a (76,0) tag breaks I2 (SHOW CREATE cannot parse/print M>65 —
  parser-level — and catalog replay cannot round-trip the schema), while a
  blind clamp to (65) over-narrows genuinely wide non-multiplication
  domains (I4: e.g. the branch-dominated `(66,0)` inference the base stored
  with its own declared form and admission profile) and provides no basis
  for the checked, shape-specific `20301` admission behaviors of CTE and
  window carriers demonstrated live. Revision 2 keeps this rejection while
  replacing the *fallback default* with the defended declared-domain tier
  (§4.2) — a deliberately asymmetric choice: enumerated function shapes are
  machine-checked complete (I2 dominates), non-enumerable genuine-shape
  tiers keep the authored type (I4 dominates).
- **A3 — reconstruct exact storage envelopes from the final optimized plan
  at the DDL seam (committed).** Single entry (`decimalMulStorageBoundary`)
  for CTAS/prepared-CTAS plus the same transformation at the single
  `inferViewColumns` assembly for views; request-scoped state with the §4.3
  memoized evaluator; zero format/wire/persistence changes; and the input
  it reads is exactly the plan execution will run — schema inference cannot
  drift from execution (TIME_WINDOW slots are read through
  `BuildTimeWindowLayout`, the same layout the operator consumes;
  AGG/WINDOW/join slot addressing mirrors the query_builder conventions
  proven by the plan-shape UTs). Wins on correctness-without-format-change;
  the cost is a maintained mirror of a small set of canonical typing rules
  (each cited to its single source of truth in §4, exhaustiveness-pinned in
  §4.2).
- **Rejected during this fix's own iterations (decision log):** inferring
  multiplication provenance from a folded `Expr_Vec`'s width/vector shape
  (round-8 approach) was replaced by I7's fold-decline rule: folding
  parents of existing Decimal256 scalar vectors also materializes genuine
  non-multiplication selectors, so shape-based inference mis-classified a
  folded genuine (66,0) selector as a carrier and narrowed a
  parent-supported domain (I4 violation, proven live). Provenance must be
  preserved across folding, never reconstructed from the folded shape.

## 8. Compatibility, delivery, operations

- **Behavioral compatibility.** Statements that errored on base (the issue
  contract) now return exact values — intended; carrier-fitting high-scale
  products previously rejected inside the scaled fallback (§1,
  F-FALLBACK-DOMAIN) return too, the same conformance class. No statement
  that returned
  on base changes value or error surface — one named exclusion
  (revision 43, decision log 89): the coefficient-passthrough family of
  §1's success criteria (a prepared set-operation fill whose session
  `div_precision_increment` differs from the internal plan build's and
  whose base materialization ran the pre-existing internal-route
  append-path coefficient-passthrough defect; the intended value is the
  base-evaluated value correctly scale-aligned into the recovered
  persisted domain) — and no statement changes result
  metadata domain except at the widened publication seam itself — where
  the carrier domain is the intended I1 metadata, flowing through §4.1's
  width-preserving consumers exactly as a bare `SELECT a*b` presents it:
  every
  domain-arithmetic decimal aggregate consumes the parent's own argument
  domain on every input (§5 — per-source seam casts for every divergent
  decimal seam plus the head envelope cast where the rebuilt head still
  diverges; no origin
  classification exists inside the aggregate), keeping every
  base-successful average's and sum's metadata, rounding
  and capacity verbatim; every envelope-divergent decimal source seam
  inside an AVG or SUM argument — AVG and SUM
  alike (revisions 6–7: uniform class trigger, admission decomposed per
  source) — re-receives the parent's own publication for that source, so
  an argument value outside the domain its source admitted — including a
  `±10^65` product inside a merged `(66,0)` argument — keeps the parent's
  checked rejection exactly where the parent's multiply performed it,
  while genuinely wide sources flow their exact parent-admitted values;
  value-selecting decimal aggregates (`min`/`max`, `any_value`,
  `max_by`, `first_value`/`last_value`/`nth_value`, `percentile_disc`) by
  construction carry no admission cast (§5) — they return input rows, so
  a base-successful selection over fitting values keeps its exact rows,
  presents the widened seam's carrier metadata for widened inputs (the
  same presentation class as any §4.1 width-preserving scalar consumer),
  and leaves storage admission to the declared-type cast (§4); LAG/LEAD
  reconciliation changes only the previously erroring 66–76-digit
  default-merge outcomes; their newly persisted reconcile-derived tags
  persist as plain catalog records in the parent's own above-65 form,
  no issue-specific marker minted (§6 M4, revisions 40/41: the
  unchanged pre-existing channel's computed requirements — v89 for
  wide-literal definitions included — persist verbatim); the storage boundary persists the parent's own
  inferred schema verbatim (§4.3: carriers persist the same
  `DECIMAL(65, s)` tag the parent's publication authored — base CTAS
  stored fitting values under it and failed oversized ones; width-reduced
  wrappers persist their base-derived widths below
  65, e.g. `DECIMAL(64,2)` for `ROUND(product, 2)`; genuine non-carrier
  wide domains keep their parent-identical persisted form, including the
  parent's pre-existing SHOW CREATE printing limitation for `M>65`); views
  follow §6 (old viewdefs re-derive identical records byte-for-byte, M4);
  insert/update admission uses the pre-existing declared-type cast
  evaluation.
- **Upgrade and compatibility (restated in revision 40,
  F-SHUTDOWN-UPGRADE-COMPATIBILITY, decision log 86 (ii)).** No
  persisted, catalog, wire, or plan-format change (I8). The repository's
  supported upgrade boundary is shutdown-only: in-cluster mixed-version
  operation, downgrade and per-feature protocol gates are out of scope
  (`.agents/skills/mo-dev/references/feature-design-review.md:147–150`,
  `upgrade-compatibility.md:3–7`/`:54–65`). Inside that boundary this
  change is forward-compatible: the changed binary reads, replays,
  rebinds and describes every base-authored persisted schema and view
  byte-identically — carrier columns persisted by the new binary are
  declarable ≤65; genuine non-carrier wide columns persist in exactly
  the form the parent persists; band views and band CTAS tables persist
  as plain catalog records in the parent's own above-65 form with no
  issue-specific `RequiredProtocolVersion` mint (§6 M4; the unchanged
  channel's computed requirements — v89 for wide-literal definitions
  included — persist verbatim, revision 41, decision log 87 (i)) —
  and legacy view records re-derive identically (M4). Executable semantics change on the
  repaired fallback path (§1): an old binary rejects the `(65,40)`
  `10^70` coefficient where the repaired kernel accepts it — a
  conformance change that takes effect cluster-wide at the shutdown
  upgrade and never inside a mixed-version cluster, so no execution
  fence, demotion or destination guard is introduced (revision 6's
  F-MIXED-VERSION apparatus is withdrawn, kept-as-history). The
  PRE-EXISTING gates — the v132/v133/v134 floors and features, the
  persisted-protocol marker channel and its established enforcement —
  stay byte-identical: this change mints no constant, bumps no
  `MORPCLatestVersion` and extends no gate. No migration, restart, or
  rollback story is required beyond the normal shutdown binary
  replacement.
- **Rollout and failure containment.** No feature flag: this is a
  conformance fix to existing DECIMAL semantics. The deployment rule is
  the repository's normal one: a shutdown upgrade replaces every CN
  binary together (revision 40 — no rolling-upgrade placement story
  exists or is needed; revision 6's demotion machinery is withdrawn).
  Failure containment is
  the §4.2 fallback tiers plus the §4.3 bounded evaluator: a boundary bug
  cannot leak a carrier width into the catalog (defended tier clamps; the
  inventory test fails loudly), cannot mis-evaluate cyclically (each slot
  is evaluated at most once against already-final inputs and recursion
  closes on the anchor seed), and cannot blow up planner time (the walk is
  linear in the size of the plan DAG it reads, with a visible per-slot
  evaluation counter). Authorization/tenant boundaries are untouched; no
  new resource that can accumulate (maps are per-statement); execution cost
  follows the §2 I8 cost model — zero on structurally skipped plans, the
  envelope walk plus per-row rebound-argument casts otherwise, budgeted
  and measured by the §9 performance row.
- **Observability.** Errors keep pre-existing code families: 1690-family at
  expression/insert evaluation and the kernel's own `Decimal256 Mul
  overflow` family (physical overflow decided solely by the checked
  256-bit coefficient range on every multiply path — inline and the
  repaired scaled fallback alike, §1), `20301 invalid input
  ... beyond the range` at CTAS value admission and at a rebound
  aggregate argument whose value escapes its base-equivalent domain
  (§5), and an explicit planning
  error for an undeclarable lag/lead default merge — the
  reconcile's derived decimal width exceeding the engine's bound
  (`>65` on the parent, `>76` on the repaired engine — §4.1). No
  placement, demotion, below-floor or catalog-admission family is added
  (revision 40: the mixed-version apparatus and the band's marker
  fences are withdrawn; the pre-existing families keep their base
  triggers untouched). Recursive
  members are admitted per value through the parent's own cast coercion
  (§4.3) — no planning reject exists for member domains. No new
  metrics/operators required.
- **Overlap with merged origin/main work (revision 38: ABSORBED into the
  controlled base refresh, F-TARGET-PROTOCOL-COLLISION).** `origin/main`
  advanced past base `5fe08a33` (revision-38 refresh head `be8eb40ad8`,
  219 commits, merge-base = the pinned base) and contains PR #31034
  (`22fab00d7d86`, "preserve precision of decimal intermediates", with its
  own design `docs/design/decimal-intermediate-precision.md`): it rewrites
  decimal arithmetic type derivation to operand-bound widths (e.g.
  DECIMAL(15,2) products publish (30,2)), retains hard 65-digit caps on
  expression publication (so the #30938 defect remains open), and touches
  files this change also owns (`pkg/sql/plan/function/list_operator.go`,
  `pkg/sql/plan/base_binder.go`, `pkg/sql/plan/function/function.go`,
  `visit_plan_rule.go`, `legacy_integer_decimal.go`, dtype fixtures
  including `decimal.result`). Composition statement: this design's
  multiplication publication supersedes that 65 cap (min(w1+w2, 76) is the
  carrier envelope of #31034's operand-bound widths), the I4 "genuine wide
  domains remain wide" concept exists identically in both designs, and the
  storage-boundary envelope of §4 remains the declared-domain authority at
  every persistence seam. Re-validation against the refreshed base is the revision-38
  implementation turn's own work: the refresh composes this change with
  #31034's restructured derivation (its dispatch semantics and fixtures
  re-checked, its persisted-expression protocol/rebind machinery reviewed
  at the view seam, HEAD-relative code pins re-verified) and with the
  three later protocol allocations (132/133/134), under the 24-file
  overlap surface recorded in decision log 84 (iv).

## 9. Validation plan (cheapest deterministic layers)

Per the UT/BVT contract, each invariant maps to the cheapest seam that can
prove it; the executable ladder follows RED on parent/base → GREEN on the
fixed tree for the same test, then related behaviors, then broader suites.
(The design checkpoint itself claims no test results; the implementation
phase re-presents collected evidence against this plan and runs the
revision-2/-3/-4/-5/-6 additions, marked *(new)*, from scratch.)

| Invariant/behavior | Focused UT | BVT (public path) |
|---|---|---|
| I1 carrier authority; exact 76/66-digit products; scale rule; negative/NULL/overflow controls | `pkg/sql/plan/function` widen + high-scale-mul + support tests with independent integer oracles (kernel-overflow rejection); *(new)* scale-rule oracles: two scale-10 operands publish scale 12 (the pre-existing `min(s1+s2, max(12,s1,s2))` rule, unchanged), `(65,65)×(65,65)` publishes (76,65) with to-scale rounding and no width rejection; *(new)* carrier-range cells independent of digit count: a 77-digit carrier-fitting product such as `(10^65−1)×500000000000` returns exact, while a product whose coefficient reaches 2^255 raises the pre-existing 1690-family kernel overflow; *(new, F-FALLBACK-DOMAIN)* scaled-fallback admission oracles: `(65,40)×(65,40)` operands holding `10^15` return the exact `10^30` at scale 40 through the high-scale dispatch (`scaleAdj < −38`, previously `Decimal256 Mul overflow`); raw-overflow-recovery cells (`d256MulSlowOne` path): an unrounded product beyond `2^255` whose rounded coefficient fits the carrier returns the rounded value; rounded magnitudes reaching `2^255` raise the unchanged `Decimal256 Mul overflow` family, with both-sign boundary cells (`2^255−1` accepted, `2^255` rejected, each sign) and base-acceptance regression cells (every coefficient below `10^65` plus the inclusive `−10^65` boundary stays accepted) | committed `dtype/issue_30938_decimal_mul_precision` exact-value statements at expression level, extended with the 77-digit acceptance, the true-overflow rejection and the high-scale `(65,40)` exact-return statements |
| I2 declarable persisted schemas (CTAS + views) — universal over every non-band shape; the §4.1 expansion band's persistence is I9's forward-compatibility contract (revision 40) | planner CTAS UT table (persisted width assertions) *(new: view record assertions through `inferViewColumns`)*; *(new)* parent-versus-design metadata oracles for width-reduced wrappers, **ROUND and TRUNCATE asserted separately with their own arithmetic**: `ROUND(a*b, 2)` over (38,2) operands persists `DECIMAL(64,2)` (61 integer digits + 2 + 1 carry), `TRUNCATE(a*b, 2)` persists `DECIMAL(63,2)` (61 + 2, no carry), each asserted equal to the parent's own authored schema in CTAS, prepared CTAS, and view replay (`DescribeViewColumns`); *(new, F-BOUNDARY-TRIGGER)* boundary-crossing wrappers whose published width is ≤65: `ROUND`/`TRUNCATE(product, 0)` over (38,10)×(38,10) publish (65,0)/(64,0) but persist the parent's **(54,0)/(53,0)** — trigger is envelope ≠ published, asserted in CTAS, prepared CTAS and view replay; *(new, F-CAPPED-CONSUMERS)* capped width-sensitive consumer chains: `TRUNCATE(product, 0) + DECIMAL(39,0)` publishes (65,0) yet persists the parent's **(54,0)**, and `TRUNCATE(product, 0) / DECIMAL(38,2)` publishes (65,4) yet persists the parent's **(59,4)** — asserted in CTAS, prepared CTAS and view replay; *(new, F-BOUNDARY-ALIGNMENT-DOMAINS)* the scale-changing implicit-alignment boundary merge `COALESCE(TRUNCATE(a*b,0), d)` with nullable `(38,10)` columns and `d DECIMAL(39,1)` — the parent's `(53,0)` operand merging to `(54,1)` while the widened derivation publishes `(64,0)` shadowed by a ledger-owned binder-inserted `CAST(… AS DECIMAL(65,1))` — persists the parent's **(54,1)** in CTAS, prepared CTAS, view authoring, view description (`DescribeViewColumns`) and legacy replay (`RegenerateViewDefinition`) — prepared CTAS exercising the retained prepare-to-execute evidence and the standalone description/replay cells the `inferViewColumns`-held generation (I10 (xiii)–(xiv), revision 17; the prepared-CTAS exercise parameterized in revision 18, F-EVIDENCE-PUBLICATION-PATH/F-PREPARED-CTAS-WITNESS, decision log 64, re-parameterized against the complete prepared cast tree and extended to the ordinary rebuild channel in revision 19, F-REBUILD-EVIDENCE-PUBLICATION/F-SIBLING-CAST-RETENTION, decision log 65, its consumption oracle and reuse witness repaired in revision 20, F-MISS-ORACLE-COLLAPSE/F-REUSE-WITNESS-REACHABILITY, decision log 66, its execute-route oracles corrected in revision 21, F-EXECUTE-CHANNEL-UNREACHABLE, decision log 67, its skip claims scoped to type-preserving fills in revision 22, F-VALUE-ARM-REBIND, decision log 68, its execute rebind's producer lineage completed in revision 23, F-SUPPLY-PRODUCER-DOMAINS, decision log 69, its shared-producer SourceStep edge coverage completed in revision 24, F-SOURCESTEP-LINEAGE, decision log 70, its set-operation branch-domain combination completed in revision 25, F-MULTIPRODUCER-DOMAINS, decision log 71, its set-operation coercion-source recovery completed in revision 26, F-SETOP-ALIGNMENT-RECOVERY, decision log 72, its set-operation common-domain reconvergence completed in revision 27, F-SETOP-COMMON-RECONVERGENCE, decision log 73); the explicit-cast control `COALESCE(CAST(TRUNCATE(a*b,0) AS DECIMAL(65,1)), d)` keeps `(65,1)` verbatim; *(new, revision 38, F-NARROW-CARRIER-ENVELOPE; oracle values corrected in revision 39, F-UNSEAMED-ENVELOPE-PROVENANCE)* NONCONSTANT narrow-carrier oracles against the refreshed parent (constant products fold to literals and never exercise the envelope rule): declared column pairs `DECIMAL(10,0) × DECIMAL(10,0)` persist the parent's `(20,0)`, declared `DECIMAL(18,0) × DECIMAL(17,0)` persists the parent's `(35,0)` and declared `DECIMAL(18,0) × DECIMAL(18,0)` the parent's `(36,0)` (declared precision 17/18 maps to DECIMAL128, `build_util.go:395–401`, so the legacy-D64 adjustment does not apply to declared columns — these are parent-parity pins revision 2's formula coincides with), declared `DECIMAL(38,0) × DECIMAL(1,0)` persists the parent's promoted `(40,0)` (the checked-D128 MUL coefficient bound shifting the promotion boundary; the discriminating declared-pair RED control, revision 2's envelope persisting `(39,0)`), and declared `DECIMAL(38,0) × DECIMAL(38,0)` persists `(65,0)`; the legacy-D64 coefficient-adjustment family is exercised through genuine DECIMAL64(18,0) intermediates — `(a+b)*a` over `DECIMAL(10,0)` columns persists the parent's `(29,0)` and `(a+b)*(a+b)` persists the parent's `(38,0)` (the parent's fixed-`DECIMAL64(18,s)` d64+d64 add publication, `list_operator.go:297–299`, counted 19 by the parent's `decimalMultiplyDomain`), with the unprovenanced reconstruction's `(21,0)`/`(22,0)` as the RED controls (demonstrated by the pre-repair candidate, decision log 85 (ii)) — each asserted equal to the refreshed parent's own authored schema in CTAS, prepared CTAS and view replay; *(new, revision 40, F-OUTPUT-DIVERGENCE-PROVENANCE)* the fixed-domain-barrier witness `CAST(a*b AS DECIMAL(10,0)) + d` over `(38,0)` columns with `d DECIMAL(10,0)` persists the parent's `(18,0)` — the SQL-authored cast contains the widening below it and the parent's fixed-`DECIMAL64(18,s)` d64+d64 add publication (`list_operator.go:296–299`) fires above the barrier — with the revision-39 wrapper-disjunction reconstruction's `(11,0)` as the RED control, asserted equal to the refreshed parent's own authored schema in CTAS, prepared CTAS and view replay | SHOW CREATE round-trip assertions per witness family *(new: view DESCRIBE/SHOW COLUMNS rows, including the (64,2) ROUND and (63,2) TRUNCATE wrapper witnesses and the (54,0)/(53,0) boundary-crossing pair and the revision-16 (54,1) scale-changing merge beside its (65,1) explicit-cast control)* |
| I3 checked admission (20301/1690-family) | — | oversized admission cases (columns, CTE recursion, time-window, percentile_disc); AVG capacity overflow is the execution-side checked division witness of I6, never a CTAS admission |
| I4 wide-inference preservation (wide literals, alignment, dominated branches, folded selectors; value-merge common-domain reconvergence *(new in revision 12, F-COMMON-DOMAIN-OVERFLOW; re-placed at the binder-owned channel in revision 13, F-RECONVERGENCE-CHANNEL; extended to the tag/ordinal pending-producer channel in revision 14, F-AGG-WINDOW-CHANNEL; completed to the full same-block result-tag inventory in revision 15, F-GROUPED-CHANNEL, with decimal TIME_WINDOW outputs resolved through the `timeTag` descent in revision 16, F-TIMEWINDOW-CHANNEL)*) | provenance UT table incl. folded-selector and guard cases; *(new in revision 12; re-placed and extended in revision 13)* reconvergence UT table: at the binder's source-domain funnel gating the reconvergence families' `GetFunctionByName` lookups (`base_binder.go`), a widened product operand whose merged `requiredWidth` exceeds 76 against a scaled sibling retries over resolver-computed parent-published operand types and binds the parent's own target — `COALESCE((38,0)×(38,0), CAST(1 AS DECIMAL(65,1)))` binds `(66,1)` with parent-domain alignment casts, never a bind failure — through DIRECT operands, a DERIVED-TABLE column (`SELECT COALESCE(v, CAST(1 AS DECIMAL(65,1))) FROM (SELECT a*b AS v FROM t) s`), a VIEW column, a CTE column (inlined and materialized forms) and a SCALAR-SUBQUERY operand (`SELECT COALESCE((SELECT a*b FROM t LIMIT 1), CAST(1 AS DECIMAL(65,1)))`) alike, with the resolver-coverage UT asserting the deferral set EMPTY across that channel corpus; *(new in revision 14, F-AGG-WINDOW-CHANNEL)* the corpus extends to the current query block's own pending producers: a direct-aggregate merge `SELECT COALESCE(AVG(a*b), <76-digit literal>) FROM t` — the provisional `AVG(a*b)` `(65,4)` against `(76,0)` needs `requiredWidth` 80 where the parent's `(65,0)` merges to `(76,0)` — retries and binds the parent's `(76,0)` with parent-domain alignment casts, a window form `SELECT COALESCE(AVG(a*b) OVER (), <76-digit literal>) FROM t` retries identically through `ctx.windows`, and both execute with parent-equivalent values, result types and CTAS-persisted parent-verbatim metadata; a genuine-wide aggregate control (an aggregate over a genuine 76-digit literal column, no multiplication provenance) never retries and keeps the family's own pre-change outcome; and the deferral-set-empty assertion covers the extended corpus; *(new in revision 15, F-GROUPED-CHANNEL)* the corpus completes the same-block result-tag inventory: the grouped merge `SELECT COALESCE(a*b, CAST(1 AS DECIMAL(65,1))) FROM t GROUP BY a*b` — the widened group output `(76,0)` against `(65,1)` needs `requiredWidth` 77 where the parent's `(65,0)` merges to `(66,1)` — reaches the merge through the `ColRef{groupTag, colPos}` reference minted at `projection_binder.go:56–65`, resolves the retained bound group key through `BindContext.groups[colPos]` with `groupOutputType`'s adjustment (`types.go:1093–1098`), retries and binds the parent's `(66,1)` with the parent-domain alignment cast on the group reference, executing with parent-equivalent values, result types and CTAS-persisted parent-verbatim metadata; the HAVING form `SELECT a*b FROM t GROUP BY a*b HAVING COALESCE(a*b, CAST(1 AS DECIMAL(65,1))) > 0` resolves identically through `having_binder.go:55–66`; the projection-fallback form `SELECT a*b AS p, COALESCE(p, CAST(1 AS DECIMAL(65,1))) FROM t` — an alias of a non-column-reference projection binds through the `projectTag` fallback (`base_binder.go:682–692`) — descends `ctx.projects[colPos]` and retries to the parent's `(66,1)`; the ORDER BY ordinal control `SELECT COALESCE(a*b, CAST(1 AS DECIMAL(65,1))) FROM t ORDER BY 1` mints `ColRef{projectTag, 0}` (`order_binder.go:380–390`) and plans parent-verbatim over the already-retried projection; the sampled-output control binds `sampleTag` references whose descent into `ctx.sampleFunc.columns[colPos]` (`build_sample.go:76`/`:96`) recovers the same parent domains; a genuine-wide grouped control (a group key over a genuine 76-digit literal column, no multiplication provenance) never retries and keeps the family's own pre-change outcome; and the deferral-set-empty assertion covers this completed inventory; *(new in revision 16, F-TIMEWINDOW-CHANNEL)* the corpus completes with decimal TIME_WINDOW outputs: the INTERVAL merge `SELECT COALESCE(MAX(a*b), CAST(1 AS DECIMAL(65,1))) FROM t INTERVAL(ts, 1, minute)` — the provisional widened retained output `(76,0)` against `(65,1)` needs `requiredWidth` 77 where the parent's `(65,0)` merges to `(66,1)` — reaches the merge through `ColRef{timeTag, colPos}`, resolves the retained output through `ctx.times[colPos]` and its `ColRef{aggregateTag, aggColPos}` argument into `ctx.aggregates[aggColPos]`, retries to the parent's `(66,1)` and executes with parent-equivalent values, result types and CTAS-persisted parent-verbatim metadata; the HAVING form `SELECT MAX(a*b) FROM t INTERVAL(ts, 1, minute) HAVING COALESCE(MAX(a*b), CAST(1 AS DECIMAL(65,1))) > 0` retries identically; a genuine-wide INTERVAL control (a genuine 76-digit literal column, no multiplication provenance) never retries and keeps the family's own pre-change outcome; the timestamp-boundary control (`_wstart`/`_wend` `Expr_Col` self-references in `ctx.times`) resolves irrelevant by construction, folding and binding base-identically; and the deferral-set-empty assertion covers the TIME_WINDOW cells; identical-type genuine-wide controls ((76,0) genuine literal against (65,1), the same surface type the widened product supplies) never drive a retry and keep the family's own pre-change outcome (bind failure or FLOAT64 fallback unchanged); the LEAST/GREATEST silent-degrade control retries exactly when the first lookup returns FLOAT64 over decimal-family inputs with multiplication provenance; and the comparison control keeps the base saturation fallback (`list_operator.go:272`) | *(new)* parent acceptance, value and metadata oracles per family over fitting values: mixed-scale `COALESCE(a*b, CAST(1 AS DECIMAL(65,1)))` over (38,0)² columns returns the parent's values typed `(66,1)`; conditional/CASE and IFF forms of the same merge return parent-verbatim values and metadata (never a bind failure); LEAST/GREATEST over the same operands return DECIMAL-typed parent values and metadata, never the FLOAT64 degradation; a 76-integral-digit product value at the COALESCE seam raises the parent's checked cast family at the parent-domain alignment cast; comparison equality control `a*b = d` over fitting values returns parent-verbatim results with the unchanged saturation fallback, plus the genuine-wide-literal witnesses persisting with base-identical admission, incl. through a view (M5); *(new in revision 15, F-GROUPED-CHANNEL)* the grouped `GROUP BY a*b` form, its HAVING variant and the projection-fallback variant of the mixed-scale merge return the parent's values typed `(66,1)` with parent-verbatim metadata and CTAS persistence |
| I5 bounded evaluation *(rewritten in revision 4)* | *(new)* deterministic once-per-slot traversal-count UTs with the evaluator counter: shared producer diamonds (the shared slot is counted once regardless of fan-out, including duplicated `SELECT x, x` outputs), cross-column recursive feedback (closed by the anchor seed), k-member `greatest(p1..pk, i)` recursive CTE (**one seed per union slot and zero recursive-member slot evaluations** — member envelopes contribute nothing to the persisted schema; exact count asserted) | recursive + shared-reader CTAS witnesses with exact values; *(new, F-RECURSIVE-ADMISSION)* paired recursive-member admission cases: (a) a member with an **unused wide branch** and fitting produced values succeeds (member `CASE WHEN n=0 THEN <66-digit literal> ELSE p END` over a (65,0)-anchored product CTE returns and persists exactly the fitting rows, no planning reject) and (b) a produced value outside the persisted anchor envelope raises the checked I3 admission at materialization — both mirroring the parent's value-based member-to-anchor coercion |
| I6 decimal aggregate base-equivalent argument domain *(rewritten in revision 5; trigger narrowed to the `{avg, sum}` class, admission decomposed per source and coercions replayed in revision 7, both `createQuery` exits covered; revision 8: seam discovery head-independent, materialized-boundary admission at the producer boundary, decision-owned replay, and the dependency-ordered finalization protocol with just-in-time copy refresh; revision 9: demand-guarded companions, the row-shape placement taxonomy, recovered-operand replay, and genuine two-reference materialization witnesses; revision 10: ONE envelope-shadow replica column per admitted argument, snapshot-first two-phase replay with the explicit fold-recovery table, and profitable filtered materialization witnesses; revision 11: per-slot scope-decomposed shadows with consumer composition, the bind-time decision ledger replacing the fold-recovery table, and shadow-expanded admission accounting; revision 12: content-keyed copy-stable decision ledger with positional folded-position ownership, the fold-entry NULL-provenance decline, and the value-merge common-domain reconvergence)* | *(new)* planner↔executor consistency UT: the planner-authored aggregate result type equals the registered aggexec return type of the argument types as bound across the §5 matrix — envelope-cast cells bound `(65,0)`→`(65,0)`, `(65,12)`→`(65,12)`, `(65,4)`→`(65,4)`, `(64,2)`→`(65,3)`, the round-16 add-chain AVG cell `(54,0)`→`(58,4)`, the SUM divergence cell `(34,0)`→`(56,0)`, the bare-product SUM cell argument `(65,0)`→result `(65,0)` (uniform class trigger — the `(76,0)` argument is wrapped at its seam), and untouched genuine/declared cells `(66,0)`→`(65,4)`, `(65,0)`→`(65,0)`, `(64,2)`→`(65,3)` — for plain and window calls, proving the single unmodified channel (byte-identical aggexec; the argument merely carries the parent domain); *(new, revised in revisions 6–8, F-ADMISSION-CONTRACT/F-SELECTOR-AGGREGATES/F-HEAD-GATE/F-PRODUCER-BOUNDARY; revised in revisions 9–11, F-SCOPE-SPANNING-ARGUMENTS)* plan-shape oracles: inside an AVG/SUM argument, the envelope cast is present on **every** decimal seam whose own §4.3 envelope differs from its own published type, discovered head-independently and cast inner-first (the AVG carrier cells, the add-chain AVG cell, the SUM below-43 cell, the bare-product SUM `(76,0)`→`(65,0)` seam and a mixed-origin argument's product arm alike — result-type agreement is no gate, and the argument head's agreement is no gate either: a `COALESCE(a*b, <76-digit literal>)` over nullable `(38,0)` columns publishes and envelops `(76,0)` yet still carries its product seam cast), with the head cast present exactly where the rebuilt head type still diverges from the argument envelope (present for a bare product, absent for a mixed `coalesce` whose rebuilt head is the envelope `(66,0)`); a seam resolving into a materialized relational producer is admitted through ONE statement-local envelope-shadow column per referenced exported slot at the producer (for a positively proven replay-safe slot — an unproven slot takes the in-place channel instead, revision 31) — each shadow replicates its slot's own producing expression with the producer-representable seam casts in place and its coercive decisions replayed from the decision ledger, it is built producer-side where every input of that expression is still in scope (projection pruning preserves admitted columns), the executor's own CASE/COALESCE machinery evaluates the replica on the producer's path so per-row demand and three-valued selection equal the base engine's by construction, each shadow crosses the boundary as an ordinary exported SINK/SINK_SCAN projection slot, and the admitting consumer recomposes its argument — shadowed slot references rebound directly to the shadows, consumer-side structure (other relations, other producers' exports, captured inputs) retained in tree with its consumer-side seams cast in place — while the original column stays carrier-exact for every reader, and no shadow exists when no AVG/SUM reader admits a seam (plan-shape probes: the single-reference form `WITH c AS (SELECT a*b AS v FROM t) SELECT MAX(v) FROM c` never materializes (fact 18's occurrence gate) and stays shadow-free; the genuine two-reference profitable form `WITH c AS (SELECT COALESCE(a*b, <66-digit literal>) AS v FROM t WHERE k % 4 = 0) SELECT (SELECT AVG(v) FROM c) av, (SELECT MAX(v) FROM c) mx` materializes exactly one SINK producer with exactly ONE envelope-shadow column serving both SINK_SCAN readers; the scope-spanning join witness `WITH c AS (SELECT COALESCE(a*b, <66-digit literal>) AS v, k FROM t WHERE k % 4 = 0) SELECT (SELECT AVG(COALESCE(c.v, u.d)) FROM c JOIN u ON c.k = u.k) av, (SELECT MAX(v) FROM c) mx` materializes one SINK with exactly ONE shadow slot (for `v`), every reference in the emitted plan resolving to an exported slot with no unexported-column read, the AVG consumer's recomposed argument being `COALESCE(shadow_v, u.d)` with no producer-side replica of `u.d`, and the independent MAX reader consuming the plain carrier-exact slot; the two-reference all-MAX form carries no shadow; and a dedup-, grouped- or set-op-interior producer carries no shadow at all — its probe asserts the in-tree seam-locus cast sits below the first row-changing operator); and no cast appears on any value-selecting decimal aggregate argument (`min`/`max`, `any_value`, `max_by`, `first_value`/`last_value`/`nth_value`, `percentile_disc`) even where its envelope diverges from its published type, nor on arguments where envelope = published (genuine-wide and untouched declared arguments); *(new, F-BINDING-PHASE/F-DEPENDENT-EXIT)* placement oracles: the pass runs on the post-flatten, post-remap final plan at both `createQuery` exits — the main body's end (`query_builder.go:4593`) and the dependent route (`:4318–4320` via `createDependentQuery`), the latter exercised by a dependent-plan reconciliation UT shaped on the pre-existing route test (`TestDependentPublicPlan`, `correlation_binding_test.go:57–73`) asserting the correlated plan's AVG/SUM calls reconcile and the correlation `OutputTypes` and dependent-call result types re-sync — and rewrites arguments inside flattened scalar subqueries (`AVG((SELECT a*b FROM t LIMIT 1))`), derived-table references (`SELECT AVG(x) FROM (SELECT a*b AS x FROM t) d`), previously pending grouped outputs (`AVG(a*b) ... GROUP BY k`) and window specifications (`AVG(a*b) OVER ()`), with the authored result re-derived through the same parent registration; *(new, F-DOWNSTREAM-PROPAGATION/F-SYNC-ORDER; revised in revision 8, F-INPUT-REFRESH/F-DEPENDENT-ENVELOPE)* finalization-order UT oracles: instrumented dependency-order counters assert every copy refresh happens as part of its producer slot's finalization, exactly once, and before every consumer that reads it re-derives — a plan walk asserts every copied slot type (final-projection and window-projection copies `query_builder.go:2332–2344`/`:2948–2960`, referencing-expression copies `projection_binder.go:80–100`, TIME_WINDOW deep copies `having_binder.go:979–990`, SINK and SINK_SCAN projection schemas `:3310–3313`/`:3231–3243` across their `SourceStep` edges `:3247–3251`, and the full correlation channel — capture-derived program `InputTypes` and callee `CorrelationSlot` types, `OutputTypes` `correlation_binding.go:208–210`, dependent-call result types `:215–217`, finalized in the producer order of `correlation_prepare.go:25–50`) equals its producer's **final** authored type, with `ValidateCorrelation`'s elementwise equalities (`correlation.go:216`/`:242`) asserted on the finalized plan; no copy of a downstream consumer finalizes from a mid-mutation producer, and no consumer re-derives from an unrefreshed copy; every scalar/aggregate consumer of a reconciled output is re-typed through its own registered rule at its dependency position, with the `AVG(1)/3` division reading the just-refreshed Col and asserted at the parent's scale-4 type (`0.3333` — never a retained scale-8 `0.33333333`), nested-aggregate and derived-table consumers asserted parent-verbatim, and the statement's final result-set metadata asserted equal to the re-derived authored type; memo validity is asserted by re-running the evaluator over the finalized subtree and requiring identical results (envelope inputs are pass-invariant); *(new, F-COERCION; revised in revision 8, F-CAST-OWNERSHIP)* decision-replay UT oracles: the replay unit is the enclosing coercive binding decision — re-invoking the decision's registered check over its recovered pre-coercion operands (fact 17: pass-inserted casts skipped by the pass-local identity set, the decision's pre-coercion operands snapshotted at pass entry, the single binder-inserted alignment cast per position verified owned by re-deriving the ORIGINAL layer over the snapshot operands at their snapshot types and comparing bitwise against the retained layer, folded positions recovered from the decision ledger — keyed by CONTENT (overload identity, arity, per-position pre-coercion domains and post-merge aligned types), captured at pass entry and surviving DeepCopyExpr copies, explicit-cast boundary folds, typed-NULL folds and vector materialization, a folded position verified OWNED positionally where its artifact's type equals the recorded aligned type, anything without an entry decision-foreign and preserved bitwise, and an observable-state ambiguity (two entries matching one node's observable key while differing in a folded position's recorded domain) raising a checked planning failure — with a coverage probe asserting every in-scope decimal coercive decision holds an entry and the ambiguity set is empty, and a projection-inlining control asserting a DeepCopyExpr'd admitted decision resolves its origin's entry and replays parent-verbatim; the paired folded-constant controls `AVG(COALESCE(avg_out, CAST(1 AS DECIMAL(64,0))))` and `AVG(COALESCE(avg_out, CAST(1 AS DECIMAL(65,1))))`, whose folded artifacts are identical at `(68,4)`, regenerate the parent's fresh targets `(65,0)` and `(66,1)` respectively, each asserted equal to a fresh parent bind's final types, values and persisted metadata, and the paired typed-NULL control (`CAST(NULL AS DECIMAL(65,1))` versus an untyped NULL) recovers the recorded `(65,1)` so its genuine scale feeds the regenerated merge) at their final envelope types regenerates exactly its owned alignment layer, so an implicit branch-alignment cast baked at binding with a stale target — grouped `AVG(COALESCE(d, a*b))` whose `(76,0)`-era alignment target differs from the parent's final-branch target — is regenerated to the parent's recomputed target and the argument and head finalize parent-verbatim `(65,0)` (never the stale `(69,4)` family produced by re-applying the binder arithmetic to retained-cast branches; the UT asserts the recovered-operand rebind equals the parent's fresh rebind and that a re-check over the retained cast tree would have produced `(69,4)`; a legitimately-changed-target control — `AVG(COALESCE(avg_out, d))` with `avg_out` snapshotted `(65,4)` and finalized `(65,0)` over `d` `(61,0)` — verifies the ORIGINAL `(65,4)` layer owned at snapshot types and regenerates `(65,0)`, never the retained `(65,4)`); decision-foreign fixed-target casts are preserved bitwise — the pass's own seam/head admission cast keeps its fixed `(65,0)` target (no identity recomputation destroys admission), a recursive member→anchor coercion keeps its anchor target with a widened product in the member tree, and a SQL-authored `CAST(d AS DECIMAL(65,4))` keeps the authored target (probe: the rebuilt plan's explicit-cast node is unchanged); *(new; revised in revision 8, F-TRANSPARENT-CONSUMERS)* aggregate divergence-classification UT: every decimal aggregate's registered result is identical between the envelope-bound and the published argument domains across the §4.2 seam pairs, or classifies as family T (transparent propagation — the §4.1 copy/merge rules, explicitly including the value-selecting families whose result necessarily equals the selected input's type) or family D (domain arithmetic — exactly `avg`, `sum`, the machine-pinned definition of the pass's subject class), with any unclassified divergence failing deterministically; aggexec unit oracles unchanged from parent | *(new)* parent-versus-design value and metadata oracles for ordinary and window AVG: fitting-product cells — 38,0×38,0 operands holding `10^31` (product `10^62`) average `10^62` typed `(65,0)` exactly as the parent, `AVG(2*3)` = `6` not `6.0000`; genuine control `AVG(coalesce(d,<66-digit literal>))` over `{1,2}` → `1.5000` with (65,4) metadata; mixed-origin control `AVG(coalesce(d, a*b, <66-digit literal>))` → `1.5000` with parent `(65,4)` metadata, the product arm's values admitted through its own `(65,0)` seam cast, the literal arm identity, no origin classification; wrapped-product control `AVG(ROUND(a*b,2))` → parent `(65,3)` metadata; capacity witnesses — a value outside any admitted seam's domain raises the parent's declared-domain checked family at that seam, and an average escaping its `(65, s)` result domain raises the pre-existing checked division/result-capacity family (`Decimal256 Div overflow` / "beyond the range"); *(new)* CTAS of AVG(product) and of genuine AVG both persist the parent's own tags ((65,0) and (65,4) respectively); *(new)* base-authored AVG view replay re-derives records identical to the persisted ones ((65,0) for the product case); *(new, F-CAPPED-CONSUMERS)* the round-16 counterexample in plain and window form: `AVG(TRUNCATE((38,10)×(38,10),0)+DECIMAL(39,0))` over `{1,2}` returns `1.5000` typed `(58,4)` parent-verbatim — never `2` typed `(65,0)` — and `SUM(ROUND((33,32)×(40,32),0))` authors `(56,0)` with the parent's checked capacity family; *(new, F-BINDING-PHASE)* value/type oracles through the placement shapes: scalar subquery, derived-table tag, grouped and window AVG each return the parent's typed values; *(new, F-DOWNSTREAM-PROPAGATION)* end-to-end propagation oracles: `AVG(1)/3` over `(38,0)²` product columns holding `1` returns the parent's exact value `0.3333` with scale-4 division metadata — never the stale scale-8 binding's `0.33333333` derived against the pre-reconciliation `(65,4)` — and the `10^31`-scale variant `AVG(a*b)/3`, whose quotient needs 62 integer digits against the parent division's `(65,4)` 61-digit capacity, raises exactly the parent's checked division/result-capacity family (parent-verbatim error surface; the runnable scale discrimination lives in the operands-1 cell); nested scalar and aggregate consumers (`AVG(a*b)+1`, `SUM(a*b)/AVG(a*b)`, derived-table and tag references) keep parent-authored types end-to-end, a TIME_WINDOW consumer over a reconciled aggregate returns the parent's values and types, and the statement's final result-set metadata equals the pass's re-derived authored type; *(new, F-ADMISSION-CONTRACT/F-PRODUCT-ADMISSION; extended in revision 8, F-HEAD-GATE/F-PRODUCER-BOUNDARY/F-DEPENDENT-ENVELOPE)* independent per-source cancellation/admission oracles: a mixed-origin argument `COALESCE(d, a*b, <66-digit literal>)` selecting product values `+10^70` and `−10^70`, and — the revision-6 head-cast hole — product values `+10^65` and `−10^65` (each fits the merged `(66,0)` head but exceeds the `(38,0)²` multiply's parent publication), each raise the parent's multiply-time invalid-input family at the product arm's own `(65,0)` seam cast — never a silently-cancelled `0`, and never a successful query — while a genuine 66-digit literal control (no multiplication, admitted by its own source's published type) and the fitting `+10^31`/`−10^31` product control average exactly (`1.5000`-family and `0` respectively); the revision-7 head-gate hole — `COALESCE(a*b, <76-digit literal>)` over nullable `(38,0)` columns (head published and envelope both `(76,0)`) selecting `+10^65`/`−10^65` product rows fails at the product seam's `(65,0)` cast and never silently averages to `0`, and a genuine 76-digit literal control fits and averages exactly; bare-product SUM over a `10^70` product value raises the same checked admission at its `(76,0)`→`(65,0)` seam cast, and fitting bare-product SUM keeps the parent's value and `(65,0)` metadata; the materialized-boundary witnesses run on the genuine two-reference profitable form (fact 18's complete gate) `WITH c AS (SELECT COALESCE(a*b, <66-digit literal>) AS v FROM t WHERE k % 4 = 0) SELECT (SELECT AVG(v) FROM c) av, (SELECT MAX(v) FROM c) mx`: the fitting value statement materializes exactly one SINK producer read by two SINK_SCAN readers and returns AVG parent-verbatim from the exported shadow slot while MAX returns the exact widened row from the original carrier-exact column — with an unfiltered two-reference DECLINE control asserting both references inline (no SINK, no shadow) — and the separate `±10^65` error statement is rejected at the shadow's in-place seam cast with the parent's multiply-time family (the two assertions live in separate statements); the scope-spanning join witness `WITH c AS (SELECT COALESCE(a*b, <66-digit literal>) AS v, k FROM t WHERE k % 4 = 0) SELECT (SELECT AVG(COALESCE(c.v, u.d)) FROM c JOIN u ON c.k = u.k) av, (SELECT MAX(v) FROM c) mx` returns AVG parent-verbatim through the recomposed `COALESCE(shadow_v, u.d)` argument alongside the MAX sibling's exact widened row, every emitted reference resolving to an exported slot, with its own separated `±10^65` error statement rejected at the shadow seam; demand controls pin the base engine's own arm semantics on the ELIGIBLE materialized producer — an unselected overflowing arm leaves the statement successful and a selected overflowing arm errors with the pre-existing checked family, a NULL earlier CASE `WHEN` predicate leaves the row pending exactly as `EvalCase`'s mask does, and `COALESCE(x, a*b, c*d)` with non-NULL `x` never evaluates either product arm; an AVG/SUM over a dependent (correlated) scalar product evaluates the callee product under the §4.1 dependent-rule envelope, re-receives the parent publication at its seam and returns the parent-verbatim value and metadata (a fitting correlated `10^62`-average executes exactly as its uncorrelated equivalent) with `ValidateCorrelation` green |
| I7 folding provenance *(decline moved to the fold walk's recursive entry in revision 12, F-NULL-FOLD-PROVENANCE; decision barrier gated at both fold engines in revision 13, F-LEDGER-COLLISION)* | decision-barrier UT pair at BOTH fold engines' recursive entries (the rule engine's `foldExpr` and the sibling `constantFoldWithPreparedExactSource`): a fold attempt inside a barrier-flagged recorded decision's operand subtree is DECLINED (the binder-inserted alignment cast and the operand's declared cast syntax survive to pass-entry capture), a speculative fold over a `DeepCopyExpr` copy declines identically, a decision flagged through the revision-14 tag/ordinal pending-producer channel (F-AGG-WINDOW-CHANNEL) — its widened operand the current query block's own pending aggregate or window output — enters the barrier at recording time and declines identically, a decision flagged through the revision-15 grouped, sampled or projected descent (F-GROUPED-CHANNEL) — its widened operand the block's pending group key, sample column or projection fallback output — enters and declines identically, non-flagged casts and decisions without multiplication provenance fold exactly as base, and plan-shape/value controls assert a protected expression evaluates value-identically to the parent's folded form (constant-dependent grouping/ordering outcomes unchanged; the retained executable shape may differ from the parent's folded plan — the documented §10 corner); *(prior)* | plan-shape UT: wide-mul trees keep executable form; genuine/narrow folds still materialize; *(new)* fold-entry coverage UTs: every destructive fold shape of a provenance-carrying constant tree is declined at `foldExpr` entry — a valued product (`2*3`-family at widened types), the NULL-product `CAST(NULL AS DECIMAL(38,0)) × CAST(NULL AS DECIMAL(38,0))` typed (76,0) (which pre-revision-12 folded to an `Isnull` literal via `GetConstantValue`'s early return, keeping (76,0)), and an enveloping wrapper fold over a NULL-producing widened tree — each keeps its executable expression shape with the widened type intact, while the inner `MaterializeDecimal256Constant` check stays green as defense | folded-selector persistence witnesses; *(new)* plain and prepared CTAS and view controls for NULL products and NULL-producing wrappers: each persists the parent's own declared schema (e.g. `DECIMAL(65,0)` for the (38,0)² NULL product) with parent-verbatim DESCRIBE/SHOW COLUMNS records, exactly as the parent's folded-NULL statement persists; genuine typed-NULL control `CAST(NULL AS DECIMAL(65,1))` (no multiplication provenance) still folds and persists unchanged; *(new in revision 16, F-TIMEWINDOW-CHANNEL)* a decision flagged through the TIME_WINDOW descent (a merge operand resolved through `ctx.times[colPos]` and its `ColRef{aggregateTag, aggColPos}` argument) enters the barrier at recording time and declines folding identically; the timestamp-boundary `_wstart`/`_wend` self-references fold base-identically |
| Consumer inventory *(extended in revision 5; check two re-founded on the two divergence families in revision 8)* | *(new)* registry exhaustiveness UT (§4.2): check one — every decimal-output overload + aggregate return type with >65-capable input either caps ≤65 or names a §4.1 rule (whitelist = inventory); check two (F-CAPPED-CONSUMERS; reformulated in revision 8, F-TRANSPARENT-CONSUMERS) — every decimal consumer's result types across each envelope/published seam pair are either identical, or the consumer classifies as family T transparent propagation (a §4.1 copy/merge rule over the types of value-producing inputs — explicitly including the `ReturnFirstArgType` selector families, which necessarily diverge) or family D domain arithmetic (`round`/`truncate`/`floor`/`ceil`, `add`/`subtract`, `divide`, `avg`, `sum` — the §5 pass's subject surfaces), and an unclassified divergence fails the UT deterministically | — |
| I8 execution-cost budget *(new in revision 5, F-PERF-PREMISE — replaces the waived-benchmark premise; revision 7: cast-bearing surface rescoped)* | structural-skip plan-shape oracles already prove zero casts and zero walk on provenance-free statements; planning-cost UT asserts the envelope walk/reconciliation runs once per affected statement and is skipped otherwise; *(new, revision 7)* the cast-bearing per-row surface is exactly the divergent seams inside AVG/SUM arguments (value-selecting aggregates pay nothing), and a mixed-origin argument pays one arm seam cast per divergent arm while its genuine arms flow cast-free — measured as a variant of (a) below; *(new, revision 8; revised in revisions 9–11)* the dependency-ordered finalization's planning-side work — snapshot-first coercion-decision replay over snapshot-and-ledger-recovered operand domains, just-in-time copy refresh and shadow-or-locus-cast construction — stays inside the same one-pass-per-statement counter budget, a materialized-boundary seam on a row-preserving path adds ONE statement-local envelope-shadow replica column per referenced exported slot at the producer whose per-row demand replicates the slot expression's own through the executor's machinery (sibling readers pay nothing), the admission reservation charges the shadow-expanded schema before the share commits (fact 18 — never retroactively), a row-changing interior moves the same per-row cast in tree to the seam locus with no extra column, and a shadow-creation oracle asserts no shadow column exists when no AVG/SUM reader admits a seam | focused parent-versus-candidate microbenchmarks (execution in the implementation phase): (a) plain `AVG(a*b)` and (b) `AVG(a*b) OVER (PARTITION BY ...)` over `(38,0)²` columns holding `10^31` at bench scale (the base runs this fitting-value query successfully, giving a direct baseline for the added per-row capacity cast), plus (a″) the mixed-origin `AVG(COALESCE(d, a*b, <66-digit literal>))` variant carrying the arm seam cast, plus (a‴) the materialized-boundary `WITH c AS (SELECT COALESCE(a*b, <66-digit literal>) AS v FROM t WHERE k % 4 = 0) SELECT (SELECT AVG(v) FROM c) av, (SELECT MAX(v) FROM c) mx` variant — a genuine two-reference profitable materialization: exactly one SINK producer, two SINK_SCAN readers, exactly ONE envelope-shadow column (the single-reference form never reaches the boundary (fact 18's occurrence gate) and remains as the first inline control, and the unfiltered two-reference form declines sharing as the second); (a′) bare-product `SUM(a*b)` over the same columns (revisions 6–7's uniform class trigger adds bare-product SUM to the cast-bearing families — its arm seam and head cast coincide); (c) a provenance-free control statement asserting no measurable delta; (d) CTAS planning time over a wide-plan synthetic asserting the linear walk bound with the §4.3 counter. **Acceptance budget:** (a)+(a′)+(a″)+(a‴)+(b) within 1.5× base execution time (the ordinary cast's per-row Format+Parse in `decimal256ToDecimal256`); (c) within noise; (d) linear. If the measured cast cost exceeds the budget, the implementation may substitute a behavior-identical same-scale limb-comparison capacity kernel (semantics-neutral, covered by the same UT family) — the semantics contract never changes |
| I9 forward compatibility under the supported shutdown-upgrade boundary *(restated in revision 40, F-SHUTDOWN-UPGRADE-COMPATIBILITY; revision 6's floor/demotion apparatus, revision 34's band marker fence and revision 38's v135 re-mint withdrawn, decision log 86 (ii))* | *(new)* forward-compatibility round-trips: every base-authored view and CTAS schema family in the I2/I3 tables replays, regenerates, describes and rebinds byte-identically on the changed binary (the pre-existing replay fixtures executed new-binary-only); *(new, revision 41 — F-PRESERVE-EXISTING-MARKERS, superseding revision 40's absolute no-marker form, decision log 87 (i))* marker-preservation assertions: this change adds NO issue-specific `ViewData.RequiredProtocolVersion` mint, floor or fence, and the unchanged pre-existing channel keeps recording exactly what it computes on base for the same statement — (a) INHERITED-NONZERO-MARKER cells: the gate-disappearance band witness view `LAG(a*b,0,N76)` over `(38,0)` columns holding 1 persists `(76,0)` WITH the channel's v89 requirement (the N76 literal marked by `decimalLiteralRequiresV82`, base `make.go:309–311`; discovered at `string_literal_form.go:1027–1032`; recorded at `persisted_ip.go:154–155`/`:249–254`; persisted at `build_ddl.go:462–478`), and the parent-supported wide-literal view `LAG(N66,0,N66)` (reconciliation skipped for matching types, base `base_binder.go:9084`) persists parent-identically WITH its base v89 requirement on the changed binary; (b) MIXED-FEATURE cells: a view whose definition also carries a pre-existing feature's requirement (e.g. a wide literal beside another computed requirement) persists the unchanged channel's computed maximum, the band adding nothing; (c) ABSENCE cells only where no existing feature requires a marker: the CAST-default band witnesses `LAG(a*b,1,CAST(0 AS DECIMAL(65,1)))` and `LAG(a*b,1,CAST(0 AS DECIMAL(30,10)))` persist `(66,1)`/`(75,10)` with the field at its base default; and no widened-multiplication or band pipeline carries any new `RequiredRemoteExpressionFeatures` bit over its Query/Pipeline trees; *(new)* untouched-gate guards: `pkg/defines/const.go` gains no `MORPCVersion135` and `MORPCLatestVersion` keeps its base value (diff-level pins), and the base's own v132/v133/v134 feature UTs run unmodified; *(new)* band admission behavior: the `(66,1)` and `(75,10)` with-default witnesses and the no-multiplication `(70,70)` scale-70 shape return exact values and persist plain catalog records in the parent's above-65 form, the `(85,20)` above-bound control keeps the checked rejection with the base-identical `LAG/LEAD common precision exceeds SQL limit 65` text on the decimal coalesce family, and the parent-supported shapes (absent, NULL or matching defaults; no-default forms over widened products or genuinely wide columns) persist parent-identical records; *(withdrawn)* the floor-rejection, demotion, destination-guard and marker-mint UT families and the real mixed-version cluster run — no mixed-version execution exists under the supported boundary | cross-version replay fixtures and SHOW CREATE round-trips per family |
| I10 final-plan reconciliation architecture *(new in revision 7, F-COERCION/F-SYNC-ORDER/F-DEPENDENT-EXIT; restructured in revision 8, F-INPUT-REFRESH/F-CAST-OWNERSHIP/F-PRODUCER-BOUNDARY; revised in revisions 9–10, F-COMPANION-DEMAND/F-ROW-CHANGING-BOUNDARY/F-REPLAY-RECOVERY/F-MATERIALIZATION-WITNESS then F-GUARD-SEMANTICS/F-PRUNED-PROJECTION-DEPS/F-POST-FOLD-RECOVERY/F-PROFITABLE-MATERIALIZATION; revision 11, F-SCOPE-SPANNING-ARGUMENTS/F-FOLD-DESTROYS-DOMAINS/F-SHADOW-ADMISSION-ACCOUNTING; revision 12, F-LEDGER-LINEAGE/F-FOLDED-LAYER-WITNESS/F-NULL-FOLD-PROVENANCE/F-COMMON-DOMAIN-OVERFLOW; revision 14, F-AGG-WINDOW-CHANNEL/F-REGISTRY-LIFECYCLE; revision 15, F-GROUPED-CHANNEL/F-VIEW-DESCRIPTION-LIFECYCLE)* | *(new, revised in revision 8)* dependency-order UT: instrumented finalization counters assert each producer slot finalizes exactly once, every referencing copy refreshes at that moment (never earlier, never later, never twice), and every consumer re-derives only after all its inputs — direct children and referenced copies alike — are final (no consumer re-derivation reads a stale copy; no copy of a downstream consumer — final projection, SINK/SINK_SCAN schema, correlation channel — finalizes from a mid-mutation producer); *(new)* dual-exit UT: a reconcilable correlated statement (AVG/SUM over a widened product inside a dependent subquery planned through `createDependentQuery`, route per `correlation_binding_test.go:57–73`) is reconciled identically at the dependent exit — argument seams cast, result re-derived, the full correlation channel (program `InputTypes`, callee `CorrelationSlot` types, `OutputTypes` `correlation_binding.go:208–210`, dependent-call result types `:215–217`) finalized in producer order (`correlation_prepare.go:25–50`) with `ValidateCorrelation` green — and the same statement forced through the main exit reconciles identically at `:4593`; *(new, revised in revision 8, F-CAST-OWNERSHIP; revised in revisions 10–11, F-POST-FOLD-RECOVERY/F-FOLD-DESTROYS-DOMAINS)* decision-ownership replay UT: each coercive binding decision's pre-coercion operands are snapshotted at pass entry — pass-inserted casts skipped by a pass-local identity set, the single binder-inserted alignment cast per position verified OWNED by re-deriving the original alignment layer over the snapshot operands at their snapshot types and comparing bitwise against the retained layer (a legitimately-changed-target control — `AVG(COALESCE(avg_out, d))` with `avg_out` snapshotted `(65,4)` and finalized `(65,0)` over `d` `(61,0)` — verifies the original `(65,4)` layer owned, then regenerates `(65,0)`), fold-decision positions recovered from the bind-time decision ledger (keyed by CONTENT — overload identity, arity, per-position pre-coercion domains and post-merge aligned types — captured at pass entry and surviving DeepCopyExpr copies, explicit-cast boundary folds, typed-NULL folds and vector materialization; a folded position verified owned positionally where its artifact's type equals the recorded aligned type; anything without an entry decision-foreign and preserved bitwise; an observable-state ambiguity — unreachable for barrier-flagged decisions whose operand syntax the fold barrier preserved to capture — raising a checked planning failure as MECHANISM-BUG defense in depth (any ungated future fold path trips it in CI), the coverage probe asserting every in-scope decimal coercive decision holds an entry and the ambiguity set empty, the BARRIER-COVERAGE probe asserting every flagged decision's operand cast syntax survives to pass-entry capture, and the projection-inlining control asserting a copied admitted decision resolves its origin's entry and replays parent-verbatim; *(new, revision 13, F-LEDGER-COLLISION)* the single-statement folded-twin regression `SELECT COALESCE(v, CAST(1 AS DECIMAL(64,0))), COALESCE(v, CAST(1 AS DECIMAL(65,1))) FROM (SELECT AVG(a*b) AS v FROM t) s` executes successfully — never a planning failure — binding the parent's two distinct result types `(65,0)` and `(66,1)`, returning parent-equivalent values, and persisting parent-verbatim metadata via CTAS, each twin's retained operand syntax resolving its own ledger entry at capture); *(new, revision 14, F-AGG-WINDOW-CHANNEL)* the same-block pending-aggregate twins `SELECT COALESCE(AVG(a*b), CAST(1 AS DECIMAL(64,0))), COALESCE(AVG(a*b), CAST(1 AS DECIMAL(65,1))) FROM t` — the `AVG(a*b)` output pending in `BindContext.aggregates` while both COALESCEs bind — execute successfully, binding the parent's two distinct result types `(65,0)` and `(66,1)`, returning parent-equivalent values and persisting parent-verbatim CTAS metadata, both decisions flagged into the barrier through the tag/ordinal channel before folding — *(new, revision 15, F-GROUPED-CHANNEL)* with the grouped pending-producer twins `SELECT COALESCE(a*b, CAST(1 AS DECIMAL(64,0))), COALESCE(a*b, CAST(1 AS DECIMAL(65,1))) FROM t GROUP BY a*b` alongside: both SELECT-list `ColRef{groupTag, colPos}` references resolve the pending group key through `BindContext.groups[colPos]`, both decisions flag into the barrier before folding, and the statement likewise executes successfully with the parent's `(65,0)` and `(66,1)`, parent-equivalent values and parent-verbatim CTAS metadata — its registered check is then re-invoked over the recovered operands at their final envelope types and regenerates its owned alignment layer with parent-arithmetic targets (`setSafeDecimalWidthAndScaleFromSource` over final recovered branch types — the `AVG(COALESCE(d, a*b))` stale-target family finalizes `(65,0)`, never the `(69,4)` a re-check over the retained cast tree would produce), while decision-foreign fixed-target casts are preserved bitwise: the pass's own seam/head admission cast keeps its fixed target (no annotation channel — a plan-shape probe asserts the seam cast's `(65,0)` target survives replay), recursive member→anchor coercions keep their anchor targets, and SQL-authored casts stay untouched; *(new, F-PRODUCER-BOUNDARY; revised in revisions 9–11, F-SCOPE-SPANNING-ARGUMENTS/F-SHADOW-ADMISSION-ACCOUNTING)* materialized-boundary UT: the two-reference profitable witness form `WITH c AS (SELECT COALESCE(a*b, <66-digit literal>) AS v FROM t WHERE k % 4 = 0) SELECT (SELECT AVG(v) FROM c) av, (SELECT MAX(v) FROM c) mx` materializes exactly one SINK producer read by exactly two SINK_SCAN readers (plan-shape assertions; the single-reference form never materializes and the unfiltered two-reference form DECLINES sharing, fact 18) and emits exactly ONE envelope-shadow column per referenced exported slot on the row-preserving path (for a positively proven replay-safe slot — an unproven slot takes the in-place channel instead, revision 31) — the producer-side replica of that slot's own expression with its producer-representable seam casts in place, evaluated by the executor's own CASE/COALESCE machinery; demand controls pin the base engine's exact arm semantics on the ELIGIBLE materialized producer — an overflowing value on an UNSELECTED COALESCE/CASE arm leaves the statement successful while the same value on the SELECTED arm raises the checked family, a NULL earlier CASE `WHEN` predicate leaves the row pending exactly as `EvalCase`'s mask does, and `COALESCE(x, a*b, c*d)` with non-NULL `x` never evaluates either product arm; the consumer's recomposed argument reads each exported shadow slot directly (no replay dependency crosses the boundary), the original column's slot stays carrier-exact (a sibling `MAX` reads it), SINK/SINK_SCAN schemas carry the shadow slots through the copy refresh, a dedup-, grouped- or set-op-interior variant carries the in-tree seam-locus cast below the first row-changing operator with no shadow, and a producer with no admitting AVG/SUM reader gains no column; the scope-spanning join witness `WITH c AS (SELECT COALESCE(a*b, <66-digit literal>) AS v, k FROM t WHERE k % 4 = 0) SELECT (SELECT AVG(COALESCE(c.v, u.d)) FROM c JOIN u ON c.k = u.k) av, (SELECT MAX(v) FROM c) mx` adds `u.d` at the consumer join: exactly one shadow slot (for `v`), the recomposed argument `COALESCE(shadow_v, u.d)` with no replica of consumer-side structure, every reference resolving to an exported slot, AVG parent-verbatim and the independent MAX reader carrier-exact; admission-accounting oracles: the reservation is charged on the shadow-expanded schema BEFORE the share commits — a one-shadow producer's charged row size and spill framing include exactly one `Decimal256` delta, a multiple-shadow producer (several referenced slots, several admitted arguments sharing slots) charges per referenced slot never per consumer, multiple producers accumulate their expanded charges in the cumulative process counters, a declined expanded reservation leaves both cumulative counters byte-identical (all-or-nothing) and inlines every occurrence, and the no-admitting-reader control emits no shadow and never exceeds the charged schema; *(new, revision 14, F-REGISTRY-LIFECYCLE)* generation-lifecycle UT family: (i) consecutive statements on one reused process — the second statement's generation sees an empty barrier/ledger and the proc field is restored to nil after each outermost exit; (ii) a failed build followed by a clean build — the defer-registered release detaches on the error path before the next generation installs, and a panic-unwind control asserts the identical restore during unwinding; (iii) a compile-retry double generation (`buildPlanForCompileRetry`) — the retry generation starts empty and re-records parent-verbatim; (iv) nested planning — a `BuildPlan` entry inside an active `Optimize` generation (the EXPLAIN inner build, `buildExplainPlan`, `build.go:595` → `:602`) shares the outer registry through the depth marker, and the optimizer's fold rules at `optimize.go:29`/`:32` running after the nested `BuildPlan` returns still consult it, so a flagged twin's operand syntax survives to capture, and the outermost exit detaches; (v) prepared double-`Optimize` independence (`prepared_binding.go:1037`/`:1048`) — each generation starts empty; and (vi) an instrumented enter/exit discipline assertion — depth balanced and the exact prior field value restored on every terminal path (success, error, panic unwind); *(new, revision 15, F-VIEW-DESCRIPTION-LIFECYCLE)* the standalone view-description lifecycle cells: (vii) a standalone `DescribeViewColumns`/`RegenerateViewDefinition` generation with NO enclosing `BuildPlan`/`Optimize` — a persisted view definition carrying the folded-twin COALESCE pair installs a fresh registry at the `inferViewColumns` entry (`build_ddl.go:240`; revision 17, F-DECISION-EVIDENCE-LIFECYCLE, decision log 63(i)) — holding through the funnel call at `build_ddl.go:301`/`:308` AND the column-record assembly at `:345–387`, the funnel's own entry-60 invocation joining nested through the depth marker — keeps the twins' operand syntax unfolded through capture, derives the parent's `DECIMAL(65,0)`/`DECIMAL(66,1)` description records, and restores the process field on return; (viii) repeated standalone calls on one process — the runtime view-columns table-function path (`view_columns.go:124–160`) and the SHOW view metadata path (`view_description.go:109–175`) each start against an empty registry; (ix) standalone error and panic-unwind restoration — a view definition whose binding fails releases the field on the error path before the next call installs, the panic-unwind control asserts the identical restore during unwinding, and the publication-probe bind-only return (`build.go:145–151`) releases identically; (x) nested-call preservation — the funnel entered inside an active outer generation (the main SELECT build at `build.go:741`/`:748`, PREPARE at `build_dcl.go:394`, CREATE VIEW's `genViewTableDef` inference at `build_ddl.go:408`) shares the outer registry through the depth marker and the outermost exit restores; and (xi) recovery-loop freshness — `regenerateViewUsingPersistedEnvironment` over the recovery process reused across pending views (`view_metadata_recovery.go:1314–1340`) installs fresh per view, a failed regeneration poisoning none of the remaining pending views; and the revision-16 additions — (xii) *(F-TIMEWINDOW-CHANNEL)* TIME_WINDOW folded twins: `SELECT COALESCE(MAX(a*b), CAST(1 AS DECIMAL(64,0))), COALESCE(MAX(a*b), CAST(1 AS DECIMAL(65,1))) FROM t INTERVAL(ts, 1, minute)` flags both decisions through the `timeTag` descent before their operand syntax folds and executes with the parent's `(65,0)`/`(66,1)`, parent-equivalent values and parent-verbatim CTAS metadata; (xiii) *(F-BOUNDARY-ALIGNMENT-DOMAINS; placement corrected in revision 17, F-DECISION-EVIDENCE-LIFECYCLE, decision log 63(i))* standalone view-column assembly reads completed decision evidence: inside the `inferViewColumns`-OWNED generation — installed at `build_ddl.go:240` with no enclosing `BuildPlan`/`Optimize` and spanning the funnel call at `:301`/`:308`, the post-funnel validators AND the column-record assembly at `:345–387` — the storage evaluator's decision-owned look-through derives the parent's `(54,1)` description record for `CREATE VIEW v AS SELECT COALESCE(TRUNCATE(a*b,0), d) FROM t` (`(38,10)` operands, `d DECIMAL(39,1)`) on BOTH standalone paths (`DescribeViewColumns`, `view_regeneration.go:142–155`; `RegenerateViewDefinition`, `:262` → `genViewTableDef` at `:271`), with the SQL-authored explicit-cast control keeping `(65,1)`, and the process field restored to its exact prior value — registry released empty — after the final consumer on success, error and panic unwind alike; (xiv) *(new, revision 17, F-DECISION-EVIDENCE-LIFECYCLE; fixture parameterized and publication revised in revision 18, F-EVIDENCE-PUBLICATION-PATH/F-PREPARED-CTAS-WITNESS, decision log 64; rebuild channel and complete-cast-tree fixture in revision 19, F-REBUILD-EVIDENCE-PUBLICATION/F-SIBLING-CAST-RETENTION, decision log 65; decision-owned ≤65-cast fixture in revision 20, F-MISS-ORACLE-COLLAPSE/F-REUSE-WITNESS-REACHABILITY, decision log 66; execute-channel unreachability verified and the execute oracles and entry-path probe corrected in revision 21, F-EXECUTE-CHANNEL-UNREACHABLE, decision log 67, its skip claims then scoped to type-preserving fills and the provisional common-VALUE execute rebind given its decision-evidence lifecycle in revision 22, F-VALUE-ARM-REBIND, decision log 68)* the prepared-CTAS refresh boundary with NO enclosing generation: `PREPARE p FROM 'CREATE TABLE t2 AS SELECT TRUNCATE(COALESCE(TRUNCATE(a*b, 0), d), ?) FROM t'` over `(38,10)` operands with `d DECIMAL(39,1)`, EXECUTE with `?=0` — the inner literal-digits TRUNCATE refines at PREPARE itself (widened `(64,0)`, parent `(53,0)`), so the widened COALESCE family `(65,1)` diverges from the parent's `(54,1)` — a recorded decision — with the binder's column-only alignment cast of the TRUNCATE arm into `(65,1)` at width ≤65 — `d` keeping its declared `(39,1)` under the binder's same-OID same-scale cast skip (`base_binder.go:8990–9050`, `:9002–9003`) — decision-owned through the merge's retained ledger entry and surviving specialization non-provisionally (`base_binder.go:5108–5111`; `visit_plan_rule.go:3017–3025`, `:4823–4852`), while the HEAD's digits argument a parameter marker holds `refineDecimalRoundingReturnType`'s TRUNCATE refinement at its early return (`base_binder.go:9360–9396`): the PREPARE snapshot carries the unrefined head `(65,1)`, its storage-envelope tag persisting the parent's `(54,1)` — as on the parent itself — and NO execute route re-derives a prepared DDL plan's inferred output types: the specialized fill is substitution-only on every route (`computation_wrapper.go:431`/`:1922`/`:3085` feeding the refresh at `:436`/`:1926`/`:3089`), the ordinary rebuild builds the PREPARE statement with markers again (`rebuildPreparePlan`) and the round/truncate prepared-numeric fallback marks the VALUE argument only (`bindPreparedNumericPrecisionFuncExpr`, `base_binder.go:3399`–`:3514`), so the filled head keeps `(65,1)` on both engines, and the pre-existing per-column `reflect.DeepEqual` skip (`build_ddl.go:1627`–`:1628`) compares the filled query's output type against the ORIGINAL query's output type — equal for every function-headed column on any fill that leaves the inferred output types unchanged — this fixture's digits-marker fill among them — so on this fixture every EXECUTE reuses the snapshot tag through the skip (a fill that changes an output column's inferred type defeats the skip; that value-arm channel and its evidence lifecycle are cell (xv), decision log 68): the observable oracle on every execute route is the parent's `(54,1)` persisting, exactly the parent's own execute (its own snapshot's `(54,1)` reused identically), protocol-floor rebuild executes re-deriving `(54,1)` from their OWN generation's live registry at binding; the evidence-consumption discrimination executes where it genuinely runs — the PREPARE-time ledger probe (the coalesce decision entry consumed by the snapshot evaluation deriving `(54,1)`, its `(65,1)`-declared empty-ledger ablation proving the tag is evidence-derived) and a LABELED harness probe bypassing the unreachable production skip: the filled execute-shape tree evaluated DIRECTLY against the retained carrier re-derives `(54,1)`, the same evaluation with the ledger withheld re-deriving `(65,1)` from the unverified arm cast's declared envelope (decision log 62's conservative branch), genuinely different and establishing that any future reachable execute-locus evaluation yields the parent's value WITH evidence and the conservative envelope WITHOUT — REFRESH-NEVER-RAN being the snapshot tag `(54,1)`, value-equal to consumption on this fixture and distinguished from it only by the harness probe, recorded as such; the authored explicit-cast control `TRUNCATE(COALESCE(TRUNCATE(a*b, 0), CAST(d AS DECIMAL(65,1))), ?)` stores `(65,1)` at PREPARE and persists `(65,1)` at EXECUTE on BOTH engines with and without the retained carrier (head equal to tag, the skip leaving it untouched); the separate no-parameter control `TRUNCATE(COALESCE(TRUNCATE(a*b, 0), d), 0)` refines at PREPARE itself and persists the parent's `(53,0)` from prepare-time evaluation alone with refresh never reached (`numParams > 0` gate, `computation_wrapper.go:1921`); `FillValuesOfParamsInPlanWithSpecialization` (`utils.go:4635`) refreshing retained entries over its re-derived subtrees, the generation-keyed process-side claim slot read-once-and-cleared at claim, the carrier released only by a replacing claim, `PrepareStmt.Close`, or the claiming wrapper scope's failure cleanup — DECOUPLED from the specialization-cache clearing (`computation_wrapper.go:1641`, `types.go:850`) — the carrier shipping as read-only conservative insurance with NO reachable widened-decimal consumption trigger in the current planner (decision log 67 (vi), its removal considered and rejected) — superseded in revision 22: entry 68 gives the carrier a genuine consumer as the read-only base of the merged execute-evidence argument on the newly reachable value-arm locus, its insurance role otherwise standing; and the rebuild validation split (decision log 66 (iv)-(v), value oracles corrected in 67 (v)): Witness A, the live-service BVT, asserting its formation probe FIRST — the stored plan's `DatabaseDefaults` non-nil for the ordinary-database fixture — then per-execute rebuilding under the PRE-EXISTING CTAS rebuild policy, preserved verbatim (`shouldRebuildPreparePlan` unconditional at `computation_wrapper.go:2834–2837` via `rebuildEveryExecute` `:1444` into `needRebuild` `:1477–1480`), each execute's rebuild generation re-recording the decision and consuming its OWN live registry with the parent's `(54,1)` persisting and each replacing claim releasing the displaced carrier; Witness B, the cached-generation-reuse UT formation, asserting its preconditions FIRST — `DatabaseDefaults` absent (the established `MOProtocolVersion` test pattern with save/restore, `expression_protocol_test.go:75–120`) and no intervening invalidation (`:1423–1425`, `:1437–1442`, `:1468–1477`, `:2256–2265`, `:1480–1492`) — then asserting NO rebuild occurred (`planGenerationReused` `:1494`, no fresh claim) BEFORE nonce identity: the second EXECUTE retains the SAME PREPARE-claimed carrier untouched with no further claim — the PREPARE-derived `(54,1)` reused through the skip — and a single controlled `ddlVersion` flip produces exactly one rebuild-and-replace whose following quiescent EXECUTE reuses the rebuilt carrier; (xv) *(new, revision 22, F-VALUE-ARM-REBIND, decision log 68, its execute rebind's producer lineage completed in revision 23, F-SUPPLY-PRODUCER-DOMAINS, decision log 69, its shared-producer SourceStep edge coverage completed in revision 24, F-SOURCESTEP-LINEAGE, decision log 70, its set-operation branch-domain combination completed in revision 25, F-MULTIPRODUCER-DOMAINS, decision log 71, its set-operation coercion-source recovery completed in revision 26, F-SETOP-ALIGNMENT-RECOVERY, decision log 72, its set-operation common-domain reconvergence completed in revision 27, F-SETOP-COMMON-RECONVERGENCE, decision log 73)* the prepared-CTAS VALUE-arm rebind boundary: `PREPARE p FROM 'CREATE TABLE t3 AS SELECT COALESCE(TRUNCATE(a*b, 0), ?) AS v FROM t'` over `(38,10)` operands — the PREPARE snapshot persisting the engine-independent provisional `VARCHAR(65535)` on both engines (the pre-existing peer-resolution provisional behavior, both arms varchar-cast) — EXECUTE `?=1.25` typed `DECIMAL(20,6)` and `?=0.5` typed `DECIMAL(20,1)`: specialization replaces the provisional common-value casts and rebinds the enclosing COALESCE (`ResetParamRefRule`, `utils.go:6678`/`:4752`–`:4760`; `visit_plan_rule.go:3320`–`:3346`, `:3861`–`:3879`), the filled head becoming `(70,6)`/`(65,1)` so the per-column skip is defeated and the execute-route boundary evaluation genuinely runs (probe: DeepEqual false, filled ledger keys absent from the retained carrier); the execute process threaded through the rule so the rebound coercive decisions record into the execute-scope registry and reach `RefreshPreparedCTASInferredColumns` merged over the retained carrier (decision log 68 (iv)); persisted-tag oracles `(59,6)` for `?=1.25` and `(54,1)` for `?=0.5` on BOTH engines, proven against an independently derived parent channel — the prepared cast-pinned form `COALESCE(TRUNCATE(CAST(a*b AS DECIMAL(65,12)), 0), ?)` whose per-execute refresh outputs `(59,6)`/`(54,1)` ARE the parent's tags (probe-verified, carrier-independent); the missing-evidence ablation asserting `?=0.5` deriving `(65,1)` — the pre-revision-22 divergence the probe observed — with the execute recordings withheld, and `?=1.25` deriving `(59,6)` with or without (its `(70,6)` alignment cast looked through by §4.1's >65 rule); binary-NULL and text bindings (`specialized=false`) firing the skip and persisting the snapshot tag on both engines as skip controls; Witness A extended to assert the formed execute-scope entries on each per-execute rebuild route and Witness B to assert the cached-plan second EXECUTE forming its own execute-scope entries with no fresh claim; cell (xiv)'s digits-marker fixture standing unchanged as the skip control; (xvi) *(new, revision 23, F-SUPPLY-PRODUCER-DOMAINS, decision log 69, its view's source-step edge coverage completed in revision 24, F-SOURCESTEP-LINEAGE, decision log 70, its set-operation branch-domain combination completed in revision 25, F-MULTIPRODUCER-DOMAINS, decision log 71, its set-operation coercion-source recovery completed in revision 26, F-SETOP-ALIGNMENT-RECOVERY, decision log 72, its set-operation common-domain reconvergence completed in revision 27, F-SETOP-COMMON-RECONVERGENCE, decision log 73)* the prepared-CTAS aggregate-producer rebind boundary: `PREPARE p FROM 'CREATE TABLE t3 AS SELECT CAST(COALESCE(MAX(a*b), ?) AS DECIMAL(65,1)) AS v FROM t'` over `DECIMAL(38,0)` operands holding 1, EXECUTE `?=0.5` typed `DECIMAL(20,1)` — PREPARE snapshotting `(65,1)` head=tag on BOTH engines (the authored CAST pins them) and retaining 3 content keys with no b#-channel entry; on the widened engine the filled MAX ColRef carries `(76,0)`, the coalesce family common needs 77 digits and the first lookup fails — the pre-revision-23 probe observing the fill error `invalid argument function coalesce, bad value [DECIMAL256 DECIMAL128]` pre-refresh: the plan-producer lineage view resolving the consumer-side triple through the projection-refresh envelope owner's AGG inventories to the retained `MAX(a*b)` call, the operand resolver recursing to the multiply's parent `(65,0)` with provenance, the retry binding the parent's family common `(66,1)` so the statement SUCCEEDS, the filled head staying `(65,1)` — the per-column skip firing — and the snapshot tag `(65,1)` persisting with value 1.0 on BOTH engines, parent-identical per decision log 69 (iii) (parent `a*b` `(65,0)`, MAX `ReturnFirstArgType` `(65,0)`, common `[(65,0),(20,1)]` -> `(66,1)`, the authored pin `(65,1)`); the execute-scope registry assertion: the retried coalesce decision recorded content-keyed by its rebound types (parents `[(65,0),(20,1)]`, aligned `(66,1)`); the independent parent channel: the parent's own engine executing the identical statement to `(65,1)`/1.0, asserted per-execute; the authored-cast pinned control `CAST(COALESCE(MAX(CAST(a*b AS DECIMAL(65,0))), ?) AS DECIMAL(65,1))` succeeding with and without the view (pinned lineage resolving today, tag `(65,1)`); the direct-expression control — cell (xv)'s VALUE-arm witness — standing unchanged with the view installed (entry 68's operand-ledger channel intact); the genuine-wide control: a producer whose parent-published domain is itself wide (a merge over a genuine 66-digit literal arm) retrying to the parent's own convergence on both engines; the lineage-withheld ablation reproducing the probe's exact fill error, proving the view load-bearing; repeated bindings asserting the same success/tag/value through the cached route (Witness B extension: second EXECUTE, no fresh claim, its own execute-scope entries formed) and both retry routes (Witness A extension); the BVT exercising the counterexample on the owned isolated service with catalog-surface persisted-tag assertions and verified teardown; (xvii) *(new, revision 24, F-SOURCESTEP-LINEAGE, decision log 70)* the prepared-CTAS MATERIALIZED shared-producer rebind boundary: `PREPARE p FROM 'CREATE TABLE t5 AS WITH c AS (SELECT a*b AS v, uuid() AS r FROM t) SELECT COALESCE(x.v, ?) AS w FROM c x JOIN c y ON x.v = y.v'` over `DECIMAL(38,0)` operands holding 1, EXECUTE `?=0.5` typed `DECIMAL(20,1)` — the value-unstable export forcing publication (`classifyCTEEvaluation`, `cte_identity.go:190`–`:245`) where natural multi-join and rollup shapes stay inlined under the reuse gates (`cte_reuse.go:105`–`:185`): the PREPARE snapshot's query `Steps` carrying exactly two entries (the SINK producer root and the consumer root), the plan holding TWO `Node_SINK_SCAN` readers with `SourceStep` and NO children (`appendSharedCTEScan`, `cte_reuse.go:1871`–`:1909`), their outputs published positionally (`Expr_Col{RelPos: 0, ColPos: i}`, `query_builder.go:3212`–`:3253`), the exported chain head coalesce -> `col(0,0)` into the JOIN slot -> `col(0,0)` into the SINK_SCAN slot, retained content keys multiply `nodcnpc#2|33:38:0>34:38:0|33:38:0>34:38:0` and coalesce `420ac26p#2|34:76:0>34:76:0|71:0:0>34:76:0`, and the snapshot tag the provisional `VARCHAR(65535)` on BOTH engines; on the widened engine the filled head operand carries `(76,0)`, its family common with the `DECIMAL(20,1)` binding needs 77 digits and the first lookup fails — the pre-revision-24 probe observing the fill error `invalid argument function coalesce, bad value [DECIMAL256 DECIMAL128]` pre-refresh: the delegated producer resolution following the complete hop chain through the storage-domain owner — JOIN side slot, SINK_SCAN slot, its SourceStep edge into the producer subtree (`stepProducer`, `build_ddl.go:1796`–`:1811`), the SINK child and the PROJECT identity mirrors to the retained multiply — the operand resolver recursing to the multiply's parent `(65,0)` with provenance, the retry binding the parent's family common `(66,1)`, the statement SUCCEEDING with the refresh deriving and persisting `(34,66,1)` and value 1.0 on BOTH engines, parent-identical (the parent's exported `v` publishes `(65,0)`; common `[(65,0),(20,1)]` -> `(66,1)`); the execute-scope registry assertion: the retried coalesce decision recorded content-keyed by its rebound types (parents `[(65,0),(20,1)]`, aligned `(66,1)`); the independent parent channel: the parent's own engine executing the identical statement to `(66,1)`/1.0, asserted per-execute; the authored-cast pinned twin exporting `CAST(a*b AS DECIMAL(65,0))` under the IDENTICAL materialized topology filling OK TODAY with head `(66,1)` and per-execute refresh `(66,1)` on the retained AND nil ledger channels (probe-verified, carrier-independent); the lineage-withheld ablation reproducing the probe's exact fill error, proving the delegation load-bearing; the recursive bounded-cycle control — a RECURSIVE CTE exporting a widened product whose produced values fit the anchor domain succeeding, the path-bounded source membership (`build_ddl.go:2319`–`:2328`) keeping the recursive member's outward scan from cycling; repeated bindings asserting the same success/tag/value through the cached route (Witness B extension: second EXECUTE, no fresh claim, its own execute-scope entries formed) and both retry routes (Witness A extension); cells (xv) and (xvi) standing with the delegation installed; the BVT exercising the materialized counterexample on the owned isolated service with catalog-surface persisted-tag `(66,1)` assertions and verified teardown; (xviii) *(new, revision 25, F-MULTIPRODUCER-DOMAINS, decision log 71)* the prepared-CTAS SET-OPERATION multi-producer rebind boundary: `PREPARE p FROM 'CREATE TABLE t6 AS SELECT COALESCE(u.v, ?) AS w FROM (SELECT a*b AS v FROM s1 UNION ALL SELECT a*b FROM s2) u'` over `DECIMAL(38,0)` operands holding 1, EXECUTE `?=0.5` typed `DECIMAL(20,1)` — the PREPARE snapshot's query holding ONE `Node_UNION_ALL` with two PROJECT branch children each exporting the widened multiply (`f:*#2(34:76:0)`), the union's own output slot published through the set-operation typing contract (`query_builder.go:4819`–`:4833`, the coalesce-family common of the branch `ProjectList` types) as `(34:76:0)` on the widened engine, the derived-table mirror feeding the head coalesce, retained content keys coalesce `420ac26p#2|34:76:0>34:76:0|71:0:0>34:76:0` and ONE shared multiply key `nodcnpc#2|33:38:0>34:38:0|33:38:0>34:38:0` (the two branches' multiplies are content-identical), and the snapshot tag the provisional `VARCHAR(65535)` on BOTH engines; the storage-domain producer resolution of the union's output slot returning BOTH branch contributors (`build_ddl.go:2367`–`:2383`, ok=true, count=2 — the exact multi-producer shape revision 24 declined while the parent resolves it unambiguously); on the widened engine the filled head operand carries `(76,0)`, its family common with the `DECIMAL(20,1)` binding needs 77 digits and the first lookup fails — the pre-revision-25 probe observing the fill error `invalid argument function coalesce, bad value [DECIMAL256 DECIMAL128]` pre-refresh: the lineage view resolving EACH contributor's parent domain and provenance exactly as entry 70 (iv) specified for one producer — through the branch PROJECT identity mirrors to the two retained multiplies, each recursing to its parent `(65,0)` with provenance — then combining the per-branch parent domains through the SAME set-operation typing contract the parent runs for that node (`[(65,0),(65,0)]` -> `(65,0)`), the retry binding the parent's family common `(66,1)`, the statement SUCCEEDING with the refresh deriving and persisting `(34,66,1)` and value 1.0 on BOTH engines, parent-identical (the parent's branches publish `(65,0)` each, the union output `(65,0)`; common `[(65,0),(20,1)]` -> `(66,1)`); the execute-scope registry assertion: the retried coalesce decision recorded content-keyed by its rebound types (parents `[(65,0),(20,1)]`, aligned `(66,1)`); the independent parent channel: the parent's own engine executing the identical statement to `(66,1)`/1.0, asserted per-execute; the authored-cast pinned twin exporting `CAST(a*b AS DECIMAL(65,0))` in BOTH branches under the IDENTICAL set-operation topology filling OK TODAY with head `(66,1)` and per-execute refresh `(66,1)` on the retained AND nil ledger channels (probe-verified, carrier-independent); the genuine-wide controls: a UNION ALL whose branch parent domains genuinely differ — one `(65,0)` widened-product branch and one branch whose parent-published domain is itself wide through a merge over a genuine 66-digit literal arm — combining through the identical contract to the parent's combined output and converging exactly as the parent's own execute on both engines, and a UNION ALL with a genuinely UNRESOLVABLE branch contributor (an out-of-case expression shape) declining with the pre-existing checked merge error, never a fabricated domain; *(this decline witness is REVISED in revision 28, F-MOD-PARENT-DOMAIN, decision log 74 (i)/(vii): the shared typing rules derive every decimal-producing supported consumer — `mod`/`%` through `DecimalModReturnType` included — so no out-of-case DECIMAL expression shape remains; the shape executed for this witness, a `(a*b) % 1` branch, moves to the SUCCESS family with parent/fixed success, value and persisted-tag assertions (§9 I10 (xxi) (a)), and the decline family is re-scoped to evidence-reconstruction failure, pinned by this cell's lineage-withheld ablation; kept as history)* the lineage-withheld ablation reproducing this probe's exact fill error, proving the combination load-bearing; repeated bindings asserting the same success/tag/value through the cached route (Witness B extension: second EXECUTE, no fresh claim, its own execute-scope entries formed) and both retry routes (Witness A extension); INTERSECT/INTERSECT_ALL/MINUS/MINUS_ALL sharing the same case arm (`build_ddl.go:2367`–`:2370`) with one representative non-UNION variant asserting the identical combination path; cells (xv), (xvi) and (xvii) standing with the combination installed; the BVT exercising the set-operation counterexample on the owned isolated service with catalog-surface persisted-tag `(66,1)` assertions and verified teardown; (xix) *(new, revision 26, F-SETOP-ALIGNMENT-RECOVERY, decision log 72)* the prepared-CTAS SET-OPERATION UNEQUAL-WIDTH rebind boundary: `PREPARE p FROM 'CREATE TABLE t7 AS SELECT COALESCE(u.v, ?) AS w FROM (SELECT a*b AS v FROM s1 UNION ALL SELECT a*b FROM s2) u'` with `s1` over `DECIMAL(38,0)` operands and `s2` over `DECIMAL(35,0)` operands, all holding 1, EXECUTE `?=0.5` typed `DECIMAL(20,1)` — the branches publishing UNEQUAL widened widths `(76,0)` and `(70,0)`, so the set-operation alignment inserts an engine-owned cast to the published `(76,0)` around the NARROWER branch's export (`query_builder.go:4968`–`:4996`, plain overload-0 `cast`, no `SyntaxExplicitCast` — probe-verified guard dump `overload=0` on the cast-bearing export): the PREPARE snapshot's query holding ONE `Node_UNION_ALL` with branch exports `f:*#2(34:76:0)` and `f:cast#2(34:76:0)` wrapping the source `f:*#2(34:70:0)` in either branch order (the cast traveling with the narrower branch, probe-verified UM/UMR), the union output published `(34:76:0)` through the set-operation typing contract, retained content keys THREE — the coalesce `420ac26p#2|34:76:0>34:76:0|71:0:0>34:76:0` plus the two DISTINCT multiply keys `nodcnpc#2|33:38:0>34:38:0|33:38:0>34:38:0` and `nodcnpc#2|33:35:0>34:35:0|33:35:0>34:35:0` (unequal operand widths defeat cell (xviii)'s key sharing) — and the snapshot tag the provisional `VARCHAR(65535)` on BOTH engines; the storage-domain producer resolution of the union's output slot returning BOTH branch contributors (ok=true, count=2 — the wider branch's retained multiply and the narrower branch's returned ALIGNMENT CAST, `build_ddl.go:2381`); the pre-revision-26 failure PROBE-OBSERVED in both branch orders: the recursion retaining the cast's fixed `(76,0)` target (`base_binder.go:9803`–`:9809`), the combination yielding `(76,0)`, the family common with the `DECIMAL(20,1)` binding needing 77 digits, the fill raising exactly `invalid argument function coalesce, bad value [DECIMAL256 DECIMAL128]` pre-refresh; the lineage view FIRST recovering the coercion's SOURCE through the engine's own prepared-route ownership predicate (`unwrapPreparedSetOperationCoercion`, `utils.go:7512`–`:7529` — PROJECT branch slot pointer identity, a plain two-argument `cast` with a `TargetType` second argument, not `SyntaxExplicitCast`, overload 0 or 3, the cast's published type equal to the slot's set-operation target — the exact unwrap-then-combine order the engine itself runs at `utils.go:7897` before `preparedSetOperationCommonType` at `:7965`), the recovered source multiply re-entering the contributor recursion exactly as entry 70 (iv) specified, EACH branch resolving to its parent `(65,0)` with provenance, the combination through the unchanged typing contract yielding `(65,0)`, the retry binding the parent's family common `(66,1)`, the statement SUCCEEDING with the refresh deriving and persisting `(34,66,1)` and value 1.0 on BOTH engines, parent-identical (the parent's branches publish `(65,0)` each, the union output `(65,0)`; common `[(65,0),(20,1)]` -> `(66,1)`); the REVERSED branch-order twin asserting the same success/tag/value (the cast and the recovery traveling with the narrower branch); the SQL-authored pinned control exporting `CAST(a*b AS DECIMAL(65,0))` in BOTH branches under the IDENTICAL unequal-operand topology — the ownership predicate NEVER unwrapping it (probe-verified: the branch exports carry recorded overload 1 with the syntax flag cleared by the prepared rebind, the predicate's overload guard declining them, unwrap=false) — filling OK TODAY with head `(66,1)` and per-execute refresh `(66,1)` on the retained AND nil ledger channels (probe-verified, carrier-independent); the genuine-decline control: an owned coercion whose recovered source cannot be resolved declining the whole resolution with the pre-existing checked merge error, never a fabricated or widened lineage; the recovery-withheld ablation reproducing this probe's exact fill error in both branch orders, proving the recovery load-bearing; repeated bindings asserting the same success/tag/value through the cached route (Witness B extension: second EXECUTE, no fresh claim, its own execute-scope entries formed) and both retry routes (Witness A extension); the execute-scope registry assertion: the retried coalesce decision recorded content-keyed by its rebound types (parents `[(65,0),(20,1)]`, aligned `(66,1)`); the independent parent channel: the parent's own engine executing the identical statement to `(66,1)`/1.0, asserted per-execute; cells (xv)–(xviii) standing with the recovery installed — cell (xviii)'s equal-width branches gain NO alignment cast (`needsCast` false), so its resolution path is recovery-invariant; the BVT exercising the unequal-width counterexample in BOTH branch orders on the owned isolated service with catalog-surface persisted-tag `(66,1)` assertions and verified teardown; (xx) *(new, revision 27, F-SETOP-COMMON-RECONVERGENCE, decision log 73 (vi))* the mixed-scale set-operation counterexample — `s(a, b DECIMAL(38,0); d DECIMAL(65,1))` all holding 1, `SELECT a*b AS v FROM s UNION ALL SELECT d FROM s` (the widened `(76,0)` product arm forcing a carrier-overflowing 77-width common TODAY, probe MX) — asserted in BOTH branch orders: the owner's FAILED coalesce lookup retrying over the resolver-computed parent-published branch vector, binding the parent's own `(66,1)` with parent-authored per-branch alignment casts (the retried binding recorded content-keyed), statement success with the parent's value 1/1.0 parent-identically, CTAS persisting `(66,1)` parent-identically; the EXECUTE-SPECIALIZED set-arm parameter case (a prepared union of a retained widened-product arm and a parameter arm, the EXECUTE binding typed `(65,1)`) converging `(66,1)` through the prepared owner's retry with the second-EXECUTE cached route parent-identical and its retry-withheld ablation reproducing the probe's exact initial-binding rejection; the independent parent engine oracle asserted per-execute (value, type AND persisted tag); the genuine-wide 77 control (MW, no multiplication provenance) failing IDENTICALLY before and after the repair; the authored-cast control (MP) binding before and after with its authored targets; the no-provenance deferral control; cells (xv)–(xix) re-asserted with the retry installed (cell (xix) recovery-invariant: scale-0 branch sets never fail the lookup, so the retry never fires); the BVT on the owned isolated service in BOTH branch orders plus the prepared parameter-arm case with catalog-surface persisted-tag `(66,1)` assertions and verified teardown through the revision-27 channels; (xxi) *(new, revision 28, F-MOD-PARENT-DOMAIN plus the revision-28 convergence obligations, decision log 74 (vii))* — five witness families: (a) THE MOD SET-OPERATION SUCCESS WITNESS replacing cell (xviii)'s withdrawn out-of-case decline oracle: `PREPARE p FROM 'CREATE TABLE t7 AS SELECT COALESCE(u.v, ?) AS w FROM (SELECT a*b AS v FROM s1 UNION ALL SELECT (a*b) % 1 AS v FROM s2) u'` over `DECIMAL(38,0)` operands holding 1, EXECUTE `?=0.5` typed `DECIMAL(20,1)` — the `%` branch's parent domain derived through the shared `DecimalModReturnType` rule over the reconstructed parent operand domains (the product `(65,0)`), the union combining to the parent's `(65,0)`, the coalesce binding the parent's `(66,1)`, the statement SUCCEEDING with persisted tag `(66,1)` and values 1.0/0.0 parent-identically — asserted LIVE at statement time on the owned base service (the parent statement-time oracle live-verified this round: information_schema w `DECIMAL(66,1)`, values 1.0/0.0) and through the in-process prepared channels (Witness A/B fill and refresh), with the decline family re-asserted ONLY as the lineage-withheld ablation reproducing the exact checked fill error; (b) THE MATERIALIZED TWO-READER REGRESSIONS: the shared producer exporting `COALESCE(a*b, <76-digit literal>)` (head `(76,0)` = envelope `(76,0)`, no head divergence) — cancelling +/-10^65 product rows rejected at AVG with the pre-change checked invalid-input family (never a silent 0), the fitting two-reference form returning AVG's parent value and metadata alongside MAX's exact widened row from the same producer, short-circuit demand preserving unselected arms, and a MAX-only reader of the same producer paying nothing; (c) THE ENVELOPE-CONSUMER INVENTORY WITNESSES: CTAS and view persisted metadata for `TRUNCATE((38,10)x(38,10),0)+DECIMAL(39,0)` -> `(54,0)` and `PERCENTILE_DISC(...) ORDER BY a*b` -> `(65,0)` parent-identically, oversized-value rejection at each declared boundary, and genuine-wide controls; THE MOD FAMILY (revision 29, F-MOD-STORAGE-ENVELOPE, completing decision 74 (i) at the storage boundary): (c-mod1) the below-65 WRAPPER case — CTAS and view records over `TRUNCATE(a*b,0) % d` (`DECIMAL(38,10)` operands, d `DECIMAL(39,0)`) persist the parent's `(53,0)` (NOT the widened head `(64,0)`), value `0`; (c-mod2) the GENUINE-WIDE MOD control — CTAS over `(a*b) % d` (`DECIMAL(38,0)` operands, d `DECIMAL(39,0)`) persists the parent's `(65,0)` (the reconstructed `*` operand envelope, NOT the `(76,0)` head), value `0`; both oracles are parent tags derived through `DecimalModReturnType` rule-for-rule and live-confirmed this round against the parent (receipts `/tmp/30938_base_modenv_probe.txt`; the current diff persists `(64,0)` and `(76,0)`, `/tmp/30938_fixed_modenv_probe.txt`), pinned as UT envelope-arithmetic assertions plus live statement-time persisted-tag BVT assertions on the owned fixed service with catalog-surface reads; (d) THE TIME_WINDOW METADATA FAMILY: the issue fixture's time-window block regenerated against the corrected finalization (the AVG(a*b) header publishing the parent's `(65,0)` where the unreconciled header reads `(65,4)`), parent/fixed result-metatype assertions, an `AVG(a*b)/3` control over unit-valued operands (division scale-4 value and metadata), extended through CTAS and prepared routes; (e) THE SECTION-4.3 MEMO/COUNT TESTS: a shared diamond proving exactly one envelope evaluation per producer slot across output columns, and a cross-column recursive case proving anchor-only seeding with no recursive-member schema evaluation; the live statement-time set-operation parity witnesses (the plain `a*b` and `%`-branch shapes succeeding on the owned fixed service with parent values and persisted tags) close the revision-27 live-route decline recorded in decision log 74 (vi) | *(new in revision 30, F-UNSTABLE-SHADOW-IDENTITY; extended in revision 31, F-REPLAY-SAFETY-POSITIVE-PROOF and F-SEEDED-RAND-ORACLE and in revision 32, F-DIAGNOSTIC-REPLAY and F-PLAN-SHAPE-SPLIT)* the unproven-slot admission family in seven witnesses — the revision-30 seeded-counting oracle is WITHDRAWN: `RAND(42)` is unplannable because the only registered RANDOM overload is zero-argument (`list_builtIn.go:9690–9711`, `fixedTypeMatch` rejecting the arity at `type_check.go:333–334`), the seeded generator (`func_builtin.go:5008–5049`) is reachable through no registered overload, and identical-seed executors would draw identical sequences anyway, so a seeded cross-site equality could never discriminate a duplicated evaluation — replaced by: (a1) the PLAN-SHAPE oracle over `WITH c AS (SELECT CAST(RAND() AS DECIMAL(38,6))*a AS v FROM t WHERE k % 4 = 0) SELECT (SELECT AVG(v) FROM c) av, (SELECT MAX(v) FROM c) mx, (SELECT MIN(v) FROM c) mn` over an `a DECIMAL(38,0)` column holding `10^30`, three-way with its assertions separated version-shared from version-specific (revision 32, F-PLAN-SHAPE-SPLIT): VERSION-SHARED on all three engines — exactly one SINK producer with exactly three SINK_SCAN readers and exactly ONE `rand` evaluation site in the producer projection; PARENT-SPECIFIC (green) — the export publishes the parent's `(65,6)` (the parent's `decimalMultiplyReturnType` clamps `min(w1+w2, 65)`, HEAD `list_operator.go:209–224`) and no shadow-admission machinery exists; REPAIRED-SPECIFIC (green) — NO shadow column for the unproven slot, the `(65,6)` envelope cast spliced in place at the multiply seam inside the original export, and the export publishing the widened `(76,6)`; FAILING-CANDIDATE-SPECIFIC (red) — the probe61 observation: TWO rand evaluation sites, both admitted (`/tmp/30938_probe61.log`); (a2) the deterministic INVOCATION-COUNT oracle at the executor-test boundary: a producing expression carrying the supported nonvolatile external call `llm_chat` (registered STRICT and nonvolatile, `list_builtIn.go:2925–2943`; per-operation cached client, `func_builtin_llm.go:46–67`; one `Chat` per row, `:69–101` with the call at `:90`; one HTTP request per `Chat`, `pkg/monlp/llm/ollama.go:99` → `ChatMsg` `:77–96`; endpoint minted by `NewLLMClient`, `pkg/monlp/llm/llm.go:46–63`) over N producer rows with an AVG admitting reader issues EXACTLY N HTTP requests against an httptest-counted endpoint on the parent and on the repaired engine alike, where the always-shadow candidate issues 2N — the replica's independent executor repeating every request; (a3) the value-consistency check, scoped WITHIN-engine only: the zero-argument `RAND()` and `UUID()` forms assert AVG = MAX = MIN by value inside the fixed engine — cross-engine random-value equality is impossible and is asserted nowhere; (b) the public shared-CTE witness live on the owned parent and fixed services — the same unseeded shape returning equal AVG/MAX/MIN values within each engine with parent-identical AVG metadata and each engine's own MAX/MIN seam metadata (the sanctioned wire-metadata divergence), no cross-engine value oracle; (c) the violating-row variant whose product exceeds the `(65,6)` integer capacity — the parent rejects at the multiply with ERROR 1690 and the fixed engine rejects at the in-place cast with the parent's multiply-time family, the whole statement failing on both engines; (d) the MAX-only control — no admitting reader, no cast, no shadow, the fixed engine returning the carrier-exact wide value where the parent rejects (the sanctioned issue divergence); (e) the CHANNEL-PREDICATE UT table over the positive replay-safety proof (extended in revision 32, F-DIAGNOSTIC-REPLAY): the revision-28/29 shadow shapes (`COALESCE(a*b, <literal>)` over Col leaves — `coalesce` audited diagnostic-free at `func_binary.go:1198–1226`/`:1227`, `*` error-only, every arm-alignment cast decimal→decimal) classify PROVEN and keep the shadow channel byte-identically, while `rand`, `uuid`, `llm_chat`, any INTERNAL-class or unlisted call and any non-Lit/Col leaf classify UNPROVEN and take the in-place channel; the revision-32 diagnostic rows: the review's `CASE WHEN UNHEX(s) IS NOT NULL THEN a*b ELSE a*d END` shape classifies UNPROVEN (`unhex` is curated — `function.go:216` — yet appends warning 1411 on malformed input, `func_unary.go:9294`, asserted by `TestUnhexWarningForInvalidHex`, `func_unary_test.go:6789–6807`: curated membership alone is not diagnostic-freedom proof), a string-sourced cast shape (`CAST(s AS DECIMAL(20,0))` over a varchar column) classifies UNPROVEN (outside the decimal→decimal warning-free family — `NewExplicitCast`, `list_operator.go:2638–2645`, reaches the string→integer appends at `func_cast.go:7396`–`:7428` and `:8471`–`:8499`), an unaudited curated member such as `greatest` classifies UNPROVEN pending a recorded per-overload audit, and a pure decimal→decimal cast shape classifies PROVEN — the SQL-reachable `CAST(a*b AS DECIMAL(65,0))` (decimal256→decimal256, inside the warning-free family; the revision-33 `DECIMAL(76,0)` form is unreachable through SQL — `mysql_sql.y:15705–15708` rejects declarations above 65 — replaced in revision 34, F-CAST-DESCRIPTOR-LEAVES; the predicate validates the cast's `Expr_T` target descriptor as metadata and recurses through its value argument, §5); (f) the stable-channel controls — the revision-28/29 two-reader witnesses re-run byte-identical; (g) the DIAGNOSTIC-COUNT oracle family (new in revision 32, F-DIAGNOSTIC-REPLAY), deterministic parent/candidate/repaired warning-count oracles at the warning-sink boundary: (g1) the UNHEX witness — a materialized producer exporting `CASE WHEN UNHEX(s) IS NOT NULL THEN a*b ELSE a*d END` with `s` a varchar column holding `'zz'` and fitting `DECIMAL(38,0)` operands, read by an AVG admitting reader: each malformed-hex row appends EXACTLY ONE warning 1411 (`ER_WRONG_VALUE_FOR_TYPE`) on the parent and on the repaired engine alike (the unproven slot's single in-place evaluation), where the always-shadow candidate appends TWO per row — the replica's independent executor repeating every diagnostic; (g2) the MASKED-ARM controls — rows whose UNHEX arm is never selected (a `k % 2 = 0` guard false, or `COALESCE(x, UNHEX(s))` with non-NULL `x`) append NOTHING on any engine, the base engine's short-circuit masks excluding evaluation and diagnostic alike, parent = candidate = repaired on the masked rows; (g3) the PROVEN-channel zero-diagnostic control — the revision-28/29 `COALESCE(a*b, <literal>)` shadow witness appends ZERO warnings on all three engines, the shadow channel introducing no diagnostic duplication for the admitted shapes; counts read through the engine's warning sink (`proc.GetWarningSink()` and the `warningDiagnosticAppender` interface, `func_cast.go:7594`) exactly as `TestUnhexWarningForInvalidHex` (`func_unary_test.go:6789–6807`) asserts one warning for `'zz'` | *(new in revision 30, F-SELECTOR-PARENT-DOMAIN)* the selector merge-retry completion family, three-way (parent-green, current-candidate-red, repaired-green) per member over `DECIMAL(38,0)` columns holding `1`: `SELECT COALESCE(<selector>, CAST(0 AS DECIMAL(65,1))) FROM t` for `MAX_BY_NON_NULL(a*b,k,k)`, `PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY a*b)`, `FIRST_VALUE(a*b)`/`LAST_VALUE(a*b)`/`NTH_VALUE(a*b,1) OVER (ORDER BY k)` and `LAG(a*b)`/`LEAD(a*b) OVER (ORDER BY k)` — each pending output publishes the widened `(76,0)`, the first merge lookup needs width 77, and the completed retry resolves the output's parent domain and binds the parent's `(66,1)` — asserting statement success, the parent's value, the parent's result type and the CTAS-persisted `(66,1)`; the `MAX_BY` control is already green; the lag/lead WITH-DEFAULT window-bind variants (`LAG(a*b,1,CAST(0 AS DECIMAL(65,1)))` and the `LEAD` twin) — *(restated in revision 33, F-LAGLEAD-PARENT-REJECTION)* the parent does NOT converge here: its reconcile derives `(66,1)` and REJECTS it at planning (derived decimal width >65 — HEAD `base_binder.go:9284–9285` final post-lookup check, sibling sites `:9208` bool-numeric and `:9239` decimal×integer/bit/year, lookup-failure fallback `:9261`; live parent receipt `/tmp/30938_bvt_dtype_base.log` row 188), so the three-way split is PARENT-SPECIFIC (green): the checked `LAG/LEAD common precision exceeds SQL limit 65` planning rejection, never a persisted schema; REPAIRED-SPECIFIC (green): the widened `>76` checks (worktree `:9272`/`:9304`/`:9353`) admit and the completed reconcile binds `(66,1)` over parent domains, values exact; CANDIDATE-SPECIFIC (red): the pre-completion tree declines with the checked LAG/LEAD family, verified at code and executable-confirmed red at implementation; — *(extended in revision 31, re-scoped in revision 33, F-LAGLEAD-PARENT-REJECTION)* the with-default PERSISTENCE assertions are repaired-specific (no parent persisted schema exists): CTAS and view records (DESCRIBE/SHOW COLUMNS/information_schema) persist the reconcile-derived `(66,1)` and the prepared route re-asserts `(66,1)` per execute, values exact — *(new in revision 34; WITHDRAWN in revision 40, decision log 86 (ii), kept-as-history; restated in revision 41)* the view definition persists its reconcile-derived tag as a plain catalog record carrying exactly the requirement the unchanged pre-existing channel computes for the same statement — the CAST-default witnesses none, a wide-literal-default witness v89 — with no below-floor family, asserted in I9's marker-preservation matrix (decision log 87 (i)); *(new in revision 35, F-LAGLEAD-SIGNAL-NEWLY-ADMITTED; re-scoped in revision 36, F-LAGLEAD-PARENT-GATE-ADMISSION)* the NEGATIVE band family joins the matrix: wide-literal `lag`/`lead` shapes with ABSENT, NULL and MATCHING defaults — matching under PARENT domains, covering the GATE-APPEARANCE mirror `LAG(a*b,0,CAST(0 AS DECIMAL(65,0)))` whose parent binds `(65,0)` with no rejection although the changed engine's gate runs and derives `(76,0)` — and a no-default multiplication view carry no band signal and persist parent-identical records — the unchanged channel's computed requirements included verbatim: the `(66,0)` wide-literal views carry the base machinery's v89 requirement on the parent and on the changed binary identically, while the parent's `(65,0)` envelope for the mirror and the no-default multiplication view carry none — and read/replay/describe/schema-infer byte-identically on the changed binary (revisions 40/41: the forged-below-floor runtime and the rollback story are withdrawn, decision log 86 (ii)/87 (i)), while the positive family retains the provenance-free scale-70 expansion and gains the GATE-DISAPPEARANCE witness `LAG(a*b,0,N76)` — parent rejects the `(76,0)` derivation at planning (HEAD `base_binder.go:9284–9285`), repaired succeeds and persists `(76,0)` as a plain catalog record carrying exactly the requirement the unchanged channel computes for the N76-carrying definition (v89), with no issue-specific mint and no below-floor family — *(new, revision 37; restated in revisions 40/41)* the NESTED band family joins the matrix: `CREATE VIEW outer_v AS SELECT * FROM <that witness view>` persists its own record as a plain catalog record (a three-level outer/mid/inner chain transitively) carrying exactly the requirement the unchanged pre-existing channel computes for its own expanded plan — the outer expansion visiting the inner's N76 literal carries the same v89 requirement — with no issue-specific mint, no forged floor and no marker-first rejections (decision log 86 (ii)/87 (i)), and the isolation controls (an outer view over a NON-band inner view, an ordinary SELECT over a band view, a CTAS from a band view) persist parent-identical records, the non-band inner's own computed requirements included — revision 30's clipped envelope derived `(65,1)`, candidate-red; the second derived pair `LAG(a*b,1,CAST(0 AS DECIMAL(30,10)))` over `(38,0)` operands: parent rejects `(75,10)` at planning (same HEAD sites), repaired binds and persists `(75,10)`, candidate clip `(65,10)` red; the undeclarable control `LAG(a*b,1,CAST(0 AS DECIMAL(65,20)))` derives `(85,20)` and fails on BOTH engines with the same checked family and the IDENTICAL message text `LAG/LEAD common precision exceeds SQL limit 65` (revision 38, F-BAND-MESSAGE-ORACLE: the decimal×decimal merge fails inside each engine's own coalesce overload resolution — neither engine's decimal coalesce common admits a width beyond its own decimal expression bound — and surfaces through the retained lookup-failure fallback (parent `:9261`, worktree `:9326`) carrying the parent's 65-message verbatim on both engines, per the live round-69 receipts; the repaired engine's `exceeds the Decimal256 limit 76` texts belong to the NON-coalesce decimal-merge families — a decimal value with an integer, BIT or YEAR default, and the bool-numeric site — whose explicit width checks are reached without the coalesce lookup); the provenance-blind band: the widened checks admit every decimal default merge whose derived common fits 76 — the committed scale-70 `f4_sel`/`f4_unsel` CTAS shapes carry no multiplication provenance (parent rejects `(70,70)`; the frozen revision-30 envelope rejects the scale at the boundary, committed `.result` lines 489/491; the revision-31+ repaired engine binds and persists `(70,70)`) and `lag_def_lit` persists `(65,0)` under the frozen clip (`.result` lines 321–324) where the revision-31+ envelope persists `(66,0)` — the stale `.result` rows are restated when the envelope repair lands at the implementation turn; the undeclarable control `LAG(a*b,1,CAST(0 AS DECIMAL(65,20)))` fails identically on both engines with the pre-existing explicit planning error; prepared-lineage variants (PREPARE/EXECUTE CTAS with a parameter merge default) re-enter the same resolver at the execute rebind; both the pending-aggregate and pending-window routes are asserted; the `quantileexactweighted` exclusion control fails identically on both engines with the pre-existing admission rejection, never the merge decline; genuine-wide no-provenance controls never retry | *(new, folded into the I6 end-to-end cells)* `AVG(1)/3` returns `0.3333` at scale 4 (never `0.33333333`); dependent-plan AVG over a widened product returns parent-verbatim values through the correlated route |
| value-selecting decimal aggregate paths *(extended in revision 7, F-SELECTOR-AGGREGATES — formerly "percentile_disc paths"; merge-retry completion in revision 30, F-SELECTOR-PARENT-DOMAIN)* | *(new)* plan-shape oracles (I6 family): no admission cast on `min`/`max`, `any_value`, `max_by`, `first_value`/`last_value`/`nth_value` or `percentile_disc` arguments even where envelope diverges from published; aggexec return-type identity cells (input type verbatim — `ReturnFirstArgType` registrations: `list_agg.go:103`/`:120`, `:370`, `:717`; window `list_window.go:209–214`/`:234–239`/`:260–265`; percentile `ordered_percentile.go:151–156`) | *(new)* SELECT and window controls over a widened product return carrier-exact rows with the widened seam's executable metadata (e.g. `MAX(a*b)`, `MIN(a*b)` and `percentile_disc(0.5) WITHIN GROUP (ORDER BY a*b)` over `(38,0)²` columns holding `10^31` return the exact `10^62` product); separate storage cases: CTAS over each persists the declared `(65,·)` schema with exact fitting values, and an oversized widened value is rejected by the declared-domain cast (checked 20301 family) at CTAS, with no admission change for base-shaped (fitting) data; `desc`/`select` through a view of each; the merge-retry completion witnesses for every family in this row, plus the lag/lead with-default expansion-band assertions — parent checked planning rejection, repaired persisted tag (revisions 31/33, F-LAGLEAD-STORAGE-ENVELOPE/F-LAGLEAD-PARENT-REJECTION) — live at I10 (xxiii) |
| View surfaces *(split in revision 3, outer-expression oracles corrected in revision 4)* | *(new)* `inferViewColumns` record assertions: carrier → (65,0), width-reduced wrapper → below-65 base form (ROUND (64,2), TRUNCATE (63,2), mid-scale pair (54,0)/(53,0)), genuine wide → parent form | *(new)* CREATE/ALTER VIEW catalog records; *(restated in revisions 40/41)* band-view catalog records persisting the reconcile-derived above-65 tags as plain catalog records carrying exactly the requirement the unchanged pre-existing channel computes for the same statement (v89 where the definition carries a wide literal; no issue-specific mint — §6 M4, decision log 87 (i)); SELECT through view asserting exact values with executable carrier result-set metadata (value and metadata oracles independent); DESCRIBE/SHOW COLUMNS/information_schema asserting the declared records; *(new, F-VIEW-OUTER-OPERATORS)* outer-expression per-operator oracles: `SELECT abs(v.c)` returns the carrier-exact value with executable metadata (positive full-carrier oracle), `SELECT v.c + 1` keeps the addition's own unchanged 65-capped domain and raises the pre-existing checked validator rejection when the sum escapes it; nested views (inner/outer records plus exact values through both); CTAS from a view with independent schema and value oracles; `DescribeViewColumns` replay equivalence; legacy-viewdef identity |
| Relational slot addressing (AGG/WINDOW/TIME_WINDOW/SAMPLE/JOIN/set-op/CTE) | plan UT families mirroring query_builder layouts (`BuildTimeWindowLayout` single source) | per-family CTAS witnesses with exact values |
| No collateral regression in decimal/bit/window dtype family | full package suites: plan, plan/function, plan/rule, aggexec | dtype(decimal,bit) healthy-window failure-key comparison (0 added/0 removed); window fixture regenerates byte-identical |

Broader evidence for CI, not this gate: full mo-tester suite, distributed
BVT topology runs and broad performance replay. The focused performance
row above is the reconciliation cast's own measured budget; the
multiplication publication widen and the §1 fallback repair themselves are
O(1) type-level decisions plus one limb comparison and require no
benchmark.

## 10. Drawbacks, risks, open questions

- The boundary mirrors a small set of canonical typing/slot rules; drift
  risk is controlled by citing the single source of truth per rule and by
  the §4.2 machine-checked inventory, but future changes to
  `refineDecimalRoundingReturnType`/`BuildTimeWindowLayout`/query_builder
  slot conventions must update the mirror (each rule names its source
  inline; the UT families pin them).
- Fold-decline skips constant folding — since revision 12 at the fold
  walk's recursive entry, covering NULL-producing folds as well as
  vector-constant materialization — on wide-multiplication
  constant trees: a planner micro-optimization traded for correct storage
  provenance; runtime result semantics are identical (the tree still
  constant-evaluates at execution, NULL results included). The
  revision-13 decision barrier widens the same trade to the operand
  subtrees of provenance-flagged recorded decisions at BOTH fold
  engines' entries: their pre-coercion cast syntax survives to the
  pass's capture instead of being destroyed and positionally
  recovered later, and speculative folds over `DeepCopyExpr` copies
  decline consistently — constantness-based plan shapes may differ
  from the parent's folded form where the parent folded a protected
  subtree, while values, grouping/ordering outcomes and persisted
  records are unchanged (§9 I7's controls). The barrier's
  registry lives only inside one planning generation — installed
  fresh at the outermost entry — `BuildPlan`,
  `(*BaseOptimizer).Optimize` or, since revision 15, the shared
  SELECT funnel `bindAndOptimizeSelectQueryWithValidatorAndCapture`,
  which the standalone view-description generations enter with no
  enclosing planner entry (revision 14, F-REGISTRY-LIFECYCLE,
  decision log 58; the third invocation in revision 15,
  F-VIEW-DESCRIPTION-LIFECYCLE, decision log 60 — moved one level
  out to `inferViewColumns`, `build_ddl.go:240`, in revision 17,
  F-DECISION-EVIDENCE-LIFECYCLE, decision log 63(i), so the
  standalone generation spans the funnel call AND the `:345–387`
  column-record assembly) — and detached on
  its return, so the trade never
  crosses statements, and folds applied after the generation ends
  see the restored nil field and fold base-identically (the
  barrier's protection is needed only until pass-entry capture,
  which the generation contains).
- The value-merge reconvergence (revision 12, F-COMMON-DOMAIN-OVERFLOW;
  re-placed in revision 13, F-RECONVERGENCE-CHANNEL; extended in
  revision 14 to the binder's tag/ordinal pending-producer channel —
  `BindContext.aggregates`/`ctx.windows` — so merges over the current
  query block's own pending aggregate and window outputs retry
  identically, F-AGG-WINDOW-CHANNEL; completed in revision 15 to
  the full same-block result-tag inventory — `BindContext.groups`,
  `ctx.sampleFunc.columns`, `ctx.projects` — so grouped, sampled
  and projected pending producers retry identically,
  F-GROUPED-CHANNEL) runs only where
  the first, widened lookup fails — or, for LEAST/GREATEST, silently
  degrades to FLOAT64 — and the binder-owned resolver marks an operand
  provenance-bearing: every narrow-provenance statement merges on the
  first attempt and pays nothing, a genuine-wide overflow without
  multiplication provenance keeps the family's own outcome with no
  retry (genuine-wide operands resolve identity parent views, so the
  parent's identical outcome stands even where provenance coexists),
  and the retry itself is one bounded planning-side resolver walk plus
  one repeated function lookup. The revision-12 deliberate loud
  REJECTION corner (a folded-twin statement failing at planning) is
  WITHDRAWN: the decision barrier preserves the distinguishing operand
  syntax, the parent-successful twin statement executes with
  parent-equivalent values, result types and persisted metadata, and
  the residual ambiguity failure stays only as mechanism-bug defense
  in depth, probe-asserted empty, because the remaining alternative
  channels (a plan-format annotation, silent preservation) violate I8
  or diverge in silence.
- The AVG/SUM argument seam casts (§5) re-apply the parent's own
  multiplication-publication admission at exactly the source boundary
  where the parent applied it — a widened product arm inside any argument
  carries its own parent-publication cast: a
  perfectly fitting 66–76-digit product returns exactly as an expression
  result (I1), but averaging or summing it runs the parent's
  declared-domain
  pipeline — a widened product value beyond the parent's declared product
  domain (e.g. `10^70`, or `±10^65` from `(38,0)²` operands) fails that
  arm's seam cast with the parent's checked
  invalid-input family where the parent failed the multiply itself
  (1690); that family relabel is recorded as a conformance tradeoff, and
  every base-successful query keeps unchanged values, metadata and
  capacity. An average of admitted values that still escapes its
  `(65, s)` result domain keeps the pre-existing checked
  division/result-capacity family. CTAS of an AVG column persists the
  parent's own AVG schema and never invokes the carrier admission path.
- The aggregate-argument rewrite inserts a real cast executed per input
  row on the rebound families (admitted values pass through numerically
  unchanged, but the default same-scale capacity check formats and
  re-parses each non-NULL coefficient — `decimal256ToDecimal256`,
  `func_cast.go:6980–7030`), replays the coercive binding decisions and
  runs the dependency-ordered finalization over the final plan's types
  once per affected statement,
  and runs the §4.3 walk once per affected
  statement; the structural skip gate (no decimal multiplication subtree
  ⇒ no walk, no cast) contains the cost, at the price of keeping the
  gate's completeness premise true (multiplication is the only >65
  publisher — the §4.2 exhaustiveness UT pins it). Revision 7 scopes the
  cast-bearing per-row surface to exactly the divergent seams inside
  AVG/SUM arguments: bare-product families pay one cast per row (arm seam
  and head coincide), a mixed-origin argument pays one arm seam cast per
  divergent arm while genuine arms flow cast-free, and value-selecting
  aggregates pay nothing at all (revision 6's blanket uniform trigger
  would have cast them too); the planning-side decision replay over recovered operands, just-in-time copy refresh and shadow-or-locus-cast construction ride the single dependency-ordered finalization — one traversal per affected statement, inside the §4.3 walk's counter budget, the envelope-shadow replica columns adding width only to producers that carry an admitted argument on a row-preserving path (no extra pass, no extra scan; sibling value-selecting readers stay on the original column; row-changing interiors take one in-tree seam-locus cast instead). The
  budget measures AVG **and** SUM plus the mixed-origin and
  materialized-boundary variants (§9 I8
  (a)/(a′)/(a″)/(a‴)/(b)); withdrawing the
  result-agreement gate also removes the need to maintain the
  width-sensitivity inventory as a trigger input (it stays as
  documentation, exhaustiveness check two, and the machine-pinned
  definition of the `{avg, sum}` subject class). The §9 performance row
  owns the acceptance budget; a behavior-identical limb-comparison
  capacity kernel is an allowed semantics-neutral substitute if
  measurement demands it.
- Defended declared-domain fallback (§4.2 tier 1) could in principle narrow
  a genuinely wide domain if the inventory test has a hole — the trade is
  deliberate (never leak a carrier into the catalog), surfaced loudly by
  the exhaustiveness UT, and bounded to shapes provably outside the live
  registry.
- §5's single-channel claim — planner-authored AVG result type equals
  `aggexec.AvgReturnType(argument types as bound)` on every construction
  path, plain and window — is enforced by the §9 consistency UT plus the
  plan-shape oracles on the inserted cast, not by any new authoring seam;
  revision 8's dependency-ordered finalization (each producer slot
  finalizes exactly once, every referencing copy refreshes at that
  moment before any consumer reads it, and each consumer re-derives at
  its dependency position through its registered rule with its coercive
  decisions replayed over final inputs) extends that guarantee to every
  downstream scalar and aggregate consumer type and to every copied type
  (final and window projections, referencing expressions, TIME_WINDOW
  deep copies, SINK/SINK_SCAN schemas across `SourceStep` edges, the
  full correlation channel — program `InputTypes`, callee `CorrelationSlot`
  types, `OutputTypes` and dependent-call result types) at both
  `createQuery` exits, before execution, with the §9 finalization-order
  oracles pinning the dependency order, the once-only copy refresh, the
  `AVG(1)/3` scale-4
  witness (`0.3333`, never `0.33333333`),
  the TIME_WINDOW copy, the dependent-route reconciliation and the final
  result-set metadata;
  §6 M2 likewise rides no new mechanism (result-set metadata follows the
  executed head projection; catalog surfaces follow the persisted
  records; outer consumer operators follow their own pre-existing result
  domains), and any upstream refactor of those pre-existing seams must
  re-check this design.
- The #31034 rebase (§8) is real work: its operand-bound publication and
  fixtures overlap this change's owners; composition is asserted at design
  level only until re-validated.
- Resolved in revision 2 (decision log 6/7/9/10): AVG consumer distinction
  (F-AVG), fallback semantics + inventory (F-FALLBACK), traversal
  algorithm/bound (F-TRAVERSAL), view contract (F-VIEW). Resolved in
  revision 3 (decision log 12–17): AVG planner↔executor channel and
  argument-domain derivation (F-AVG2-EXEC, F-AVG2-DERIVE), non-monotone
  fixpoint iteration and unbounded visits (F-DATAFLOW), boundary 65-floor
  losing base schemas below 65 (F-BOUNDARY-FLOOR), decimal view
  presentation ownership (F-VIEW-PRESENT), scale-rule and overflow-oracle
  projection (F-SCALE-OVERFLOW). Resolved in revision 4 (decision log
  18–21): AVG base-equivalent argument domain (F-AVG-BASE-CAPACITY),
  affected-domain boundary trigger with separate ROUND/TRUNCATE oracles
  (F-BOUNDARY-TRIGGER), value-based recursive-member admission
  (F-RECURSIVE-ADMISSION), per-operator outer-view expression contracts
  (F-VIEW-OUTER-OPERATORS). Resolved in revision 5 (decision log 22–25):
  scaled-fallback declared-domain admission removed in favor of the
  physical carrier criterion (F-FALLBACK-DOMAIN), capped width-sensitive
  consumers reconstructed by their owning rules and the aggregate binding
  generalized beyond AVG (F-CAPPED-CONSUMERS), aggregate reconciliation
  relocated from binding time to the final plan (F-BINDING-PHASE), and
  the inference-only performance premise replaced by a documented cost
  model with an acceptance budget (F-PERF-PREMISE). Resolved in revision
  6 (decision log 26–28): downstream type propagation after aggregate
  reconciliation — copied slot/reference types re-synced and downstream
  consumer types re-derived before execution (F-DOWNSTREAM-PROPAGATION),
  uniform envelope-divergence aggregate admission with the
  registered-result agreement gate withdrawn, closing the mixed-origin
  cancellation hole and the bare-product SUM admission hole
  (F-ADMISSION-CONTRACT), and mixed-version execution admission via the
  minted `DecimalMultiplicationCarrierDomain` feature with its
  `MORPCVersion135` protocol floor, MULTICN→ONECN demotion and the
  destination not-supported guard (F-MIXED-VERSION). Resolved in revision
  7 (decision log 29–33): implicit binder coercions reconstructed when
  producer types change — stale baked targets rebuilt by replaying the
  binder's own alignment arithmetic, SQL-authored casts immutable
  (F-COERCION); finalization reordered dependency-first — consumers
  re-derived before any copy re-syncs, copies (projection, reference,
  TIME_WINDOW, SINK/SINK_SCAN, correlation) synchronized last from final
  producer slots (F-SYNC-ORDER); the reconciliation owner invoked at both
  `createQuery` exits, including the dependent-query route with its
  correlation copies (F-DEPENDENT-EXIT); the admission trigger narrowed
  from every decimal aggregate to the domain-arithmetic class `{avg,
  sum}` — value-selecting aggregates return input rows and need no domain
  cast (F-SELECTOR-AGGREGATES); and admission decomposed per source —
  each divergent decimal seam inside an AVG/SUM argument re-receives its
  own parent publication inner-first, so merged-envelope admission no
  longer substitutes for the parent multiply's per-source admission
  (F-PRODUCT-ADMISSION). Resolved in revision
  8 (decision log 34–39): the S1/S2/S3 global sweeps replaced by one
  dependency-ordered finalization over the producer/consumer/copy DAG —
  copies are inputs to consumer decisions, so every copy refreshes at
  its producer slot's finalization, before any consumer of it re-derives
  (F-INPUT-REFRESH); seam discovery made head-independent, closing the
  `(76,0)`-head masked cancellation (F-HEAD-GATE); materialized-boundary
  admission through statement-local companion projections, keeping
  sibling readers carrier-exact (F-PRODUCER-BOUNDARY); decision-owned
  coercion replay replacing the flag-predicate classifier that also
  matched admission and anchor casts (F-CAST-OWNERSHIP);
  dependent-expression and correlation-slot envelope rules with the
  channel finalized in producer order and `InputTypes` joined to the
  copy surface (F-DEPENDENT-ENVELOPE); and the exhaustiveness oracle
  re-founded on the two divergence families so transparent-propagation
  consumers classify soundly (F-TRANSPARENT-CONSUMERS). Attempted in revision 9 (decision log 40–43, SUPERSEDED by entries 44–47: the round-21 review found the conjunctive guards demand-inexact, the consumer-side companion under-specified across pruning, regeneration-equality inverting on changed targets, and the two-reference witnesses silently inlined): companion capture made demand-exact —
  the companion column evaluates the seam's envelope cast only under
  the arm-selection guard chain of the seam's short-circuit ancestors,
  matching the executor's row masks and the parent's producer demand
  (F-COMPANION-DEMAND); materialized-boundary admission placed by row
  shape — guarded companions cross only row-preserving edges, and seams
  separated from their reader by an interior dedup, aggregation or set
  operation take an in-tree seam-locus cast below the first such
  operator (F-ROW-CHANGING-BOUNDARY); coercion-decision replay given a
  recoverable operand list — pass-inserted casts skipped by identity,
  the single binder-inserted alignment cast unwrapped with
  regeneration-verified ownership, folded positions rebound from
  `Literal.Src` (F-REPLAY-RECOVERY); and the materialized-boundary
  witnesses re-founded on genuine two-reference producers with
  separated error and value statements (F-MATERIALIZATION-WITNESS).
Resolved in
  revision 10 (decision log 44–47): the guarded companion is replaced
  by ONE statement-local envelope-shadow column per admitted argument —
  a structural replica of the whole argument expression with the seam
  casts in place, built producer-side where every argument input is in
  scope, evaluated by the executor's own CASE/COALESCE machinery so
  per-row demand and three-valued selection equal the base engine's by
  construction, and exported as an ordinary SINK/SINK_SCAN slot the
  consumer binds to directly (F-GUARD-SEMANTICS,
  F-PRUNED-PROJECTION-DEPS); coercion-decision replay becomes
  snapshot-first two-phase — operands snapshotted at pass entry,
  ownership verified by re-deriving the ORIGINAL layer at the snapshot
  types against the retained layer, the final layer regenerated at the
  final envelope types — folded positions recover through the explicit
  fold table, and the `Literal.Src` channel is withdrawn
  (F-POST-FOLD-RECOVERY — folded-position recovery is superseded by
  entry 49's bind-time decision ledger in revision 11); and the materialization witnesses plus the
  (a‴) benchmark re-base on a deterministically profitable filtered
  producer with an unfiltered decline control, and the public
  contract's single-reference simultaneity claim is replaced by
  separated reachable two-reference statements
  (F-PROFITABLE-MATERIALIZATION).
Resolved in
  revision 11 (decision log 48–50): the per-argument shadow
  construction decomposes by scope — ONE shadow per (producer,
  referenced exported slot) replicates that slot's own producing
  expression and covers exactly the producer-representable divergent
  seams, while a seam reading another relation, another producer's
  export or a captured input keeps its in-tree cast at the consumer,
  which RECOMPOSES its argument over the exported slots
  (F-SCOPE-SPANNING-ARGUMENTS — revision 10's whole-argument
  replication claim is withdrawn: `AVG(COALESCE(c.v, u.d))` over a
  shared `c` joined to `u` has inputs in both scopes); folded-position
  recovery is replaced by a bind-time statement-local DECISION LEDGER
  recording each coercive decision's pre-coercion operand domains at
  the binding site, because constant folding destroys declared width
  and scale before any later snapshot can read them — the fold table
  and the pad-only lemma are withdrawn (F-FOLD-DESTROYS-DOMAINS);
  and the materialization admission reservation charges the
  SHADOW-EXPANDED output schema — one fixed-width Decimal256 per
  provenance-carrying exported slot — before the share commits,
  all-or-nothing with the existing counters
  (F-SHADOW-ADMISSION-ACCOUNTING). Resolved in revision 12 (decision
  log 51–54): the value-merge families reconverge carrier-overflowing
  common domains over the operands' parent-published types at the merge
  call sites, the parent's outcome by construction, comparisons
  excluded by their existing saturation fallback
  (F-COMMON-DOMAIN-OVERFLOW); the decision ledger is keyed by CONTENT —
  copy-stable across projection inlining and shadow construction,
  pass-entry captured, an observable-state ambiguity failing loudly
  (F-LEDGER-LINEAGE — supersedes entry 49's node-identity keying);
  folded-position ownership verification becomes positional — artifact
  type against the recorded post-merge aligned type — with folded
  values re-normalized exactly into their recorded domains before
  regeneration (F-FOLDED-LAYER-WITNESS); and the folding decline guards
  the recursive fold entry, so NULL-producing folds of
  provenance-carrying trees no longer bypass the materialization-time
  guard (F-NULL-FOLD-PROVENANCE). Resolved in revision 13 (decision
  log 55–56): the statement-local decision barrier keeps every
  provenance-flagged recorded decision's operand cast syntax unfolded
  until pass-entry capture — the parent-successful folded-twin
  statement executes with parent-equivalent values, result types and
  persisted metadata, and the residual collision failure classifies
  mechanism bugs only (F-LEDGER-COLLISION); and the reconvergence
  retry moves from the merge check call sites — whose contract is a
  pure `[]types.Type` — into the binder-owned source-domain-channel
  funnel, resolving parent-published domains through direct trees,
  producer references and scalar-subquery projections with
  genuine-wide outcomes preserved verbatim
  (F-RECONVERGENCE-CHANNEL). Resolved in revision 14 (decision
  log 57–58): the source-domain resolver gains the binder's
  tag/ordinal pending-producer channel — pending aggregate outputs
  resolved through `BindContext.aggregates` and pending window
  outputs through `ctx.windows`, recursing into the retained
  producing calls and deriving each producer output's parent type
  through the parent's registered result rule over the resolved
  parent argument domains, feeding both reconvergence retries and
  barrier provenance (F-AGG-WINDOW-CHANNEL); and one
  planning-generation lifecycle owner installs a fresh empty
  statement registry at both outermost planner entries — `BuildPlan`
  and `(*BaseOptimizer).Optimize` — shares it with nested entries
  through a depth marker, and restores the exact prior field value
  when the outermost entry returns on success, error and panic
  unwind alike, establishing statement isolation, retry/reprepare
  freshness and bounded retention on the reused session process
  (F-REGISTRY-LIFECYCLE). Resolved in revision 15 (decision
  log 59–60): the tag/ordinal channel completes the same-block
  result-tag inventory — grouped producers descend
  `BindContext.groups[colPos]` (the retained bound group key,
  `group_binder.go:272–290`; the output type with
  `groupOutputType`'s grouping-set nullability adjustment,
  `types.go:1093–1098`), sampled producers descend
  `ctx.sampleFunc.columns[colPos]` (`build_sample.go:76`/`:96`) and
  projected producers descend `ctx.projects[colPos]`
  (`order_binder.go:380–390`/`:450–460`,
  `base_binder.go:682–692`), while `timeTag` (timestamp
  boundaries only, `base_binder.go:625–644`) and
  `resultTag`/`sinkTag` (never minted by expression binders,
  `bind_context.go:181–198`) resolve irrelevant by construction —
  so the grouped merge `SELECT COALESCE(a*b, CAST(1 AS
  DECIMAL(65,1))) FROM t GROUP BY a*b` retries to the parent's
  `(66,1)` and the grouped folded twins execute with
  parent-equivalent values, result types and persisted metadata,
  the inventory feeding both reconvergence retries and barrier
  provenance (F-GROUPED-CHANNEL); and the planning-generation
  lifecycle owner gains its third invocation at the shared SELECT
  funnel `bindAndOptimizeSelectQueryWithValidatorAndCapture`
  (`build.go:67`) — the funnel every covered generation flows
  through and the standalone view-description generations (the
  runtime view-columns table function, SHOW view metadata,
  standalone schema inference and persisted-environment
  regeneration) enter with no enclosing `BuildPlan`/`Optimize` —
  installing fresh before `builder.bindSelect`, holding through
  `builder.createQuery()`, releasing on every terminal path
  including the publication-probe bind-only return
  (`build.go:145–151`), sharing through the depth marker when
  nested, and keeping per-call freshness on the regeneration's
  reused recovery process — the planned production set unchanged
  at 17 production files (F-VIEW-DESCRIPTION-LIFECYCLE). Resolved
  in revision 16 (decision log 61–62): the revision-15 `timeTag`
  exclusion was incomplete — whenever a TIME_WINDOW clause is
  present (`ctx.timeTag > 0`, `query_builder.go:6216`, set before
  any clause binds), aggregate binding routes through
  `BindTimeWindowFunc` (`base_binder.go:3369–3376`,
  `having_binder.go:955–1010`), which retains the
  boundary-adjusted aggregate call in `ctx.times` and publishes
  it as `ColRef{timeTag, colPos}` (`makeTimeWindowProjectionExpr`,
  `having_binder.go:721–736`), so a decimal TIME_WINDOW output
  reached the reconvergence families with no channel and no
  barrier provenance; the resolver now descends
  `ctx.times[colPos]`, distinguishing timestamp boundary
  self-references (`Expr_Col`, irrelevant by construction) from
  retained aggregate outputs (`Expr_F`, recursing uniformly
  through their `{aggregateTag, aggColPos}` arguments into
  `ctx.aggregates[aggColPos]` with the parent's registered
  rule), and the witness `COALESCE(MAX(a*b), CAST(1 AS
  DECIMAL(65,1))) INTERVAL(ts, 1, minute)` retries to the
  parent's `(66,1)` (F-TIMEWINDOW-CHANNEL). Separately, §4.1's
  casts rule kept every ≤65-target envelope, so the merge
  families' REAL binder-inserted scale-changing alignment casts
  below width 66 shadowed the producer's parent envelope with
  no aggregate to trigger §5's replay; storage-envelope
  evaluation now looks through a verified decision-owned ≤65
  cast to the operand's reconstructed parent domain —
  `COALESCE(TRUNCATE(a*b,0), d)` over `(38,10)` operands with
  `d DECIMAL(39,1)` persists the parent's `(54,1)` in CTAS,
  prepared CTAS, view authoring, view description and legacy
  replay, with SQL-authored and decision-foreign fixed-target
  casts preserved verbatim (F-BOUNDARY-ALIGNMENT-DOMAINS). Both
  repairs land inside already-owned files (the `base_binder.go`
  resolver; the `build_ddl.go` envelope evaluator consuming the
  already-carried registry read-only) — the planned production
  set stays 17 production files. Resolved in revision 17 (decision log 63): entry 62's look-through
  reads the completed statement-local decision ledger, but TWO
  boundaries evaluated storage envelopes with NO ledger available,
  each losing the parent's `(54,1)` to the shadowing `(65,1)` the
  ledger-miss rule preserves. Entry 60 installed the standalone
  view-description generation at the shared funnel, but
  `inferViewColumns` (`build_ddl.go:240`) assembles view column
  records at `:345–387` AFTER its funnel call at `:301`/`:308`
  returns, so every standalone description and replay path evaluated
  against the restored registry; the standalone owner moves one level
  out to `inferViewColumns` itself, its generation spanning the funnel
  call, the post-funnel validators and the assembly, released on
  success, error and panic unwind alike, the funnel keeping its
  invocation as the defensive standalone entry (entry 63(i)). And
  prepared-CTAS refresh (`RefreshPreparedCTASInferredColumns`,
  `build_ddl.go:2396–2476`) runs after planning, caching and parameter
  specialization have returned — no generation active and no process
  in its signature — at every entry path
  (`computation_wrapper.go:1922–1926`, `:436`, `:3084–3089`); the
  completed ledger RETAINS through prepare on the repository's own
  `DecimalFloatProofs` build-time-evidence carrier pattern
  (`prepared_binding.go:996`/`:1059` claimed into the bundle, cached
  on `types.go:410` and populated/cleared at `:833`/`:850`, mirrored
  on the computation-wrapper cache slot), specialization
  (`utils.go:4635`, already in the set) refreshing retained entries
  over its re-derived subtrees, each refresh call site forwarding the
  carrier into read-only evaluation under entry 62's ownership rule, a
  ledger miss preserving the declared envelope — the witness
  re-deriving the parent's `(54,1)` and the explicit-cast control
  keeping `(65,1)` (entry 63(ii)). The owner move lands inside the
  already-owned `build_ddl.go`; `pkg/sql/plan/prepared_binding.go`,
  `pkg/frontend/types.go` and `pkg/frontend/computation_wrapper.go`
  join the planned production set, i.e. 20 production files.

  Resolved in revision 18 (decision log 64): revision 17 routed the
  retained prepared-CTAS ledger through the `DecimalFloatProofs`
  bundle and the specialization cache, but neither runs for a
  prepared CTAS — PREPARE resolves CREATE TABLE through
  `getPreparePlan`'s default branch to `BuildPlan`
  (`build_dcl.go:99–101`), `createPrepareStmtInSession` constructs
  the session's `PrepareStmt` directly
  (`mysql_cmd_executor.go:3167–3173`) with no bundle on its path,
  and execute's sourceBindingQuery gate
  (`computation_wrapper.go:1629`) requires a top-level Query a DDL
  payload never has, so the bundle claim and the cache population
  (`:1916` → `:2045` → `types.go:833`) never execute for it.
  Publication moves to the real prepared-DDL lifecycle (entry 64
  (i)-(iii)): the authoring generation's outermost release stashes
  the completed-ledger copy in a generation-keyed, read-once
  process-side claim slot — stale slots unclaimable, unclaimed
  slots discarded by the next fresh install, a PREPARE failing
  before construction never claiming — `createPrepareStmtInSession`
  claims it LATE into a new `PrepareStmt` field for the cached-plan
  execute channel (`:1921–1926`), while both retry paths
  (`:416–440`, `:3067–3089`) claim their FRESH
  `buildPlanWithPrepareMode` generation's stash, replacing any
  earlier copy, and release pairs with the specialization-cache
  lifetime (`types.go:850`, `PrepareStmt.Close`). And revision 17's
  fixture never reached storage refresh (entry 64 (iv)): no
  parameter influenced the inferred output type, so the
  `reflect.DeepEqual` skip (`build_ddl.go:2452–2453`) fired and
  `:2470` never ran; the parameterized witness
  `COALESCE(TRUNCATE(a*b, ?), d)`, EXECUTE `?=0`, makes
  specialization necessarily change the inferred output type
  (`refineDecimalRoundingReturnType`'s param-marker early return,
  `base_binder.go:9360–9396`), with a typed oracle on every entry
  path asserting the boundary evaluation ran and consumed retained
  evidence, the explicit-cast `(65,1)` control and the
  no-parameter no-op control. `pkg/sql/plan/prepared_binding.go`
  leaves and `pkg/frontend/mysql_cmd_executor.go` joins the planned
  production set — still 20 production files.

  Resolved in revision 27 (decision log 73): the twenty-sixth
  review's finding verified the previous repair and exposed the set
  operation's OWN common-type binding as the remaining owner of
  the same defect family: both owners (the initial
  `buildUnionWithResultLen` at `query_builder.go:4828`–`:4830`
  and the prepared `preparedSetOperationCommonType` at
  `utils.go:7588`–`:7590`) look up `coalesce` directly over
  branch surface types, and the merge checker's decimal
  common-domain arm (`function/type_check.go:1113`–`:1117`)
  rejects a carrier-overflowing width BEFORE any alignment cast
  exists or any outer consumer retry can run — the mixed-scale
  counterexample (a widened `(76,0)` product arm meeting a
  `(65,1)` sibling) computes width 77 and fails TODAY at both
  owners (probe MX/MXR), while the parent combines `(66,1)` and
  succeeds; cell (xix)'s scale-0 branches bind fine and could
  never see it. The repair retries the FAILED lookup once per
  owner over resolver-computed parent-published branch domains
  (the shared entry-56 resolver with the entry-72 coercion-source
  recovery preserved), authors the parent's own common type and
  per-branch alignment view, and RE-ARMS the original rejection
  on retry failure and for every no-provenance branch set — the
  genuine-wide 77 control (probe MW) failing identically before
  and after, SQL-authored cast arms keeping their authored
  targets (probe MP). Section 9 I10 gains cell (xx); entry 56
  gains one kept-as-history annotation covering its funnel-scope
  sentence; the planned production set stays 21 production files
  (both owners already inside it).

  Resolved in revision 26 (decision log 72): the
  twenty-fifth design review's finding verified at code and by an
  instrumented probe that entry 71 (iii)'s contributor evaluation
  retains a set-operation-OWNED alignment cast as the branch lineage
  wherever branch widths differ (`query_builder.go:4968`–`:4996`
  inserts it, `build_ddl.go:2381` returns it unchanged,
  `base_binder.go:9803`–`:9809` treats it as a fixed target), so a
  prepared merge consumer over a UNION ALL of UNEQUAL widened-product
  branches (`(38,0)²` publishing `(76,0)`, `(35,0)²` aligned to
  `(76,0)`) still failed the parent's converging bind; the probe
  reproduced the pre-existing merge-bind error pre-refresh in BOTH
  branch orders and observed the engine's ownership predicate
  recovering the engine-inserted coercion's source multiply while
  declining the SQL-authored overload-1 pinned casts. The lineage view
  now recovers each set-operation-owned coercion's SOURCE through
  that existing predicate (`unwrapPreparedSetOperationCoercion`,
  `utils.go:7512`–`:7529`) before deriving the contributor's parent
  domain, combining through the unchanged typing contract and
  reserving decline for genuinely unresolvable sources; the planned
  production set stays 21 files.

  Resolved in revision 25 (decision log 71): the
  twenty-fourth design review's first finding verified at code
  and by an instrumented probe that entry 70 (iv)'s delegation
  declined every multi-producer result while the delegated
  resolver's set-operation case returns all branch contributors
  by design (`build_ddl.go:2367`–`:2383`), so a prepared
  merge consumer over a UNION ALL of two widened-product
  branches still lost lineage; the probe's set-operation
  counterexample reproduced the pre-existing merge-bind error
  pre-refresh with the storage domain resolving both branch
  contributors, the pinned twin succeeding under the identical
  topology. The lineage view now resolves each contributor's
  parent domain and provenance branch-wise and combines them
  through the parent's own set-operation typing contract
  (`query_builder.go:4819`–`:4833`), restoring the parent's
  `(66,1)`/1.0 parity and reserving decline for genuinely
  unresolved cases; the planned production set stays 21 files.
  The second finding was a projection-only contradiction with
  I2 (`:124`–`:134`): the Challenger projection is
  re-aligned so widened multiplication never authors a wider
  schema than the parent's own inference would have authored
  while parent-genuine above-65 inferred domains persist,
  retaining the `DECIMAL(66,1)` witnesses explicitly.

  Resolved in revision 24 (decision log 70): the
  twenty-third design review's first finding verified at code and
  by an instrumented probe that entry 69 (iv)'s lineage-view
  closure indexes physical children only
  (`utils.go:8143`–`:8146`), so a prepared merge consumer
  over a materialized shared CTE exporting a widened product
  still lost lineage at the SINK_SCAN source-step edge; the
  probe's unstable-export counterexample reproduced the
  pre-existing merge-bind error pre-refresh across two SINK_SCAN
  readers of one producer step, the pinned twin succeeding under
  the identical topology. The view's producer resolution now
  delegates to the complete existing storage-domain owner
  (`resolveChildRelationalOutput`, `build_ddl.go:2173`–
  `:2395`) with its SourceStep traversal and path-bounded cycle
  handling, restoring the parent's `(66,1)`/1.0 parity; the
  planned production set stays 21 files. The second finding was
  a projection-only regression: §9 I10 (xv) and entry 68
  (vi)–(vii) stand intact here, and the Challenger projection
  restores the VALUE-arm paragraph verbatim alongside the
  aggregate-producer and materialized-CTE clauses.

  Resolved in revision 23 (decision log 69): the
  twenty-second design review verified at code, confirmed by an
  instrumented probe, that revision 22's execute-rebind evidence
  lifecycle covered the refresh boundary but left the earlier
  fill-time rebind without producer lineage: a rebound merge
  consumer referencing a remapped aggregate producer (`MAX(a*b)`
  re-expressed as `Expr_Col{RelPos: -2}`,
  `query_builder.go:2332`–`:2344`) reached the retry funnel
  with neither binder context nor channel entry, so the widened
  `(76,0)` operand's 77-digit family common failed the first
  lookup and the declined retry propagated the pre-existing
  merge error before `RefreshPreparedCTASInferredColumns` could
  run. The resolution scope gains a plan-producer lineage view
  reusing the projection-refresh envelope owner's per-node
  inventories, attached around the same fill-and-refresh window
  on all three execute routes with retained evidence read-only,
  restoring the parent's `(65,1)`/1.0 parity on the reviewer
  counterexample; the planned production set stays 21 files.

  Resolved in revision 22 (decision log 68): the
  twenty-first design review verified at code that
  revision 21 overgeneralized its fixture — a parameter
  marker in a provisional common-VALUE arm is rebound at
  EXECUTE (`ResetParamRefRule`, `utils.go:6678`;
  `visit_plan_rule.go:3320`–`:3346`, `:3861`–`:3879`,
  with a nil process at `base_binder.go:7448`–`:7449`),
  changing the output column's inferred type and defeating
  the per-column refresh skip, so the boundary evaluation IS
  reachable there and runs without decision evidence. The
  instrumented probe (harness deleted after capture) fixed
  the ground truth: the value-arm fixture
  `COALESCE(TRUNCATE(a*b, 0), ?)` over `(38,10)` operands
  snapshots `VARCHAR(65535)` on both engines; EXECUTE
  `?=1.25` persists `(59,6)` with or without the carrier
  (the `(70,6)` cast looked through by §4.1's >65 rule),
  but EXECUTE `?=0.5` persists `(65,1)` where the parent's
  own execute derives `(54,1)` — the divergence this
  revision repairs. The lifecycle: the rule carries
  the execute scope's process, the pre-existing binder
  recording hook captures the rebound coercive decisions
  into an execute-scope registry, and the refresh consumes
  the retained-carrier entries overlain by that registry,
  persisting the parent's `(54,1)`/`(59,6)` per binding on
  both engines; provenance-free statements keep an empty
  execute registry (`decisionOwnedCastOperand` sees the
  carrier alone) and byte-identical plans.
  `pkg/sql/plan/visit_plan_rule.go` joins the planned
  production set — 21 production files — the
  digits-marker fixture standing as the skip control and
  Witnesses A/B keeping their formations with execute-scope
  entry assertions added.

  Resolved in revision 21 (decision log 67): implementing revision
  20's witness verified at code and by an instrumented planner probe
  that its execute-route premises collapse — no execute route
  re-derives a prepared DDL plan's inferred output types: every fill
  is substitution-only (base `computation_wrapper.go:431`/`:1922`/
  `:3085` feeding the refresh at `:436`/`:1926`/`:3089`), the
  ordinary rebuild builds the PREPARE statement with markers again
  (`rebuildPreparePlan`, base `:948`–`:951`), and the
  round/truncate prepared-numeric fallback marks the VALUE argument
  only (`base_binder.go:3399`–`:3514`), so the fixture's outer
  TRUNCATE head keeps `(65,1)` on both engines through EXECUTE,
  never the claimed (64,0)/(53,0) — and the pre-existing
  per-column refresh skip (base `build_ddl.go:1627`–`:1628`)
  compares the filled query's output type against the ORIGINAL
  query's output type, equal for every function-headed column, so
  the execute-route boundary evaluation never runs and the parent's
  own EXECUTE persists its PREPARE snapshot tag `(54,1)`, not the
  claimed `(53,0)`. The observable contract is corrected, not the
  engine: PREPARE storing the parent's `(54,1)` against the widened
  head `(65,1)` remains the genuine consumption discrimination,
  every EXECUTE route persists `(54,1)` on both engines (snapshot
  reuse through the skip or rebuild-generation re-derivation), and
  the empty-ledger ablation relocates to a labeled harness probe
  bypassing the unreachable skip — `(54,1)` with retained
  evidence, `(65,1)` without — while the explicit-cast control
  persists its authored `(65,1)` at PREPARE and EXECUTE on both
  engines and the no-parameter control keeps the parent's `(53,0)`.
  Witness A/B keep their formations, probes and claim lifecycle with
  value oracles corrected to `(54,1)`; the retained carrier ships as
  read-only conservative insurance with no reachable widened-decimal
  consumption trigger, removal considered and rejected. No
  production file joins or leaves the planned set — still 20
  production files.

  Resolved in revision 20 (decision log 66): revision 19's witness oracle
  collapsed — its sibling alignment cast targets (76,12), and casts wider than
  65 are looked through unconditionally, independently of the retained ledger
  (§4.1, `build_ddl.go:1870–1881`), so an empty ledger reconstructs the same
  (53,0) the witness claimed for consumption, the claimed (64,0) miss being
  unreachable — and its cached-generation reuse scenarios were unreachable: on
  the changed binary every ordinary-database CTAS plan carries a
  non-nil `DatabaseDefaults` (`database_defaults.go:33–42`, `:210–250`,
  installed at `build_ddl.go:4466–4471`; the CTAS grammar offers no
  charset/collation table option), so `shouldRebuildPreparePlan` returns true
  unconditionally (`computation_wrapper.go:2834–2837`), feeding
  `rebuildEveryExecute` at `:1444` into `needRebuild` at `:1477–1480` — every
  EXECUTE rebuilds even without invalidation. The fixture now embeds an inner
  literal-held TRUNCATE — `TRUNCATE(COALESCE(TRUNCATE(a*b, 0), d), ?)`,
  EXECUTE `?=0` — so the widened COALESCE family (65,1) diverges from the
  parent's (54,1), a recorded decision whose ≤65 alignment casts are
  decision-owned: the boundary evaluator keeps an unverified ≤65 cast's
  declared envelope and looks through a verified decision-owned one (decision
  log 62, `build_ddl.go:1870–1881`), making the three oracle values genuinely
  distinct — consumption persists the parent's (53,0), the empty-ledger
  ablation reconstructs (64,0), refresh-never-ran leaves the snapshot's
  (54,1) — with the explicit-cast and no-parameter controls re-parameterized
  and the typed probe unchanged on all four entry paths. The rebuild
  validation splits into reachable witnesses: Witness A, the live-service BVT,
  asserts the formation probe (the stored plan's `DatabaseDefaults` non-nil)
  and per-execute rebuilding under the PRE-EXISTING CTAS rebuild policy —
  preserved verbatim — each execute consuming its own rebuild generation's
  ledger with carrier replacement; Witness B, the cached-generation-reuse UT
  formation, asserts its preconditions first (`DatabaseDefaults` absent via
  the established `MOProtocolVersion` test pattern with save/restore; no
  intervening invalidation), then asserts NO rebuild occurred before checking
  nonce identity and repeated consumption. All placements stay inside the
  revision-18 owners — the planned production set stays 20 production files,
  the round changing only §9 fixtures, oracles and UT/BVT witnesses.

  Resolved in revision 19 (decision log 65): entry 64 left the ordinary
  prepared-plan rebuild channel unspecified — a session DDL or schema change
  between PREPARE and EXECUTE flips the cached-version comparison, and a mode or
  protocol change flips the same `needRebuild`
  (`computation_wrapper.go:1423–1425`, `:1468–1480`), `rebuildPreparePlan`
  builds a FRESH generation at `:1537`, and the replacement commits at
  `:1563–1566` (`prepareStmt.PreparePlan = newPlan`, `:1565`), bypassing
  `createPrepareStmtInSession` and both retry claims, while the same path clears
  the specialization cache at `:1640–1642` — revision 18's paired release owner
  — before the CTAS specialization consumer at `:1922`–`:1926`. Publication now
  covers the fourth channel: the rebuild generation stashes its ledger copy
  through the same entry-64 machinery, and the executor claims it inside the
  `needRebuild` block, ordered AFTER the replacement commit and BEFORE the cache
  clearing, replacing the prior carrier on success and leaving prior plan and
  carrier untouched on failure. Release decouples from the specialization-cache
  clearing to the seed-plan generation's own lifetime — a replacing claim,
  `PrepareStmt.Close` on DEALLOCATE and eviction, or the claiming wrapper
  scope's failure cleanup — so the cached-execute channel keeps its consumer
  evidence for the seed plan's whole lifetime. And entry 64 (iv)'s fixture still
  kept output types equal: the retained non-provisional sibling cast aligning
  `d` into the merge family (`base_binder.go:8990–9050`, `:5108–5111`;
  `visit_plan_rule.go:3017–3025`, `:4823–4852`) held the COALESCE head at
  `(76,12)`, the prepare-stored type, so the DeepEqual skip still fired and the
  parent's matching envelope was `(65,12)`. The witness now places the parameter
  marker in the TRUNCATE head — `TRUNCATE(COALESCE(a*b, d), ?)`, EXECUTE `?=0` —
  so the snapshot's unrefined head `(76,12)` (storage tag `(65,12)`,
  parent-derivable) refines to `(64,0)` at specialization on both engines,
  defeating the skip, with a typed probe oracle on every entry path — including
  the rebuild channel — discriminating consumption (parent `(53,0)`) from a
  ledger miss (`(64,0)`) and refresh-never-ran (`(65,12)`), the authored
  interior-cast control persisting `(64,0)` from a stored `(65,1)` on both
  engines, the no-parameter control persisting the parent's `(53,0)` from
  prepare-time evaluation with refresh never reached, and a schema-invalidation
  scenario asserting the consumed ledger's nonce equals the rebuild generation's
  with carrier replacement. No alignment replay is adopted; cast ownership stays
  with the provisional-cast machinery. All placements live inside the
  revision-18 owners — the planned production set stays 20 production files.

## 11. Design decision log

Accepted tradeoffs and resolved questions (so implementation review does
not reopen them without new evidence):

1. Publication widened to min(w1+w2, 76) for multiplication only; declared
   65-domain enforcement moves to the storage boundary (round 1).
2. Kernel authority + shared validator ≥76 defer confirmed as the single
   physical-overflow decision point (round 1; pre-existing guard).
   **Corrected in part by entry 22 (revision 5)**: the pre-existing kernel
   was not single-criterion on every multiply path — the scaled fallback
   kept a declared `DECIMAL(65, s)` admission until revision 5 removed it.
3. Storage boundary reconstructs envelopes from the optimized plan at the
   DDL seam; A1/A2 rejected (§7).
4. LAG/LEAD shared expression result capped at 76 with value-admission at
   the boundary; undeclarable scale merges rejected at planning (rounds
   6–8).
5. Folding preserves multiplication provenance (decline instead of
   shape-inference); folded genuine scalars keep typed envelopes (rounds
   8–9).
6. ~~Relational resolution with two path-scoped cycle bounds~~ ~~(revision
   2: memoized least-fixpoint dataflow)~~ **superseded by entry 12
   (revision 3)**: the least fixpoint was itself unsound — the LAG/LEAD
   transfer is lossy and non-monotone, and growth per slot did not bound
   visits; see §4.3 (round-14 review).
7. ~~AVG Rule A applies unconditionally on width>65~~ ~~(revision 2:
   provenance-gated binder Rule A, aggexec base-identical)~~ **superseded
   by entry 13 (revision 3)**: no multiplication-keyed AVG rule exists
   anywhere; see §5 (round-14 review).
8. Constant-folded wide-mul trees stay executable (micro-optimization
   trade, §10).
9. `percentile_disc` joins the value-arg envelope rules; the width-
  preserving consumer inventory is machine-checked; the typed fallback is
  replaced by the §4.2 provenance tiers (round-12 review).
10. Views: M1 catalog contract at the single `inferViewColumns` assembly,
    M3 CTAS-through-view equivalence, M4 legacy-viewdef identity without
    protocol bump, M5 outcome map (round-12 review); **M2 revised by entry
    15 (revision 3)** — the claimed `restoreViewMySQLSpecialTypes`
    reconciliation is ENUM/SET-only and does not exist for decimals,
    replaced by the two-owner presentation contract of §6 (round-14
    review).
11. PR #31034 overlap recorded; composition stated; rebase/re-validation
    deferred to the implementation turn after PASS (round-12 review).
12. ~~Revision-2 least-fixpoint dataflow~~ **superseded (revision 3,
    F-DATAFLOW; recursion clause revised by entry 20 in revision 4)**:
    envelope evaluation is a demand-driven memoized one-shot walk with
    anchor-seeded recursion (§4.3) — every slot evaluated at most once
    against already-final inputs, recursion closed on the anchor domain;
    ~~recursive members checked by an admission envelope against the
    anchor's declarable projection~~ — the planning reject is withdrawn in
    revision 4: union slots keep the anchor seed as the persisted schema
    and member admission is the parent's own value-based cast coercion
    (F-RECURSIVE-ADMISSION); the work bound is the linear walk itself,
    with once-per-slot counts pinned by §9.
13. ~~Revision-2 provenance-gated AVG Rule A~~ ~~(revision 3, F-AVG2-EXEC,
    F-AVG2-DERIVE: AVG as the parent's uniform function of the argument's
    published type with zero deviation)~~ **superseded by entry 18
    (revision 4, F-AVG-BASE-CAPACITY)**: the executor's re-derivation
    channel and the defeat of origin classification stand, but the
    published argument type changed out from under the width-sensitive
    formula, newly rejecting previously successful averages and changing
    base rounding and persisted AVG schemas; the aggregate argument is
    bound in the base-equivalent domain instead (§5).
14. ~~Revision-2 boundary `max(65, ·)` floor~~ **superseded (revision 3,
    F-BOUNDARY-FLOOR)**: the boundary persists the envelope-derived
    declared type verbatim, preserving base-derived widths below 65 (e.g.
    `DECIMAL(64,2)` for `ROUND((38,2)×(38,2), 2)` via the parent's
    `refineDecimalRoundingReturnType` mirror), with parent-versus-design
    metadata oracles in CTAS, prepared CTAS and view replay.
15. Decimal view presentation ownership (revision 3, F-VIEW-PRESENT;
    **outer-expression clause revised by entry 21 in revision 4**):
    result-set metadata and values are owned by the executed rebound plan
    (carrier domain, exact values, no narrowing cast); catalog surfaces
    are owned by the persisted M1 declared records; ~~outer expressions
    carrier-exact~~ — outer expressions keep each operator's own
    pre-existing result-domain contract (revision 4,
    F-VIEW-OUTER-OPERATORS); nested views included; both pinned by
    independent §9 oracles.
16. Scale rule and overflow oracle corrected (revision 3,
    F-SCALE-OVERFLOW): the product scale is the pre-existing
    `min(s1+s2, max(12,s1,s2))` rule stated verbatim; physical overflow is
    decided solely by the kernel's checked 256-bit coefficient range —
    carrier-fitting 77-digit products are accepted and coefficient-range
    overflow rejects, with §9 controls for scaled rounding, 77-digit
    acceptance and true overflow. The sole-criterion claim did not hold
    inside the scaled fallback until entry 22 (revision 5) repaired it.
17. M4 strengthened (revision 3): the verbatim boundary rule extends
    legacy replay byte-identity to width-reduced wrapper shapes (`(64,2)`
    family), not only to ≤65-published shapes.
18. AVG base-equivalent argument domain (revision 4,
    F-AVG-BASE-CAPACITY): the widened publication changes the argument
    type the width-sensitive AVG formula consumes, so a `(76,0)` product
    argument newly derived `(65,4)` — rejecting a fitting `10^62` average
    the parent returned under `(65,0)`, changing base rounding (`6` →
    `6.0000`), and altering persisted AVG CTAS/view schemas. At aggregate
    binding, a decimal AVG argument whose envelope differs from its
    published type is wrapped in an ordinary cast to the envelope domain;
    planner and executor both read that bound argument type through the
    pre-existing registration (byte-identical aggexec), argument values
    keep the parent's per-value admitted domain with the checked cast
    family, and genuine-wide inputs receive no cast (envelope == typed). §5
    holds the matrix and §9 the consistency, plan-shape and
    parent-versus-design value/metadata oracles for ordinary and window
    AVG, CTAS and legacy-view replay.
19. Affected-domain boundary trigger (revision 4, F-BOUNDARY-TRIGGER):
    the boundary fires wherever the memoized envelope differs from the
    published type, not only above published width 65 — wrappers over
    mid-scale products (e.g. `ROUND`/`TRUNCATE(product, 0)` over
    `(38,10)×(38,10)`) publish `(65,0)`/`(64,0)` yet must persist the
    parent's `(54,0)`/`(53,0)`; columns whose envelope equals their
    published type keep the authored type exactly (genuine domains
    preserved). §9 separates ROUND and TRUNCATE metadata oracles
    (ROUND `(64,2)`, TRUNCATE `(63,2)` over `(38,2)` products plus the
    mid-scale pair) across CTAS, prepared CTAS and view replay.
20. Value-based recursive-member admission (revision 4,
    F-RECURSIVE-ADMISSION): the parent coerces recursive members to the
    anchor's projection types with ordinary casts
    (`query_builder.go:5904–5927`), admitting per value; revision 3's
    admission-envelope planning reject is withdrawn because unused wide
    member branches made it reject queries whose every produced value
    fits. Union slots persist the anchor-seeded envelope (the parent's
    anchor-typed columns), no recursive-member slot is envelope-evaluated
    for the schema, and oversized produced values keep the checked
    admission/kernel families; §9 pins the paired fitting/out-of-range
    cases and the zero-member-evaluation counts.
21. Per-operator outer-view expression contracts (revision 4,
    F-VIEW-OUTER-OPERATORS): a rebound view column presents its carrier
    type to outer expressions, but each consumer operator keeps its own
    pre-existing result-domain contract — width-preserving consumers
    (e.g. `abs`) evaluate carrier-exactly, while 65-capped operators
    (e.g. addition via `decimalAddSubReturnType`) keep their cap and the
    pre-existing checked validator rejection. M2/M5, §9 and the public
    contracts are aligned; the positive full-carrier oracle at the view
    seam is `abs`.
22. Scaled-fallback admission joins the physical criterion (revision 5,
    F-FALLBACK-DOMAIN): the multiply kernel's scaled fallback admitted
    against the `DECIMAL(65, s)` digit domain (`d256MulScaledFallback`'s
    call of `d256MulResultFits`, `arith_decimal_fast.go:2193–2218`),
    reached from the `scaleAdj < −38` high-scale dispatch and the
    raw-overflow recovery (`d256MulSlowOne`), contradicting I1 for
    carrier-fitting coefficients such as the `10^70` rounding of a
    `(65,40)×(65,40)` product holding `10^15`. Revision 5 replaces that
    one comparison with the inline path's exact physical criterion
    (rounded coefficient magnitude below `2^255`). Acceptance strictly
    widens — every previously accepted coefficient (magnitude below
    `10^65` and the inclusive `−10^65` boundary) stays accepted;
    round-half-up, raw-overflow recovery and the pre-existing
    `Decimal256 Mul overflow` family are unchanged; the inline and
    validator paths stay byte-identical. §9 pins the exact-return case,
    raw-overflow recovery, the `≥2^255` rejection, both-sign boundaries
    and base-acceptance regression cells. Corrects entries 2 and 16.
23. Width-sensitive capped consumers reconstruct through their owning
    rules (revision 5, F-CAPPED-CONSUMERS): 65-capped does not imply
    width-insensitive — `add`/`subtract` (`decimalAddSubReturnType`),
    `divide` (`decimalDivisionReturnType`), the round family and
    `avg`/`sum` (below argument width 43) derive different results across
    the widening seam, and revision 2–4's fallback would clamp their
    published type instead of reconstructing the parent's below-cap type
    (`TRUNCATE((38,10)x(38,10),0)+z(39,0)`: parent `(54,0)`, not clamped
    `(65,0)`; its AVG over `{1,2}`: `(58,4)`/`1.5000`, not `(65,0)`/`2`).
    §4.1 mirrors the scalar rules over operand envelopes, §4.2's
    exhaustiveness UT gains the seam-pair check two (class (ii)
    whitelist), the boundary persists the chain results `(54,0)`/`(59,4)`
    parent-verbatim, and §5's rebound trigger generalizes to every
    decimal aggregate whose registered result differs — bare-product SUM
    stays cast-free. **Superseded in part by entry 27 (revision 6)**: the
    registered-result agreement gate is withdrawn; the cast fires
    wherever envelope ≠ published, so bare-product SUM receives the
    envelope cast like every divergent decimal aggregate argument, and
    the width-sensitivity inventory survives as documentation and
    exhaustiveness check two, not as a trigger input.
24. Aggregate reconciliation at the final plan (revision 5,
    F-BINDING-PHASE): revision 4's "at aggregate binding" insertion had
    no positional envelope resolver — pending grouped/window outputs live
    in BindContext binding tags (`base_binder.go:3613–3635`) and scalar
    subqueries stay `Expr_Sub` (`base_binder.go:1010–1031`) until
    `flattenSubqueries` (`query_builder.go:6700–6719`, `:11511`,
    `:11619`/`:11644`). The pass now runs at the end of `createQuery`
    (`query_builder.go:4593`) on the post-flatten, post-remap
    (`:4538`/`:4540`) final plan, walking `AggList`/`WinSpecs` with the
    shared §4.3 evaluator, so flattened scalar subqueries, derived-table
    references and grouped/window aggregates all bind the base-equivalent
    argument domain, and the authored result type is re-derived through
    the parent's registration. §9 pins the placement shapes.
    **Extended by entry 26 (revision 6)**: the pass gains the P1–P3
    propagation sweeps — reconciled node types, copied slot/reference
    types and downstream consumer types are all made consistent before
    execution. **Placement superseded in part by entries 30–31 (revision
    7)**: the sweeps are reordered and renamed S1–S3 and the single
    `:4593` exit is superseded by invocation at both `createQuery` exits
    (the main body and the dependent route).
25. Execution-cost model replaces the inference-only premise (revision 5,
    F-PERF-PREMISE): the rebound aggregate argument cast is a same-scale
    `(76,s)→(65,s)` decimal cast that bypasses the duplication fast path
    (`func_cast.go:1456–1468`, which requires `from.Width ≤ to.Width`)
    and formats/re-parses every non-NULL coefficient
    (`decimal256ToDecimal256`, `:6980–7030`). I8 documents the true cost
    model (structural-skip plans pay zero; provenance-carrying plans pay
    one statement-scoped walk plus the per-row casts), §9 states the 1.5×
    acceptance budget with the parent-versus-candidate benchmark family
    (the base successfully runs the fitting-value AVG control, giving a
    direct baseline), and a behavior-identical limb-comparison capacity
    kernel is an allowed semantics-neutral substitute if measurement
    demands it.
26. Downstream type propagation after reconciliation (revision 6,
    F-DOWNSTREAM-PROPAGATION): the planner copies types by value —
    aggregate/window outputs into separate projection expressions
    (`query_builder.go:2332–2344`/`:2948–2960`), referencing expressions
    (`projection_binder.go:80–100`), TIME_WINDOW deep copies
    (`having_binder.go:979–990`, executor `timewin.go:559–572`), and the
    final projection read by `GetResultColumnsFromPlan`
    (`build.go:999–1003`) — so rewriting an aggregate in place leaves
    stale copies and stale consumer contracts (`AVG(a*b)/3` would keep
    the scale-8 division bound against the pre-reconciliation `(65,4)`
    where the parent used scale 4, `arithmetic.go:668–671`). The
    reconciliation therefore runs three ordered sweeps inside the
    final-plan pass: **P1** child-to-parent reconcile (aggregate
    argument/result types, TIME_WINDOW deep copies included), **P2**
    re-sync of every copied Col Typ from its producer slot, **P3**
    post-order re-derivation of scalar and aggregate consumer types
    through each node's registered rule with the re-typed children as
    inputs. The memo stays valid across the sweeps because the §4.3
    evaluator reads only pass-invariant inputs (anchor seeds and operator
    rules), never a type the pass mutates; induction over P3 guarantees
    each re-derived consumer type equals the evaluator's envelope for
    that slot. The sweep is bounded to chains transitively consuming a
    reconciled output; M2 (result-set metadata) follows the repaired
    head projection with no separate mechanism. §9 pins the copied-type
    equality walk, the `AVG(a*b)/3` scale-4 witness, nested consumers,
    TIME_WINDOW copies and final result metadata. **Reordered and
    extended by entries 29–31 (revision 7)**: the P1/P2/P3 order is
    superseded by S1/S2/S3 — consumer re-derivation (old P3) must
    complete before any copy re-syncs (old P2), because a copy re-synced
    from a still-mutating downstream producer finalizes from mid-mutation
    state; S2 additionally rebuilds implicit coercion casts (entry 29);
    the copy enumeration extends to SINK/SINK_SCAN schemas across
    `SourceStep` edges and the dependent route's correlation `OutputTypes`
    and dependent-call result types, and the pass now runs at both
    `createQuery` exits (entries 30/31).
27. Uniform envelope-divergence aggregate admission (revision 6,
    F-ADMISSION-CONTRACT): registered-result agreement is not
    domain-admission equivalence — `avgDecimal256ReturnType` maps both
    `(76,0)` and `(66,0)` to `(65,4)`, so a mixed-origin argument
    (`COALESCE(d, a*b, <66-digit literal>)` selecting `+10^70`/`−10^70`)
    could cancel to zero unchecked under revision 5's gate (its
    mixed-origin matrix row demanded the cast while the gate suppressed
    it — an internal contradiction), and bare-product SUM admitted
    carrier-exact values the parent's product typing never produced. The
    trigger is now uniform: **every decimal aggregate argument whose
    §4.3 envelope differs from its published type receives the envelope
    cast** (domain admission, not capacity clamping), with no
    result-agreement gate; bare-product SUM casts `(76,0)`→`(65,0)`,
    out-of-domain values raise the parent's multiply-time admission
    family at the cast, and §9 gains the independent cancellation
    oracles (mixed-origin AVG, bare-product SUM) plus the SUM benchmark
    (a′). The width-sensitivity inventory stays as documentation and
    exhaustiveness check two, no longer a trigger input. **Narrowed and
    decomposed by entries 32–33 (revision 7)**: the trigger subject is
    now the domain-arithmetic class `{avg, sum}` exactly (the §4.2
    class (ii) aggregate membership, machine-pinned by check two) —
    value-selecting aggregates return input rows and admit nothing — and
    the single head envelope cast is decomposed per source: every
    divergent decimal seam inside the argument re-receives its own
    parent publication inner-first, closing the `(66,0)`-head `±10^65`
    hole this entry's uniform head cast left open. **Discovery made
    head-independent by entry 35 (revision 8)**: "wherever envelope ≠
    published" is evaluated per seam with no conditioning on the
    argument head's own envelope comparison — a `(76,0)`-envelope head
    no longer masks a divergent product seam beneath it.
28. Mixed-version execution admission (revision 6, F-MIXED-VERSION):
    the repaired scaled fallback changes executable kernel semantics —
    an old worker rejects the `(65,40)`-family rounded `10^70`
    coefficient (`arith_decimal_fast.go:2193–2218`) that the repaired
    kernel accepts, and the same pipeline stays decodable by old CNs, so
    unchanged wire/plan formats do not by themselves guarantee
    identical execution. Admission uses the repository's established
    remote-expression feature machinery: plan finalization sets a new
    `DecimalMultiplicationCarrierDomain` feature on any pipeline whose
    DECIMAL multiplication either publishes width > 65 or is statically
    fallback-reachable (`(s1+s2) − max(12,s1,s2) > 38`, the static form
    of the `scaleAdj < −38` dispatch — e.g. `(65,40)×(65,40)`); a
    narrow multiply sets nothing.
    The feature maps to the minted `defines.MORPCVersion135` (the next free
    floor after the refreshed base's 132/133/134 allocations, revision 38
    F-TARGET-PROTOCOL-COLLISION); a below-floor CN demotes the
    statement to one-CN placement (`expression_protocol.go:60–73`),
    destination validation raises the checked not-supported family
    (`remoterun.go:2440–2490`), and the `requiredProtocol` fence guards
    streams (`remoterun.go:150–157`) — the `ScalarDivision` and
    `DecimalDivisionSemantics` (v97) precedents
    (`string_literal_form.go:380–382`, `:936`, `:1015–1035`);
    `temporal_incomplete_protocol_test.go` is the test shape. The
    no-format-change claim stands: this is a compute-time capability,
    not a persisted, catalog, or wire-format change; the fence covers
    exactly the rolling-upgrade window and pinning below the floor
    demotes rather than diverges; every record the new binary persists
    is a declared ≤65 tag (I2/I4), so downgrade/rebind is the identity
    for old binaries. §9 I9 pins the analysis, floor-mapping and
    admission UTs, the downgrade/rebind converse, and lists the real
    mixed-version cluster run for CI.
29. Implicit coercion reconstruction (revision 7, F-COERCION): the
    binder materializes COALESCE/CASE/IF-family branch alignment and
    decimal argument alignment as ordinary overload-0 `cast` nodes whose
    `TargetType` it computed from the binding-moment branch types
    (`appendCastBeforeExpr`, `base_binder.go:8987–9050`; merge
    arithmetic `setSafeDecimalWidthAndScaleFromSource`,
    `type_check.go:1074–1123`; construction `newCheckResultWithCast`,
    `func_binary.go:1136–1158`), and implicit casts are exactly those
    lacking `SyntaxExplicitCast` (the binder's own test,
    `base_binder.go:13035–13041`, flag set at `:11894–11910`). Revision
    6 re-derived expression `Typ` fields but left those baked targets
    stale: the parent, binding the same SQL over the final branch types,
    computes a different alignment target (its maxInt-65/scale-4
    arithmetic over stale branch types yields the `(69,4)` family where
    the parent's final answer is `(65,0)`), so a reconciled plan could
    execute an alignment the parent never authored. S1/S2 therefore
    rebuild every implicit cast inside affected trees with targets
    recomputed by the same binder arithmetic over the final child types,
    and never touch SQL-authored casts (user semantics). §9 I6/I10 pin
    the grouped `AVG(COALESCE(d, a*b))` parent-verbatim `(65,0)` oracle
    and the explicit-cast preservation control. **Classifier superseded
    by entry 37 (revision 8)**: the flag predicate (overload-0 `cast`
    without `SyntaxExplicitCast`) does not delimit the replay set — it
    also matches this pass's own seam/head admission casts and the
    recursive member→anchor coercions, so the replay unit becomes the
    enclosing coercive binding decision, which regenerates exactly its
    owned alignment layer while decision-foreign fixed-target casts
    survive bitwise.
30. Dependency-first finalization order (revision 7, F-SYNC-ORDER):
    revision 6's order — reconcile (P1), then re-sync copies (P2), then
    re-derive consumers (P3) — lets a copy that reads a
    further-downstream producer (the final projection's copied type of
    `AVG(a*b)/3`, a SINK/SINK_SCAN projection schema
    `query_builder.go:3310–3313`/`:3231–3243` populated across
    `SourceStep` edges `:3247–3251`) finalize from a producer whose own
    re-derivation has not run: the copy would capture the stale
    pre-reconciliation type (the scale-8 division) and the later P3
    would repair the producer but not the copy. The order is now S1
    (reconcile and admit), S2 (re-derive every consumer post-order,
    replaying coercions), S3 (re-sync every copy, last, from final
    producer slots only). §9 I6/I10 pin the order with instrumented
    pass counters, the `AVG(1)/3` scale-4 value witness (`0.3333`,
    never a retained scale 8's `0.33333333`), and the SINK/SINK_SCAN
    copied-schema equality assertions. **Sweep structure superseded by
    entry 34 (revision 8)**: S3-last still re-derives consumers from
    stale copies — copies are inputs to consumer decisions, so the
    global sweeps are replaced by one dependency-ordered finalization
    that refreshes every copy at its producer slot's finalization,
    before any consumer of it re-derives.
31. Both `createQuery` exits reconciled (revision 7, F-DEPENDENT-EXIT):
    `createQuery` returns through the main body's end
    (`query_builder.go:4593`) and, for correlated statements, through
    the dependent route (`query_builder.go:4318–4320` →
    `createDependentQuery`, `correlation_binding.go:148`) — the route
    `TestDependentPublicPlan` exercises
    (`correlation_binding_test.go:57–73`). Revision 6 named only the
    main exit, leaving a correlated statement's final plan (with its
    correlation `OutputTypes` deep copies,
    `correlation_binding.go:208–210`,
    and the dependent scalar call's result `Typ`, `:215–217`)
    unreconciled. The reconciliation owner is invoked immediately before
    each return, and S3's copy surface includes the correlation copies.
    §9 I10 pins the dual-exit UT (identical reconciliation at either
    exit, correlation copies re-synced). **Copy surface extended by
    entry 38 (revision 8)**: the enumeration covered `OutputTypes` and
    the dependent-call result `Typ` but omitted the program `InputTypes`
    and callee `CorrelationSlot` types whose elementwise equalities
    `ValidateCorrelation` enforces; the channel finalizes in the
    producer order of `correlation_prepare.go:25–50`.
32. Admission class narrowed to the domain-arithmetic aggregates
    (revision 7, F-SELECTOR-AGGREGATES): revision 6's trigger — every
    decimal aggregate argument whose envelope diverges — cast the
    value-selecting aggregates too, but `min`/`max` return an input row
    by registration (`ReturnFirstArgType`, `list_agg.go:103`/`:120`),
    as do `any_value` (`:370`), `max_by` (`:717`),
    `first_value`/`last_value`/`nth_value` (window registrations,
    `list_window.go:209–214`, `:234–239`, `:260–265`)
    and `percentile_disc`
    (`ordered_percentile.go:151–156`): no formula consumes width, the
    returned value is a row the source produced and the consumer's
    domain applies downstream exactly as it does for a widened scalar
    publication — a domain cast would newly reject base-successful
    selections and re-round exact rows. The trigger subject is exactly
    the §4.2 class (ii) aggregate membership `{avg, sum}`, the same
    list check two machine-pins, so a future width-sensitive decimal
    aggregate must join the class (and this pass) or fail the UT.
    Width-insensitive decimal-result aggregates (`percentile_cont`,
    `median`, `quantile_exact_weighted` — fixed-shape results,
    `ordered_percentile.go:134`, `median2.go:34`,
    `quantile_exact_weighted.go:87`) stay out of the class for the same
    reason. §9 I6's plan-shape oracles pin no-cast on the selector
    families; the value-selecting row pins carrier-exact SELECT/window
    controls and CTAS admission. **Oracle re-founded by entry 39
    (revision 8)**: the class-(ii)-only exhaustiveness whitelist is
    replaced by the two-family classification — transparent-propagation
    consumers (the selector families) necessarily diverge across a
    seam pair and are accepted as family T, while the machine-pinned
    `{avg, sum}` subject is the family-D aggregate membership.
33. Admission decomposed per source (revision 7, F-PRODUCT-ADMISSION):
    revision 6's single head cast equated admission against the merged
    argument envelope with the parent's per-source product admission:
    `COALESCE(d (38,0), a*b, <66-digit literal>)` merges to `(66,0)`,
    which admits `±10^65` product values — but the parent's
    `(38,0)²` multiply published `(65,0)` and rejected `±10^65` at its
    precision check (`checkDecimal256ResultPrecision`,
    `arithmetic.go:121–150`, limit `10^65` for that width), so the
    revision-6 shape silently averaged or summed values the parent never
    produced (the `±10^70` oracle missed it: `±10^70` exceeds even the
    `(66,0)` head). Admission is now per source: every divergent decimal
    seam inside the argument receives its own envelope cast inner-first
    (the product arm re-receives `(65,0)`, rejecting `±10^65` and
    `±10^70` alike with the parent's multiply-time family — the cast
    admission runs the same decimal target-domain check,
    `func_cast.go:7019–7021`), inner envelopes then flow through the
    rebuilt coercions to the head, and the head casts only where it
    still diverges from the argument envelope. A genuine wide source
    (its published domain admitted by the parent — e.g. a 66-digit
    literal) carries an identity envelope and flows unchanged, so the
    mixed argument admits exactly the union of parent-admitted source
    domains, matching the parent's own behavior on each source. §9 I6
    pins the `±10^65` cancellation oracles, the `±10^70` controls, the
    genuine-literal control and the fitting `±10^31` control.
    **Extended by entries 35–36 (revision 8)**: seam discovery is
    head-independent — the argument head's own envelope agreement never
    suppresses a divergent seam's cast (the `COALESCE(a*b, <76-digit
    literal>)` masked case, entry 35) — and a seam resolving into a
    materialized relational producer is admitted consumer-side over the
    producer's statement-local companion projection, keeping sibling
    readers carrier-exact (entry 36). **Extended by entries 40–41, 43
    (revision 9)**: the companion now carries the seam's guarded cast at
    the producer's own demand (entry 40), placement splits by row shape
    with in-tree locus casts across row-changing interiors (entry 41),
    and the witnesses re-base on two occurrences with separated error
    and value statements (entry 43). **Subsumed by entries 44–47
    (revision 10)**: the companion projection is replaced by the
    producer-side envelope-shadow replica column (entries 44–45),
    replay is snapshot-first two-phase (entry 46), and the witnesses
    re-base on a profitable filtered producer (entry 47).
    **Refined by entry 48 (revision 11)**: per-source admission
    itself is unchanged; across a materialized boundary the seam set
    partitions by scope — a seam inside the referenced exported
    slot's own producing expression is admitted producer-side within
    that slot's shadow, and every other seam (another relation,
    another producer's export, a captured input) keeps its in-tree
    envelope cast at the consumer.
34. Dependency-ordered finalization (revision 8, F-INPUT-REFRESH):
    revision 7's S1/S2/S3 global sweeps treated copies as outputs to be
    repaired after all consumer re-derivation, but copies are INPUTS to
    consumer decisions: the division in `AVG(a*b)/3` reads the pull-up
    projection's value-copied Col (`query_builder.go:2332–2344`) and the
    division executor runs the retained result scale
    (`arithmetic.go:668–671`), so S2 computed the division from the
    stale `(65,4)` copy (scale 8, `0.33333333`) and S3 refreshed the
    copy afterwards with no further re-derivation. No global sweep order
    expresses "refresh before read" in both directions — consumers
    before copies repairs nothing, copies before consumers re-derives
    from mid-mutation producers. The sweep structure is withdrawn in
    favor of ONE dependency-ordered finalization over the plan's
    producer/consumer/copy DAG: nodes child-to-parent, producer steps
    before consumer steps, expression trees post-order; a producer slot
    finalizes exactly once and every referencing copy (pull-up and
    window projections `query_builder.go:2332–2344`/`:2948–2960`, binder
    references `projection_binder.go:80–100`, TIME_WINDOW copies
    `having_binder.go:979–990`, fill references, SINK/SINK_SCAN schemas
    `:3310–3313`/`:3231–3243` across `SourceStep` edges `:3247–3251`,
    and the correlation channel per entry 38) refreshes from it
    immediately as part of that finalization; a consumer re-derives
    through its registered rule only when every input it reads — direct
    children and referenced copies alike — is final, replaying its
    coercive decisions per entry 37. The plan graph is acyclic, so the
    order exists and terminates; the §4.3 memo stays valid because the
    evaluator reads only pass-invariant inputs. §9 I6/I10 pin the
    dependency-order counters, the once-only copy refresh, the
    `AVG(1)/3` scale-4 witness and the stale-copy freedom assertions.
35. Head-independent seam discovery (revision 8, F-HEAD-GATE): revision
    7 conditioned per-source seam discovery on the argument head's
    envelope differing from its published type, but a `COALESCE(a*b,
    <76-digit literal>)` argument over nullable `(38,0)` columns
    publishes `(76,0)` with envelope `(76,0)` (the product's `(65,0)`
    envelope combined with the genuine literal's `(76,0)`), so the gate
    found no divergence, inserted no cast inside the argument, and a
    canceling `+10^65`/`−10^65` product pair flowed into the aggregate
    although the parent's multiply rejected each value
    (`checkDecimal256ResultPrecision`, `arithmetic.go:121–150` — the
    same hole family as entry 33's, through a head the merged domain
    cannot expose). Seam discovery is now head-independent: every
    decimal-typed subexpression of every decimal AVG/SUM argument whose
    OWN §4.3 envelope differs from its OWN published type receives its
    envelope cast, inner-first, with the head treated as one
    subexpression among them (its admittance cast is exactly revision
    7's head cast when it diverges). §9 I6 pins the masked-head
    cancellation oracle and the genuine 76-digit literal control.
36. Materialized-boundary admission via companion projections (revision
    8, F-PRODUCER-BOUNDARY): per-source seam casts cannot physically
    live where revision 7 put them when the seam's subexpression
    resolves through a materialized relational producer: shared-CTE
    reuse materializes ONE producer with ONE merged output column read
    by N consumers through SINK/SINK_SCAN (`cte_reuse.go:41–64`), so a
    cast inside the producer narrows every sibling reader (a `MAX` of
    the same column would lose its carrier-exact row, violating entry
    32) while a cast at the consumer has no arm left to cast — the
    branch merge already happened inside the producer. The finalization
    pass therefore emits one statement-local companion projection per
    divergent seam admitted by an AVG/SUM reader — the seam
    subexpression's own uncast value, an ordinary projection column
    carrying the existing widened type (no new expression kind, no
    plan-format change) — leaves the original column carrier-exact for
    every other reader, lets the companion slots flow through the
    SINK/SINK_SCAN schemas as ordinary slots (entry 34's copy refresh
    covers them), and rebuilds the admitting consumer's argument by
    replaying the reference-resolved tree with each divergent seam
    replaced by its seam cast over the companion column reference,
    inner-first over nested seams (the rest of the replayed tree reads
    the original columns). Companion columns are created only when at
    least one AVG/SUM consumer admits a divergent seam in that producer
    — a MAX-only statement adds nothing — and they never appear in
    result columns or on the wire. §9 I6/I8/I10 pin the shared-producer
    AVG+MAX witnesses, the companion-free probe, the SINK/SINK_SCAN
    slot-refresh assertion and the (a‴) benchmark variant. **Extended by
    entries 40–41, 43 (revision 9)**: the companion is now the seam's
    guarded cast column — evaluated only under the arm-selection guards
    of the seam's short-circuit ancestors, so its evaluation demand and
    error surface equal the parent's producer-side multiply demand
    (entry 40); it is confined to row-preserving paths, with dedup-,
    grouped- and set-op-interior producers taking an in-tree seam-locus
    cast below the first row-changing operator instead (entry 41); and
    the §9 witnesses and the (a‴) benchmark move to genuine
    two-reference forms with a one-SINK, two-SINK_SCAN plan-shape
    assertion and separated error and value statements (entry 43).
    **Subsumed by entries 44–47 (revision 10)**: the guarded companion
    is replaced by ONE envelope-shadow replica column per admitted
    argument evaluated by the executor's own machinery (entry 44),
    built producer-side and exported as an ordinary slot (entry 45),
    replay is snapshot-first two-phase (entry 46), and the witnesses
    plus the (a‴) benchmark re-base on a profitable filtered producer
    with a decline control (entry 47).
    **Refined by entry 48 (revision 11)**: the per-admitted-argument
    granularity is replaced by one shadow per (producer, referenced
    exported slot): an argument spanning producer scopes is
    recomposed consumer-side from the slots' shadows plus its
    in-tree consumer-side seam casts rather than replicated
    wholesale inside one producer.
37. Decision-owned coercion replay (revision 8, F-CAST-OWNERSHIP):
    revision 7 delimited the replay set by a flag predicate (overload-0
    `cast` without `SyntaxExplicitCast`, `base_binder.go:13035–13041`),
    but that profile also matches the pass's own seam/head admission
    casts — ordinary plan-expression casts built through the same
    constructors with no annotation channel — so a replay keyed on it
    would have "recomputed" a `(76,0)`-era alignment target over a seam
    cast's final child and silently destroyed admission; it equally
    matches fixed-target casts born outside alignment decisions (the
    recursive member→anchor coercions `makePlan2CastExpr`,
    `query_builder.go:5922`, construction `make.go:1060–1086`, and the
    `appendCastBeforeExprWithOverload` alignments,
    `base_binder.go:12079–12096`), whose targets carry user or anchor
    semantics no re-derivation may touch. The replay unit is now the
    enclosing coercive binding DECISION: each decision's registered
    check (COALESCE/CASE/IF-family branch merges, decimal argument
    alignment — `setSafeDecimalWidthAndScaleFromSource`,
    `type_check.go:1074–1123`, via `newCheckResultWithCast`,
    `func_binary.go:1136–1158`) is pure in argument types and session
    state, so re-invoking it with the final child types regenerates
    exactly the alignment-cast layer it owns at its argument positions —
    parent-verbatim by construction — while decision-foreign casts
    (SQL-authored explicit, seam/head admission, member→anchor) are
    preserved bitwise with no plan-format annotation. §9 I6/I10 pin the
    stale-`(69,4)` regeneration oracle and the seam-target,
    anchor-target and explicit-cast survival controls. **Extended by
    entry 42 (revision 9)**: the re-invoked check runs over the
    decision's RECOVERED pre-coercion operands rather than the retained
    cast tree — recovery itself is ownership-by-regeneration, so no
    identity field or annotation is needed.
38. Dependent-expression envelopes and channel closure (revision 8,
    F-DEPENDENT-ENVELOPE): the §4.3 evaluator dispatched only
    `Expr_Lit`/`Expr_Col`/`Expr_F` (`build_ddl.go:1822–1958`), so a
    dependent scalar call carrying a widened product and every
    correlation slot were opaque to divergence detection: a correlated
    `AVG((SELECT a*b ...))` kept the widened-derived `(65,4)` result
    domain, newly rejecting a fitting `10^62` average the parent
    returns. Revision 7's copy enumeration also covered `OutputTypes`
    (`correlation_binding.go:208–210`) and the dependent-call result
    `Typ` (`:215–217`) but omitted the program `InputTypes` and callee
    `CorrelationSlot` types whose equalities `ValidateCorrelation`
    enforces elementwise (`correlation.go:216` slot `Typ` equals
    `InputTypes[ord]`; `:242` dependent-call inputs equal `InputTypes`).
    The evaluator gains two rules: an `Expr_Dependent` envelope resolves
    through the callee program's root output envelope, and an
    `Expr_Slot` envelope resolves through the caller-captured expression
    at the slot's ordinal. Finalization closes the channel in the
    producer order the executor already owns
    (`refreshPreparedCorrelationExpr`, `correlation_prepare.go:25–50`):
    the caller's capture expressions finalize, the program `InputTypes`
    refresh, the callee slot types refresh, the callee program finalizes
    internally by the same protocol (its dependent scalar product
    carries the §4.1 dependent-rule envelope through its seam), the
    `OutputTypes` refresh, the caller's dependent-call result re-derives,
    and the caller's consumers proceed. `ValidateCorrelation` holds by
    construction and is asserted by §9 I10's dual-exit/channel UT.
39. Two-family exhaustiveness oracle (revision 8,
    F-TRANSPARENT-CONSUMERS): revision 7's check two whitelisted only
    the domain-arithmetic class (ii), but transparent-propagation
    consumers NECESSARILY diverge across an envelope/published pair: the
    `ReturnFirstArgType` registrations (`list_agg.go:103`/`:120`, body
    `:876–877`; `any_value` `:370`; `max_by` `:717`; the window families
    `list_window.go:209–214`/`:234–239`/`:260–265`;
    `ordered_percentile.go:151–156`) return `typs[0]` verbatim, so their
    result differs across a `(65,0)`/`(76,0)` seam exactly as their
    input does — the whitelist would have failed the design's own
    selector exemptions (entry 32). Check two now classifies every
    divergent decimal consumer as family T — transparent propagation, a
    §4.1 copy/merge rule over value-producing input types (scalar
    selectors, value-arg aggregates, window first/last/nth_value,
    `percentile_disc`, `lag`/`lead`, casts) — or family D — domain
    arithmetic, §4.2 class (ii), the §5 pass's subject and admission
    surface — and a divergence naming neither family fails the UT
    deterministically. The machine-pinned `{avg, sum}` subject is the
    family-D aggregate membership. §9's consumer-inventory row pins the
    classification.
40. Demand-guarded companion capture (revision 9, F-COMPANION-DEMAND):
    revision 8's companion projected the seam's uncast value as an
    unconditional producer column, but the value it captured can sit
    under short-circuit operators: the executor evaluates COALESCE arms
    on a shrinking row mask — each arm only where every earlier arm
    yielded NULL (`EvalCoalesce`,
    `pkg/sql/colexec/evalExpression.go:1628–1658`; CASE/IF forms
    `:1600–1616`; pinned by `TestFlowControlShortCircuitInvalidCast`,
    `evalExpression_test.go:2994`) — so an unconditional companion
    evaluates the seam on rows the base engine never touches (an
    unselected arm's physical overflow newly aborts a statement the
    parent executes successfully), while applying the seam cast
    consumer-side checks only the consumer's demanded rows, narrower
    than the parent's producer-side multiply admission for a shared
    producer. The CTE-sharing code itself enforces exactly this demand
    law (`cteOutputDemandPreservesEvaluation`, `cte_reuse.go:412–443`:
    a deterministic-but-fallible output must not be evaluated on an
    expanded row domain). The companion is therefore the seam's own
    envelope cast wrapped in the arm-selection guard chain of the
    seam's short-circuit ancestors — an ordinary guarded CASE whose
    guard conjoins the selection conditions (COALESCE: all earlier arms
    NULL; CASE/IF: the branch predicate with earlier ones negated),
    reusing the executor's own masks — evaluated once at the producer:
    nothing computes on masked rows (demand-equivalent to the base by
    construction), a violating value on a SELECTED arm raises the
    parent's multiply-time admission family at the parent's producer
    demand, and the companion is NULL on unselected rows, so the
    consumer's rebuilt merge still reads the original columns for the
    other arms. §9 I6/I10 pin the unselected-overflow success control,
    the selected-overflow checked rejection and the
    `TestFlowControlShortCircuitInvalidCast`-shaped mask probe.
    **Subsumed by entry 44 (revision 10, F-GUARD-SEMANTICS)**: the
    conjunctive guard chain does not reproduce the executor's demand —
    a NULL `WHEN` predicate keeps the row pending, eager AND and
    skipped COALESCE intermediate arms defeat guard arithmetic — so the
    guarded companion is replaced by ONE structural envelope-shadow
    replica column per admitted argument whose evaluation IS the
    executor's own machinery.
    **Refined by entry 48 (revision 11)**: demand equivalence still
    comes from evaluating a structural replica through the executor's
    own machinery, but the replica unit is now the referenced
    exported slot's producing expression; consumer-side seams keep
    their pre-existing in-tree casts exactly where the base engine
    evaluates them, so the partitioned construction still evaluates
    nothing the base engine does not.
41. Row-shape placement taxonomy (revision 9,
    F-ROW-CHANGING-BOUNDARY): a companion column is one value per
    producer ROW, so it can travel only where row correspondence is
    preserved — projection/filter chains and SINK/SINK_SCAN
    materialization (the finalization's copy refresh carries its slots;
    emitted on the post-prune, post-remap final plan, it references the
    producer's final slots directly and nothing prunes or renumbers
    afterwards; positional output resolution reads the final
    projection/group/agg layouts, `query_builder.go:2310–2344`,
    `build_ddl.go:2095–2107`). Across an interior row-changing operator
    a per-row companion cannot meaningfully exist: a dedup step would
    fold the companion into the dedup key (splitting value-equal rows
    whose companions differ), and grouped/window aggregation and set
    operations emit rows with no source-row correspondence at all. For
    any seam-to-reader path crossing such an operator inside the
    producer, the design places no companion: the seam takes its
    ordinary in-tree envelope cast at its own locus below the FIRST
    row-changing operator, inside the guarded arm subtree — demand-exact
    (it inherits the arm's own mask), value-identical on the parent's
    admitted domain, statement-rejecting exactly where the parent's
    multiply rejected — gated, like the companion, on an admitting
    AVG/SUM reader downstream (the grouped form's shape is
    `MIN(CAST(a*b→(65,0)))` per group). §9 I6/I10 pin the dedup,
    grouped and set-op variants' locus-cast probes. **Subsumed by
    entry 45 (revision 10, F-PRUNED-PROJECTION-DEPS)**: the row-shape
    taxonomy stands, but the row-preserving construction is the
    producer-built envelope-shadow column exported as an ordinary
    SINK/SINK_SCAN slot — a consumer-side replay would read columns
    pruned out of the producer's export.
42. Recovered-operand decision replay (revision 9,
    F-REPLAY-RECOVERY): re-invoking a decision's registered check needs
    the decision's OWN operand list, but `plan.Function` carries no
    decision identity (`plan.pb.go:3180–3190`), and re-invoking over
    the RETAINED cast tree is simply wrong: a retained `(65,4)`
    alignment cast fed back into `setSafeDecimalWidthAndScaleFromSource`
    (`type_check.go:1074–1123`) merges to `(69,4)` where a fresh rebind
    of the original operands produces the parent's `(65,0)`. The binder
    inserts at most ONE alignment cast per argument position per
    decision (`base_binder.go:8987–9054`), so recovery is decidable
    without any annotation channel: (i) casts this pass inserted are
    skipped via a pass-local identity set; (ii) the single candidate
    alignment cast at a decision's argument position is unwrapped and
    ACCEPTED only when the re-invoked check over the unwrapped operand
    regenerates it exactly — regeneration IS the ownership test, so
    foreign fixed-target casts (member→anchor coercions, SQL-authored
    casts) are restored and survive bitwise; (iii) a decision position
    that constant-folded is rebound from the literal's `Src` recovery
    channel (`plan.pb.go:1880`), which the binder sets precisely for
    implicit overload-0 casts (`base_binder.go:4865–4875`) as the
    fold's own provenance record. The check then re-runs over the
    recovered operands at their final envelope types and the pass
    rebuilds only positions whose regenerated layer differs. §9 I6/I10
    assert the recovered-operand rebind equals the parent's fresh
    rebind `(65,0)` and that the retained-cast re-check would have
    produced `(69,4)`. **Subsumed by entry 46 (revision 10,
    F-POST-FOLD-RECOVERY)**: regeneration-equality over FINAL operands
    inverts on legitimately changed targets, and `Literal.Src` is
    populated only by the prepared-peer FLOAT/string provenance
    machinery; recovery becomes snapshot-first two-phase with the
    explicit fold table (entry 46).
    **Refined by entry 49 (revision 11)**: the fold table is
    withdrawn — a folded position's original operand domain now
    comes from the bind-time decision ledger recorded before folding
    can destroy it; the snapshot-first two-phase protocol otherwise
    stands.
43. Genuine materialization witnesses (revision 9,
    F-MATERIALIZATION-WITNESS): `reusableCTEProducer` declines sharing
    unless the CTE name has at least two occurrences (`cte_reuse.go:73`;
    occurrences appended per table binding,
    `query_builder.go:5670–5686`), so revision 8's flagship
    materialized-boundary witnesses — `SELECT AVG(v), MAX(v) FROM c`
    and `SELECT v/3 FROM c`, each binding `c` once — never materialized
    at all: they inlined, exercised the flattened (companion-free) path
    and proved nothing about companions; and one witness statement that
    rejects `±10^65` cannot simultaneously demonstrate the sibling
    MAX's exact row. §9 now pins the genuine two-reference form `WITH c
    AS (SELECT COALESCE(a*b, <66-digit literal>) AS v FROM t) SELECT
    (SELECT AVG(v) FROM c) av, (SELECT MAX(v) FROM c) mx` with the
    plan-shape assertion of exactly one SINK producer and two
    SINK_SCAN readers, the finalization witness `SELECT (SELECT v/3
    FROM c) q, (SELECT v FROM c) w`, separated error and value
    statements, and the (a‴) benchmark re-based on the same
    two-reference shape. **Subsumed by entry 47 (revision 10,
    F-PROFITABLE-MATERIALIZATION)**: two occurrences past the
    occurrence gate still INLINE on an unfiltered N-row producer
    (inline 2N < shared 4N), so those witnesses exercised the
    flattened path after all; they re-base on the filtered
    `WHERE k % 4 = 0` producer with an unfiltered decline control.
44. Envelope-shadow admission, not guard arithmetic (revision 10,
    F-GUARD-SEMANTICS): revision 9's conjunctive companion guard chain
    still mis-evaluated the executor's demand and three-valued truth —
    `EvalCase` carries a row whose `WHEN` predicate is NULL in
    `remaining` rather than dispatching it
    (`evalExpression.go:1605–1614`), eager AND can evaluate a product
    an earlier failed predicate would mask (`:1886–1892`), and
    `COALESCE(x, a*b, c*d)` with non-NULL `x` skips BOTH product arms
    where guard arithmetic covering the third arm evaluates `a*b` —
    each raising the kernel overflow on a statement the base engine
    completes. No guard expression over the seam reproduces those
    masks, so the row-preserving construction is ONE statement-local
    **envelope-shadow column** per (producer, admitted argument): a
    structural replica of the whole argument expression with the seam
    casts applied in place and the decision's coercions replayed over
    snapshot-verified recovered operands. The executor's own
    `EvalCoalesce`/`EvalCase` machinery evaluates the replica, so
    demand, NULL propagation and error timing equal the
    unmaterialized plan's BY CONSTRUCTION, and ONE shadow serves every
    divergent seam of ONE argument. §9 I6/I10 pin the NULL-predicate,
    skipped-arm and unselected-overflow demand controls.
    **Refined by entry 48 (revision 11, F-SCOPE-SPANNING-ARGUMENTS)**:
    the per-(producer, admitted argument) unit is replaced by
    per-(producer, referenced exported slot): the shadow replicates
    that slot's own producing expression — the only unit every
    producer-side input is guaranteed in scope for — while the
    no-guard principle is unchanged (one structural replica evaluated
    by the executor's own machinery). Revision 10's whole-argument
    claim fails for `AVG(COALESCE(c.v, u.d))`, whose `u.d` input
    exists only consumer-side; "ONE shadow serves every divergent
    seam of ONE argument" narrows to "one shadow serves every
    producer-representable divergent seam of ONE slot", with the
    consumer recomposing its argument over the slots and casting
    consumer-side seams in tree.
45. Producer-side shadow construction and slot rebinding (revision
    10, F-PRUNED-PROJECTION-DEPS): revision 9 built the companion
    consumer-side by replaying the argument, but a materialized
    producer exports only its projection — the argument's own inputs
    (pre-aggregation columns pruned from the export,
    `cte_reuse.go:1877–1897`, `query_builder.go:3226–3250`) are out of
    consumer scope. The shadow is therefore built INSIDE the
    producer's finalization, where the argument tree and every input
    remain in scope and projection pruning preserves admitted
    columns, and it crosses the boundary as an ordinary exported
    SINK/SINK_SCAN projection slot; the consumer's argument is
    rebound as a direct slot reference, so no replay dependency
    crosses the boundary. The row-shape taxonomy (entry 41) stands:
    dedup-, grouped- and set-op-interior producers take the in-tree
    seam-locus cast below the first row-changing operator with no
    shadow column.
    **Refined by entry 48 (revision 11)**: the construction locus is
    unchanged (inside producer finalization, exported as an ordinary
    SINK/SINK_SCAN slot), but the shadow's CONTENT is now the slot's
    own producing expression — which BY CONSTRUCTION needs no input
    outside the producer — and the consumer argument rebinds to the
    shadow slot for that reference while the rest of the argument
    stays in tree with its coercions replayed from the ledger, so no
    replay dependency crosses the boundary for any scope shape.
46. Snapshot-first two-phase replay with the fold table (revision
    10, F-POST-FOLD-RECOVERY): revision 9 verified alignment-cast
    ownership by regenerating over FINAL operands and comparing — but
    a legitimately changed target (`COALESCE(avg_out, d)`: `avg_out`
    snapshotted `(65,4)`, finalized `(65,0)` over `d` `(61,0)`) makes
    the final regeneration differ from the decision's OWN retained
    layer, inverting the ownership verdict; and folded positions
    rebounded from `Literal.Src`, whose population is gated on the
    prepared-peer FLOAT/string provenance machinery
    (`base_binder.go:4861–4875`) — ordinary constant folds
    (`rule/constant_fold.go:320–388`, `:661–662`,
    `utils.go:2099–2132`) create decimal literals and typed NULLs
    with no `Src`. Replay is therefore snapshot-first: (i) the
    decision's pre-coercion operands are SNAPSHOT at pass entry;
    (ii) ownership is verified by re-deriving the ORIGINAL alignment
    layer over the snapshot operands at their snapshot types and
    comparing bitwise against the retained layer — changed targets
    still verify owned; (iii) the check then regenerates the FINAL
    layer at the final envelope types. Folded positions recover
    through an explicit table: (a) an ordinary valued decimal
    literal is re-typed from its own digits by the pad-only lemma
    (`bindNumVal`, `base_binder.go:11526`; `type_check.go:1074–1123`);
    (b) a typed NULL recovers as the untyped NULL the fold consumed;
    (c) a vector fold (`Expr_Vec`) recovers through the element
    type; (d) anything outside the table is decision-foreign at that
    position, preserved bitwise, with §9's determinism probe
    asserting (a)–(c) cover every fold reachable in scope.
    `Literal.Src` is WITHDRAWN.
    **Refined by entry 49 (revision 11, F-FOLD-DESTROYS-DOMAINS)**:
    the snapshot-first two-phase protocol (i)–(iii) stands unchanged,
    but folded-position recovery through the explicit table is
    REPLACED by the bind-time decision ledger, because the table's
    premise fails: the merge check consumes DECLARED width and scale
    (`type_check.go:1074–1123`), so `CAST(1 AS DECIMAL(64,0))` and
    `CAST(1 AS DECIMAL(65,1))` align identically to `(68,4)` and fold
    to the same artifact although their fresh targets after `avg_out`
    finalizes to `(65,0)` differ — `(65,0)` versus `(66,1)` — and
    folding discards the distinction before any pass-entry read
    (`rule/constant_fold.go:262–264`, `:323–327`, `:530–574`). The
    pad-only lemma, untyped-NULL and element-type channels are
    withdrawn; a ledger miss degrades to decision-foreign
    preservation plus the §9 coverage probe, exactly the table's (d)
    role.
47. Profitable materialization witnesses (revision 10,
    F-PROFITABLE-MATERIALIZATION): passing fact 18's occurrence gate
    is not sufficient — `reusableCTEProducer` shares only when the
    shared plan beats inlining (`cte_reuse.go:826–837`, safety factor
    1 for two occurrences, `:812–813`), and an unfiltered producer
    costs `outcnt = N` (`stats.go:2266–2268`, `:1812–1816`), so
    revision 9's two-reference witnesses inlined at 2N against a
    N + 3N shared cost and silently proved nothing (only three or
    more occurrences spill an unfiltered producer, `:150–151`,
    `:104–113`; the demand veto at `:412–443` can decline
    regardless). The materialization witnesses, the finalization
    witness and the (a‴) benchmark re-base on a deterministically
    profitable filtered producer `WHERE k % 4 = 0` — selectivity 1/4
    sits below the cost model's one-half break-even, so the shared
    plan wins — with the same one-SINK/two-SINK_SCAN plan-shape
    assertions, the same separated error and value statements, and
    an unfiltered DECLINE control asserting both references inline;
    the public contract's single-reference AVG/MAX simultaneity
    claim is replaced by the separated reachable two-reference
    forms.
48. Scope-decomposed shadows with consumer composition (revision 11,
    F-SCOPE-SPANNING-ARGUMENTS): revision 10 replicated the
    argument's COMPLETE original expression inside the producer and
    replaced the consumer argument wholesale with one exported slot
    — but that presumes every argument input exists inside the
    producer, which is false for `AVG(COALESCE(c.v, u.d))` over a
    shared CTE `c` joined to `u`: `c` produces `v` while `u.d`
    exists only at the consumer join (eligible pinned-build consumer
    joins are supported, `cte_reuse.go:1208–1246`, `:1684–1695`;
    SINK_SCAN exposes only the producer's own outputs, `:1877–1897`).
    The shadow unit is therefore decomposed by scope: ONE
    statement-local envelope-shadow column per (producer, REFERENCED
    EXPORTED SLOT) whose producing expression carries a divergent
    decimal seam — a structural replica of THAT SLOT'S OWN producing
    expression with the producer-representable seam casts in place
    and its coercions replayed from the decision ledger (entry 49).
    Every input of a slot's producing expression is producer-side BY
    CONSTRUCTION (the producer computed the slot from those inputs),
    so no guard arithmetic is reintroduced and projection pruning
    preserves the shadow exactly as it preserved the slot. A
    divergent seam that reads another relation, another producer's
    export or a captured input is CONSUMER-SIDE and keeps the in-tree
    construction: row-preserving paths take the envelope cast at the
    seam's own position; across a row-changing operator the
    taxonomy-(B) locus cast below the first row-changing operator
    stands. The consumer RECOMPOSES its argument: each reference to
    a shadowed slot rebinds to that slot's shadow, and the remainder
    of the argument — consumer-side seams included — stays in tree
    with its coercive decisions replayed from the ledger. The
    executor's own machinery evaluates the recomposed argument on
    exactly the base engine's demand: producer-side seams were
    evaluated there by the parent (entry 44's per-slot equality
    holds), consumer-side seams are evaluated where the parent
    evaluated them, and nothing is evaluated twice or on masked-out
    rows. §9 I6/I8/I10 pin the materialized-CTE join witness —
    `WITH c AS (SELECT COALESCE(a*b, <66-digit literal>) AS v, k
    FROM t WHERE k % 4 = 0) SELECT (SELECT AVG(COALESCE(c.v, u.d))
    FROM c JOIN u ON c.k = u.k) av, (SELECT MAX(v) FROM c) mx` —
    one SINK, two SINK_SCAN readers, exactly one shadow slot, every
    reference resolving to an exported column, AVG parent-verbatim
    through the recomposed `COALESCE(shadow_v, u.d)` argument, the
    independent MAX reader carrier-exact, and the separated ±10^65
    error statement rejected at the shadow seam.
49. Bind-time decision ledger (revision 11, F-FOLD-DESTROYS-DOMAINS):
    revision 10's snapshot records operands at pass entry, but
    constant folding already ran during binding: a coercive
    decision's operand merges with DECLARED width and scale
    (`setSafeDecimalWidthAndScaleFromSource`,
    `type_check.go:1074–1123`), and distinct source domains can
    merge identically and fold to the same artifact —
    `CAST(1 AS DECIMAL(64,0))` and `CAST(1 AS DECIMAL(65,1))` in
    `COALESCE(avg_out, x)` both align to `(68,4)` over `avg_out`
    `(65,4)` and fold to the same Decimal256 value and type, yet
    after `avg_out` finalizes to `(65,0)` their fresh targets DIFFER
    — `(65,0)` versus `(66,1)`. Folding recursively replaces the
    explicit casts and their alignment parents with scalar vectors
    (`rule/constant_fold.go:262–264`, `:323–327`, `:530–574`), so
    neither the final tree nor any pass-entry snapshot retains the
    distinction, and a typed NULL's source expression is discarded
    outright. The pass therefore records a statement-local DECISION
    LEDGER at the alignment site (`base_binder.go:8987–9054`): keyed
    by (decision node identity, argument position), each entry
    stores the operand's pre-coercion `plan.Type` AS MERGED at
    binding. The ledger survives folding because it is keyed by node
    identity, not tree position: phase (ii)'s ownership verification
    is fold-insensitive (a fold materialized the alignment output, so
    the verified equality is the stored one), and phase (iii)
    finalizes an unfolded position by the ordinary §4.3 final
    envelope while a folded position identity-finalizes the LEDGER
    domain (fold outside an admitted decision is declined for
    widened-multiplication trees, so the folded position the pass
    admits is always the alignment output). A ledger miss means the
    position never passed an admitted widened-multiplication decision
    — decision-foreign, preserved bitwise — with §9's coverage probe
    asserting the admitted set covers every fold in scope, so a
    missed reachable fold fails loudly instead of degrading. The
    fold table and the pad-only lemma are WITHDRAWN; snapshot-first
    two-phase replay (entry 46) stands; `Literal.Src` stays
    withdrawn. §9 I6/I10 pin the paired folded-constant controls —
    `(64,0)` versus `(65,1)` finalizing to `(65,0)` versus `(66,1)`,
    asserted against a FRESH parent bind — and the paired typed-NULL
    control (`CAST(NULL AS DECIMAL(65,1))` versus untyped NULL
    recovers the genuine scale), comparing values, final types and
    persisted metadata rather than fold-table membership. Its
    node-identity keying is SUPERSEDED by entry 52's content keying
    (revision 12, F-LEDGER-LINEAGE — the record-at-binding contract
    stands).
50. Shadow-expanded admission reservation (revision 11,
    F-SHADOW-ADMISSION-ACCOUNTING): revision 10 admitted
    materialization from the ORIGINAL output schema and appended
    shadow columns during finalization, so the reservation's byte
    estimates, spill framing and cumulative process limits
    (`cte_reuse.go:152–179`, `:712–768`, `:771–808`) undercharged by
    the shadow bytes — near a cap the selected plan can then fail
    the runtime reservation
    (`sql/internal/materialized/source.go:491–518`) or exceed the
    charged budget. The reservation is now computed from the
    SHADOW-EXPANDED schema BEFORE the share commits: the producer's
    storage types plus ONE fixed-width Decimal256 payload (32 bytes)
    per exported slot whose producing expression carries
    widened-multiplication provenance — an occurrence-independent
    conservative bound, since a slot shadows at most once per
    statement no matter how many consumers admit it. The same
    estimators, per-column spill framing and cumulative counters run
    over the expanded schema; admission is all-or-nothing — a
    declined reservation leaves every counter byte-identical and the
    occurrences inline, so inline execution charges nothing and no
    cleanup path restores phantom bytes. The runtime checkpoint then
    always compares against the SAME schema the admission charged:
    the finalization's appends stay strictly inside it and its
    emission can never exceed it. §9 I6/I8/I10 pin the accounting
    oracles: the one-shadow reservation byte delta, multiple shadows
    charged per SLOT never per consumer, multiple producers summing
    cumulatively, a declined reservation leaving the counters
    byte-identical with the inline fallback running, and the
    no-admitting-reader control (no shadow emitted; the emitted
    schema never exceeds the charged schema).
51. Value-merge common-domain reconvergence (revision 12,
    F-COMMON-DOMAIN-OVERFLOW): the widened publication changes the
    operand types the value-merge families' common-domain computation
    consumes, so a merge the parent computed successfully can now
    overflow the carrier: `setSafeDecimalWidthAndScaleFromSource`
    (`type_check.go:1074–1123`) requires
    `requiredWidth = maxIntegralWidth + maxScale ≤ 76`
    (`decimalTypeForRequiredWidth`, `:1125–1151`; carrier width
    `types.go:869–871`), and `(76,0)` merged with `(65,1)` needs 77
    where the parent's `(65,0)` needed 66. The failure surfaces differ
    per caller: COALESCE/IFNULL reject the statement at binding
    (`func_binary.go:883`, `:1136–1157`), CASE and IFF exhaust their
    support loops into the same bind failure (`operatorSet.go:559` →
    `:571–572`, `:896`), and LEAST/GREATEST silently degrade the
    result to FLOAT64 (`func_builtin_leastgreatest.go:376`, `:681`) —
    each a regression on previously successful statements, invisible
    to the §5 finalization because binding failed first. On the
    merge's failure, and only where an operand subtree carries
    widened-multiplication provenance, the call site retries the
    identical merge over each operand's PARENT-published type (the
    §4.1 mirrors evaluated pure in types; multiplication contributes
    its parent's per-carrier-family publication with its result scale —
    `min(dw1+dw2, 38)` on the narrow route, `min(dw1+dw2, 65)` on the
    decimal256 carrier, revision 38 F-NARROW-CARRIER-ENVELOPE). The retry's outcome is
    the parent's by construction: success authors the parent's own
    target (e.g. `COALESCE((38,0)×(38,0), CAST(1 AS DECIMAL(65,1)))`
    binds `(66,1)`) with the binder's ordinary parent-domain alignment
    casts, and an operand value beyond the parent target's capacity
    (a 76-integral-digit product value against that `(66,1)` merge)
    raises the parent's own checked cast family at execution — the
    parent-family equivalent of rejecting the statement, since the
    parent rejected that value at the multiply itself; failure takes
    the family's own branch (bind failure or FLOAT64 fallback), which
    is also what the parent produces over genuinely >76-merged
    genuine-wide operand types, so no-provenance failures never retry.
    Every narrow-provenance statement merges at first attempt and
    pays nothing. Comparisons (`list_operator.go:272`) already carry
    the base engine's saturated fallback — on failure they keep
    `T_decimal256` width with the largest representable scale and let
    the checked cast reject only values that actually overflow — so
    they need no rule and §9 pins the `a*b = d` equality control.
    §9 also pins parent acceptance, value and metadata oracles for
    mixed-scale COALESCE, CASE/IFF and LEAST/GREATEST reconvergence.
    **Placement SUPERSEDED by entry 56 (revision 13,
    F-RECONVERGENCE-CHANNEL)**: the retry's outcome contract, the
    worked examples, the no-retry discipline and the comparison
    exclusion stand, but the retry cannot live at the merge check
    call sites — a checker's contract is a pure `[]types.Type`
    (`function/function.go:1161`) and a reference operand's producing
    expression is not reconstructible from its surface type — so it
    moves into the binder's source-domain-channel funnel
    (`base_binder.go`), and the four family files leave the planned
    production set.
52. Content-keyed decision ledger with pass-entry capture (revision
    12, F-LEDGER-LINEAGE): entry 49 keyed ledger entries by
    (decision-node identity, argument position), but identity does
    not reach the trees the replay must serve: projection inlining
    deep-copies decision subtrees with fresh `Expr`/`Function`
    objects (`opt_misc.go:469–479`, `deepcopy.go:1179–1184`,
    `:1284–1296`), and the §5 shadow replicas are newly constructed
    copies — an identity key is unresolvable for both. Entries are
    now keyed by CONTENT: function overload identity, argument
    count, and per position the pre-coercion operand type exactly as
    the merge consumed it together with that position's post-merge
    aligned type. `DeepCopyExpr` preserves every type and payload the
    key reads, so a copy — semantically the same decision at the same
    operand domains — resolves its origin's entry and replays
    identically, and a shadow replica resolves each replicated
    decision's entry by its own observable signature; I8 forbids the
    only alternative (a persisted decision-identity annotation; A1's
    tag-propagation rejection stands). At pass entry the capture
    walks each in-scope decision, computes its observable signature —
    a retained alignment cast unwraps to its operand's observable
    type, a folded position contributes its artifact's type (the
    entry's recorded domain is adopted at regeneration, never
    observed) — and binds the unique matching entry to a pass-local
    handle the rest of the pass uses. A miss degrades to
    decision-foreign preservation with the coverage probe asserting
    totality; a COLLISION — two entries matching one observable
    signature while differing in a folded position's recorded domain
    — cannot be resolved soundly: the distinguishing record is
    exactly what folding destroyed, preservation would silently
    retain the stale artifacts and diverge from the parent's values
    and metadata, and picking either entry diverges for the other
    twin; the pass raises a checked planning failure and the §9
    probe asserts the ambiguity set empty. The collision needs a
    deliberately constructed same-value cross-domain folded twin
    pair inside admitted arguments; its parent-successful rejection
    is the recorded loud corner (§10), never silent divergence.
    **Ambiguity semantics SUPERSEDED by entry 55 (revision 13,
    F-LEDGER-COLLISION)**: rejecting the parent-successful twin
    statement was itself a compatibility-contract violation, so the
    guarantee moved from corpus-probed absence to operand-syntax
    preservation — the decision barrier keeps the distinguishing casts
    available to capture — and the checked planning failure now
    classifies only ungated-fold mechanism bugs, still probe-asserted
    unreachable.
53. Positional ownership for folded positions (revision 12,
    F-FOLDED-LAYER-WITNESS): revision 11 claimed phase (ii)
    verification was 'fold-insensitive' — re-invoking the check
    over snapshot operands regenerates the retained layer 'whether
    or not a position folded' — but at a folded position there is
    no retained layer: folding replaced the alignment subtree with
    a scalar artifact (`rule/constant_fold.go:319–330`, `:390–402`),
    so the bitwise comparison against the retained layer is
    unsatisfiable exactly where revision 11 relied on it. Ownership
    is now verified per position: an unfolded position keeps the
    bitwise structural test; a folded position is owned when its
    artifact's type equals the entry's recorded post-merge aligned
    type — the fold preserves the folded node's type, and only a
    decision-owned alignment fold materializes with that recorded
    target (SQL cannot author a >65 cast target, pass-inserted
    admission casts carry ≤65 envelope targets and are skipped by
    the identity set, and I7's entry-level decline keeps
    provenance-carrying trees unfolded), so the positional witness
    cannot false-positive. Regeneration re-normalizes the folded
    value into the recorded pre-coercion domain — the alignment
    cast is an exact fixed-point scale lift that only appends
    zeros, so the artifact's coefficient re-bases into the
    operand's own declared domain loss-free (`1.0000` at `(68,4)`
    re-bases to `1` at `(64,0)` or `1.0` at `(65,1)`) — the
    regenerated alignment layer builds over it and folds again by
    the ordinary rules to the parent's fresh artifact (`1` at
    `(65,0)` versus `1.0` at `(66,1)`), exactly the parent's fresh
    rebind, and the §9 paired folded-constant and typed-NULL
    controls keep discriminating `(65,0)` from `(66,1)` against
    fresh parent binds.
54. Fold-entry provenance decline (revision 12,
    F-NULL-FOLD-PROVENANCE): revision 11's decline lived inside
    `MaterializeDecimal256Constant` (`rule/constant_fold.go:530`),
    reached only after `GetConstantValue` produced a foldable
    constant — but `GetConstantValue` returns the `Isnull` literal
    directly at `:661–662` with no decimal256 dispatch, so a
    NULL-producing widened tree (`CAST(NULL AS DECIMAL(38,0)) ×
    CAST(NULL AS DECIMAL(38,0))` typed `(76,0)`) folded to NULL
    keeping the widened `(76,0)` type: the fold destroyed the
    expression syntax the boundary's provenance recognition
    reads, while the parent's statement — the same tree at its
    `(65,0)` publication — persisted `(65,0)`. The decline now
    guards the fold walk's recursive ENTRY (`foldExpr`, `:138`):
    a tree carrying widened-multiplication provenance keeps its
    executable expression shape across every destructive fold
    shape — valued scalar-vector materialization, the `Isnull`
    literal return, and the enveloping wrapper folds containing
    them — while constant trees without provenance fold
    unchanged. The inner `MaterializeDecimal256Constant` check
    stays as defense in depth; untyped runtime evaluation is
    unchanged (the tree still constant-evaluates to the same
    NULL or value at execution). §9 pins plain and prepared CTAS
    and view controls for NULL products and NULL-producing
    wrappers against the parent's persisted records, and the
    genuine typed-NULL control (no multiplication provenance)
    keeps folding unchanged.
55. Statement-scoped decision barrier at both fold engines' entries
    (revision 13, F-LEDGER-COLLISION): revision 12 accepted the
    folded-twin collision as a deliberate loud corner — twin merge
    decisions whose distinguishing record lived only in operand cast
    syntax (`COALESCE(avg_out, CAST(1 AS DECIMAL(64,0)))` versus
    `COALESCE(avg_out, CAST(1 AS DECIMAL(65,1)))` over the
    pre-finalization `avg_out` `(65,4)`, both aligning to and folding
    as the identical `(68,4)` artifact) produced one observable
    signature matching two entries, so the pass rejected the
    statement at planning although the PARENT executes it with two
    distinct results `(65,0)` and `(66,1)`; the deliberate rejection
    violated the design's compatibility contract and corpus probing
    could not substitute for preserving the lineage. The
    distinguishing record now SURVIVES to capture: at decision
    recording time every recorded decimal coercive decision the
    source-domain resolver (entry 56) marks provenance-bearing enters
    the statement-local DECISION BARRIER — a content-signature set
    riding the same statement-local registry as the decision ledger,
    carried on the statement's compile-time `process.Process` as a
    planner-owned opaque field (the `WarningSink any`/`planSnapshotTS`
    precedent; its planning-generation lifecycle is owned by
    entry 58's enter/release pair — installed fresh and empty at
    both outermost planner entries, shared with nested entries
    through a depth marker, and restored to the exact prior field
    value on the outermost return, on success, error and panic
    unwind alike; consulted during planning only, never serialized,
    never transmitted — remote pipelines build their own processes
    from the shipped finished plan) — and BOTH fold
    engines guard their recursive entry against it: the rule engine's
    `foldExpr` (`rule/constant_fold.go:138`) and the sibling engine
    `constantFoldWithPreparedExactSource` (`pkg/sql/plan/utils.go:1888`)
    each receive the same `proc` at every call site, so exactly two
    gates cover every present and future mutating fold application
    (the audit's enumerated set — `optimize.go:29`/`:32`,
    `partition_topn.go:42`, and the sibling engine's in-build
    callers) without per-site edits. At a barrier-matched node the
    engine skips the argument recursion and the destructive rewrite,
    so each ledger entry's pre-coercion witness structure survives
    every post-recording fold and every speculative `DeepCopyExpr`
    fold consistently (constantness answers may see the retained
    executable shape where the parent folded; values, grouping and
    ordering outcomes are identical — §9 I7's controls — and the
    plan-shape difference is the documented §10 corner). A position
    folded during its own binding before any decision inside it was
    recorded stays decision 53's positional-ownership domain, copies
    resolve the same preserved structure by content (entry 52), and
    shadow replicas are built from the protected trees the barrier
    already kept. With operand syntax preserved, capture
    distinguishes the twins structurally, both bind their own
    entries, and the twin statement executes with parent-equivalent
    values, result types and persisted metadata (§9 I10's
    single-statement regression); the residual ambiguity check stays
    as MECHANISM-BUG defense in depth — unreachable for recorded
    decisions while the barrier stands, probe-asserted empty so a
    future ungated fold path fails loudly in CI instead of silently
    diverging; a statement with no provenance-flagged decision pays
    an O(1) empty-set check per fold entry and folds
    base-identically (I8).
56. Binder-owned source-domain channel and retry funnel (revision 13,
    F-RECONVERGENCE-CHANNEL): entry 51 placed the reconvergence retry
    at the four merge check call sites, but a merge checker consumes
    only `[]types.Type` (`function/function.go:1161`, reached through
    `function.GetFunctionByName(ctx, name, lookupTypes)`,
    `base_binder.go:8323–8327`) — no expression provenance and no
    operand subtrees — and a widened product and a genuine wide
    literal supply the same `(76,0)` while needing different retry
    decisions; behind a producer reference the surface type cannot
    recover the producing expression (`bindColRef` resolves a copied
    `Binding` type, `base_binder.go:653–657`). The retry therefore
    moves into the binder around the reconvergence families'
    (`coalesce`, `ifnull`, `case`, `if`, `iff`, `least`, `greatest`)
    function-resolution funnel — the direct `GetFunctionByName` call
    and its expression-synthesis twin, both enumerated at
    implementation with a totality assertion over the families' call
    sites: on the family's first-lookup failure, or on a successful
    FLOAT64 result over decimal-family inputs for `least`/`greatest`
    (the silent-degrade signature,
    `func_builtin_leastgreatest.go:376`/`:681`), the binder runs its
    SOURCE-DOMAIN RESOLVER over the argument expressions. The
    resolver computes, per operand, a (provenance flag,
    parent-published type) pair: (a) a direct expression operand is
    walked against the statement-local decision ledger — a recorded
    widened multiplication publication's parent type derives from its
    entry's recorded pre-coercion operand types via the §4.1 mirror
    (parent-unique for scale-0 surfaces since `min(76, 65)` is
    unconditional) — with totality asserted by the ledger coverage
    probe; (b) a producer-reference operand (ColRef through a derived
    table, view, CTE including its materialized scan, or a
    query-level alias) resolves through a planner-local per-column
    channel carried on the producer's `Binding` and populated at
    producer-binding construction from the producing expression's
    recorded signature — the established `headingProvenance`
    precedent (`bind_context.go:106–134`, `:510–562`, plus
    `cte_reuse.go:1912`'s per-node map) threads exactly such
    syntax through derived tables and CTEs, cloned and merged by the
    same rules, never serialized; set-operation outputs compute
    their channel over branch channels through the family's own
    merge; (c) a scalar-subquery operand resolves its retained
    subquery projection head (`Expr_Sub` stays structurally
    navigable until `flattenSubqueries`,
    `base_binder.go:1010–1031`). Non-widened operands resolve
    identity views (parent = surface; genuine wide literals keep
    their natural width on both engines, so an identically typed
    product and literal never drive the same decision), and an
    unresolvable operand defers to the family's own outcome with
    §9's channel-coverage UT asserting the deferral set empty over
    the direct / derived-table / view / CTE / scalar-subquery
    corpus. With at least one provenance-bearing operand the binder
    retries the family's lookup once over the resolved parent
    vector: success authors the parent's own overload and
    parent-domain alignment casts on the real argument expressions
    (the recorded ledger entry is the retried binding), failure
    re-arms the family's original outcome untouched — so a
    genuine-wide failure is retried only where multiplication
    provenance also participates and still lands the parent's
    identical outcome. Entry 51's outcome contract, worked examples,
    no-retry discipline and comparison exclusion stand; the
    placement moves, and the four family files
    (`function/type_check.go`, `func_binary.go`, `operatorSet.go`,
    `func_builtin_leastgreatest.go`) exit the planned production
    set, whose reconvergence owners become `base_binder.go` plus
    the registry carrier in `pkg/vm/process`.
    **(The funnel-scope sentence of THIS entry is EXTENDED by
    revision 27, F-SETOP-COMMON-RECONVERGENCE, decision log 73
    (iii)-(iv): the SAME source-domain resolver and the
    retry/re-arm discipline extend to the two set-operation
    common-type owners `query_builder.go:4828`–`:4830` and
    `utils.go:7588`–`:7590`, which are NOT binder family call
    sites and retry only their OWN FAILED lookups; the
    base-binder family funnel and its totality assertion
    otherwise stand unchanged — kept as history.)**

57. Tag/ordinal pending-producer channel for same-query-block
    aggregates and windows (revision 14, F-AGG-WINDOW-CHANNEL):
    entry 56's resolver channels reach direct expression trees,
    producer references and retained scalar-subquery projections —
    but a merge whose widened operand is the CURRENT query block's
    own pending aggregate or window output is none of these: the
    aggregate binds as a `ColRef{aggregateTag, colPos}` recorded in
    `BindContext.aggregates` (`having_binder.go:480–499`) and the
    window call as a `ColRef{windowTag, colPos}` recorded in
    `ctx.windows` (`buildWindowColRefExpr`,
    `window_binder.go:1187–1195`; the retained bound call,
    `:1444–1453`), with no `Binding` column and no `Expr_Sub` head.
    The witness `SELECT COALESCE(AVG(a*b), L76) FROM t` with
    `(38,0)` columns holding 1 and a genuine 76-digit literal `L76`
    shows the defect: the PARENT's product publishes `(65,0)`, the
    parent's AVG over it returns `(65,0)`, and the parent's merge
    `(65,0)`+`(76,0)` computes `requiredWidth` 76 and succeeds; the
    widened `(76,0)` product lets AVG provisionally return `(65,4)`,
    and `(65,4)`+`(76,0)` needs 80 — bind failure before the final
    plan ever reconciles, with no channel to retry. The resolver
    gains channel (d): an operand `ColRef` bound at the current
    block's `aggregateTag`/`windowTag` resolves through
    `ctx.aggregates[colPos]` / `ctx.windows[colPos].WindowFunc` —
    the retained producing call — recursing uniformly with channels
    (a)–(c) into its argument trees, and the producer OUTPUT's
    parent-published type derives by applying the producer's
    registered result rule over the arguments' resolved parent
    domains (AVG over the parent argument domain `(65,0)` yields the
    parent's registered output `(65,0)`; window frame, partition and
    order keys never produce values and contribute their unchanged
    binds). The retried merge then recomputes `(65,0)`+`(76,0)` →
    the parent's `(76,0)` target with parent-domain alignment casts;
    failure re-arms the family's original outcome, and an
    unresolvable pending producer defers to it, with §9 I4's
    extended deferral-set-empty assertion covering the
    direct-aggregate and window cells and the genuine-wide aggregate
    control (an aggregate over non-product genuine-wide operands)
    never retrying. The same channel feeds the entry-55 barrier: a
    recorded decision flagged provenance-bearing through channel (d)
    — both twins of `SELECT COALESCE(AVG(a*b), CAST(1 AS
    DECIMAL(64,0))), COALESCE(AVG(a*b), CAST(1 AS DECIMAL(65,1)))
    FROM t` — enters the barrier before its operand syntax folds in
    the same query block, so §9 I10's same-block regression executes
    with the parent's two distinct result types `(65,0)`/`(66,1)`,
    parent-equivalent values and parent-verbatim CTAS metadata. No
    new production file joins the reconvergence owners: the channel
    lives inside the already-owned `base_binder.go` resolver and
    shares the statement-local ledger/barrier registry.

58. Planning-generation registry lifecycle on the reused session
    process (revision 14, F-REGISTRY-LIFECYCLE): entries 55–56
    attach the statement-local decision ledger and the decision
    barrier's content-signature set to the statement's compile-time
    `process.Process`, but a session REUSES one process across
    statements (`ses.proc`, `mysql_cmd_executor.go:6220`; the
    statement loop `:6453–6472`), a compile RETRY rebuilds the plan
    on the same process (`buildPlanForCompileRetry`,
    `computation_wrapper.go:3011–3070`), and prepared statements
    replan on it again — so without a lifecycle owner a finished or
    failed generation could leak its registry into the next
    statement, poison a retry with the abandoned attempt's
    decisions, or retain statement-sized structure on the long-lived
    process. ONE carrier-provided enter/release pair owns the
    field's lifecycle, invoked at the two outermost plan entries
    through which every planning generation funnels — `BuildPlan`
    (`build.go:721`) and `(*BaseOptimizer).Optimize`
    (`optimize.go:54`, which calls `BuildPlan` at `:55` and then
    runs the fold rules at `:29`/`:32`): initial builds
    (`mysql_cmd_executor.go:4241`/`:4263`/`:4275`), retries,
    prepared rebuilds (`BuildPreparedExecutionPlan`,
    `prepared_binding.go:1022`, with its `Optimize` calls at
    `:1037`/`:1048`) and EXPLAIN recursion (`buildExplainPlan`,
    `build.go:595` → `:602`) all enter through them. ENTRY with no
    active generation saves the field's exact prior value, installs
    a fresh empty registry and marks depth one; a NESTED entry —
    required because the optimizer's fold rules run INSIDE the same
    generation after the nested `BuildPlan` returns
    (`optimize.go:29`/`:32` must still consult the barrier that
    nested binding recorded) — increments the depth and shares the
    active registry; a defer-registered release (the
    `warning_attempt.go:119–185` LIFO-restore precedent) decrements
    on every return path and, at depth zero, restores the saved
    prior value — nil in production — on success, error and panic
    unwind alike; a nil `proc` (test mocks) is a no-op. The
    boundaries follow: consecutive statements on the reused session
    process each plan against an empty registry; a failed build
    leaves nothing for the retry or the next statement; in-build
    nesting (inline view binding, `query_builder.go:12510`/`:12531`;
    the EXPLAIN inner build) shares — never overwrites — the outer
    registry; folds applied after the generation ends see the
    restored nil field and fold base-identically (the barrier is
    needed only until pass-entry capture, which the generation
    contains: the §10 value-identical corner stands); and retention
    on the long-lived process stays bounded to the saved prior
    value. §9 I10's lifecycle UT family pins consecutive statements,
    failed-then-clean builds, panic-unwind identity, retry double
    generations, nested planning with post-nesting rule folds,
    prepared double-`Optimize` independence and the instrumented
    depth-discipline assertion. The planned production set grows
    from 15 to 17 files: `pkg/sql/plan/build.go` and
    `pkg/sql/plan/optimize.go` join for the two outermost-entry
    calls; the carrier helpers stay in the planned `pkg/vm/process`
    file.

59. Same-block result-tag inventory completion — grouped, sampled
    and projected pending producers (revision 15, F-GROUPED-CHANNEL):
    entry 57's tag/ordinal descent covers only the current block's
    pending aggregate and window producers, but a grouped expression
    reaches the same merges through ANOTHER binding tag: the
    projection binder mints `ColRef{groupTag, colPos}` through
    `lookupGroupByAst` (`projection_binder.go:56–65`) and the HAVING
    binder through `lookupGroupByExpr` (`having_binder.go:55–66`),
    with the retained bound keys in `BindContext.groups`
    (`group_binder.go:272–290`) and the block's group output types
    read through `groupOutputType` (`types.go:1093–1098`): a widened
    operand behind a group reference matched no channel and could
    neither retry nor flag the entry-55 barrier. The witness `SELECT
    COALESCE(a*b, CAST(1 AS DECIMAL(65,1))) FROM t GROUP BY a*b`
    with nullable `(38,0)` columns holding 1 shows the defect: the
    PARENT's group key publishes `(65,0)`, the SELECT-list reference
    binds `groupOutputType` `(65,0)` and the parent merges
    `(65,0)`+`(65,1)` into `(66,1)`; the widened publication makes
    the group key `(76,0)`, and `(76,0)`+`(65,1)` needs
    `requiredWidth` 77 — bind failure before the final plan
    reconciles. The completed inventory resolves every remaining
    result-tag family by construction, each descent recursing
    uniformly with channels (a)–(c). (i) `groupTag` descends
    `BindContext.groups[colPos]`: the group output IS the retained
    bound producing expression, so no registered result rule
    applies — the parent-published group output type is the parent's
    own bind of that retained expression, read through the
    statement-local ledger, with `groupOutputType`'s grouping-set
    nullability adjustment applied on top (it depends only on the
    retained expression's type and `groupingFlag`, both
    parent-invariant); the retried witness recomputes
    `(65,0)`+`(65,1)` → the parent's `(66,1)` with the parent-domain
    alignment cast on the group reference, and the HAVING form binds
    identically through `having_binder.go:55–66`. (ii) `sampleTag`
    (minted at `build_sample.go:76`/`:96`, beside the
    `{groupTag, sampleTag}` binding pair at `:137`) descends
    `ctx.sampleFunc.columns[colPos]`, the retained bound sample
    column. (iii) `projectTag` (ORDER BY ordinal and alias
    references, `order_binder.go:380–390`/`:450–460`; the
    UNION-alias fallback for a non-column-reference projection,
    `base_binder.go:666–680`/`:682–692`) descends
    `ctx.projects[colPos]`, the retained bound projection
    expression. (iv) `timeTag` mints only the timestamp boundary
    columns `TimeWindowStart`/`TimeWindowEnd`
    (`base_binder.go:625–644`), never a decimal multiplication
    producer. (v) `resultTag`/`sinkTag` are never minted by
    expression binders — the tags serve only the
    `rootTag()`/`topTag()` plan-construction accessors
    (`bind_context.go:181–198`) and the recursive-CTE plumbing that
    rewrites already-bound trees — so no operand reaching the funnel
    can carry them; (iv) and (v) resolve irrelevant by construction
    and the deferral set stays empty without entries for them.
    Failure re-arms the family's original outcome, an unresolvable
    producer defers to it, and genuine-wide grouped controls (a
    group key over a genuine 76-digit literal column) never retry —
    §9 I4's deferral-set-empty assertion extends over the grouped,
    HAVING, projection-fallback, ordinal and sampled cells. The same
    resolutions feed the barrier: both decisions of `SELECT
    COALESCE(a*b, CAST(1 AS DECIMAL(64,0))), COALESCE(a*b, CAST(1 AS
    DECIMAL(65,1))) FROM t GROUP BY a*b` flag in before their
    operand syntax folds, so §9 I10's grouped twin regression
    executes with the parent's `(65,0)`/`(66,1)`, parent-equivalent
    values and parent-verbatim CTAS metadata. No new production file:
    the descent lives inside the already-owned `base_binder.go`
    resolver and shares the statement-local ledger/barrier registry.

60. Registry lifecycle coverage of standalone view-description
    generations (revision 15, F-VIEW-DESCRIPTION-LIFECYCLE): entry
    58 installs and releases the statement registry only at
    `BuildPlan` and `(*BaseOptimizer).Optimize`, but one production
    family binds SELECTs through NEITHER: the runtime view-columns
    table function (`view_columns.go:124–160`) calls
    `DescribeViewColumns` (`view_regeneration.go:142–155`), and the
    SHOW view-metadata path (`view_description.go:109–175`,
    describing at `:161`), the standalone schema inference
    (`view_schema_inference.go:385`, requested through
    `view_schema_request.go:113–118`) and the persisted-environment
    regeneration (`regenerateViewUsingPersistedEnvironment`,
    `compile/view_metadata_recovery.go:1314–1340` →
    `RegenerateViewDefinition`, `view_regeneration.go:262` →
    `genViewTableDef` at `:271`) all converge on `inferViewColumns`
    (`build_ddl.go:240`), which calls
    `bindAndOptimizeSelectQueryWithValidatorAndCapture`
    (`build.go:67`) DIRECTLY at `build_ddl.go:301`/`:308` — binding
    the stored definition (`builder.bindSelect`, `build.go:99`) and
    running its folds (`builder.createQuery()`, `build.go:153`) with
    no lifecycle entry, so a persisted view whose definition carries
    the entry-55 folded twins would bind with no barrier and no
    registry, and a registry an ancestor happened to install would
    never release along this path. The carrier-provided
    enter/release pair gains its THIRD invocation at that single
    shared funnel: every generation entry 58 covers already flows
    THROUGH it — the main SELECT build calls it at
    `build.go:741`/`:748`, PREPARE at `build_dcl.go:394`, CREATE
    VIEW's `genViewTableDef` inference at `build_ddl.go:408`,
    EXPLAIN's inner build and inline view binding alike — so a
    nested call increments the depth and SHARES the active registry
    (the added invocation changes nothing where entry 58 already
    installed, and the outer `Optimize` lifetime still governs the
    post-`BuildPlan` fold rules at `optimize.go:29`/`:32`), while
    each standalone path enters with no active generation, saves the
    field's exact prior value, installs fresh and marks depth one,
    and the defer-registered release restores the saved value at
    depth zero on success, error and panic unwind alike — covering
    even the publication-probe bind-only return
    (`build.go:145–151`). `proc` is non-nil on every standalone
    path: the isolated compiler contexts build on
    `NewViewBindingProcess` (`process2.go:121–152`, a child owning
    its mutable binding context — used at
    `sql_executor_context.go:72–100` and
    `frontend/compiler_context.go:92–106`), the borrowed-generation
    precedent `BorrowViewSchemaResources`
    (`pkg/vm/process/view_schema_resource.go:19–48`) shows scoped
    planner resources on such processes are established practice,
    and the regeneration path carries an explicit process REUSED
    across pending views (`view_metadata_recovery.go:1314–1340`) —
    the per-call freshness this entry installs on that reuse is
    exactly the leak class the fourteenth finding names; a nil
    `proc` stays a no-op. §9 I10's lifecycle UT family extends with
    the standalone cells (vii)–(xi): a folded-twin persisted view
    definition described with no enclosing generation (fresh
    install, barrier-held twins, parent-verbatim description
    records, field restored), repeated standalone calls each
    starting empty, standalone error and panic-unwind restoration,
    nested-call sharing preservation, and recovery-loop freshness on
    the reused recovery process. The planned production set stays 17
    files: the invocation lands inside the already-owned
    `pkg/sql/plan/build.go`.

61. TIME_WINDOW pending-producer resolution through `timeTag` (revision 16,
    F-TIMEWINDOW-CHANNEL): entry 59(iv) excluded `timeTag` as minting only
    the timestamp boundary columns, but the TIME_WINDOW aggregate routing
    retains decimal producers behind the same tag: whenever `ctx.timeTag >
    0` (`query_builder.go:6216`, set before any clause binds), the shared
    function binding routes aggregates through `BindAggFunc` and then
    `b.impl.BindTimeWindowFunc` (`base_binder.go:3369–3376`), which appends
    the retained call — a `DeepCopyExpr` of `ctx.aggregates[aggColPos]` with
    its top-level arguments rewritten to `ColRef{aggregateTag, aggColPos}`,
    sliding and `MAX_BY` remapped (`having_binder.go:955–1010`, appending at
    `:1003`) — to `ctx.times`, and publishes it as `ColRef{timeTag, colPos}`
    with `Typ: ctx.times[colPos].Typ` (`makeTimeWindowProjectionExpr`,
    `having_binder.go:721–736`). The witness `SELECT COALESCE(MAX(a*b),
    CAST(1 AS DECIMAL(65,1))) FROM t INTERVAL(ts, 1, minute)` with nullable
    `(38,0)` columns holding 1 reproduces the entry-59 defect verbatim: the
    PARENT's retained output publishes `(65,0)` and the merge computes
    `requiredWidth max(65,64)+max(0,1) = 66`, binding `(66,1)`; the widened
    `(76,0)` retained output needs 77 and bind-fails BEFORE the final plan
    reconciles, with no entry-59 channel able to recover the retained
    producer's parent domain — and the entry-55 barrier likewise could not
    flag the folded twins whose produced operand is the block's own
    TIME_WINDOW-routed aggregate output. The descent resolves by entry
    shape: a `ColRef{timeTag, colPos}` reads `ctx.times[colPos]` — an
    `Expr_Col` entry is a timestamp boundary self-reference
    (`TimeWindowStart`/`TimeWindowEnd` mint it at
    `base_binder.go:611–612`/`:625–644`), timestamp-typed and never a
    decimal producer, resolving nothing by construction; an `Expr_F` entry
    is the retained producing expression — the descent recurses uniformly
    with channels (a)–(c) into it, its top-level `ColRef{aggregateTag,
    aggColPos}` arguments descending `ctx.aggregates[aggColPos]` per channel
    (d), and the producer output's parent type derives from the parent's
    registered result rule over the resolved parent argument domains — the
    sliding and `MAX_BY` remaps (`having_binder.go:997–1001`) vary only
    argument ordinals and window configuration, never the registered rule.
    The retried witness recomputes `(65,0)`+`(65,1)` → the parent's `(66,1)`
    with the parent-domain alignment cast on the TIME_WINDOW reference; the
    HAVING form `SELECT MAX(a*b) FROM t INTERVAL(ts, 1, minute) HAVING
    COALESCE(MAX(a*b), CAST(1 AS DECIMAL(65,1))) > 0` binds identically
    through the same routing (`base_binder.go:3369–3376` with `ctx.timeTag >
    0`). Failure re-arms the family's original outcome, an unresolvable
    `Expr_F` defers to it with §9 asserting the deferral set empty over the
    completed corpus, a genuine-wide INTERVAL control (a genuine 76-digit
    literal column, no multiplication provenance) never retries, and the
    timestamp-boundary control (the boundary self-references in `ctx.times`)
    folds and binds base-identically. The same resolution feeds the entry-55
    barrier: both decisions of `SELECT COALESCE(MAX(a*b), CAST(1 AS
    DECIMAL(64,0))), COALESCE(MAX(a*b), CAST(1 AS DECIMAL(65,1))) FROM t
    INTERVAL(ts, 1, minute)` flag in before their operand syntax folds, so
    §9 I10's TIME_WINDOW twin regression executes with the parent's
    `(65,0)`/`(66,1)`, parent-equivalent values and parent-verbatim CTAS
    metadata. No new production file: the descent lands inside the
    already-owned `base_binder.go` resolver and shares the statement-local
    ledger/barrier registry — the production set stays 17 files.

62. Decision-owned pre-alignment domains at storage boundaries below width
    66 (revision 16, F-BOUNDARY-ALIGNMENT-DOMAINS): §4.1's casts rule keeps
    every ≤65-target cast's declared typed envelope and relies on §5's
    replay for the alignment layers it owns, but §5's replay fires only for
    chains that transitively consumed a reconciled aggregate output — and
    the merge families are the ONE implicit-alignment family inserting REAL
    plan-level casts: `coalesceDecimalResult` computes the uniform target
    and `newCheckResultWithCast` requests it (`func_binary.go:1125–1160`;
    CASE/IFF `operatorSet.go:559`→`:571–572`/`:896`; LEAST/GREATEST
    `func_builtin_leastgreatest.go:376`/`:681`), and the binder's
    application loop wraps operands whose types differ from the target (the
    same-decimal-same-scale skip at `base_binder.go:9001–9003`;
    `appendCastBeforeExpr` at `:9050`), while the add/subtract and divide
    mirrors align at execution with no plan-level casts at all. The witness
    `SELECT COALESCE(TRUNCATE(a*b, 0), d) FROM t` with nullable `a`, `b`
    `DECIMAL(38,10)` and `d DECIMAL(39,1)` — arithmetic verified at code:
    the parent's product publishes `(65,12)` (scale floor 12,
    `decimalMultiplyReturnType`, `list_operator.go:209–230`, base cap
    `min(w1+w2, 65)`) and `TRUNCATE(·, 0)` derives `(53,0)`
    (`refineDecimalRoundingReturnType`, `base_binder.go:9360–9396` — integer
    digits 53 plus result scale 0, no carry for truncate); the parent merges
    `max(53,38)+max(0,1) = 54` into `(54,1)` with per-position alignment
    casts; the widened publication `(76,12)` derives `TRUNCATE` `(64,0)`,
    merges `max(64,38)+max(0,1) = 65` into `(65,1)`, and inserts the
    scale-changing `CAST(TRUNCATE(·) AS DECIMAL(65,1))`. §4.1 read that
    cast's own declared `(65,1)` envelope into the merge, and with no
    aggregate in the tree §5 replays nothing — the boundary would persist
    `(65,1)` where the parent persisted `(54,1)`, violating the CTAS/view
    parent-verbatim contract including legacy replay. The rule: for
    storage-envelope evaluation ONLY, a ≤65-target cast verified
    DECISION-OWNED is looked through to the operand's reconstructed parent
    envelope — ownership is the revision-12 content-keyed identity
    (F-LEDGER-LINEAGE): the decision's function overload identity, arity,
    per-position pre-coercion operand types as the merge consumed them and
    per-position post-merge aligned types, verified by re-invoking the
    decision's own registered check over the live operand binds exactly as
    §5's ownership verification does, folded positions recovering their
    original operand domains from the same ledger. The rule is independent
    of §5's aggregate-driven replay — it changes NO plan chain and reads its
    decision evidence at every storage-envelope evaluation: CTAS, prepared
    CTAS, view authoring, view description
    (`inferViewColumns`/`DescribeViewColumns`) and legacy replay
    (`RegenerateViewDefinition`) alike. SQL-authored casts and
    decision-FOREIGN fixed-target casts (the pass's seam/head admission
    casts, recursive member→anchor coercions) keep their declared typed
    envelope exactly; a ledger miss preserves the declared typed envelope;
    an observable-state ambiguity stays the checked planning failure of §5
    fact 17. The evidence is available wherever evaluation runs: the ledger
    is recorded by the already-owned `base_binder.go` recorder on the
    planner-owned statement registry, and entries 58/60 install that
    registry on every planning generation that evaluates storage envelopes —
    CTAS and CREATE/ALTER VIEW authoring inside `BuildPlan`, and standalone
    view-column assembly through the third funnel invocation
    (`bindAndOptimizeSelectQueryWithValidatorAndCapture`, `build.go:67`),
    inside which `inferViewColumns` (`build_ddl.go:240`) derives description
    records — so description and legacy replay re-derive the parent's schema
    deterministically: the witness persists the parent's `(54,1)`
    everywhere, and the explicit-cast control `COALESCE(CAST(TRUNCATE(a*b,
    0) AS DECIMAL(65,1)), d)` persists `(65,1)` verbatim, §9 I2/I10
    asserting both. No new production file: the look-through lands inside
    the already-owned `build_ddl.go` envelope evaluator consuming the
    already-carried registry read-only — the production set stays 17 files.

63. Decision-evidence lifetime through standalone view-column assembly
    and prepared-CTAS refresh (revision 17,
    F-DECISION-EVIDENCE-LIFECYCLE): entry 62's look-through evaluates
    storage envelopes from the completed statement-local decision ledger
    (§4.1 casts, §6), but TWO boundaries evaluate them with NO ledger
    available, and with entry 62's miss rule preserving the declared
    cast target each boundary described and refreshed the shadowing
    `(65,1)`, never the parent's `(54,1)`. (i) STANDALONE VIEW-COLUMN
    ASSEMBLY: entry 60 installed the standalone view-description
    generation at the shared funnel
    (`bindAndOptimizeSelectQueryWithValidatorAndCapture`, `build.go:67`)
    and released it when the funnel returned, yet `inferViewColumns`
    (`build_ddl.go:240`) calls that funnel at `:301`/`:308` and then
    assembles the view column records at `:345–387` — AFTER the funnel
    returns, after the defer-registered release already restored the
    process field. Every standalone description and replay path
    (`view_columns.go:124–160` → `DescribeViewColumns`,
    `view_regeneration.go:142–155`; `view_description.go:161`;
    `view_schema_inference.go:385`;
    `view_metadata_recovery.go:1314–1340` → `RegenerateViewDefinition`,
    `view_regeneration.go:262` → `genViewTableDef` at `:271`) therefore
    evaluated storage envelopes — the entry-62 look-through included —
    against a RESTORED registry. Resolution: the standalone
    view-generation owner moves one level OUT to `inferViewColumns`
    itself — its entry at `build_ddl.go:240`, seeing no active
    generation, saves the field's exact prior value, installs a fresh
    empty registry and marks depth one, holding through the funnel call,
    the post-funnel validators and the whole `:345–387` assembly; the
    funnel's own invocation is RETAINED as the defensive standalone
    entry (every current funnel caller — the
    `bindAndOptimizeSelectQuery`/`...WithValidator` wrapper chain
    (`build.go:34`/`:54`, calling the funnel at `:62`) serving
    `build.go:741`/`:748` and `build_dcl.go:394` inside `BuildPlan`
    generations, and `inferViewColumns` itself at
    `build_ddl.go:301`/`:308` — now supplies an enclosing generation, so
    the funnel entry joins NESTED through the depth marker, increments
    and decrements without releasing, and only a future direct caller
    with no enclosing generation would install there); and the
    defer-registered release at `inferViewColumns`'s own return restores
    the saved prior value on success, error and panic unwind alike.
    Nested CREATE/ALTER VIEW authoring is unchanged (`BuildPlan`
    outermost → `genViewTableDef` at `build_ddl.go:408` →
    `inferViewColumns` → the funnel, each level joining nested), the
    recovery loop's reused process gets per-view install/release
    freshness exactly as entry 60 established, and a nil `proc` stays a
    no-op — a placement change inside the already-owned `build_ddl.go`,
    no new production file. (ii) PREPARED-CTAS REFRESH (**superseded by
    entry 64, revision 18 — the bundle-and-cache carrier specified
    below never runs for a prepared CTAS; kept as history**):
    `RefreshPreparedCTASInferredColumns` (`build_ddl.go:2396–2476`, its
    storage evaluation behind the `:2470` boundary call) runs AFTER
    planning, caching and parameter specialization have returned — the
    cached-plan execute path (`computation_wrapper.go:1922–1926`, after
    `FillValuesOfParamsInPlanWithSpecialization` at `:1922`), the
    prepare-expression retry (`:436`) and the compile-retry
    (`:3084–3089`) — with a signature carrying no process and no
    compiler context, so no generation entry (58, 60 or 63(i)) is active
    at exactly the evaluation that must choose the parent's `(54,1)`
    over the shadowing `(65,1)`. Resolution: RETAIN the completed ledger
    through prepare on the repository's own build-time-evidence carrier
    pattern, `DecimalFloatProofs` (bundled on `PreparedExecutionPlan`,
    `prepared_binding.go:996`, populated at `:1059` by
    `BuildPreparedExecutionPlan`, `:1022`; session-cached on
    `PrepareStmt.runtimeDecimalFloatProofs`,
    `pkg/frontend/types.go:410`, populated at `:833` and cleared with
    the specialization cache at `:850`; mirrored on the computation
    wrapper's slot, `computation_wrapper.go:100`, populated at `:1918`,
    cleared at `:2025`, consulted at the execute gate `:1854`; matched
    by `MatchPreparedDecimalFloatProofs`,
    `prepared_decimal_float.go:35`). The OUTERMOST release of a
    generation that authored a prepared CTAS (the authoring path flags
    the registry) stashes a copy of the completed statement ledger in a
    process-side claim slot alongside the registry restore; the
    prepare-bundle constructor claims the slot into a retained-ledger
    field of `PreparedExecutionPlan`, and the retry paths claim it
    directly after their fresh build returns; the session's
    `PrepareStmt` caches the carrier alongside its specialization cache
    with the same populate/clear pairing (DEALLOCATE and eviction
    release it); `FillValuesOfParamsInPlanWithSpecialization`
    (`utils.go:4635`, already in the set) refreshes retained entries for
    every subtree its parameter substitution re-derives; and
    `RefreshPreparedCTASInferredColumns` gains a retained-evidence
    parameter, its three call sites forwarding the claimed carrier
    (`:1926` from the cached `PrepareStmt` field; `:436` and `:3089`
    from the freshly claimed slot). Evaluation consults the retained
    ledger READ-ONLY under entry 62's ownership rule — decision-owned
    iff position and target match a recorded insertion AND the operand
    subtree's own type equals the recorded pre-coercion operand type —
    recomputed over the runtime operand types, a ledger miss preserving
    the declared envelope (conservative), an observable-state ambiguity
    staying §5-fact-17's checked planning failure. The carrier is
    execution-scoped, session-local and in-memory — never persisted,
    cataloged, serialized or transmitted (remote pipelines build their
    own processes from the shipped finished plan), so no format,
    protocol or mixed-version boundary changes; a no-parameter prepared
    CTAS re-derives nothing and refreshes nothing. §9 I10 gains: revised
    (xiii) — the `inferViewColumns`-owned standalone generation, the
    parent's `(54,1)` description records on both standalone paths, the
    explicit-cast control `(65,1)`, the process field restored after the
    final consumer on every terminal path; new (xiv) — the refresh
    boundary on both entry paths plus the prepare-expression retry, the
    specialization refresh, DEALLOCATE and eviction release and the
    no-parameter control. Production-set accounting: (i) is a placement
    change inside the already-owned `build_ddl.go`; (ii) joins three
    files — `pkg/sql/plan/prepared_binding.go`, `pkg/frontend/types.go`
    and `pkg/frontend/computation_wrapper.go` — i.e. 20 production
    files. The revision-18 accounting supersedes this tally: entry 64 swaps the set membership — `pkg/frontend/mysql_cmd_executor.go` joins, `pkg/sql/plan/prepared_binding.go` leaves — the set staying 20 production files.

64. Retained-evidence publication on the real prepared-DDL lifecycle
    and a genuinely parameterized refresh witness (revision 18,
    F-EVIDENCE-PUBLICATION-PATH and F-PREPARED-CTAS-WITNESS): entry
    63(ii) routed the retained prepared-CTAS ledger through
    `PreparedExecutionPlan`'s `DecimalFloatProofs` bundle and the
    sourceBindingQuery specialization cache, but PREPARE builds a
    CTAS on the DDL route where neither runs — `getPreparePlan`
    resolves CREATE TABLE through its default branch to `BuildPlan`
    (`build_dcl.go:99–101`), `createPrepareStmtInSession` builds the
    plan through `buildPlanWithAuthorization`
    (`mysql_cmd_executor.go:3104` → `buildPlan` →
    `buildPlanWithPrepareMode`, `:4205`; default `plan2.BuildPlan`,
    `:4273`) and constructs the session's `PrepareStmt` directly
    (`:3167–3173`) with no bundle anywhere on the path, and at
    execute the sourceBindingQuery gate requires a top-level `Query`
    (`computation_wrapper.go:1629`) that a DDL payload never has, so
    the specialization-cache population (`:1916` → `:2045` →
    `types.go:833`) never runs for CTAS either. The repository's
    build-time evidence-carrier pattern stays the precedent, but
    capture, publication and release ride the REAL prepared-DDL
    lifecycle. (i) CAPTURE AND PUBLICATION ON THE PREPARE/DDL PATH,
    tied to the prepared-plan generation: the OUTERMOST release of a
    generation that authored a prepared CTAS — flagged when the
    built inner plan is `Ddl` carrying a `Query` — stashes a copy of
    the completed statement ledger in a process-side claim slot
    alongside the registry restore, keyed by that generation's
    nonce; `createPrepareStmtInSession` claims the slot LATE,
    immediately before the successful `PrepareStmt` construction at
    `mysql_cmd_executor.go:3167–3173` that stores the very plan the
    generation produced, into a new `PrepareStmt` retained-evidence
    field (`pkg/frontend/types.go`); the claim succeeds only when no
    generation has installed since the stash (a stale slot is
    unclaimable), claiming CLEARS the slot (read-once), a PREPARE
    failing before construction — parse, build or authorization
    error — never claims, and an unclaimed slot is discarded by the
    next generation's fresh install, so a failed publication leaves
    nothing behind and no evidence migrates across generations.
    (ii) REBUILD REPLACEMENT: the prepare-expression retry
    (`computation_wrapper.go:416–440`) and the compile-retry
    (`:3067–3089`) each run a FRESH `buildPlanWithPrepareMode` build
    (`:416`/`:3070`) — a fresh generation with its own outermost
    entry (decision log 58) — and claim THAT generation's stash
    immediately after the fresh build returns, REPLACING, never
    accumulating with, any previously claimed copy, so the retained
    evidence in forced publication always belongs to the plan
    generation it specializes; the cached-plan execute path
    (`:1921–1926`, FillValues at `:1922`, refresh at `:1926`) runs
    no fresh generation and forwards the stored `PrepareStmt` field
    — the sole publication channel for that path. (iii)
    PER-EXECUTION SPECIALIZATION OWNERSHIP:
    `FillValuesOfParamsInPlanWithSpecialization` (`utils.go:4635`,
    already in the set) substitutes typed parameter literals over
    its DeepCopied plan and refreshes the retained entries for every
    subtree its substitution re-derives, per execution; evaluation
    consults the retained ledger READ-ONLY under entry 62's
    ownership rule recomputed over the runtime operand types, a
    ledger miss preserving the declared envelope (conservative), an
    observable-state ambiguity staying §5-fact-17's checked planning
    failure; release pairs with the statement's existing
    specialization-cache lifetime — `clearRuntimeSpecializationCache`
    (`types.go:850`) and `PrepareStmt.Close`, so DEALLOCATE and
    session eviction release it, and a copy claimed by a retry whose
    rebuild loses publication is released with the wrapper scope
    that claimed it. (**release owner superseded by entry 65
    (i)-(ii), revision 19, F-REBUILD-EVIDENCE-PUBLICATION — the carrier
    release decouples from the specialization-cache clearing; kept as
    history**) `RefreshPreparedCTASInferredColumns` gains the
    retained-evidence parameter unchanged from entry 63(ii), its
    three call sites forwarding per channel (`:1926` from the stored
    `PrepareStmt` field; `:436` and `:3089` from the
    fresh-generation claim). The carrier remains execution-scoped,
    session-local and in-memory — never persisted, cataloged,
    serialized or transmitted (remote pipelines build their own
    processes from the shipped finished plan), so no format,
    protocol or mixed-version boundary changes; a no-parameter
    prepared CTAS re-derives nothing and refreshes nothing (the
    refresh branch itself requires `numParams > 0`,
    `computation_wrapper.go:1921`). (iv) THE WITNESS REACHES STORAGE
    REFRESH (F-PREPARED-CTAS-WITNESS): entry 63(ii)'s fixture
    `COALESCE(TRUNCATE(a*b,0), d)` binds no parameter an output type
    depends on, so specialization leaves the inferred output types
    equal, the per-column `reflect.DeepEqual` skip
    (`build_ddl.go:2452–2453`) fires and the `:2470` boundary
    evaluation never runs — the `(54,1)` assertion could pass from
    prepare-time metadata without consuming the retained carrier.
    The witness is now genuinely parameterized: `PREPARE p FROM
    'CREATE TABLE t2 AS SELECT COALESCE(TRUNCATE(a*b, ?), d) FROM
    t'` over `a`, `b` DECIMAL(38,10) and `d DECIMAL(39,1)`, EXECUTE
    with `?=0` — the digits argument a parameter marker leaves
    `refineDecimalRoundingReturnType`'s TRUNCATE refinement at its
    early return (`base_binder.go:9360–9396`), so PREPARE stores the
    unrefined input-family column type, while specialization binds
    `0` as an I64 literal and re-derives the refined TRUNCATE arm
    and the merged column — a type necessarily different from the
    stored one, so the DeepEqual skip cannot fire and `:2470`
    genuinely evaluates; a typed probe oracle asserts on EACH entry
    path that the boundary evaluation RAN and CONSUMED the forwarded
    retained ledger at the COALESCE merge decision, the re-derived
    column persisting the parent's `(54,1)` only through that
    consumption (a ledger miss re-derives the shadowing `(65,1)`
    verbatim), the parameterized explicit-cast control
    `COALESCE(CAST(TRUNCATE(a*b, ?) AS DECIMAL(65,1)), d)` keeping
    `(65,1)` regardless of evidence, and the separate no-parameter
    prepared CTAS control asserting refresh is never reached. (**fixture and oracle superseded
    by entry 65 (iii)–(iv), revision 19, F-SIBLING-CAST-RETENTION; kept
    as history**)
    Production-set accounting: `pkg/frontend/types.go` (the new
    `PrepareStmt` field) and `pkg/frontend/computation_wrapper.go`
    (claim and forwarding across the three channels) stay in the
    set, `pkg/frontend/mysql_cmd_executor.go` JOINS it (the late
    claim at `createPrepareStmtInSession`), and
    `pkg/sql/plan/prepared_binding.go` LEAVES it (the bundle route
    abandoned — its `BuildPlan`/`Optimize` invocations remain
    ordinary decision-log-58 generation entries) — the planned
    production set stays 20 production files; (iv) changes only §9
    fixtures and oracles, no production file.

65. Publication on the ordinary prepared-plan rebuild path and the
    complete-cast-tree parameterized witness (revision 19,
    F-REBUILD-EVIDENCE-PUBLICATION and F-SIBLING-CAST-RETENTION): entry 64
    specified capture, publication and the two retry claims but left ORDINARY
    EXECUTE REBUILDING unspecified — a session DDL or schema change between
    PREPARE and EXECUTE flips the cached-version comparison
    (`computation_wrapper.go:1423–1425`) and a mode or protocol change flips the
    same `needRebuild` (`:1468–1480`); the path builds a FRESH plan through the
    same funnel (`rebuildPreparePlan` called with the same `buildPlan` at `:1537`,
    the helper `:934–970` invoking `buildFn` at `:966` — the `buildPlan` →
    `buildPlanWithPrepareMode` stack entry 64 verified) and commits the
    replacement (`prepareStmt.PreparePlan = newPlan`, `:1565`), bypassing
    `createPrepareStmtInSession` AND both retry claims — so the fresh seed plan
    had no specified matching carrier — and the same path clears the
    specialization cache at `:1640–1642` on every rebuild, while revision 18
    paired the carrier release with exactly that clearing — a pairing that would
    sever the carrier before the CTAS specialization consumer at `:1922`–`:1926`.
    And entry 64 (iv)'s fixture still did not change the output type (verified at
    code): PREPARE's argument alignment inserts a column-only CAST aligning `d`
    from (39,1) to the merge family's (76,12) — (65,12) on the parent
    (`argsCastType` with `appendCastBeforeExpr`, `base_binder.go:8990–9050`); that
    cast is NOT marked provisional (`base_binder.go:5108–5111`), so specialization
    preserves it (`visit_plan_rule.go:3017–3025`,
    `unwrapNumericPrefixDependentImplicitCast` `:4823–4852`), and even with the
    TRUNCATE arm refined to (64,0) the retained `d` cast keeps the COALESCE head
    at (76,12) — equal to the prepare-stored type, so the per-column
    `reflect.DeepEqual` skip (`build_ddl.go:2452–2453`) still fires, and the
    parent's matching envelope is (65,12), not the asserted (54,1). Revision 19
    resolves both at design level. (i) THE REBUILD CLAIM, ORDERED AFTER THE
    REPLACEMENT COMMIT AND BEFORE THE CLEAR: the rebuild's fresh build is a fresh
    decision-log-58 generation whose outermost release stashes its ledger copy in
    the same nonce-keyed, read-once process-side claim slot per entry 64 (i) — the
    route reuses the same funnels, so the stash is not new machinery; the executor
    claims that slot INSIDE the `needRebuild` block, immediately AFTER the
    replacement commit at `:1563–1566` and BEFORE the specialization-cache
    clearing at `:1640–1642` — the failed-rebuild return at `:1538–1540` never
    claims, the column-definition failure return at `:1558–1561` never claims,
    both failure branches ride the deferred `prepareStmt.needsRebuild = true`
    marker (`:1524`), and no plan-generation entry runs between the rebuild return
    and the claim (the intervening column-definition derivation at `:1542–1561`
    builds no plan), so the stash is always claimable, with entry 64 (i)'s
    stale-slot miss standing as the defensive bound; the claimed copy REPLACES,
    never accumulates with, the previously stored carrier — the displaced copy
    released with the claim — so within one execute the order is fresh build
    (`:1537`) → replacement commit (`:1563`–`:1566`) → claim into the stored
    `PrepareStmt` field → specialization-cache clear (`:1641`) → FillValues
    (`:1922`) → refresh (`:1926`), and the refresh consumer always reads the
    carrier the seed plan's own generation published. (ii) RELEASE OWNED BY THE
    SEED-PLAN GENERATION, NOT THE CACHE CLEAR: the retained carrier is released
    only by (a) a replacing claim — this rebuild channel, both retry channels of
    entry 64 (ii), or the late PREPARE claim of entry 64 (i) — (b)
    `PrepareStmt.Close` on DEALLOCATE and session eviction (`types.go:856`), or
    (c) the wrapper-scope failure cleanup of the execute scope that claimed it;
    `clearRuntimeSpecializationCache` (`types.go:845–854`) is NO LONGER a release
    owner for the retained carrier — the specialization cache is rebuilt per
    execute while the carrier must survive every specialization-cache cycle its
    seed plan serves — which keeps the seed plan's evidence alive for its actual
    consumer lifetime: the cached-plan execute path (`:1921`–`:1926`) forwards the
    stored `PrepareStmt` field on every execute of the same generation until a
    retry or rebuild replaces it; the PREPARE-time channel is unchanged (one
    PREPARE builds one generation and claims its stash late, entry 64 (i)). (iii)
    THE WITNESS USES THE COMPLETE PREPARED CAST TREE: the parameterized fixture
    places the parameter marker in the TRUNCATE HEAD's digits argument — `PREPARE
    p FROM 'CREATE TABLE t2 AS SELECT TRUNCATE(COALESCE(a*b, d), ?) FROM t'` over
    `a`, `b` DECIMAL(38,10) and `d` DECIMAL(39,1), EXECUTE with `?=0`. The sibling
    alignment cast still aligns `d` into the merge family (widened (76,12); parent
    (65,12)) and survives specialization as verified above, but the
    parameter-marker digits argument now holds the TRUNCATE refinement at
    `refineDecimalRoundingReturnType`'s early return (`base_binder.go:9360–9396`)
    over the MERGED head: the PREPARE snapshot carries the unrefined head type
    (76,12) — its storage-envelope tag persisting the parent's (65,12), exactly as
    the parent's own snapshot carries (65,12) — while EXECUTE's specialization
    substitutes `0` as an I64 literal and re-derives the refined TRUNCATE head —
    (64,0) widened, (53,0) on the parent — necessarily different from the
    snapshot's on BOTH engines, so the per-column `reflect.DeepEqual` skip
    (`build_ddl.go:2452–2453`) cannot fire and the `:2470` boundary evaluation
    genuinely runs. NO ALIGNMENT REPLAY IS ADOPTED: specialization's preservation
    of decimal alignment casts is the verified engine semantics, the retained
    sibling cast now participates in the witness's own consumption oracle, and
    cast ownership stays with the pre-existing provisional-cast machinery — no
    replay owner and no compatibility change. The typed probe oracle asserts on
    EVERY entry path — the cached-plan execute
    (`computation_wrapper.go:1922`–`:1926`), the prepare-expression retry
    (`:436`), the compile-retry (`:3084`–`:3089`) and (iv)'s rebuild channel —
    that the boundary evaluation RAN and CONSUMED the forwarded retained ledger at
    the merge decision, the persisted tag discriminating all three failure modes
    with one oracle value: CONSUMPTION persists the parent's (53,0) — the parent's
    own re-derived tag for this same statement — a LEDGER MISS re-derives the
    widened derivation (64,0) verbatim, and REFRESH NEVER RUNNING leaves the
    snapshot's prepare-time (65,12). Controls: the authored interior-cast control
    `TRUNCATE(CAST(COALESCE(a*b, d) AS DECIMAL(65,1)), ?)` stores (65,1) at
    PREPARE and persists the re-derived (64,0) at EXECUTE on BOTH engines — the
    author's explicit envelope honored with no evidence dependence; the separate
    no-parameter control `TRUNCATE(COALESCE(a*b, d), 0)` refines at PREPARE itself
    and persists the parent's (53,0) from prepare-time evaluation alone, the probe
    asserting refresh is never reached with `numParams == 0`
    (`computation_wrapper.go:1921`). (**fixture and oracle superseded by entry 66 (i)–(iii),
    revision 20, F-MISS-ORACLE-COLLAPSE — the >65 sibling cast is looked
    through independently of the ledger, collapsing the miss oracle onto
    consumption; kept as history**) (iv) VALIDATION NOW COVERS THE REBUILD
    CHANNEL: §9 I10 (xiv) gains a schema-invalidation scenario — PREPARE the (iii)
    fixture, ALTER a referenced table between PREPARE and EXECUTE so the version
    comparison at `computation_wrapper.go:1423–1425` flips (a mode or protocol
    change at `:1468–1480` exercises the same channel), then EXECUTE with `?=0`:
    the probe asserts generation identity — the consumed ledger's nonce equals the
    REBUILD generation's nonce, not the PREPARE generation's — evidence
    consumption persisting the parent's (53,0) through the rebuilt plan, carrier
    replacement at the claim (the PREPARE-generation carrier released with it),
    and a second consecutive EXECUTE of the same rebuilt generation consuming the
    same replaced carrier with no further claim, while a no-invalidation EXECUTE
    keeps consuming the PREPARE-claimed carrier (entry 64's channels stand
    unchanged). (**reuse assertions superseded by entry 66 (iv)–(v),
    revision 20, F-REUSE-WITNESS-REACHABILITY — `DatabaseDefaults` forces
    per-execute rebuilding on the ordinary fixture; kept as history**)
    Production-set accounting: the rebuild claim, its ordering and the
    forwarding live in `pkg/frontend/computation_wrapper.go`, the carrier field
    and its release pairing in `pkg/frontend/types.go`, the late PREPARE claim in
    `pkg/frontend/mysql_cmd_executor.go` — all already in the planned set since
    revision 18 — so the planned production set stays 20 production files;
    (iii)–(iv) change only §9 fixtures and oracles, no production file.

66. The decision-owned width-≤65-cast witness fixture with a genuine
    empty-ledger ablation, and the split rebuild-validation witnesses
    (revision 20, F-MISS-ORACLE-COLLAPSE and F-REUSE-WITNESS-REACHABILITY):
    entry 65 (iii)'s typed probe oracle claimed a LEDGER MISS re-derives the
    widened (64,0) verbatim — impossible (verified at code): the sibling
    alignment cast targets (76,12), and a cast whose target is wider than 65
    is binder-generated and looked through UNCONDITIONALLY, independently of
    the retained ledger (§4.1; `build_ddl.go:1870–1881`), so with no ledger at
    all the reconstruction looks through the (76,12) cast to the product's
    (65,12), `d` contributes its own (39,1), COALESCE reconstructs (65,12) and
    TRUNCATE(..., 0) reconstructs (53,0) — the very value claimed for
    consumption, so the miss oracle could never discriminate a miss from
    consumption. And entry 65 (iv)'s reuse scenarios were unreachable
    (verified at code): the ordinary prepared CTAS carries no explicit
    charset/collation option — the CTAS grammar has no `table_options` slot
    (`mysql_sql.y:10889–10936`) — so at the minted protocol floor
    (`defines.MORPCVersion118`, `pkg/defines/const.go:156`) every
    ordinary-database CTAS plan records a non-nil `DatabaseDefaults`
    (`DatabaseDefaultsEnabled`, `database_defaults.go:33–42`;
    `GetDatabaseDefaults` `:210–240` with `DecodeDatabaseDefaults` `:182–206`
    always returning non-nil; `tableDatabaseDefaults` `:242–251`, installed at
    `build_ddl.go:4466–4471`), `shouldRebuildPreparePlan` then returns true
    UNCONDITIONALLY (`computation_wrapper.go:2834–2837`), feeding
    `rebuildEveryExecute` (`:1444`) into `needRebuild` (`:1477–1480`) — EVERY
    EXECUTE of the revision-19 fixture rebuilds, so the no-invalidation
    keeps-the-PREPARE-carrier and second-execute reuses-the-rebuilt-carrier
    assertions could never run. Revision 20 resolves both at design level. (i)
    THE FIXTURE GAINS A DECISION-OWNED ≤65 ALIGNMENT CAST: `PREPARE p FROM
    'CREATE TABLE t2 AS SELECT TRUNCATE(COALESCE(TRUNCATE(a*b, 0), d), ?) FROM
    t'` over `a`, `b` DECIMAL(38,10) and `d` DECIMAL(39,1), EXECUTE `?=0` —
    the inner literal-digits TRUNCATE refines at PREPARE itself ((64,0)
    widened, (53,0) parent), so the widened COALESCE family is (65,1) against
    the parent's (54,1) — a DIVERGENT merge, a recorded decision — and the
    binder's column-only alignment casts of both arms into (65,1) land at
    width ≤65 (`base_binder.go:8990–9050`), surviving specialization
    non-provisionally (`base_binder.go:5108–5111`;
    `visit_plan_rule.go:3017–3025`, `:4823–4852`); the boundary evaluator's
    ≤65-cast rule (decision log 62, `build_ddl.go:1870–1881`) is exactly the
    evidence-dependent branch — a ≤65 cast verified decision-owned through
    the retained ledger contributes its operand's reconstructed parent
    envelope, while an unverifiable one keeps its declared envelope — so this
    fixture's reconstruction REQUIRES retained evidence. The head's parameter
    marker still holds `refineDecimalRoundingReturnType`'s early return
    (`base_binder.go:9360–9396`): the PREPARE snapshot carries the unrefined
    head (65,1), its storage-envelope tag the parent's (54,1) — as on the
    parent itself — and EXECUTE's specialization refines the head to (64,0)
    widened, (53,0) on the parent — necessarily different from the snapshot's
    on both engines, so the per-column `reflect.DeepEqual` skip
    (`build_ddl.go:2452–2453`) cannot fire and the `:2470` boundary evaluation
    genuinely runs. (**execute-specialization premise and
    entry-path probe superseded by entry 67 (i)–(iii), revision 21,
    F-EXECUTE-CHANNEL-UNREACHABLE — the fill is substitution-only on
    every route and the skip compares filled against original query
    output, so the head never specializes and the boundary evaluation
    never runs at any execute locus; kept as history**) (ii) THREE
    SEPARATED ORACLES, THE PROBE ON EVERY ENTRY
    PATH: the typed probe oracle still asserts on every entry path — the
    cached-plan execute (`computation_wrapper.go:1922`–`:1926`), the
    prepare-expression retry (`:436`), the compile-retry (`:3084`–`:3089`) and
    (iv)-(v)'s rebuild channels — that the boundary evaluation RAN and
    CONSUMED the forwarded retained ledger at the merge decision, and the
    persisted tag now discriminates three genuinely distinct values:
    CONSUMPTION persists the parent's (53,0) — the operands' parent envelopes
    (53,0) and (39,1) merge to (54,1) and TRUNCATE(..., 0) re-derives (53,0),
    the parent's own re-derived tag for the same statement; the EMPTY-LEDGER
    ablation — the same fixture driven through the same typed-probe UT
    harness with publication withheld, so the forwarded carrier is empty —
    reconstructs (64,0) (the unverified casts keep their declared (65,1), the
    merge re-derives (65,1) and TRUNCATE (64,0)), genuinely different from
    consumption; REFRESH NEVER RUNNING leaves the snapshot's prepare-time
    (54,1), distinct from both. (**oracle values superseded by
    entry 67 (iii), revision 21 — consumption is the parent's (54,1)
    (its own snapshot tag), the ablation relocates to a labeled harness
    probe ((54,1) with retained evidence, (65,1) without), and
    refresh-never-ran equals consumption in value here; kept as
    history**) (iii) CONTROLS RE-PARAMETERIZED AND
    PRESERVED: the authored explicit-cast control
    `TRUNCATE(COALESCE(TRUNCATE(a*b, 0), CAST(d AS DECIMAL(65,1))), ?)` keeps
    the author's (65,1) envelope on both engines (an explicit cast is user
    semantics — never looked through, never decision-owned), storing (65,1)
    at PREPARE and persisting the re-derived (64,0) at EXECUTE on both
    engines with and without the retained carrier — no evidence dependence.
    (**explicit-cast EXECUTE oracle superseded by entry 67 (iv),
    revision 21 — the control persists (65,1) at EXECUTE on both
    engines, nothing re-deriving (64,0); kept as history**)
    the no-parameter control `TRUNCATE(COALESCE(TRUNCATE(a*b, 0), d), 0)`
    refines at PREPARE itself and persists the parent's (53,0) from
    prepare-time evaluation alone, the probe asserting refresh is never
    reached with `numParams == 0` (`computation_wrapper.go:1921`). No
    alignment replay is adopted — cast ownership stays with the pre-existing
    provisional-cast machinery. (iv) WITNESS A, MANDATORY PER-EXECUTE
    REBUILDING — THE POLICY PRESERVED: the pre-existing CTAS rebuild policy
    stands verbatim (no change to `shouldRebuildPreparePlan`,
    `tableDatabaseDefaults` or the `:1444` / `:1477–1480` wiring); the
    live-service BVT turns the policy itself into the witness — an executable
    formation probe FIRST asserts the stored plan's `DatabaseDefaults` IS
    non-nil for the ordinary-database fixture, failing loudly if the formation
    precondition is absent, then EVERY EXECUTE rebuilds: the probe asserts per
    execute that a fresh generation built and committed (`:1563–1566`),
    claimed its own stash ordered after the commit and before the
    `:1640–1642` clearing, and CONSUMED that execute's OWN rebuild
    generation's ledger — the persisted tag the parent's (53,0), the consumed
    nonce equal to the rebuilding execute's own, never the PREPARE
    generation's after the first EXECUTE — and that each replacing claim
    released the displaced carrier (the PREPARE-generation carrier at the
    first EXECUTE, the previous execute's at each following one); an ALTER
    between executes is subsumed by the unconditional channel, the version
    comparison at `:1423–1425` and the policy agreeing on rebuilding. (**persisted-tag
    oracle superseded by entry 67 (v), revision 21 — the persisting
    value is the parent's (54,1), re-derived by each rebuild
    generation's own binding; kept as history**) (v)
    WITNESS B, CACHED-GENERATION REUSE — PRECONDITIONS ASSERTED FIRST: a UT
    formation makes reuse reachable — with the process-level
    `MOProtocolVersion` global set BELOW the minted floor via the established
    `rt.SetGlobalVariables(moruntime.MOProtocolVersion, ...)` test pattern
    with save/restore (`pkg/sql/compile/expression_protocol_test.go:75–120`),
    `DatabaseDefaultsEnabled` is false (`database_defaults.go:33–42`) and the
    fixture's stored plan carries a NIL `DatabaseDefaults` — and the witness
    asserts its preconditions as executable checks BEFORE any identity
    assertion: `DatabaseDefaults == nil` with `shouldRebuildPreparePlan`
    false, and no intervening invalidation (equal cached and current versions
    at `:1423–1425`, no subscription or named-snapshot dependency, no EXPLAIN
    wrapper at `:1437–1442`, CTAS outside `preparedDDLNeedsCatalogRefresh`
    `:2256–2265`, mode and protocol unchanged at `:1468–1477`, plan-generation
    reuse supported and the statistics gates quiet at `:1480–1492`); THEN the
    no-rebuild probe asserts NO rebuild occurred — `planGenerationReused`
    (`:1494`) true and no fresh claim recorded by the claim-slot probe — and
    only then nonce identity: a second consecutive EXECUTE consumes the SAME
    PREPARE-claimed carrier with no further claim, persisting the parent's
    (53,0) (**carrier-consumption phrasing and value oracle superseded
    by entry 67 (v), revision 21 — the second EXECUTE retains the
    PREPARE-claimed carrier untouched with no fresh claim, reusing the
    PREPARE-derived (54,1) through the skip; kept as history**); a controlled single `ddlVersion` flip produces exactly one
    rebuild-and-replace — the consumed nonce changing exactly once, to the
    rebuild generation's — whose following quiescent EXECUTE reuses the
    rebuilt carrier again with no further claim, the restore-in-cleanup
    discipline of the cited precedent keeping the formation scoped to the
    witness. (vi) PRODUCTION-SET ACCOUNTING AND HONEST SCOPE: (i)–(v) change
    only §9 fixtures, oracles and UT/BVT witnesses and their harness — no
    production file joins or leaves the planned set, which stays 20 production
    files — and the pre-existing CTAS rebuild policy, the publication
    funnels, the claim ordering and the release ownership of entries 64–65
    stand unchanged.


67. The execute-route refresh channel unreachable for
    widened-multiplication shapes, the execute oracles corrected
    (revision 21, F-EXECUTE-CHANNEL-UNREACHABLE): implementing entry
    66's witness verified at code and by an instrumented planner
    probe that two premises collapse. (i) NO EXECUTE ROUTE
    RE-DERIVES A PREPARED DDL PLAN'S INFERRED OUTPUT TYPES: every
    execute-path fill is substitution-only — the specialized fill
    replaces parameter markers with literal expressions and touches
    no function result type (base `computation_wrapper.go:431`/
    `:1922`/`:3085` feeding the refresh at `:436`/`:1926`/`:3089`),
    the ordinary rebuild builds the PREPARE statement again WITH
    markers (`rebuildPreparePlan`, base `:948`–`:951`), the
    round/truncate prepared-numeric fallback marks the VALUE
    argument only (`bindPreparedNumericPrecisionFuncExpr`,
    `base_binder.go:3399`–`:3514`), and `ResetParamRefRule`
    rebinds only integer-argument, timestampadd and marked
    numeric-value positions **(INCOMPLETE — provisional
    common-VALUE positions are also rebound, changing an
    output column's type where an arm is provisional;
    corrected by decision log entry 68 (i); kept as
    history**) — the fixture's outer TRUNCATE head
    keeps `(65,1)` through filling on both engines (probe: filled
    head `(65,1)` unchanged), the claimed EXECUTE specialization to
    `(64,0)` widened / `(53,0)` parent existing nowhere; the
    parent's own EXECUTE persists its PREPARE-derived snapshot tag
    — base `genAsSelectCols` taking the head's type verbatim
    (`build_ddl.go:1685`), so `(54,1)` — never a re-derived
    `(53,0)`. (ii) THE REFRESH SKIP COMPARES FILLED AGAINST ORIGINAL
    QUERY OUTPUT, NEVER THE STORED TAG: the pre-existing per-column
    `reflect.DeepEqual(source.Typ, originalColumns[i].Typ)` skip
    (base `build_ddl.go:1627`–`:1628`) compares two forms of the
    SAME head expression's type, equal for every function-headed
    column on both engines (probe: the filled-against-original pair
    equal; the refresh leaving the snapshot tag `(54,1)` with the
    retained carrier AND with a nil carrier alike) — the
    execute-route boundary evaluation is unreachable for this
    feature class BY CONSTRUCTION: only a bare-parameter output
    column changes type on filling, and a bare parameter carries no
    widened-multiplication provenance **(OVERGENERALIZED — a
    provisional common-VALUE arm changes type on filling too,
    defeating the skip for its output column; corrected by
    decision log entry 68 (ii)–(iii); kept as history**).
    (iii) ORACLES CORRECTED, THE
    CONTRACT UNCHANGED: PREPARE stores the parent's envelope-derived
    snapshot tag `(54,1)` against the widened head `(65,1)` (parent
    snapshot `(54,1)`/`(54,1)`) — the genuine consumption
    discrimination, verified by the round's ledger probe (the
    coalesce decision entry pre `[(64,0),(39,1)]`, aligned
    `[(65,1),(39,1)]`, result `(65,1)`, provenance true; direct
    evaluation of the snapshot head against the claimed ledger
    re-derives `(54,1)`); EVERY execute route persists `(54,1)` on
    both engines — cached-plan executes reuse the snapshot tag
    through the skip, protocol-floor rebuilds derive `(54,1)` from
    their own generation's live registry at binding — exactly
    the parent's own execute behavior; the EMPTY-LEDGER ablation
    becomes a LABELED HARNESS discrimination, bypassing the
    unreachable production skip — the filled execute-shape tree
    evaluated DIRECTLY through the storage-boundary evaluator
    re-derives `(54,1)` with retained evidence and `(65,1)` without
    it — the fixture's complete filled cast tree carrying exactly
    ONE column-only alignment cast, the inner TRUNCATE arm into
    `(65,1)`, `d` keeping its declared `(39,1)` under the binder's
    same-OID same-scale cast skip (`base_binder.go:9002`–`:9003`),
    entry 66's both-arms description corrected accordingly — the
    unverified ≤65 arm cast keeping its declared envelope,
    decision log 62's conservative branch, a nil ledger rendering
    every ownership question false (`decisionOwnedCastOperand`,
    `build_ddl.go:2025`–`:2027`),
    verified by the same probe, still genuinely different from
    consumption and establishing that any future reachable
    execute-locus evaluation yields the parent's value WITH evidence
    and the conservative envelope WITHOUT; REFRESH-NEVER-RAN remains
    the snapshot tag `(54,1)` — equal to consumption IN VALUE on
    this fixture, distinguished from it only by the harness's typed
    probe assertion that the evaluation ran and consumed, recorded
    as such rather than by the tag alone. (iv) CONTROLS CORRECTED:
    the authored explicit-cast control
    `TRUNCATE(COALESCE(TRUNCATE(a*b, 0), CAST(d AS DECIMAL(65,1))),
    ?)` stores `(65,1)` at PREPARE and persists `(65,1)` at EXECUTE
    on BOTH engines — head equal to tag, the skip leaving it
    untouched, entry 66 (iii)'s re-derived `(64,0)` not existing for
    the (i)-(ii) reasons; the no-parameter control
    `TRUNCATE(COALESCE(TRUNCATE(a*b, 0), d), 0)` refines at PREPARE
    itself and persists the parent's `(53,0)` from prepare-time
    evaluation alone, refresh never reached (`numParams > 0`,
    `computation_wrapper.go:1921`) — unchanged and genuine. (v)
    WITNESSES A/B VALUE ORACLES CORRECTED: Witness A keeps its
    formation probe, per-execute rebuild assertions, claim ordering
    and carrier replacement — each execute's rebuild generation
    re-recording the coalesce decision and consuming its OWN live
    registry at binding — the persisting value corrected to the
    parent's `(54,1)`; Witness B keeps its preconditions-first
    formation, no-rebuild probe, nonce identity and controlled
    single-invalidation rebuild-and-replace, its second-EXECUTE
    assertion honestly re-described: the PREPARE-claimed carrier is
    RETAINED untouched with no fresh claim and the PREPARE-derived
    `(54,1)` is reused through the skip — no execute-route
    evaluation consuming retained evidence for this feature class.
    (vi) RETAINED-CARRIER DISPOSITION, HONESTLY SCOPED: the carrier
    ships as read-only conservative insurance at zero observable
    behavior cost, its release ownership and ordering per entries
    64–65 unchanged; it has NO reachable widened-decimal
    consumption trigger in revision 21's reviewed routes (ii), and
    its removal was considered and rejected **(superseded —
    entry 68 (iv) gives the carrier a genuine consumer as the
    read-only base of the merged execute-evidence argument, its
    insurance role otherwise standing; kept as history**) — a
    larger production surgery
    gaining no behavior and losing the conservative with-evidence
    answer if any future planner route ever evaluates a
    widened-decimal storage boundary at an execute locus; its
    mechanics are witnessed by the claim/nonce/lifetime UT cells
    standing from entries 64–65. The evaluator's digits handling
    is a recorded consistency property, never an observable oracle
    while (ii)'s skip stands: `decimalConstIntArg`
    (`build_ddl.go:2124`–`:2129`) reads bare literals only, the
    fill's canonical `CAST(literal)` digits staying unread —
    exactly the bind-time semantics, digits unbound at PREPARE
    staying unknown at any hypothetical execute-locus evaluation.
    (vii) PRODUCTION-SET ACCOUNTING: no production file joins or
    leaves the planned set — still 20 production files
    **(superseded by entry 68 (v): `pkg/sql/plan/visit_plan_rule.go`
    joins — 21 production files; kept as history**); revision
    21 changes §9 fixtures/oracles/witnesses, §10's resolved
    narrative, entry 66's supersession annotations and the
    public-contract projection's prepared-CTAS sentence only, the
    pre-existing CTAS rebuild policy of entry 66 (iv) standing
    verbatim.

68. The provisional common-VALUE execute rebind channel and its
    decision-evidence lifecycle (revision 22,
    F-VALUE-ARM-REBIND): entry 67 generalized its digits-marker
    fixture's skip conclusion and missed the reachable
    execute-time rebind; the review-finding verification
    confirmed every citation at code, and an instrumented
    planner probe (harness deleted after capture) fixed the
    observable ground truth. (i) THE REBIND CHANNEL EXISTS AND
    CHANGES OUTPUT TYPES: the DDL fill wraps the query with
    `replaceParamValsWithSelection` (`utils.go:4752`–`:4760`),
    installing `NewResetParamRefRule` (`utils.go:6678`) whose
    expression walk (`utils.go:6766`) replaces provisional
    common-VALUE operand casts (`visit_plan_rule.go:3320`–
    `:3346`, `provisionalCommonValueCast`, setting
    `needResetFunction`) and rebinds the enclosing functions
    through `bindPreparedFuncExprImplByPlanExpr`
    (`visit_plan_rule.go:3861`–`:3879`) with a NIL process
    (`base_binder.go:7448`–`:7449`), so no rebound coercive
    decision is recorded — the unchanged pre-existing test
    `TestPrepareCoalesceNumericHintPreservesRuntimeSource`
    (`prepared_common_value_test.go:68`–`:110`) demonstrating
    the rebind changing output types (decimal<->varchar) — and
    ledger keys are content-keyed per binding
    (`constant_fold.go:1407`–`:1429`), so re-derived decisions
    never match PREPARE-retained keys. (ii) OBSERVED GROUND
    TRUTH: the value-arm fixture `PREPARE p FROM 'CREATE TABLE
    t3 AS SELECT COALESCE(TRUNCATE(a*b, 0), ?) AS v FROM t'`
    over `(38,10)` operands snapshots `VARCHAR(65535)` on BOTH
    engines — the pre-existing peer-resolution provisional
    behavior wrapping both arms in varchar casts,
    engine-independent — retaining 3 ledger keys. EXECUTE
    `?=1.25` typed `DECIMAL(20,6)` specializes (filled head
    `(34,70,6)`), DEFEATS the skip (`reflect.DeepEqual` false;
    the filled key
    `420ac26p#2|34:64:0>34:70:6|33:20:6>34:70:6` absent from the
    retained carrier) and the refresh persists `(34,59,6)` with
    the retained carrier AND with nil alike — matching the
    parent only through §4.1's >65 look-through of the
    `(70,6)` alignment cast. EXECUTE `?=0.5` typed
    `DECIMAL(20,1)` specializes (filled head `(34,65,1)`),
    defeats the skip, misses the carrier and the refresh
    persists `(34,65,1)` with either ledger — DIVERGING from
    the parent, whose own per-execute refresh derives
    `(34,54,1)`: missing evidence has teeth at the `(65,1)`
    cast's unverified <=65 target (decision log 62's
    conservative branch). Independent parent oracles were
    captured through the cast-pinned prepared fixture
    `COALESCE(TRUNCATE(CAST(a*b AS DECIMAL(65,12)), 0), ?)`:
    `(34,59,6)` and `(34,54,1)` — the parent's own tags for
    the same bindings, carrier-independent. Binary-NULL and
    text bindings do not specialize, fire the skip and persist
    the snapshot tag on both engines. (iii) SCOPED CONCLUSION:
    the skip conclusion is restricted to fills leaving every
    output column's inferred type unchanged — entry 67's
    digits-marker fixture among them, retained as the skip
    control; wherever a provisional common-VALUE arm changes an
    output column's type, the execute-route boundary
    evaluation runs and REQUIRES decision evidence matching the
    rebound types. (iv) THE LIFECYCLE — EXECUTE-SCOPE
    RECORDING MERGED OVER THE RETAINED CARRIER:
    `ResetParamRefRule` gains the execute scope's
    `*process.Process` field, set at its `utils.go:6678`
    construction; `bindPreparedFuncExprImplByPlanExpr` passes
    it instead of nil (`base_binder.go:7448`–`:7449`), so
    the PRE-EXISTING recording hook (`base_binder.go:8295`–
    `:8299`, its nil-process skip documented at `:8285`–
    `:8287`) records each rebound coercive decision
    content-keyed by its rebound binding into an execute-scope
    registry installed around the fill-and-refresh window with
    the entry-58 enter/release discipline (defer-restored on
    success, error and panic unwind). The refresh argument at
    the wrapper (`computation_wrapper.go:1950` and the sibling
    routes `:436`/`:3089`) is the single merged ledger:
    retained-carrier entries OVERLAIN BY the execute registry's
    entries, the carrier read-only and content-key collisions
    resolving to the freshest (execute) binding; the hook
    records only widened-provenance decisions, so a
    provenance-free statement has an empty execute registry,
    the merged ledger equals the carrier and its plan stays
    byte-identical; a nil process (test mocks) stays the
    pre-existing no-op. With the entry recorded, the
    coalesce's `(65,1)` alignment cast is decision-VERIFIED,
    the evaluator looking through to the operand envelopes —
    anchored truncate refinement `(53,0)` and the declared
    value arm `(20,1)` — deriving the parent's `(54,1)`; the
    `(70,6)` case derives `(59,6)` through the same evidence
    (identical to today's >65 recovery, now evidence-driven).
    Missing evidence on a statement carrying widened
    provenance stays the decision-log-62 conservative branch
    (the unverified <=65 cast keeping its declared envelope —
    the observed `(65,1)` ablation), never a silence.
    **(This lifecycle completes the refresh boundary's
    merged-evidence argument but, as written, left the EARLIER
    fill-time rebind's retry without producer lineage — a
    rebound merge consumer referencing a remapped aggregate,
    window or relational producer reference reaching the funnel
    with neither binder context nor channel entry, the declined
    retry propagating the merge error before the refresh could
    run; corrected by decision log entry 69 (the plan-producer
    lineage view at (iv) there); kept as history**) (v)
    PRODUCTION-SET ACCOUNTING: `pkg/sql/plan/visit_plan_rule.go`
    joins the planned production set (the rule field and its
    construction wiring); `pkg/sql/plan/utils.go`,
    `pkg/sql/plan/base_binder.go` and
    `pkg/frontend/computation_wrapper.go` were already owned —
    NOW 21 production files, every earlier 20-file tally in
    this document superseded by this paragraph; the entry-66
    (iv) CTAS rebuild policy stands verbatim, Witnesses A/B
    keep their formations, and the carrier lifecycle of
    entries 64–65 is unchanged apart from the merged read
    defined in (iv). (vi) VALIDATION PLAN: the §9 I10 (xv)
    cell — the entry-path witness driving production
    PREPARE -> specialized EXECUTE -> refresh asserting the
    persisted tags `(59,6)` and `(54,1)` on the two bindings,
    the skip defeated (`reflect.DeepEqual` false), the
    execute registry's formed coalesce/truncate entries and
    the merged read consuming them; the independent parent
    metadata oracles through the cast-pinned prepared channel
    (`(59,6)`/`(54,1)`, carrier-independent by probe); the
    evidence-consumption/missing-evidence assertions — with
    execute recordings withheld, `0.5` deriving `(65,1)`
    (today's observed divergence), with them `(54,1)`; `1.25`
    deriving `(59,6)` under both, documenting the >65
    look-through; binary-NULL/text skip controls persisting
    the snapshot tag on both engines; Witness A extended to
    assert the formed execute-scope entries on each
    per-execute rebuild route, Witness B to assert the
    cached-plan second EXECUTE forming its own execute-scope
    entries with no fresh claim; cell (xiv)'s digits-marker
    fixture standing unchanged as the skip control; the BVT
    executing the value-arm prepared CTAS on the live owned
    isolated service for both bindings and asserting the
    persisted declared tags through the catalog surfaces.
    (vii) PUBLIC CONTRACT: the projection's prepared-CTAS
    sentences gain the per-binding parent-parity contract
    (`(59,6)`/`(54,1)` on both engines, derived with
    execute-scope evidence, parent-identical), the provisional
    `VARCHAR(65535)` snapshot recorded as engine-independent
    and the revision citations updated; no other contract
    statement changes.

69. The execute-time rebind's producer-domain lineage (revision 23,
    F-SUPPLY-PRODUCER-DOMAINS): entry 68 (iv) installed the rebound
    decision-evidence lifecycle at the refresh boundary but left the
    EARLIER fill-time rebind without producer lineage; the twenty-second
    review's finding was verified at code, and an instrumented planner
    probe (harness deleted after capture) fixed the observable ground
    truth. (i) THE GAP — REBIND-TIME RETRY WITHOUT LINEAGE: an aggregate
    pull-up re-expresses a consumer's aggregate argument as a column
    reference — `MAX(a*b)` becomes `Expr_Col{RelPos: -2, ColPos: aggIdx +
    groupSize}` in the AGG node's own ProjectList
    (`query_builder.go:2332`–`:2344`) whose carried type is the
    aggregate's published type (MAX's `ReturnFirstArgType`,
    `list_agg.go:110`–`:124`) — so on the widened engine the execute-time
    rebind of the enclosing COALESCE sees a `(76,0)` operand whose family
    common with a `DECIMAL(20,1)` binding needs 77 digits: the first
    lookup fails with `invalid argument function coalesce, bad value
    [DECIMAL256 DECIMAL128]` (probe-observed); the retry funnel
    (`base_binder.go:9648`–`:9674`) then demands a resolved
    parent-published domain per operand with at least one proven
    (`:9650`–`:9659`), and the operand resolver's ColRef case
    (`base_binder.go:9714`–`:9778`) resolves only through the binder's
    BindContext tag/ordinal inventories or the `b#<tag>#<pos>` producer
    channel (the key helper `decimalBoundaryBindingChannelKey`,
    `base_binder.go:9680`), its surface fallback returning prov=false —
    neither channel reaching the execute scope: the scope attaches only at
    binder-backed binding (`withDecimalBoundaryBindScope`,
    `base_binder.go:9496`; struct `:9480`–`:9483`) and
    `ResetParamRefRule`'s rebind runs binderless. The retry therefore
    declines, the original bind failure propagates at the fill, and
    `RefreshPreparedCTASInferredColumns` never runs — entry 68 (iv)'s
    merged evidence arriving after the failure. (ii) OBSERVED GROUND
    TRUTH: the reviewer counterexample `PREPARE p FROM 'CREATE TABLE t3 AS
    SELECT CAST(COALESCE(MAX(a*b), ?) AS DECIMAL(65,1)) AS v FROM t'` over
    `DECIMAL(38,0)` operands holding 1, EXECUTE `?=0.5` typed
    `DECIMAL(20,1)`: PREPARE snapshots head=tag=`(34,65,1)` on BOTH
    engines (the authored CAST pins them), retaining 3 content keys —
    coalesce `420ac26p#2|34:76:0>34:76:0|71:0:0>34:76:0`, MAX
    `7ib8fim8#1|34:76:0>34:76:0`, multiply
    `nodcnpc#2|33:38:0>34:38:0|33:38:0>34:38:0` — with no b#-channel keys
    and both snapshot merge arms the provisional `f:cast(61,65535,0)`; the
    fill raises exactly the reviewer error inside the fill, pre-refresh.
    The authored-cast pinned control `MAX(CAST(a*b AS DECIMAL(65,0)))`
    retains `7ib8fim8#1|34:76:0>34:65:0`, fills OK with head `(34,65,1)`
    and refreshes tag `(34,65,1)` — pinned lineage already works today.
    The direct-expression control `COALESCE(a*b, ?)` retains 2 keys and
    fails with the IDENTICAL fill error: today's route fails both shapes,
    and entry 68's process threading repairs only the direct shape's
    operand-ledger channel — the aggregate-producer shape additionally
    needs this entry's lineage view. (iii) THE PARENT ORACLE: parent `a*b`
    publishes `(65,0)`; MAX derives `ReturnFirstArgType` `(65,0)`; the
    coalesce family common of `[(65,0),(20,1)]` is `(66,1)`; the authored
    CAST pins `(65,1)` at PREPARE, so the filled head never changes, the
    per-column skip fires at the refresh and the snapshot tag `(65,1)`
    persists with value 1.0 — the observable parent execute is statement
    success + `(65,1)` + 1.0 on every route. (iv) THE REPAIR — A
    PLAN-PRODUCER LINEAGE VIEW REUSING THE EXISTING ENVELOPE OWNER: the
    resolution scope gains one field beside bctx and builder
    (`decimalBoundaryScope`, `base_binder.go:9480`–`:9483`) — a
    plan-producer lineage view attached around the same fill-and-refresh
    window entry 68 (iv) installs its registry at, the attach riding the
    same rule construction (`utils.go:6678`) — mapping each consumer-side
    `(nodeID, relPos, colPos)` triple to the retained producing EXPRESSION
    by the EXACT per-node indexing the projection-refresh envelope owner
    already owns: `resolveColumnType`, the per-node closure at
    `utils.go:8161`–`:8232` inside `refreshPreparedPlanProjectionTypes`
    (`:8102`), fed by the walk's `childProjectLists` (`:8143`–`:8146`) and
    `childProjectSize` (`:8157`–`:8159`) — AGG RelPos -1 ->
    `GroupBy[colPos]`, RelPos -2 -> `AggList[colPos - len(GroupBy)]`;
    WINDOW RelPos -1 -> `WinSpecList[colPos - childProjectSize]`;
    JOIN/APPLY and the generic fallback ->
    `childProjectLists[relPos][colPos]`; the set-op families through the
    same branch lists `preparedSetOperationOutputType` consumes.
    **(That closure's inventories index PHYSICAL children only
    — `childProjectLists` is built from `node.Children`
    (`utils.go:8143`–`:8146`) with no `Node_SINK_SCAN` arm
    — so a consumer reference remapped across a MATERIALIZED
    shared-CTE reader edge (`SourceStep`, no children, positional
    outputs) still lost lineage at the scan; producer resolution
    delegated to the complete storage-domain owner by decision log
    entry 70 (iv) — the funnel behavior, read-only discipline
    and decline contracts stated here stand; kept as history**)
    The
    operand resolver's ColRef case gains ONE step before its surface
    fallback: when the binder context and the b#-channel both miss, the
    triple resolves through the view (on the counterexample two hops — the
    consumer's ProjectList slot is itself `Expr_Col{RelPos: -2}`,
    re-entering the view scoped to the AGG child to reach `AggList[0]`,
    the MAX call) and the recovered producing expression recurses through
    `decimalBoundaryAggregateParentDomain` (`base_binder.go:9842`) /
    `decimalBoundaryCallParentDomain` (`:9881`): MAX's argument `a*b`
    recursing to the multiply's parent `(65,0)` with provenance, the retry
    rebinding the parent's `(66,1)`; a resolved expression that is itself
    a producer-relative ColRef re-enters the view scoped to the producing
    child under a depth guard; a triple outside the view returns ok=false,
    the funnel declining exactly as today — never a fabricated lineage.
    The view reads plan expressions only: the plan, the retained carrier
    and the statement registry stay READ-ONLY through resolution, and a
    provenance-free statement forms no view entries, its plan
    byte-identical. (v) EXECUTE-ROUTE SEQUENCING: the scope carrying the
    lineage view installs before the fill and releases after the refresh
    on all three execute routes — the prepared-expression retry route fill
    `computation_wrapper.go:431` / refresh `:443`, the cached-plan route
    fill `:1942` / refresh `:1950` (forwarding
    `prepareStmt.retainedDecimalBoundaryLedger` read-only), the
    prepared-retry route fill `:3109` / refresh `:3120` — under the
    entry-58 enter/release discipline (defer-restored on success, error
    and panic unwind); these freshly verified worktree lines supersede
    entry 68's stale sibling citations `:436`/`:3089` (shifted by the
    preserved implementation). **(That enumeration covers the FRONTEND
    fill windows only — a prepared CREATE TABLE AS SELECT runs a SECOND,
    independent execute-time fill inside the internal executor's
    follow-up reparse, which filled process-less until revision 42;
    decision log entry 88 installs this same discipline on that fourth
    window. Kept as history**)
    (vi) VALIDATION PLAN: the §9 I10 (xvi) cell
    — the reviewer counterexample as the entry-path witness asserting
    statement success, the persisted tag `(65,1)` (parent-pinned, the skip
    firing) and the value 1.0 on both engines, and the retried coalesce
    decision recorded content-keyed by its rebound types (parents
    `[(65,0),(20,1)]`, aligned `(66,1)`) in the execute-scope registry;
    the independent parent oracle of (iii) asserted per-execute against
    the parent's own engine; the authored-cast pinned control succeeding
    with and without the view; the direct-expression control of cell (xv)
    standing with the view installed; the genuine-wide control — a
    producer whose parent-published domain is itself wide (a merge over a
    genuine 66-digit literal arm) — retrying to the parent's own
    convergence; the lineage-withheld ablation reproducing the probe's
    exact fill error, proving the view load-bearing; repeated bindings
    through the cached route (Witness B extension — second EXECUTE, no
    fresh claim, its own execute-scope entries) and both retry routes
    (Witness A extension); the BVT exercising the counterexample on the
    owned isolated service with catalog-surface persisted-tag assertions
    and verified teardown. (vii) PRODUCTION-SET ACCOUNTING AND PUBLIC
    CONTRACT: every seam lands inside the already-owned
    `pkg/sql/plan/utils.go`, `pkg/sql/plan/base_binder.go` and
    `pkg/frontend/computation_wrapper.go` — the planned production set
    stays 21 production files **(superseded in revision 42:
    `pkg/sql/compile/sql_executor.go` joins the set, 22 production
    files — decision log entry 88 (v); kept as history**); the public
    contract's prepared-CTAS
    semantics gain the aggregate-producer parity clause (the
    counterexample's per-binding parent parity with the lineage view, the
    pre-existing merge error surfacing with lineage withheld), no other
    contract statement changed.

70.  Producer lineage across materialized shared-CTE SourceStep edges, and
    the VALUE-arm projection restoration (revision 24,
    F-SOURCESTEP-LINEAGE and F-VALUE-ARM-PROJECTION): the twenty-third
    review's two findings were verified at code, and an instrumented
    planner probe (harness deleted after capture) fixed the observable
    ground truth. (i) THE GAP — THE VIEW CLOSURE INCOMPLETE AT SOURCE-STEP
    EDGES: entry 69 (iv)'s view maps consumer-side triples through
    `resolveColumnType`'s per-node indexing (`utils.go:8161`–`:8235`),
    whose `childProjectLists` feed is built from `node.Children` ONLY
    (`utils.go:8143`–`:8146`) and whose case table carries no
    `Node_SINK_SCAN` arm; a materialized shared-CTE consumer is a
    `Node_SINK_SCAN` holding `SourceStep` with NO children
    (`appendSharedCTEScan`, `cte_reuse.go:1871`–`:1909`; identity-reader
    path `cte_identity.go:146`), its outputs published positionally
    (`Expr_Col{RelPos: 0, ColPos: i}`, `query_builder.go:3212`–`:3253`),
    so entry 69's closure cannot follow its producing subtree and a
    prepared merge consumer over such a producer reaches the retry funnel
    lineage-blind exactly as in revision 22 — while the existing
    storage-domain producer resolver already traverses these edges for the
    storage boundary (`resolveChildRelationalOutput`,
    `build_ddl.go:2173`–`:2395`; the
    SINK_SCAN/RECURSIVE_SCAN/RECURSIVE_CTE case `:2296`–`:2338` following
    `stepProducer`, `:1796`–`:1811`, with path-bounded cycle handling
    `d.sources` at `:2319`–`:2328`), and §4.1's edge inventory explicitly
    requires them. (ii) OBSERVED GROUND TRUTH: the probe counterexample
    `PREPARE p FROM 'CREATE TABLE t5 AS WITH c AS (SELECT a*b AS v, uuid()
    AS r FROM t) SELECT COALESCE(x.v, ?) AS w FROM c x JOIN c y ON x.v =
    y.v'` over `DECIMAL(38,0)` operands holding 1, EXECUTE `?=0.5` typed
    `DECIMAL(20,1)` — the value-unstable export forcing publication
    through `classifyCTEEvaluation` (`cte_identity.go:190`–`:245`) where
    natural self-join, three-way and rollup CTE shapes stay inlined under
    this harness (the reuse gates `cte_reuse.go:105`–`:185` declining:
    drain witness, positive finite cost, storage fit and profitability):
    plan `Steps` carries exactly 2 entries (the SINK producer node, then
    the consumer root), the plan holding TWO `Node_SINK_SCAN` readers with
    `SourceStep=[0]` and empty Children, the exported chain head coalesce
    -> `col(0,0)` into the JOIN slot -> `col(0,0)` into the SINK_SCAN
    slot, retained content keys multiply
    `nodcnpc#2|33:38:0>34:38:0|33:38:0>34:38:0` and coalesce
    `420ac26p#2|34:76:0>34:76:0|71:0:0>34:76:0`, snapshot tag the
    provisional `VARCHAR(65535)` `(61:65535:0)`; the fill raises exactly
    `invalid argument function coalesce, bad value [DECIMAL256
    DECIMAL128]` pre-refresh. The pinned twin exporting `CAST(a*b AS
    DECIMAL(65,0))` under the IDENTICAL materialized topology retains the
    `(65,0)`-armed coalesce key, fills OK with head
    `f:coalesce#2(34:66:1)` and refreshes tag `(34,66,1)` with the
    retained AND nil ledgers alike — the carrier-independent parent
    channel under this topology, working today. The VALUE-arm finding
    verified as a projection-only regression: §9 I10 (xv) and entry 68
    (vi)–(vii) stand intact in this document with both per-binding
    oracles, the provisional snapshot and the binary-NULL/text controls;
    the round-37 handler projection had replaced the paragraph instead of
    appending. (iii) THE PARENT ORACLE: the parent's exported `v`
    publishes `(65,0)`; the coalesce family common of `[(65,0),(20,1)]` is
    `(66,1)`; the refresh derives the tag from the merged evidence — the
    observable parent execute is statement success + persisted tag
    `(66,1)` + value 1.0 on every route; with lineage withheld the widened
    `(76,0)` operand forces a 77-digit common and the pre-existing checked
    merge-bind error surfaces at the fill. (iv) THE REPAIR — DELEGATION TO
    THE COMPLETE PRODUCER-RESOLUTION OWNER: the view's producer resolution
    moves off entry 69 (iv)'s closure onto the existing storage-domain
    resolver — the scope's lineage field becomes a read-only adapter
    constructed once per fill-and-refresh window over the filled plan's
    query through the SAME constructor the storage boundary uses
    (`newCtasStorageDomain`, `build_ddl.go:1770`–`:1782`), resolving each
    consumer-side `(nodeID, relPos, colPos)` triple by delegating to
    `resolveRelationalOutput`/`resolveChildRelationalOutput`
    (`build_ddl.go:2150`/`:2173`), whose pre-existing case table covers
    the COMPLETE edge set: identity mirrors
    PROJECT/SORT/PARTITION/FILTER/FILL (`:2175`–`:2187`), the AGG
    inventories with positional GroupBy-then-AggList fallback
    (`:2188`–`:2201`), SAMPLE slots (`:2202`–`:2236`), WINDOW
    self-reference and passthrough (`:2237`–`:2257`), TIME_WINDOW slots
    (`:2258`–`:2295`), the SourceStep case (`:2296`–`:2338`), JOIN
    side-relative slots (`:2339`–`:2367`) and set-operation branch lists
    (`:2368`–`:2395`) — with `d.sources` path membership bounding cycles
    (`:2319`–`:2328`, membership removed after the descent so sibling
    paths still revisit) and `d.active` guarding expression revisits
    (`:1843`–`:1851`); an unresolved, out-of-case or multi-producer result
    returns ok=false, the funnel declining exactly as today — never a
    fabricated lineage — while a single resolved producing expression
    recurses through
    `decimalBoundaryAggregateParentDomain`/`decimalBoundaryCallParentDomain`
    precisely as entry 69 (iv) specified, and a resolved producer-relative
    ColRef re-enters the delegation scoped to the producing child under
    the same depth guard. On the probe chain the delegation walks head
    coalesce -> JOIN slot -> SINK_SCAN slot -> SourceStep -> SINK child ->
    PROJECT mirror -> the retained multiply — the hop entry 69's closure
    could not take — reaching the parent `(65,0)` with provenance so the
    retry binds `(66,1)`; the view keeps its read-only discipline (the
    domain's node/step indexes are window-local maps; the plan, the
    retained carrier and the statement registry stay READ-ONLY), and a
    provenance-free statement constructs no adapter, its plan
    byte-identical.
    **(The multi-producer arm of THIS entry's decline clause is
    REVISED by decision log 71 (iii): the delegated resolver's
    set-operation case returns all branch contributors by design
    (`build_ddl.go:2367`–`:2383`), so a valid multi-producer
    resolution is the parent's own branch inventory, not an
    unresolvable shape — decline stands only for out-of-case
    and genuinely unresolved resolutions; kept as history)**
    (v) EXECUTE-ROUTE SEQUENCING: unchanged from entry 69
    (v) — the scope installs before the fill and releases after the
    refresh on the same three freshly verified routes
    (`computation_wrapper.go:431`/`:443`, `:1942`/`:1950`,
    `:3109`/`:3120`) under the entry-58 enter/release discipline; the
    adapter is constructed at scope install and discarded with it, and
    `refreshPreparedPlanProjectionTypes`' own projection-refresh walk is
    untouched — the view no longer borrows its indexing. (vi) VALIDATION
    PLAN: the §9 I10 (xvii) cell — the materialized counterexample
    asserting topology FIRST (two `Node_SINK_SCAN` with `SourceStep` and
    no children, the two-step query, positional exports, provisional
    snapshot tag, retained keys), then statement success with persisted
    tag `(66,1)` and value 1.0 on both engines through the cached route
    (Witness B extension — second EXECUTE, no fresh claim, its own
    execute-scope entries) and both retry routes (Witness A extension),
    the retried coalesce recorded content-keyed `[(65,0),(20,1)]` ->
    `(66,1)`; the independent parent engine oracle asserted per-execute;
    the pinned materialized twin asserted (fill OK, refresh `(66,1)` on
    both ledger channels — true today); the lineage-withheld ablation
    reproducing the probe's exact fill error; the recursive bounded-cycle
    control (a RECURSIVE CTE exporting a widened product with fitting
    values succeeding, the path membership preventing outward-scan
    cycling); cells (xv) and (xvi) asserted with the delegation installed;
    the BVT exercising the materialized counterexample on the owned
    isolated service with catalog-surface persisted-tag assertions and
    verified teardown; the Challenger projection re-verified byte-for-byte
    against cells (xv) AND (xvi) before submission. (vii) PRODUCTION-SET
    ACCOUNTING AND PUBLIC CONTRACT: the delegation's adapter lands inside
    the already-owned `pkg/sql/plan/build_ddl.go`, the scope field stays
    inside the already-owned `pkg/sql/plan/base_binder.go`, the route
    sequencing stays inside `pkg/frontend/computation_wrapper.go` — the
    planned production set stays 21 production files; the
    `resolveColumnType` closure of entry 69 (iv) is RETRACTED as the
    resolution substrate (its paragraph annotated kept-as-history, its
    funnel/read-only/decline contracts standing); the public contract's
    prepared-CTAS semantics gain the materialized shared-CTE parity clause
    (statement success, persisted tag `(66,1)`, value 1.0 per binding, the
    pre-existing merge error surfacing with lineage withheld) and restore
    the VALUE-arm paragraph verbatim alongside the aggregate-producer
    paragraph, no other contract statement changed. (viii) THE PROJECTION
    RESTORATION: the Challenger projection rebuilds the prepared-CTAS
    semantics as the VALUE-arm paragraph VERBATIM restored (the
    `DECIMAL(20,6)`/1.25 -> `(59,6)` and `DECIMAL(20,1)`/0.5 -> `(54,1)`
    persisted-tag oracles, the provisional `VARCHAR(65535)` snapshot, the
    binary-NULL and text skip controls, the missing-evidence `(65,1)`
    ablation), followed by the revision-23 aggregate-producer paragraph
    and the revision-24 materialized shared-CTE clause, the whole
    projection re-checked under the 64000-byte budget.

71.  Branch-wise parent-domain resolution across set-operation producers,
    and the projection's schema-width alignment (revision 25,
    F-MULTIPRODUCER-DOMAINS and F-SCHEMA-WIDTH-PROJECTION): the
    twenty-fourth review's two findings were verified at code, and an
    instrumented planner probe (harness deleted after capture) fixed the
    observable ground truth. (i) THE GAP — THE DELEGATION DECLINES THE
    RESOLVER'S OWN SET-OPERATION CONTRIBUTORS: entry 70 (iv)'s repair
    states that an unresolved, out-of-case or multi-producer result
    returns ok=false, the funnel declining exactly as today — yet the
    delegated resolver's set-operation case returns ALL branch
    contributors DELIBERATELY (`build_ddl.go:2367`–`:2383`, each branch's
    `ProjectList[idx]` resolved through `resolveRelationalOutput`, an
    unresolvable branch keeping the authored type): a prepared merge
    consumer over a set operation of two widened-product branches resolves
    to TWO producers, so revision 24's funnel still declined exactly where
    the parent has an unambiguous combined domain, and the single-producer
    materialized-CTE witness (xvii) could not detect the regression. (ii)
    OBSERVED GROUND TRUTH: the reviewer counterexample `PREPARE p FROM
    'CREATE TABLE t6 AS SELECT COALESCE(u.v, ?) AS w FROM (SELECT a*b AS v
    FROM s1 UNION ALL SELECT a*b FROM s2) u'` over `DECIMAL(38,0)`
    operands holding 1, EXECUTE `?=0.5` typed `DECIMAL(20,1)`: the
    snapshot query holding ONE `Node_UNION_ALL` with two PROJECT branch
    children each exporting the widened multiply (`f:*#2(34:76:0)`), the
    union's published output slot `(34:76:0)` typed through the
    set-operation contract, the derived-table mirror feeding the head
    coalesce (`61:65535:0` provisional common), retained content keys
    coalesce `420ac26p#2|34:76:0>34:76:0|71:0:0>34:76:0` and ONE shared
    multiply key `nodcnpc#2|33:38:0>34:38:0|33:38:0>34:38:0` (the
    branches' multiplies content-identical), snapshot tag the provisional
    `VARCHAR(65535)` `(61:65535:0)`; the storage domain resolving the
    union's output slot 0 to BOTH branch producers (ok=true, count=2,
    owner nodes 1 and 3); the fill raising exactly `invalid argument
    function coalesce, bad value [DECIMAL256 DECIMAL128]` pre-refresh —
    the outcome revision 24's decline would still produce with its
    delegation installed. The pinned twin `CAST(a*b AS DECIMAL(65,0))` in
    BOTH branches under the IDENTICAL set-operation topology retains the
    `(65,0)`-armed coalesce key
    `420ac26p#2|34:65:0>34:65:0|71:0:0>34:65:0`, fills OK with head
    `f:coalesce#2(34:66:1)` and refreshes tag `(34,66,1)` with the
    retained AND nil ledgers alike — the carrier-independent parent
    channel under this topology, working today. (iii) THE REPAIR —
    BRANCH-WISE COMBINATION THROUGH THE EXISTING SET-OPERATION TYPING
    CONTRACT: the lineage view's resolution result gains ONE combination
    rule — when the delegated resolution returns MORE THAN ONE producing
    expression for a consumer-side triple (the set-operation case today),
    the view resolves EACH contributor's parent domain and provenance
    EXACTLY as entry 70 (iv) specified for a single producer (the operand
    recursion into
    `decimalBoundaryAggregateParentDomain`/`decimalBoundaryCallParentDomain`,
    resolved producer-relative ColRefs re-entering the delegation under
    the same depth guard), then combines the per-branch parent domains
    through the SAME typing contract the parent applies to that set
    operation's output column: `query_builder.go:4819`–`:4833` collects
    the branch `ProjectList[columnIdx]` types and resolves the
    coalesce-family common through `GetFunctionByName(ctx, "coalesce",
    ...)` with `ShouldDoImplicitTypeCast` (`argsCastType[0]`), the
    temporal-FSP and character-width refinements (`:4895`–`:4960`) and
    `setExpressionCollation` (`:4941`) standing untouched —
    `[(65,0),(65,0)]` -> `(65,0)`, so the retry binds the parent's
    `(66,1)` and the statement succeeds parent-identically. Decline
    (ok=false) is RESERVED for the genuinely unresolved cases: a branch
    whose producer expression cannot be resolved at all (the resolver's
    authored-type fallback), a contributor whose parent domain cannot be
    derived, or a combined common the carrier cannot hold. Bounded
    recursion and the read-only discipline stand unchanged (`d.sources`
    path membership `:2319`–`:2328`, `d.active` `:1843`–`:1851`; plan,
    retained carrier and statement registry READ-ONLY; a provenance-free
    statement's plan byte-identical). On the probe chain the delegation
    walks head coalesce -> derived-table mirror -> UNION_ALL output slot
    -> per-branch PROJECT mirrors -> the two retained multiplies; each
    branch resolves to the parent `(65,0)` with provenance; the
    combination yields `(65,0)`; the retry binds `(66,1)`.
    **(The contributor-evaluation arm of THIS entry's repair is
    REVISED by decision log 72 (iii): a returned set-operation
    contributor can itself be the engine-inserted alignment cast
    (`query_builder.go:4968`–`:4996`), a fixed target to the
    recursion (`base_binder.go:9803`–`:9809`); the view first
    recovers the coercion's SOURCE through the existing ownership
    predicate (`utils.go:7512`–`:7529`) and derives THAT
    expression's parent domain — SQL-authored casts (recorded
    overload 1 after the prepared rebind, or syntax-flagged at
    authoring) stay fixed targets; kept as history)**
    (iv) GENERALITY: the rule applies uniformly to every variant in the case
    arm — UNION, UNION_ALL, INTERSECT, INTERSECT_ALL, MINUS, MINUS_ALL
    (`:2367`–`:2370`) — and composes with the hop chains of entries 69–70
    (a set operation nested under a JOIN, AGG, WINDOW or SINK_SCAN chain
    resolves branch-wise at the set-operation node and continues
    single-producer above it); mixed branch sets combine under the
    identical contract (a `(65,0)` widened-product branch plus a branch
    whose parent-published domain is genuinely wide through a merge over a
    genuine 66-digit literal arm combining to the parent's own combined
    output — the genuine-wide control); a branch set containing a
    genuinely unresolvable contributor declines with no fabricated
    lineage. *(The unresolvable-contributor premise is narrowed in revision 28, F-MOD-PARENT-DOMAIN, decision log 74 (i): with the `mod`/`%` rule the shared typing owners cover every decimal-producing supported consumer, so the decline stands only for evidence-reconstruction failure — withheld or corrupted lineage — never for an unsupported expression shape; kept as history.)* (v) EXECUTE-ROUTE SEQUENCING: unchanged from entries 69 (v)
    and 70 (v) — the scope installs before the fill and releases after the
    refresh on the same three routes (`computation_wrapper.go:431`/`:443`,
    `:1942`/`:1950`, `:3109`/`:3120`) under the entry-58 enter/release
    discipline; the adapter is constructed at scope install and discarded
    with it. (vi) VALIDATION PLAN: the §9 I10 (xviii) cell — the
    set-operation counterexample asserting topology FIRST (one
    `Node_UNION_ALL`, the two PROJECT branch exports, the widened
    published union output, the derived-table mirror, the provisional
    snapshot tag, the retained keys), the storage-domain resolution
    asserting BOTH contributors (the pre-revision-25 decline premise),
    then statement success with persisted tag `(66,1)` and value 1.0 on
    both engines through the cached route (Witness B extension — second
    EXECUTE, no fresh claim, its own execute-scope entries) and both retry
    routes (Witness A extension), the retried coalesce recorded
    content-keyed `[(65,0),(20,1)]` -> `(66,1)`; the independent parent
    engine oracle asserted per-execute; the pinned set-operation twin
    asserted (fill OK, refresh `(66,1)` on both ledger channels — true
    today); the genuine-wide controls (the mixed-width branch set
    converging parent-identically; the unresolvable-branch set declining
    with the pre-existing checked error); the lineage-withheld ablation
    reproducing the probe's exact fill error; repeated bindings across the
    cached and retry routes; one representative non-UNION variant
    (INTERSECT) asserting the identical combination path; cells (xv),
    (xvi) and (xvii) asserted with the combination installed; the BVT
    exercising the set-operation counterexample on the owned isolated
    service with catalog-surface persisted-tag assertions and verified
    teardown; the Challenger projection re-verified byte-for-byte against
    cells (xv), (xvi) AND (xvii) before submission. (vii) PRODUCTION-SET
    ACCOUNTING: the combination rule lands inside the already-owned
    `pkg/sql/plan/build_ddl.go` lineage adapter and reuses the
    already-owned set-operation typing resolution in
    `pkg/sql/plan/query_builder.go` READ-ONLY — no new owner, no new file;
    the planned production set stays 21 production files. (viii) THE
    PROJECTION ALIGNMENT (F-SCHEMA-WIDTH-PROJECTION): the second finding
    verified the Challenger projection's universal 65-digit
    persisted-schema claims (`M <= 65 always` in the CTAS and view
    statements) contradict I2 (`:124`–`:134`), which preserves the
    identical non-carrier domains the parent's own inference persisted
    above 65, and the recorded `(66,1)` witnesses (the materialized-CTE
    parity clause, the pinned-twin refresh tag `34:66:1`). The projection
    is re-aligned to I2/I4 EXACTLY: declaration parsing still caps
    DDL-declared `DECIMAL(M,S)` at 65; the widening NEVER authors a
    persisted schema wider than the parent's own inference would have
    authored for the same statement (no widening-introduced expansion);
    the parent's genuinely wider INFERRED domains persist exactly as the
    parent's records persist them — the materialized-CTE and set-operation
    witnesses persisting `DECIMAL(66,1)` explicitly retained; the
    set-operation parity clause (statement success, persisted tag
    `(66,1)`, value 1.0 per binding, the pre-existing merge error
    surfacing with lineage withheld) is appended after the materialized
    shared-CTE clause; the whole projection re-checked under the
    64000-byte budget.

72.  Set-operation alignment-coercion source recovery in the lineage
    view (revision 26, F-SETOP-ALIGNMENT-RECOVERY): the twenty-fifth
    review's finding was verified at code, and an instrumented planner
    probe (harness deleted after capture; logs
    /tmp/30938_setalign_compile.log COMPILE-EXIT=0 and
    /tmp/30938_setalign_run.log RUN-EXIT=0 PASS) fixed the observable
    ground truth. (i) THE GAP — THE RECURSION RETAINS THE SET
    OPERATION'S OWN ALIGNMENT CAST: entry 71 (iii) resolves each
    returned contributor AS-IS, but where set-operation branch widths
    differ the planner inserts an engine-owned alignment cast around
    the narrower branch's export (`query_builder.go:4968`–`:4996`,
    `needsCast`; numeric through overload 0, character through
    overload 3); the delegated resolver returns that cast unchanged
    (`build_ddl.go:2381`), and the operand recursion's direct-cast arm
    treats it as a FIXED target (`base_binder.go:9803`–`:9809`), so a
    prepared merge consumer over a UNION ALL of unequal widened-product
    branches combines the retained `(76,0)` target, needs a 77-digit
    family common with the `DECIMAL(20,1)` binding and still fails —
    and the equal-width witness (xviii) structurally cannot detect the
    regression (`needsCast` false: no cast exists). (ii) OBSERVED
    GROUND TRUTH: the reviewer counterexample generalized to `s1`
    `DECIMAL(38,0)` and `s2` `DECIMAL(35,0)` operands holding 1,
    EXECUTE `?=0.5` typed `DECIMAL(20,1)` — shape UM (`(38,0)`
    first, `(35,0)` second): ONE `Node_UNION_ALL` (children [1 3])
    publishing `col(0,0,34:76:0)`, branch exports `f:*#2(34:76:0)` and
    `f:cast#2(34:76:0)` wrapping `f:*#2(34:70:0)`; the guard-level dump
    of the cast-bearing export: PROJECT branch, branch-slot pointer
    identity, a plain two-argument `cast` with a `TargetType` second
    argument, not `SyntaxExplicitCast`, overload 0, published type
    equal to the slot target — the ownership predicate UNWRAPPING it
    to `f:*#2(34:70:0)`; DOMOUTS ok=true count=2 (the retained
    multiply `f:*#2(34:76:0)` and the returned cast); the fill raising
    exactly `invalid argument function coalesce, bad value [DECIMAL256
    DECIMAL128]` pre-refresh — the outcome revision 25 would still
    produce here. Shape UMR (branches reversed): the cast travels WITH
    the narrower branch, the ownership dump and unwrap identical, the
    same fill error — width-order invariant. Shape UMP (SQL-authored
    `CAST(a*b AS DECIMAL(65,0))` in BOTH branches): both branch
    exports carry RECORDED OVERLOAD 1 with the syntax flag cleared by
    the prepared rebind, the predicate's overload guard DECLINING them
    (unwrap=false), the fill succeeding TODAY with head
    `f:coalesce#2(34:66:1)` and refresh tag `(34,66,1)` on retained
    AND nil ledger channels — the carrier-independent parent channel
    under unequal operands, working today. (iii) THE REPAIR —
    RECOVER THE OWNED COERCION'S SOURCE BEFORE CONTRIBUTOR EVALUATION:
    the branch-wise combination rule (entry 71 (iii)) gains ONE
    preceding step — each contributor seated on a set-operation
    branch slot FIRST passes through the existing ownership predicate
    `unwrapPreparedSetOperationCoercion` (`utils.go:7512`–`:7529`),
    the engine's own prepared-route discriminator, already invoked by
    the engine before its own set-operation common resolution
    (`utils.go:7897` unwrap, the combination at `:7965`) — the
    unwrap-then-combine order this repair mirrors: an OWNED coercion
    (PROJECT branch, slot pointer identity, a plain two-argument
    `cast` with a `TargetType` second argument, not syntax-flagged,
    overload 0 or 3 — the engine-inserted alignment families;
    SQL-authored casts carry recorded overload 1 or the syntax flag
    and keep their fixed targets under the standing
    `base_binder.go:9803`–`:9809` arm; the cast's published type
    equal to the slot's set-operation target type) yields its SOURCE
    expression, which re-enters the contributor recursion exactly as
    entry 70 (iv) specified (the operand recursion into
    `decimalBoundaryAggregateParentDomain`/
    `decimalBoundaryCallParentDomain`, resolved ColRefs re-entering
    the delegation under the depth guard); an owned coercion whose
    SOURCE cannot be resolved declines the whole resolution
    (ok=false), decline RESERVED for the genuinely unresolved cases,
    the entry-71 reservations standing. On the probe chain each branch
    resolves to the parent `(65,0)` with provenance, the combination
    through the unchanged typing contract yields `(65,0)`, the retry
    binds the parent's `(66,1)` and the statement succeeds
    parent-identically. Bounded recursion (`d.sources`
    `:2319`–`:2328`, `d.active` `:1843`–`:1851`) and the read-only
    discipline stand unchanged. (iv) GENERALITY: the recovery applies
    uniformly to every variant in the case arm — UNION, UNION_ALL,
    INTERSECT, INTERSECT_ALL, MINUS, MINUS_ALL
    (`build_ddl.go:2367`–`:2370`), the same alignment loop serving
    them all — and composes with the entry 69/70/71 hop chains;
    equal-width branch sets gain no cast and resolve exactly as entry
    71 specified (cell (xviii) recovery-invariant); character-family
    coercions share the one predicate (overload 3). (v) EXECUTE-ROUTE
    SEQUENCING: unchanged from entries 69 (v), 70 (v) and 71 (v) —
    the scope installs before the fill and releases after the refresh
    on the same three routes (`computation_wrapper.go:431`/`:443`,
    `:1942`/`:1950`, `:3109`/`:3120`) under the entry-58 enter/release
    discipline. (vi) VALIDATION PLAN: the section-9 I10 (xix) cell —
    the unequal-width set-operation counterexample asserting topology
    FIRST (one `Node_UNION_ALL`, the unequal branch exports with the
    engine-owned cast identified, the THREE retained keys, the
    provisional snapshot tag), the storage-domain resolution asserting
    BOTH contributors with the cast-bearing one named, the ownership
    recovery asserted at guard level (unwrap fires on the engine's
    coercion, declines the authored control), the retry binding the
    parent's `(66,1)`, then statement success with persisted tag
    `(66,1)` and value 1.0 on both engines through the cached route
    (Witness B extension — second EXECUTE, no fresh claim, its own
    execute-scope entries formed) and both retry routes (Witness A
    extension), the retried coalesce recorded content-keyed
    `[(65,0),(20,1)]` -> `(66,1)`; the REVERSED branch-order twin
    asserted identically; the independent parent engine oracle
    asserted per-execute; the SQL-authored pinned control asserted
    (fill OK, refresh `(66,1)` on both channels — true today); the
    recovery-withheld ablation reproducing the probe's exact fill
    error in BOTH orders; the unresolvable-source decline control;
    cells (xv), (xvi), (xvii) AND (xviii) asserted with the recovery
    installed; the BVT exercising the unequal-width counterexample in
    BOTH branch orders on the owned isolated service with
    catalog-surface persisted-tag assertions and verified teardown;
    the Challenger projection re-verified byte-for-byte against the
    aligned cells before submission. (vii) PRODUCTION-SET ACCOUNTING:
    the recovery lands inside the already-owned
    `pkg/sql/plan/build_ddl.go` lineage adapter and REUSES the
    already-owned predicate inside `pkg/sql/plan/utils.go` READ-ONLY —
    no new owner, no new file; the planned production set stays 21
    production files. (viii) THE PROJECTION: the set-operation parity
    clause stands (the recovery restores parent equivalence on exactly
    the surface the clause names — statement success, persisted tag
    `(66,1)`, value 1.0 per binding, the pre-existing merge error
    surfacing with lineage withheld); the clause gains ONE sentence
    naming the unequal-width source recovery and the authored-cast
    preservation; citations re-pointed to revision 26 and decision log
    44-72; the whole projection re-checked under the 64000-byte
    budget.

73.  Set-operation common-domain reconvergence at both set-operation
    common-type owners (revision 27, F-SETOP-COMMON-RECONVERGENCE):
    the twenty-sixth review's finding was verified at code, and a
    same-class planner probe (harness deleted after capture; logs
    /tmp/30938_setcommon_compile.log COMPILE-EXIT=0 and
    /tmp/30938_setcommon_run.log RUN-EXIT=0, --- PASS:
    TestZZSetCommonProbe) fixed the observable ground truth. (i) THE
    GAP — THE SET OPERATION'S OWN COMMON-TYPE BINDING HAS NO
    PROVENANCE RETRY: BOTH common-type owners look up `coalesce`
    directly over branch SURFACE types — the initial binding in
    `buildUnionWithResultLen` (`query_builder.go:4828`–`:4830`, a
    failed lookup returning `the %d column cann't cast to a same
    type` immediately) and the prepared execute-time resolution in
    `preparedSetOperationCommonType` (`utils.go:7588`–`:7590`,
    invoked from the reconciliation walk at `:7965`); the merge
    checker's decimal common-domain arm rejects a
    carrier-overflowing width BEFORE any alignment cast exists and
    BEFORE any outer consumer retry (`function/type_check.go:1113`
    –`:1117`: `requiredWidth` = max integral + max scale,
    `decimalTypeForRequiredWidth` declining widths > 76, decimal256's
    declared width 76 at `types.go:869`–`:871`;
    `coalesceDecimalResult` failing at `func_binary.go:881`–`:892`,
    surfacing at `:1139`–`:1143`). Entry 56's retry funnel covers
    the seven merge families at the BASE BINDER only; entry 72's
    recovery serves the OUTER consumer's execute-time retry AFTER
    the set operation has bound. The finding's counterexample:
    `s(a, b DECIMAL(38,0); d DECIMAL(65,1))` all holding 1,
    `SELECT a*b AS v FROM s UNION ALL SELECT d FROM s` — the
    parent's product publishes `(65,0)`, the common of `(65,0)` with
    `(65,1)` computes max integral 65 and max scale 1 = width 66,
    BINDING `(66,1)` and returning 1/1.0; this design's widened
    product publishes `(76,0)`, and integral 76 + scale 1 = 77 > 76
    is REJECTED at BOTH owners. Cell (xix)'s scale-0 branch set
    (`(76,0)`/`(70,0)`, width 76 scale 0 = 76) binds fine, so the
    planned validation could never detect this EARLIER failure. (ii)
    OBSERVED GROUND TRUTH (this turn's probe on the worktree widened
    route): MX — the counterexample raises exactly `SQL parser
    error: the 0 column cann't cast to a same type` at INITIAL
    binding; MXR — the reversed branch order raises the identical
    error (width-order invariant); MZ control — the scale-0
    `(76,0)`/`(70,0)` union BINDS today; MP control — SQL-authored
    `CAST(a*b AS DECIMAL(65,0))` in the (38,0)-operand branch
    against the same `(65,1)` sibling BINDS TODAY with its authored
    target (the owner combining `(66,1)` over parent-domain
    exports); MW genuine-wide control — a 76-digit integer-literal
    branch with a scaled `9.9` sibling (integral 76 + scale 1 = 77,
    NO multiplication provenance) raises the IDENTICAL rejection
    TODAY and must raise it identically AFTER the repair. (iii) THE
    REPAIR — ONE RETRY AT EACH OWNER'S OWN FAILURE: when an
    owner's coalesce lookup FAILS, the owner runs the shared
    source-domain resolver (entry 56 channels (a)-(d), the entry
    69/70 hop chains, entry 71's per-branch domain view, and entry
    72's coercion-source recovery — already the engine's OWN order
    at `utils.go:7897` before this owner's invocation) over the
    PER-BRANCH EXPRESSIONS both sites already hold (the initial
    owner's branch `ProjectList[colIdx]`; the prepared owner's
    `branchExprs`); with at least one provenance-bearing branch the
    owner RETRIES the lookup ONCE over the resolved
    parent-published branch vector — success authors the PARENT'S
    OWN common type and the parent-authored per-branch alignment
    view (initial route: the parent target `(66,1)` on the finding
    and the per-branch parent types feeding the standing `needsCast`
    alignment at `query_builder.go:4968`–`:4996` exactly as if the
    branch exports had carried the parent types; execute route: the
    caller at `utils.go:7965` receiving the parent target and
    refreshing the set output and its pending casts
    parent-identically); FAILURE RE-ARMS the owner's original
    outcome untouched — the `:4830` error / the `:7590` return —
    byte-identically. A branch set without ANY widened-multiplication
    provenance DEFERS to the family's own outcome (the MW
    identical-failure control preserved); SQL-authored cast arms
    keep their fixed targets (entry 72's predicate declines them,
    and a parent holds the same authored cast, so the parent domain
    IS the authored target — the MP channel standing); a retried
    combination that STILL overflows re-arms, since the parent fails
    that statement too. (iv) GENERALITY: all six set-operation
    variants share the ONE initial owner `buildUnionWithResultLen`
    and the ONE prepared owner `preparedSetOperationCommonType`, so
    the retry is variant-uniform; the integer-literal exact-typing
    arms (`query_builder.go:4797`, `utils.go:7573`) stand upstream
    untouched; non-decimal and character families never engage (the
    resolver attaches no widened-multiplication provenance to them);
    composition with the entry 69/70/71/72 chains sits at the
    RESOLVER level, unchanged. Entry 56's funnel and its totality
    assertion over the base-binder family call sites are UNCHANGED
    — these two owners are NOT binder family call sites; they retry
    their OWN lookups under the entry (iii) discipline (annotated
    kept-as-history at entry 56). (v) EXECUTE-ROUTE SEQUENCING:
    unchanged from entries 69 (v)-72 (v): no new route, no new
    lifecycle — the execute owner already runs inside the standing
    reconciliation walk, and the retry is READ-ONLY over the
    statement-local ledger plus the resolver channels. (vi)
    VALIDATION PLAN: section 9 I10 gains cell (xx) — the
    mixed-scale counterexample in BOTH branch orders asserting the
    retry's outcome first (the owner's retried binding
    content-keyed, the union publishing the parent's `(66,1)` with
    parent-authored per-branch alignment casts), statement success
    with the parent's value 1/1.0 parent-identically, CTAS
    persisting `(66,1)` parent-identically; the EXECUTE-SPECIALIZED
    set-arm parameter case (a prepared union of a retained
    widened-product arm and a parameter arm, the EXECUTE binding
    typed `(65,1)`) converging `(66,1)` through the prepared owner's
    retry with the second-EXECUTE cached route parent-identical and
    its retry-withheld ablation reproducing THIS probe's exact
    initial-binding rejection; the independent parent engine oracle
    asserted per-execute (value, type AND persisted tag); the
    genuine-wide 77 control (MW) asserted failing IDENTICALLY before
    and after; the authored-cast control (MP) asserted binding
    before and after with its authored targets; the no-provenance
    deferral control; cells (xv)-(xix) re-asserted with the retry
    installed (cell (xix) recovery-invariant — scale-0 branch sets
    never fail the lookup, so the retry never fires); the BVT on the
    owned isolated service exercising the counterexample in BOTH
    branch orders and the prepared parameter-arm case with
    catalog-surface persisted-tag `(66,1)` assertions and verified
    teardown; the Challenger projection re-verified byte-for-byte
    against the aligned cells before submission. (vii)
    PRODUCTION-SET ACCOUNTING: no new file, no new owner — the
    retry lands inside the already-owned
    `pkg/sql/plan/query_builder.go` (initial owner) and
    `pkg/sql/plan/utils.go` (prepared owner; entry 72 reused its
    predicate READ-ONLY, and the retry now writes this one's failure
    branch — the SAME file), reusing the already-owned resolver
    and the registry carrier; the planned production set stays 21
    production files. (viii) THE PROJECTION: the set-operation
    parity clause stands; ONE sentence appended to `semantics[4]`
    after the revision-26 sentence covering the common-type
    reconvergence at BOTH owners — the parent's own common type
    (the mixed (38,0)x(38,0) / (65,1) example converging `(66,1)`)
    bound on the initial AND the prepared execute-time resolutions,
    with identical-width genuine-wide unions keeping the parent's
    rejection and SQL-authored cast arms keeping their authored
    targets; citations re-pointed to revision 27 and decision log
    44-73; the whole projection re-checked under the 64000-byte
    budget.

74.  Post-implementation-review convergence: MOD parent-domain
    derivation, post-remap producer-seam resolution, and the
    mandatory completion of the approved envelope, finalization
    and memoized-walk invariants (revision 28,
    F-MOD-PARENT-DOMAIN, F-SEAM-RESOLUTION, F-ENVELOPE-COVERAGE,
    F-TIME-WINDOW-FINALIZATION, F-MEMOIZED-WALK): the complete
    implementation of the revision-27-approved design received an
    independent pre-PR implementation review with five findings;
    each was verified at code this round. Finding 1 was
    DESIGN-MATERIAL (the parent-domain resolver's out-of-case
    premise wrongly excluded a derivable supported consumer), so
    this revision corrects the design BEFORE any production
    change; findings 2-5 were unmet obligations of text this
    document already approved (F-HEAD-GATE's head-independent
    seam discovery, F-PRODUCER-BOUNDARY's materialization-boundary
    taxonomy, section 4.1/4.2's envelope rules and registry-wide
    inventory, log 26's P1 TIME_WINDOW inclusion, and section
    4.3's memoized anchor-seeded walk), reaffirmed here as
    mandatory and pinned with the explicit section 9 witnesses of
    cell (xxi). (i) THE MOD GAP — THE PARENT-DOMAIN RESOLVER
    MISSES A DERIVABLE SUPPORTED CONSUMER: the resolver's
    function-family case table (`base_binder.go:10083`-`:10147`)
    enumerates aggregate heads, value selectors, `*`, `+`, `-`,
    `/`, the rounding wrappers and the merge families but has NO
    `mod`/`%` arm (the switch falls to its not-found result at
    `:10146`), while the shared typing owner
    `DecimalModReturnType`
    (`pkg/sql/plan/function/decimal_mod.go:19`-`:54`) derives the
    MOD result domain over the original operands — the review's
    `(a*b) % 1` union branch is therefore derivable through the
    same typed-consumer mirrors the case table already applies,
    and the review's UT oracle expecting EXECUTE failure pins a
    compatibility regression: verified live against the parent
    engine this round (receipts /tmp/30938_base_mod_probe.txt
    and /tmp/30938_base_mod_probe2.txt), the statement-time CTAS
    `CREATE TABLE o1 AS SELECT COALESCE(u.v, CAST(0.5 AS
    DECIMAL(20,1))) AS w FROM (SELECT a*b AS v FROM s1 UNION ALL
    SELECT (a*b) % 1 AS v FROM s2) u` over DECIMAL(38,0) operands
    holding 1 SUCCEEDS on the parent — information_schema shows w
    DECIMAL(66,1), values 1.0/0.0 — while the current diff
    declines the identical statement with the checked merge-bind
    error (/tmp/30938_fixed_mod_iso2.txt), contradicting the
    design's no-divergence contract for supported statements.
    DECISION: the parent-domain resolver gains a `mod`/`%` rule
    mirroring `DecimalModReturnType` over the reconstructed
    parent operand domains, at the case-table locus beside the
    existing `/` mirror — a consumer derivable through the shared
    typing rules is never unresolvable. (COMPLETED AT THE STORAGE
    BOUNDARY in revision 29 — kept as history: this DECISION's
    `mod`/`%` rule reached the parent-domain RESOLVER only; section
    4.1's envelope mirror and section 4.2's consumer
    classification now carry it (decision log 75), with CTAS/view
    metadata witnesses the set-operation witness of cell (xxi)
    (a) cannot establish.) CONSEQUENCE FOR THE
    DECLINE FAMILY: with the MOD rule the shared typing owners
    cover EVERY decimal-producing supported consumer named by
    section 4.2, so no out-of-case decimal expression shape
    remains; cell (xviii)'s decline witness (a `genuinely
    UNRESOLVABLE branch contributor (an out-of-case expression
    shape)`) is WITHDRAWN as an oracle (annotated kept-as-history
    in section 9), its `(a*b) % 1` branch moves to the success
    family with parent/fixed success, value and persisted-tag
    assertions (cell (xxi) (a)), and the decline family is
    re-scoped to EVIDENCE-RECONSTRUCTION FAILURE ONLY (withheld
    or corrupted lineage), already pinned load-bearing by the
    lineage-withheld ablation reproducing the exact checked fill
    error; section 10's `reserving decline for genuinely
    unresolvable sources` language narrows accordingly. (ii)
    F-SEAM-RESOLUTION — SEAM DISCOVERY IS DEAD AFTER THE REMAP:
    `createQuery` invokes the reconciliation AFTER
    `remapAllColRefs` (`query_builder.go:4538`, the pass at
    `:4598`; the correlation route likewise,
    `correlation_binding.go:111` before `:141`), and
    `remapAllColRefs` clears `node.BindingTags`
    (`query_builder.go:4174`); the pass nevertheless keys its
    producer inventory off the cleared tags
    (`build_ddl.go:10550`-`:10553`), treats a remapped
    `ColRef.RelPos` as a binding tag (`:10779`-`:10782`), and
    skips discovery when an export's envelope equals its
    published type (`:10775`). The review's counterexample — the
    design's own section-5 two-reader witness shape, a
    materialized CTE exporting `COALESCE(a*b, <76-digit
    literal>)` (head `(76,0)` = envelope `(76,0)`, so no head
    divergence) carrying cancelling +/-10^65 product rows —
    loses the product seam's required `(65,0)` admission and can
    average to a silent zero, contradicting F-HEAD-GATE (section
    5: every decimal-typed subexpression whose envelope differs
    from its own published type receives the cast whether or not
    the argument head diverges). DECISION: producer seams resolve
    through POST-REMAP node/slot identity — reusing the complete
    producer-resolution owner that infers declared storage tags
    (the lineage view of decision log 69-72, SourceStep ownership
    across the materialization boundary per log 70), retained
    evidence read-only — independently of head-type divergence;
    the BindingTags channel and the head-envelope skip are
    removed. Cell (xxi) (b) pins the materialized two-reader
    regressions: cancellation rejection, fitting two-reference
    values, short-circuit demand, and an unaffected MAX-only
    reader. (iii) F-ENVELOPE-COVERAGE — RULES SECTION 4.1
    ALREADY OWNS ARE UNIMPLEMENTED: the shared envelope owner
    (`build_ddl.go:1876`-`:1980`) handles `*` and selected
    wrappers but lacks the section-4.1 mirrors for `+`, `-` and
    `/` (`decimalAddSubReturnType`, `decimalDivisionReturnType`)
    and the PERCENTILE_DISC selection rule —
    `TRUNCATE((38,10)x(38,10),0)+DECIMAL(39,0)` persists `(65,0)`
    instead of the parent's `(54,0)`, and CTAS over
    `PERCENTILE_DISC(...) ORDER BY a*b` persists `(76,0)`
    instead of the parent's `(65,0)`, bypassing the intended
    declared-domain boundary. DECISION: the mirrors and the
    PERCENTILE_DISC rule land at the shared envelope owner, and
    section 4.2's promised registry-wide consumer inventory check
    is added over these consumers; cell (xxi) (c) pins CTAS and
    view metadata, oversized-value rejection and genuine-wide
    controls. (iv) F-TIME-WINDOW-FINALIZATION — LOG 26'S P1
    TIME_WINDOW INCLUSION IS UNMET: `reconcileNode`
    (`build_ddl.go:10620`-`:10637`) switches AGG and WINDOW only,
    and the projection refresh (`utils.go:8360`-`:8390`,
    `:8455`-`:8480`) has no Node_TIME_WINDOW case, leaving
    TIME_WINDOW AggList producers and their self-referencing
    output slots stale — pinned in the repository today: the
    issue fixture's time-window header publishes AVG(a*b) as
    DECIMAL(65,4) (the .result file's time-window block,
    `issue_30938_decimal_mul_precision.result:780`-`:783`)
    where the parent's argument `(65,0)` yields AVG `(65,0)`
    (`pkg/sql/colexec/aggexec/sumavg2.go:65`-`:78`), and the
    stale scale feeds downstream expression inference (the
    fixture ran with nometa, so the header was never compared).
    DECISION: P1 finalization includes TIME_WINDOW producers,
    their output copies and their downstream consumers in
    dependency order through `BuildTimeWindowLayout` (this
    document's single layout source, sections 3 and 9); cell
    (xxi) (d) pins the regenerated fixture header `(65,0)`,
    parent/fixed metadata assertions, an `AVG(a*b)/3` control
    over unit-valued operands, and CTAS plus prepared routes.
    (v) F-MEMOIZED-WALK — SECTION 4.3'S MEMO AND ANCHOR-ONLY
    RECURSION ARE UNIMPLEMENTED: the storage-domain evaluator
    carries path-local source/active sets, deleting `active`
    entries after each visit (`build_ddl.go:1750`-`:1756`,
    `:1843`-`:1850`), enumerates ALL SourceStep contributors
    (`:2313`-`:2333`) instead of seeding recursive slots solely
    from anchor domains, and constructs a fresh evaluator per
    output column (`:2606`-`:2623`, callers `:389`, `:2761`,
    `:2826`). DECISION: the statement-scoped memo of section 4.3
    — ONE evaluation per producer slot per statement, shared
    across every output column — and anchor-only recursive
    seeding are implemented as specified; cell (xxi) (e) pins
    the deterministic shared-diamond count test and the
    cross-column recursive test. (vi) EXECUTE-ROUTE SEQUENCING
    AND LIVE-ORACLE SCOPING: no new route and no lifecycle
    change — sequencing stands from entries 69 (v)-73 (v).
    EVIDENCE RECORD from this round's owned isolated services:
    (a) the parent's statement-time success for the MOD
    set-operation shape ((66,1), values 1.0/0.0 — (i) above);
    (b) the parent's PREPARED route for these set-operation CTAS
    shapes — the MOD branch AND the plain `a*b` control alike —
    died in a PRE-EXISTING, MOD-INDEPENDENT TN-side server panic
    (`slice bounds out of range [:32] with capacity 24` via
    `vector.(*Vector).unionBatch`,
    pkg/container/vector/vector.go:8899; service logs
    base_modprobe.log and base_modiso.log; receipts
    /tmp/30938_base_mod_probe3.txt and
    /tmp/30938_base_mod_iso1.txt): transaction-node behavior
    outside this issue's scope, recorded here only because it
    scopes the oracles — the parent-engine prepared-route parity
    oracles for these shapes continue through the in-process
    fill/refresh channels (Witness A/B), the LIVE parent/fixed
    parity oracle stands at statement time, and the fixed
    engine's live prepared route must complete CHECKED — success
    per this contract, never a crash (the current diff already
    declines checked on that route); (c) the current diff's live
    statement-time route declines the plainest supported
    set-operation coalesce shapes (plain AND MOD —
    /tmp/30938_fixed_mod_iso1.txt and
    /tmp/30938_fixed_mod_iso2.txt), an implementation convergence
    gap this revision makes explicit: implementation converges
    only with the live statement-time parent/fixed parity
    witnesses (success, value, persisted tag) on the owned fixed
    service alongside the in-process prepared channels. (vii)
    VALIDATION PLAN: section 9 I10 gains cell (xxi) — the five
    witness families (a)-(e) specified in the row — and cells
    (xv)-(xx) are re-asserted with the corrections installed; the
    issue fixture's time-window block is regenerated, the
    out-of-case decline UT is replaced by the MOD success
    assertions, and the BVT exercises the live statement-time
    parity witnesses on the owned isolated service with
    catalog-surface persisted-tag assertions and verified
    teardown. (viii) PRODUCTION-SET ACCOUNTING AND THE
    PROJECTION: no new file, no new owner — the MOD rule lands
    inside the already-owned `pkg/sql/plan/base_binder.go`
    resolver and the seam, envelope, inventory, finalization and
    memo work lands inside the already-owned
    `pkg/sql/plan/build_ddl.go` and `pkg/sql/plan/utils.go`
    owners; the planned production set stays 21 files. THE
    PROJECTION: semantics gains the MOD set-operation parity
    sentence, the `unresolvable branch` limitation narrows to
    evidence-reconstruction failure, the pre-existing
    MOD-independent TN-side prepared-route panic of the parent
    binary is recorded as an environmental limitation, and
    citations re-point to revision 28 and decision log 44-74; the
    whole projection re-checked under the 64000-byte budget.

75.  F-MOD-STORAGE-ENVELOPE — MOD'S SHARED TYPING RULE COMPLETED AT
    THE STORAGE-ENVELOPE BOUNDARY (revision 29; resolves the
    independent design-only review of revision 28, artifact
    `pre-pr-review-1793536399133.json`, one P2 finding). (i)
    FINDING, VERIFIED AT CODE AND LIVE THIS ROUND: revision 28's
    decision 74 (i) added `mod`/`%` to the parent-domain RESOLVER
    only (its DECISION above); section 4.1's envelope mirrors and
    section 4.2's consumer classification still omitted it, so a
    width-sensitive MOD chain over a widened product falls to the
    envelope owner's typed fallback (`build_ddl.go:1979`; the
    transparent and value-arg inventories `:1600`-`:1617` and
    `:1625`-`:1636` carry no `mod`) and persists the PUBLISHED
    head even where the parent's own inference derives a different
    tag. The shared typing owner is exact: `DecimalModReturnType`
    (`pkg/sql/plan/function/decimal_mod.go:22`-`:54`) derives
    result precision as the MAXIMUM operand width and result scale
    as the MAXIMUM operand scale over the original operands.
    Witness A (below-65 wrapper): with a,b `DECIMAL(38,10)` and d
    `DECIMAL(39,0)`, the parent product publishes `(65,12)`;
    `refineDecimalRoundingReturnType`
    (`base_binder.go:9419`-`:9455`) derives `TRUNCATE(x, 0)` as
    `(53,0)` (precision = integral digits + 0, no carry reserve
    for truncate); the parent's MOD then derives `(53,0)`. The
    widened head computes `(76,12)` -> `(64,0)` -> `(64,0)`, and
    no specified fallback recovers `(53,0)` from `(64,0)`. LIVE
    against the parent and the current diff on owned isolated
    services (isolated port 38000, template launch, verified
    teardown): the parent persists `decimal(53,0)` for BOTH the
    CTAS record and the view record of `TRUNCATE(a*b,0) % d`,
    value `0` (receipt `/tmp/30938_base_modenv_probe.txt`); the
    current diff persists `decimal(64,0)` (receipt
    `/tmp/30938_fixed_modenv_probe.txt`) — an I2 violation: the
    widening authored a wider schema than the parent's own
    inference. Witness B (genuine-wide MOD control): CTAS over
    `(a*b) % d` with (38,0)x(38,0) operands and d `DECIMAL(39,0)`:
    the parent persists `decimal(65,0)`, value `0`; the current
    diff persists `decimal(76,0)` — the widened head verbatim
    (same receipts). The planned set-operation MOD witness of cell
    (xxi) (a) exercises the prepared-CTAS RESOLVER and cannot
    establish this storage boundary, exactly as the review states.
    (ii) DECISION — SECTION 4.1 MIRROR: `mod`/`%` mirrors
    `DecimalModReturnType` rule-for-rule over the operand
    ENVELOPES at the shared envelope owner, placed beside the
    divide mirror: result scale `max(s1, s2)`, result precision
    `max(w1, w2)`, each operand width its envelope width `integral
    + scale`; the typing owner's integer-family digit recovery
    (`integerDecimalDigits`) and non-decimal pass-through apply
    verbatim (temporal, approximate, string, BIT and unknown
    operands keep the selected type, hence their authored typed
    envelopes). Reconstructions: Witness A envelopes `(53,0)` x
    `(39,0)` -> `(53,0)`; Witness B envelopes (the `*` case's
    `(65,0)`) x `(39,0)` -> `(65,0)`; both persist parent-verbatim
    at CTAS and view boundaries. (iii) DECISION — SECTION 4.2
    CLASSIFICATION: `mod` joins item (ii)'s width-sensitive
    mirrored set as its ONE member not 65-capped (its
    maximum-of-operand-precisions derivation publishes the widened
    `(76,0)` head across the seam); the registry-wide inventory
    check promised by decision 74 (iii) extends to `mod`, and item
    (ii)'s no-other-consumer sentence stands with `mod` inside the
    mirrored set. (iv) WITNESSES: cell (xxi) (c) gains the MOD
    family (c-mod1)/(c-mod2) — UT envelope arithmetic assertions
    (reconstructed envelopes `(53,0)` and `(65,0)`) plus live
    statement-time persisted-tag BVT assertions on the owned fixed
    service with catalog-surface reads (CREATE TABLE AS SELECT and
    CREATE VIEW records), value assertions `0`, and parent-derived
    oracles (this round's receipts), never observed fixed-binary
    values. (v) ENTRY 74 (i) is annotated kept-as-history
    (resolver-only scope; storage completion here); section 4.1
    gains the bullet beside divide; section 4.2 item (ii) gains
    the classification sentence; no other section's content or
    numbering changes. (vi) SEQUENCING AND PRODUCTION SET:
    ordering stands from decisions 69 (v)-74 (vi); the mirror
    lands inside the already-owned `pkg/sql/plan/build_ddl.go`
    envelope owner and its UTs inside the already-owned issue UT
    file — the planned production set stays 21 files with no new
    owner. (vii) VALIDATION PLAN AND THE PROJECTION: cheapest
    discriminating regression first — the (c-mod1) envelope UT red
    against the base (the parent IS the oracle) and green on the
    candidate, then (c-mod2); the MOD family then rides decision
    74 (vii)'s issue-UT ladder and live statement-time BVT parity
    witnesses; no baseline, race-belt or full-suite rerun is
    triggered by this revision. THE PROJECTION: semantics gains
    the MOD storage-envelope sentence with BOTH witnesses
    (persisted `(53,0)` and `(65,0)`, values `0`), sources and
    limitations re-point to revision 29 (`sha256` recorded at
    submission) and decision log 44-75, and the whole projection
    re-checked under the 64000-byte budget.

76.  Post-implementation-review convergence: value-stability-conditional
    shared-producer admission channels, and completion of parent-domain
    resolution for the supported value-selecting selectors (revision 30;
    resolves the independent pre-PR implementation review of the
    revision-29 implementation, artifact
    `pre-pr-review-1793553775104.json`, two P2 findings). Both findings
    were verified at code and by an executable probe on the candidate
    tree (`zz_issue30938_review61_diag_test.go`, harness deleted after
    capture; probe log `/tmp/30938_probe61.log`, EXIT=1 with exactly
    the designed failures: the materialized producer's projection
    carried TWO `rand` evaluation sites, both admitted, and each of the
    four selector merges declined with `invalid argument function
    coalesce, bad value [DECIMAL256 DECIMAL256]` while the `max_by`
    control bound `(66,1)`). Finding 1 is design-material — it
    contradicts fact 15's evaluation-identity premise — so this design
    is corrected BEFORE any production change; finding 2 completes a
    gap against behavior this document already contracts (the
    value-selecting row's convergence guarantees and entry 74 (i)'s
    resolver completeness claim). (i) F-UNSTABLE-SHADOW-IDENTITY (§3,
    §5, facts 15/16/18, §9 I10 (xxii)). ROOT CAUSE: the shadow emission
    deep-copies the slot's producing expression (`build_ddl.go:11257`,
    appended at `:11278`), and every expression copy owns an
    independent executor (`evalExpression.go:269–275`); for a
    value-unstable producing expression the replica's second evaluation
    diverges — a seeded `rand(N)` advances its per-executor sequence
    (`func_builtin.go:5008–5049`), an unseeded `rand()` or `uuid()`
    draws fresh — so the admitting AVG/SUM reader consumes values no
    sibling reader of the same export observes, breaking the
    single-publication contract the materialization itself enforces
    (`cte_identity.go:28`, `:219–220`, `:242`). DECISION: admission
    channel selection per (producer, referenced exported slot) by the
    SAME value-stability classification that chose materialization — a
    value-stable slot keeps the revision-11 shadow replica (its
    deterministic re-evaluation is value-identical row for row); a
    value-unstable slot emits NO shadow and takes each
    producer-representable divergent seam's envelope cast IN PLACE
    inside the original export, with the export's published type
    unchanged and the casts carrying the design-owned cast identity
    (fact 17), so the section-4.1 mirrors recover through them and
    consumer recomposition recognizes the admission. PROPERTIES: the
    unstable call is evaluated exactly once, literally; demand equals
    the parent's producer demand on the base engine's own masks (an
    unselected overflowing arm admits nothing); fitting rows give every
    consumer — admitting and value-selecting alike — the same
    carrier-exact values the parent produces; a selected violating row
    raises the parent's multiply-time family at the in-place cast
    during materialization, the whole statement failing on both engines
    (the parent rejects at the multiply — the sanctioned errors[4]
    divergence); a statement with no admitting AVG/SUM reader of the
    slot splices nothing and is byte-identical to revision-29 behavior;
    the revision-28/29 two-reader witnesses are unaffected (their
    `uuid()` sits in a separate unadmitted column). REJECTED
    ALTERNATIVE: eager factoring of the unstable call into its own
    shared slot reassigns a seeded generator's draws from the base
    engine's lazy arm-masked evaluation order to eager column-order
    draws — observable through the seeded form — so the factored plan
    is not the parent's evaluation. SECTION AMENDMENTS: §3 step 4 and
    §5's emission passage become the two-channel spec; fact 15 gains
    the single-evaluation amendment; fact 16 gains the per-slot channel
    selection and its 'no double evaluation' close is rescoped to
    value-identical re-evaluation plus never-replicated unstable slots;
    fact 18's storage bound now charges a superset of the slots that
    can actually shadow and stays conservative; §9 I10 gains cell
    (xxii) with the deterministic counting regression (seeded
    `rand(42)`, one row, AVG = MAX = MIN by value — a duplicated
    executor breaks the equality), the live public shared-CTE witness,
    the violating-row and MAX-only controls, the unseeded
    internal-consistency variant and the stable-channel byte-identity
    controls. (ii) F-SELECTOR-PARENT-DOMAIN (§9 I10 (xxiii),
    value-selecting row). ROOT CAUSE: the pending-output parent-domain
    resolver's selector switch (`decimalBoundaryAggregateParentDomain`,
    `base_binder.go:10051–10065`, mirrored by the call form at
    `:10084`) covers avg/avg_tw_result/sum/min/max/any_value/max_by
    only, so a merge over a widened MAX_BY_NON_NULL, PERCENTILE_DISC,
    FIRST_VALUE/LAST_VALUE/NTH_VALUE or LAG/LEAD output — pending
    aggregate tag `:9911–9915`, pending window tag `:9916–9921`,
    prepared-lineage execute rebind alike, all funneling into the same
    switch — declines its retry and retains the binding error where the
    parent converged (probe-confirmed:
    `COALESCE(MAX_BY_NON_NULL(a*b,k,k), CAST(0 AS DECIMAL(65,1)))` over
    `(38,0)` operands needs width 77 on the widened `(76,0)` head and
    declines; the parent's `(65,0)` head converges to `(66,1)`). The
    lag/lead WITH-DEFAULT window bind is the same defect at its own
    locus: `reconcileLagLeadDefaultType` (`base_binder.go:9206`,
    called at `:8961`) resolves decimal-decimal defaults through a
    direct `coalesce` registry lookup (`:9318`) outside the merge-retry
    funnel (`:8383–8385`, family `:9477–9485`), failing to the checked
    `LAG/LEAD common precision exceeds SQL limit 65` (`:9321`) on
    widened operands where the parent binds `(66,1)`. DECISION: the
    resolver is completed rule-for-rule with section 4.1's per-selector
    operand rules (already complete at `build_ddl.go:1633–1644`,
    `:2033–2042`, `:2044–2079` — no section-4.1 change; ANNOTATED
    kept-as-history in revision 31: the lag/lead envelope at
    `build_ddl.go:2044–2080` was NOT complete — its 65-digit
    integral clip (`:2073–2075`) persisted `(65,1)` where the
    parent persists `(66,1)` — section 4.1's lag/lead bullet is
    rewritten in revision 31, F-LAGLEAD-STORAGE-ENVELOPE; ANNOTATED
    kept-as-history in revision 33, F-LAGLEAD-PARENT-REJECTION:
    this entry's premise that the parent binds `(66,1)` for the
    with-default merge is INCORRECT — the parent's reconcile
    rejects every derived decimal width >65 at planning (HEAD
    `base_binder.go:9208`/`:9239`/`:9285`, fallback `:9261`; live
    parent receipt `/tmp/30938_bvt_dtype_base.log` row 188), so no
    parent `(66,1)` binding or persisted schema exists; the
    completion DECISION stands — the repaired engine's 66–76
    admission is the intentional expansion of decision log 79 with
    the §4.1 storage policy) evaluated
    over
    reconstructed parent domains: max_by_non_null, percentile_disc,
    first_value, last_value and nth_value resolve to the parent domain
    of the value argument; lag/lead resolve through
    `reconcileLagLeadDefaultType` over the parent domains of the value
    and (when present) default arguments — the default Expr still
    passed for the literal rules — at BOTH loci (the resolver and the
    window-bind reconcile, whose decimal-decimal tail joins the retry
    over parent domains and re-arms its identical checked rejection on
    failure or absent provenance); the 2-arg form resolves to the value
    argument's parent domain. EXCLUSIONS VERIFIED:
    `quantileexactweighted` rejects DECIMAL256 value arguments on both
    engines with its pre-existing admission family
    (`quantile_exact_weighted.go:36–68`,
    `aggexec/quantile_exact_weighted.go:57–72`; the probe control
    observed the admission rejection, never the coalesce decline) and
    stands outside the completion; `percentile_cont`'s
    decimal128(38,s+1) result is width-independent and stands excluded.
    Entry 74 (i) is annotated kept-as-history a second time (its
    completeness claim covered the selectors then reviewed). (iii)
    VALIDATION PLAN AND THE PROJECTION: cheapest discriminating
    regressions first — the probe shapes become permanent three-way
    UTs (parent-green, current-candidate-red, repaired-green) per §9
    I10 (xxii)/(xxiii); the focused issue-UT ladder and the live A/B
    witnesses ride decision 74 (vii)'s channels; no baseline, race-belt
    or full-suite rerun is triggered — the revision-29 evidence (the
    focused ladder, the 372/372 BVT and the live P1–P7 receipts)
    exercises only value-stable slots and covered selectors, so its
    inputs are unchanged and it is reused under the
    validation-evidence contract. THE PROJECTION: semantics gains the
    two-channel admission sentence and the selector merge-convergence
    sentence; the errors family retains the sanctioned multiply-time
    divergence and the pre-existing quantileexactweighted rejection;
    sources and limitations re-point to revision 30 (`sha256` recorded
    at submission) and decision log 44–76; the whole projection is
    re-checked under the 64000-byte budget. (iv) SEQUENCING AND
    PRODUCTION SET: both repairs land inside the already-owned
    `pkg/sql/plan/build_ddl.go` and `pkg/sql/plan/base_binder.go`; the
    planned production set stays 21 files with the `pkg/vm/process`
    registry carrier and no new owner; witnesses land in the
    already-owned issue UT files and the BVT fixture.

77. **Revision 31 — the round-62 design-only review's three findings**
    (2026-11-02; artifact `pre-pr-review-1793556020172.json`,
    REQUEST_CHANGES against revision 30, sha256
    `85f1fa30a9a934be42106595ebfb83aaa41156ac0bcf7178ed6bf089e4cefeae`;
    all three findings verified VALID at code on the frozen HEAD
    `5fe08a3307` before this revision).

    (i) F-REPLAY-SAFETY-POSITIVE-PROOF (§3 step 4, §5, facts 15/16/18,
    §9 I10 (xxii)). FINDING: revision 30 gated the shadow channel on
    the value-stability flag alone, but value stability is not
    replay-safety — the nonvolatile `llm_chat` registration
    (`list_builtIn.go:2925–2943`, STRICT, no volatile flag, reported
    stable by `function.GetFunctionValueUnstableByID`) performs one
    HTTP request per row (`func_builtin_llm.go:69–101`, the per-row
    `Chat` at `:90`; one request per `Chat`,
    `pkg/monlp/llm/ollama.go:99` → `ChatMsg` `:77–96`), so a shadow
    replica of a slot whose producing expression contains it issues
    every external request twice — an observable duplicated effect the
    single-publication contract does not sanction. ROOT CAUSE:
    revision 30 reused the MATERIALIZATION classification
    (`cte_identity.go:219–220` reading
    `function.GetFunctionValueUnstableByID`, `function.go:151`) as the
    channel predicate, but that classification answers a different
    question — may several consumers share one evaluation — not the
    shadow question — may this expression be evaluated twice with no
    observable difference. DECISION: the shadow channel requires
    POSITIVE replay-safety proof over every node of the slot's
    producing expression: every `Expr_F` node is a decimal arithmetic
    operator (`*`, `+`, `-`, `/`) or a plain `cast` (kernel-pure value
    transforms), or an overload the curated purity gate
    `function.GetFunctionIsProvenPureFoldableById` accepts
    (`function.go:337–350` over `provenPureValueFunctions`,
    `:190–305`, excluding INTERNAL-class, volatile and real-time
    overloads by construction; the gate's own comment records the
    llm_chat counterexample at `:323–336`), and every leaf is a Lit or
    Col. (ANNOTATED kept-as-history in revision 34: a binder-produced
    cast's second argument is an `Expr_T` target descriptor, validated
    as METADATA, and the leaf rule scopes to VALUE arguments —
    F-CAST-DESCRIPTOR-LEAVES, decision log 80 (ii).)
    Every other slot — volatile, real-time, INTERNAL-class,
    unlisted (`llm_chat` included), or carrying any non-Lit/Col leaf —
    takes revision 30's in-place single-evaluation channel, which
    needs no proof because nothing is replicated. The eager-factoring
    alternative stays REJECTED (entry 76 (i)'s seeded-draw
    reassignment rationale, now read for volatile and effectful calls
    generally). The revision-28/29 shadow witnesses (`COALESCE(a*b,
    <literal>)` over Col leaves: `coalesce` curated, `*` kernel-pure)
    classify PROVEN and keep the shadow channel byte-identically, so
    their evidence is reused unchanged. The deterministic counted-call
    validation case rides `llm_chat` at the executor-test boundary (§9
    I10 (xxii) (a2)): N producer rows issue exactly N HTTP requests on
    the parent and on the repaired engine, 2N on the always-shadow
    candidate. Production note: the purity gate already exists in
    `pkg/sql/plan/function/function.go`; the channel-selection
    predicate lands inside the already-owned `pkg/sql/plan/build_ddl.go`
    — no new production file.
    ANNOTATED kept-as-history in revision 32 (F-DIAGNOSTIC-REPLAY):
    this entry's two-tier proof — curated membership and the blanket
    plain-`cast` exemption — is narrowed by entry 78 (i): curated
    membership admits only per-overload audited overloads
    (`coalesce` at this revision) and casts only the decimal→decimal
    warning-free family; every other provision of this entry stands.

    (ii) F-LAGLEAD-STORAGE-ENVELOPE (§4.1 lag/lead bullet, §9 I10
    (xxiii)). FINDING: section 4.1's lag/lead storage envelope clipped
    a widened default merge's integral capacity into `65 − scale`
    (`build_ddl.go:2073–2075`), so `LAG(a*b, 1,
    CAST(0 AS DECIMAL(65,1)))` over `(38,0)` operands persisted
    `(65,1)` where the parent's own reconcile derives and persists
    `(66,1)`, and the `DECIMAL(30,10)` default pair persisted
    `(65,10)` where the parent persists `(75,10)`. ROOT CAUSE: the
    envelope rule was written against the parent's DECLARED-domain
    surface instead of its reconcile computation:
    `reconcileLagLeadDefaultType` (`base_binder.go:9206`, called at
    `:8961`) resolves the default merge through the ordinary
    `coalesce` registry lookup (`:9323`) and the common rule
    `setSafeDecimalWidthAndScaleFromSource`
    (`type_check.go:1113–1122` — maximum integral width, maximum
    scale, width = integral + scale, carrier-bounded at 76), which
    yields `(66,1)` and `(75,10)` with no 65-digit clip. DECISION: the
    §4.1 bullet reproduces the parent's reconcile rule exactly —
    `combineDecimalStorageEnvelope` (`build_ddl.go:1656`) already
    computes maximum integral width and maximum scale, so dropping
    the clip makes it the coalesce-common computation rule-for-rule —
    an over-76 merged width keeps a rejection as unreachable defense
    in the parent's own checked `LAG/LEAD common precision exceeds
    SQL limit 65` family (`base_binder.go:9326`; the completed
    reconcile of entry 76 (ii) re-arms that rejection at bind, so no
    such statement reaches the storage boundary), the dead
    scale-rejection branch (`build_ddl.go:2069–2072`, the constant at
    `:1744`) goes with the clip, and the no-default/NULL-default
    pass-through is unchanged. The planned with-default witnesses (§9
    I10 (xxiii)) are extended to assert the parent's persisted tag
    parent-identically at every storage surface — CTAS, view records
    (DESCRIBE/SHOW COLUMNS/information_schema) and the prepared route
    per execute — for both derived pairs, with values and checked
    errors parent-identical.

    ANNOTATED kept-as-history in revision 33
    (F-LAGLEAD-PARENT-REJECTION): this entry's premise — that the
    parent's own reconcile derives and persists `(66,1)` and
    `(75,10)` — was verified against the WORKTREE-MODIFIED
    reconcile (its citations `base_binder.go:9206`, called
    `:8961`, lookup `:9323`, family `:9326` are the modified
    file's line numbers, whose three width checks the frozen diff
    widened >65→>76) and is INCORRECT for the assigned parent:
    HEAD `reconcileLagLeadDefaultType` (`git show
    5fe08a3307:pkg/sql/plan/base_binder.go:9147–9288`, called at
    `:8902`) rejects every derived decimal common wider than 65
    (`:9208`; `:9239`, its comment recording the persisted-DDL
    expressibility intent; `:9285`; lookup-failure fallback
    `:9261`), so both witness shapes fail at planning, the parent
    persists nothing, and 'values and checked errors
    parent-identical' has no referent (live parent receipt
    `/tmp/30938_bvt_dtype_base.log` row 188). The clip-removal
    DECISION stands with the corrected justification: the
    envelope reproduces the REPAIRED engine's reconcile
    rule-for-rule for the expanded 66–76 band — persist the
    reconcile-derived tag, no clip, no scale rejection — the
    admission being the intentional expansion decision log 79
    records; the with-default witnesses are re-planned three-way
    at §9 I10 (xxiii) — parent checked planning rejection,
    repaired success and persisted tag, candidate clip-red.

    (iii) F-SEEDED-RAND-ORACLE (§9 I10 (xxii)). FINDING: revision
    30's deterministic counting regression required `RAND(42)`, but
    the only registered RANDOM overload is zero-argument
    (`list_builtIn.go:9690–9711` — the `rand(1)` form its comment
    names was never registered), so the arity-1 call is rejected at
    planning (`fixedTypeMatch`, `type_check.go:333–334`); the seeded
    generator (`func_builtin.go:5008–5049`) is reachable through no
    registered overload; and identical-seed executors would draw
    identical sequences anyway, so the seeded cross-site equality
    could never discriminate a duplicated evaluation. DECISION: the
    oracle is replaced by the trio of §9 I10 (xxii) (a1)–(a3): the
    plan-shape oracle (one rand site, no shadow, the in-place cast —
    three-way: parent-green, current-candidate-red per the probe61
    log `/tmp/30938_probe61.log`, repaired-green), the deterministic
    invocation-count oracle at the executor-test boundary over
    `llm_chat`'s HTTP requests (parent N, candidate 2N, repaired N),
    and the within-engine-only value-consistency check for
    zero-argument `RAND()`/`UUID()` (cross-engine random-value
    equality is impossible and asserted nowhere). The RAND()-form
    consistency and plan-shape checks are retained as complementary.

    (iv) REJECTED ALTERNATIVES: reusing the materialization
    classification with an llm_chat carve-out (a deny-list decays with
    every new registration; the positive proof cannot silently
    weaken); declaring every nonvolatile function replay-safe (the
    finding's exact counterexample); keeping the 65-clip and
    special-casing lag/lead tags at the boundary (a second owner
    duplicating the reconcile rule; the un-clipped envelope rule IS
    the REPAIRED reconcile's computation for the expanded band
    (ANNOTATED in revision 33: the parent's computation derives
    the common and then rejects derived widths >65 —
    F-LAGLEAD-PARENT-REJECTION)); live unseeded cross-engine value
    oracles (impossible by definition).

    (v) EXCLUSIONS: no change to the materialization classification
    itself (`cte_identity.go` — sharing semantics are correct as
    classified); no change to `pkg/sql/plan/function/function.go`
    (the purity gate exists); no relaxation of any revision-29
    contract the Challenger validated; the eager-factoring rejection
    stands.

    (vi) VALIDATION PLAN: the §9 I10 (xxii)/(xxiii) witnesses as
    amended ride decision 74 (vii)'s issue-UT ladder and live A/B
    channels on design PASS; revision-29 evidence whose inputs these
    amendments do not touch (the value-stable shadow witnesses, the
    covered-selector merges, the 107/107 ladder, the 13/13 anchor
    run, BVT 372/372 twice, the live P1–P7 receipts) is reused under
    the validation-evidence contract; no baseline, race-belt or
    full-suite rerun.

    (vii) SEQUENCING AND PRODUCTION SET: both repairs land inside the
    already-owned `pkg/sql/plan/build_ddl.go` and
    `pkg/sql/plan/base_binder.go`; the planned production set stays
    21 files with the `pkg/vm/process` registry carrier and no new
    owner; witnesses land in the already-owned issue UT files and the
    BVT fixture. THE PROJECTION: the two-channel admission sentence
    gains the positive-proof condition, the value-selecting semantics
    gain the lag/lead expansion-band contract (ANNOTATED
    kept-as-history in revision 33: 'persisted-tag parity' has no
    referent — the parent rejects these merges at planning, so the
    projection states parent checked rejection and repaired
    persisted tag, F-LAGLEAD-PARENT-REJECTION), and sources and
    limitations re-point to revision 31 (`sha256` recorded at
    submission) and decision log 44–77; the whole projection is
    re-checked under the 64000-byte budget.

78. **Revision 32 — the round-63 design-only review's two findings**
    (2026-11-02; artifact `pre-pr-review-1793557659411.json`,
    REQUEST_CHANGES against revision 31, sha256
    `88807e8976ebcdfb0da48551fe70a941286ead7c95220a7dabc76cc6e17ad9f3`;
    both findings verified VALID at code on the frozen HEAD
    `5fe08a3307` before this revision).

    (i) F-DIAGNOSTIC-REPLAY (§3 step 4, §5, facts 15/16/18, §9 I10
    (xxii)). FINDING (P2): revision 31's positive replay-safety proof
    treats `GetFunctionIsProvenPureFoldableById` membership and plain
    `cast` calls as proof of effect-free replay, but equal returned
    values do not establish replay safety: `unhex` belongs to the
    curated set (`function.go:216`) and its registration carries none
    of the gate's exclusion flags (`list_builtIn.go:9276–9294`), yet
    malformed input appends warning 1411 (`unhexToBytes`,
    `func_unary.go:9277`, the `AppendWarningDiagnostic` at `:9294`;
    `TestUnhexWarningForInvalidHex`, `func_unary_test.go:6789–6807`,
    asserts exactly one warning for `'zz'`); and the blanket cast
    exemption includes the `NewExplicitCast` overload
    (`list_operator.go:2638–2645`) whose conversion paths append
    warnings (string→integer, `func_cast.go:7396`–`:7428`,
    `:8471`–`:8499`). A materialized producer exporting `CASE WHEN
    UNHEX(s) IS NOT NULL THEN a*b ELSE a*d END` with `s='zz'`
    therefore qualified for the shadow channel and would duplicate
    client-visible warnings. ROOT CAUSE: revision 31 proved VALUE
    replay-safety (deterministic, free of external side effects) but
    replay safety also has a DIAGNOSTIC half: a replicated evaluation
    must not duplicate the client-visible warning stream, and neither
    curated membership nor the plain-cast blanket supplies that
    evidence. DECISION: the positive proof gains a diagnostic-freedom
    tier, each kind carrying POSITIVE diagnostic-free evidence for
    the actual overload and conversion domains: (a) decimal
    arithmetic operators are error-only kernels — the whole-file
    warning-sink sweep of `arithmetic.go` and
    `arith_decimal_fast.go` finds zero `GetWarningSink`/
    `AppendWarningDiagnostic` references, so evaluation appends no
    diagnostic and an error aborts both evaluations identically;
    (b) a `cast` is admitted only when its argument and result types
    are BOTH decimal (`T_decimal64`/`T_decimal128`/`T_decimal256`) —
    the one conversion family whose `func_cast.go` paths carry no
    diagnostic append, the whole-file enumeration placing every
    append on string→integer (`:7396`/`:7409`/`:7428`/`:8471`/
    `:8482`/`:8499`), string→decimal inside
    `castModeAssignmentIgnore` (`:8645`/`:8986`/`:9082`, a mode
    explicit CAST never selects) or the time/json/mediumint families
    (`:1152`/`:3269`/`:5170`/`:5207`/`:9573`) — the blanket
    plain-`cast` exemption WITHDRAWN; (c) curated membership is
    narrowed to per-overload audited overloads: the audit table
    admits exactly `coalesce` (`CoalesceGeneral`,
    `func_binary.go:1198–1226`, a pure selector over
    already-evaluated arms whose process never reaches a warning
    sink; `CoalesceStr`, `:1227`, takes `_ *process.Process`); every
    other curated member — `unhex` the canonical counterexample — is
    UNPROVEN pending a recorded audit. Every unproven slot takes the
    existing in-place single-evaluation channel of fact 16, so no
    replica ever duplicates an observable effect or diagnostic; the
    revision-28/29 shadow witnesses classify PROVEN under the amended
    predicate (`coalesce` audited, `*` error-only, arm-alignment
    casts decimal→decimal) and keep the shadow channel
    byte-identically, so their evidence is reused unchanged.
    VALIDATION: §9 I10 (xxii) gains the deterministic
    parent/candidate/repaired WARNING-COUNT oracle family (g) —
    malformed-hex rows append exactly one warning 1411 per row on the
    parent and on the repaired engine, two per row on the
    always-shadow candidate — with masked-arm controls (unselected
    arms append nothing on any engine) and the zero-diagnostic
    proven-channel control; the channel-predicate UT table (e) gains
    the UNHEX, string-sourced-cast, unaudited-curated and
    decimal→decimal-cast rows.

    (ii) F-PLAN-SHAPE-SPLIT (§9 I10 (xxii) (a1)). FINDING (P3): cell
    (xxii) (a1)'s three-way oracle described a parent-green run that
    also required the repair's in-place `(65,6)` admission cast and
    widened `(76,6)` export, but the assigned parent's
    `decimalMultiplyReturnType` (HEAD `list_operator.go:209–224`)
    publishes `min(w1+w2, 65)` and has no shadow-admission machinery.
    DECISION: the oracle's assertions are separated into
    VERSION-SHARED assertions (exactly one SINK producer with exactly
    three SINK_SCAN readers and exactly ONE `rand` evaluation site in
    the producer projection — observable on parent, candidate and
    repaired alike) and VERSION-SPECIFIC assertions (parent: the
    export publishes `(65,6)`, no admission machinery; repaired: no
    shadow column, the `(65,6)` envelope cast spliced in place at the
    multiply seam inside the original export, the export publishing
    the widened `(76,6)`; failing candidate: the probe61 observation,
    TWO `rand` evaluation sites both admitted).

    (iii) REJECTED ALTERNATIVES: auditing every curated member now
    (the shadow witnesses need only `coalesce`; each further audit is
    recorded when a witness requires it, keeping the admitted surface
    minimal); treating warnings as errors for replay purposes (a
    warning-appending expression is simply unproven and takes the
    in-place channel — no engine behavior changes); counting
    diagnostics only on the repaired engine (a warning-count oracle
    is three-way exactly like the invocation-count oracle — the
    parent and candidate oracles are what make it discriminating).

    (iv) EXCLUSIONS: no change to the materialization classification
    (`cte_identity.go`); no change to
    `pkg/sql/plan/function/function.go` (the purity gate exists); no
    change to `unhex`'s registration or diagnostics (its warning
    behavior is pre-existing and correct); no relaxation of any
    revision-29 contract the Challenger validated; the
    eager-factoring rejection stands; entry 77 (i) is annotated
    kept-as-history for its narrowed tiers.

    (v) VALIDATION PLAN: the amended §9 I10 (xxii) witnesses ride
    decision 74 (vii)'s issue-UT ladder and live A/B channels on
    design PASS; revision-29/31 evidence whose inputs these
    amendments do not touch (the value-stable shadow witnesses, the
    covered-selector merges, the executed
    lag/lead integer-default receipts (the revision-31 planned
    with-default envelope witnesses were never executed and are
    superseded by the revision-33 re-planned oracles,
    F-LAGLEAD-PARENT-REJECTION), the
    107/107 ladder, the 13/13 anchor run, BVT 372/372 twice, the
    live P1–P7 receipts, the probe61 log) is reused under the
    validation-evidence contract; no baseline, race-belt or
    full-suite rerun.

    (vi) SEQUENCING AND PRODUCTION SET: the channel-predicate
    narrowing lands inside the already-owned
    `pkg/sql/plan/build_ddl.go`; the planned production set stays 21
    files with the `pkg/vm/process` registry carrier and no new
    owner; witnesses land in the already-owned issue UT files. THE
    PROJECTION: the two-channel admission sentence gains the
    diagnostic-freedom condition and the `unhex` exclusion, and
    sources and limitations re-point to revision 32 (`sha256`
    recorded at submission) and decision log 44–78; the whole
    projection is re-checked under the 64000-byte budget.

79. **Revision 33 — the round-64 design-only review's finding**
    (2026-11-02; artifact `pre-pr-review-1793558552709.json`,
    REQUEST_CHANGES against revision 32, sha256
    `3a5558ffb32a397c76e56760a5281faa475134fb40f57bf35ac75842fcc6742f`;
    the finding verified VALID at code on the frozen HEAD
    `5fe08a3307e246e1ab9951ab690b50557af0f509`).

    (i) F-LAGLEAD-PARENT-REJECTION (§4.1 lag/lead bullet, §9 I10
    (xxiii), the §6 observability clause; annotations on entries
    76 (ii), 77 (ii)/(iv)/(vii) and 78 (v)). FINDING (quoted
    action): "Reconcile §4.1, the decision log, validation plan
    and public contracts with the complete parent reconciliation
    rule, including its final width check. Distinguish preserved
    behavior from any intentionally expanded behavior and define
    its storage policy explicitly. Plan parent assertions for the
    actual checked rejection instead of nonexistent persisted
    schemas." Section 4.1's bullet and cell (xxiii) claimed the
    parent persists `(66,1)` for the `CAST(0 AS DECIMAL(65,1))`
    default pair over `(38,0)` operands and `(75,10)` for the
    `DECIMAL(30,10)` default pair. VERIFICATION: the claim was
    written against the WORKTREE-MODIFIED `base_binder.go` —
    revision 31's citations (`:9206`, called `:8961`, lookup
    `:9323`, family `:9326`) are the modified file's line
    numbers, whose three width checks the frozen diff widens —
    while the assigned parent (`git show
    5fe08a3307:pkg/sql/plan/base_binder.go`) carries
    `reconcileLagLeadDefaultType` at `:9147–9288` (called at
    `:8902`) with FOUR `LAG/LEAD common precision exceeds SQL
    limit 65` rejections: the bool-numeric path (`:9208`), the
    decimal×integer/bit/year path (`:9239`, whose comment records
    the design intent — "The shared exact-arithmetic helper
    permits 76 internal digits, but this public result must
    remain expressible in persisted SQL DDL"), the
    coalesce-lookup-failure fallback (`:9261`) and the final
    post-lookup width check the review cites (`:9284–9285`).
    Witness arithmetic: the parent's `(65,0)` publication merged
    with `(65,1)` derives common `(66,1)` — 66 > 65, a checked
    planning rejection; with `(30,10)` it derives `(75,10)` —
    likewise rejected. The parent persists NOTHING for either
    shape. LIVE PARENT RECEIPT, already on disk and re-confirmed:
    `/tmp/30938_bvt_dtype_base.log` row 188 — `create table
    lag_def_lit as select lag(1, 0, <66-digit literal>) over ()
    ...` fails on the base engine with exactly this family
    ("Data truncation: data out of range: data type DECIMAL,
    LAG/LEAD common precision exceeds SQL limit 65"), while the
    integer-default lag/lead rows over fitting operands succeed
    with `(65,0)` metadata (rows 181/182) and the wide-operand
    rows fail with the issue's own ERROR 1690 (rows 186/187).
    ASYMMETRY CONFIRMED: the ordinary coalesce and set-operation
    common-type owners carry NO >65 width check — the committed
    UNION-family `(66,1)` rows (`.result` lines 718–765 and
    845–853) are genuine parent receipts — so only the LAG/LEAD
    reconcile rejects the 66–76 band, and the revision-30/31
    claims for the OTHER selectors (which converge through the
    ordinary coalesce common) stand verified. ROOT CAUSE: the
    revision-31 author read the reconcile in the worktree, where
    the frozen diff had already replaced the 65 bound with 76,
    and recorded the repaired behavior as the parent's.
    DECISION: (a) §4.1's bullet is rewritten to state the
    parent's complete rule and to record the expansion
    explicitly: the frozen diff's three `>76` checks (worktree
    `:9272`/`:9304`/`:9353`, message "exceeds the Decimal256
    limit 76") admit the 66–76 band, and the admission is
    PROVENANCE-BLIND — every decimal default merge whose derived
    common fits 76 binds, multiplication present or not (the
    committed scale-70 `f4_sel`/`f4_unsel` shapes bind
    `(70,70)`; `lag_def_lit` binds `(66,0)`) — sanctioned by the
    issue's removal of the 65-digit expression cap, required for
    coherence with the revision-30 completion (a rejection here
    would leave LAG/LEAD declining shapes its sibling selectors
    accept through the check-free ordinary coalesce common) and
    consistent with MySQL's expression typing, which declares no
    width rejection for intermediate results; section 3076
    already states the change band exactly — only the
    previously erroring 66–76 default-merge outcomes change.
    (b) STORAGE POLICY, explicit: admitted merges persist the
    reconcile-derived declared tag (`(66,1)`, `(75,10)`,
    `(70,70)`, `(66,0)`) at every storage boundary — no clip,
    no scale rejection; the revision-31 clip removal stands with
    the corrected justification that the envelope reproduces the
    REPAIRED reconcile rule-for-rule for the expanded band (no
    parent persisted schema exists to reproduce); a derived
    width above 76 keeps the checked rejection with the updated
    message; the lookup-failure fallback (worktree `:9326`)
    retains the parent's message text and is re-armed
    identically by the entry-76 (ii) completion. (c) ORACLES
    re-planned three-way per the review action: §9 I10 (xxiii)'s
    with-default assertions become PARENT-SPECIFIC (green) —
    checked planning rejection with the exact family text at the
    cited HEAD sites, never a persisted-schema assertion;
    REPAIRED-SPECIFIC (green) — success, exact values and the
    persisted reconcile-derived tag at CTAS, view and prepared
    surfaces; CANDIDATE-SPECIFIC (red) — the frozen
    revision-30 envelope's `(65,1)`/`(65,10)` clip and scale
    rejection. The `(65,20)` undeclarable control fails on BOTH
    engines (message text diverges as recorded); the stale
    committed `.result` rows the frozen envelope produced —
    `lag_def_lit` persisting `(65,0)` (lines 321–324) and the
    `f4_sel`/`f4_unsel` scale rejections (lines 489/491) — are
    restated to `(66,0)` and `(70,70)` when the envelope repair
    lands at the implementation turn.

    (ii) REJECTED ALTERNATIVES: gating the widened checks on
    multiplication provenance (a provenance test inside the
    parent's typing owner re-narrows the expansion the issue
    sanctions, leaves LAG/LEAD the lone selector declining
    `(66,1)` merges its siblings accept, and splits one rule
    across two loci); reverting the three checks wholesale (the
    revision-30 completion's bound merges would fail planning,
    re-opening F-SELECTOR-PARENT-DOMAIN); keeping the 65-clip
    at the storage boundary (revision 31's finding in reverse —
    a second owner re-deriving a narrower tag than the reconcile
    bound); asserting parent parity through persisted schemas
    for these two shapes (the parent's checked planning
    rejection IS the parity oracle — there is no persisted
    schema to compare).

    (iii) EXCLUSIONS: no production change — the frozen diff's
    three widened checks, the unchanged fallback site and the
    revision-31 envelope repair are exactly the contract this
    revision records; no change to the ordinary coalesce or
    set-operation common owners (their check-free behavior is
    the asymmetry's verified half); no change to any
    revision-29 contract the Challenger validated; the design
    gate bars implementation validation this turn, so the
    re-planned oracles are recorded as planned and are NOT
    claimed as executed.

    (iv) VALIDATION PLAN: on design PASS the cheapest
    discriminating regressions run first — the two with-default
    witness shapes and the `f4`/`lag_def_lit` band as parent
    checked-rejection oracles (the recorded base receipts where
    the harness already ran them, live against the base
    endpoint where the plan re-runs them) and repaired
    success/persist oracles, riding decision 74 (vii)'s
    issue-UT ladder and live A/B channels; the executed
    lag/lead integer-default receipts (base rows 181/182,
    186/187 and 188, and their fixed-engine counterparts in the
    committed `.result`), the revision-28/29 shadow witnesses,
    the covered-selector merges, the 107/107 ladder, the 13/13
    anchor run, BVT 372/372 twice, the live P1–P7 receipts,
    the probe61 log and the 16 Challenger receipts are reused
    under the validation-evidence contract — their inputs,
    modes and terminal results are untouched by this revision;
    no baseline, race-belt or full-suite rerun.

    (v) SEQUENCING AND PRODUCTION SET: no production file
    moves; the planned production set stays 21 files with the
    `pkg/vm/process` registry carrier and no new owner (ANNOTATED
    kept-as-history in revision 34: the production set GROWS for the
    band fence — `pkg/sql/plan/persisted_ip.go` new to the diff,
    three `pkg/sql/compile` floor sites new to the diff, the
    `pkg/pb/plan/string_literal_form.go` feature block and their UT
    files — decision log 80 (v)); the re-planned witnesses land in the
    already-owned issue UT
    files and the BVT fixture at the implementation turn. THE
    PROJECTION: the LAG/LEAD semantics sentence is restated as
    the expansion-band contract (parent checked rejection for
    derived width >65; intentional admission ≤76 persisting
    the reconcile-derived tag; >76 residual rejection on both
    engines), the edge-case and error entries carry the two
    message texts, and sources and limitations re-point to
    revision 33 (`sha256` recorded at submission), decision
    log 44–79 and the round-64 artifact; the whole projection
    is re-checked under the 64000-byte budget.

80. **Revision 34 — the round-65 design-only review's two findings**
    (artifact `pre-pr-review-1793559592252.json`, a design-only
    REQUEST_CHANGES against revision 33, sha256
    `fd1524524e2879bef0627cd3b0a8b7f78b74ea5e8fb805895b1ca013b47eb16a`).

    (i) FINDING ONE (P2, quoted): "Close the compatibility contract
    for newly persisted LAG/LEAD domains" — design lines 1145–1159
    persist the newly admitted `(66,1)`/`(75,10)`/`(70,70)` tags while
    §6 M4 (:3000–3001), §8 (:3153–3158) and §9 I9 "still promise
    declarable, parent-identical records readable and rebindable after
    rollback"; a band view's stored definition "encounters the
    assigned parent's >65 planning rejection at
    base_binder.go:9284–9285"; "the proposed multiplication
    execution floor does not close persisted-view admission;
    RequiredPersistedExpressionProtocolVersion in
    pkg/sql/plan/persisted_ip.go supplies no requirement for this
    expansion"; public_contracts semantics[3]/edge_cases[4] "retain
    unconditional parent-schema guarantees". ACTION (quoted):
    "Define authoring, replay, mixed-version and rollback behavior
    for the expanded LAG/LEAD persistence band, including statements
    without multiplication. Reconcile I2, the view and compatibility
    sections, and public_contracts with that decision. Extend the
    validation plan with version-specific view-read/replay and
    schema-round-trip outcomes; remove unsupported universal
    parent-parity assertions."

    VERIFICATION (this turn, against the frozen HEAD and the
    worktree; the finding is VALID on every evidence point): §4.1's
    STORAGE POLICY persists the three band tags (:1145–1159, read
    verbatim); M4's downgrade sentence (:3000–3001), §8's
    mixed-version claims (:3138–3158) and I9's downgrade/rebind
    converse (:3242) are universal and false for the band;
    `RequiredPersistedExpressionProtocolVersion`
    (`persisted_ip.go:90–182`, read in full) maps features v72…v130
    with no entry for this expansion, while
    `DecimalMultiplicationCarrierDomain` maps only at the execution
    sites (`expression_protocol.go:154–155`,
    `remoterun.go:158–159` and `:2478–2480`,
    `expression_destination_protocol.go:54–56`); the marker
    machinery the fix rides exists at HEAD — the authoring gates
    (`build_ddl.go:266–351`), the persisted field
    (`ViewData.RequiredProtocolVersion`, `:497`), the replay check
    (`view_regeneration.go:182–202`) and the schema-inference
    observation (`view_schema_inference.go:355–375`) — so a minted
    v132 marker is enforced by below-floor binaries with no code
    change on their side; the feature walk visits window functions
    (`string_literal_form.go:1508`/`:1617`/`:1697`), and the band
    signal is exact: a `lag`/`lead` decimal256 result wider than 65
    arises only through the repaired reconcile (the parent rejects
    the derivation at planning; `mysql_sql.y:15705–15708` caps SQL
    declarations at 65); `buildDropView` (`build_ddl.go:7679–7717`)
    does not replay the definition, so DROP removes a band view
    below the floor; the parent's own prepared-CTAS `(66,1)`
    receipts establish that above-65 catalog records read on old
    binaries. A design-premise/contract defect decidable by
    inspection, so no probe harness was built and none was needed;
    the design gate bars implementation validation this turn.

    DECISION — F-LAGLEAD-PERSISTED-FLOOR: (a) one new feature
    signal `DecimalWindowDefaultCarrierDomain`, set by the
    `RequiredRemoteExpressionFeatures` walk when a `lag`/`lead`
    window function (registry ids 119/121) carries a decimal256
    result wider than 65 — provenance-blind by construction, so the
    no-multiplication band shapes (`(70,70)`, `(66,0)`) are covered
    exactly as the action requires (ANNOTATED kept-as-history in
    revision 35: the width-alone detector is superseded by RECORDED
    newly admitted reconcile evidence — F-LAGLEAD-SIGNAL-NEWLY-ADMITTED,
    decision log 81); (b) the signal maps to v132 at
    the persisted site (`RequiredPersistedExpressionProtocolVersion`
    — the review's named gap) and at the three execution sites
    beside the multiplication entries (scope floor, stream fence,
    destination guard); `DecimalMultiplicationCarrierDomain`
    deliberately stays EXECUTION-ONLY — its persisted records
    re-derive byte-identically below the floor (I2), so the
    ScalarDivision precedent holds for it and persisted-view
    admission is closed for exactly the expansion, nothing wider
    (ANNOTATED kept-as-history in revision 36: the
    persisted-site mapping moves to the pre-existing
    `BindContext.persistedExpressionProtocolRequirement`
    bind-time channel and the three execution-site mappings
    are WITHDRAWN — multiplication-provenance band
    statements keep the tree-derived floor while
    provenance-free band statements execute
    value-identically on width-generic kernels, so
    compilation and pipeline gates need no band evidence;
    F-LAGLEAD-PERSISTED-CHANNEL, decision log 82 (b));
    (c) the four-part contract: AUTHORING mints
    `RequiredProtocolVersion: 132` into the view record through the
    established observation channels, barred in a skew window by the
    catalog admission fence with the checked `…local catalog
    admission protocol version 132…` family; REPLAY/READ/DESCRIBE
    on a below-floor binary raise the checked `persisted expression
    semantics require all CNs to support protocol version 132`
    family before the parent's planning rejection would fire; MIXED
    VERSION execution demotes band statements to the upgraded
    coordinator and guards below-floor destinations exactly as for
    widened multiplications (ANNOTATED kept-as-history in
    revision 36: band statements mint no execution floor and
    keep ordinary placement — F-LAGLEAD-PERSISTED-CHANNEL,
    decision log 82 (b)); ROLLBACK leaves band views unreadable
    below the floor — loud, checked, recoverable by DROP (no
    replay) or re-upgrade — while band CTAS tables keep reading
    in-domain data in the parent's own above-65 record form and
    every non-band record re-derives byte-identically (I2/I4 as
    scoped); (d) the reconciliations land at §2 I2/I4/I8, §3 step
    6, §4.1 (the bullet's cross-version paragraph), §6 M4, §8
    (both paragraphs and the observability clause), §9 (I2 row
    title, I9 row, View surfaces row, I10 (xxiii)) and the
    projection, with kept-as-history annotations on entries 77 and
    79.

    REJECTED ALTERNATIVES: mapping
    `DecimalMultiplicationCarrierDomain` on the persisted side too
    (over-fencing — plain multiplication views re-derive
    byte-identically below the floor, so barring their reads after
    rollback narrows compatibility with zero divergence prevented);
    leaving replay to the parent's raw planning rejection with no
    marker (the review's identified gap — the message names a SQL
    limit rather than the version contract, and authoring,
    mixed-version and rollback behavior stay accidental); clipping
    band tags to ≤65 at persistence (entry 79 (ii)'s
    already-rejected second owner re-deriving a narrower tag than
    the reconcile bound); provenance-gating the band signal on
    multiplication presence (the width>65 lag/lead decimal256 result
    is reachable only through the repaired reconcile, so a
    provenance conjunct adds nothing and splits one rule across two
    loci — entry 79 (ii)).

    (ii) FINDING TWO (P3, quoted): "Clarify cast metadata traversal
    and use a reachable positive control" — the §5 predicate
    "admits decimal casts but requires every leaf to be Lit or Col",
    while "actual casts contain an Expr_T target descriptor as their
    second argument (pkg/sql/plan/make.go:1073–1083)", and §9 I10
    "retains CAST(a*b AS DECIMAL(76,0)) as a positive control,
    although mysql_sql.y:15708 rejects SQL declarations above 65".
    ACTION (quoted): "Specify validation of the cast target
    descriptor as metadata and recurse through the value argument.
    Test an actual binder-produced legal decimal cast, or explicitly
    identify the width-76 control as an internal typed-expression
    fixture." VERIFIED VALID: the cast builder (`make.go:1073–1083`)
    forms `Args: {expr, t}` with `t` an `Expr_T` (`plan.TargetType`),
    so an unqualified Lit/Col leaf rule classifies every real cast
    UNPROVEN and contradicts clause (b); `mysql_sql.y:15705–15708`
    raises "For decimal(M), M must between 0 and 65." for
    declarations above 65, and the control's single occurrence
    (:3243) is unreachable through SQL. DECISION —
    F-CAST-DESCRIPTOR-LEAVES: §5's predicate now validates the
    descriptor as METADATA (its `Typ` decides the decimal→decimal
    family test) and scopes the leaf rule to VALUE arguments; the
    positive control becomes the SQL-reachable `CAST(a*b AS
    DECIMAL(65,0))` (decimal256→decimal256, inside the
    warning-free family, exercising the binder's own descriptor
    shape); entry 77 annotated. REJECTED ALTERNATIVE: keeping the
    `(76,0)` control labeled as an internal typed-expression fixture
    (a SQL-reachable control exercises the binder-produced
    descriptor the finding names, at no cost).

    (iii) EXCLUSIONS: no production change this turn — the frozen
    revision-29 implementation stays byte-identical and the design
    gate bars implementation validation, so every oracle this entry
    adds is recorded as planned and NOT claimed as executed; no
    change to the ordinary coalesce or set-operation owners, to any
    Challenger-validated contract, or to the marker machinery itself
    (it pre-exists at the assigned base; the change mints signals
    through it).

    (iv) VALIDATION PLAN on design PASS, cheapest first: the
    band-signal (`DecimalWindowDefaultCarrierDomain`) and
    floor-mapping UTs (pure plan fixtures, no
    service), then the view-marker catalog oracles and the
    forged-floor authoring/replay gate UTs (the established
    temporal-incomplete test shape), then the live band matrix on
    the owned fixed service with below-floor outcomes asserted
    through forged-floor runtimes — riding decision 74 (vii)'s
    channels; the revision-33 reuse set (the executed lag/lead
    receipts, the shadow witnesses, the covered-selector merges, the
    107/107 ladder, the 13/13 anchor run, BVT 372/372 twice, live
    P1–P7, probe61, the 16 Challenger receipts) is reused under the
    validation-evidence contract — this revision changes no input,
    mode or terminal result; no baseline, race-belt or full-suite
    rerun.

    (v) SEQUENCING AND PRODUCTION SET: the planned production set
    GROWS to close the floor — `pkg/sql/plan/persisted_ip.go` (new
    owner: the v132 persisted mapping), the three execution sites
    `pkg/sql/compile/expression_protocol.go`,
    `pkg/sql/compile/remoterun.go` and
    `pkg/sql/compile/expression_destination_protocol.go` (one band
    entry beside each multiplication entry), the
    `pkg/pb/plan/string_literal_form.go` feature block (the field,
    its `Any()` inclusion and the walk detection) — plus their UT
    files and the view-marker oracles, all landing only after design
    PASS; the witnesses land in the already-owned issue UT files and
    the BVT fixture. THE PROJECTION: semantics[3] and edge_cases[4]
    gain the band exception, the LAG/LEAD semantics/edge/error
    entries gain the version clause and the two protocol families,
    limitations gains the band mixed-version clause and re-points to
    revision 34 with its sha256 and the round-65 artifact, sources
    re-points likewise, and the whole projection is re-checked under
    the 64000-byte budget.

81. **Revision 35 — the round-66 design-only review's finding**
    (artifact `pre-pr-review-1793561068798.json`, a design-only
    REQUEST_CHANGES against revision 34, sha256
    `063b7fe54669ae0676fe1dbfa0633f42cb456ceea4d009f4ca73f69acae11a37`).

    (i) FINDING (P2, quoted verbatim): "Restrict the persisted floor
    to newly admitted LAG/LEAD reconciliations" — evidence: "Design
    lines 1209–1213 and 8118–8132 classify every decimal256 LAG/LEAD
    result wider than 65 as newly admitted, then map it to the v132
    persisted floor. That premise is false on the assigned parent:
    function/list_window.go:142–193 returns the first argument's
    type, and HEAD:base_binder.go:8898 enters reconciliation only
    when a third argument exists and differs in type. Consequently,
    LAG over a 66-digit literal without a default already succeeds;
    matching wide defaults and NULL defaults are also preserved, as
    recorded by issue_30938_decimal_mul_precision.test:283–289. The
    proposed detector fences these unchanged expressions,
    unnecessarily rejecting view authoring during upgrade and view
    reads after rollback through persisted_ip.go:250–297 and
    view_regeneration.go:190–199. This contradicts the non-band
    compatibility contract. Section 9 I9 tests only narrow/non-window
    negative controls, so its planned tests miss the regression."
    ACTION (quoted): "Define the signal from evidence of newly
    admitted default reconciliation rather than result width alone,
    and specify how that distinction survives binding/finalization.
    Align the design and public contracts. Add planned negative
    feature, authoring and replay oracles for wide literals with
    absent, NULL and matching defaults, plus a no-default
    multiplication view; these must gain no v132 persisted floor.
    Retain positive coverage for the provenance-free scale-70
    expansion."

    (ii) VERIFICATION — VALID at code on the frozen HEAD
    5fe08a3307e246e1ab9951ab690b50557af0f509 (byte reads and the
    committed receipts; a design-premise/contract defect decidable by
    inspection, so no probe harness; the design gate bars
    implementation validation this turn). HEAD `list_window.go`
    LAG/LEAD `retType` returns `parameters[0]` (registry blocks
    `:142–193`, the reviewer's citation); HEAD
    `base_binder.go:8893`/`:8898` enters the reconcile only when a
    third argument exists with a differing type;
    `reconcileLagLeadDefaultType` short-circuits a T_any default at
    `:9148–9150`, and the caller's `:8980–8983` fallback casts the
    default to the value type with NO width check; the >65
    rejections (`:9208`/`:9239`/`:9261`/`:9285`) sit INSIDE the
    reconcile alone. Committed fixed-engine receipts
    (`test/distributed/cases/dtype/issue_30938_decimal_mul_precision
    .test:283–289` with `.result:480–487`): matching and NULL wide
    defaults persist `(66,0)` and return the exact value. Under
    revision 34's width-alone detector each of these
    parent-supported shapes mints the v132 marker: skew-window
    authoring is barred and below-floor read/replay/describe/
    schema-inference raise the persisted-protocol family for records
    the parent authors and reads today — the reviewed regression
    against I2's non-band byte-identical contract; a no-default
    multiplication view persists the parent's `(65,0)` envelope tag
    and needs no floor, exactly as the action states. §9 I9's
    negatives covered only a ≤65 lag/lead result and non-window
    expressions, exactly as reviewed.

    DECISION — F-LAGLEAD-SIGNAL-NEWLY-ADMITTED: (a) the signal
    `DecimalWindowDefaultCarrierDomain` is RECORDED evidence that
    binding newly admitted a `lag`/`lead` default reconciliation —
    the `:8898` gate passed and the repaired reconcile returned a
    decimal common of width 66–76, the parent's rejection region;
    result width alone sets nothing. Parent-supported wide results
    (absent default, NULL default, matching default, no-default
    `lag`/`lead` over a widened product or a genuinely wide
    parent-inferred column, and every reconcile deriving ≤65)
    record nothing, mint no marker, keep normal placement, and
    re-derive byte-identically below the floor (ANNOTATED
    kept-as-history in revision 36: the changed-gate trigger
    is superseded by the PARENT-gate determination — newly
    admitted behavior is decided from the parent's operand
    domains and the parent's `:8898` gate, covering both
    gate transitions; F-LAGLEAD-PARENT-GATE-ADMISSION,
    decision log 82 (a)); (b) SURVIVAL
    THROUGH BINDING/FINALIZATION: the reconcile locus records the
    admission into the statement-scoped bind-time decision ledger —
    the same content-keyed carrier threading coercive decisions
    through finalization and DeepCopyExpr copies (§9 I10) — and the
    `RequiredRemoteExpressionFeatures` walk sets the signal from
    that record at statement scope (the floor fences whole
    pipelines and whole view records, so statement scope loses no
    precision); the record is recomputed at every binding
    (prepare, execute rebind, view authoring, replay on a
    floor-satisfying binary) and the computed v132 floor flows
    through the identical plan-lifecycle channels as the
    multiplication feature's floor; nothing is serialized — the
    persisted face is the minted marker alone (ANNOTATED
    kept-as-history in revision 36: no ledger-to-walk handoff
    exists — `RequiredRemoteExpressionFeatures` receives no
    ledger and every execution gate reconstructs features
    from trees; the band's only consumer is the
    view-authoring marker path, reached through the
    pre-existing
    `BindContext.persistedExpressionProtocolRequirement`
    channel, and execution gates are proven to need no band
    evidence; F-LAGLEAD-PERSISTED-CHANNEL, decision log
    82 (b)); (c) the negative
    oracles of record (planned, §9 I9/I10 (xxiii)): feature UTs
    over the four negative shapes set nothing while the
    `(66,1)`/`(75,10)` witnesses and the provenance-free scale-70
    merge keep setting the signal; authoring negatives mint no
    marker and persist parent-identical records; replay/read/
    describe/schema-inference negatives run byte-identical on a
    forged-below-floor runtime; rollback round-trips; (d)
    reconciliations landed at §2, §4.1, §6 M4, §8, §9 I9 and I10
    (xxiii), the projection (sources and limitations re-pointed to
    this revision), and a kept-as-history annotation on entry 80
    (a).

    REJECTED ALTERNATIVES: keeping the width-alone detector (the
    reviewed regression); re-deriving the distinction from
    final-plan shape — a binder-inserted cast on the first argument
    marks a reconcile, but the cast is elided where the derived
    common equals the value type, under-fencing genuinely band
    views back to the parent's raw planning rejection that the
    round-65 review rejected; testing third-argument presence alone
    (matching and NULL defaults stay over-fenced); exempting
    literal first arguments (a genuinely wide parent-inferred
    COLUMN with no default over-fences identically — the
    distinction is the reconcile event, not operand provenance).

    EXCLUSIONS: no production change this turn (the frozen
    revision-29 implementation stays byte-identical pending design
    PASS; the band floor machinery remains planned production set);
    no executed oracles — every added assertion is planned, not
    executed; the multiplication feature stays execution-only
    (ScalarDivision precedent).

    VALIDATION PLAN (cheapest first, on design PASS): the
    band-signal UT table (positive and negative rows) and the
    floor-mapping UTs; then the authoring/replay negative gates on
    forged-floor runtimes beside the band matrix; then the live
    band matrix with the negative shapes' version-specific
    outcomes. SEQUENCING: design-only checkpoint → independent
    review → implementation per decision log 80 (v) extended with
    the ledger record and these oracles. PROJECTION: semantics,
    edge_cases, errors and limitations restate the signal and the
    negative shapes' outcomes; sources and limitations re-point to
    this revision (sha256 recorded at submission) and the round-66
    artifact; the whole projection is re-checked under
    the 64000-byte budget.

82. **Revision 36 — the round-67 design-only review's two findings**
    (artifact `pre-pr-review-1793561911159.json`, a design-only
    REQUEST_CHANGES against revision 35, sha256
    `98249bc89b0cea34b4fd42e0c99bbee5d7a335274a8be4c39c14bae838679d09`).

    (i) FINDING ONE (P2, quoted verbatim): "Account for reconciliation gates
    changed by multiplication widening" — evidence: "Design lines 1213–1232 and
    8333–8342 require the repaired reconciliation gate to execute before
    recording the band, and classify matching defaults as parent-supported.
    Counterexample: LAG(a*b,0,N76) OVER (), where a,b are DECIMAL(38,0) columns
    holding 1 and N76 is a 76-digit all-nine numeric literal. The parent
    multiplication publishes (65,0), so HEAD:base_binder.go:8898 enters
    reconciliation against the (76,0) default and rejects its common width at
    :9284–9285. The proposed multiplication publishes (76,0), making both
    argument types equal; the gate is skipped and no admission record exists.
    The resulting view therefore misses the v132 persisted fence despite being
    unreplayable on the parent. The planned matching-literal negatives do not
    cover this gate transition." ACTION (quoted): "Determine newly admitted
    behavior using the parent operand domains and parent gate, including cases
    where widening makes the repaired gate disappear. Align the design and
    public contracts. Plan a parent-rejection/new-success view witness for this
    product-plus-matching-default case, asserting its v132 marker and
    below-floor rejection; retain genuinely parent-supported matching-literal
    negatives."

    VERIFICATION — VALID at code on the frozen HEAD
    5fe08a3307e246e1ab9951ab690b50557af0f509 (byte reads of HEAD
    and worktree sources; a design-premise/contract defect
    decidable by inspection, so no probe harness; the design gate
    bars implementation validation this turn). HEAD
    `base_binder.go:8898` enters the reconcile when a third
    argument exists with a differing type; on the parent the
    `(38,0)²` multiply publishes `(65,0)`, the gate fires against
    the literal's `(76,0)`, and the reconcile's derived common is
    rejected at `:9284–9285`; on the changed engine the widened
    publication `(76,0)` equals the default's type, the gate is
    skipped, and revision 35's trigger — the changed engine's
    gate passing — records nothing, so the view persists `(76,0)`
    with no marker and a below-floor binary replays the
    definition into the parent's raw planning rejection: the
    reviewed hole. The symmetric transition confirms the same
    root: on `LAG(a*b,0,CAST(0 AS DECIMAL(65,0)))` the parent's
    gate is SKIPPED (published `(65,0)` equals the default), the
    parent binds `(65,0)` with no rejection and persists its
    byte-identical envelope record (the committed fixed-engine
    receipts `.result:308–316`/`:358–366` persist the parent's
    `(65,0)` for with-default lag CTAS), while the changed
    engine's gate RUNS and derives `(76,0)` — revision 35 records
    the band and mints the marker over a parent-supported record,
    contradicting I2 exactly as the round-66 width-alone detector
    did. The changed engine's gate position is movable by the
    widening itself, so it cannot be the band's determinant.

    DECISION — F-LAGLEAD-PARENT-GATE-ADMISSION: (a) the band is
    determined at the `lag`/`lead` binding locus from the
    PARENT's operand domains and the PARENT's gate: the value
    argument's parent-published domain is recovered through the
    statement-scoped decision ledger's recorded widening
    decisions (the current type when the argument carries no
    widened-multiplication provenance — genuine wide columns and
    literals keep their natural width), the parent's `:8898` gate
    is evaluated over those parent domains, and the parent's
    reconcile is evaluated over them; BAND holds exactly when the
    parent's gate is entered, the parent-domain reconcile derives
    a decimal common wider than 65 (the parent's planning
    rejection), and the changed engine admits the derivation
    (width ≤76). The case matrix: the `(66,1)`/`(75,10)`
    witnesses and the provenance-free scale-70 merge stay band;
    the GATE-DISAPPEARANCE `LAG(a*b,0,N76)` becomes band
    (persists `(76,0)`, mints the marker); the GATE-APPEARANCE
    mirror `LAG(a*b,0,CAST(0 AS DECIMAL(65,0)))`, genuine
    matching defaults, NULL defaults, absent defaults, no-default
    wide results and every parent-domain reconcile deriving ≤65
    stay non-band; derivations above 76 stay rejected on both
    engines; (b) the oracles of record (planned, §9 I9/I10
    (xxiii)): the parent-rejection/new-success GATE-DISAPPEARANCE
    view witness asserting its v132 marker and below-floor
    rejection, the GATE-APPEARANCE mirror negative asserting no
    marker and a byte-identical `(65,0)` record, the genuinely
    parent-supported matching-literal negatives retained, and the
    band-signal UT table re-keyed on the parent-gate
    determination; (c) reconciliations landed at §4.1, §9 I9 and
    I10 (xxiii), the projection, and a kept-as-history annotation
    on entry 81 (a).

    REJECTED ALTERNATIVES: keeping the changed-gate trigger (the
    reviewed hole — the widening moves the gate); keying on the
    changed reconcile's derived width (the APPEAR case
    over-fences a byte-identical `(65,0)` record); re-deriving
    parent domains from final-plan shape (the alignment cast is
    elided where the derived common equals the value type —
    entry 81's own rejected alternative); declaring every wide
    lag/lead view band (the round-66 width-alone regression).

    (ii) FINDING TWO (P2, quoted verbatim): "Define the ledger-to-protocol
    handoff after planning" — evidence: "Design lines 8343–8355 place the signal
    exclusively in the statement ledger and say its floor follows
    multiplication's existing lifecycle channels. Those channels reconstruct
    multiplication features from expression trees; they do not retain bind-time
    evidence. RequiredRemoteExpressionFeatures(owner) at
    pkg/pb/plan/string_literal_form.go:945 receives no ledger. BuildPlan
    releases the registry on return (build.go:735;
    decimal_boundary_registry.go:120–138), retaining a claim only for CTAS.
    Later placement and pipeline analysis independently inspect Query/Pipeline
    objects (expression_protocol.go:38; remoterun.go:2187). The proposed design
    names no handoff that preserves the band evidence across these boundaries or
    cached prepared SELECT executions." ACTION (quoted): "Specify the concrete
    carrier, APIs, publication point and lifetime connecting binding evidence to
    persisted-feature analysis, compilation and pipeline gates, including
    ordinary and cached prepared statements. Extend the planned validation with
    post-planning-release, cached-execution and send-time assertions for a
    provenance-free scale-70 band statement, plus isolation controls."

    VERIFICATION — VALID at code on the frozen HEAD and the
    worktree (byte reads; the same inspection-decidable contract
    class). Worktree `RequiredRemoteExpressionFeatures(owner
    any)` (`pkg/pb/plan/string_literal_form.go:945`) takes no
    ledger argument; `BuildPlan`'s defer releases the
    decimal-boundary registry (`build.go:728–745`), and
    `decimal_boundary_registry.go:100–145` stashes a read-once
    claim only for prepared-CTAS generations; every execution
    gate reconstructs features from plan trees —
    `expression_protocol.go:38` and `:154–155`,
    `remoterun.go:158–159`/`:2187`/`:2478–2480`,
    `expression_destination_protocol.go:54–56`. No channel named
    by revision 35 carries bind-time band evidence across the
    planning boundary, exactly as reviewed.

    DECISION — F-LAGLEAD-PERSISTED-CHANNEL: (a) CARRIER AND
    APIs: the band's only consumer is the view-authoring marker
    path, and the handoff rides the PRE-EXISTING
    `BindContext.persistedExpressionProtocolRequirement *int64`
    (`types.go:945–949`), installed exclusively for
    view-definition binding (`builder.persistedViewTarget`
    non-empty, `bind_context.go:64–67`) and shared by pointer
    with every nested query block (`:70`) (ANNOTATED kept-as-history
    in revision 37: the every-block claim missed `bindView`'s
    isolated expansion context — superseded by the explicit
    expansion share, F-LAGLEAD-VIEW-EXPANSION-PROPAGATION,
    decision log 83 (i)); at the `lag`/`lead`
    binding locus a sibling of the established
    `observeExpressionProtocol` raise
    (`persisted_ip.go:190–208`) lifts the shared requirement to
    v132 when BAND holds (entry 82 (i)); non-view statements
    hold a nil field, so the raise is a no-op by construction
    and no band evidence survives binding; (b) PUBLICATION POINT
    AND LIFETIME: the established path consumes the raise
    unchanged — `captureColumnTypes` (`build_ddl.go:287–291`)
    into the pre-optimize requirement, the post-optimize maximum
    (`:326–331`), the authoring gates (`:332–341`) and the
    `ViewData.RequiredProtocolVersion` mint (`:451–453`, `:467`);
    the marker is the only serialized face, replay, describe and
    schema inference recompute the raise at every rebinding on a
    floor-satisfying binary, and a below-floor binary's gate
    fires before any rebinding; (c) COMPILATION AND PIPELINE
    GATES NEED NO BAND EVIDENCE, by construction: a band
    statement carrying widened-multiplication provenance is
    fenced at every execution site by the tree-reconstructible
    `DecimalMultiplicationCarrierDomain` floor, and a
    provenance-free band statement executes VALUE-IDENTICALLY on
    below-floor kernels — the lag/lead window kernels and the
    decimal256 cast kernels are width-generic and untouched by
    this change, the parent already binds and executes
    coalesce-family plans at widths up to 76, and the band's
    only parent/changed divergence is the bind-time planning
    rejection — so revision 34's three execution-site band
    mappings are WITHDRAWN (entry 80 (b) annotated
    kept-as-history) and the band mints the PERSISTED floor
    alone; (d) ORDINARY AND CACHED PREPARED STATEMENTS: a
    prepared SELECT reading a band view validated the marker
    from the catalog record when its plan was built (the cache
    lives within one binary's process) and each rebuild
    revalidates it at binding, and a prepared CREATE VIEW
    (preparable through `getPreparePlan`'s default `BuildPlan`
    route, `build_dcl.go:29–102`) carries the minted marker in
    its cached plan's TableDef while each execute-time rebuild
    rebinds the definition and recomputes the raise — no
    bind-time evidence crosses a plan-cache lifetime; (e) the
    oracles of record (planned, §9 I9): post-planning-release
    (the marker stands in the authored view record after the
    registry release), cached-execution (a prepared scale-70
    band statement re-executed from cache keeps identical values
    and placement), send-time (no execution feature is set for a
    provenance-free scale-70 band statement at any gate), and
    isolation controls (a sibling non-band statement lifts
    nothing; a band view followed by a non-band view mints no
    second marker); (f) reconciliations landed at §3 step 6,
    §4.1, §6 M4, §8, §9 I9, kept-as-history annotations on
    entries 80 (b)/(c) and 81 (b), and the projection.

    REJECTED ALTERNATIVES: threading the ledger into
    `RequiredRemoteExpressionFeatures` and every gate (no such
    channel exists — the review's named gap — and it would
    change four sites' signatures for evidence they are proven
    not to need); retaining the three execution-site band
    mappings (impossible without the rejected ledger threading,
    and unnecessary by the execution-identity argument);
    serializing the band record into a plan-cache field (a
    plan-format change against I8, where the marker already
    fences the only cross-version consumer); minting the
    execution floor anyway for uniformity (over-fencing
    provenance-free band statements into one-CN demotion — a
    placement regression with no correctness gain).

    EXCLUSIONS: no production change this turn (the frozen
    revision-29 implementation stays byte-identical pending
    design PASS — the execution-site band mappings it carries
    are withdrawn here and their removal lands in the
    implementation turn, never in this checkpoint); no executed
    oracles — every added assertion is planned, not executed;
    the multiplication feature stays execution-only
    (ScalarDivision precedent); no change to the band's
    persisted marker version (v132 stands).

    VALIDATION PLAN (cheapest first, on design PASS): the
    re-keyed band-signal UT table (parent-gate positives
    including the GATE-DISAPPEARANCE row, negatives including
    the GATE-APPEARANCE mirror) and the persisted-channel UTs;
    then the execution-identity and send-time assertions for
    the provenance-free scale-70 band statement with the
    isolation controls; then the authoring/replay gates on
    forged-floor runtimes with the GATE-DISAPPEARANCE view
    witness; then the live band matrix. SEQUENCING: design-only
    checkpoint → independent review → implementation per
    decision log 80 (v) ADJUSTED: the three execution-site band
    entries leave the planned production set (the
    multiplication entries stay), the `string_literal_form.go`
    feature block keeps only the multiplication condition, and
    `persisted_ip.go` gains the binding-locus raise beside its
    existing observation owner. PROJECTION: semantics,
    edge_cases, errors and limitations restate the parent-gate
    determination and the persisted-only floor; sources and
    limitations re-point to this revision (sha256 recorded at
    submission) and the round-67 artifact; the whole projection
    is re-checked under the 64000-byte budget.

83. **Revision 37 — the round-68 design-only review's two findings**
    (artifact `pre-pr-review-1793563339549.json`, a design-only
    REQUEST_CHANGES against revision 36, sha256
    `078cb9d863ae0b7863f754755b2b771f4ab7efdce69cbe7a239454dea0ff7772`).

    (i) FINDING ONE (P2, quoted verbatim): "Propagate the persisted
    requirement across isolated view-binding contexts" — evidence:
    "Design lines 1271–1286 and decision 82 assume the requirement
    pointer is shared with every nested query block and reaches root
    capture unchanged. However, assigned base
    5fe08a3307:pkg/sql/plan/query_builder.go:12532–12534 creates
    viewCtx with NewBindContext(builder, nil) and copies only
    ctasExecutionProtocolRequirement. bind_context.go:63–70
    therefore allocates a separate persisted accumulator. For CREATE
    VIEW outer_v AS SELECT * FROM marked_band_v, the band raise during
    inner expansion cannot reach the outer root captured at
    build_ddl.go:287–291. The proposed post-planning walk
    deliberately carries no band signal, so it cannot repair this loss.
    The inner marker protects reads, but does not publish the outer
    marker or apply its promised v132 authoring requirement when the
    read floor permits 132 and the authoring floor remains below it."
    ACTION (quoted): "Specify how isolated view expansion shares or
    returns its persisted requirement to the outer authoring owner. Add
    planned nested-view assertions for the outer v132 marker, authoring
    rejection with read floor 132 and authoring floor below 132, replay
    behavior, and non-band isolation controls."

    VERIFICATION — VALID at code on the frozen HEAD
    5fe08a3307e246e1ab9951ab690b50557af0f509 (byte reads of HEAD
    sources; a design-premise/channel defect decidable by
    inspection, so no probe harness; the design gate bars
    production changes and implementation validation this turn).
    HEAD `query_builder.go:12532` creates the view-expansion
    context with `NewBindContext(builder, nil)` and
    `:12533–12534` shares ONLY `ctasExecutionProtocolRequirement`
    ("Preserve execution admission while keeping view name
    resolution isolated."); the constructor's install condition
    (`bind_context.go:64–67`, `parent == nil` and a non-empty
    `builder.persistedViewTarget`) therefore arms the expansion
    context with a FRESH private accumulator during outer view
    authoring (`build.go:85` set the generation's target), and
    the parent-inherit (`:70–71`) cannot run with a nil
    parent. The raise owner reads the binder's CURRENT context
    (`persisted_ip.go:200–208`: nil-check, then max-raise), so
    a band raise fired while binding the inner expansion lifts
    the private accumulator that `bindView` discards on return,
    and the outer capture reads the ROOT context's accumulator
    (`build_ddl.go:288–290`) — never seeing v132, the
    reviewed loss exactly as quoted. The inner view's marker
    still gates reads at expansion
    (`query_builder.go:12548–12556`), matching the review's
    "inner marker protects reads" scope. The complete HEAD
    enumeration of production `NewBindContext(builder, nil)`
    sites confirms `bindView` is the ONLY expression-binding
    break: statement roots on fresh builders whose target is
    never set (`build.go:198`/`:291`/`:399`/`:443`/`:501`,
    `build_ddl.go:1652`/`:3254`, `build_delete.go:43`/`:87`,
    `build_dml_util.go:251`/`:408`, `build_insert.go:105`,
    `build_load.go:716`, `build_table_clone.go:49`,
    `iceberg_dml_delete.go:42`/`:110`,
    `iceberg_dml_merge.go:46`, `bind_multi_insert.go:64`/
    `:563`, `frontend/util.go:696`/`:733`) hold the nil field
    by construction; the channel's only install root is
    `build.go:89` under the `:85` target set; and the
    STATISTICS union assembler (`subscription_metadata.go:323`)
    is a plan-assembly carrier (it populates UNION_ALL node
    metadata from already-bound branch contexts) that no
    expression binds through, so no raise can target it.

    DECISION — F-LAGLEAD-VIEW-EXPANSION-PROPAGATION: (a)
    SHARE MECHANISM: the isolated view-expansion context joins
    the generation's accumulator through one planned production
    line beside the existing ctas-execution share (HEAD
    `query_builder.go:12533–12534`), copying the pointer
    the reference-binding context holds into
    `viewCtx.persistedExpressionProtocolRequirement` — the
    root accumulator at top level, the inherited pointer in
    nested blocks (`bind_context.go:70–71`), or the
    pointer an outer expansion already shared — so the
    raise at the `lag`/`lead` binding locus
    (`persisted_ip.go:200–208`, reading the binder's
    current context) lands in the one accumulator the outer
    capture reads; (b) TRANSITIVITY AND NO-OP CASES: a
    three-level chain (an outer view over a mid view over a
    band inner view) reaches the outer root accumulator
    through two applications of the same line; non-view
    generations hold nil on both sides (the share copies nil,
    the raise stays a no-op, no band evidence survives
    binding); CTAS generations run on `genAsSelectCols`' own
    builder with an empty target (`build_ddl.go:1651–1652`),
    so band CTAS keeps its plain catalog record; the inner
    view's own marker check at expansion
    (`query_builder.go:12548–12556`) is untouched —
    read protection stands where no outer authoring is live;
    (c) the oracles of record (planned, §9 I9): the outer
    v132 marker catalog assertion on `CREATE VIEW outer_v AS
    SELECT * FROM <band inner view>` with three-level
    transitivity; the authoring rejection with the read floor
    permitting 132 and the catalog-admission floor forged
    below 132; replay/regeneration/describe/schema-inference
    behavior — success on a floor-satisfying binary,
    marker-first persisted-protocol rejection at the outer
    record (`parsePersistedViewDefinition`) on a
    forged-below-floor runtime, independent of the inner
    marker's expansion-time check; and the non-band isolation
    controls (an outer view over a non-band inner view lifts
    nothing; an ordinary SELECT over a band view holds the nil
    field; CTAS from a band view mints no marker); (d)
    reconciliations landed at §3 step 6, §4.1
    publication, §6 M4/M5, §8, §9 I9/I10, a
    kept-as-history annotation on entry 82 (ii) (a), and the
    projection.

    REJECTED ALTERNATIVES: returning the expansion context's
    accumulator to the `bindView` caller (a new signature and
    merge protocol for one pointer — the explicit share
    preserves the established max-raise owner and the
    nil-field no-op cases verbatim); installing at capture
    time from the inner view's persisted marker (the capture
    reads authored records, not bind evidence, and a
    not-yet-persisted inner definition has no record to read
    — the raise must flow through the binding channel
    itself); sharing at the STATISTICS union assembler too
    (no expression binds through that carrier — the share
    would be dead code, and the enumeration pins its no-op
    status instead); minting the outer marker from the inner
    record's marker at authoring (the outer definition's band
    status is a property of its EXPANDED plan — a
    definition inlining the same merge SQL directly must mint
    identically, which only the binding-locus raise
    guarantees).

    (ii) FINDING TWO (P3, quoted verbatim): "Correct M4's remaining
    gate-transition statement" — evidence: "Design lines
    3160–3162 say parent-supported results, 'both gate-transition
    cases included,' mint no marker. Section 4.1 and I9 instead
    correctly require the GATE-DISAPPEARANCE witness to mint v132
    because its parent rejects planning." ACTION (quoted): "Limit
    M4's no-marker statement to parent-supported cases, including
    GATE-APPEARANCE, and retain GATE-DISAPPEARANCE as a marked
    positive."

    VERIFICATION — VALID at the document level: revision 36's
    §6 M4 parenthesis reads "parent-supported wide
    `lag`/`lead` results, both gate-transition cases included,
    mint no marker and round-trip byte-identically below the
    floor", while §4.1's parent-gate matrix and the §9
    I9/I10 oracles (decision log 82 (i)) classify
    GATE-DISAPPEARANCE as parent-REJECTED band minting v132 and
    GATE-APPEARANCE as parent-supported non-band — M4
    mislabels one of the two transitions, exactly as quoted.

    DECISION — F-M4-GATE-TRANSITIONS: M4's parenthesis now
    limits the no-marker statement to parent-supported wide
    `lag`/`lead` results (the GATE-APPEARANCE case included)
    and names GATE-DISAPPEARANCE a parent-rejected marked
    positive; the §6 M5 outcome map gains the nested-band
    row and the §8 exception sentence names the
    outer-over-band case. No contract change — revision
    36's parent-gate matrix and oracles stand verbatim.

    EXCLUSIONS: no production change this turn (the frozen
    revision-29 implementation stays byte-identical pending
    design PASS); no executed oracles — every added
    assertion is planned, not executed; the band's persisted
    marker version stands at v132; the multiplication feature
    stays execution-only; the band membership matrix is
    unchanged.

    VALIDATION PLAN (cheapest first, on design PASS): the
    nested-view propagation UTs (outer marker, three-level
    transitivity, isolation controls) beside the revision-36
    persisted-channel UTs; then the authoring-rejection witness
    on the forged split-floor runtime; then the marker-first
    replay/describe/schema-inference family; then the live band
    matrix extended with the nested rows. SEQUENCING:
    design-only checkpoint → independent review →
    implementation per decision log 80 (v) as adjusted by
    entry 82, PLUS the one-line expansion share in
    `query_builder.go`'s `bindView` beside the ctas precedent
    (the file already belongs to the planned production set;
    no file joins or leaves it). PROJECTION: semantics,
    edge_cases and errors restate the nested-view propagation
    and M4's corrected classification; sources and limitations
    re-point to this revision (sha256 recorded at submission)
    and the round-68 artifact; the whole projection is
    re-checked under the 64000-byte budget.

84. **Revision 38 — the round-70 implementation-phase pre-PR review's
    five P2 findings** (artifact `pre-pr-review-1793570718605.json`, an
    implementation-stage REQUEST_CHANGES against the implemented
    revision 37, design sha256
    `0b88870d9259dcbaa3a815a02b7381046a12839278ce51914cb2ec46a621c413`;
    the review verified and reused the revision-37 design-only PASS).
    Two findings are design-material and are resolved in this revision;
    three are verified-valid implementation-phase obligations whose
    design contracts already exist and are unchanged.

    (i) FINDING ONE (P2, quoted): "Require replay safety before
    duplicating shared expressions" — the shadow path
    (`build_ddl.go:11122` entry, `:11234` expression copy, `:11255`
    independent evaluation appended) checks relational shape alone, so
    a materialized CTE exporting `CASE WHEN UNHEX(s) IS NOT NULL THEN
    a*b ELSE a*d END` with `s='zz'` duplicates warning 1411
    (`func_unary.go:9294`) when an AVG reader takes the shadow and a
    MAX reader the original; RAND-bearing expressions draw independent
    values and `llm_chat` repeats one external request per evaluation.
    VERIFICATION: VALID — the worktree shadow path carries no
    replay-safety or diagnostic-freedom predicate (the only volatility
    check in the file is the unrelated volatile-defaults guard), while
    decision log 77 (i) (F-REPLAY-SAFETY-POSITIVE-PROOF) and 78 (i)
    (F-DIAGNOSTIC-REPLAY) already contract exactly that predicate —
    a positive per-node proof (decimal arithmetic error-only kernels,
    decimal→decimal casts, the curated audited set with `coalesce` the
    admitted member and `unhex` the canonical excluded counterexample)
    over literal/column leaves, every unproven slot taking the in-place
    single-evaluation channel. DISPOSITION: implementation-phase
    obligation; no design-contract change. The implementation turn
    completes the predicate in the shadow channel and adds the
    deterministic call-count, warning-count and shared-value
    regressions the finding names.

    (ii) FINDING TWO (P2, quoted): "Complete parent-domain resolution
    for supported selectors" — `decimalBoundaryAggregateParentDomain`'s
    switch (`base_binder.go:10162–10176`) covers only
    `avg`/`avg_tw_result`/`sum`/`min`/`max`/`any_value`/`max_by`,
    omitting `max_by_non_null`, `percentile_disc`,
    `first_value`/`last_value`/`nth_value` and default-aware
    `lag`/`lead`, so `COALESCE(MAX_BY_NON_NULL(a*b,k,k), CAST(0 AS
    DECIMAL(65,1)))` over `(38,0)` operands declines its merge retry
    where the parent binds `(66,1)` (base `list_agg.go:730` registers
    the three-argument selector with `ReturnFirstArgType`;
    `type_check.go:1113–1147` admits the common;
    `base_binder.go:9076–9110` returns it). VERIFICATION: VALID —
    confirmed against the pinned base's registration and binding path.
    DISPOSITION: implementation-phase obligation; revision 30's
    F-SELECTOR-PARENT-DOMAIN already contracts the completion, and the
    value-selecting exemption matrix (no admission cast on these
    selectors' arguments) is unchanged. The implementation turn
    completes the shared resolver for every approved selector,
    default-aware LAG/LEAD included, with the
    parent-success/candidate-failure/repaired-success nested-merge
    tests the finding names.

    (iii) FINDING THREE (P2, quoted): "Preserve ordinary decimal
    schemas outside the widening change" — the storage envelope runs
    for every decimal output and `build_ddl.go:1898–1917` applies
    `min(w1+w2,65)` to every multiplication family, so `CREATE TABLE
    dst AS SELECT a*b AS p FROM src` over `DECIMAL(10,0)` columns
    persists `(20,0)` where the pinned base publishes and persists
    `DECIMAL128(38,0)`. VERIFICATION: VALID — design-material. On the
    pinned base the non-promoted narrow family's retType
    (`5fe08a3307:pkg/sql/plan/function/list_operator.go:1701–1714`)
    publishes a FIXED `DECIMAL128(38, s)`; on the refreshed base
    (#31034's restructuring) the same family publishes the
    domain-derived `DECIMAL128(min(dw1+dw2, 38), s)` and the
    decimal256-carrier family `DECIMAL256(min(dw1+dw2, 65), s)`, with
    `decimalMultiplyDomain` counting a full-envelope DECIMAL64 operand
    19 (legacy signed-int64 coefficient) and a full-envelope DECIMAL128
    operand 39 (released checked-D128 MUL coefficient) — so the
    regression family shifts but survives the refresh: the revision-2
    envelope persists `(35,0)` where the refreshed parent authors
    `(36,0)` for `DECIMAL(18,0) × DECIMAL(17,0)`, `(36,0)` where it
    authors `(38,0)` for `DECIMAL(18,0)²`, and `(39,0)` where it
    authors the promoted `(40,0)` for `DECIMAL(38,0) × DECIMAL(1,0)`.
    DECISION (F-NARROW-CARRIER-ENVELOPE): §4.1's multiplication bullet
    is rewritten to mirror the parent's own `decimalMultiplyReturnType`
    per carrier family over the parent's `decimalMultiplyDomain`
    operand domains; the §4 boundary paragraph, the
    multiplication-carriers consequences bullet and the merge-retry
    passage are restated to match; §9 I2 gains NONCONSTANT
    narrow-carrier column-pair oracles against the refreshed parent
    (constant products fold to literals and never exercise the rule)
    with the revision-2 envelope's `(35,0)`/`(36,0)`/`(39,0)` as the
    RED controls. The widened decimal256-carrier publication and every
    >65/band contract are untouched.

    (iv) FINDING FOUR (P2, quoted): "Allocate a protocol floor unused
    on the target branch" — `pkg/defines/const.go:170` assigns the
    widened-multiplication feature version 132 and
    `expression_protocol.go:154–155` accepts workers at that floor,
    but origin/main already assigns 132 to decimal float rounding and
    strict textual float conversions (#31191,
    `861a2c4cb68961d8b6b01f18e0aa6ac52a3a505f`) and advertises versions
    through 134; those binaries still cap multiplication at 65
    (`list_operator.go:319–340` on the target) yet would pass this
    feature's execution and persisted-view fences. VERIFICATION: VALID —
    design-material. origin/main
    `be8eb40ad8bb7b35cbd3c1f50a5a6d23e1f6da1e` (219 commits past the
    pinned base; merge-base verified =
    `5fe08a3307e246e1ab9951ab690b50557af0f509`) allocates
    `MORPCVersion132` (decimal float rounding and strict textual float
    conversions), `MORPCVersion133` (typed JSON MIN/MAX aggregate
    comparison) and `MORPCVersion134` (ordered ODKU result summary and
    captured session LAST_INSERT_ID baseline) with
    `MORPCLatestVersion = 134`, and its `decimalMultiplyReturnType`
    keeps `min(…, min(oid.ToType().Width, 65))` — the #30938 defect
    stands on the target. DECISION (F-TARGET-PROTOCOL-COLLISION):
    (a) CONTROLLED BASE REFRESH `5fe08a3307` → `be8eb40ad8`, justified
    by this verified prerequisite; the 24-file overlap surface between
    the frozen production set and upstream's 2527-file delta is
    recorded (`/tmp/30938_r70_overlap.txt`) for the implementation
    turn's per-file composition, and §8's #31034 overlap observation is
    absorbed into the refresh; (b) FLOOR RE-ALLOCATION: both features
    (`DecimalMultiplicationCarrierDomain`, execution-only, and
    `DecimalWindowDefaultCarrierDomain`, persisted) re-mint
    `defines.MORPCVersion135`, the next free floor, with
    `MORPCLatestVersion` lifting to 135 with this change; every
    normative v132 reference in §3 step 6, §4.1, §6 M4/M5, §8 and §9
    renumbers to v135 and the two checked persisted-protocol message
    families embed 135 (entries 76–83 keep their v132 text as history —
    the floors they describe are superseded by this entry); (c) §9 I9
    gains the floor-rejection oracle family: runtimes reporting each
    refreshed-base pre-existing floor (132/133/134) fail the
    widened-multiplication execution gate and the band's persisted
    fence; (d) BASE-SIDE ANCHORS VERIFIED on the refreshed base:
    `RemoteExpressionFeatures` and `DecimalDivisionSemantics`
    (`pkg/sql/compile/expression_protocol.go`,
    `pkg/pb/plan/string_literal_form.go`), the LAG/LEAD gate text and
    fallback (`base_binder.go`),
    `ctasTemporalSchemaType`/`decimalDivSchemaType` (`build_ddl.go`),
    `bindView` with the ctas share-copy precedent (`query_builder.go`),
    `observeExpressionProtocol` (`persisted_ip.go`) and
    `decimalMultiplyReturnType` (`list_operator.go`, restructured by
    #31034 into `decimalArithmeticInputs` with the `keepCheckedD128`
    narrow route — cited at rule level, HEAD-relative pins re-verified
    at implementation); `DecimalBoundaryDecision` remains this change's
    own addition. Pre-refresh `5fe08a3307:` citations remain
    evidence-of-record.

    (v) FINDING FIVE (P2, quoted): "Supply the approved execution-cost
    validation" — §9 I8 (revision 5, F-PERF-PREMISE) requires
    parent/candidate benchmarks for plain and window AVG, SUM,
    mixed-origin arguments and materialized shared producers with a
    1.5× execution budget, an unaffected control and a linear planning
    check; no implementation report or execution receipt establishes
    these measurements, and small functional BVT timings plus the
    Challenger's 16 correctness scripts do not prove the budget.
    VERIFICATION: VALID — the contracted measurements were never run;
    the per-row admission cast (`func_cast.go:7020` formats and
    re-parses each coefficient on the same-scale narrowing path) makes
    them load-bearing. DISPOSITION: implementation-phase obligation;
    §9 I8's contract is unchanged. The implementation turn supplies the
    reproducible A/B measurements for the approved workload matrix and
    planning bounds before any approval request, optimizing the
    admission mechanism without weakening its semantics if the budget
    fails and rerunning the affected correctness proof.

    (vi) F-BAND-MESSAGE-ORACLE (reconciliation of the round-69
    implementation report's errors[5]/risks note): the undeclarable
    control `LAG(a*b,1,CAST(0 AS DECIMAL(65,20)))` derives `(85,20)`
    and fails on BOTH engines with the IDENTICAL text `LAG/LEAD common
    precision exceeds SQL limit 65` — the decimal×decimal merge fails
    inside each engine's own coalesce overload resolution (neither
    engine's decimal coalesce common admits a width beyond its own
    decimal expression bound) and surfaces through the retained
    lookup-failure fallback (parent `:9261`, worktree `:9326`) carrying
    the 65-message verbatim; the repaired engine's `exceeds the
    Decimal256 limit 76` texts belong to the NON-coalesce decimal-merge
    families (decimal value with integer, BIT or YEAR default; the
    bool-numeric site) whose explicit width checks are reached without
    the coalesce lookup. §9 I10's oracle is restated; the revision-33
    era's "message text diverges" passages (this log, kept as history)
    are superseded for the decimal×decimal band family by the live
    round-69 receipts.

    EXCLUSIONS: no production change this turn — the frozen rev55
    implementation stays byte-identical pending design PASS (backups
    `/tmp/30938_my_fix_backup_rev55_tracked.patch` and
    `/tmp/30938_my_fix_backup_rev55_untracked.tgz`); no executed
    oracles — every added assertion is planned, not executed; the band
    membership matrix, the parent-gate admission, the nested-view
    propagation and every >65 contract are unchanged.

    VALIDATION PLAN (cheapest first, on design PASS): the §9 I2
    narrow-carrier RED leg (the revision-2 envelope persists
    `(35,0)`/`(36,0)`/`(39,0)` against the refreshed parent's
    `(36,0)`/`(38,0)`/`(40,0)`) beside the base-refresh composition
    build; then the §9 I9 floor-rejection UTs (132/133/134 runtimes);
    then the finding-one predicate and finding-two resolver completions
    with their red/green ladders; then the §9 I8 benchmark family
    (finding five); carried rev55 evidence whose inputs survive the
    refresh is reused under the validation-evidence contract, and every
    oracle whose base behavior the refresh changes is rerun against the
    refreshed parent.

    SEQUENCING: design-only checkpoint → independent review →
    implementation in the order: (1) controlled base refresh with the
    24-file overlap resolution and pin re-verification; (2) floor
    re-allocation 132→135 (`pkg/defines/const.go`, execution gates,
    persisted markers, message families, tests); (3) the §4.1
    per-family envelope rule in `build_ddl.go` with the §9 I2 oracles;
    (4) the shadow replay-safety/diagnostic-freedom predicate; (5) the
    selector parent-domain completion; (6) the §9 I8 benchmark family;
    (7) re-presented evidence per the validation-evidence contract.
    PROJECTION: semantics, edge_cases and errors restate the floor
    re-allocation, the narrow-carrier persistence and the message
    routing; sources and limitations re-point to this revision (sha256
    recorded at submission) and the round-70 artifact; the whole
    projection is re-checked under the 64000-byte budget.

    85. ROUND-71 IMPLEMENTATION-PHASE EMPIRICAL CORRECTION
    (F-UNSEAMED-ENVELOPE-PROVENANCE; supersedes the revision-38 example
    values at §4.1's multiplication bullet and multiplication-carriers
    consequence bullet, the §9 I2 narrow-carrier oracle values and this
    log's 84 (iii) example values and 84 VALIDATION PLAN example values —
    those passages stay as history; log 85's VALIDATION PLAN below carries
    the corrected RED controls).
    Context: composing the §9 I2 narrow-carrier ladder produced RED
    controls the implementation did not deserve and exposed one genuine
    implementation defect; both were verified at byte level and by
    bound-plan probes (receipts `/tmp/30938_r71_probe1.log`,
    `/tmp/30938_r71_probe2.log`; probe harness
    `pkg/sql/plan/zz_probe_narrow_30938_test.go`, removed after the
    diagnosis).

    (i) FINDING (declared narrow columns are DECIMAL128): the plan-side
    DDL declaration mapping (`build_util.go:395–401`, refreshed-base
    code) assigns precision ≤16 to DECIMAL64, precision 17–38 to
    DECIMAL128 and >38 to DECIMAL256, so a declared `DECIMAL(18,0)` or
    `DECIMAL(17,0)` column is DECIMAL128-typed and the parent's
    `decimalMultiplyDomain` — whose legacy 18→19 adjustment keys on the
    operand OID being DECIMAL64 (`list_operator.go:157–209`, #31034) —
    counts it 18, not 19. Bound-plan probe (candidate, with every line
    of the publication and coercion path verified byte-identical to the
    refreshed base — the round-71 diff touches only the decimal256
    publication cap, `list_operator.go:337–354`): `a*b` over declared
    `DECIMAL(18,0) × DECIMAL(17,0)` binds `DECIMAL128(35,0)` and `a*a`
    over `DECIMAL(18,0)` binds `DECIMAL128(36,0)`; the refreshed parent
    persists exactly those tags (the base has no storage-boundary pass —
    `storageBoundary`/`envelopeOwned` do not exist at `be8eb40ad8`).
    #31034's own base tests pin the adjustment's real domain: a
    DECIMAL64(18,0) operand counts 19
    (`decimal_intermediate_precision_test.go:39` and `:50–66` —
    `GetFunctionByName("*", d64(18,0), d64(18,0))` publishes
    `DECIMAL128(38,0)`). CONSEQUENCE: revision 38's §9 I2 oracle values
    for the two declared-(18,0) families were wrong — the refreshed
    parent authors `(35,0)`/`(36,0)`, coinciding with revision 2's
    `min(w1+w2,65)` values, so those families are parent-parity pins;
    the declared-pair discriminating RED control is the
    coefficient-promotion family `DECIMAL(38,0) × DECIMAL(1,0)`:
    revision 2 persists `(39,0)` where the parent authors the promoted
    `(40,0)` (adjusted nominal 39+1 > 38 promoting the pair to the
    decimal256 route; bound-plan probe: operands cast to
    `DECIMAL256(39,0)`/`DECIMAL256(1,0)`, published
    `DECIMAL256(40,0)`). The five-family declared-pair oracle set
    stands corrected at §9 I2; the F-NARROW-CARRIER-ENVELOPE decision
    itself (mirror the parent's per-carrier-family publication) is
    unchanged and already implemented.

    (ii) FINDING (unprovenanced reconstruction misclassifies unseamed
    intermediates — genuine implementation defect): the parent's
    decimal64+decimal64 add/subtract publication is a FIXED
    `DECIMAL64(18, s)` shortcut (`list_operator.go:297–299`), so
    `(a+b)` over declared `DECIMAL(10,0)` columns publishes
    `DECIMAL64(18,0)`, and the parent's multiplication publication over
    that intermediate counts the legacy 19: `(a+b)*a` publishes
    `DECIMAL128(29,0)` and `(a+b)*(a+b)` publishes `DECIMAL128(38,0)`
    (bound-plan probe). The revision-38 envelope's add/subtract mirror
    reconstructs `{11,0}` for that sum (integerDigits+scale+1 — the
    general rule the shortcut bypasses), the `env ≠ typed` branch of
    `decimalMultiplyEnvelopeDomain` misclassified the unseamed sum as a
    widened seam, and the unguarded storage boundary persisted `(21,0)`
    and `(22,0)` (CTAS probe receipts) where the parent persists
    `(29,0)`/`(38,0)` — a regression against the design's own I4
    invariant ("every tree untouched by the widening seam keeps its
    authored type exactly as the parent did"). DECISION
    (F-UNSEAMED-ENVELOPE-PROVENANCE): the envelope evaluator threads
    widened-multiplication provenance — leaves (literal/column/
    parameter) are unseamed; a `*` node is a seam exactly when its
    published type exceeds the parent publication reconstructed from
    its operands' parent domains (the widened 76-cap engaged); a
    wrapper node carries the disjunction of its operands' provenance,
    with each unseamed operand contributing its PUBLISHED domain (the
    parent's own publication, OID-keyed `decimalMultiplyDomain`
    adjustments included) and each seamed operand its reconstruction —
    and the storage boundary replaces a column's persisted tag only
    when the column's expression carries the seam. Unseamed trees
    therefore persist their published (parent-identical) tags whatever
    a mirror rule would reconstruct, and the reconstruction's fidelity
    is required only on seamed trees, where the existing mirrors stand.
    The legacy-D64 adjustment family's discriminating witnesses become
    intermediate-shaped: `(a+b)*a` and `(a+b)*(a+b)` over
    `DECIMAL(10,0)` columns persist the parent's `(29,0)`/`(38,0)`,
    with the unprovenanced reconstruction's `(21,0)`/`(22,0)` as the
    RED controls (demonstrated by the pre-repair candidate, receipts
    cited above). EXCLUSIONS: the widened decimal256-carrier
    publication, every >65/band contract, the version-135 floor and the
    §5/§6/§7 contracts are untouched; no production change this turn —
    the implementation stays as composed pending design PASS (backups
    `/tmp/30938_r71_pre_refresh_tracked.patch`,
    `/tmp/30938_my_fix_backup_rev55_tracked.patch`).

    VALIDATION PLAN (cheapest first, on design PASS): (1) the
    provenance-gated envelope repair in `build_ddl.go`; (2) the §9 I2
    ladder — RED leg: the corrected suite fails on the pre-repair
    candidate for exactly the coefficient-promotion control and the two
    intermediate witnesses (`(21,0)`/`(22,0)` against the expected
    `(29,0)`/`(38,0)`) while the declared-pair pins pass; GREEN leg:
    post-repair every family persists the parent tags in CTAS, prepared
    CTAS and view replay; (3) the §9 I9 floor-rejection UTs; (4) the F1
    predicate and F2 resolver ladders; (5) the §9 I8 benchmark family;
    (6) evidence re-presentation per the validation-evidence contract.
    SEQUENCING: design-only checkpoint → independent review →
    implementation in the order above; the rev38-approved contracts not
    named here carry their approval unchanged. PROJECTION: semantics
    and edge_cases restate the corrected narrow-carrier witnesses and
    the unseamed-tree persistence guarantee; sources and limitations
    re-point to this revision (sha256 recorded at submission) and the
    round-71 artifact; the whole projection is re-checked under the
    64000-byte budget.

86. **Revision 40 (2026-11-02) — F-OUTPUT-DIVERGENCE-PROVENANCE and
    F-SHUTDOWN-UPGRADE-COMPATIBILITY (round-72 pre-PR design review, two
    P2 findings and one P3 finding, artifact
    `state/artifacts/issue-30938/pre-pr-review-1793575608291.json`).**
    (i) FINDING (verbatim scope): "Preserve parent typing after an
    explicit cast pins the domain" — the revision-39 wrapper-disjunction
    provenance rule still narrows schemas: `CTAS SELECT CAST(a*b AS
    DECIMAL(10,0)) + d AS v` over `(38,0)` columns with `d DECIMAL(10,0)`
    — the parent publishes `DECIMAL(18,0)` through its fixed-d64 shortcut
    (`list_operator.go:296–299`) while revision 39's add mirror
    reconstructs `(11,0)`. VERIFIED VALID at byte level:
    `decimalAddSubReturnType` (`list_operator.go:296`) returns
    `DECIMAL64(18, max(s1,s2))` whenever both operands are DECIMAL64-typed
    (`:297–299`), and both `CAST(a*b AS DECIMAL(10,0))` and `d` are
    DECIMAL64(10,0) on either engine, so the parent publishes
    `DECIMAL64(18,0)` for the sum; the revision-39 mirror implemented
    only the general integer-digits-plus-scale rule (`(10+1,0)` →
    `(11,0)`) and the disjunction rule let that reconstruction replace
    the published tag on a seamed tree — an I4 violation of the same
    class as round-71's, this time ABOVE a SQL-authored cast that had
    already pinned the domain. ROOT: the revision-39 model propagated
    the seam upward as a disjunction and reconstructed wrapper envelopes
    with re-implemented mirrors that omit publication shortcuts, and it
    gave SQL-authored fixed-domain casts no containment semantics.
    DECISION (F-OUTPUT-DIVERGENCE-PROVENANCE): (1) every node's
    PARENT-SIDE type is computed node-wise — a leaf keeps its published
    type; a non-multiplication internal node's parent-side type
    RE-INVOKES the node's own unchanged publication rule over the
    operands' parent-side types (the authoritative typing source: every
    shortcut and cap fires exactly as the parent's; the re-implemented
    add/subtract mirror is retired and every per-shape §4.1 rule is
    realized the same way); a multiplication node's parent-side type is
    the parent's pre-change `decimalMultiplyReturnType` — the one
    publication this change alters — retained pure-in-types over the
    operands' parent-side types with the OID-keyed
    `decimalMultiplyDomain` adjustments (revision 38,
    F-NARROW-CARRIER-ENVELOPE, unchanged); (2) a FIXED-DOMAIN BARRIER —
    a SQL-authored cast to a declared DECIMAL target, a declared-domain
    admission cast, or a recursive member→anchor coercion — pins
    parent-side := published (both engines evaluate the identical
    authored target per row), so widening below a barrier is CONTAINED;
    (3) a node carries the seam exactly on OUTPUT-DOMAIN DIVERGENCE
    (published ≠ parent-side); a wrapper whose own publication maps the
    divergent operand to the parent's output (shortcut or cap — e.g. an
    addition's own 65 cap absorbs `(38,0)×(38,0) + d` into the parent's
    `(65,0)`) is seam-free and terminates propagation; (4) the §4.3
    boundary persists the parent-side type only on a column whose
    expression ROOT carries the seam — contained widening below a
    barrier or an absorbing wrapper keeps the published type verbatim
    (I4); (5) §5 admission sources are the seam points not dominated by
    a containing barrier, each re-receiving its parent-side type as
    before. The round-71 `(a+b)*a` / `(a+b)*(a+b)` oracles
    (`(29,0)`/`(38,0)`, RED controls `(21,0)`/`(22,0)`) stand — now
    reproduced by the re-invoked shortcut itself. SUPERSESSION: entry
    85 (ii)'s wrapper-disjunction rule ("an unseamed operand contributes
    its PUBLISHED domain") is superseded by (1)–(3); entry 85 (i)'s
    oracle-value corrections and entry 38's F-NARROW-CARRIER-ENVELOPE
    stand. VALIDATION (per the review's verbatim action): the
    cast-plus-addition witness joins the §9 I2 ladder in CTAS, prepared
    CTAS and view replay, asserting the parent's `(18,0)` tag with
    `(11,0)` as the RED control against the pre-repair candidate.
    (ii) FINDING (verbatim scope): "Align compatibility design with
    shutdown-only upgrades" — the design (§3 step 6, §4.1 band
    passages, §6 M4/M5, §8) and the diff add a new `MORPCVersion135`
    floor, demotion, destination guards and the band marker, violating
    `.agents/skills/mo-dev/references/feature-design-review.md:147–150`
    ("Upgrade is shutdown-only: in-cluster mixed-version operation,
    downgrade, and per-feature protocol gates are out of scope") and
    `upgrade-compatibility.md:3–7`/`:54–65` (no per-feature
    `MORPCVersion` constant, no `MORPCLatestVersion` bump, no
    `AllCNsSupportProtocol` gate for a new feature; existing gates stay
    unextended until a dedicated cleanup PR). VERIFIED VALID against the
    contract text and the current diff (the composition mints
    `MORPCVersion135`, sets `MORPCLatestVersion = 135`, adds the feature
    analysis in `pkg/pb/plan/string_literal_form.go`, the
    `expression_protocol.go` / `expression_destination_protocol.go` /
    `remoterun.go` gates, and the `persisted_ip.go` band raise with its
    `base_binder.go` call site). DECISION
    (F-SHUTDOWN-UPGRADE-COMPATIBILITY): the entire apparatus is
    WITHDRAWN — (a) no `MORPCVersion135` mint, no `MORPCLatestVersion`
    bump; (b) no `DecimalMultiplicationCarrierDomain` feature, no
    `RequiredRemoteExpressionFeatures` analysis, no MULTICN→ONECN
    demotion, no destination guard; (c) no
    `DecimalWindowDefaultCarrierDomain` signal — the parent-gate band
    determination's only consumer was the marker — no
    `raiseLagLeadExpansionBandProtocol`, no expansion share, no
    `ViewData.RequiredProtocolVersion` mint names the issue-specific mint
    only (scoped in revision 41, decision log 87 (i): the unchanged
    pre-existing channel keeps recording its own computed requirements,
    v89 for wide-literal definitions included), no catalog-admission or
    below-floor read/replay/describe/schema-inference fence, no rollback
    story; (d) the band ADMISSION (derived decimal common widths 66–76
    admitted, wider rejected with the base-identical coalesce-family
    text) stands as core behavior, its error-text contract unchanged;
    (e) the PRE-EXISTING machinery — the v132/v133/v134 allocations,
    the `RequiredProtocolVersion` field and its established gates, the
    `persistedExpressionProtocolRequirement` channel — stays
    byte-identical and unextended. FORWARD COMPATIBILITY under the
    supported boundary: the changed binary reads, replays, rebinds,
    describes and schema-infers every base-authored persisted schema and
    view byte-identically (I2/I4); band-admitted views and CTAS tables
    persist plain catalog records in the parent's own above-65 form with
    the marker field carrying exactly the requirement the unchanged
    pre-existing channel computes for the same statement (v89 for
    wide-literal definitions included — the absolute base-default
    phrasing is superseded in revision 41, decision log 87 (i)); after
    the shutdown upgrade
    every CN runs the changed binary, so the repaired fallback kernel
    (§1) executes uniformly and no execution fence is needed; downgrade
    is unsupported per the contract. IMPLEMENTATION REMOVALS (on design
    PASS): `pkg/defines/const.go` (the minted constant and the
    `MORPCLatestVersion` bump), `pkg/pb/plan/string_literal_form.go`
    (the feature and its detection), `pkg/sql/compile/expression_protocol.go`,
    `expression_destination_protocol.go` and `remoterun.go` (the floor,
    demotion and destination gates), `pkg/sql/plan/persisted_ip.go`
    (`raiseLagLeadExpansionBandProtocol`) and its `base_binder.go` call
    site; the two untracked band/protocol test files
    (`pkg/sql/compile/decimal_multiplication_protocol_test.go`,
    `pkg/sql/plan/decimal_lag_lead_band_issue30938_test.go`) are
    rewritten to the §9 I9 forward-compatibility / no-marker / admission
    families. SUPERSESSION: entries 26–28 (F-MIXED-VERSION), 80
    (F-LAGLEAD-PERSISTED-FLOOR), 82 (F-LAGLEAD-PARENT-GATE-ADMISSION's
    determination machinery and 82 (b)'s channel), 83 (i)
    (F-LAGLEAD-VIEW-EXPANSION-PROPAGATION) and 84's v135 re-mint
    (F-TARGET-PROTOCOL-COLLISION) are superseded as decisions and kept
    as history — the band-admission behavior they analyzed stands; the
    §10 revision-6 resolution narrative and the revision-history entries
    for revisions 6/34/35/36/37/38 remain as history under this
    supersession; §10's revision-20 narrative phrase "at the minted
    protocol floor" is re-pointed to the changed binary. EXCLUSIONS:
    the widened decimal256-carrier publication, the >65/band admission
    contracts, the §5/§6/§7 core contracts and the F1/F2 validation
    obligations are untouched; no production change this turn — the
    implementation stays as composed pending design PASS. VALIDATION
    PLAN (cheapest first, on design PASS): (1) the output-divergence
    parent-side repair in `build_ddl.go` (publication re-invocation
    replacing the add/subtract mirror; barrier containment); (2) the §9
    I2 ladder with SEPARATE RED VARIANTS (the review's P3 disposition,
    verified at byte level: the pre-repair candidate's multiplication
    branch already combines the promoted operand widths over their
    typed domains — `build_ddl.go:1925–1944`, `:2151–2187` — so it
    persists the parent's `(40,0)` for the declared `(38,0)×(1,0)`
    pair): RED variant A, the current pre-provenance candidate, fails
    exactly the two intermediate witnesses (`(21,0)`/`(22,0)` against
    `(29,0)`/`(38,0)`) and the cast-plus-addition witness (`(11,0)`
    against `(18,0)`); RED variant B, the revision-2-envelope revert,
    additionally fails the coefficient-promotion control (`(39,0)`
    against `(40,0)`); the corrected declared-column cases
    `(20,0)`/`(35,0)`/`(36,0)`/`(65,0)` are parent-parity controls
    passing on both variants (the `(40,0)` declared
    coefficient-promotion case passes on variant A and is variant B's
    RED — corrected in revision 41, decision log 87 (ii)); GREEN leg: post-repair every
    family persists the parent tags in CTAS, prepared CTAS and view
    replay;
    (3) the §9 I9 forward-compatibility / no-marker / untouched-gate /
    band-admission families; (4) the F1 predicate and F2 resolver
    ladders; (5) the §9 I8 benchmark family; (6) evidence
    re-presentation per the validation-evidence contract. SEQUENCING:
    design-only checkpoint → independent review → implementation in the
    order above; the rev38-approved contracts not named here carry their
    approval unchanged. PROJECTION: public_contracts restate the barrier
    rule and the shutdown-upgrade compatibility model (sources and
    limitations re-point to this revision, sha256 recorded at
    submission); the whole projection is re-checked under the
    64000-byte budget.

87. **Revision 41 (2026-11-02) — F-PRESERVE-EXISTING-MARKERS and
    F-VALIDATION-MATRIX-ALIGNMENT (round-73 pre-PR design review, one
    P2 finding and one P3 finding, artifact
    `state/artifacts/issue-30938/pre-pr-review-1793577123632.json`).**
    (i) FINDING (P2, quoted verbatim): "Preserve existing persisted
    markers when withdrawing the new marker" — evidence (quoted):
    "Revision 40 design lines 3181–3196 and I9 at line 3436 require
    RequiredProtocolVersion to remain absent/default on every authored
    view, including band witnesses; public_contracts repeats this
    requirement. This conflicts with leaving existing machinery
    unchanged. On pinned base be8eb40ad8, LAG(N66,0,N66), with N66 a
    66-digit literal, is parent-supported: base_binder.go:9084 skips
    reconciliation for matching types. make.go:309–311 marks that
    literal, pb/plan/string_literal_form.go:1027–1032 discovers
    DecimalLiteralSemantics, persisted_ip.go:154–155/:249–254 records
    version 89, and build_ddl.go:462–478 persists it. The newly
    admitted N76-default band witness likewise carries the existing
    literal requirement. Unconditionally requiring an absent marker
    would erase established catalog metadata or require an incorrect
    test oracle." ACTION (quoted): "Define withdrawal as adding no
    issue-specific marker or floor. Preserve all requirements computed
    by existing machinery, including version 89 for wide literals.
    Update M4, I9 and public_contracts accordingly; plan assertions
    for inherited nonzero markers and mixed-feature views, using
    absence only when no existing feature requires a marker."
    VERIFICATION — VALID at byte level on the pinned base
    `be8eb40ad8bb7b35cbd3c1f50a5a6d23e1f6da1e`: the LAG/LEAD reconcile
    is skipped when the default's type equals the value type
    (`base_binder.go:9084`, `if len(args) >= 3 &&
    !argsType[2].Eq(returnType)`), so `LAG(N66,0,N66)` binds without
    reconciliation and is parent-supported; `decimalLiteralRequiresV82`
    (`make.go:309–311`) marks a plain decimal literal whose canonical
    form differs or whose normalized width exceeds the DECIMAL128 width
    38 (N66 and N76 included); the feature walk discovers
    `DecimalLiteralSemantics` from the literal
    (`string_literal_form.go:1027–1032`); the floor records
    `MORPCVersion89` for that feature (`persisted_ip.go:154–155`)
    through the `persistedExpressionProtocolRequirement` channel
    (`persisted_ip.go:249–254`); and view authoring persists the
    computed requirement into `ViewData.RequiredProtocolVersion`
    (`build_ddl.go:462–463`, `:477`). Revision 40's absolute
    statements — §6 M4's "keeps its base default on every record this
    change authors" and §9 I9's "keeps its base default on every
    authored view record — the band witnesses included" — therefore
    contradict the design's own leave-existing-machinery-untouched
    contract and would force incorrect test oracles: the
    gate-disappearance band witness `LAG(a*b,0,N76)` persists WITH the
    inherited v89 requirement, and erasing it would delete established
    catalog metadata. DECISION — F-PRESERVE-EXISTING-MARKERS: (1) the
    withdrawal adds NO issue-specific marker or floor — no new
    constant, no `MORPCLatestVersion` bump, no feature bit, no
    demotion, no destination guard, no catalog-admission or below-floor
    fence, no rollback story (entry 86 (ii) stands); (2) EVERY
    requirement the unchanged pre-existing channel computes for the
    same statement persists verbatim, band and non-band records alike —
    the v89 wide-literal requirement included (the gate-disappearance
    witness's N76; the parent-supported `LAG(N66,0,N66)`'s N66); (3)
    marker-ABSENCE assertions apply only where no existing feature
    requires a marker (the CAST-default band witnesses); (4)
    mixed-feature views keep the unchanged channel's computed maximum,
    the band adding nothing. SUPERSESSION: entry 86 (ii)'s absolute
    phrasings — (c)'s "no `ViewData.RequiredProtocolVersion` mint" and
    the forward-compatibility paragraph's "the marker field at its
    base default" — are superseded as stated and annotated in place;
    every other provision of entry 86 stands. RECONCILIATIONS LANDED:
    §1 band sentence, §2 cost paragraph, §4 band block, §4 band
    persistence, §6 preamble, §6 M4, §6 M5, §8 upgrade bullet, §9
    I9/I10/View-surfaces rows, and the projection. VALIDATION (per the
    review's verbatim action): §9 I9's marker-preservation matrix —
    (a) inherited-nonzero-marker cells (the N76 band witness persists
    `(76,0)` WITH v89; `LAG(N66,0,N66)` persists parent-identically
    WITH v89), (b) mixed-feature cells (the unchanged channel's
    computed maximum, no band addition), (c) absence cells only for
    marker-free definitions (the CAST-default witnesses `(66,1)` /
    `(75,10)` at the base default) — beside the carried
    no-new-feature-bit and untouched-gate guards.
    (ii) FINDING (P3, quoted verbatim): "Reconcile the remaining
    validation-plan contradictions" — evidence (quoted): "The active
    I10 row at design line 3437 still requires
    RequiredProtocolVersion:135, below-floor rejection and nested
    marker propagation, despite revision 40 withdrawing those
    mechanisms. Decision log 86 lines 9383–9387 also says the
    coefficient-promotion control fails RED variant B, then includes
    its expected (40,0) among controls passing on both variants."
    ACTION (quoted): "Align the active validation matrix with decision
    log 86, retain superseded expectations only as history, and exclude
    the coefficient-promotion control from variant B's
    passing-control list." VERIFICATION — VALID at byte level: the I10
    row carries the revision-34 sentence "persists
    `RequiredProtocolVersion: 135` (the band fence, §6 M4), read/replay
    below the floor raising the checked persisted-protocol family", the
    revision-35/36 sentence's "forged-below-floor runtime with clean
    rollback round-trip" and "mints the v135 marker with below-floor
    read/replay rejection", and the revision-37 nested sentence's
    "mints v135 through the `bindView` expansion share", "forged split
    floor" and "reject marker-first at the outer record" — all ACTIVE
    statements of mechanisms entry 86 (ii) withdrew; and entry 86's
    validation plan lists `(40,0)` among the declared-column cases
    "passing on both variants" although variant B (the
    revision-2-envelope revert) persists `(39,0)` for that declared
    `(38,0)×(1,0)` pair — the RED the same paragraph pins. DECISION —
    F-VALIDATION-MATRIX-ALIGNMENT: the I10 row's three sentence
    clusters are restated to plain-record persistence with inherited
    channel-computed requirements (the revision-34 marker-fence
    sentence marked withdrawn, kept-as-history); the validation plan's
    parent-parity controls passing on both variants are
    `(20,0)`/`(35,0)`/`(36,0)`/`(65,0)`, and the `(40,0)` declared
    coefficient-promotion case passes on variant A and stands as
    variant B's RED (annotated in place at entry 86). SEQUENCING:
    unchanged — design-only checkpoint → independent review →
    implementation per entry 86's plan with the corrected RED legs.
    PROJECTION: public_contracts restate the marker-preservation
    contract (sources and limitations re-point to this revision, sha256
    recorded at submission); the whole projection is re-checked under
    the 64000-byte budget.

88. **Revision 42 (2026-11-03) — F-INTERNAL-FOLLOWUP-FILL-LINEAGE (the
    internal executor's CTAS follow-up fill joins the execute-route
    discipline; discovered while constructing the round-18 pre-PR
    implementation review's F1 live witnesses, artifact
    `state/artifacts/issue-30938/pre-pr-review-1793698927931.json`).**
    (i) ORIGIN: the round-18 review's first finding (P2, "Honor JOIN
    input ordinals when resolving producer domains") requires prepared
    CTAS regressions "with unequal left/right domains and a merge in
    JOIN ON, in both input orders, asserting the parent common type and
    checked execution outcome". Its in-process repair — the lineage
    view's JOIN/APPLY owner case resolves the reference through the
    owner's actual input ordinal `owner.Children[col.RelPos]` instead of
    the first participating child — binds the parent's `(66,1)` through
    the with-process fill channel, and the round-18 in-process oracles
    pass on that channel. Constructing the demanded LIVE witnesses on
    the owned isolated service exposed a divergence no in-process
    channel can see: the prepared CTAS's execute fails with the PLANNER
    decline `invalid argument function coalesce, bad value [DECIMAL256
    DECIMAL128]` where the parent commits only its own checked
    evaluation rejection. (ii) OBSERVED GROUND TRUTH — THE FOURTH
    EXECUTE-TIME FILL WINDOW, VERIFIED AT CODE AND LIVE: entries 68 (iv)
    and 69 (v) install the generation, process and lineage view around
    every FRONTEND fill window; a prepared CREATE TABLE AS SELECT runs a
    SECOND, independent fill the design never enumerated. The first
    (frontend) fill binds the DDL plan's query with process, generation
    and lineage and refreshes the inferred-column carrier
    (`computation_wrapper.go:1969`-`:1991`); execution then reaches
    `populateCreatedTable` (`pkg/sql/compile/ddl.go:2638`-`:2740`),
    which runs the retained `CreateAsSelectSql` — an INSERT ... SELECT
    whose SELECT text is the formatted `stmt.AsSource` AST
    (`build_ddl.go:6040`-`:6045`), the parameter marker surviving into
    the reparsed text — through the internal executor with the semantic
    parameter values (`ddl.go:2712`) and the outer session attached
    (`ddl.go:2718`). The internal prepared flow
    (`sql_executor.go:614`-`:681`) builds a fresh plan, resets it and
    specializes it through `specializeInternalPreparedPlan`
    (`sql_executor.go:903`-`:922`, called at `:644` and inside the
    retry-rebuild closure at `:675`) — which fills through
    `FillValuesOfParamsInPlanWithSpecializationPreservingDMLWrites`
    (`utils.go:4764`-`:4770`), the NO-PROCESS variant: the lineage gate
    (`utils.go:6905`-`:6909`) stays closed, rebound decisions record
    nowhere (no entered generation; the recorder no-ops without one,
    `decimal_boundary_registry.go:165`-`:167`), and no refresh runs on
    this route — none is needed for the carrier, which the first fill
    already refreshed. An instrumented in-process probe of this exact
    route (harness deleted after capture, per entry 69's precedent)
    fixed the bind-time ground truth for the review witness `CREATE
    TABLE r18pt1 AS SELECT x.v AS lv, y.v AS rv FROM (SELECT a*b AS v
    FROM t1 LIMIT 100) x JOIN (SELECT c*d AS v FROM t2 LIMIT 100) y ON
    COALESCE(y.v, ?) = x.v` over `DECIMAL(38,30)` and `DECIMAL(38,0)`
    column pairs, EXECUTE `?=0.5` typed `DECIMAL(20,1)`: the fresh
    internal bind snapshots the ON-clause COALESCE as the provisional
    `VARCHAR(65535)` merge (both arms wrapped in provisional varchar
    casts, the parameter typed `(71,0,0)`) — the contractual
    engine-independent provisional snapshot — while a SELECT-list
    COALESCE over the same operands binds `DECIMAL256 (76,0)`; at the
    execute fill, entry 68's provisional common-VALUE rebind re-derives
    only the provisional-VARCHAR merge over the real binding domains
    `[(76,0),(20,1)]`, the first lookup fails (the family common needs
    77 digits), and the retry demands the right input's parent-published
    domain with provenance — which only the lineage view can supply for
    a JOIN output column. Process withheld, the resolver's surface
    fallback returns prov=false, the retry declines, and the original
    bind failure propagates: live receipt ERROR 20203 `invalid argument
    function coalesce, bad value [DECIMAL256 DECIMAL128]` at
    `execute r18p1`, and identically for the mirror order `r18p2` (the
    merge over the LEFT input), cascading to `no such table` on the
    following DESC/SELECT — both orders fail because the lineage gate
    closes before the ordinal resolution matters. (iii) THE PARENT
    ORACLE — VERIFIED LIVE ON THE PINNED BASE
    `be8eb40ad8bb7b35cbd3c1f50a5a6d23e1f6da1e` (design-premise probe,
    receipts `/tmp/30938_r84/r84_base_oracle.log` and
    `/tmp/30938_r84/r84_base_oracle2.log`): on the parent the same
    rebind runs, but the parent's own `(66,1)` overload for
    `[(65,0),(20,1)]` exists, so the first lookup succeeds without any
    retry; the comparison against the scaled product then aligns
    `[(66,1),(65,30)]` to `DECIMAL256(76,30)` — an evaluation-time
    alignment common above the 65 publication clamp, the expression
    domain's own 76-digit intermediate width the issue text describes —
    whose 46 integer digits reject the `10^60` right product. The
    observable parent execute is statement failure with the checked
    evaluation family in BOTH input orders: `execute r18p1` raises
    `ERROR 20301 (HY000): invalid input: 1000...000.0 beyond the range,
    can't be converted to Decimal256(76,30)` and the mirror `execute
    r18p2` raises `ERROR 20301 (HY000): invalid input: 1000...000 beyond
    the range, can't be converted to Decimal256(76,30)` — never the
    planner decline, never a persisted table. The repaired candidate's
    live outcome must equal it: with the ordinal repair resolving the
    right input's parent `(65,0)`, the completed retry binds `(66,1)` —
    the parent common type — and execution raises the same checked
    rejection at the same comparison seam. (iv) THE REPAIR
    — THE INTERNAL ROUTE JOINS THE EXECUTE-ROUTE DISCIPLINE:
    (a) `specializeInternalPreparedPlan` gains `proc *process.Process`
    and `divIncrement int32`; both call sites pass the executor's own
    per-execution top process (`sql_executor.go:520`-`:532`) — already
    created before planning and already carrying the outer session
    (`:539`) — so no process lifecycle changes;
    (b) the fill rides a new public wrapper in the already-owned
    `utils.go`,
    `FillValuesOfParamsInPlanWithSpecializationPreservingDMLWritesAndProcess`,
    delegating to
    `fillValuesOfParamsInPlanWithSpecializationSelected(ctx, proc,
    preparePlan, paramVals, true, nil, divIncrement)` — the preserving
    semantics retained (the follow-up is an INSERT ... SELECT whose
    write roots the fill must not rewrite), the process threaded, so
    the lineage gate installs the view for a widened-provenance plan and
    rebound decisions record into the entered generation;
    (c) the route enters `process.EnterDecimalBoundaryGeneration(proc)`
    around the specialization and releases on every return path
    (entry-58's defer-restored discipline on success, error and panic
    unwind): the internal process is a fresh top process with no active
    registry, so entry installs its own generation; the fill's
    provenance-witness read (`base_binder.go:10388`) and b#-channel
    read (`:10292`) then behave exactly as on the frontend routes —
    without entry those reads no-op and a sibling merge rebound earlier
    in the same fill cannot witness provenance, a divergence from the
    frontend channels the design does not permit;
    (d) the increment is recovered from the attached session through the
    established minimal-interface idiom (the
    `interface{ GetSessId() uuid.UUID }` assertion at
    `sql_executor.go:540`-`:544`): assert
    `interface{ GetSessionSysVar(string) (interface{}, error) }` on the
    internal process's session and read `div_precision_increment`,
    falling back to `function.DefaultDivPrecisionIncrement` when the
    session is absent, the assertion fails, the read errors or the value
    is not `int64` — `sessionDivPrecisionIncrement`'s exact logic
    (`session.go:1520`-`:1531`) without importing `pkg/frontend` (an
    import cycle) — so entry 68's contract that parent-domain recovery
    consumes the statement's own setting (review round 13, F1) holds on
    this route, and a session-less background execution degrades to the
    engine default exactly as the frontend's nil-session guard does;
    (e) NO refresh runs on this route: the inferred-column carrier was
    refreshed on the frontend's first plan
    (`computation_wrapper.go:1986`) before the follow-up executes, the
    internal plan feeds data insertion only, and its rebound entries die
    with its generation at release — no ledger crosses the route
    boundary, matching entry 68 (iv)'s read-once claim discipline.
    (v) SEQUENCING AND PRODUCTION SET: `pkg/sql/compile/sql_executor.go`
    joins the production set — the planned set becomes 22 production
    files (entry 69 (vii)'s 21-file accounting superseded, annotated in
    place); `pkg/sql/plan/utils.go` was already owned. The process gate
    at `utils.go:6905` stands unchanged: the no-process variant remains
    exported for EXPLAIN and other process-less fills, and the
    lineage-withheld ablation oracle (entry 69 (vi)) keeps declining
    with the exact checked fill error — the repair threads the existing
    process through this route rather than weakening the gate, so no
    unanalyzed fill gains lineage. (vi) VALIDATION PLAN: (a) the
    round-18 in-process oracles stand — the with-process binding heals
    `(66,1)` in both input orders — extended by one leg driving the NEW
    preserving-and-process wrapper over an INSERT-root plan, asserting
    the bound parent common type and the preserved write roots; (b) the
    process-less leg of the same harness keeps declining with the exact
    20203 text (the ablation control, proving the gate load-bearing);
    (c) live at implementation: the round-18 fixture block returns with
    corrected expectations — both `execute r18p1` and the mirror
    `r18p2` raise the parent's checked evaluation rejection
    byte-identically against the pinned base run of the same statements
    (parent parity in both input orders, the review's "checked execution
    outcome"), the created tables never materialize on either engine,
    and the F2 ABS-projection witness keeps its byte-identical rows;
    (d) the four-package sweeps (`pkg/sql/plan`,
    `pkg/sql/plan/substrait`, `pkg/sql/compile`, `pkg/frontend`),
    `go vet`, `golangci-lint` and `gofmt` gates rerun on the changed
    packages at implementation, and the internal-executor protocol tests
    (`pkg/sql/compile/decimal_multiplication_protocol_test.go`) stand.
    (vii) PROJECTION AND SEQUENCING: at implementation the public
    contract's prepared-CTAS parity clause gains one sentence — the
    follow-up reparse's execute-time fill consumes the same
    producer-lineage and session-increment discipline as the frontend
    routes, the lineage-withheld decline remaining the checked ablation
    behavior — no other contract statement changed **(superseded in
    revision 43: §1's value-retention criterion gains the
    coefficient-passthrough exclusion — decision log entry 89; kept as
    history)**; the projection is
    re-checked under the 64000-byte budget at submission. Sequencing
    unchanged: design-only checkpoint, independent review, then
    implementation per this plan; the round-18 in-process repairs and
    their oracles are preserved, and the round-18 live fixture block is
    deferred to the implementation turn so the committed `.result`
    never records the un-repaired divergence.

89. **Revision 43 (2026-11-04) — F-PREPARED-CTAS-VALUE-CONTRACT (entry
    88 reopened: the intended prepared-CTAS numeric behavior and
    compatibility scope decided explicitly, division evaluation and
    output alignment reconciled; the pre-PR implementation review's P2
    finding, artifact
    `state/artifacts/issue-30938/pre-pr-review-1793727196297.json`).**
    (i) FINDING, VERIFIED LINE-BY-LINE AGAINST THE REPOSITORY: revision
    42's §1 success criterion ("No query that succeeded on base
    returns a different value or loses a previously persistable
    schema/value domain") and entry 88 (vii)'s "no other contract
    statement changed" are contradicted by demonstrated numeric
    behavior on the entry-88 route: the fixture's prepared UNION CTAS
    at `div_precision_increment=10`
    (`issue_30938_decimal_mul_precision.test:781`-`:784`) materializes
    `0.3333000000` on the repaired engine (`.result:1183`; retained
    execution log `/tmp/30938_r85/r85_cand_classify_r13.log`) where
    the pinned base `be8eb40ad8` materializes `0.0000003333`
    (`r85_base_classify_r13.log`, the r84-binary isolation run
    `r85_r84bin_classify_r13.log` reading identically), and the
    ordinary frontend CTAS at the same setting materializes
    `0.3333333333` (`r85_base_frontend_r13.log`); all three engines
    persist the identical `DECIMAL(75,10)` tag and the statement
    succeeds on all three. (ii) MECHANISM — TWO INDEPENDENT INCREMENTS
    ON THE INTERNAL PREPARED ROUTE, VERIFIED AT CODE AND LIVE. (a) The
    EVALUATION increment: the internal executor builds its plan
    through its own build context, which does not consume the
    session's `div_precision_increment`, and decimal division executes
    at its retained result scale (`arithmetic.go:668`-`:671`), so the
    witness's `(c*d)/?` evaluates to `0.3333` at scale 4 on BOTH
    engines — a pre-existing internal-route property this design does
    not change (the frontend routes do consume the session setting:
    the fixture's frontend prepared SELECT at the same session
    increment returns `0.3333333333`, `.result:1160`, so the
    internal/frontend evaluation-increment divergence exists on the
    base itself). (b) The RECOVERY increment: entry 88 (iv) threads
    the attached session's setting into the fill's lineage scope
    (`sql_executor.go:649`-`:650`), and the set-operation retry's
    parent-domain derivation consumes it
    (`utils.go:7970`-`:7975`), so the repaired fill rebinds the union
    output to the increment-10 parent derivation `(75,10)` — the same
    tag the frontend-derived persisted schema already carries on both
    engines. On the pinned base the process-less fill leaves the raw
    `(74,4)` vector flowing into the `(75,10)` column through the
    PRE-EXISTING append-path coefficient-passthrough defect — the raw
    coefficient 3333 written under the column's scale 10 with no
    rescaling — materializing `0.0000003333`, the base expression's
    own evaluated value corrupted by three orders of magnitude; on the
    repaired engine the rebound arm's ordinary alignment evaluates
    `(74,4)`→`(75,10)`, materializing `0.3333000000`. (iii) DECISION —
    INTENDED NUMERIC BEHAVIOR, RECONCILING DIVISION EVALUATION AND
    OUTPUT ALIGNMENT. (a) OUTPUT ALIGNMENT: where entry 88's recovery
    rebinds a set-operation output, the materialized value is the
    base-evaluated expression value correctly scale-aligned into the
    recovered persisted domain — `0.3333000000` for the witness. The
    base's `0.0000003333` is the coefficient-passthrough defect's
    output, not a compatibility oracle: no compatibility contract can
    require preserving a value corrupted orders of magnitude from the
    base engine's own evaluation, and preserving it would require the
    repaired route to deliberately reproduce the passthrough. (b)
    DIVISION EVALUATION: the internal route's evaluation-increment
    behavior is explicitly UNCHANGED — the internal plan build keeps
    consuming its own build context, so the prepared route keeps the
    base's evaluation precision (`0.3333` at scale 4) while the
    frontend routes keep consuming the session setting
    (`0.3333333333`); making the internal build session-aware would
    change every internal prepared statement's division results — a
    pre-existing divergence family outside this issue's scope, of the
    same kind as the documented decimal-param append-path crash
    defect. The prepared-vs-ordinary value difference at non-default
    increments (`0.3333000000` vs `0.3333333333`) is therefore
    intended, documented contract, not an unreconciled contradiction.
    (c) SCHEMA AND SUCCESS: the persisted tag `DECIMAL(75,10)` and the
    statement's success are identical on both engines — unchanged.
    (iv) COMPATIBILITY SCOPE: §1's value-retention criterion gains the
    named exclusion recorded there (the coefficient-passthrough
    family: a prepared set-operation fill whose session increment
    differs from the internal plan build's, whose base materialization
    ran the passthrough defect); every other base-successful statement
    keeps its exact base value, and entry 88 (vii)'s "no other
    contract statement changed" is superseded accordingly (annotated
    in place). No production code changes: the demonstrated behavior
    of the implemented entry-88 repair IS the decided behavior; this
    entry decides the contract it satisfies. (v) VALIDATION PLAN —
    INDEPENDENTLY CALCULATED VALUE ASSERTIONS (the finding's demanded
    oracles; regenerating the current `.result` alone is
    insufficient): four live legs on the owned isolated services with
    expected values calculated independently of the engine (exact
    decimal arithmetic), each leg also asserting the persisted schema
    tag identical across engines: (1) the prepared UNION CTAS at the
    DEFAULT increment — the candidate's value equals the base's value
    AND the independently calculated value (`0.3333`, schema
    `(69,4)`); (2) the prepared UNION CTAS at a NON-DEFAULT increment
    (10) — the candidate's value equals the independently calculated
    aligned value (`0.3333000000`, schema `(75,10)`), the oracle being
    the independent calculation per (iii)(a), with the base's
    divergent `0.0000003333` recorded as the excluded defect family;
    (3) the ordinary (non-prepared) UNION CTAS at the default
    increment — candidate equals base byte-identically (`0.3333`),
    proving the ordinary route untouched; (4) the ordinary UNION CTAS
    at a non-default increment (10) — candidate equals base
    byte-identically (`0.3333333333`). The existing in-process oracles
    (the `(75,10)`/`(69,4)` common-type and generation-restoration
    legs of entry 88 (vi)) stand unchanged. PROJECTION: the public
    contract's prepared-CTAS clause gains the value sentence of
    (iii)(a)-(b); sources and limitations re-point to this revision
    (sha256 recorded at submission), the whole projection re-checked
    under the 64000-byte budget.

90. **Revision 44 (2026-11-04) — F-PREPARED-CTAS-EXEMPTION-SCOPE
    (resolves the independent pre-PR design-only review of revision
    43, artifact
    `state/artifacts/issue-30938/pre-pr-review-1793728646262.json`,
    one P2 finding: the alignment decision and the four-leg
    validation plan supported; the acceptance projection's blanket
    exemption contradicts them).** (i) FINDING, VERIFIED LINE-BY-LINE
    AGAINST THE REPOSITORY AND THE RETAINED EVIDENCE: the
    projection's `limitations[8]` claimed the pre-change binary
    cannot complete PREPARED executions of set-operation CTAS shapes
    (with or without widened products), substituting in-process proof
    for live parity — but the bigint-bound prepared UNION CTAS
    division witness
    (`/tmp/30938_r85/r85_base_classify_r13.sql:7`-`:11`,
    `set @r13_three = 3; prepare r13div_u from "create table r13uout
    as select a * b as v from r13div union all select (c * d) / ?
    from r13div"; execute r13div_u using @r13_three`) executes
    SUCCESSFULLY on the pinned base (`r85_base_classify_r13.log:3`-`:6`
    records the persisted `DECIMAL(75,10)` tag and the value
    `0.0000003333`), entry 89 (i) acknowledges that success ("the
    statement succeeds on all three"), and entry 89 (v) requires four
    LIVE legs with independently calculated value comparisons — so
    the blanket exemption permitted omitting exactly the numeric
    regression those legs must detect. Additionally §8's
    behavioral-compatibility claim ("No statement that returned on
    base changes value or error surface") and the projection's
    `limitations[5]` ("behavior differences appear only for
    expression results previously rejected with ERROR 1690") retained
    unconditional compatibility claims contradicted by revision 43's
    named value exception. Every citation confirmed verbatim this
    round (read-only inspection; no service launched, no executable
    run). (ii) DECISION — EXEMPTION RESTRICTED TO THE CRASH
    EVIDENCE: the prepared-route parity exemption applies ONLY to the
    shapes and bindings with baseline crash evidence — entry 74 (vi)
    (b)'s COALESCE-merge prepared set-operation CTAS witnesses bound
    with a `DECIMAL(20,1)` parameter (the MOD branch and the
    plain-`a*b` control), which died on the parent binary in the
    pre-existing, modulo-independent TN-side `unionBatch`
    slice-bounds panic (receipts `/tmp/30938_base_mod_probe3.txt` and
    `/tmp/30938_base_mod_iso1.txt`; service logs `base_modprobe.log`
    and `base_modiso.log`): for those shapes parent parity continues
    through the in-process fill/refresh channels, live parity stands
    at statement time, and the changed engine's live prepared route
    completes CHECKED (success per this contract), never a crash. The
    bigint-bound division witness is NOT exempt: its prepared-route
    parity is LIVE value and schema validation per entry 89 (v) legs
    (1)-(2) on both engines — the candidate's value equal to the
    independently calculated value (`0.3333`, schema `(69,4)`, at the
    default increment; the aligned `0.3333000000`, schema `(75,10)`,
    at increment 10, the base's passthrough `0.0000003333` recorded
    as the excluded defect family), the persisted tag asserted
    identical across engines. (iii) §8 QUALIFIED: the
    behavioral-compatibility bullet gains §1's revision-43 named
    exclusion (amended in place above), so the value-retention
    contract reads identically at both seams. (iv) PROJECTION — ONE
    CONSISTENT CONTRACT: `limitations[5]` gains the named
    coefficient-passthrough exception; `limitations[8]` is restricted
    to the crash-evidenced shapes and bindings of (ii) and explicitly
    retains live value/schema validation for the successful
    bigint-bound witness; `limitations[7]` re-points to this revision
    (sha256 recorded at submission); the whole projection re-checked
    under the 64000-byte budget. (v) NO PRODUCTION, TEST OR FIXTURE
    CHANGES: the finding is a contract-consistency repair; entry 89
    (v)'s four live legs stand unchanged and are reinforced — no
    exemption covers them. Applicable revision-42 decisions remain
    reusable (the revision-42 PASS, artifact
    `pre-pr-review-1793708619171.json`, carries its unchanged
    provisions; revision 43's supported alignment decision and
    four-leg plan stand). The upstream PR #31034 overlap record (§8,
    kept since revision 2) stands unchanged: that PR overlaps the
    widening partially and decides none of this design's
    prepared-CTAS value contract, exemption scope, aggregate
    admission or band families.
