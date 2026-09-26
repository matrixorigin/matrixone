# Direct row comparison with a scalar subquery

Design revision: 3. Owning issue: #28295. Implementation: PR #29238. Design review decisions and exact revisions are recorded in the task record.

## Contract

MatrixOne accepts `(x, y) op (SELECT a, b ...)` and the reversed form for `=`, `<>`, `<`, `<=`, `>`, `>=`, and `<=>`. The row width must equal the subquery column count. This is a **scalar** subquery: zero rows produce a nullable row, one row supplies the values, and more than one row raises the existing scalar cardinality error 1242. Comparison uses SQL three-valued row rules; `<=>` yields a non-NULL Boolean. These rules also apply to correlated and prepared execution. Existing row-valued `IN` and `ANY` retain their own cardinality rules.

The original issue was reproduced on historical main with direct comparison rejected as `[TUPLE TUPLE]`; related `IN` and `= ANY` controls worked. PR #29238 adds binding and flattening of the direct form. Review found three routes that can violate the scalar or single-evaluation contract. The review's SQL counterexamples are source-derived and need execution evidence before being described as observed runtime failures.

## Ownership and plan flow

`baseBinder` owns arity and operator admission and puts the bound row in the scalar `SubqueryRef.Child`. `QueryBuilder` owns flattening, correlation, cardinality, and production of per-column comparison operands. The existing SINGLE/LEFT join and aggregate fallback own row existence and empty-aggregate values. Execution owns function evaluation. The comparison must remain one scalar unit until cardinality is enforced; translating it to `IN` or `ANY` would change the contract.

The no-FROM PROJECT substitution is valid only when its PROJECT and VALUE_SCAN child are proven to emit exactly one row. `LIMIT`, `OFFSET`, or `RankOption` on either node removes that proof. Such shapes use the ordinary scalar path. The shortcut's EXISTS, IN, and quantified cases also depend on row existence, so the guard applies before its switch. The planner must validate the child index and shape before dereferencing it.

After flattening, ordinary `=`, `<>`, and `<=>` field comparisons combine under the existing SQL Boolean rules. Lexicographic operators call `unwindTupleComparison`, which puts a non-final field in both strict and equality branches. A volatile value there could run twice. The new direct row-scalar path admits that expansion only when each non-final field on both sides is nonvolatile. Volatile final fields remain admissible because this expansion uses them once. The existing `containsVolatileFunction` classifier treats unknown function metadata and nested subqueries conservatively. The binder normalizes reversed operators before this common check.

For a correlated scalar aggregate, post-join reconstruction restores an empty aggregate result and applies HAVING consistently to the returned row. The current reconstruction copies its HAVING predicate into a CASE for each output column. Volatile HAVING cannot be copied safely and is rejected **before** the original FILTER is bypassed. Deterministic HAVING remains accepted. A failed plan build has no sequence side effect or partially published result.

The existing negative `AuxId` map memoizes **flattening**, not evaluation at runtime. It cannot establish one volatile evaluation per row. Supporting the rejected shapes would need a materialized, executor-visible value/status for each outer row and optimizer rules preserving that boundary. That design is outside this PR.

## Alternatives and decision

| Option | Result |
| --- | --- |
| Keep direct row comparison unsupported | Leaves #28295 unresolved. |
| Materialize every volatile field and HAVING status | Can support more SQL, but adds plan nodes, runtime state and optimizer constraints to this planner change. Its once-per-row and empty-result behavior need a separate design. |
| Guard the unsafe rewrites and use existing scalar plans | Chosen. It preserves deterministic direct comparisons with small changes and reports a clear unsupported error for shapes that would silently change evaluation count. |

The chosen limit concerns the new direct row-scalar ordering and correlated aggregate reconstruction paths. It does not expand this PR into pre-existing tuple comparison rewrites or ordinary scalar HAVING behavior. Revisit only with a concrete shared materialization design and tests.

## Failure, compatibility, and cost

An arity mismatch fails at bind time. A scalar with multiple rows keeps error 1242 even if an early field could decide a comparison. Zero rows, NULL fields and HAVING suppression must preserve one coherent nullable row. Unsupported volatile shapes fail during planning, before execution or side effects. A failed prepared execution leaves the statement reusable under its existing lifecycle. No new goroutine, cache, global state, catalog value, wire field or storage format is introduced; restart, backup and mixed-version data migration do not apply.

The guards inspect row fields and HAVING expressions linearly at planning time. Skipping no-FROM substitution may retain a scalar join for a paginated constant query. No per-row allocation is added by these guards. Existing comparison expansion is linear in row width; deterministic non-final fields may appear in two branches. Row width and expression depth remain subject to existing SQL/planner limits. Do not claim a general throughput improvement from this correction.

## Validation and acceptance

1. Public SQL tests distinguish no-FROM `LIMIT 0` and `LIMIT 1 OFFSET 1` (empty scalar) from `LIMIT 1 OFFSET 0` (one row), including an empty `<=>` result. Inspect the plan once to identify pagination placement; the guard must cover that actual placement.
2. A planner test rejects a volatile non-final field for both row operand orders and representative ordering operators. A volatile final-field control still binds. A public sequence probe, if available, checks whether the pre-fix rewrite actually doubles side effects; source evidence alone is labeled as a hazard.
3. A planner test rejects volatile HAVING in a correlated two-column aggregate row and retains a deterministic HAVING plus empty COUNT/SUM control. A public probe, if available, distinguishes a partially NULL row from one coherent scalar row.
4. Reuse existing tests for equality, inequality, ordering, NULL, zero/one/multiple rows, correlation, arity, prepared SQL/binary execution, type coercion and `IN`/`ANY`. Add a case only when it distinguishes a new invariant. The relevant planner UT and public SQL test must pass on the repaired/rebased source; affected CGo prerequisites, static analysis and current CI are checked by their own modes.

Acceptance requires no wrong result for the three reviewed paths, no regression in existing direct comparison controls, no new runtime resource owner, and a clean rebase with the current main. Real volatile evaluation support remains a documented limitation, not a silent wrong-result path.

## Non-equality correlated aggregate follow-up

The existing LEFT JOIN plus reaggregation fallback bypasses the scalar aggregate subtree. Executed counterexamples on revision `f8cd4525` showed three missing proofs: a root `LIMIT 0` was lost, an unmatched synthetic join row contributed to `COUNT(1)` and `SUM(COALESCE(...))`, and an earlier flattened scalar output disappeared from the new aggregate schema. The fallback owns all three contracts for both ordinary scalar and direct row comparisons.

Before replacing the subtree, require no Limit, Offset, or RankOption on every bypassed PROJECT or AGG node. Require the current outer stream to expose the sole bound base scan and its Row_ID unchanged, through only transparent FILTER/SORT wrappers; grouping that scan's columns, including Row_ID, then preserves its outputs and duplicate rows. Prior scalar joins, aggregates, projects, and other output-changing outer compositions are unsupported until a live-output passthrough and remapping design exists. These checks must precede appending replacement JOIN/AGG nodes and return specific unsupported errors.

On the inner side, require an accessible non-null Row_ID from the admitted scan path. It marks real rows after the LEFT JOIN. Rewrite `COUNT(*)` to `COUNT(marker)`. Admit known unary NULL-skipping COUNT, SUM, MIN, MAX, and AVG aggregates over direct inner columns or typed literals; mask each argument to typed NULL when the marker is absent, preserving the original overload, DISTINCT flag, and result type. Ordered `GROUP_CONCAT` is an existing user of this fallback: its executor skips a row when a concatenated value is NULL. Admit its configured argument list only when every value/order argument is a direct inner column or typed literal; mask each to NULL on the synthetic row and preserve its argument indexes, order configuration and DISTINCT flag. Reject other unknown or multiargument aggregates, computed/volatile/correlated arguments, and inaccessible markers. In particular `SUM(COALESCE(v,0))` must fail explicitly until argument materialization on real inner rows is designed; it must never silently return zero on an empty input. The marker check and CASE result must be nullable at the join consumer, so optimizer nullability cannot erase the mask. This changes only expression work inside the existing join/reaggregate plan; it adds no runtime owner or persistent format.

Public QA uses the existing two-outer/one-inner fixture. Distinct cases prove root `LIMIT 0` rejection, `COUNT(1)`/`SUM(v)` on empty and matched rows, literal SUM and DISTINCT count, matched NULL versus missing row, explicit rejection of computed arguments and prior scalar outputs, and a valid query after repeated errors. Planner tests cover pagination placement, nullable marker masking, outer/inner shape guards, and preservation of ordered `GROUP_CONCAT` configuration; public SQL checks its empty and matching-row results. Reuse existing equality, HAVING, quantified, prepared, and cardinality controls; run the owning planner suite and public BVT after implementation. Rebase on current main and invalidate only evidence affected by its delta.
