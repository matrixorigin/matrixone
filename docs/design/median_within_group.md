# `MEDIAN ... WITHIN GROUP` design

**Revision:** v1, 2026-08-26  
**Status:** Proposed for review  
**Related:** [#25110](https://github.com/matrixorigin/matrixone/issues/25110),
PR #27476

This document is the design contract for the `MEDIAN` ordered-set spelling
introduced by PR #27476.  It is intentionally versioned in the repository so
that parser, binder, planner, and test changes can be reviewed against one
stable semantic contract.

## Scope

The supported new form is:

```sql
MEDIAN(value_expression)
  WITHIN GROUP (ORDER BY value_expression [ASC | DESC])
```

The value and `ORDER BY` expression must denote the same expression.  The
direction is accepted for SQL compatibility and does not change the median
value.  The existing `MEDIAN(value_expression)` form is unchanged.  Existing
`GROUP_CONCAT` and `PERCENTILE_CONT`/`PERCENTILE_DISC` ordered-set paths keep
their existing contracts; this change does not generalize their semantics.

The following are outside this revision:

- more than one value or `WITHIN GROUP` order expression;
- using a different order key as an implicit median sort expression;
- ordered-set window syntax (`... WITHIN GROUP (...) OVER (...)`);
- new aggregate functions or a new executor implementation.

Invalid shapes and a value/order mismatch return a syntax error before an
aggregate is added to the query plan.  Type checking, NULL handling, and the
runtime median algorithm remain the existing MEDIAN implementation's
responsibility.

## Expression-equivalence contract

The equality check is semantic within the query block, not a comparison of
the two bound plan objects.  The validator performs these steps on independent
clones:

1. Apply the same source-name and output-alias/ordinal precedence used by the
   real binder, retaining query-block and correlation depth.
2. Resolve columns to binding identity (binding tag, column position, and
   correlation depth).  Explicit database qualifiers remain significant,
   while compatibility schemas and `lower_case_table_names` follow normal
   catalog resolution rules.
3. Recursively remove parser `ParenExpr` wrappers, including wrappers beneath
   predicate nodes such as `IS TRUE`, `IS FALSE`, and `IS UNKNOWN`.  These are
   binder no-ops and must not affect the key.  Operator tree shape, literal
   identity, parameter ordinal, variable scope, and explicit casts remain
   significant.
4. Bind scalar subqueries and derived/CTE/set-operation bodies in isolated
   validation scopes.  Local references are compared locally; correlated
   references retain their outer scope identity.  Unused CTE declarations do
   not become consumers merely because validation visited a clone.
5. Format the resulting canonical AST key and compare the keys.  No
   commutative rewriting or value execution is performed, so expressions that
   only happen to evaluate to the same value are not treated as equivalent.

For example, `a`, `A`, and `table.a` can be equivalent when they resolve to
the same column under the active name mode, and `cast((a) is true as signed)`
is equivalent to `cast(a is true as signed)`.  An inner column and a
correlated outer column, a different database, a different variable scope,
or a different literal/parameter remains non-equivalent.

After validation succeeds, the real value expression is bound exactly once
by `HavingBinder` and that plan expression is passed to the existing MEDIAN
executor.  The validation binds are never reused as executable plan nodes.

## Planner-state ownership and isolation

Validation is read-only with respect to the real query plan.  The temporary
builder/context graph deep-copies mutable bindings, node/table metadata,
alias/projection maps, CTE/view state, and expression metadata before binding a
clone.  Subquery and set-operation validation allocates fresh tags after the
parent range and records local contexts in the temporary builder.  A visited
pointer set bounds recursive AST/CTE traversal.  No validation-only node,
aggregate, CTE occurrence, or view consumer is appended to the executable
query.

The only state that crosses the validation boundary is the boolean
accept/reject result and the canonical equality key.  This keeps retries,
prepared executions, and nested MEDIAN expressions from sharing identity or
mutable planner state.

## Compatibility and diagnostics

- Existing non-`WITHIN GROUP` MEDIAN binding continues through its previous
  numeric overload path.
- `MEDIAN ... WITHIN GROUP` requires exactly one value expression and exactly
  one order expression; a mismatch uses the stable diagnostic
  `median requires the WITHIN GROUP ORDER BY expression to match its value
  expression`.
- Existing ordinary and window aggregate validation remains unchanged.  The
  new ordered-set form is rejected when combined with `OVER`.
- The feature does not alter catalog formats, wire protocols, or executor
  aggregate IDs; it lowers to the existing one-argument MEDIAN aggregate.

## Cost and limits

For an expression with `n` AST nodes, canonicalization is linear in `n` aside
from the existing binder work for scalar subqueries and CTE bodies.  The
validator performs no data scan or runtime expression evaluation.  Cloning and
isolating metadata are bounded by the expression/query-block graph and are
released with the temporary builder; the executable path performs one normal
value bind.  This deliberately favors deterministic correctness over trying to
compare identity-bearing plan nodes or introducing a second partial binder.

## Validation matrix

Unit/planner coverage includes:

- ordinary MEDIAN controls with and without redundant parentheses;
- `IS TRUE`/`IS FALSE`/`IS UNKNOWN` wrappers, including a parenthesized inner
  column;
- case-insensitive column/function identifiers and equivalent qualifications;
- scalar subqueries, correlated versus local references, aliases, ordinals,
  derived tables, CTEs, recursive CTEs, and set operations;
- lower-case table-name modes, compatibility schemas, variables, parameters,
  literals, wrong databases, and deliberately different expressions;
- validation-state isolation and CTE-consumer non-leakage.

The public ordered-set BVT keeps a successful MEDIAN `WITHIN GROUP` query and
the negative mismatch path.  Any future grammar or canonicalization change
must add a positive and nearest negative case to both the planner matrix and
the public SQL coverage when it changes this contract.

## Alternatives considered

1. **Compare bound `plan.Expr` values.** Rejected: scalar-subquery node IDs,
   query-block tags, and other execution identities make semantically equal
   expressions compare unequal.
2. **Compare only deparsed SQL.** Rejected: aliases, ordinals, correlation,
   database/name modes, and variable scopes are resolved semantics rather than
   spelling.
3. **Use a broad expression simplifier or commutative normalizer.** Rejected:
   it could change literal/parameter identity or accept expressions whose
   evaluation is not the same ordered-set key.
4. **Add a second execution binder for the order expression.** Rejected: it
   duplicates planner state and can create discarded identity-bearing nodes;
   only the accepted value expression is bound for execution.

