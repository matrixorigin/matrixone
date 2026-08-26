# `MEDIAN() WITHIN GROUP` design

**Revision:** v2, 2026-08-26
**Status:** Implemented; approval requested
**Related:** [#25110](https://github.com/matrixorigin/matrixone/issues/25110),
PR #27476

This revision replaces the v1 duplicate-expression proposal. The reviewed
decision is intentionally narrow: expose one ordered-set spelling for the
existing `MEDIAN` aggregate without adding a second expression binder,
expression-equivalence rules, planner graph cloning, or a new executor.

The v1 spelling existed only on this open PR branch. It was never merged into
`main`, released, or documented as a supported MatrixOne contract, and its
design status was explicitly `Proposed for review`. Revision v2 therefore has
no released-SQL migration burden. It intentionally removes that unapproved
prototype instead of retaining a compatibility branch whose only purpose would
be to preserve the rejected duplicate-expression design.

## Normative contract

MatrixOne accepts:

```sql
MEDIAN()
  WITHIN GROUP (
    ORDER BY value_expression
      [ASC | DESC]
  )
```

The contract is:

- `MEDIAN` has no direct argument in the ordered-set form.
- `WITHIN GROUP` is required and contains exactly one `ORDER BY` item.
- The order item's expression is the aggregate input and is bound exactly once
  with the same aggregate-input rules as `MEDIAN(value_expression)`.
- `ASC`/`DESC` is accepted grammar and does not affect the median result:
  order direction cannot change the middle value, and the existing `MEDIAN`
  implementation ignores NULL inputs. `NULLS FIRST`/`NULLS LAST` remains
  rejected by the MySQL-dialect parser, consistently with other local aggregate
  `ORDER BY` items.
- The result type, numeric compatibility conversions, empty/all-NULL behavior,
  and runtime algorithm are exactly those of ordinary `MEDIAN(expr)`.
- `MEDIAN(expr)` and its existing window form remain unchanged.
- `MEDIAN(expr) WITHIN GROUP (...)` is not accepted. It is deliberately not
  an alias for the new form.
- `MEDIAN() WITHIN GROUP (...) OVER (...)` is rejected as unsupported.
- A top-level SELECT alias or ordinal receives no special top-level
  `ORDER BY` treatment inside `WITHIN GROUP`; the item is an aggregate input.
  For example, integer `1` is a literal input, not an ordinal.

Diagnostics are stable at the binder boundary for shapes that reach it:
multiple order items return
`median requires exactly one WITHIN GROUP ORDER BY expression`, and combining
the ordered-set form with `OVER` returns the existing
`function-local ORDER BY in window function is not yet implemented` diagnostic.
Missing clauses and the former duplicate-value spelling fail in the parser.

## Standards and interoperability evidence

This feature is a **MatrixOne compatibility extension**, not a claim that
`MEDIAN() WITHIN GROUP` is itself a standard built-in function.

The syntax follows the ordered-set aggregate model:

- PostgreSQL's ordered-set documentation states that aggregated inputs are
  introduced by `WITHIN GROUP (ORDER BY ...)`, while arguments before
  `WITHIN GROUP` are direct arguments evaluated once per aggregate. It also
  exposes the zero-direct-argument shape
  `mode() WITHIN GROUP (ORDER BY value)`.
  See [PostgreSQL aggregate functions](https://www.postgresql.org/docs/current/functions-aggregate.html)
  and [aggregate expression syntax](https://www.postgresql.org/docs/current/sql-expressions.html#SYNTAX-AGGREGATES).
- Existing products generally expose median as `MEDIAN(expr)`, not by
  duplicating the row expression on both sides of `WITHIN GROUP`. Examples:
  [Amazon Redshift MEDIAN](https://docs.aws.amazon.com/redshift/latest/dg/r_MEDIAN.html),
  [Snowflake MEDIAN](https://docs.snowflake.com/en/sql-reference/functions/median),
  and [Oracle MEDIAN](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/MEDIAN.html).

Issue #25110 originally defines the general `WITHIN GROUP` surface. Its
[2026-08-22 status analysis](https://github.com/matrixorigin/matrixone/issues/25110#issuecomment-5378210572)
explicitly lists MEDIAN among the aggregates not yet attached after the core
GROUP_CONCAT/percentile MVP. This revision closes only that MEDIAN attachment
gap. It does not claim to complete a generic user-defined ordered-set aggregate
framework.

## Parser and planner design

The parser has two disjoint MEDIAN productions:

1. `MEDIAN(expr) [OVER (...)]` for the existing aggregate/window behavior.
2. `MEDIAN() WITHIN GROUP (ORDER BY expr) [OVER (...)]` for this extension.

The second production stores no direct `FuncExpr.Exprs`, stores the single
ordered-set item in `FuncExpr.OrderBy`, and marks `WithinGroup`.

`HavingBinder.bindMedianWithinGroupAgg` validates the order-item count, binds
that one expression once, applies the same stored MySQL ENUM/SET numeric
contract as ordinary MEDIAN, and calls the existing one-argument MEDIAN
overload. It does not bind an expression for comparison, clone a query builder,
walk CTE/view metadata, use reflection, or create validation-only plan nodes.

## Cost and resource bound

Let `n` be the AST size of the order expression and `P(n)` the existing cost
of binding that expression, including any scalar subqueries it explicitly
contains.

- Feature-specific validation is `O(1)` time and allocation.
- Total planning cost is `O(P(n))`, because the expression is bound once.
- Peak feature-specific state is one bound expression reference plus the
  existing parser order item; there is no copied parent plan/context graph.
- A scalar subquery occurrence creates the same one subquery plan it would
  create in ordinary `MEDIAN(subquery)`; nesting does not multiply validation
  passes.
- The resource ceiling is therefore the existing statement/parser/binder
  ceiling and caller cancellation/deadline. No MEDIAN-specific node/depth/byte
  budget is added because this path introduces no additional traversal or
  amplification beyond ordinary MEDIAN.

`BenchmarkBuildMedianForms` is reproducible reference evidence. It compares
ordinary and ordered-set forms for a column and for a scalar subquery using the
same parse-plus-plan loop and reports allocations. The acceptance invariant is
not a wall-clock threshold: the ordered-set form must stay in the same
allocation/complexity class and must never add a second subquery bind or a
parent-plan clone. Any future change that adds such a pass requires a new design
revision and an explicit resource budget.

The executable regression
`TestMedianWithinGroupDoesNotDuplicateScalarSubqueryPlan` compares the node-type
multiset produced by ordinary and ordered-set scalar-subquery MEDIAN calls. It
fails if the wrapper introduces an additional subquery/planner branch. The
single `BindExpr` call in `bindMedianWithinGroupAgg` is the source-level bound;
the benchmark remains diagnostic evidence rather than a flaky timing gate.

Reference run on linux/amd64 (AMD Ryzen 9 5900HX, Go benchmark
`-benchtime=200x -count=3`) produced the following stable allocation counts:

| Form | Bytes/op | Allocs/op |
|---|---:|---:|
| `MEDIAN(a)` | 35,353 | 355 |
| `MEDIAN() WITHIN GROUP (ORDER BY a)` | 35,873 | 366 |
| `MEDIAN((SELECT 1))` | 44,616-44,617 | 450 |
| `MEDIAN() WITHIN GROUP (ORDER BY (SELECT 1))` | 47,592-47,593 | 455 |

The ordered-set wrapper adds 520 bytes/11 allocations for the column case and
2,976-2,977 bytes/5 allocations for the scalar-subquery case. Most
importantly, the scalar case creates one subquery plan and does not scale with
the already-built parent planner graph. Runtime samples are intentionally not
used as a gate because scheduler and allocator noise dominates this small
planning benchmark.

## Alternatives considered

| Alternative | Interoperability | Planner cost/risk | Decision |
|---|---|---|---|
| `MEDIAN(value) WITHIN GROUP (ORDER BY value)` | No standard or vendor precedent found; duplicates an expression that ordered-set syntax normally treats as two different argument classes | Requires a durable semantic-equality contract across aliases, variables, subqueries, CTEs, name modes, and future binder rules; the v1 prototype added about 1.2k non-generated planner lines and cloned parent state | Rejected |
| Compare two bound `plan.Expr` values | Keeps duplicate syntax | Identity-bearing subquery node IDs make equal source expressions unequal; binding twice creates discarded planner state | Rejected |
| Canonicalize two ASTs with an isolated validation binder | Can emulate duplicate syntax | Duplicates binder semantics, deep-copies reachable planner/context graphs, and can become superlinear for nested subqueries | Rejected |
| Rewrite to `PERCENTILE_CONT(0.5) WITHIN GROUP (...)` | Uses an established ordered-set form | Changes MEDIAN's existing type/compatibility contract and couples the feature to percentile interpolation behavior | Rejected |
| `MEDIAN() WITHIN GROUP (ORDER BY value)` | Follows the zero-direct-argument ordered-set shape while being explicitly documented as a MatrixOne extension | One normal aggregate-input bind and existing MEDIAN execution | Selected |

## Validation contract

Parser tests cover deparse/parse round trips for the selected zero-argument
shape. Planner tests cover:

- ordinary MEDIAN and the ordered-set form;
- ascending and descending order, plus the explicit NULL-ordering rejection;
- scalar-subquery input;
- rejection of the former duplicate-value spelling;
- rejection of multiple order items;
- rejection of the window combination.

The public BVT compares ordinary and ordered-set MEDIAN on the same nullable
data and includes the multiple-order-item negative control. The structural
regression guards single-subquery planning, while the benchmark records column
and scalar-subquery allocation evidence. Generated
`mysql_sql.go` must be regenerated from `mysql_sql.y`, and regeneration must
be byte-stable.
