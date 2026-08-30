# `APPROX_PERCENTILE` ordered-set syntax

**Status:** Implemented; approval requested
**Related:** [#25110](https://github.com/matrixorigin/matrixone/issues/25110)

## Contract

MatrixOne accepts the existing aggregate form:

```sql
APPROX_PERCENTILE(value_expression, percentile)
```

and the equivalent ordered-set form:

```sql
APPROX_PERCENTILE(percentile)
  WITHIN GROUP (ORDER BY value_expression [ASC | DESC])
```

The ordered-set form has one direct percentile argument and exactly one order
item. The order expression is the aggregate input. The percentile keeps the
existing `APPROX_PERCENTILE` contract: it must be a non-NULL compile-time
constant in `[0, 1]`, and the value expression must use one of the numeric types
already supported by the aggregate.

`ASC` is the default. `DESC` reverses the percentile direction: percentile `p`
over descending values is evaluated as percentile `1-p` by the existing
ascending approximate-percentile executor. This is exact for the rank and
linear-interpolation definition used by the executor, including the endpoint
cases `p=0` and `p=1`.

The following remain unsupported:

- multiple `WITHIN GROUP ORDER BY` expressions;
- combining the ordinary two-argument form with `WITHIN GROUP`;
- `APPROX_PERCENTILE(...) WITHIN GROUP (...) OVER (...)`;
- dynamic percentile expressions and unsupported value types.

## Parser and binder lowering

The parser stores the direct percentile in `FuncExpr.Exprs`, stores the order
item in `FuncExpr.OrderBy`, and sets `FuncExpr.WithinGroup`.

The aggregate binder uses a shared ordered-set specification for `MEDIAN`,
`APPROX_PERCENTILE`, `PERCENTILE_CONT`, and `PERCENTILE_DISC`. The shared path:

1. validates the direct-argument and order-item counts;
2. binds the order expression once as the first aggregate argument;
3. appends the direct arguments;
4. records the order direction when it affects the result; and
5. resolves the existing aggregate overload.

The resulting `APPROX_PERCENTILE` plan arguments remain `[value, percentile]`,
the same shape as the ordinary syntax. An ordered-set call is distinguished by
the one-byte direction marker in `Function.AggConfig`; the ordinary form keeps
an empty marker. This also lets `EXPLAIN` reconstruct the ordered-set syntax.

## Execution and compatibility

No new aggregate executor or serialized aggregate-state format is introduced.
At compile time, an ordered-set `DESC` marker converts the validated decimal
percentile text to its exact complement. The executor still receives its
existing textual percentile configuration, so mixed execution paths and
partial-state merge use the established `APPROX_PERCENTILE` protocol.

The complement calculation uses exact rational arithmetic and preserves the
source decimal scale. It does not round through `float64`; for example,
`0.950` becomes `0.050`.

The ordinary syntax, window behavior of the ordinary syntax, result types,
NULL handling, bounded KLL-style sketch, spill accounting, and distributed
merge behavior are unchanged.

## Validation

Regression coverage includes:

- parser format/parse round trips;
- planner lowering for ascending and descending calls;
- preservation of the ordinary two-argument form;
- exact complement configuration and decimal-scale preservation;
- `EXPLAIN` reconstruction;
- invalid direct-argument count, multiple order keys, nonconstant percentile,
  and window-form rejection;
- public BVT equivalence between ordinary and ordered-set ascending syntax and
  the descending percentile result.
