# Ordered-set window functions

## Status

Proposed for review for the ordered-set aggregates already supported by MatrixOne.
This is a follow-up to issues #25110 and #25144 and to the scalar aggregate
designs in `median_within_group.md`, `approx_percentile_within_group.md`, and
`20260910-prepared-and-discrete-percentiles.md`.

## SQL surface

The existing ordered-set forms may be followed by an `OVER` clause:

```sql
PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY value [ASC | DESC]) OVER (...)
PERCENTILE_DISC(p) WITHIN GROUP (ORDER BY value [ASC | DESC]) OVER (...)
APPROX_PERCENTILE(p) WITHIN GROUP (ORDER BY value [ASC | DESC]) OVER (...)
MEDIAN() WITHIN GROUP (ORDER BY value [ASC | DESC]) OVER (...)
```

`WITHIN GROUP ORDER BY` selects and orders aggregate input values. `OVER`
independently defines partitioning, window ordering, and the frame. The
existing restrictions remain unchanged: there is exactly one ordered-set
expression, percentile `p` is a non-null constant or direct prepared marker
fixed for the execution, and the existing aggregate-specific input type rules
apply.

Ordinary forms such as `MEDIAN(value) OVER (...)` and
`APPROX_PERCENTILE(value, p) OVER (...)` retain their behavior.

## Planner lowering

Parser nodes already retain `WithinGroup`, the function-local `OrderBy`, and
the separate `WindowSpec`. The window binder now shares the ordered-set shape
validation used by scalar aggregates and lowers calls to their established
executor signatures:

- `MEDIAN`: `[value]`;
- approximate and exact percentiles: `[value, p]`.

The `ASC`/`DESC` bit remains in aggregate configuration. Window partition,
order, and frame expressions remain in the window specification, so a window
`ORDER BY` cannot be confused with the ordered-set direction.

## Execution and memory

The window operator continues to use the existing aggregate implementations.
For a full-partition frame, ordered percentile, approximate percentile, and
median are evaluated once per input partition and the single result is
broadcast to its output rows. This avoids retaining the same ordered values in
one aggregate state per output row.

Exact numeric percentiles created by the window operator receive the same
bounded-run spill configuration used by grouped aggregation. Other explicit
frames use the generic window-frame evaluator; their work depends on the total
frame cardinality, while their output remains bounded to the normal window
output chunk.

The existing type restrictions still apply. In particular,
`PERCENTILE_CONT` does not interpolate `DECIMAL256`, and
`PERCENTILE_DISC` does not accept JSON or array/vector inputs.

## Validation

Planner tests cover all four ordered-set forms, independent ordered-set and
window directions, invalid shapes, nonconstant percentiles, and prepared
percentile markers. Window operator tests cover continuous and descending
discrete results plus full-partition broadcasting. The distributed ordered-set
case covers partition-wide and explicitly framed window results.
