# Ordered-set window functions

## Status

Proposed for review for the ordered-set aggregates already supported by
MatrixOne. Merge of the implementation is gated on explicit maintainer approval
of this design in PR #29106; a code review without that explicit decision is not
treated as design approval. This is a follow-up to issues #25110 and #25144 and
to the scalar aggregate designs in `median_within_group.md`,
`approx_percentile_within_group.md`, and
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

Window output is emitted in batches of at most `DefaultBatchSize` rows. The
full-partition path finalizes every logical partition in the current
materialized input generation once and retains only one result value per
partition across those output calls. Each call copies the applicable cached
value into its output batch. The cache is released after the generation's last
output chunk, or immediately by an error path, `Reset`, or `Free`. It is never
carried into the next prepared-statement execution or the next materialized
input generation.

Exact numeric percentiles created by the window operator receive the same
bounded-run spill configuration used by grouped aggregation. Other explicit
frames use the generic window-frame evaluator; their work depends on the total
frame cardinality, while their output remains bounded to the normal window
output chunk.

### Capacity model

For `N` rows and `P` logical partitions in one materialized generation:

- the existing window input and evaluated argument vectors retain `O(N)` data;
- a full-partition ordered-set execution owns `P` aggregate groups and performs
  exactly `N` `Fill` operations, independent of the number of output chunks;
- after finalization, the cross-chunk cache retains `P` result values and each
  returned output owns at most `DefaultBatchSize` result values;
- exact numeric percentile state spills bounded sorted runs according to
  `sort_spill_mem`; its final merge uses bounded per-run cursors and result
  scratch;
- aggregate-specific state that does not implement spill keeps its existing
  capacity behavior, but only one state per partition rather than one state per
  output row;
- a non-full frame intentionally uses the generic evaluator. Its CPU work can
  depend on the sum of frame cardinalities, although live output state remains
  bounded by one output chunk.

This optimization does not make the already-materialized window input itself
streaming and does not claim constant RSS for all aggregate implementations.

### Cancellation and failure ownership

The query context is supplied both to spill triggered by `Fill` and to
context-aware aggregate finalization. Consequently tail-run writes, run
compaction, sorting, and spilled-rank selection observe query cancellation.
All window finalization paths use `aggexec.FlushWithContext`; aggregates without
a context-aware finalizer retain the established `Flush` behavior.

On cancellation or any spill/finalization error, the chunk-local aggregate is
freed before returning, which closes its spill file and releases resident
state. A partially built partition-result cache is also freed. `Reset` and
`Free` cover early consumer termination (for example `LIMIT`) and make the
operator safe for reuse after both success and failure.

### Prepared execution and compatibility

A direct prepared percentile marker is materialized once for each execution
and remains fixed for that execution. `Reset` drops the cached partition
results and prepared-parameter provenance before the next `Prepare`; no value,
spill file, or aggregate state from the previous execution may be reused.

The change adds no aggregate ID, protobuf field, or partial-aggregate wire
format. It lowers to the aggregate argument/configuration shapes already used
by scalar ordered-set aggregates. An older planner continues to reject the new
window SQL form. During a rolling upgrade, the once-per-partition resource
bound is guaranteed only on CNs containing this window implementation; feature
acceptance therefore requires all executing CNs to be upgraded before relying
on that bound.

The existing type restrictions still apply. In particular,
`PERCENTILE_CONT` does not interpolate `DECIMAL256`, and
`PERCENTILE_DISC` does not accept JSON or array/vector inputs.

## Validation

Planner tests cover all four ordered-set forms, independent ordered-set and
window directions, invalid shapes, nonconstant percentiles, and prepared
percentile markers. Window operator tests cover continuous and descending
discrete results, full-partition broadcasting across more than one output
chunk, exactly-once spill input accounting, cancellation during finalization,
early-stop cleanup, and reuse boundaries. Aggregate-level spill tests cover
cancelled tail writes, compaction, rank selection, file closure, and subsequent
reuse. The distributed ordered-set case covers partition-wide and explicitly
framed window results.

Acceptance requires:

1. explicit maintainer approval of this design revision;
2. planner, window, aggregate spill, and distributed ordered-set tests passing;
3. no result or resource-state carry-over across prepared executions;
4. unchanged behavior for ordinary aggregate windows and scalar ordered-set
   aggregates.
