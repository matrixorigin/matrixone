# Sort-based ROLLUP design

Status: implementation complete in the dedicated worktree; validation evidence
is complete

Owner issue: [#20653](https://github.com/matrixorigin/matrixone/issues/20653)

Implementation branch: `codex/issue-20653-sort-rollup`

## Problem and scope

The current ROLLUP lowering expands `ROLLUP(a, b)` into three independent
grouping-set aggregates joined by `UNION ALL`. That preserves SQL results, but
re-reads the input and builds three aggregation states. Issue #20653 asks for a
physical shape equivalent to:

```text
Scan -> Sort(a, b) -> one Rollup Aggregate
```

This design adds a physical path for a single-query-block ROLLUP. The planner
uses a conservative cost selector when statistics are available; the existing
hash/grouping-set path remains the fail-safe for unknown statistics and
unsupported shapes.

## Goals and non-goals

Goals:

- sort the input once by the grouping equality keys when no suitable order is
  already available;
- reuse a proven global input order and maintain the full grouping level and
  each live ROLLUP prefix in one streaming aggregate pass;
- preserve the distinction between a source NULL and a NULL-like rollup value;
- keep the path coordinator-local and fail closed when its ordering or state
  contract cannot be proved;
- make the forced sort path useful for plan validation and benchmarks while
  allowing the default planner to choose between the two physical paths.

Non-goals of this first implementation:

- CUBE, arbitrary GROUPING SETS, windows, or repeated grouping expressions;
- remote serialization of the final rollup operator;
- aggregate spill/reload inside the streaming aggregate. Its active prefix
  state is bounded, while an unordered input still uses the existing spilling
  sort operator before the aggregate; a capacity failure never silently falls
  back to an unordered partial aggregate.

## Semantics and invariants

For `ROLLUP(k1, k2)`, the ordered input has contiguous classes by `(k1,k2)`.
The executor owns one state for each live prefix:

```text
full key:    (k1, k2)
subtotal:    (k1, NULL-like)
grand total: (NULL-like, NULL-like)
```

Every input row is added exactly once to every currently applicable state.
When a key boundary is observed, the state machine flushes and recreates only
the prefixes that ended, from the deepest level to the shallowest. Each
finished row is appended to a bounded output batch immediately; aggregate
state is never combined by averaging finalized values and is never retained
for already-emitted groups.

The rollup value is a vector constant carrying grouping metadata. Its value is
NULL-like and its grouping bitmap is independent from the ordinary NULL bitmap:

```text
row kind                   GROUPING(k1)  GROUPING(k2)
detail with source NULL          0            0
subtotal for k1                  0            1
grand total                      1            1
```

All rollup key output types are nullable. Empty input still emits exactly one
grand-total row (`COUNT(*) = 0`, other standard aggregates NULL).

The comparator used by the boundary state machine is the same SQL ORDER BY
comparator used by the sort/merge operator. The first implementation applies a
conservative type whitelist for which the hash equality domain and SQL order
domain are proven compatible. FLOAT32/FLOAT64 (including NaN and scale
normalization cases) and JSON are deliberately excluded and fall back to the
existing hash/grouping-set rewrite; this avoids treating an order peer as two
groups or two hash-equal values as separate rollup boundaries.
String and pad-space keys are also outside the current whitelist, so their
existing physical-key support remains a defensive boundary for a future
extension; today they use the legacy path.

## Eligibility and planner contract

The planner considers the sort path only for one non-empty ROLLUP list over a
single base-table source, or over a conservative derived table whose explicit
`ORDER BY` already establishes the complete grouping-key prefix. Grouping terms
must be distinct direct column references, their bound types must pass the same
conservative order/equality whitelist as the executor, and the query block must
have no named or inline window expression. Joins, ordinary derived tables,
table functions, CTE references, and other source forms fall back to the
existing rewrite. The planning probe
deep-copies the source, WHERE, and grouping AST before binding, so eligibility
and cardinality checking cannot mutate the real query block.
An unresolved grouping name that matches any explicit SELECT alias is rejected
as well; this prevents alias binding from collapsing two visible ROLLUP levels
onto one source expression after the sort path has bypassed grouping-set
expansion.

The session-only `rollup_algorithm` enum provides `COST` (the default), `SORT`,
and `HASH` modes. `COST` uses the model below; `SORT` forces the eligible sort
path for plan validation and benchmarks; `HASH` forces the legacy
hash/grouping-set path. Unsupported forms, unknown statistics, or an infeasible
streaming aggregate always fall back to hash. The old
`optimizer_hints=rollupSort=1|2` spelling remains a compatibility fallback when
the new variable is left at `COST`.
Aggregates whose result observes input order (`GROUP_CONCAT`, `JSON_ARRAYAGG`,
and `JSON_OBJECTAGG`) are also kept on the legacy path because the grouping-key
sort is not their user-requested intra-group order.

The planner adds an internal SORT over visible equality keys only when the
input-order property is not already proven. A uniqueness-derived subset hash
key is not inferred for this path because it cannot replace a visible ROLLUP
level. Post-aggregate filters remain above the marked aggregate; ordinary
aggregate filter pushdown is a semantic barrier for sort ROLLUP. If a future
eligible type needs an explicit physical equality key (for example, pad-space
strings), that key must be added to both the whitelist and the ordered-state
proof before this path is enabled.

### Cost model

The selector uses dimensionless relative work units. `Stats.Cost` supplies the
scan component and the remaining coefficients are calibrated relative weights;
they are not presented as elapsed time. Let `N` be the filtered input rows,
`L` the number of grouping keys, and `B = L + 1` the number of hash branches.
For each prefix, `K_i` is its key-width factor and `A` is the aggregate update
factor derived from the selected aggregate functions:

```text
HashWork = B * (ScanCost + hash-branch-startup)
         + N * (hash-key-cost * sum(K_i)
                + hash-aggregate-cost * A * B)

SortWork(unordered) = ScanCost
         + N * log2(N + 1) * compare-cost * full-key-factor
         + N * boundary-cost * L
         + N * aggregate-cost * A * B
         + sort-startup-cost

SortWork(ordered) = ScanCost
         + N * boundary-cost * L
         + N * aggregate-cost * A * B
```

Hash branches may overlap in wall time, so the estimate combines total CPU/IO
work with a bounded branch-latency component. The overlap is capped by an
explicit `max_dop`, or by the CN's effective `GOMAXPROCS` when `max_dop = 0`;
it does not pretend that `UNION ALL` removes branch work. Unordered sort
includes the full input sort and the per-level aggregate updates: it saves
repeated scans and hash lookups, not aggregate updates. When order is reused,
the sort comparison/startup terms and row-scaled sort-workspace bound are
removed. Configured sort workspace and aggregate-state limits add bounded
penalties. Both ordered and unordered sort paths check the same bounded
streaming state: the active `L+1` prefix states plus fixed per-input-batch
scratch. Unordered input additionally pays for its sort workspace and can spill
there; it does not materialize one aggregate state per emitted group. `N_cap`,
retained below as a diagnostic output-group upper bound, is the larger of the
filtered input estimate and the table-row estimate so stale or optimistic
filtered `Outcnt` cannot make diagnostics look artificially small.
For the eligible direct derived-table shape, wrapper `TableCnt` is not treated
as a leaf-table invariant: when it is stale or generic, the derived output
`Outcnt` is used as the ROLLUP input cardinality. Leaf scan statistics retain
the strict `Outcnt <= TableCnt` consistency check.

The state estimate is admitted automatically only for aggregates with a fixed
state contract whose arguments can be proven fixed-width by the isolated
probe. DISTINCT and variable-cardinality
aggregates such as `GROUP_CONCAT`, JSON/array aggregation, percentiles, and
bitmap construction use the legacy hash path. The bound also includes hidden
aggregates referenced by `HAVING` or `ORDER BY`, not only projected expressions.

Automatic selection requires `SortCost < 0.80 * HashCost`. The probe binds the
WHERE predicate on an isolated scan before reading `N`; unsupported predicates,
missing/unknown statistics, non-finite values, and overflow all select hash.

### Benchmark evidence

The reproducible operator benchmark is:

```text
go test -mod=mod ./pkg/sql/colexec/group -run '^$' \
  -bench 'BenchmarkRollupAlgorithms/(large_ordered_low_ndv|large_unordered_low_ndv|large_ordered_one_key|large_ordered_single_group|large_ordered_many_levels|large_ordered_wider_ndv|large_ordered_high_ndv|large_ordered_avg_many_levels|large_ordered_very_many_levels|large_ordered_extreme_levels|million_ordered_low_ndv)/(sort|hash-serial|hash-parallel)$' \
  -benchtime=5x -count=3
```

The derived-order shape can be run separately with:

```text
go test -mod=mod ./pkg/sql/colexec/group -run '^$' \
  -bench 'BenchmarkRollupAlgorithms/million_(ordered_low_ndv|derived_order_low_ndv|derived_order_avg_low_ndv)/(sort|hash-serial|hash-parallel)$' \
  -benchtime=5x -count=3
```

It runs on the same materialized `int32` input, includes the sort operator for
unordered cases, skips it for the paired ordered cases, and executes all
legacy grouping-set branches in the hash case. Every large case has at least
100,000 input rows. On the development M4 CN (`GOMAXPROCS=10`), the measured
matrix was:

```text
shape                              sort          hash-serial       hash-parallel
100000 rows, 3 keys, NDV=4         1.10–1.20 ms   4.44–4.51 ms       2.38–2.42 ms
100000 rows, 3 keys, unordered     2.40–2.49 ms   4.53–4.72 ms       2.40–2.43 ms
100000 rows, one key, NDV=8        0.42–0.45 ms   0.60–0.62 ms       0.58–0.60 ms
100000 rows, one complete group    3.75–4.00 ms   36.53–37.19 ms     11.12–12.01 ms
100000 rows, 12 keys, NDV=2        3.73–3.91 ms   36.67–37.50 ms     10.66–11.44 ms
100000 rows, 12 keys, NDV=4        3.85–4.03 ms   36.68–36.95 ms     11.37–12.44 ms
100000 rows, 12 keys, NDV=64       5.58–6.12 ms   38.40–39.01 ms     10.76–12.24 ms
100000 rows, 12 keys, AVG          6.37–6.52 ms   39.51–39.67 ms     11.26–12.25 ms
100000 rows, 20 keys, NDV=2        6.63–6.86 ms   87.39–91.03 ms     22.72–24.37 ms
100000 rows, 32 keys, NDV=2       10.19–10.60 ms  202.27–205.88 ms     46.93–53.68 ms
1000000 rows, 3 keys, NDV=4       10.61–11.17 ms   43.81–46.05 ms     22.48–22.72 ms
1000000 rows, child ORDER BY      24.49–24.64 ms   96.75–99.30 ms     40.65–41.55 ms
1000000 rows, child ORDER BY + AVG 23.84–25.27 ms  64.06–69.27 ms     36.15–42.28 ms
```

The 100000-row cases show the actual advantage of the new combination:
reusing an existing order removes the global sort, and streaming keeps only
`L+1` active states. The strongest CPU wins are many ROLLUP levels or very few
complete-key runs. The single-group `BulkFill` fast path makes even one ordered
key faster in this operator-level test, but its margin is small and the
automatic selector remains conservative. The serial hash column approximates a
`max_dop=1` or otherwise serialized UNION ALL; that is where the advantage is
largest, reaching about 20x on 32 levels. With concurrent branches, 12 levels
are about 2.8–3.0x faster and 32 levels about 4–5x faster. The paired unordered
low-NDV case remains a counterexample where adding a global sort is not better
than parallel hash. These measurements are calibration evidence, not
machine-independent elapsed-time constants; the planner keeps a cost margin
and falls back to hash when statistics or order proof is uncertain.
Known empty-table statistics remain valid and estimate one grand-total group.

The last row models the concrete derived-table query shape:

```sql
SELECT d.a, d.b, d.c, count(*)
FROM (SELECT a, b, c FROM t ORDER BY a, b, c) AS d
GROUP BY d.a, d.b, d.c WITH ROLLUP;
```

For this shape, the forced SORT plan contains one table scan, one sort, and one
streaming aggregate. The forced HASH plan expands to four table scans, four
sorts, five aggregates, and three `UNION ALL` nodes. The derived-order benchmark
therefore charges the child `ORDER BY` once for SORT and once per grouping-set
branch for HASH; it is a comparison of the current physical plan shapes, not a
claim that an arbitrary unordered ROLLUP should always sort. The additional
`AVG` row uses two grouping keys plus a third measure column and matches the
issue's `AVG(x)` shape.

The aggregate marker is carried in `ExtraOptions`, which is already part of
the plan representation. The final group is compiled only after the global
ordered stream is available, on one coordinator scope, with no partial-result
merge stage. The operator is not encoded for remote execution; remote pipeline
conversion rejects it explicitly rather than dropping the marker.

## Executor state and lifecycle

`sortRollupState` owns the equality-key mapping, one-row last-key vectors, one
rollup sentinel per logical level, active group IDs, and bounded per-input-batch
group-ID scratch. `Group` owns aggregate state and output batches, and its
existing `free`/`Reset -> Prepare` lifecycle releases both the new vectors and
aggregate state.

The streaming implementation uses one single-group aggregate executor per
active ROLLUP prefix, one-row last-key vectors, and one bounded output batch.
At a key boundary it flushes the finished prefixes, emits their grouping
sentinels, and recreates those single-group executors. Its retained aggregate
state is therefore proportional to the number of levels, not the total number
of input groups. Complete key-runs use the aggregate API's single-group
`BulkFill` after chunk-level capacity preflight, avoiding repeated group-id
dispatch for a state that can only contain group 1. `agg_spill_mem` is resolved
to the effective executor threshold before setup and is enforced against that
bounded state; unordered
input sorting remains responsible for its own workspace/spill limit.
The capacity check includes retained aggregate/key vectors, last-key vectors,
and per-input-batch group-id scratch; initialization, empty-input
finalization, and every materialization boundary are checked. Exceeding it
returns an OOM error and `Free` releases the partially built state. This is an
explicit experimental boundary, not a claim of streaming spill support.
Cancellation is checked before input and before output. Partial prepare or
fill failure is terminal and `Free` owns cleanup. `EvalReset` is rejected for
this fully-draining operator because resetting generic group state without
resetting prefix IDs would be unsafe.

## Alternatives

1. Keep the current hash/grouping-set rewrite. It is semantically mature and
   can execute branches concurrently, but it repeats scans and hash work.
2. Use the implemented streaming rollup aggregate with recyclable per-prefix
   aggregate states. It gives bounded state with the existing aggregate API;
   an internal Sort is added only when no compatible input order is available.
   The cost model selects it only when the estimated work has a safety margin;
   `SET SESSION rollup_algorithm = 'SORT'` remains available for controlled
   experiments.

## Compatibility, rollout, and observability

No catalog, storage, or plan wire schema changes are made. The plan marker is
internal and the remote pipeline rejects sort ROLLUP rather than attempting a
mixed-version transfer. Rollback is setting `rollup_algorithm = 'HASH'`, which
selects the legacy hash path even when statistics would otherwise choose sort.
Existing grouping bitmap,
NULLability, cancellation, and aggregate diagnostics remain the public
contracts. Operator memory is reported through the existing group analyzer.

## Acceptance and validation

- plan test proves `SORT -> marked AGG`, nullable grouping outputs, and no
  generated `UNION ALL` for the eligible form;
- executor tests cover two input batches, empty input, source NULL versus
  rollup NULL, grouping bitmap, aggregate values, and cleanup;
- planner tests prove repeated/direct-ineligible and inline-window forms do not
  take the sort path, derived/CTE sources fall back, and outer filters stay
  above the aggregate;
- executor tests cover count-based and byte-based capacity limits, automatic
  capacity, initialization failure followed by `Reset -> Prepare`, and
  cleanup;
- compiler tests prove one finalizing coordinator scope retains every ordered
  input stream and explicit remote rejection;
- cost-model tests prove hash selection for the measured high-cardinality
  counterexample, sort selection for a small/selective input, per-level
  aggregate charging, unknown-statistics fallback, and aggregate-capacity
  rejection;
- SQL BVT proves `GROUPING()` and empty-input behavior through the real service;
- benchmark uses identical deterministic data, repeated measured iterations,
  the same plan shape, and the same output-draining behavior for hash and sort,
  recording elapsed time and peak MPool usage. A cost selector is accepted only
  after comparing NDV, key width, rollup depth, skew, ordering reuse, and
  spill/capacity behavior.
