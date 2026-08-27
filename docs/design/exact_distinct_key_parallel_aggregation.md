# Exact DISTINCT-Key Parallel Aggregation

Status: approved MVP design (2026-08-27)

Owner issue: [#27720](https://github.com/matrixorigin/matrixone/issues/27720)

Related bounded-memory work: [#27698](https://github.com/matrixorigin/matrixone/issues/27698)

Implementation series: `aunjgr:codex/distinct-key-path-b` followed by the
bounded DISTINCT-state spill work tracked in #27698.

## Problem and invariant

Exact `COUNT(DISTINCT d)` currently obtains distributed parallelism by assigning
complete final groups to owners. That topology is efficient when the final
`GROUP BY` key has enough balanced values, but a global aggregate or a query
with only a few final groups cannot expose enough owners even when `d` has very
high NDV.

The required invariant is:

```text
For every active final group g and DISTINCT value d, all equal (g, d) rows are
deduplicated by one owner before the final aggregate is computed.
```

Routing by `d` is a safe coarsening for the single-argument MVP: equal `(g, d)`
pairs necessarily have equal `d`, while hash collisions only co-locate unequal
pairs. The receiving Group remains the authoritative equality check.

Goals:

- expose DISTINCT-key parallelism for global and few-group high-NDV queries;
- preserve exact SQL results, output types, NULL behavior, and planner binding
  tags;
- retain the complete-group-owner path for sufficiently many final groups;
- fail closed during mixed-version operation;
- reuse existing shuffle, Group, MergeGroup, spill, and allocation ownership.

Non-goals for this MVP:

- composite or expression DISTINCT keys;
- multiple incompatible DISTINCT argument sets;
- runtime skew sampling or high-cardinality hot-group detection;
- local pre-deduplication before exchange;
- carrying a precomputed canonical hash between operators;
- a second DISTINCT spill implementation. #27698 owns fallback-state spill.

## Alternatives and decision

### Keep only final-group ownership

This is the path introduced by #27693. It avoids exact-state merging and remains
the preferred topology when an active final group key has at least the existing
64-owner threshold. It cannot parallelize a global or few-group aggregate.

### Shuffle aggregate partial-state blobs

Group's intermediate aggregate states are encoded in `Batch.ExtraBuf` for a
whole batch. The shuffle operator can split row vectors, but cannot split that
opaque state blob by selected rows. Making partial state row-addressable would
introduce a new batch and wire contract substantially larger than this change.

### DISTINCT-key decomposition (selected)

For the eligible shape, rewrite:

```text
input rows
  -> hash shuffle by d
  -> complete Group by (g..., d)
  -> local partial Group by g...
  -> MergeGroup by g...
```

The `(g, d)` stage emits at most one row per exact pair. The final stage converts
`COUNT(DISTINCT d)` to ordinary `COUNT(d)` and combines decomposable ordinary
aggregates. Existing Group spill can partition the synthesized pair table; the
fallback exact-DISTINCT state remains the responsibility of #27698.

The MVP recomputes hashes in Shuffle and Group. Adding transported hash metadata
would widen the pipeline contract and is deferred until profiling demonstrates
that its saved CPU outweighs the compatibility and maintenance cost.

## Planner contract

The mixed-aggregate rewrite is eligible only when all of these are true:

- every grouping flag is active;
- all DISTINCT aggregates are unary `COUNT(DISTINCT d)` over the same `d`;
- `d` is a direct column with an existing safe hash-shuffle type: signed or
  unsigned 16/32/64-bit integer, CHAR, VARCHAR, or TEXT;
- every ordinary aggregate is SUM, COUNT, COUNT(*), MIN, MAX, or AVG and has no
  ordered aggregate configuration;
- selected DISTINCT NDV is large enough to satisfy the existing exact-state
  shuffle cost test;
- the highest usable final-group-key NDV is below the existing 64-owner
  threshold;
- statistics are finite and the cluster compatibility gate is enabled.

Missing or unreliable statistics, grouping sets, tuples, multiple DISTINCT
sets, unsupported key types, and old protocol versions retain the current plan.
The established single-aggregate COUNT/SUM DISTINCT logical rewrite remains;
large supported COUNT DISTINCT additionally receives forced `d` ownership.

The synthesized pair aggregate is marked only inside `QueryBuilder`. Shuffle
planning consumes that marker and writes the ordinary `HashMapStats` decision;
no planner protobuf field is added.

## Aggregate and binding contract

The original outer aggregate keeps its binding tags, aggregate slot count,
slot order, and result types. Parent projections, HAVING, ORDER BY, aliases, and
subqueries therefore require no rebinding pass.

| Original aggregate | `(g, d)` helper | Final aggregate |
| --- | --- | --- |
| `COUNT(DISTINCT d)` | `d` group key | `COUNT(d)` |
| `SUM(x)` | `SUM(x)` | internal sum-combine |
| `COUNT(x)` / `COUNT(*)` | matching count | internal count-combine |
| `MIN(x)` / `MAX(x)` | matching aggregate | matching aggregate |
| `AVG(x)` | `SUM(x)`, `COUNT(x)` | internal weighted avg-combine |

Sum-combine returns the first argument type instead of applying SUM's widening
rules a second time. Count-combine returns zero for empty input. Avg-combine
uses `(partial sum, partial count, typed NULL result witness)` and the existing
AVG decimal finalization rules, including result scale.

The three functions are planner-only aggregate IDs and are absent from the SQL
name registry. Their state is fixed-width and owned by the existing `aggExec`:

- Group growth owns resident vectors;
- successful Flush transfers result-vector ownership to the caller;
- failed Flush leaves resident state owned by `aggExec.Free`;
- partial and spill serialization reuse the existing fixed-vector codecs;
- no goroutine, file, queue, lock, or new cleanup state is introduced.

For `d IS NULL`, the pair stage retains `(g, NULL)` so ordinary aggregates still
observe those rows; final `COUNT(d)` excludes the NULL pair. Empty global input
returns zero for counts and NULL for SUM/AVG/MIN/MAX.

## Compatibility and rollout

The aggregate IDs travel in existing remote pipeline fields, but older CNs do
not recognize them. MORPC version 33 is therefore the capability boundary:

- the planner does not select the mixed rewrite below version 33;
- remote pipeline encoding validates every internal combine aggregate and fails
  closed below version 33;
- version 32 remains assigned to cross-transaction plan-cache generation;
- rolling upgrade keeps the old topology until the deployment-wide minimum
  version reaches 33; rollback lowers the gate before older CNs participate.

There is no user setting or catalog/on-disk migration.

## Validation and acceptance

Deterministic unit coverage must prove:

- path selection for global/few groups and retention of Path A at 64+ owners;
- fallback for small NDV, missing/old protocol capability, grouping sets,
  unsupported keys, and distinct-set mismatch;
- unchanged outer bindings and exact helper/result types;
- SUM/COUNT/AVG NULL, empty, overflow, integer, float, Decimal128, and
  Decimal256 behavior;
- aggregate-state serialization plus a real Group-to-MergeGroup round trip;
- version 32 rejection and version 33 remote acceptance;
- allocation-account cleanup with zero residual debt.

No BVT is added for the optimizer selector because deterministic SQL fixtures do
not provide the required post-selection NDV statistics; a small SQL result test
would exercise the fallback and duplicate executor coverage without proving the
new topology. Performance validation belongs in the ClickBench/performance
harness: compare global/few/hot high-NDV cases with the current topology and
retain the balanced 64+ group control, reporting CPU, allocations, peak
accounted memory, shuffle rows/bytes, and wall time.

Acceptance for this MVP is exact result equivalence and real multi-owner plan
construction for eligible one/few-group inputs without regressing selection of
the balanced complete-group-owner path. Runtime skew adaptation, local
pre-deduplication, composite keys, and bounded fallback-state spill remain
explicit follow-up work rather than implicit claims of this PR.
