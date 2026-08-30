# Proof-gated grouped aggregate elimination for bounded queries

- Status: implementation complete; current-tree planner UT validated
- Tracking issue: [matrixorigin/matrixone#27730](https://github.com/matrixorigin/matrixone/issues/27730)
- Parent performance issue: [matrixorigin/matrixone#27685](https://github.com/matrixorigin/matrixone/issues/27685)
- Implementation PR: [matrixorigin/matrixone#27850](https://github.com/matrixorigin/matrixone/pull/27850)
- Last updated: 2026-08-30

## 1. Decision

Optimize grouped Top-K only when a logical proof removes work before execution.
The first implementation recognizes a direct table scan grouped by its complete
declared primary key. Every surviving row is then exactly one group, so supported
aggregates can be rewritten as row expressions and the aggregate operator can be
deleted. Existing LIMIT pushdown can subsequently stop the scan early.

Two execution-time callback designs were evaluated and rejected:

1. The original generic Group-to-Top callback for COUNT/SUM/MIN/MAX still scanned,
   hashed and aggregated all rows, retained all groups until EOF and made Top
   compare every group. It regressed a 10M integer COUNT case by 14.6%.
2. A narrower chunk finalizer for wide unordered GROUP_CONCAT removed only about
   2% of clean-process peak RSS and about 1% of wall time in the activating case.
   That did not justify a new cross-operator ownership protocol.

This revision therefore contains no Group/Top callback, aggregate finalization
interface or execution hot-path change. Queries that do not satisfy the planner
proof use the pre-change physical path.

## 2. First-principles invariant

For an exact query, an input row or group may be discarded only when a proof shows
that doing so cannot change a returned value or selected row. LIMIT is bounded
demand, not such a proof by itself.

For arbitrary unordered input and `GROUP BY key ORDER BY COUNT(*) LIMIT K`, no
group can generally be evicted before EOF: unseen rows may still increase its
count. Approximate heavy hitters and distribution assumptions are outside scope.

The implemented proof is relational uniqueness:

```text
direct scan + complete declared primary key in GROUP BY
    => at most one input row per group
    => aggregate(single row) = proven row expression
    => hash/group operator is redundant
```

## 3. LIMIT opportunity map

| Capability | Can reduce base scan? | Required proof | This revision |
| --- | --- | --- | --- |
| plain LIMIT | yes | no semantic barrier | existing machinery |
| ordered index/storage Top-K | yes | exact order and filter coverage | existing machinery |
| dynamic Top bound/zone maps | sometimes | monotone comparator bound | existing machinery |
| unique grouping-key elimination | yes | complete declared PK | implemented |
| ordered streaming group prefix | yes | preserved key order and compatible ordering | future |
| metadata aggregate pruning | sometimes | exact object bounds | future |
| distributed local Top-K | not necessarily | exact local/global Top-K law | separate work |
| result chunk finalization | no | material copying only | rejected |

This distinction is important: deleting aggregation can reduce hash work even
when an ORDER BY still requires a full scan, while a plain bounded unordered query
can reduce both aggregation and scan work.

## 4. Rewrite eligibility

The aggregate-bearing extension accepts only all of the following:

- a bounded LIMIT demand reaches the aggregate through unary Project, Filter or
  Sort nodes;
- demand never crosses Join, Union, shared CTE or another multi-input boundary;
- the AGG has exactly one direct TABLE_SCAN child;
- the scan exposes one relation tag and a real declared primary key (not the
  synthetic/fake key);
- every declared primary-key component occurs as a direct grouping column from
  that scan;
- no embedded aggregate projection/filter, inactive grouping-set key, DISTINCT
  aggregate or aggregate-specific configuration exists;
- every grouping expression and aggregate argument whose evaluation can move
  past bounded demand is truncation-safe: a direct column/literal or a
  structurally proven total, side-effect-free cast whose target range covers the
  complete source domain;
- every semantic row predicate already owned by the direct scan is
  truncation-safe. The proof accepts typed values, total casts, boolean
  connectors, comparisons, BETWEEN/IN and NULL/boolean tests; an arbitrary
  deterministic scalar is not assumed total;
- every Filter predicate on the bounded-demand path to Aggregate, including
  HAVING before the first filter-pushdown pass, satisfies the same proof;
- every aggregate belongs to the proven single-row family below.

An aggregate-bearing query without LIMIT deliberately keeps its established plan,
even though the uniqueness law would make elimination correct. This rollout rule
limits plan-shape changes to the workload that motivated the optimization and
makes the no-regression boundary explicit.

The older no-AggList effectless-aggregate rewrite remains available independently;
the LIMIT requirement applies only to the new aggregate-bearing extension.

## 5. Single-row aggregate laws

For a group containing one row:

- `COUNT(*)` is INT64 1;
- `COUNT(expr)` is `IF(expr IS NULL, 0, 1)`; a direct proven non-null column may
  use constant 1;
- `MIN`, `MAX` and `ANY_VALUE` are the argument cast to the declared aggregate
  return type, preserving NULL;
- `SUM` and `AVG` use that replacement only when it is exact over the complete
  input type domain. Floating-point SUM/AVG retain Aggregate because their
  arithmetic state canonicalizes signed zero, while a direct cast does not;
- decimal AVG is eliminated only when the source decimal domain fits completely
  in the declared result precision and scale. Wider source domains retain
  Aggregate, preserving the established result/error behavior independently of
  fixes to the aggregate implementation;
- deterministic metadata is not a totality proof. Text-to-number casts,
  narrowing casts, arbitrary scalar functions and volatile expressions retain
  Aggregate because replacing a blocking consumer with a bounded streaming path
  could otherwise suppress an error or externally visible evaluation;
- unsupported or mixed aggregate families reject the complete rewrite.

The rewrite converts AGG to Project, appends the row expressions after the group
columns, and remaps the aggregate output binding tag to those new project
positions. It clears aggregate-only metadata, then lets existing projection
pruning and LIMIT pushdown operate normally.

For `SELECT pk, COUNT(*) FROM t GROUP BY pk LIMIT 10`, work changes from scanning
N rows and building N hash groups to scanning/projecting only the bounded demand
plus the storage reader's batch/block granularity. With OFFSET, the existing
source demand is `K + offset` as required by the scan contract.

LIMIT pushdown is deliberately repeatable because projection pruning can expose
the direct Project-to-Scan edge only after the first pass. If that scan already
owns an inner `(limit, offset)` window, the outer window is composed instead of
overwriting it: offsets add, the inner remaining cardinality is reduced by the
outer offset, and the resulting limit is the minimum remaining bound. Literal
overflow and any dynamic expression that would require runtime arithmetic fail
closed by retaining the two pagination owners. The layers are also retained when
a nonzero outer OFFSET exhausts a nonempty inner LIMIT: replacing that path with
`LIMIT 0` would skip filters that the inner window previously had to evaluate.
A scan with no existing window can still take dynamic LIMIT/OFFSET expressions
directly.

For `ORDER BY COUNT(*)`, aggregate elimination is still valid, but the current
revision retains Sort and scans all rows. Removing an all-tie order is a separate
future rule: it must prove every order expression constant and preserve LIMIT,
OFFSET and any rank/tie semantics.

## 6. Semantic barriers and failure closure

- A WHERE predicate remains on the scan before uniqueness reasoning; a subset of
  a primary key is still unique.
- HAVING remains semantically before bounded demand. After aggregate elimination,
  filter optimization may keep it above the row projection or push the rewritten
  predicate into the scan; in the latter plan the scan evaluates its filter before
  its LIMIT, so rejected rows still cannot consume the result demand.
- Any inactive GroupingFlag rejects the branch. The all-active sibling of a
  ROLLUP/CUBE/GROUPING SETS expansion is protected because LIMIT demand never
  crosses the enclosing Union.
- A Join child is not a direct scan and cannot inherit outer bounded demand.
- Correlated scalar aggregates retain their established decorrelation shape.
- Missing, incomplete or malformed PK metadata, or a non-total single-row
  aggregate conversion, fails closed.
- Truncation safety covers aggregate arguments, extra grouping expressions,
  HAVING/other Filter predicates on the demand path, and every scan `FilterList`
  predicate. This prevents LIMIT/OFFSET pushdown from skipping errors or
  externally visible evaluations, including the first-pass HAVING path before
  filter pushdown. `BlockFilterList` holds derived pruning copies; the semantic
  predicate remains in `FilterList`, so it is not a second unproved evaluation
  owner.
- Binding remapping is performed only after every aggregate expression in the
  candidate has been proven, preventing partial rewrites.

No new runtime ownership, cancellation, spill, memory-accounting or distributed
wire behavior exists in this design.

## 7. Validation evidence

### 7.1 Target-host A/B

Exact base `a15139da62` and the redesigned binary were run against the same data
on the NVMe mount of `10.222.1.55`. Each timed series used warm-up runs and new
client connections consistently. Representative medians:

| Shape, 10M rows | Base | Redesigned | Effect |
| --- | ---: | ---: | ---: |
| PK group, unordered LIMIT 10 | 146.4 ms | 9.7 ms | **15.1x faster** |
| PK group, LIMIT 10 OFFSET 100000 | 149.3 ms | 10.4 ms | **14.4x faster** |
| PK group, ORDER BY COUNT LIMIT 10 | 160.1 ms | 24.8 ms | **6.5x faster** |
| no-PK integer COUNT Top-K control | 249.0 ms | 249.4 ms | +0.2% |
| Q35-like VARCHAR COUNT Top-K control | 496.0 ms | 497.7 ms | +0.3% |
| below-threshold GROUP_CONCAT control | 122.5 ms | 123-125 ms | within noise |

EXPLAIN for the first case contains Project directly over Table Scan with
`Limit: 10`; AGG is absent. On the 10M-row target table the storage reader admitted
8,193 input rows (one source batch plus the boundary row) and emitted 10, instead
of feeding all 10M rows into aggregation. EXPLAIN for the ordered case contains
Sort over the rewritten Project/Scan, proving that its gain is hash elimination
rather than a claim of reduced scan.

### 7.2 Rejected GROUP_CONCAT experiment

With 10,000 groups, a raised `group_concat_max_len` and about 77 MiB of aggregate
output, the chunk-finalization prototype measured about 185 ms versus 188 ms.
Clean service restarts showed peak-RSS increases of about 548 MiB versus 559 MiB.
The result was far below the required 2x memory improvement, so all prototype
execution changes were removed.

### 7.3 Repository validation map

The public BVT was run twice for the aggregate-elimination revision before the
final scan-predicate and nested-pagination hardening. Those runs remain feature
and fixture evidence, but they are not exact-head evidence for the final planner
code. Current-tree validation covers the focused planner matrix and the complete
`pkg/sql/plan` package; public BVT must be rerun when the delivery gate
requires service-level evidence.

- focused planner tests: PK elimination, aggregate family, nullable COUNT, HAVING,
  missing PK, DISTINCT/configured aggregate, grouping family, unbounded fallback,
  decimal domain containment, floating-point signed-zero fallback, fallible and
  volatile expression plus WHERE/HAVING predicate fallback, safe
  comparison/boolean/range predicate controls, and a total widening-cast
  control;
- exhaustive small-window LIMIT/OFFSET composition against sequential slice
  semantics, plus public nested-query plans, dynamic-expression fallback,
  overflow fallback and repeated-pass idempotence;
- pre-existing grouping-set, correlated-scalar-aggregate and physical-group-key
  regressions;
- full `pkg/sql/plan` package suite;
- a public SQL BVT covering NULL, signed/unsigned/decimal casts, WHERE, HAVING,
  OFFSET, unsupported aggregate fallback, wide-decimal AVG and floating-point
  signed-zero HAVING behavior, run twice on one service;
- build of the complete `mo-service` binary;
- final diff inspection proving no colexec change remains relative to the base.

## 8. Rollout and future work

Future additions must provide a separate proof and performance gate. Promising
directions are ordered streaming grouping, exact constant-order removal after the
unique rewrite, and storage metadata bounds. None should reuse a generic callback
whose only evidence is LIMIT.

Acceptance for a new shape requires:

1. exact result equivalence, including NULL, collation, ties and OFFSET;
2. a measured resource removed before adding a protocol;
3. a large-benefit case on realistic cardinality;
4. controls proving non-eligible plans retain their prior path within 3%;
5. complete fallback, lifecycle and distributed-boundary closure proportional to
   the layer being changed.

## 9. Decision log

| Decision | Reason |
| --- | --- |
| Reject generic cheap-aggregate callback | It removed no scan/hash work and regressed COUNT by 14.6%. |
| Reject GROUP_CONCAT chunk finalizer | About 2% RSS reduction did not justify cross-operator complexity. |
| Treat LIMIT as demand, not proof | Prevents invalid early group eviction. |
| Start with declared PK uniqueness | Exact, stable and capable of O(N) to O(K). |
| Require LIMIT for aggregate-bearing rollout | Bounds plan-shape impact and preserves unlimited controls. |
| Stop demand at relational boundaries | Protects joins, CTEs and grouping-set families. |
| Prove Filter-path and scan predicates before removing Aggregate | Closes both pre-pushdown HAVING and post-pushdown WHERE paths against hidden late errors or volatile evaluations. |
| Compose existing pagination or retain both owners | Makes the required second pushdown pass semantic and idempotent for nested queries. |
| Keep non-eligible execution byte-for-byte | Makes the no-regression guarantee structural, not heuristic. |
