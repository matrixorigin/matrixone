# Exact affine SUM-family reduction

- Status: design reviewed; implementation validated
- Tracking issue: [matrixorigin/matrixone#27682](https://github.com/matrixorigin/matrixone/issues/27682)
- Implementation PR: [matrixorigin/matrixone#27977](https://github.com/matrixorigin/matrixone/pull/27977)
- Last updated: 2026-09-02

## 1. Decision

For one query block, reduce a family of at least three aggregates of the form

```sql
SUM(x + c)
```

when `x` is a deterministic exact-integer expression whose complete declared
input domain can be proven by the planner, and the family contains any two
consecutive integer constants `c0` and `c0 + 1`. Keep those two original
aggregates as physical anchors and replace every other family result with

```text
S(c) = S(c0) + (c - c0) * (S(c0 + 1) - S(c0))
```

If several adjacent pairs exist, choose the lowest pair whose largest absolute
derived coefficient remains inside the proven DECIMAL(38,0) product bound.
This preserves the stable choice for ordinary families while admitting the
complete safe family when a lower pair is too far from an extreme shift but a
more central pair works.

The initial range prover covers signed and unsigned 8/16/32-bit columns,
widening casts to BIGINT, integer literals, unary sign, and checked integer
`+`, `-`, and `*` compositions. It is deliberately extensible through one
range-proof boundary rather than through workload- or SQL-text exceptions.
All guards are structural and type-based. Failure to prove any guard retains
the original aggregates. This phase changes only query-local logical plan
expressions; it adds no executor, state, protocol, configuration, or persistent
format.

ClickBench Q30 is the motivating scale case: it requests 89 shifted sums over
the same `SMALLINT UNSIGNED` column. The rule itself has no workload, table,
column, SQL-text, or constant-count special case; it applies to every expression
that satisfies the same proof. On current `main`, Q30's aggregate work grows
nearly linearly with the number of expressions even though every result belongs
to one affine family.

## 2. First-principles invariant

For every non-NULL input value `x` and integer constant `c`:

```text
(x + (c0 + 1)) - (x + c0) = 1
SUM(x + (c0 + 1)) - SUM(x + c0) = COUNT_NONNULL(x)
SUM(x + c) = SUM(x + c0) + (c - c0) * COUNT_NONNULL(x)
```

The proof is valid only when every operation is exact and every removed
per-row expression is total over the complete declared input domain. The rule
therefore requires:

- ordinary, non-DISTINCT `SUM` with one argument and no aggregate config;
- canonical registered overloads for every matched aggregate, shift, cast, and
  exact-integer arithmetic expression;
- a deterministic exact-integer base accepted by the recursive interval prover;
- immutable source-free, non-prepared literal signed-64 constants, including
  negative shifts;
- every base-plus-constant endpoint fits signed 64-bit;
- the absolute value of every shifted-input endpoint is at most
  `floor((10^38 - 1) / (2^64 - 1))`, proving that a DECIMAL(38,0) SUM cannot
  overflow even at the maximum representable input-row cardinality;
- at least three structurally identical bases and any consecutive anchors;
- every rebound derived expression has exactly the original public type.

The full-cardinality absolute-value bound proves that every eligible original
SUM is total. It separately proves that every signed anchor delta times the
maximum row cardinality fits DECIMAL(38,0); the anchor difference is exactly the
non-NULL row count, and the final addition equals an already-proven original
SUM. Thus signed values, negative constants, and an anchor above the minimum
family constant do not rely on cancellation or error-order assumptions. The
two physical anchors are members of the original aggregate set, so the rewrite
cannot introduce an anchor failure that the original query did not have.

NULL and empty-input behavior is preserved without a special case. Both
anchors ignore exactly the same NULL rows. On empty or all-NULL input they are
NULL, and ordinary scalar arithmetic propagates NULL. Duplicate aggregate ASTs
remain governed by the binder's existing aggregate de-duplication.

## 3. Eligibility barriers

The following retain the baseline plan:

- 64-bit full-domain integers, `BIGINT UNSIGNED`, decimal, float, string,
  temporal, enum, internal, or future types when their complete range cannot be
  proved inside the DECIMAL(38,0) bound;
- fractional, dynamic/prepared, variable, or non-literal shifts;
- no consecutive constant pair or fewer than three family members;
- division, modulo, bitwise, conditional, volatile/erroring, implicit
  narrowing, or any expression outside the exact-integer interval grammar;
- `DISTINCT`, aggregate configuration, multiple arguments, filters or ordered
  aggregate forms;
- a shifted-input bound that could overflow DECIMAL(38,0) at a `uint64` row
  cardinality, even when its per-row addition fits signed 64-bit;
- malformed/unregistered function IDs, unexpected overloads, or a derived
  type different in ID, width, scale, charset, or nullability.

Prepared specialization may temporarily represent a runtime parameter as a
literal whose `Literal.Src` retains the parameter provenance for later plan
reuse. Prepared-numeric metadata can likewise mark a literal, an aggregate, or
any nested base node whose value, type, or overload will be rebound. Every such
node remains a barrier: the rewrite never bakes one execution's runtime value
or overload into the reusable expression layout.

Implicit and user-explicit integer casts use different registered overload IDs.
Both are eligible only for a statically proven exact widening to BIGINT; syntax
provenance does not change the values or error behavior in that case. Comparison
and set-operation cast overloads remain barriers. Compound bases such as
`smallint_col * 2` are eligible only when all four interval products fit signed
64-bit over the column's complete declared domain.

A direct `SUM(narrow_column)` is not normalized into the shifted family. Its
aggregate overload and public result may be `INT64` or `UINT64`, while
`SUM(narrow_column + c)` is widened to `DECIMAL128(38,0)`. Retaining it as the
separate third Q30 aggregate preserves its distinct overflow domain, partial
state, and result metadata; numerical equivalence alone is insufficient to
cross that boundary.

## 4. Plan transformation and ownership

The rewrite runs after HAVING, SELECT, window/time-window, ORDER BY, and LIMIT
binding, but before aggregate-argument subquery flattening and AGG construction.
At that point every expression container holding the query block's aggregate
tag is known. Shape-sensitive planner consumers still run afterward and are
part of the transformation contract: in particular, deep scalar-correlation
flattening proves NULL-on-empty behavior from the rewritten projection tree.
The registered STRICT `+`, `-`, and `*` operators introduced by this rule retain
that proof; NULL-observing consumers such as `COALESCE` and `CASE`, plus every
unaudited function, remain conservative barriers.

The phase first builds the complete candidate result without mutating the bind
context. It then atomically installs a compact aggregate list and rewrites all
aggregate-tag references in projections, HAVING, ORDER BY, window/time-window,
and fill expressions. A correlated, invalid, malformed, or not-yet-understood
expression container that could hide an aggregate reference aborts the entire
rewrite and leaves the original context untouched. Known scalar leaves remain
accepted explicitly, so a future protobuf expression variant cannot silently
bypass this boundary.
The normal final column-remapping pass remains the authority for physical slot
pruning and local positions.

The output headings, aliases, result order, grouping keys, HAVING placement,
window placement, DISTINCT, pagination, and query-cache behavior are unchanged.
No new memory owner exists: two established SUM states replace three or more
states, and derived scalar expressions live in existing consumer operators.

## 5. Alternatives

1. **Status quo.** Correct, but evaluates and maintains every aggregate
   independently. It does not meet the target cost.
2. **Executor-level shared argument evaluation or a fused multi-output SUM.**
   Broader and useful for unrelated aggregate families, but it changes group,
   merge, spill, serialization, reset/reuse, and memory-accounting paths. It is
   disproportionate for an algebraically reducible family.
3. **One shared `SUM(base)` plus `COUNT(base)`.** It uses only two states and is
   algebraically simple, but adds a COUNT result-width assumption absent from
   the original query. Keeping two original adjacent sums derives the count in
   the aggregate's exact decimal domain and preserves original anchor behavior.
4. **Two adjacent original SUM anchors.** Selected. It reuses established
   aggregate semantics, has a local rollback boundary, and makes all omitted
   results exact scalar derivations, including values below the selected
   anchors through a negative multiplier.

## 6. Cost model and acceptance

For a family of `A` aggregates over `N` rows, the baseline performs `A` argument
evaluations and aggregate fills per row. The selected plan performs two of each
plus `O(A)` scalar operations per output group. For scalar aggregation and small
group cardinality, expected aggregate work improves toward `A/2`; for very high
group cardinality the scalar projection cost can dominate, so the initial rule
still requires at least three family members and validation includes grouped
controls.

Current-main evidence on one isolated 16-core CN with 10,000,000 persisted NVMe
rows:

| Shifted family width | Baseline | Two-anchor equivalent |
|---:|---:|---:|
| 2 | 46 ms | 45 ms |
| 3 | 69 ms | 44 ms |
| 4 | 80 ms | 46 ms |
| 8 | 152 ms | 49 ms |
| 32 | 564 ms | 47 ms |
| 89 | 1,549 ms | 50 ms |

For the complete Q30-shaped 90-column projection (one unshifted sum plus 89
shifted sums), current main takes 1.55--1.63 seconds with 90 physical SUMs. The
implemented rule takes 52--55 ms with three physical SUMs, a 28--31x wall-time
improvement. Their one-row output SHA-256 values are identical. The main update
rebased during validation changes only Iceberg lifecycle code, so the planner
and aggregate baseline remains semantically fresh.

One million rows with one million groups challenge the opposite cost shape,
where the derived scalar expressions run once per input group:

| Family width | Baseline | Implemented rule |
|---:|---:|---:|
| 2 (non-eligible control) | 31 ms | 30 ms |
| 3 | 34--36 ms | 32--33 ms |
| 8 | 53--56 ms | 49--51 ms |
| 32 | 150--165 ms | 132--141 ms |

Acceptance is exact result and metadata equality, a physical aggregate count of
three for the Q30 shape, no activation for every barrier above, and no material
regression for grouped or width-two controls. The measurements above satisfy
those gates on the rebased implementation.

## 7. Validation contract

Deterministic planner tests prove:

- Q30-shaped activation and exact physical aggregate count;
- aggregate references in SELECT, HAVING, ORDER BY aliases/direct expressions,
  windows, and grouped queries remain valid;
- NULL, all-NULL, and empty input preserve results and result metadata;
- signed and unsigned 8/16/32-bit sources, negative shifts, unshifted members,
  non-minimum adjacent anchors, and checked compound integer bases activate;
- every excluded type, dynamic/fractional shift, family without adjacent
  constants, DISTINCT, configured/malformed aggregate, unsafe constant,
  different base, and width-two family retains the original aggregate list;
- expression traversal is fail-closed for unknown and malformed containers and
  the context is not partially mutated;
- prepared literals and prepared-numeric metadata at every aggregate/base level
  remain barriers across repeated executions with different parameter values;
- ROLLUP branches, window consumers, and time-window/sliding/FILL consumers
  preserve exact values and physical aggregate references.
- deep-correlated scalar consumers remain decorrelatable after a removed SUM is
  represented by strict affine arithmetic, while NULL-observing and unaudited
  consumers remain barriers.

Distributed SQL compares fixed expected results and metadata across empty,
NULL, grouped, reordered, and boundary-valued inputs. Real-server A/B evidence
uses the same data, CN topology, cache state, query text, and repeated fresh
clients for widths 1/2/3/4/8/32/90. Scale validation includes enough rows to
make aggregate evaluation dominate client overhead. Counterexamples explicitly
challenge per-row addition overflow, aggregate overflow, NULL placement,
constant order, aliases, HAVING, grouping, and prepared parameters.

## 8. Compatibility and rollback

There is no wire, storage, catalog, configuration, upgrade, mixed-version,
security, tenant-isolation, or recovery change. The coordinator serializes only
ordinary SUM and scalar arithmetic already understood by existing CNs. Removing
the single planner call restores the baseline aggregate list. A malformed or
unsupported shape always falls back before mutation.

## 9. Design review record

```text
Change scope: query-local exact affine SUM-family logical-plan reduction
Trigger: materially affects the aggregate hot path
Design: this document, revision on codex/issue-27682-affine-sum
Blocking findings: closed prepared-plan value capture and future/malformed expression traversal gaps; preserved direct narrow-SUM overflow domain
Decision log: recursive exact-integer interval proof; lowest safe consecutive anchors; signed delta bound; >=3 members; fail closed; no executor changes
Decision: PASS
Implementation deviations: none
```
