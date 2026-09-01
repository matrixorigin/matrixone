- Status: draft
- Start Date: 2026-09-01
- Authors: aptend
- Implementation PR: #27915
- Issue for this RFC: #26768

# Safe Subquery Predicate Rewrites

# Summary

This RFC defines four fail-closed optimizer changes that expose useful
subquery predicates before join costing without depending on accurate table
statistics:

1. keep equality predicates on existential `MARK` joins hashable while
   totalizing the marker at its boolean consumer;
2. replace a filtering `OR` of compatible positive `EXISTS` branches with one
   `SEMI` join over their `UNION ALL` key inputs;
3. expose structurally common predicates from cross-relation DNF while leaving
   single-relation DNF available to range folding; and
4. accelerate an uncorrelated scalar equality with an exact runtime filter
   while leaving its `SINGLE` join, filter, and cardinality-error scope in their
   original logical position.

The rules are independent legality closures. They do not form a general
subquery optimizer, do not search alternative plans, and do not use query
names, benchmark IDs, row-count thresholds, NDV estimates, or cost estimates
to decide semantic eligibility. A rule that cannot prove every precondition
retains the existing plan.

This document covers the complete change in implementation PR #27915. The
outer/anti/shuffle work split into #27934 is not part of this design.

# Status and Review Order

The design gate is mandatory because the change affects the optimizer hot path,
changes more than 500 non-generated production lines, crosses planner, compile,
execution, and plan-wire boundaries, and can silently change SQL results if a
legality condition is wrong.

This RFC is intentionally `draft`. Review of #27915 is design-only until an
exact revision of this document is approved. After approval, the RFC will be
renamed with its approval date and advanced to `in-progress`; only then may the
implementation be reviewed against it. The existing prototype is evidence and
is not assumed to be the design answer.

# Motivation

TPC-DS exposes correlated and uncorrelated subquery shapes that the current
planner often represents with large `MARK` or late `SINGLE` builds. Predicates
that are already present in the SQL can remain hidden from hash-join selection,
scan filtering, or join costing:

- nullable equality under `EXISTS` is wrapped in `IS TRUE` on the `MARK` join,
  so the equality is no longer recognized as a hash key;
- a filtering `EXISTS(...) OR EXISTS(...)` is represented as multiple `MARK`
  joins whose build sides must be materialized before the final boolean filter;
- the single-table-DNF guard examines only selected argument positions and can
  mistake a cross-relation DNF for a single-relation range predicate, leaving a
  common join key hidden inside `OR`; and
- an uncorrelated scalar equality is evaluated above its `SINGLE` join, too late
  to reduce a large descendant scan.

The 1 TiB TPC-DS investigation in #26768 demonstrates the operational impact:
subquery-heavy plans can retain repeated large builds and scan billions of rows
before applying selective values. A prototype Q14b execution completed in
1757.689 seconds and read 25,923,890,232 rows; this is directional scale evidence,
not a controlled before/after performance result.

The scalar case also establishes the main correctness constraint. Moving a
`SINGLE + FILTER` pair below an `INNER`, `SEMI`, or `ANTI` join is not generally
legal. The original join can eliminate every outer row, in which case a
multi-row scalar subquery is never observed. Moving `SINGLE` below that join can
raise error 1242 where the original query returns no rows.

# Goals

- Preserve SQL result, NULL, volatility, expression-error, and scalar
  cardinality-error semantics for every admitted shape.
- Expose exact equality information early enough for existing hash joins,
  filter pushdown, runtime filtering, and costing to consume it.
- Make admission independent of statistics quality. Statistics may cost the
  resulting legal plan but must not prove legality.
- Fail closed in the planner and fail open for the optional scalar runtime
  filter.
- Add bounded planner work and bounded runtime state.
- Preserve mixed-version execution when a plan containing the new runtime
  filter annotation reaches an older CN.

# Non-goals

- General decorrelation, a memo optimizer, or cost-based enumeration of
  arbitrary subquery alternatives.
- Reordering outer joins or changing `SINGLE` cardinality-check placement.
- Rewriting `IN`, `ANY`, or `ALL`, whose three-valued result differs from
  `EXISTS`.
- Rewriting projected boolean disjunctions, negative existential disjunctions,
  non-equality correlations, or branches with different outer keys.
- Making runtime filters mandatory for correctness.
- Fixing base statistics, cardinality estimation, spill policy, or query-wide
  memory admission tracked by #26768.
- Benchmark-specific SQL matching or hints.

# Semantic Invariants

The design is governed by these invariants:

1. **Observational equivalence.** Enabling a rewrite may change the typed plan
   but not rows, result types, SQL errors, or externally observable evaluation
   scope.
2. **Three-valued separation.** `EXISTS`/`NOT EXISTS` produce total booleans;
   `IN`/`ANY`/`ALL` may need UNKNOWN. The rules must not share admission logic
   across those contracts.
3. **Scalar scope preservation.** `SINGLE` remains at its original logical
   position. More-than-one-row detection remains owned by that operator and is
   evaluated only for outer rows that reach it.
4. **Exact-key identity.** A factored or combined equality uses the same typed,
   structural outer expression and a type-compatible inner expression. Textual
   similarity, column names, NDV, and estimated uniqueness are not proof.
5. **Safe evaluation.** A rewrite does not newly evaluate volatile or fallible
   expressions, cross a `LIMIT`/`OFFSET`, cross a nullable join side, or bypass
   an aggregate, window, projection, or another `SINGLE` unless a specific rule
   proves equivalence.
6. **Optional-filter monotonicity.** A scalar runtime filter may only remove a
   row that the original scalar equality cannot accept. Missing, malformed,
   unsupported, or multi-row build state publishes `PASS`.
7. **No inferred facts from statistics.** Stale or contradictory statistics can
   affect cost but cannot make an otherwise illegal rule eligible.

The smallest negations are respectively a changed result/SQL error, UNKNOWN
being converted to a total boolean in an `IN` expression, error 1242 moving
across an eliminating join, unequal outer keys being combined, a volatile or
fallible expression being evaluated on fewer rows, and an optional filter
dropping a possible match.

# Design

## 1. Hashable existential MARK predicates

Subquery flattening distinguishes existential markers from value-comparison
markers:

- For `EXISTS` and `NOT EXISTS`, retain a raw equality on `MARK` only when its
  operands and resolved comparison are total and side-effect free. Such an
  equality remains visible to existing hash-join lowering. Predicates without
  this proof retain the historical `IS TRUE` placement.
- Wrap the produced marker in `IS TRUE` at its boolean consumer. This converts
  a NULL marker to `FALSE`, which is the required existential result.
- Apply `NOT` only after totalization for `NOT EXISTS`.
- For `IN`, `ANY`, and `ALL`, retain the existing per-predicate `IS TRUE`
  handling because their three-valued semantics need the comparison result.

Filter pushdown recognizes both a direct marker and `IS TRUE(marker)` as a
positive filtering existential and converts it to `SEMI`. It recognizes only a
totalized negative marker as `ANTI`. A projected marker stays `MARK`; it is not
collapsed merely because a downstream expression happens to mention it.

The equality and all residual predicates remain on the join. Only the location
of existential totalization changes.

## 2. Filtering OR-of-EXISTS

A disjunction is rewritten only if all of the following are proved:

- the expression is a filter-context OR tree with at least two distinct
  positive `IS TRUE(marker)` leaves;
- those markers correspond exactly to a consecutive `MARK` prefix above one
  common outer input;
- every `MARK` has exactly one outer and one inner input and a marker binding;
- every join predicate is a typed equality with one expression from the common
  outer side and one expression from that branch's inner side;
- every equality and projected key expression is total and side-effect free,
  so moving key evaluation to a union-fed build cannot expose a new error when
  the common outer input is empty;
- each branch has the same number of keys, every outer key is structurally
  identical and unique within the branch, and corresponding inner key types
  are equal;
- neither the keys nor any expression in an inner subtree contains a volatile
  function.

For each branch the planner projects its inner keys. It combines those
projections with `UNION ALL`, then creates one `SEMI` join between the original
outer input and the union output using the common typed equality keys.

`UNION ALL` is deliberate. Duplicate inner keys do not change `SEMI` semantics,
and adding a distinct set operation would introduce unnecessary work. The
existing hash build may deduplicate keys as part of its normal implementation.
NULL equality remains non-matching on both plans.

The rule rejects `NOT EXISTS`, `IN`/`ANY`/`ALL`, projected markers, non-equality
correlation, mixed outer keys, ambiguous or duplicate keys, incompatible inner
types, a non-consecutive marker shape, and volatile branches. Rejection leaves
the original filter and `MARK` chain untouched.

## 3. Cross-relation DNF factoring

The existing distributive transformation is retained:

```text
(X AND B) OR (X AND C)  =>  X AND (B OR C)
```

Structural hash only selects candidates; structural equality is the collision
resolver and identity proof. Every residual branch is retained. A common
condition is moved only when its resolved predicate and operands are total and
side-effect free; structural equality alone does not prove evaluation safety.

This RFC changes the single-table guard. Instead of inspecting only the first
argument of selected functions, the planner recursively collects relation tags
from columns and correlated columns in functions, lists, windows, subexpressions,
literal sources, and order expressions. If the complete relation set is known
and contains exactly one relation, the DNF is left intact for composite-key
range folding. If it contains multiple relations, common structural predicates
may be factored so an equality becomes visible as a join key. Unknown or invalid
relation references do not provide a single-table proof.

The transformation is an equivalence in SQL three-valued boolean algebra. The
totality guard also preserves MatrixOne's observable expression-error scope. It
does not discard residual predicates or manufacture an equality from statistics.

## 4. Scalar predicate runtime filter

The planner does not move `FILTER + SINGLE`. After column remapping and removal
of unnecessary projections, it performs one final plan traversal and considers
a runtime filter only for this shape:

```text
FILTER(outer_output = scalar_output)
  SINGLE(original outer subtree, uncorrelated scalar subtree)
```

Admission requires:

- a non-barrier filter with no `LIMIT`, `OFFSET`, rollup, or unsafe predicate;
- an uncorrelated, non-right `SINGLE` with no join condition, existing runtime
  filter, or shuffle build;
- a typed equality between one outer output and one scalar output;
- direct lineage from the outer output to a table-scan column;
- lineage crossing only either side of `INNER`, or the preserved left side of
  `SEMI`/`ANTI`;
- no aggregate, window, projection, outer join, nested `SINGLE`, limit, or
  offset on that path; and
- truncation-safe predicates on the filter, every crossed join, and the target
  scan.

The planner attaches an existing exact runtime-filter probe to the scan and an
exact build spec to `SINGLE`. The build uses `RAW_V1`, has upper limit one, and
sets the new `RuntimeFilterSpec.scalar_predicate` annotation. No statistics are
consulted.

The compile layer passes this spec to the broadcast hash-build operator used to
materialize the scalar side. The executor decides from actual materialized
cardinality:

| Build observation | Message | Reason |
|---|---|---|
| zero rows | `DROP` | an empty scalar subquery yields NULL; equality cannot be TRUE |
| exactly one NULL | `DROP` | equality with NULL cannot be TRUE |
| exactly one supported non-NULL value | one-value `IN` | exact necessary condition for the original equality |
| more than one row | `PASS` | original `SINGLE` must decide whether error 1242 is observable |
| malformed shape, unsupported encoding, or missing payload | `PASS` | optional optimization has no proof |

The original `SINGLE` and filter still execute. The runtime filter is only a
necessary-condition prefilter, not the owner of scalar semantics.

# End-to-end Ownership

| State or decision | First owner | Consumers | Bound/terminal behavior |
|---|---|---|---|
| existential versus value-comparison marker | subquery flattener | filter pushdown, join lowering | one annotation-free expression shape per marker |
| OR-of-EXISTS eligibility and new nodes | filter-pushdown rule | stats and physical join selection | nodes proportional to admitted branches and key count; rejection is no-op |
| DNF relation set and structural match | expression optimizer | filter pushdown and join costing | temporary state bounded by expression size |
| scalar build/probe spec and message tag | final planner pass | compile, hash build, scan | at most one scalar filter per eligible filter node |
| actual scalar cardinality decision | hash build | runtime-filter receiver and scan | one terminal `DROP`, `IN`, or `PASS` message |
| one-value payload memory | runtime-filter message | receiver | existing message release callback owns cleanup |

Cancellation, timeout, send failure, and downstream failure retain the existing
runtime-filter and operator lifecycle. The new path does not add a goroutine,
queue, retry, file, or persistent cache. Serialization failure follows the
existing optional-runtime-filter fallback when possible; query cancellation and
non-optional operator errors retain their existing error propagation.

# Rule Order and Fixpoint Behavior

The order is fixed:

1. flatten subqueries and establish `MARK`/`SINGLE` semantics;
2. normalize and push filters, including DNF factoring, filtering marker
   conversion, and the bounded OR-of-EXISTS rewrite;
3. run existing statistics, join-order, build/probe, index, and distribution
   passes on the resulting legal tree;
4. finish column remapping and projection removal; then attach scalar predicate
   runtime filters without moving nodes or recalculating statistics;
5. compile build/probe operators and choose the scalar message from actual build
   cardinality.

No generic fixpoint is introduced. OR-of-EXISTS repeatedly consumes independent
eligible filter conjuncts at the same boundary, but a produced `SEMI` join is
not treated as proof for another semantic rule. DNF factoring recurses over the
expression tree once through its existing normalization path. The final scalar
traversal only annotates an existing plan.

# Statistics and Costing

Statistics do not participate in the legality predicates above. This is
intentional: the motivating workloads include stale and mathematically invalid
NDV estimates.

After a legal logical rewrite, the existing optimizer may use statistics to
choose join order, build side, distribution, and access path. Bad statistics can
still produce a slow physical choice; they cannot make these rules change query
semantics. The scalar runtime filter uses actual build cardinality and exact
value encoding, so it remains safe when the scalar estimate is zero, one, or
arbitrarily wrong.

# Performance and Resource Model

- Existential marker normalization adds no plan traversal or retained state. It
  can replace a loop-capable marker predicate with an existing hashable equality.
- OR-of-EXISTS adds `O(branches * keys)` project expressions and `branches - 1`
  union nodes, replacing the same number of marker build relationships with one
  semi build. It reads the same logical inner inputs. The union-fed hash state is
  bounded by the combined branch key stream; it does not add an independent
  unbounded cache.
- DNF relation collection and structural comparison are bounded by expression
  size. Hash collision candidates are confirmed by structural equality.
- Scalar annotation performs one final plan traversal and adds at most one
  one-value payload per eligible filter. It reuses scalar batches already
  required by `SINGLE`; it adds no per-row planner work and no long-lived state.

Before implementation approval, a controlled planner benchmark must compare
the rewrite-disabled base and implementation on the full TPC-H and TPC-DS query
corpora. Median and p95 planning time and allocation bytes must each remain
within 5% of the base unless an outlier has an attributed, accepted tradeoff.
Plan node counts and maximum planning time must also be reported so the median
cannot hide expansion.

Runtime acceptance requires reporting per-query wall time, rows/bytes scanned,
peak query memory, spill bytes, and terminal result for every changed large
query. No query-specific improvement is a correctness gate, but a rule that
materially regresses the fixed TPC-H corpus or increases peak memory without an
accepted explanation is not ready.

# Compatibility, Rollout, and Rollback

The first three rules produce existing plan node and expression types and add no
catalog, storage, client, or configuration contract.

The scalar rule adds optional protobuf field
`RuntimeFilterSpec.scalar_predicate`. It has no persisted state:

- a new executor receiving an old plan sees the default `false` value and uses
  existing runtime-filter behavior;
- an old executor ignores the unknown field. A zero-row scalar build may still
  publish safe `DROP`; a non-empty loop-build lacks the ordinary unique-key
  payload and therefore fails open with `PASS`;
- serialization/deep-copy paths must preserve the field when both sides support
  it; and
- restart, backup/restore, and downgrade need no migration because plans and
  runtime-filter messages are ephemeral.

The new annotation changes no trust or tenant boundary. Runtime-filter tags and
payloads stay inside the existing query-scoped message path. The one-value
payload has a fixed cardinality bound, so it does not add a denial-of-service
amplifier.

No user-visible feature flag is proposed. Each rule has narrow, fail-closed
admission and the scalar executor fails open. Rollout uses normal CI plus the
fixed plan/performance corpora. Rollback is a code revert; old and new binaries
can coexist during that rollback because the only new wire field is optional.

Ordinary `EXPLAIN` exposes the admitted join and runtime-filter build/probe
shape. Existing statement and operator profiles provide rows/bytes scanned,
runtime-filter type, memory, spill, and terminal status. No per-row metric or
high-cardinality label is added. If corpus comparison shows an unexplained plan
or resource regression, rollout stops and the responsible rule is reverted.

# Alternatives

## Keep the current plans

This has the lowest implementation risk but leaves exact, already-present
predicates unavailable to existing physical planning and preserves the observed
large-build amplification.

## Tune statistics or add query-specific hints

Better statistics are necessary work under #26768 but do not expose a predicate
hidden by expression or subquery shape. Query IDs, SQL text, benchmark schema
names, and hand-tuned thresholds would overfit the incident and are rejected.

## Push SINGLE and its filter below joins

This applies the scalar predicate earlier with no execution annotation, but it
changes the scope of error 1242 when an intervening join eliminates every probe
row. Truncation-safe expression checks cannot prove away the cardinality error
owned by `SINGLE`. This alternative is rejected.

## Require an at-most-one-row proof

Admitting logical scalar pushdown only for an aggregate-without-grouping or a
trusted unique key can be safe, but it covers only a subset and couples the rule
to a growing uniqueness proof system. It may be added independently later. It
does not replace the actual-cardinality runtime filter for general scalar
subqueries.

## Use a generic bloom filter or ordinary runtime filter

A scalar equality needs at most one exact value. Bloom encoding adds false
positives and machinery with no benefit. Treating the scalar build as an
ordinary join-key build loses the critical `>1 => PASS` contract and can move
cardinality responsibility into optional-filter code. A distinct annotation on
the existing exact runtime-filter protocol is smaller and explicit.

## Introduce a memo optimizer or generic boolean-subquery framework

Those mechanisms could enumerate more alternatives but materially expand
ownership, cost, and testing scope. The four recurring contracts here are
independently recognizable and fail-closed; a framework is not justified by
this change.

# Validation Plan

Every semantic rule needs a typed plan oracle and, when SQL-visible, an
independent result/error oracle. Whole `EXPLAIN` snapshots are not correctness
oracles.

| Contract | Minimal witness | Nearby control | Typed oracle | Public or differential oracle |
|---|---|---|---|---|
| existential equality stays hashable | nullable correlated `EXISTS` equality | projected `IN`; fallible cast equality | only a total raw equality remains on `MARK`; totalization is at consumer | NULL/non-match/match results for `EXISTS`, `OR`, and `NOT EXISTS` |
| filtering OR becomes one semi build | two positive branches with one common outer key | different outer keys, `NOT EXISTS`, non-equality, projected boolean, volatile or fallible key | reachable `UNION ALL + SEMI`, no admitted `MARK` | result equals original OR-of-EXISTS reference |
| composite OR keys align | two branches with two common outer keys | duplicate/missing/incompatible key | ordered typed key mapping | result equivalence with NULL and duplicates |
| cross-table DNF exposes common key | common equality plus ternary residuals | same DNF over one table; fallible common predicate | total common structural predicate is a top-level conjunct; residual OR remains | optimized and rewrite-disabled forms return the same rows/errors |
| scalar filter preserves scope | multi-row scalar above eliminating `INNER`, `SEMI`, and `ANTI` | corresponding join lets one probe row survive; correlated scalar | `SINGLE` stays above join; probe reaches safe scan only for uncorrelated shape | eliminated case returns empty; surviving case raises error 1242 |
| scalar actual-cardinality state machine | zero, one NULL, one value, two rows | malformed build shape and unsupported encoding | exact `DROP`/`IN`/`PASS` message and one-value bound | query results/errors equal runtime-filter-disabled execution |
| scalar lineage is fail-closed | inner either side, semi/anti preserved side | outer/nested-single/project/window/unsafe predicate/limit | probe only on admitted table scan | no changed results or expression errors |
| wire compatibility | serialize and remote-run a marked spec | default/unmarked spec | marker survives deepcopy and remote serialization | mixed-version fallback is `PASS` for non-empty unsupported state |

The required implementation evidence is:

- focused planner tests for every positive and rejection cell above;
- owning-package tests for `pkg/sql/plan`, `pkg/sql/compile`, and
  `pkg/sql/colexec/hashbuild`;
- public embedded SQL tests for nullable existential results and scalar
  cardinality scope;
- protobuf regeneration check and remote operator round trip;
- scoped lint/static checks and a complete build;
- a controlled TPC-H/TPC-DS ordinary-`EXPLAIN` corpus comparison; and
- isolated scale evidence for affected TPC-DS queries, with merge/background
  work and service configuration recorded.

Performance data, generated plans, local fixtures, and benchmark result files
remain external evidence and are not committed with this RFC.

# Prototype Evidence Available for Design Review

The current prototype has deterministic unit and embedded-SQL coverage for the
core plan shapes, nullable existential results, scalar cardinality scope, and
the scalar cardinality state machine. On its rebased head, the four owning
package suites, scoped lint, protobuf generation, license check, and complete
build pass. The scalar wire marker is covered by deep-copy and remote-run round
trips. An isolated 1 GiB Q14b run completes in 2.90 seconds; the 1 TiB run cited
above completes without OOM under the test configuration.

These results show implementability. They do not replace the design decision,
the controlled compile-time comparison, mixed-version execution evidence, or a
complete before/after performance report. The prototype must be aligned with
this RFC after design approval by adding the explicit totality gates, correlated
scalar rejection proof, and public/differential OR-of-EXISTS and DNF controls;
those gaps keep implementation review blocked.

# Drawbacks and Risks

- Four closures in one PR increase review surface even though their legality is
  independent.
- OR-of-EXISTS can create a wider union-fed key stream; poor physical costing
  may still select an expensive build.
- A final whole-plan scalar annotation pass adds planning work to queries with
  no eligible scalar predicate.
- The scalar optimization relies on an optional plan-wire annotation and on the
  established runtime-filter delivery path.
- Conservative rejection leaves some valid opportunities unoptimized.
- Existing statistics and physical costing can still turn a legal rewrite into
  a plan with little or no performance benefit.

# Ready Gate

This RFC can advance to `in-progress` only when reviewers accept:

1. the four semantic invariants and fixed rule order;
2. the complete positive and rejection preconditions for each rule, including
   totality/fallibility and correlation;
3. the scalar `DROP`/`IN`/`PASS` state machine and original `SINGLE` ownership;
4. the optional protobuf compatibility and rollback contract;
5. the planner/runtime resource bounds and 5% planning-overhead budget; and
6. the validation matrix, including public result/error and controlled corpus
   evidence.

# Decision Log

- Keep the implementation series in #27915 but block implementation approval
  until this RFC revision is accepted.
- Treat legality as stats-independent; use statistics only after a legal tree
  exists.
- Keep `SINGLE` in place and use an exact, actual-cardinality runtime filter.
- Use `UNION ALL + SEMI` for admitted positive existential disjunctions.
- Preserve single-table DNF for existing range folding.
- Use one optional protobuf marker instead of inferring scalar semantics from
  hash-build flags.

# Unresolved Questions

There are no blocking design questions intentionally deferred to
implementation. Two non-blocking follow-ups remain owned by optimizer
maintainers and require separate evidence before expansion:

- whether an at-most-one-row proof should later permit logical scalar pushdown;
- whether additional boolean subquery shapes recur often enough to justify a
  common framework rather than more independent fail-closed rules.
