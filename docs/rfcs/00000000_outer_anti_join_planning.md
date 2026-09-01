- Status: draft
- Start Date: 2026-09-01
- Authors: aptend
- Implementation PR: https://github.com/matrixorigin/matrixone/pull/27934
- Issue for this RFC: https://github.com/matrixorigin/matrixone/issues/26768

# Fail-Closed Outer and Anti Join Planning

# Summary

This RFC defines five related planner properties: two guarded outer-join
associations, `LEFT JOIN ... IS NULL` conversion to ANTI, conservative ANTI
cardinality, and exact left-key shuffle reuse.  Each property is admitted only
from SQL semantics and declared metadata.  Statistics rank legal alternatives;
they never prove legality.

The implementation is planner-only.  It adds no protocol, catalog, storage, or
runtime-operator state.  This document is carried in the implementation PR, but
design review is a distinct first phase: implementation approval remains
blocked until this revision is approved and the RFC advances to `in-progress`.

# Motivation

TPC-DS exposed repeatable plan pathologies that are not benchmark-specific:

- an INNER join on a unique dimension remains above a large LEFT join even
  though it can first reduce the preserved input;
- an INNER equality on the nullable input remains above a LEFT join even though
  it rejects every NULL-extended row;
- the standard anti-join spelling remains a LEFT join plus filter;
- ANTI cardinality can collapse toward zero based on right-child filter
  selectivity, which is not key-overlap evidence; and
- a left-preserving join discards an exact left-key distribution and causes a
  later reshuffle or broadcast.

The same shapes occur in TPC-H and ordinary analytic SQL.  The design therefore
forbids query numbers, table names, scale factors, literal values, and sampled
cardinality thresholds in legality checks.

## Goals

1. Preserve bag semantics, SQL three-valued logic, expression evaluation/error
   domains, and local operator boundaries.
2. Make legality independent of estimated row counts and NDV accuracy.
3. Bound additional planning work and provide one operational rollback switch.
4. Prove public reachability and results with minimum deterministic data, then
   use TPC-DS only as plan-quality and runtime evidence.

## Non-goals

- general outer-join enumeration or a memo optimizer;
- inference of uniqueness, foreign-key completeness, or key overlap from data;
- computed-key distribution equivalence;
- runtime adaptive join switching; and
- changing persisted plans, wire formats, catalog metadata, or executors.

# Technical Design

## Invariants and first owner

`QueryBuilder` is the sole owner of these transformations.  A candidate is
either fully proved and committed to the in-memory plan, or rejected without
changing the plan.  No new state survives planning.

For every admitted rewrite:

- output rows and duplicate multiplicities are unchanged;
- NULL extension and UNKNOWN filtering are unchanged;
- volatile or fallible expressions are not evaluated on a different row set;
- LIMIT, OFFSET, ORDER BY, projection, aggregation, window, runtime-filter, and
  dedup-local semantics are not crossed;
- a declared key is used only after every key column is matched by a bare typed
  equality; and
- output distribution is reused only for the identical preserved-side bare
  column.

Unknown lineage, nullability, totality, uniqueness, or distribution fails
closed to the legacy plan.

## Rule 1: preserved-side association

The rule transforms:

```text
(A LEFT JOIN B ON p) INNER JOIN C ON q(A, C)
    ->
(A INNER JOIN C ON q) LEFT JOIN B ON p
```

It requires all of the following:

- the upper predicate references only `A` and `C`;
- `q` contains complete equalities to a declared primary key of `C`, proving
  that joining `C` cannot multiply an `A` row;
- predicates on both joins are deterministic and structurally total; and
- neither join node owns local semantics.

The upper INNER join may remove `A` rows but cannot multiply them, so moving it
before NULL extension preserves the LEFT-join result and may reduce its input.

## Rule 2: nullable-side association

The rule transforms:

```text
(A LEFT JOIN B ON p) INNER JOIN C ON q(B, C)
    ->
A INNER JOIN (B INNER JOIN C ON q) ON p
```

Every upper condition must reference only `B` and `C`, and at least one
ordinary equality must contain a bare `B` column.  That equality rejects the
NULL-extended `B` row.  Both join predicate lists must be deterministic and
structurally total, and neither node may own local semantics.  Predicates that
also reference `A`, computed null-rejection, RIGHT/FULL joins, and fallible
expressions retain the original plan.

## Rule 3: LEFT/NULL-filter to ANTI

The rule transforms:

```text
A LEFT JOIN B ON equi_match
WHERE B.marker IS NULL
    ->
A ANTI JOIN B ON equi_match
```

`marker` must trace through pure column projections to a declared NOT NULL
column of a `B` table scan.  No ancestor, sibling filter, projection, ordering,
aggregate, window, update expression, or other consumer may observe a `B`
binding.  The join must be an ordinary equi LEFT join, and its predicates and
marker must be deterministic.  Nullable markers, computed expressions,
disjunctions, right-preserving joins, and volatile or fallible expressions fail
closed.

## Rule 4: ANTI cardinality

Right-child selectivity is filtering internal to the right input; it does not
measure key overlap.  Without overlap statistics, estimated ANTI output is 50%
of the finite non-negative left estimate.

When the logical left child is a base scan and every column of its declared
primary key has a bare equality to the right subtree, each right input row can
eliminate at most one left key.  The estimator may then apply:

```text
max(0.5 * left_rows, left_rows - right_rows)
```

clamped to `[0, left_rows]`.  Partial composite keys, keys from the right side,
computed keys, and uniqueness merely propagated through joins or aggregates do
not qualify.

## Rule 5: shuffle lineage

An INNER, LEFT, SEMI, or ANTI join that shuffled on a logical left equality key
remains partitioned by that key.  A following join may reuse the distribution
only when it consumes the exact bare output column after remapping.  RIGHT/FULL
joins, right/build-side columns, computed projections, changed keys, invalid
shuffle indexes, and ambiguous lineage require normal distribution planning.

When several equality conditions are legal shuffle candidates, exact reuse is
preferred; otherwise the first eligible condition remains the stable
tie-breaker.

## Fixed composition order

The rules do not run to a fixpoint and no rule treats another rule output as a
proof.  The order is:

1. existing filter normalization and pushdown;
2. existing effectless-aggregate cleanup;
3. guarded LEFT/NULL-filter to ANTI conversion;
4. one recursive statistics recalculation;
5. existing join ordering entry;
6. preserved-side association;
7. nullable-side association;
8. existing inner-join associative rules and build/probe selection; and
9. physical shuffle candidate selection and exact lineage reuse.

Every rule revalidates its own tags, NULL, totality, key, and local-semantics
requirements at its point of use.

## Cost and resource bounds

The change allocates no runtime resource and creates no goroutine, queue, file,
or retained cache.  It adds bounded planner work only:

- one plan traversal for LEFT/ANTI recognition;
- one existing recursive statistics pass after a successful-phase boundary;
- two plan traversals at join-ordering time; and
- constant work per shuffle equality candidate, plus bounded subtree tag
  enumeration already used by the planner.

The acceptance budget is no more than 5% regression in median full planning
time for the same TPCH/TPC-DS fixture corpus and no individual control query
above 10% after repeated same-host measurement.  End-to-end acceptance requires
the TPCH performance gate to pass, the affected TPC-DS plans to change only by a
named rule, and target TPC-DS 1 TiB queries to complete without OOM or timeout.

## Observability, rollout, and rollback

The optimizer records named transformation history for outer association and
LEFT-to-ANTI conversion.  Plain `EXPLAIN` and the statement plan expose the
resulting join type/tree, estimated ANTI output, shuffle key, and reuse method;
validation classifies each changed plan by these structural properties.

The global optimizer hint `outerAntiPlanning=1` is the emergency rollback
boundary.  It restores legacy behavior for all five changes: no LEFT-to-ANTI
pass or extra stats pass, no new outer association, legacy ANTI estimation, and
legacy shuffle candidate ordering/lineage.  Default `0` enables the design.
Rollout is: deterministic UT/public SQL, fixture plan comparison, dedicated 129
TPC-DS target run, TPCH control gate, then normal CI.  A wrong result, unexplained
control-plan change, planning-budget breach, OOM, or timeout blocks rollout and
uses the global hint while the responsible rule is reverted.

## Compatibility and security

All changes are transient planner decisions over existing node types and
fields.  There is no protobuf, client/server, catalog, disk, backup/restore,
upgrade, mixed-version, authentication, authorization, or tenant-isolation
change.  Rolling upgrade therefore uses the existing plan-to-pipeline contract.
The rollback hint is parsed by the existing optimizer-hint mechanism; older
versions ignore its unknown key and retain their own legacy behavior.

## Validation matrix

| Property | Positive public/typed proof | Required counterexamples |
|---|---|---|
| LEFT to ANTI | matched, unmatched, duplicate-right, and NULL-left rows; real SQL plan contains ANTI | nullable/computed marker, projected right value, extra right predicate, OR marker, fallible expression |
| Preserved association | unique third input; real SQL places it below LEFT; literal and equivalent-query results | non-unique input, nullable-side reference, LIMIT/local semantics, fallible predicate |
| Nullable association | bare nullable-side equality; real SQL removes LEFT barrier; literal and equivalent-query results | preserved-side reference, mixed tags, no null-rejecting equality, LIMIT, fallible predicate |
| ANTI cardinality | complete left composite PK lower bound | partial PK, right-side-only PK, non-finite/negative estimates |
| Shuffle lineage | exact logical-left bare key before and after remap | FULL/RIGHT, build key, computed/changed key, invalid index |
| Composition | LEFT-to-ANTI followed by costing and shuffle; outer rules followed by build/probe selection | rollback hint restores all legacy decisions |

Public data is deliberately minimal: zero/one unmatched row, duplicate matches,
and NULL values.  Scale is not used as a correctness oracle.

# Drawbacks

- Planning does extra linear work even for queries that contain no eligible
  outer join.
- The 50% ANTI estimate is deliberately coarse until key-overlap statistics
  exist.
- One global rollback switch sacrifices all five improvements when only one is
  suspect; this is intentional to keep the operational contract small.
- Fail-closed guards miss legal transformations involving computed but provably
  total/null-rejecting expressions.

# Rationale / Alternatives

## Keep the current planner

This has zero planning overhead but retains avoidable large joins, false-small
ANTI estimates, and reshuffles.  It does not address the observed general
shapes.

## Add query/table-specific exceptions

This is smaller initially but has no semantic contract, overfits TPC-DS, and
cannot safely generalize to production SQL.  It is rejected.

## Introduce a memo optimizer or runtime adaptive joins

Both can explore more alternatives and tolerate uncertain estimates, but they
are substantially larger architectural changes.  They still require the same
bag/NULL/error-domain equivalence proofs.  This RFC keeps the proofs useful and
the implementation independently reversible while those designs remain future
work.

# Unresolved Questions

None.  Exact-head performance measurements are an implementation acceptance
artifact, not an unresolved semantic design decision.
