- Status: proposed
- Design revision: v1 (2026-09-01)
- Authors: MatrixOne optimizer team
- Implementation PRs: [#27914](https://github.com/matrixorigin/matrixone/pull/27914), [#27915](https://github.com/matrixorigin/matrixone/pull/27915)
- Issue for this RFC: [#26768](https://github.com/matrixorigin/matrixone/issues/26768)

# Stats-Independent Analytic Plan Rewrites

## Summary

This RFC defines the legality, ordering, resource, compatibility, and
validation contracts for two related optimizer change sets:

1. sharing repeated analytic computation: multi-reference CTEs, grouping-set
   inputs, and partial `SUM` below unique dimension joins; and
2. exposing plan properties hidden by SQL syntax: existential and scalar
   subqueries, common DNF keys, outer/anti joins, and shuffle lineage.

The implementation may use statistics to choose among plans already proved
equivalent. Statistics, benchmark query identity, table names, scale factors,
and constants are never correctness evidence.

This revision is not approved until the design PR has an approving review.
Implementation approval is blocked until then.

## Motivation

TPC-DS 1 TiB exposed repeated fact-table work, late filters, unnecessary
outer-join barriers, underestimated ANTI output, and lost distribution. The
same shapes occur in ordinary analytic SQL. Fixing individual query numbers
would hide the underlying defects and make the optimizer brittle; the goal is
to admit only transformations that follow from SQL semantics and explicit
schema facts.

## Scope and non-goals

In scope:

- materializing a deterministic multi-reference CTE once;
- sharing the detailed input of planner-generated grouping sets;
- pushing distributive partial sums through non-multiplying dimension joins;
- positive existential MARK-to-SEMI transformations;
- early placement of an uncorrelated scalar-filter pair;
- extracting a common typed join equality from every DNF arm;
- guarded LEFT-join association and LEFT-to-ANTI conversion;
- conservative ANTI cardinality and exact shuffle-lineage reuse.

Not in scope:

- a general memo or cost-based rewrite search;
- runtime adaptive plans;
- making inaccurate statistics a semantic proof;
- sharing recursive, correlated, volatile, or partially consumed producers;
- query-, schema-, table-, benchmark-, or scale-specific branches.

## First-principles invariants

Every rule must preserve all of the following. Failure to prove any item keeps
the pre-existing plan.

1. **Bag semantics:** rows, duplicates, NULLs, and aggregate values are
   unchanged.
2. **Three-valued logic:** NULL extension, marker truth tables, and residual
   predicates are preserved.
3. **Evaluation domain:** a rewrite must not evaluate volatile or potentially
   failing expressions on additional rows or columns.
4. **Correlation and scalar cardinality:** correlated inputs do not cross a
   scope boundary, and a scalar subquery retains its `SINGLE` cardinality
   check.
5. **Lineage:** every moved or reused expression traces to exact binding tags
   and column positions through supported projection shapes.
6. **Uniqueness:** row-preservation or non-multiplication claims require a
   complete declared primary-key equality; estimates never substitute for it.
7. **Consumption:** a materialized producer is shared only when every reader
   drains it, or when the producer is the bounded statement-local
   `mo_current_roles` closure.
8. **Determinism:** recursive, external, side-effecting, volatile, and unknown
   operators remain on the old path.
9. **Bounded resources:** planner work, plan growth, materialized memory, spill,
   and executor ownership remain bounded as specified below.
10. **Fail closed:** missing metadata, unsupported type/operator shape,
    ambiguous ownership, an invalid estimate, or a failed proof returns the
    unchanged plan.

## Rule architecture and ordering

The optimizer does not run these rules to a generic fixpoint. Each rule is a
single deterministic tree/graph pass at a named boundary. A later rule may see
the output of an earlier rule, but must re-prove its own preconditions instead
of treating the earlier rewrite as evidence.

### Bind-to-logical boundary

After binding a SELECT and before ordinary `createQuery` optimization:

1. record all non-recursive CTE occurrences;
2. admit and build a shared CTE producer;
3. rewrite only reachable consumers to `SINK_SCAN`;
4. rewrite compatible planner-generated grouping-set branches to share their
   input.

CTE sharing precedes grouping-set sharing because a grouping branch may contain
a nested CTE source. Both passes leave their historical inline/branch plans in
place when rejected.

### Logical rewrite boundary

For each query step, the relevant order is:

1. normalize/push filters and expose positive existential predicates;
2. remove a provably effectless aggregate;
3. move an eligible uncorrelated `FILTER + SINGLE` pair;
4. convert a guarded LEFT/NULL-filter idiom to ANTI;
5. recalculate statistics;
6. perform join ordering and remove redundant join conditions;
7. apply guarded join association;
8. choose build/probe sides;
9. pull aggregates and recalculate statistics;
10. push eligible partial sums through unique dimensions;
11. push SEMI/ANTI joins and optimize DISTINCT aggregation;
12. recalculate statistics and finalize build/probe sides.

The scalar and LEFT-to-ANTI rules run before the first full costing boundary so
join enumeration sees the exposed selective shape. Partial aggregation runs
after join association, where fact/dimension sides and uniqueness proofs are
stable.

### Physical distribution boundary

After logical rewrites stabilize, the planner swaps legacy join children,
recalculates physical stats, determines hash-on-PK state, and chooses shuffle.
Shuffle-lineage reuse is allowed only here, against the final remapped join
keys. No full `ReCalcNodeStats` pass is allowed after physical shuffle metadata
is fixed.

## Detailed contracts

### Multi-reference CTE reuse

An eligible CTE has at least two reachable, type-compatible occurrences, a
deterministic non-correlated non-recursive producer, and complete consumer
drain. The ordinary in-memory admission is 32 MiB. A predicate-aware or proven
hash-build source may use the existing spill owner, with an 8 GiB planner
ceiling; statement/CN accounting remains authoritative.

Consumer predicates remain in place. If every consumer constrains a common
producer column with deterministic total predicates, their remapped
disjunction may also bound the producer. Output expressions are shared only
when their full row-and-column evaluation domain is already required or they
are structurally total. A `LIMIT`, `OFFSET`, Top-N projection boundary,
fallible cast/function, incomplete predicate copy, or consumer join that can
reduce the evaluation domain rejects sharing.

A consumer admitted as the equality-SEMI hash input receives a physical
hash-build marker. Later costing must preserve that build role; otherwise the
complete-drain proof is invalid.

### Grouping-set input sharing

Only the internal `UNION ALL` created by one `ROLLUP`, `CUBE`, or `GROUPING
SETS` binding is eligible. User-written `UNION ALL` is excluded. Branches must
have identical typed group expressions and aggregate state shapes,
deterministic expressions, complete consumption, and positive cost.

The selected form evaluates input expressions once, emits one derived batch
per grouping set, uses NULL vectors for inactive keys, and adds a hidden set id
to keep equal values from different sets distinct. It retains at most one
input batch plus the current derived batch.

The vector grouping representation is gated by MORPC version 42. Version 41 is
already assigned to current-role closure support on `main`; peers below 42 get
the historical branch-per-grouping-set plan. The protobuf fields are append-only.

### Partial SUM through unique dimensions

Only distributive supported `SUM` states may move. Every crossed join must be
an inner equality join whose dimension keys cover a declared primary key. The
partial group retains original fact grouping columns and every fact join key;
join and final aggregate references are remapped to that output. Grouping
sets, unsupported states/types, incomplete lineage/uniqueness, and a
non-reducing estimate keep the original join-then-aggregate plan.

### Existential MARK and OR-of-EXISTS

A marker may become hashable/SEMI only when it is consumed solely as a positive
Boolean filter. `NOT`, `IN`, `ANY`, projected markers, mixed marker
expressions, non-equality correlation, and volatile build subtrees are rejected.

Compatible OR arms may share one `UNION ALL` build only when every arm has the
same deterministic typed outer equality keys and no marker escapes. Duplicates
in the union are harmless because SEMI observes existence. The original
residual meaning is preserved.

### Uncorrelated scalar filters

The `FILTER + SINGLE` pair moves as one unit to the smallest input supplying
all non-scalar dependencies. The scalar input remains `SINGLE`. The filter and
scalar subtree must be deterministic, uncorrelated, total under truncation,
and free of limit/barrier boundaries. The rule crosses INNER joins and only the
preserved left input of SEMI/ANTI joins.

### Common DNF equality

For `(K AND A) OR (K AND B)`, `K` may be copied to the join condition only when
every disjunct contains the same typed bare-column equality and lineage places
its operands on opposite inputs. The original DNF remains as a residual.
Computed keys, partial arm coverage, incompatible types/casts, volatile
expressions, and ambiguous lineage are rejected. The walk does not distribute
expressions or enumerate a Cartesian expansion of terms.

### LEFT and ANTI transformations

- A join may move below the preserved side of a LEFT join only when equality
  plus a complete declared key proves it neither removes nor multiplies rows.
- `LEFT JOIN ... WHERE nullable_side_not_null_key IS NULL` may become ANTI only
  when the marker traces through pure projections to a declared NOT NULL scan
  column on the nullable side.
- `(A LEFT JOIN B) INNER JOIN C` may associate only for a null-rejecting bare
  column equality from the nullable side.

Both moved predicates must be deterministic and structurally total. RIGHT/FULL
joins, computed/nullable markers, partial keys, non-equality predicates, and
unknown lineage stay unchanged.

ANTI output uses a conservative default fraction when overlap is unknown. A
lower bound involving right-row count is allowed only for a complete PK
equality proving that each right row excludes at most one left key. This is a
cost estimate only.

### Shuffle lineage

An exact left distribution may survive a left-preserving join only when the
next join uses the same bare left key. The proof follows explicit column
lineage after final remapping. Right/build lineage, changed or computed keys,
ambiguous projections, and RIGHT/FULL joins force the existing reshuffle.

## Resource and failure model

- Planner graph walks keep visited state and are linear in reachable plan nodes
  plus inspected expression nodes. DNF extraction visits the existing tree and
  never expands it distributively.
- OR-of-EXISTS plan growth is linear in the number of admitted branches and
  equality keys. Rejected rules add no reachable nodes.
- CTE materialization uses the existing materialized-source memory account,
  spill files, FD accounting, cancellation, reset, and cleanup paths.
- Grouping expansion owns only vectors for the retained input batch and current
  grouping set. Projection, Group, and MergeGroup release them on reset, free,
  and error.
- No rule adds a goroutine, channel, lock, RPC wait, or persistent format.
- Allocation, expression, codec, child, and cancellation errors propagate
  through existing owners; no fallback converts an execution error into a
  different result.

## Compile-time and plan-quality budgets

The design is accepted only when measured on the complete TPC-H and TPC-DS
fixture suites:

- planner wall time p50 must not regress by more than 5%, and p95 by more than
  10%, against the same build and fixture statistics;
- no rejected/control query may gain reachable scans, joins, or materialized
  producers;
- no accepted CTE may exceed the 32 MiB resident or 8 GiB spill-planner bound;
- grouping-set sharing must reduce repeated detailed inputs and must not create
  more aggregate states than the legacy branches;
- partial aggregation must reduce estimated fact rows before dimension joins;
- outer/existential rewrites must not increase fact-scan count;
- shuffle reuse must preserve the exact key and must not increase planned
  repartitions outside its admitted shape.

TPC-DS 1 TiB runtime is supporting performance evidence, not a correctness
oracle. A faster target query does not offset a semantic failure or an
unexplained control-plan regression.

## Validation matrix

| Rule | White-box/typed proof | Black-box acceptance | Mandatory unchanged controls |
|---|---|---|---|
| CTE reuse | reachability, drain, type, determinism, row-domain, memory/spill and build-role tests | public SQL duplicate/NULL/result checks; spill/reset/error paths | recursive/correlated/volatile/fallible/early-stop/unreachable/incompatible producers |
| grouping sets | internal-origin marker, typed branch compatibility, MORPC 41/42 plan boundary, codec round trips | distributed ROLLUP/CUBE/GROUPING SETS results with NULLs and spill | user UNION ALL, incompatible state/type, old protocol, partial consumer |
| partial SUM | complete PK, key remap, supported-state and reduction tests | public SQL results with duplicate dimensions, NULLs and empty input | non-unique/partial key, outer/non-equi join, unsupported aggregate/type |
| MARK/OR EXISTS | positive marker ownership and typed equality plan tests | EXISTS results with duplicates, NULLs, multiple arms | NOT/IN/ANY/projected/mixed/volatile/non-equality/correlated markers |
| scalar filter | pair placement, dependency, totality and retained SINGLE tests | scalar 0/1/>1-row behavior and result checks | correlated/volatile/fallible/barrier/limit/incomplete dependency |
| DNF key | exact common-key and residual retention tests | DNF result checks with NULLs and duplicates | missing-arm key, computed/incompatible/volatile/ambiguous key |
| LEFT/ANTI | null-rejection, pure marker lineage, complete uniqueness and rule-order tests | public outer/anti result checks with duplicates and NULLs | nullable/computed marker, partial PK, non-total predicate, RIGHT/FULL/non-equi join |
| ANTI estimate | bounded estimate and complete-key tests | plan-only cost comparison; SQL result unchanged | missing/partial/computed/nullable/non-equality key |
| shuffle lineage | exact post-remap key and preserved-side tests | multi-owner plan plus exact result | build/right lineage, key change/expression, ambiguous projection, RIGHT/FULL join |

Relevant planner, compiler, executor, and public issue packages must pass unit
tests and `go vet`; repository SCA and distributed BVT must pass on the exact
rebased implementation heads.

## Alternatives

### Query-specific rules

Rejected. They do not generalize and conceal missing semantic proofs.

### Trust estimates to prove legality

Rejected. Stats may be stale or internally inconsistent; they can rank only
already legal alternatives.

### General memo/fixpoint optimizer now

Deferred. It would improve global search, but is a much larger architecture
change. These rules define local equivalence contracts that remain useful in a
future memo.

### Runtime adaptive materialization and join choice

Deferred. It needs observation, topology-switch, ownership, and rollback
protocols. The current proposal uses existing bounded spill and deterministic
fallbacks.

### Split every rule into a separate PR

Deferred for this series. It would produce many cross-dependent PRs and force
reviewers to reconstruct ordering and interaction across them. The selected
alternative is one approved versioned design plus two integration PRs:
#27914 for shared-computation execution/planning and #27915 for logical
join/subquery robustness. Within each PR, mechanisms remain isolated in named
helpers and typed positive/negative test closures, so review and targeted
rollback do not depend on query-specific switches. If the integration review
cannot establish one mechanism independently, that mechanism must be split
before approval.

## Rollback

Every rule retains the old plan as its fail-closed path and is isolated in a
named implementation/test closure. A correctness or plan-quality regression is
handled by a targeted revert of that closure, not by adding query/table
exceptions. Grouping-set execution has an additional deterministic runtime
fallback: protocol versions below 42 always receive the legacy plan. Reverting
one rule must not require reverting unrelated stats or executor memory-bound
work.

## Approval record

Approval is the GitHub approving review on the exact commit of the design PR.
After any semantic design amendment, increment the revision, obtain approval on
the new exact head, and update both implementation PR bodies to link that
revision before requesting implementation re-review.
