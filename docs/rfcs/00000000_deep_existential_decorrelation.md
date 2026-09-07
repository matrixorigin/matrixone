- Status: drafted
- Start Date: 2026-09-07
- Authors: MatrixOne planner contributors
- Implementation PR: not opened; design only
- Issue for this RFC: [#28293](https://github.com/matrixorigin/matrixone/issues/28293)
- Source baseline: `e7cadcf03150e9ca7a9bb3eeb789d0735295329a`
- Design review: pending independent review of this revision

# Deep existential decorrelation without domain products

## Summary

Support the two-level deep correlated predicates reported in #28293 using
existential normalization and SEMI/MARK joins. Preserve each outer input row
and avoid materializing pairs of inner matching rows. Keep the existing
successful subquery-flattening path unchanged.

This design does **not** introduce general domain decorrelation, a correlated
APPLY executor, shared domain producers, or a new physical join operator.
Those are separate capabilities. In particular, projected IN and NOT IN are
not silently included in the truth-only IN transformation below.

The change is a planner capability extension with a hot-path/performance
complexity trigger under the mo-dev design contract. Implementation follows
review of this versioned document. A design approval is not an assertion that
implementation or performance acceptance has passed.

## Motivation and evidence

The current inner-column guard rejects a condition such as `j.grp=o.grp`
because flattening the `j` subquery into a MARK join would hide the right-side
column needed by a predicate pulled another level upward.

Independent investigation established three different hazards:

| Candidate translation | Actual observation | Design consequence |
|---|---|---|
| Domain cross product, 2,000 distinct keys and 2,000 middle rows | 4,000,000 intermediate rows; about 584 ms vs 2.63 ms for executable reference | Do not introduce a domain product |
| Same domain, ordinary equality reattachment | About 491 ms despite removing LoopJoin | Hash reattachment does not fix pair expansion |
| Ordinary inner-join collapse, independent I/J each 100 rows with two join keys | 5,000 rows vs 100 with existential reordering | Preserve SEMI rather than enumerating witnesses |

The first semantic translation probe ran on MO `4fdb9e9161` and MySQL 9.6.0.
The performance and independent-review probes ran on development MO
`3d67664696`. These are algorithm witnesses, not an exact-revision release
benchmark. The original query currently rejects, so the performance reference
is executable equivalent SQL, not its error latency.

The chosen SEMI/existence-summary rules were separately checked on MO
`3d67664696` against MySQL 9.6.0 using independent I/J tables: six shapes
(AND, NOT EXISTS, deepest OR, NOT EXISTS over that OR, truth-only nested IN,
and direct-only correlation) over 25 fixtures, including both/one empty,
all-NULL, mismatched middle/inner payloads, duplicates and seeded small
relations. All 150 result comparisons passed. This validates the explicit
relational translations, not the unimplemented automatic rewrite. The
deterministic seed is 28293.

## Contract and scope

Let O be the existing outer input, I the middle relation and J the innermost
relation. Different aliases of one table remain different bindings; the
algorithm must also work when I and J are independent tables and keys are
nonunique or nullable.

Required invariants:

1. Outer row multiplicity is unchanged until the original boolean filter is
   applied. Duplicate outer values must not collapse or multiply.
2. EXISTS is two-valued. IN may be normalized to existence only where the
   consumer observes whether IN is TRUE, not whether it is FALSE or UNKNOWN.
3. Every original existentially quantified relation contributes its nonempty
   requirement, even in a disjunct that does not reference that relation.
4. A retained join predicate references columns supplied by that join. No
   pending subquery or correlation escapes into ordinary optimizer/remapping
   or distributed execution.
5. No new path is entered for a subquery that the old algorithm can flatten
   successfully. No NYI is caught after mutating the old plan.
6. Each witness-producing SEMI emits at most one row per probe row. There is
   no new O-by-I/J or I-by-J product and no full-scan MARK loop for admitted
   equijoin witnesses.

### Admission matrix

| Dimension | Admitted | Retain current behavior outside this scope |
|---|---|---|
| Depth | Inner references exactly two query blocks outward | Deeper chains |
| Inner query blocks | One base-table binding per block; FILTER and transparent PROJECT; local safe predicates | Joins inside I/J, derived tables, CTE/view boundaries, table functions |
| Inner subquery | Positive EXISTS; scalar IN used as a WHERE conjunct | Inner NOT EXISTS/NOT IN, ANY/ALL, row-valued IN |
| Outer consumer | EXISTS, NOT EXISTS or scalar IN as a conjunct of outer WHERE | Projected markers, outer OR/CASE/COALESCE, IS FALSE/UNKNOWN, HAVING or JOIN ON |
| Middle WHERE | Scalar conjunctions and one positive inner subquery conjunct | OR around the inner subquery, multiple nested subqueries |
| Deepest WHERE | A conjunction or a top-level OR of conjunctions of scalar predicates | Boolean expansion requiring distribution of AND over OR |
| Query-block barriers | None in I/J | AGG, DISTINCT, HAVING, WINDOW, SAMPLE, ORDER BY, LIMIT/OFFSET, locks |
| Cross-relation predicates after normalization | Ordinary equality of direct columns of the same admitted type | Non-equality, NULL-safe equality, implicit coercion, mixed-side expressions |
| Movable expressions | Proven total and side-effect-free using existing planner proof helpers | Volatile functions, unsafe casts, deterministic-but-fallible expressions |

Initially, substitution/equality classes admit boolean and same-width signed
or unsigned integer columns. Other primitive types may use existing simple
flattening, but are not added to this rewrite until substitution, hashing,
collation and special-value contracts are tested. NULL-bearing integer keys
are explicitly included. Bound NULL literals/local null tests are allowed.

Use an explicit planner-local consumer classification: `FilterTrue`,
`NegatedExistsFilter`, and `Ineligible`. An occurrence is `FilterTrue` only
when it is itself a bound WHERE conjunct (through transparent parentheses /
AND splitting). A syntactic NOT EXISTS or the equivalent direct NOT(EXISTS)
at that position is `NegatedExistsFilter`; no NOT is pushed through an IN or
an inner quantifier. Every other function path is ineligible. In particular,
the existing `nullResultRejected` boolean is **not** this classification:
rejecting NULL does not mean that FALSE is unobservable.

This matrix covers the reported EXISTS AND, direct-only reference, deepest OR,
outer NOT EXISTS, and the nested scalar IN witness specified below. It does
not claim arbitrary deep correlated IN compatibility. Any reported variant
that cannot satisfy this matrix remains outstanding; closing the issue must
use the exact agreed SQL inventory, not the feature name alone.

## Logical rules

### R1: truth-only scalar IN

For a WHERE occurrence, including a WHERE inside outer NOT EXISTS:

```text
T(x IN (SELECT y FROM S WHERE p))
    = EXISTS s in S: T(p(s)) AND T(x = y(s))
```

Here T means IS TRUE. An empty set, a NULL comparison or a FALSE comparison
cannot supply a witness. The rewrite preserves filtering, not the complete
three-valued IN result. It must not be applied through NOT, IS FALSE,
COALESCE or a projected IN consumer.

The typed comparison produced by the IN binder is retained. The admitted
same-type scalar case requires no reconstruction of casts or comparison
semantics. Truth-only equality predicates remain ordinary equality on SEMI
joins, whose matching rule is TRUE; do not wrap keys in a way that defeats
hash-key extraction.

The reported nested IN example normalizes from:

```sql
o.grp IN (
  SELECT i.grp FROM I i WHERE i.id IN (
    SELECT j.id FROM J j WHERE j.id=i.id AND j.grp=o.grp
  )
)
```

to an existential conjunction containing `o.grp=i.grp`, `i.id=j.id`,
`j.id=i.id` and `j.grp=o.grp`. Under TRUE matching the outer equality can be
represented by an anchor key and the remaining comparison `i.grp=j.grp`.
One legal plan is O SEMI (I SEMI J), using `(id,grp)` for the inner match and
`grp` for the outer match. Neither inner NULL nor outer NULL is a match.
There is no per-domain membership/NULL summary in this truth-only plan.

### R2: select an anchor and retain existence

After R1 and local-filter separation, consider each conjunctive arm:

```text
EXISTS i EXISTS j: Q(i,j) AND R(j,o)
    = EXISTS j: R(j,o) AND EXISTS i: Q(i,j)
```

J is the anchor that exposes every outer correlation key. Emit:

```text
O SEMI_R (J SEMI_Q I)
```

I is the symmetric alternative. Selection is structural: all outer references
must be representable at that anchor and every witness join must have at least
one ordinary equijoin key. This version chooses the original middle relation
when both anchors are legal; otherwise it chooses the sole legal anchor. It
does not assume that NDV/cost statistics are ready during binding. After
lowering, ordinary statistics and existing SEMI build/probe selection operate
on the emitted plan. No competing-plan cloning/search or INNER/product
fallback is added. The performance gates below can reject an implementation
whose stable choice is measurably worse than the equivalent SEMI reference.

For the original query the executable reference is:

```sql
SELECT o.id FROM O o
WHERE EXISTS (
  SELECT 1 FROM J j WHERE j.grp=o.grp
    AND EXISTS (SELECT 1 FROM I i WHERE i.id=j.id)
);
```

No uniqueness assumption on `id` is required. `J SEMI I` emits at most |J|
rows, however many matching I rows exist. Outer NOT EXISTS applies the
negation to this same two-valued existence result, permitting outer ANTI or
NOT(IS TRUE(marker)); it does not reverse an inner negative quantifier.

### Equality substitution obligations

Use bound `(binding tag, column position, type)` identities, not aliases or
SQL text. To replace an outer column by an anchor column, retain the original
ordinary equality between them on the outer/anchor match. Substitute only
within the same conjunction, never across OR arms. Retain all other original
constraints after substitution; do not erase nullable `x=x` without preserving
its NOT NULL requirement. No functional dependency is inferred merely because
I and J scan the same table or share a primary-key spelling.

Pure outer predicates are combined as T(predicate) with that arm's marker;
they are not moved to a global outer filter across another arm or a negation.
Local I/J predicates remain attached to their own input.

For a single positive arm, outer-local gates may filter O before its SEMI.
For a single NOT EXISTS arm, keep T(outer-local predicate) in the final ANTI
ON condition alongside an actual hash equality key; do **not** filter O first.
For example, with outer `flag=0`, NOT EXISTS with an inner `o.flag=1` predicate
must retain that outer row. For multiple arms, combine each gate outside that
arm's MARK as `T(gate) AND IS TRUE(marker)` before OR/negation. Do not put a
left-only residual into MARK and thereby defeat its pure hash-key contract.

### R3: top-level scalar OR and empty inputs

Distribute the two positive existential quantifiers over the already explicit
top-level disjunction, without converting arbitrary boolean trees to DNF:

```text
EXISTS i EXISTS j: (A(i,j,o) OR B(i,j,o))
    = E_A(o) OR E_B(o)
```

Each arm still quantifies both I and J. For the reported OR shape:

```text
EXISTS i EXISTS j: (j.id=i.id OR j.grp=o.grp)
 = EXISTS (I SEMI_id J)
   OR (EXISTS I AND EXISTS j: j.grp=o.grp)
```

The EXISTS I guard in the second arm is mandatory. Do not apply this identity
to a middle WHERE such as `i.k=1 OR EXISTS J`: its first arm never quantified J.

Each arm follows R2 where possible. A disconnected relation becomes one
statement-local boolean existence summary, computed by an existing no-group
aggregate `COUNT(*) > 0`; that aggregate has exactly one output row on empty
input too. It is computed once in the ordinary build scope, not once per O
row. Such single-row summary attachment is permitted; a product with an
unbounded relation is not.

Decorate the **single** O stream with two-valued MARK results and scalar
summaries, OR those booleans, then apply the original filter/NOT EXISTS.
Never UNION ALL copies of O, nor DISTINCT O to compensate. New witness arms
own separate scan nodes and binding tags; no node is shared across mutable
planner trees. Preserve each scan's snapshot, tenant/account and visibility
metadata using the existing scan cloning/rebinding machinery.

The first version admits at most eight explicit arms and copies each I/J
input at most once per arm. The check precedes cloning. This is a documented
planning-work cap, not a runtime knob. A too-large previously unsupported
region retains NYI. Identical arms may be structurally removed, but no
correctness or performance contract relies on a later optimizer doing so.

## Binding, pending state and commit boundary

Choose **read-only preclassification plus a planner-local pending region**.
Do not introduce a new protobuf operator and do not catch NYI to retry a
mutated tree.

1. At the start of `flattenSubquery`, before **any** type/depth fast return or
   `pullupCorrelatedPredicates`, look up the current subquery context's block
   identity in `pendingByOwner`. A hit dispatches directly to step 4: a
   depth-one outer subquery may own a pending depth-two descendant. Use this
   constant-time owner index, not a graph traversal or a global pending flag;
   an unrelated sibling must not enter region lowering.
   Only on a miss check subquery type, explicit consumer mode and query-block
   nesting depth. Keep a scalar nesting-depth field established at
   `baseBindSubquery` entry and inherited by same-block binding contexts; do
   not repeatedly walk parent chains. Ordinary depth-one queries return
   immediately without a new traversal or allocation. For possible two-level
   candidates, examine the bound subquery without mutation. Only defer when the admitted
   simple subtree demonstrably contains a predicate with an inner binding and
   a depth-two correlated reference that the old MARK guard would reject.
   Scalar aggregates and every old-success shape bypass this mechanism.
2. Register this occurrence in QueryBuilder-owned pending state and index it
   by its immediate consumer query block's identity, preserving
   its original typed Expr_Sub, original query-block identities and column
   references. Return the unchanged input node plus the original boolean
   Expr_Sub. Do not move filters, add projection keys or decrement depth.
3. Complete the middle block's ordinary binding. Pending references may exist
   only in that block's WHERE FILTER while its base scan/transparent projection
   are assembled. Any barrier or non-admitted consumer terminates with the
   established NYI; no generic optimizer is called on pending state.
4. At the immediate outer `flattenSubquery` entry, detect the pending child
   before old pull-up and analyze the complete two-level region and its actual
   consumer. Build an immutable descriptor: relation inputs, predicates,
   occurrence polarity, typed IN comparisons, owner bindings and arm list.
5. Validate the entire descriptor, choose anchors and count arms before
   generating nodes. Resolve correlated identities against the recorded owner
   contexts, not by applying `decreaseDepth` to a rearranged expression.
6. Construct new plan nodes and fresh expressions. Resolve all new column
   references to actual input bindings and rebuild required projections.
   Publish the replacement root only after successful construction and remove
   the consumed pending occurrences. Build errors/cancellation abort the
   statement and discard its QueryBuilder; they do not resume old flattening.
7. Before the first `createQuery` optimizer pass, validate that no reachable
   pending Expr_Sub or unresolved CorrColRef remains in any query step or
   expression-owned subquery graph. An unresolved region returns NYI; a broken
   internal reference returns an internal planning error before execution.

Allocate the pending registry lazily on the first actual deferral and set a
QueryBuilder-local `hadPendingRegions` flag. Step 7's graph validation runs
only when this flag was set; do not introduce a whole-query validation walk
for every existing successful query. Traversal of a candidate is linear in
its expression/input region, checks planning cancellation and allocates no
proof descriptor until a legacy-rejection witness has been found.

The pending registry is scoped to a QueryBuilder. Mint a block identity at
each SQL subquery bind entry and inherit it only into same-block binding
contexts; keep each pending occurrence plus its owning consumer block in
`pendingByOwner`. The parent subquery context is therefore identifiable before
its final project/root node is assigned. Do not key by SQL spelling, mutable
root node alone, or a reusable pointer alone. It is neither
cached nor serialized. Prepared execution must receive only the finalized
plan; a reprepare creates fresh binding/pending state. There are no background
workers, channels, shared producers or new runtime ownership transitions.

The read-only detector must be tested against the existing guard on admitted
simple shapes. A syntactic suspicion alone is insufficient to defer: changing
old-success behavior would violate the compatibility contract. A depth-one or
ordinary scalar subquery takes only the existing fast checks and allocations.

## Physical plan and resource constraints

- Retain SEMI/MARK output cardinalities; do not let later optimization convert
  witness elimination into unconstrained many-to-many INNER enumeration.
- Filter-only IN uses SEMI, so nullable composite equality keys retain ordinary
  TRUE matching without requiring nullable composite Hash MARK support.
- A single arm lowers directly to SEMI or ANTI at the final WHERE consumer.
  For multiple OR arms, existential MARK booleans must still lower efficiently:
  admit only a single equality key, or composite equality keys proven NOT NULL
  on both actual inputs. Reject a new nullable-composite MARK arm before
  generation; this version has no alternate nullable-MARK lowering. Do not
  introduce a full-scan LoopJoin fallback. The reported OR arms require only
  a single outer key and meet the existing hash-MARK contract.
- Do not globally redefine `IsEqualFunc` to include NULL-safe equality.
- O is scanned/consumed once. I/J work is bounded by the explicit arm count;
  there is no multiplying domain cardinality or shared CTE producer assumption.
- Hash build allocation, spill, cancellation and distributed delivery use
  existing operators and owners. Multi-CN validation must still check their
  actual selection, broadcast bytes, and termination for the new plan shapes.
- Snapshot, account and scan metadata must survive any per-arm cloning.
  `FOR UPDATE` and side-effecting relations are outside the new path.

## Change map

| Owner | Proposed change | Invariant/evidence |
|---|---|---|
| `pkg/sql/plan/flatten_subquery.go` | Pre-mutation detection and pending dispatch | Old-success plans stay on old path; guard differential tests |
| New `pkg/sql/plan/deep_existential.go` | Typed region analysis, truth rules, anchor/arm lowering | Logical counterexamples and typed graph assertions |
| QueryBuilder/BindContext definitions and `query_builder.go` | Scoped pending ownership and pre-optimizer finalization check | No pending/correlated references escape; cancellation/reprepare tests |
| Existing scan cloning and expression/project helpers | Reuse with full metadata, fresh tags | Independent tables, aliases, snapshots, tenant tests |
| Planner UT and distributed subquery BVT | Result, reject-control and plan-shape coverage | Exact matched cases and original issue inventory |

No colexec, protobuf, wire, catalog or on-disk format change is planned. If
implementation requires one to meet this design, review the expanded scope
before delivering it. There is no mixed-version format migration.

## Validation and performance acceptance

### Semantic and planner evidence

Use independent I/J fixtures with duplicates, NULL and empty inputs. Retain:

- All five issue witnesses: AND EXISTS, direct-only reference, deepest scalar
  OR, outer NOT EXISTS, and the specified nested IN.
- Same results under commuted equalities, independent aliases/tables and
  nonunique keys. Preserve duplicate O rows and unmatched O rows as appropriate.
- NULL and empty behavior before and after truth-only IN normalization,
  including inner WHERE IN under outer NOT EXISTS.
- Reject-controls for inner negation, middle OR, projected IN/NOT IN, aggregate
  and pagination barriers, coercion, volatile/fallible expressions and depth
  beyond two. Existing successful queries in these families remain successful
  via the old path.
- No mutation when preclassification says no; no pending state reaches
  optimize/remap/serialization. Positive deferral and parent-level failure,
  cancellation and prepared rebind each have explicit tests.
- Every newly emitted reference resolves after projection pruning. Each arm
  has fresh bindings; no cloned node retains a stale runtime-filter/join tag.

### Performance gates

The original error latency is not a performance oracle. Use the best proven
equivalent SEMI reference for each admitted shape, including the independent
duplicate-fanout witness, not a slower INNER reference.

1. Existing successful shallow and deep controls: unchanged operator class,
   scans and runtime-filter eligibility; no pending allocation on the ordinary
   shallow path. Measure planner latency as well as execution latency.
2. New equijoin witnesses: typed and physical plans have no full relation
   product or full-scan loop; inner SEMI output is bounded by its probe input,
   not the number of matches. Test 1x/4x input scaling and high/low NDV/skew.
3. Strong outer selectivity and duplicate-heavy inputs: compare input/output
   rows, scans and allocations with the SEMI reference. No O copy or domain
   scan is permitted. Do not claim bounded peak memory from cumulative output
   bytes.
4. On an isolated exact implementation revision, paired warm runs with rotated
   order must show no statistically supported >5% slowdown against unchanged
   old controls or the equivalent reference for cases lasting >=100 ms. For
   smaller cases, batch statements and evaluate per-statement planning and
   execution distributions; do not use a one-off millisecond ratio. If noisy,
   improve the measurement instead of declaring pass.
5. Peak allocation and spill volume must not grow due to witness multiplicity;
   accept only increases explained by the documented <=8 independent arms
   versus the corresponding explicit-arm reference. Compare broadcast/shuffle
   traffic and verify cancellation on one and multiple CNs.

A correct but slower candidate does not satisfy this RFC. An arm that cannot
meet physical constraints is not admitted by this first version. No arbitrary
query is promised a universal linear bound; the bound is for the declared
equijoin witness family and its corresponding reference plan.

## Alternatives and drawbacks

| Alternative | Decision |
|---|---|
| Remove NYI/check or expose arbitrary right payload from MARK | Violates column/cardinality semantics |
| Always collapse I/J into INNER | Demonstrated duplicate fanout |
| General domain decorrelation | Mature direction, but unnecessary for this truth-only scope; requires additional binding, sharing and NULL physical design |
| Per-row APPLY fallback | Current APPLY is not a generic subplan executor; would add lifecycle and repeated execution costs |
| Pre-binding SQL-text/alias rewrite | Does not have reliable binding/type/consumer information |
| Catch NYI then reuse old tree | Existing pull-up mutates before rejection |
| This scoped pending-region and SEMI rewrite | Chosen: typed semantics, no new execution operators, explicit scope/performance bounds |

The price is limited coverage and a small planner-local pending-state protocol.
OR arms may repeat I/J scans up to the documented bound. This proposal must
not be presented as general deep-correlation support. A later domain RFC can
extend the boundary without weakening these contracts.

## Review closure and unresolved questions

The initial review's duplicate-fanout issue is addressed by mandatory SEMI
plans; polarity/barrier issues by the admission matrix and T semantics;
mutation issues by pre-mutation detection and explicit pending ownership;
per-domain IN NULL issues by restricting normalization to truth-only IN.

No algorithm-choice placeholder remains in the admitted scope. Independent
review must verify these decisions before implementation. Implementation
proofs, exact-revision benchmarks and owner approval are still pending, and
are not represented as completed by this document.

## References

- [Unnesting Arbitrary Queries (2015)](https://15799.courses.cs.cmu.edu/spring2025/papers/11-unnesting/neumann-btw2015.pdf)
- [Improving Unnesting of Complex Queries (2025)](https://15799.courses.cs.cmu.edu/spring2025/papers/11-unnesting/neumann-btw2025.pdf)
- [Issue #28293](https://github.com/matrixorigin/matrixone/issues/28293)
- Local investigation artifacts: `/tmp/mo-28293-independent/`, including
  `perf/assessment.md`, `independent_design_review.md` and
  `review_duplicate_fanout.json`. The measurements above and the normative
  examples are reproduced here so the design does not depend on ephemeral
  artifacts remaining available.
