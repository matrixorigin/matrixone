# Bounded regular-index backfill

Owner: pkg/sql/plan, applyIndicesForProject.
Tracking: MatrixOne #32665, implementation PR #32675.
Design revision: 7; reviewed before the corresponding implementation.
Approval covers the planner design. Delivery still requires correctness and
performance acceptance, including the original unlimited useful-index case.

## Problem and invariant

A forced regular index can generate many keys for a small ORDER BY LIMIT.
Building a runtime filter and backfilling every key wastes work. The index can
preselect keys only when it has every qualifying predicate and the complete SQL
order. The final SQL Top and original base filters remain authoritative.

Invariant: every row eligible for the final LIMIT/OFFSET remains available.
The negation is a missing or wrongly ordered row after key preselection.

## Existing owners and admission

Use the existing regular-index pass, regularIndexColumnMap, exprStructuralEqual, predicate-stability
checks, buildCandidateLimit, isPositiveLiteralLimit, and ordinary Project/Top
operators. No reader, protocol, catalog, cache or worker-topology changes.

Admit only SELECT without FOR UPDATE, with:
- a direct SORT -> optional row-preserving PROJECT -> INDEX join;
- two direct scans, a complete regular nonunique index, and private base/build access;
- no intermediate JOIN filter, existing build/probe dependency or reader limit;
- a positive bounded literal LIMIT+OFFSET and no rank option;
- known base-scan cardinality estimating more than twice that candidate budget;
- stable base predicates whose decoded column references are fully mapped and fixed-size, and stable projection expressions;
- distinct direct fixed-size SQL order columns including every PK component;
- the established safe scalar ordering domain, excluding float/spatial keys.

Check incoming references from reachable query steps for the INDEX join and both scans. Ignore orphan nodes left by earlier rewrites. Pruning a shared base scan would also restrict another consumer through its runtime filter.

Unknown statistics, unmapped/wide predicate references, generic joins, shared access, truncated
required values, wide/repeated keys, volatile expressions and unsupported budgets
decline without mutation. Default statistics are placeholders, not evidence.
Admission is a cost preference; correctness never depends on an estimate.

## Rewrite and proof

1. Recheck the original stable predicates on decoded index values. Retain an existing equivalent recheck rather than evaluating it twice.
2. Project the stored PK and mapped order values. Reuse the stored PK slot when
   a scalar PK is also an order key; materialize each required value once.
3. Add the existing logical Top with K=LIMIT+OFFSET and no OFFSET.
4. Redirect only the INDEX join's right PK reference to that private projection.
5. Retain the original final SQL comparator, LIMIT, OFFSET and base filters.

Both scans retain the existing transactional snapshot and object references.
Exact SQL rechecks reject encoded-prefix false positives before preselection.
All PK components make the admitted logical SQL order total. The first K index
rows therefore contain every row required by the final SQL Top. If runtime-filter
admission returns PASS, the retained base filters and final Top still evaluate
the complete original query correctly. No filter cardinality limit is weakened.

The normal statistics, runtime-filter generation, remapping, projection removal
and compiler consume ordinary plan nodes. Reapplication declines because the
right child has become SORT.

## Resources, lifecycle and compatibility

The new Top retains at most K rows of fixed-size order values and stored PKs,
charged to the existing memory pool. Composite PK components must also be
fixed-size because every component occurs in the admitted order. Cost is
O(index candidates * log K), with O(K * retained key width) extra storage.
Backfill, key sorting and serialization can then handle at most K keys.

Normal Project/Top/IndexBuild owners handle empty input, cancellation, expression
or allocation failure, partial preparation, reset, reuse and Free. No new
goroutine, queue, retry, metric cardinality or accumulating state is added.
The wait graph remains index scan -> index Top -> filter build -> base scan ->
final Top. Existing local/remote node encoding and operator lifecycle apply.
No public API, persistent representation, tenant boundary or upgrade protocol
changes.

## Alternatives and cost evidence

Retaining all keys remains the fallback. Static reader limits cannot implement
the admitted mixed SQL ordering and complete predicate rechecks across generic
joins. New cache or pipeline scheduling mechanisms have larger ownership costs.

Paired prototypes improved OFFSET by about 59-62%, but also exposed 11-14%
point-lookup and 17-35% oversized-LIMIT regressions. This requires known-cardinality
admission and a majority estimated reduction before adding another Top.
Projection-slot reuse avoids duplicating the PK. These decisions bound complexity
and costs; they do not establish the overall PR's performance acceptance.

## Validation before delivery

Typed public planner cases cover useful ordering, OFFSET, mapped predicates,
and decline controls for residuals, missing PK, volatility, width, duplicates,
unknown/small cardinality and invalid budgets. Public SQL compares exact rows
against IGNORE INDEX with persisted data, NULL/ties, workspace changes, rollback,
and prepared reuse. Existing one-/two-CN fixtures validate the execution boundary.

Owning UT, relevant race/static checks and paired real-target latency/resource
measurements must pass. Include point and oversized-LIMIT controls, profitable
prefix cases, automatic-plan controls, and the original unlimited VARCHAR case.
Report pending or slower cases explicitly; no regression gate is waived here.
