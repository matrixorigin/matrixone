# Bounded regular-index backfill

Owner: pkg/sql/plan, the existing regular-index rewrite.
Tracking: MatrixOne #32665, PR #32675.
Design revision: 11. Delivery requires correctness and path-controlled
performance acceptance, including the original useful-index case.

## Contract and existing responsibilities

A useful forced index must remain an index access. An unusable, unfiltered,
noncovering index does not justify an extra lookup. Explicit FOR JOIN hints keep
their existing priority and column-remapping owner.

An index may preselect backfill keys only when it can evaluate every qualifying
predicate and the complete SQL order. The final SQL Top, LIMIT/OFFSET and base
filters remain authoritative. No eligible final row may disappear.

Reuse regularIndexColumnMap, predicate-stability checks, buildCandidateLimit,
the existing ordering-domain checks, and ordinary Project/Top/IndexBuild nodes.
The rewrite adds no executor, protocol, catalog representation or cache.

## Admission and key preselection

Admit SELECT without FOR UPDATE, with:

- SORT over an optional row-preserving PROJECT and a direct INDEX join;
- direct base/index scans and a complete regular nonunique index;
- no intervening filter, pagination, existing build/probe dependency or reader limit;
- positive bounded literal LIMIT+OFFSET, without a rank option;
- known base cardinality predicting more than twice the candidate budget;
- stable predicates whose required decoded references are fully mapped and fixed-size;
- stable projections and distinct direct fixed-size SQL order columns;
- every PK component in that order, in the established safe ordering domain.

Reject floating-point/spatial ordering, truncated required values, wide keys,
unmapped or volatile predicates, repeated order keys and unsupported budgets.
Default statistics provide no cardinality evidence. Cost estimates only govern
admission; they never establish result correctness.

Count references from reachable query steps to the INDEX join and both scans.
An orphan node left by an earlier rewrite is not a consumer. A shared base scan
cannot be pruned because another consumer would see the restrictive filter.

Recheck all stable SQL predicates on decoded index values, reusing equivalent
existing rechecks. Project the stored PK and required order values once; reuse
the stored PK slot when possible. Add the ordinary Top with K=LIMIT+OFFSET and
no OFFSET, and redirect only the private INDEX build reference. Keep the original
transactional snapshots, base predicates and final SQL Top.

These exact rechecks remove encoded-prefix false positives before preselection.
The complete PK makes the admitted order total. The first K qualifying index
rows therefore contain every row needed by the final SQL Top. Runtime-filter
PASS still evaluates the original base predicates and final Top.

## Natural order and residuals

Prove natural order independently from predicate coverage. The SQL order must
match the stored order after stable typed leading equalities, with the ordinary
NULL placement and a uniform direction. Unsupported physical/logical ordering
uses the decoded-key Top.

When residual predicates are fully mapped, evaluate them before the inner Top.
Skip a leading-equality recheck only after proving that it is the actual typed
serialized lookup prefix. Keep every original predicate on the base scan.
Residuals disable the static reader limit.

Without residuals, the existing reader may select K per sorted persisted block
after PK filtering and tombstones. Unsorted blocks and workspace data retain full
reads. The global index Top still selects K across all sources. The final SQL
Top remains responsible for the complete result, including PASS.

## Statistics and scheduling

Finalize index access predicates before publishing the scan. If a bounded
backfill Top adds exact SQL rechecks to its private scan, refresh that scan
through the canonical statistics owner before publishing Project/Top. Refresh
only estimates, preserving physical placement and hash/shuffle decisions.
Parent nodes then consume estimates for the complete filter list.

For a secondary-index prefix/range built from equalities and a decoded range,
the encoded bound carries the equality-prefix estimate and the decoded range
contributes its own estimate. During scan statistics calculation, prove when an
encoded bound implies a static integer equality or exact IN recheck and count it
only once in an estimation-only view. Preserve the executable expressions and
their selectivities for filter ordering and block estimation. Dynamic or
unproved values retain their established estimates.

Regular-index scans retain real row/cost/block estimates; do not cap them to
manufacture a TP schedule. Spatial policy stays in its existing owner.
CalcNodeDOP remains the planner scheduling owner. Do not introduce separate
literal-key boundaries or mutate planned worker/receiver counts at runtime.

The existing runtime reader scheduler can use actual pruned block counts,
including ordered scans. IndexReaderParam retains its existing reader scheduling.
Plain block-list splitting divides scan work; existing shuffle operators own
bucket routing and keep all planned workers, including empty-reader workers,
active. Unrelated query branches do not participate in this scan-local decision.
Remote fragments without a complete plan retain their existing reader count.
Existing grouping transfers every backend reader to one worker and fills unused
planned slots with EmptyReaders. Local Top thresholds and final MergeTop remain
distinct. A runtime-filter DROP follows the empty-reader terminal path before
acquiring a relation or backend readers.

## Hot-path costs and ownership

Scalar decoding uses fixed local buffers and the existing byte order. Varlena
sorting compares existing descriptors through the standard sort contract.
Top/MergeTop sort retained row offsets through their authoritative comparator
rather than rebuilding and draining a heap. Their privately owned batch can
skip Shuffle only for the identity permutation; generic vector/batch Shuffle
ownership is unchanged. Spill handling is unchanged.

Reserve the existing vector key encoder's known result capacity once. Keep each
encoded key independently owned; public encoding and storage formats do not
change. IN remains a per-key MVCC lookup, including old snapshots with later
versions.

Only immutable persisted-block search may turn a sorted dense integer IN set
into exact inclusive endpoints. Reject gaps, decreasing/wrapping values, NULL,
constant and unsupported vectors. This optimization never changes memory-state
iteration into a range.

## Unsorted persisted-key membership

Ordinary integer IN search uses exact binary membership for sorted search keys
on unsorted blocks. Tiny sets and actually unsorted ordinary search keys retain
the existing linear fallback. The caller's sorted flag alone does not establish
this proof.
Floating-point and custom comparison domains keep their existing search.

Implicit primary keys are unsorted. For at most BlockMaxRows keys, their
reader-local filter may build one immutable membership map. Larger key sets use
binary search. The existing filter closure owns the map until reader close/reset;
there is no global cache or new resource owner. This does not change MVCC memory
iteration, key encoding, SQL predicates or result ordering.

## Lifecycle and resource budget

The added Top retains at most K rows of bounded order values and stored PKs,
using existing memory ownership. Its selection work is O(candidates * log K);
retained state is O(K * retained width). Complete PK components must be fixed-size.
Reference maps live only for the query builder.

Existing operator and reader owners close success, empty input, expression or
allocation failure, cancellation, partial preparation, reset and reuse. The wait
graph remains index scan -> index Top -> filter build -> base scan -> final Top.
There is no new goroutine, queue, retry loop or accumulating state. API, wire,
on-disk representation, tenant and upgrade boundaries retain their contracts.

## Evidence required before delivery

Typed controls prove admission, decline, private access, complete statistics,
metadata fallback, byte ownership and worker/EOF invariants. Real SQL compares
exact ordered rows with IGNORE INDEX over persisted data, NULL/ties, residuals,
workspace changes, rollback and prepared reuse.

Small BVT fixtures prove public result and lifecycle contracts. Typed plans and
large real tables separately prove that cost-based preselection actually runs.
Use the existing one-/two-CN fixtures and owning normal/race/static checks.

Pair latency/resource measurements against the verified target with both data
layouts and startup orders. Include point, oversized LIMIT, dense/sparse ranges,
automatic plans, neighboring key counts, PASS, stale statistics and the original
VARCHAR case. Recheck long-lived snapshot and larger-input behavior. Report any
missing or slower case explicitly. A forced index can reasonably cost more than
a better automatic access path: establish the actual plans and explain that
extra work rather than applying the same-path regression budget. Same-effective-
path and unexplained regressions remain subject to the performance gate.
