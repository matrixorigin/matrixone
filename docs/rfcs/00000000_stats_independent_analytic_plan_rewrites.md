- Status: in-progress
- Start Date: 2026-09-01
- Design revision: v8 (2026-09-12)
- Authors: MatrixOne optimizer team
- Implementation PRs: [#27914](https://github.com/matrixorigin/matrixone/pull/27914), [#27915](https://github.com/matrixorigin/matrixone/pull/27915), [#27934](https://github.com/matrixorigin/matrixone/pull/27934), [#28752](https://github.com/matrixorigin/matrixone/pull/28752)
- Issue for this RFC: [#26768](https://github.com/matrixorigin/matrixone/issues/26768)

# Stats-Independent Analytic Plan Rewrites

## Summary

This RFC is the single design record for the complete four-PR rewrite series.
It defines the legality, global ordering, resource, compatibility, rollout,
rollback, and validation contracts for:

1. sharing repeated analytic computation: multi-reference CTEs and
   grouping-set inputs;
2. exposing predicates hidden by subquery and Boolean syntax: existential
   `MARK`, filtering OR-of-EXISTS, common cross-relation DNF, and an exact
   scalar-predicate runtime filter;
3. fail-closed outer/anti planning: two guarded outer associations,
   LEFT/NULL-filter to ANTI, conservative ANTI cardinality, and exact
   preserved-side shuffle reuse; and
4. bounded analytic execution: batched projection for unique residual-free
   inner hash joins, exact decimal `SUM` prefix reuse for nested grouping sets,
   and exact bounded per-partition `RANK` with boundary ties.

The implementation may use statistics to choose among plans already proved
equivalent. Statistics, benchmark query identity, table names, scale factors,
and constants are never correctness evidence.

The implementations, rollback cohorts, and acceptance evidence are aligned, so
this revision is `in-progress`.  Design and implementation are reviewed
together: a GitHub `APPROVE` on the exact candidate head accepts the RFC,
implementation, and evidence together.  No separate design-only approval or PR
is required.

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
- positive existential MARK-to-SEMI transformations;
- attaching an exact runtime filter for an uncorrelated scalar equality while
  leaving its `SINGLE + FILTER` pair in place;
- extracting a common typed join equality from every DNF arm;
- guarded LEFT-join association and LEFT-to-ANTI conversion;
- conservative ANTI cardinality and exact shuffle-lineage reuse;
- batching output projection for unique residual-free INNER hash joins;
- reusing finalized exact decimal `SUM` values along strict grouping prefixes;
- bounded per-partition `RANK` for a small literal upper bound while retaining
  every row tied at the rank boundary.

Not in scope:

- a general memo or cost-based rewrite search;
- runtime adaptive plans;
- making inaccurate statistics a semantic proof;
- sharing recursive, correlated, volatile producers, or a producer without one
  guaranteed complete legacy evaluation witness;
- moving a `SINGLE` join or its cardinality check across another join;
- computed-key distribution equivalence or inferred uniqueness;
- partial `SUM` below a join until an aggregate-state merge contract can prove
  identical value and error semantics for an explicitly enumerated type set;
- approximate or floating-point aggregate-state prefix reuse;
- bounded `RANK` for prepared/dynamic bounds, unsafe predicates, unsupported
  partition-key equality, or bounds above 1024;
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
7. **Consumption:** an eagerly materialized CTE is shared only when at least
   one legacy occurrence is guaranteed to drain the complete producer. A
   witness behind joins is kept on the exact build path of every
   planner-approved full-build join ancestor. Other readers may stop early
   because the source supports independent release. The bounded statement-local
   `mo_current_roles` closure, optionally through direct one-column identity
   projections only, remains the sole no-witness exception.
   Grouping-set output uses the same independently readable bounded source.
8. **Determinism:** recursive, external, side-effecting, volatile, and unknown
   operators remain on the old path.
9. **Bounded resources:** planner work, plan growth, materialized memory, spill,
   and executor ownership remain bounded as specified below.
10. **Fail closed:** missing metadata, unsupported type/operator shape,
    ambiguous ownership, an invalid estimate, or a failed proof returns the
    unchanged plan.
11. **Optional-filter monotonicity:** an optional runtime filter may remove only
    a row that the original predicate cannot accept.  Missing, malformed,
    unsupported, or multi-row scalar state publishes `PASS`.
12. **Stable ordering:** an executor specialization must preserve the legacy
    probe/output order and the SQL peer relation; batching may not regroup rows
    across source batches when that would reorder the stream.
13. **Exact aggregate state:** reuse across grouping prefixes is allowed only
    when rebinding the coarser aggregate over the finer finalized value returns
    the identical exact type and scale, and preserves value, overflow, NULL,
    empty-input, and error semantics.

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
   input, subject to the negotiated protocol gate.

CTE sharing precedes grouping-set sharing because a grouping branch may contain
a nested CTE source. Both passes leave their historical inline/branch plans in
place when rejected.

### Logical rewrite boundary

For each query step, the relevant order is:

1. flatten subqueries and establish `MARK` and `SINGLE` semantics;
2. normalize and push filters, including total cross-relation DNF factoring,
   filtering-marker conversion, and bounded OR-of-EXISTS folding;
3. remove a provably effectless aggregate;
4. convert a guarded LEFT/NULL-filter idiom to ANTI;
5. recalculate statistics only if that conversion changed the tree;
6. perform join ordering and remove redundant join conditions;
7. apply preserved-side and nullable-side outer associations;
8. choose build/probe sides;
9. pull aggregates and recalculate statistics;
10. push SEMI/ANTI joins and optimize DISTINCT aggregation;
11. recalculate statistics and finalize build/probe sides.

LEFT-to-ANTI runs before the first full costing boundary so join enumeration
sees the legal shape.  The scalar rule never moves a logical node and therefore
is not part of this boundary.

After filter pushdown has exposed window-result predicates, the planner may
annotate the dedicated `PARTITION` child of one `ROW_NUMBER` or `RANK` window
with a proved literal upper bound. This pass does not move the window or its
filter, and it does not run for prepared plans.

### Physical distribution boundary

After logical rewrites stabilize, the planner swaps physical join children,
recalculates physical stats, determines hash-on-PK state, and chooses shuffle.
Shuffle-lineage reuse is allowed only here, against the final remapped join
keys.  Runtime-filter generation and projection removal then run.  Only after
final child orientation and final column remapping may the scalar rule attach a
build spec to the existing `SINGLE` and a probe spec to a safe descendant scan.
No full `ReCalcNodeStats` pass is allowed after physical shuffle metadata is
fixed, and the scalar annotation does not recost the tree.

At physical operator construction, a unique residual-free INNER hash join may
select the batched projection loop without changing the logical plan. A bounded
`RANK` annotation selects the peer-aware two-stage Partition Top-N only when the
negotiated runtime protocol supports its wire contract; otherwise compilation
uses the historical unbounded partition path.

## Detailed contracts

### Multi-reference CTE reuse

An eligible CTE has at least two reachable, type-compatible occurrences, a
deterministic non-correlated non-recursive producer, and at least one complete
legacy evaluation witness. The ordinary in-memory admission is 32 MiB. A
predicate-aware or proven hash-build source may use the existing spill owner,
with an 8 GiB planner ceiling; statement/CN accounting remains authoritative.
Variable-width spill additionally requires a known declared row capacity well
below the 64 MiB record limit. The source recursively splits a wider multi-row
producer batch into independently readable records; a schema whose one row can
approach the record limit keeps the inline plan. Total storage admission uses
the larger of the statistical row width and this declared-capacity width, so a
narrow average cannot disguise a bounded but much wider materialized result.
Planner-introduced sources reserve their estimated retained memory and their
full estimated bytes against one statement-local spill ledger. Spill is
reserved even below 64 MiB because the source also has a bounded in-memory
batch count, which estimated row counts cannot prove will be respected. The
cumulative reservation must fit `processLimitationSize`, the
explicit `processLimitationSpillSize` when set, and the 8 GiB planner ceiling;
otherwise the corresponding CTE or grouping-set rewrite is skipped.

Consumer predicates remain in place. If every consumer constrains a common
producer column with deterministic total predicates, their remapped
disjunction may also bound the producer. Output expressions are shared only
when their full row-and-column evaluation domain is already required or they
are structurally total. A fallible cast/function or incomplete predicate copy
that expands the producer's evaluation domain rejects sharing. When the exact
union of consumer row domains cannot be proved, every producer expression must
also be structurally total, including filters, joins, projections, grouping
keys, aggregate arguments, HAVING and ordering: inline predicate pushdown may
have avoided evaluating any of them on rows which eager materialization would
visit. A zero/dynamic
`LIMIT`, a limit on a non-blocking operator, `APPLY`, sampling, or a consumer
join that can skip this exact input cannot establish the required witness;
sharing remains possible only when another occurrence supplies one. A positive
literal limit is a witness only on hash aggregation or sort, which must consume
their complete input before producing the first row; `OFFSET` alone does not
shorten a fully consumed stream. A consumer predicate hidden above a projection
or other tag-remapping boundary makes the row-domain proof inexact unless that
boundary is explicitly inverted; it is never treated as an unfiltered consumer.

A witness admitted as an equality INNER/SEMI hash input, or as the logical
right build of a non-right LEFT hash/loop join, receives a physical build
marker. Every join ancestor is checked even after an aggregate or sort
established a local drain, because an empty hash build can skip its probe
subtree entirely. CROSS, full outer, probe-side, conflicting build
dependencies, and unknown join shapes cannot establish a witness. Later join
ordering and costing must keep the marked subtree on each physical build path;
otherwise the complete-evaluation proof is invalid. Non-witness readers may
stop early only because their independent source release cannot block the
producer or another reader. A predeclared runtime-filter dependency, fixed join
order, or direct function-scan build that pins the opposite child rejects that
witness path.

### Grouping-set input sharing

Only the internal `UNION ALL` created by one `ROLLUP`, `CUBE`, or `GROUPING
SETS` binding is eligible. User-written `UNION ALL` is excluded. Branches must
have identical typed group expressions and aggregate state shapes,
deterministic expressions, and positive cost. Determinism and CTE totality
checks cover expressions stored outside the ordinary node expression lists as
well, including `VALUES` expressions carried by `RowsetData`.

The selected form evaluates input expressions once, emits one derived batch
per grouping set, uses NULL vectors for inactive keys, and adds a hidden set id
to keep equal values from different sets distinct. The expansion operator
retains at most one input batch plus the current derived batch. The reduced
aggregate output is then written once to the bounded materialized source and
read independently by every branch. A lazy `UNION ALL`, `LIMIT`, `OFFSET`, or
cancellation may stop any branch without backpressuring the producer or other
readers; last-owner release reclaims memory and spill state.

Admission accounts for the actual fanout shape: one output write plus one full
output scan per grouping branch. The comparison uses the number of branches,
estimated producer work, declared input/output row widths (including Varlena
cells and variable-width payload capacity), and a twofold safety margin.
Unknown, invalid, overflowing, or marginal estimates keep the legacy plan.
Estimated materialized output above 8 GiB is rejected before execution; the
statement/CN resource owner remains authoritative below that planner ceiling.
The declared width of one output row must also stay well below the shared
source's 64 MiB spill-record bound; larger or unknown rows retain the legacy
branches, while wider multi-row batches are split by the source.
Grouping-set sources share the same cumulative planner reservation with CTE
sources, so several individually bounded rewrites cannot silently exceed an
explicit statement memory or spill cap. Spill admission includes record,
vector, nullable-bitmap, and transient-provenance framing at the legal worst
case of one record per estimated row; a payload-only estimate is not a valid
upper bound when operators emit small batches. The source contract accepts
only finalized positional batches: SQL-invisible `Attrs` are dropped before
retention or spill, and aggregate-state `ExtraBuf` is rejected rather than
silently lost on the in-memory clone path. Transient spill encode/decode
buffers use the statement/CN execution-memory ledger; the recursive-only
`cte_max_memory_bytes` quota is not reused by non-recursive shared sources.
Retained clones and spill-decoded vector storage use the same execution
allocation account until their owner releases them. Oversized batches are
encoded directly from bounded row ranges, without first compacting a second
copy of the whole producer batch.

A finite top-level `sql_select_limit` can prevent lazy `UNION ALL` branches
from starting. Ordinary finite caps and all dynamic prepared caps therefore
keep both shared-source rewrites disabled: the cap is materialized only after
optimization and cannot participate in the logical drain proof.

Grouping-set sharing requires a legacy full-drain witness. If any legacy branch
may not start or may stop early, every grouping and aggregate expression must
also be structurally total. This prevents the eager producer from evaluating a
previously inactive fallible expression on rows that an outer `LIMIT` or a
conditionally skipped input would never reach. There is one proof-based
exception: if every legacy grouping branch is independently proved to drain its
complete input, the rewrite may accept deterministic expressions without a
structural-totality proof. In that case the old plan already evaluates every
branch's active grouping expressions and every aggregate over the same complete
row domain. The exception changes evaluation placement, not its row/column
domain. Expression-removal safety, deterministic subtree, aggregate
configuration, type, and branch-rewrite checks still apply; an unknown or
partial drain proof keeps the totality requirement.

#### Exact decimal SUM prefix reuse

Before considering dynamic grouping expansion, a strict nested prefix chain
may reuse one already-required legacy aggregate branch as the input to its
coarser suffix. The pivot is the finest admitted prefix. It executes the common
raw producer and its aggregation once, writes finalized grouping columns and
aggregate values to the bounded materialized source, and every coarser branch
groups that stream by a strict subset of the keys. The pivot branch itself is
rewritten as an identity reader, so its values are not aggregated twice.

This alternative is exact only for the explicitly enumerated fixed-precision
decimal128/decimal256 `SUM` values. The coarser branch binds `SUM` over the
finalized finer values only when that binding returns the identical decimal
type and scale. Decimal addition then uses the same exact checked accumulator
contract as ordinary partial aggregation: NULL finer groups remain absent from
the sum, an empty coarser group remains NULL, and an overflow remains an
overflow. Every branch must have the same aggregate shape and a strictly nested
`GroupingFlag` chain. Floating-point, integer widening, DISTINCT, AVG, unknown
or configured aggregate state, a non-prefix grouping lattice, additional
aggregate buffers, an inherited grouping sentinel, or any unsupported type
keeps the legacy branches. Statistics choose whether the already-legal reuse is
economic; they never establish the prefix or aggregate-state equivalence.

Prefix reuse has a separate 32 GiB planner ceiling because it materializes one
reduced legacy aggregate branch rather than multiplying detail rows across all
grouping sets. Admission inflates the pivot cardinality estimate by four, caps
it at the relational one-output-row-per-input-row bound, requires a twofold
modeled byte-work advantage, and includes declared variable-width capacity.
The 32 GiB value is not an execution allowance: the same statement/CN memory,
spill-byte, file-descriptor, record-size, cumulative-reservation, cancellation,
and cleanup owners described above remain authoritative. Unknown, overflowing,
marginal, or over-budget estimates fail closed. Dynamic grouping expansion
continues to use its more conservative 32x cardinality factor and 8 GiB planner
ceiling because it multiplies the detailed input by the number of grouping
sets.

Runtime-empty input is not equivalent to the absence of grouping sets.  If and
only if an all-rolled grouping set exists, projection emits one key-only
synthetic row carrying a hidden marker.  `Group` publishes the grouping key but
passes `GroupNotMatched` to every aggregate, and `MergeGroup` merges multiple
empty CN partials into one logical row.  Thus `COUNT(*)` is zero, `SUM` is NULL,
and `GROUPING()` bits remain correct.  An empty input with no all-rolled set
emits no row.  The rollup sentinel is separate from SQL NULL and duplicate
grouping sets retain their duplicate output rows.

Grouping provenance is scoped to the grouping extension that created it.
Dynamic grouping distinguishes its own rollup sentinel from an ordinary SQL
NULL; it therefore cannot consume a sentinel inherited from an inner
ROLLUP/CUBE/GROUPING SETS relation without splitting one outer SQL NULL group
into two. Until relational boundaries explicitly normalize inherited grouping
provenance, an input subtree that can expose a grouping sentinel keeps the
legacy outer plan. A grouping-set candidate whose output feeds another grouping
extension also keeps its legacy shape, because materializing the inner sentinel
can change how that outer legacy branch hashes it. The proof follows both child
edges and materialized source steps, and captures consumer ancestry before any
candidate rewrite, so rewrite order cannot bypass either guard.

The vector grouping representation is gated by cumulative MORPC v49. Peers on
v48 or older get the historical branch-per-grouping-set plan. The protobuf
fields are append-only, and mixed-version tests use the real v48/v49 boundary.
Sender and receiver recursively reject v49 grouping metadata when the
negotiated runtime version has rolled back below v49, so a prepared or cached
plan cannot bypass the planning-time gate.

### Existential MARK and OR-of-EXISTS

A marker may become hashable/SEMI only when it is consumed solely as a positive
Boolean filter.  For `EXISTS` and `NOT EXISTS`, a raw equality remains on
`MARK` only when the resolved comparison and both operands are structurally
total and side-effect-free; the marker is totalized with `IS TRUE` at its
Boolean consumer, and `NOT` is applied only afterward.  `IN`, `ANY`, and `ALL`
retain their historical three-valued predicate handling.  Projected markers,
mixed marker expressions, non-equality correlation, and unsafe build subtrees
are rejected.

Compatible OR arms may share one `UNION ALL` build only when every arm has the
same structurally identical typed outer equality keys, every projected inner
key and comparison is total and side-effect-free, inner types agree, the MARK
prefix is consecutive, and no marker escapes.  Duplicate inner keys are
harmless because SEMI observes existence.  Different outer keys, duplicate or
missing key positions, `NOT EXISTS`, value-comparison markers, fallible keys,
and volatile branches retain the original MARK chain and filter.

### Uncorrelated scalar filters

The `FILTER + SINGLE` pair never moves.  After final physical orientation and
column remapping, an exact runtime filter may be attached only for a typed
equality between one existing `SINGLE` outer output and its uncorrelated scalar
output. The outer expression must trace as an exact bare-column lineage to a
table scan through only INNER or preserved-side non-right SEMI/ANTI joins with
deterministic total join/filter predicates. A logical shuffle flag does not
change that value or row-domain proof, so it is not itself a rejection reason;
physical delivery is proved separately after scopes are built.

For INNER joins, lineage may follow either child. Following child 1 (the
physical build side) is admitted only when child 0 is independently proved safe
to skip: the complete subtree must be deterministic and total, with a narrow
fallback for a plain raw-column table scan with total scan predicates. This is
required because an empty/NULL/nonmatching scalar filter can empty HashBuild
before the sibling probe runs. Without that proof, build-side lineage is
rejected. SEMI/ANTI lineage may follow only logical child 0 of a non-right join.
The lineage may not cross an outer join, aggregate, window, projection, nested
`SINGLE`, limit, offset, or another evaluation barrier.

The runtime filter is optional: actual scalar cardinality publishes `DROP` for
zero rows or one NULL, one exact `IN` value for one supported non-NULL row, and
`PASS` for multiple rows, malformed/unsupported encoding, or missing state.
The original `SINGLE` and filter still execute and remain the sole owners of
cardinality error 1242 and final predicate semantics. After physical scopes are
built, every consumer must have exactly one producer on the same execution CN,
and every producer must have a colocated consumer. A distributed scalar may
therefore have one complete producer per execution CN. If this topology cannot
be proved, compilation removes both optional endpoints before any pipeline
starts and executes the unchanged logical plan.

### Batched unique INNER hash-join projection

The hash join may batch result projection only for an INNER join whose build
map is proved unique and which has no residual predicate or unmatched-row
contract. `Find` still determines one build ordinal for each probe row. The
operator records at most one hashmap chunk of matching probe/build ordinals in
fixed operator-owned scratch, appends probe vectors in one selection, and
appends build vectors in consecutive source-batch runs. It never sorts or
groups non-consecutive build batches, so probe order, repeated references to the
same build row, NULLs, and result-column order are identical to the row-at-a-
time path. Non-unique, residual, outer, mark, semi/anti, single, ASOF, and
unmatched-output paths retain their existing loops.

The scratch arrays are statically bounded by `hashmap.UnitLimit` and reused for
the operator lifetime; the result remains bounded by `DefaultBatchSize` and is
charged through the existing result allocation account. A partial vector append
failure returns the allocation error through the existing terminal cleanup; no
row is published from the incomplete result batch. Reset/reuse overwrites all
selected slots and retains no source batch beyond the existing hash-join owner.

### Exact bounded per-partition RANK

A single `ROW_NUMBER` or `RANK` window with non-empty PARTITION/ORDER keys may
use Partition Top-N when a conjunction proves a non-negative literal upper
bound at most 1024. `OR`, a wrapped/ambiguous rank reference, unsafe expression,
prepared parameter, incompatible partition-key equality (floating NaN, JSON,
or array forms), multiple windows, missing order/partition keys, and larger or
dynamic bounds keep the ordinary partition/window plan. The original window
and filter remain, so the annotation only reduces their input.

`ROW_NUMBER` retains at most N rows per partition. `RANK` retains an N-row core
heap plus every row equal under the complete SQL ORDER BY comparator to the
current worst core row. When a better row changes the boundary, obsolete ties
are recycled; boundary peers may legitimately make retained rows grow to the
full partition because exact `RANK <= N` can output that many rows. Output is
sorted by the same comparator and emitted in `DefaultBatchSize` chunks. The
two-stage distributed path pre-reduces on each input and then applies the same
peer-aware reduction after merge; a with-ties operator is invalid without
`PreReduce`. Partition equality normalizes the rollup sentinel to SQL NULL on
private key copies, and rank offsets are relative to each reduced partition.

The heap/core bound, peer-growth exception, vector ownership, varlen compaction,
cancel checks, error propagation, reset, and free paths are executor invariants.
The tie-link slice is charged to the process memory pool, obsolete slots are
reused, and all retained batches, keys, links, and output are released by the
existing Partition owner. This is an exact output-sensitive bound, not a claim
that ties are capped or spillable.

### Common DNF equality

For `(K AND A) OR (K AND B)`, `K` may be copied to the join condition only when
every disjunct contains the same typed structural equality, lineage places its
operands on opposite inputs, and the resolved predicate and operands are total
and side-effect-free. The original DNF remains as a residual. Computed unsafe
keys, partial arm coverage, incompatible types/casts, volatile expressions,
and ambiguous lineage are rejected.  The single-table guard recursively
collects all relation tags; a known single-relation DNF remains intact for
range folding, while unknown lineage cannot prove single-relation ownership.
The walk does not distribute expressions or enumerate a Cartesian expansion of
terms.

### LEFT and ANTI transformations

- `(A LEFT JOIN B) INNER JOIN C` may become `(A INNER JOIN C) LEFT JOIN B`
  only when the upper predicate references only A/C and complete equality to a
  declared key of C proves the moved join cannot multiply an A row.
- `LEFT JOIN ... WHERE nullable_side_not_null_key IS NULL` may become ANTI only
  when the marker traces through pure projections to a declared NOT NULL scan
  column on the nullable side, no remaining consumer observes a nullable-side
  binding, and the marker test is the pure filter idiom.
- `(A LEFT JOIN B) INNER JOIN C` may become
  `A INNER JOIN (B INNER JOIN C)` only when every upper predicate references
  B/C and at least one bare B equality is null-rejecting.

Both moved predicates must be deterministic and structurally total. RIGHT/FULL
joins, computed/nullable markers, partial keys, non-equality predicates, and
unknown lineage stay unchanged.

ANTI output uses 50% of the finite non-negative logical-left estimate when key
overlap is unknown. A
lower bound involving right-row count is allowed only for a complete PK
equality proving that each right row excludes at most one left key. This is a
cost estimate only and is clamped to `[0, left_rows]`; a partial PK, a PK found
only on the right, a computed key, or propagated uniqueness does not qualify.

### Shuffle lineage

An exact left distribution may survive a left-preserving join only when the
next consumer uses the same bare left key. The proof follows explicit column
lineage after final remapping. Right/build lineage, changed or computed keys,
ambiguous projections, and RIGHT/FULL joins force the existing reshuffle.

Distribution scope is part of that proof. A simple multi-CN shuffle gives one
cluster-global owner for each key and may be reused by an aggregate on the same
key. A hybrid join keeps probe rows partitioned only within their originating
CN; another hybrid join may reuse that local partition because its build side
is delivered to every CN's matching bucket, but an aggregate must reshuffle to
establish cluster-global key ownership. Exact key lineage alone never upgrades
local ownership to global ownership.

## Compatibility and security

CTE and outer/anti rewrites use existing plan node and expression types.  They
change no catalog, storage, backup, client, authentication, authorization, or
tenant boundary.

Grouping-set sharing adds append-only pipeline fields and is never planned
below MORPC v49; a v48 or older deployment receives the complete legacy branch
plan. Send and receive boundaries also fail closed if a plan containing those
fields is transmitted after a runtime protocol rollback. Decimal prefix reuse
uses the same v49 materialized-source representation and introduces no further
wire field. Scalar filtering adds the optional
`RuntimeFilterSpec.scalar_predicate` plan field.  A new executor receiving an
old plan sees false.  An old executor ignores the unknown field; non-empty
unsupported state cannot synthesize the exact one-value payload and therefore
fails open with `PASS`.  Zero-row `DROP` remains a necessary condition.  The
field must survive deep-copy and remote serialization when both peers support
it.  Plans and messages are ephemeral, so restart, downgrade, and
backup/restore require no migration.

Runtime-filter tags and payloads remain inside the existing query-scoped
message path.  The payload cardinality is at most one, so the design adds no
tenant-crossing state or denial-of-service multiplier. Scalar-predicate
messages use the existing current-CN address. A logical lineage may cross a
shuffle only because compilation later proves the concrete delivery topology:
each scan consumer must have exactly one colocated scalar producer, and every
producer must serve a colocated consumer. If placement or parallel expansion
violates that relation, both optional endpoints are removed before execution.

Exact bounded `RANK` adds append-only `partition_top_n_with_ties` fields to the
plan node and pipeline instruction. Planning and physical compilation require
MORPC v67. A v66 or older negotiated runtime compiles the annotated node through
the historical full partition sort/window path with its limit and with-ties
state removed; a remotely received pipeline that nevertheless contains the v67
field is rejected before execution. Encode/decode and recursive child-pipeline
validation use the same v66/v67 boundary, so plan caching or a live protocol
rollback cannot silently discard boundary peers. The unique hash-join batching
path uses no new plan or wire field.

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
  and error. The reduced aggregate output uses the shared materialized source:
  up to 64 MiB or 4096 batches remain resident, overflow requires statement-
  admitted query-scoped spill bytes and one admitted file descriptor, and the
  8 GiB dynamic-expansion or 32 GiB prefix-reuse planner ceiling prevents
  obviously uneconomic plans. Reader release, cancellation, reset, and the last
  owner close both memory and spill state; retained and decoded vector
  allocations remain charged to the execution account for their complete
  ownership interval.
- The scalar runtime filter adds at most one fixed-cardinality value payload per
  eligible filter.  It reuses the existing query-scoped message owner and adds
  no goroutine, queue, retry, or persistent cache.
- Unique hash-join batching adds two fixed `hashmap.UnitLimit` ordinal arrays to
  the operator and no input retention, queue, file, or goroutine. Result vectors
  remain under the existing allocation account and batch-size limit.
- Bounded `ROW_NUMBER` retains at most N rows per observed partition. Bounded
  `RANK` additionally retains exactly the live peers of the Nth ordering key;
  this can equal the full partition because the SQL result itself can equal the
  full partition. Retained rows, private ordering keys, and the mpool-accounted
  tie links have one Partition owner and are released on reset, free, cancel,
  and error. There is no new file, FD, goroutine, queue, or persistent state.
- No rule adds a goroutine, channel, lock, RPC wait, or persistent format other
  than the append-only optional plan-wire fields described above.
- Allocation, expression, codec, child, and cancellation errors propagate
  through existing owners; no fallback converts an execution error into a
  different result.

## Implementation acceptance budgets

These measurements are implementation-approval gates, not prerequisites for
accepting the semantic design.  Base and exact candidate use the same host,
toolchain, fixture DDL/statistics, and ordinary `EXPLAIN` corpus.  The report
must preserve raw artifacts and exact revisions.

- rejected and control queries must not regress planner wall time or allocation
  bytes by more than 5% at p50 or 10% at p95;
- admitted queries may add proof metadata and plan nodes, but must remain below
  15% planner wall-time and 25% allocation-byte regression at p50, and below
  25% at p95; maximum time and reachable node counts are also reported so a
  median cannot hide expansion;
- no rejected/control query may gain reachable scans, joins, or materialized
  producers;
- no accepted CTE may exceed the 32 MiB resident or 8 GiB spill-planner bound;
- no accepted grouping-set materialization may exceed its 64 MiB/4096-batch
  resident bounds, its 8 GiB dynamic-expansion or 32 GiB prefix-reuse planner
  ceiling, or the lower statement/CN spill budget; modeled saved producer
  byte-work must exceed output write/read traffic by the twofold margin;
- grouping-set sharing must reduce repeated detailed inputs and must not create
  more aggregate states than the legacy branches;
- the unique hash-join path must allocate no per-row/per-match scratch and must
  preserve byte-identical ordered results for matched, NULL, repeated-build,
  allocation-failure, reset, and reuse cases;
- Partition Top-N may be admitted only for N <= 1024; `RANK` must emit every
  boundary peer in bounded output batches, and a large peer set must terminate
  without unaccounted memory or a lost/duplicated row;
- outer/existential rewrites must not increase fact-scan count;
- shuffle reuse must preserve the exact key and must not increase planned
  repartitions outside its admitted shape.

TPC-DS 1 TiB runtime is supporting performance evidence, not a correctness
oracle. A faster target query does not offset a semantic failure or an
unexplained control-plan regression.

For every changed 1 TiB target, record terminal result, wall time, rows/bytes
scanned, peak query memory, and spill bytes.  The fixed TPC-H corpus is the
no-regression control.  An unavailable exact-head scale run may remain an
explicit open artifact only when the corresponding plain plan, deterministic
result oracle, and prior successful resource profile are retained; it cannot be
claimed as a performance pass.

## Validation matrix

| Rule | White-box/typed proof | Black-box acceptance | Mandatory unchanged controls |
|---|---|---|---|
| CTE reuse | reachability, complete-evaluation witness, type, determinism, row-domain, memory/spill and build-role tests | public SQL duplicate/NULL/result checks; spill/reset/error/partial-reader paths | no-witness empty-build probe, recursive/correlated/volatile/fallible/unreachable/incompatible producers |
| grouping sets | internal-origin marker, typed branch compatibility, every-branch drain/conditional totality, inherited-sentinel exclusion, byte-aware fanout/storage gate, MORPC v48/v49 plan and send/receive boundaries, codec round trips | distributed ROLLUP/CUBE/GROUPING SETS results with SQL NULL, rollup sentinel, duplicates, nested grouping extensions, runtime-empty input, early-stop readers, and spill | user UNION ALL, partial drain with fallible expression, incompatible state/type, inherited grouping provenance, old protocol, unknown/wide variable row, high-cardinality output, no all-rolled set |
| decimal prefix reuse | strict nested prefix, canonical decimal SUM, identical rebound type/scale, 4x cardinality and 32 GiB/statement admission tests | exact decimal positive/negative/NULL/empty and multi-prefix rollup results with spill/reset | float/integer/AVG/DISTINCT/configured aggregate, non-prefix sets, overflow/unknown estimate, inherited sentinel |
| MARK/OR EXISTS | positive marker ownership, totality, typed keys, and reachable `UNION ALL + SEMI` tests | independent EXISTS/OR results with duplicates, NULLs, multiple/composite arms | NOT/IN/ANY/projected/mixed/volatile/fallible/non-equality/correlated/different-key markers |
| scalar filter | retained `SINGLE`, actual-cardinality state machine, exact join lineage, safe-to-skip build sibling, one-producer-per-CN topology | scalar 0/1/>1-row results/errors; local/shuffled delivery; empty/NULL/one-value sibling error and volatile controls | correlated, unsafe build sibling, right SEMI/ANTI, outer/nested-single/project/window/barrier/limit/unsafe predicate, missing/duplicate/non-colocated endpoint |
| unique hash projection | unique residual-free admission, fixed scratch, ordered consecutive-build-batch selections | byte-identical matched/NULL/repeated-build results; allocation error and reset/reuse cleanup | non-unique, residual, unmatched-output, MARK/SINGLE/SEMI/ANTI/outer/ASOF paths |
| RANK Partition Top-N | literal-bound recognition, complete peer comparator, obsolete-tie recycle, two-stage exactness, mpool ownership, MORPC v66/v67 boundary and codec round trip | differential ROW_NUMBER/RANK results with NULL/order ties, >batch peer output, grouping sentinel, reset/cancel/error | prepared/dynamic/>1024/OR/wrapped/unsafe bounds, unsupported partition equality, multi-window, old protocol |
| DNF key | exact total common-key, complete relation walk, residual retention | differential DNF results/errors with NULLs and duplicates | single-table range DNF, missing-arm key, computed/fallible/incompatible/volatile/ambiguous key |
| LEFT/ANTI | null-rejection, pure marker lineage, complete uniqueness and rule-order tests | public outer/anti result checks with duplicates and NULLs | nullable/computed marker, partial PK, non-total predicate, RIGHT/FULL/non-equi join |
| ANTI estimate | bounded estimate and complete-key tests | plan-only cost comparison; SQL result unchanged | missing/partial/computed/nullable/non-equality key |
| shuffle lineage | exact post-remap key and preserved-side tests | multi-owner plan plus exact result | build/right lineage, key change/expression, ambiguous projection, RIGHT/FULL join |

Relevant planner, compiler, executor, and public issue packages must pass unit
tests and `go vet`; repository SCA and distributed BVT must pass on the exact
rebased implementation heads.

Cross-rule tests cover the declared composition boundaries: CTE to grouping,
DNF to shuffle, MARK to build-side selection,
and LEFT-to-ANTI to costing/shuffle.  CTE producer error/cancel while another
consumer drains, and materialization spill disk/FD failure, require terminal
error plus cleanup oracles rather than plan snapshots.

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

### Partial SUM through a unique dimension join

Deferred.  Declared dimension uniqueness proves only that the join does not
multiply a matched fact row.  It does not prove that `SUM(SUM(x))` preserves
floating rounding, integer/decimal overflow and error timing, or the evaluation
domain of a fallible fact expression on orphan keys.  The current series must
retain join-then-aggregate and remove the prototype rule.  A future RFC may
admit an explicit type set only after defining an aggregate-state merge (not a
second SQL `SUM`), orphan-row totality, overflow, NULL/empty, and exact
black-box contracts.

The decimal grouping-prefix rule does not weaken this decision. It crosses no
join, changes no orphan-row domain, and reuses a legacy grouping branch that the
same query is already proved to drain. Its narrower exact-type contract does
not authorize a general partial aggregate pushdown.

### Split every rule into a separate PR

Deferred for this series. It would produce many cross-dependent PRs and force
reviewers to reconstruct ordering and interaction across them. The selected
alternative is one approved versioned design plus four integration PRs:
#27914 for shared-computation execution/planning, #27915 for subquery/Boolean
predicate exposure, #27934 for outer/anti/shuffle planning, and #28752 for the
bounded shared-computation, scalar-lineage, hash-projection, and Partition
Top-N follow-up. Within each PR, mechanisms remain isolated in named
helpers and typed positive/negative test closures, so review and targeted
rollback do not depend on query-specific switches. If the integration review
cannot establish one mechanism independently, that mechanism must be split
before approval.

## Observability, rollout, and rollback

Every admitted logical transformation records a named optimizer-history entry.
Plain `EXPLAIN` and statement/operator profiles expose the resulting producer,
grouping domain, join tree/type, ANTI estimate, runtime-filter tag/type, shuffle
key/reuse method, Partition Top-N keys/limit, memory, spill, and terminal
status. No per-row or high-cardinality metric is added.

Three global `optimizer_hints` rollback cohorts are owned by `QueryBuilder` and
default to `0` (enabled): `sharedComputation=1` restores all #27914 legacy
paths, `subqueryPredicatePlanning=1` restores all #27915 legacy paths, and
`outerAntiPlanning=1` restores all #27934 legacy paths.  Each switch is parsed
once at planning entry and must be covered by positive and rollback plan tests.
The #28752 grouping-prefix/full-drain changes remain inside
`sharedComputation`, and its scalar/DNF changes remain inside
`subqueryPredicatePlanning`; those switches restore the corresponding legacy
plans. Grouping-set execution also has a deterministic compatibility fallback:
protocol versions below v49 always receive the legacy plan. The optional
scalar runtime filter keeps its runtime `PASS` fallback for
non-selective scalar results; an unproved current-CN topology instead removes
the physical filter before execution, independently of the planner switch.

Bounded `RANK` is additionally contained by MORPC v67: lowering the negotiated
version to v66 selects the legacy full-partition path, and a stray v67 pipeline
is rejected. Unique hash projection and the v67 executor loop have no durable
state or cross-query switch; after an incident their bounded implementation
commits are independently revertible without changing plan, catalog, or stored
data. They are not folded into an unrelated optimizer hint merely to create a
per-row hot-path branch. A protocol rollback, cohort switch, or targeted revert
is applied before admitting new statements; already-running statements retain
their original process-owned cleanup path.

Rollout is deterministic UT/public SQL and wire/error-path tests, frozen
TPCH/TPC-DS plan corpus, isolated 1 TiB targets, TPCH performance control, then
normal CI.  A wrong result/error, unexplained control-plan change, budget
breach, leak/deadlock, OOM, or timeout stops rollout and enables the owning
cohort switch.  Once isolated, one mechanism is removed by targeted revert;
query, table, benchmark, and literal exceptions are forbidden.  Reverting one
cohort does not require reverting unrelated stats or executor memory work.

## Approval record

The RFC and its owning implementation are one review unit. Review comments may
iterate on design or code in any order, but the decisive GitHub `APPROVE` applies
to the complete exact head: RFC, production code, tests, and attached evidence.
Any later semantic or implementation commit changes that head and requires
re-review under normal GitHub rules. All four implementation PR bodies link
the same RFC revision so reviewers can assess the global order while approving
each PR's final implementation diff.

## Decision log

- v8 identifies follow-up #28752 and closes its design delta. It specifies the
  all-branches-drain evaluation-domain proof, the separate 32 GiB/4x decimal
  prefix admission, exact decimal SUM rebinding, safe build-side and shuffled
  scalar lineage with one-producer-per-CN compile validation, fixed-scratch
  unique hash projection, and exact peer-aware RANK Partition Top-N behind
  MORPC v67. It also records output-sensitive tie growth, compatibility
  fallback, rollback ownership, and deterministic positive/negative evidence.
- v7 aligns the accepted contract with independently released bounded
  materialization: one guaranteed CTE legacy drain preserves eager producer
  evaluation while other readers may stop safely. For grouping output,
  readers may stop independently, admission prices one write plus every full
  branch scan with declared row widths and a twofold margin, and an 8 GiB
  planner ceiling bounds spill exposure. It also checks every CTE join ancestor
  for an exact preserved hash-build path, adds v48 send/receive rollback fences,
  and restores the independent Parquet fanout capability to its v45 owner.
- v6 advances the aligned series to `in-progress`.  Branch-local numeric
  placeholders are integration metadata assigned against the merge base; the
  reviewed compatibility contract is the predecessor fallback and its boundary
  test.
- v5 distinguishes unchanged-path overhead from bounded work performed only
  after a rule is admitted.  This keeps the 5% control-path gate while giving
  semantic rewrites a fixed, workload-independent 15% wall/25% allocation
  budget; measured exact-head maxima are 12.1% and 20.2%.
- v4 supersedes the closed v1 design and the two PR-local draft RFCs; reviewers
  need one global order and counterexample matrix for all three PRs.
- Allocate grouping-set expansion at final rebase to the next contiguous
  unowned MORPC version and test its real predecessor; open-branch numbering is
  not a design invariant.
- Synthesize runtime-empty all-rolled grouping rows explicitly; statistics do
  not prove non-empty execution.
- Keep `SINGLE + FILTER` in place. Build-side scalar lineage is legal only when
  the skipped sibling is proved deterministic and total; shuffled lineage is
  legal only when compilation proves one complete colocated producer per CN.
- Defer partial SUM rather than use statistics or an unspecified numeric state
  as a semantic proof.
- Treat corpus and 1 TiB measurements as exact-head implementation gates, not
  semantic design prerequisites.
- Use three operational rollback cohorts plus targeted code reverts; do not add
  query or table exceptions.

## Ready gate

Before requesting decisive approval, the final candidate closes the global
non-fixpoint order; conditional totality and build-side skip proofs; scalar
physical topology; exact decimal prefix semantics; unique projection order;
RANK peer, resource, and MORPC v66/v67 contracts; resource ownership; the three
rollback cohorts plus targeted executor rollback; implementation budgets; and
the positive/counterexample/cross-rule matrix. No blocking semantic question is
intentionally deferred.
