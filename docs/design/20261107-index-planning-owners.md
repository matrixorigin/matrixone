# Existing owners for index planning and plan reuse

Status: design-first decision for the replacement of PR #31944 and issue
#32318. The user requested a complete transitive refactor in a new PR while
leaving #31944 unchanged. Source is `db225c815a0cfb38cde193e49fd1891ee737204a`;
the imported feature baseline is `eb5f432079e24748cfe3718a279eca12183474ff`,
initially on main `aeab8787f2b8acd4a80d47a2f23fd263d6f0ef2d`. The implementation PR will
link this reviewed design revision. This document replaces the intermediate
20261106 borrowed-read and temporal-access-consolidation documents; those
superseded API descriptions are removed. Type inversion and cost semantics
remain in [temporal index ranges](20261028-temporal-index-ranges.md) and
[temporal CBO](20261030-temporal-cbo.md). This is an ownership refactor, not an
expansion of eligible SQL or a claim that a new correctness bug was found.

## Contract

Retain the typed mixed DATETIME/TIMESTAMP comparison, fixed-zone inversion,
precision rounding, sentinels, physical tails, execution-time endpoints and
independent SQL residual. A TRUE SQL predicate must pass every necessary
physical candidate. Type algorithms and their tests remain type-specific.

Move generic read identity, ordinary resolution, statement execution evidence,
candidate provenance and cache decisions to their existing owners. A new name
is not sufficient: remove the superseded interface, data representation,
independent decisions and routes, and migrate every producer and consumer.
The user explicitly requires cleanup: no deprecated aliases, temporal-to-generic
forwarders, temporary dual paths or old fields retained as compatibility shims.
Optional arguments are allowed only as the final ordinary API's real opt-in to
borrowed observation; they must not select an old implementation.

The supported statement/workspace contract remains serialized. A read stamp
detects a transition; it is not a lock, a permission or a resource owner.
Optimization failure still declines to ordinary BASE when it did before;
resolver errors, cancellation and mandatory metadata failures retain their
original error owner and ordering. No unbounded retry or fresh relation lookup
is introduced.

## Responsibility and deletion map

| Guarantee | Current duplication | Existing owner after this change |
| --- | --- | --- |
| Retain execution-context expressions before folding | Function-ID checks repeated in both generic folders and constant-purpose classification | Existing function registry declares a runtime-evaluation boundary for the two actual internal endpoint families. Generic folders, constant analysis and runtime-tree consumers use that semantic fact; delete the temporal-ID classifier. |
| Transaction/workspace/snapshot/visibility identity | `engine.TemporalRead` wraps `client.CatalogReadStamp`; frontend, view provider and disttae repeat comparisons | Bind the actual caller operator and sampled visibility to `client.CatalogReadStamp`; its matching methods implement shared identity comparisons. Delete `TemporalRead`. |
| Ordinary descriptor and borrowed relation came from one authorized lookup | Separate `BorrowingTableResolver`, frontend injection, `ResolveBaseForPlanning` and `ResolveIndexForPlanning` | Optional caller-owned `TableResolution` output on ordinary `CompilerContext.Resolve` and `ResolveIndexTableByRef`. Delete the richer interface, both side-entry methods and resolver injection. |
| Statement/context and placement still match | Temporal intent/input types and scheduling wrapper | One execution intent passed through the existing root builder/frontend/compile attempt, with placement checked by the existing scheduler. Delete the temporal scheduling type and temporal-only input route names. |
| Necessary predicate provenance | `Expr.TemporalCandidateFilter` throughout copying, pruning and compile | A generic `Expr.CandidateFilter` fact consumed by existing filter/index compilation. Preserve residuals, OR unknown behavior, bounded seek and physical-index placement checks. |
| Execution values affect access selection | `Query.TemporalIndexNeedsReplan` and scattered frontend decisions | Publish `Query.ExecutionDependentAccess`; the existing frontend cache owner derives a single reuse policy for ordinary generations, prepared logical plans and physical compiles. Delete the temporal policy helper and duplicated case-specific cache decisions. |

`TemporalCandidate`, bound evaluation and typed range inversion remain necessary
type semantics. Comparison classification, cost observation, metadata walks and
scan construction belong to the existing comparison/index/reader owners.
`BorrowedTable` is synchronous evidence owned by the ordinary transaction; it
does not become a relation cache or gain a Close operation.
Copied index statistics, index-pair/schema witnesses and their fixed budgets
are exposed as the existing relation's `IndexPlanningRelation` capability.
They carry no temporal control state. The block-observation request uses the shared comparison operator and typed scalar
bound. Precision and zone are comparison inputs, not a temporal control path.

## Function-owned evaluation boundaries

The two internal endpoint families must remain executable, including their
arguments, until the current reader/execution context exists. Value stability
and the existing real-time flag do not express that subtree boundary: NOW can
fold at a statement boundary, while folding an endpoint's children early would
capture an earlier execution value. Preserve both current behaviors.

Declare the missing `runtimeOnly` fact on the existing function registration,
only for those two actual endpoint families. Both generic folding engines stop
at that boundary; the existing constant-purpose owner denies plan/fold-candidate
classification but still proves runtime constancy from the ordinary overload
and argument rules. Runtime-bound-tree observation derives from the same
registration. Delete `IsTemporalBoundFunction`, `ContainsTemporalBound`, and
their per-ID folding branches without aliases. This adds no evaluator, cache,
registry, dynamic flag or broader admission. A function-family boundary retains
its existing malformed-overload behavior; unknown unrelated functions keep the
normal lookup/error policy. CASE/ELT laziness, diagnostics and prepared source
rules remain independent existing contracts.

## Read identity

Raw `CatalogReadStamp()` and `TryCatalogReadStamp()` retain their existing
locking, status and error behavior. Bind the *actual caller's* operator after
sampling; do not substitute the underlying operator behind a wrapper. Sample
workspace visibility outside the operator lock. Optional borrowing preserves
nonblocking Try, pointer-workspace admission and BASE fallback. View metadata
preserves blocking capture, unsupported-workspace errors, unstable visibility
errors and its statement/role/protocol/resource-generation checks.

Centralize common operator, transaction ID, workspace, revision and visibility
comparison on the stamp. Provide explicit semantic-timestamp and structural-
timestamp match modes: temporal reads currently use `Timestamp.Equal`, whereas
view reads use `reflect.DeepEqual`. Do not silently change one to the other.
Sampling policy remains with the caller; the shared data/matching owner must
not turn a mandatory view failure into an optional decline. Do not change the
existing behavior of incomparable workspace wrappers as incidental cleanup.

The data is bounded by one statement/optimizer attempt. Disttae retains its
readonly/DDL, engine, tenant, base/hidden schema, table identity, subscription,
budget and final pair checks. Cost statistics never grant authority.

Optional block observation tries the subscription and engine read locks in
their existing order, and tries the subscriber's client lock. Cached column
metadata uses the S3-FIFO owner's try-get operation. `Get` and `TryGet` share
the locked lookup, admission, retention and frequency-update behavior; only
acquisition of the index-shard mutex differs. Small public entry points select
the lock mode in the same lookup frame, avoiding another call inside the
ordinary locked path. A contended shard or absent entry
returns no value, does not run admission, and leaves references and frequency
unchanged. A successful lookup retains the value under that lock. The caller
releases the retained value and any independent admission reservation. Admission
errors and retain panics preserve the ordinary lookup's cleanup behavior.

The generic try operation only promises not to wait for its index-shard mutex;
hash, admission, retain and release callbacks must themselves be bounded and
must obey the cache's existing reentrancy contract. Metadata observation uses
nil admission and Go-managed immutable `metaBytes`, whose retain and release
are no-ops. A borrowed metadata view holds its Go slice through the synchronous
observation and returns copied column values. Pooled or mutable metadata would
require revisiting this lifetime contract.

Initial sampling and final revalidation use these same operations. A failed try
discards all partial counts. An unavailable necessary-range candidate leaves the
established fallback available; an independently eligible ordinary index winner
survives an incomplete optional refinement. Final stats and read-domain checks
still run after an unavailable final snapshot or transport. The existing final
cancellation-cause check retains its error precedence and iterator cleanup. No
optional observation waits for a subscription, transport, engine or cache lock.
The migration from the deleted FIFO owner to S3-FIFO requires reference-lifetime,
contention, cancellation and ordinary-lookup performance controls in addition to
the existing planner and SQL result controls.

## Ordinary resolution and root lifetime

Add a variadic optional `*TableResolution` output to the ordinary name/index
resolver methods, including the real `ResolveWithoutTemporaryAlias` namespace
variant. Resolution function values, prepared schema resolvers and modification
checks use this final signature directly, with no old-signature adapter.
Zero-output calls keep the old work: no stamp capture, native lookup,
allocation or subscription is added to the ordinary non-candidate path. The
root builder requests one output only when its execution input is admitted.
The resolver clears requested outputs before work and fills them only after
the original resolution, temporary/subscription/snapshot policy, metadata
recovery and borrow checks succeed.

Migrate every implementation, wrapper and mock of both ordinary methods. A
wrapper's real Resolve override remains the route actually invoked; no richer
method may be discovered through Go method promotion. A wrapper that changes
view/tenant/snapshot semantics explicitly declines optional borrowing while
retaining ordinary descriptor/error behavior. Synthetic and internal compilers
may decline by leaving the output empty. Existing descriptor callers need no
new lookup or native capability.

The ordinary CompilerContext also owns borrowed-result validation, so the
builder rechecks the original statement/process/transaction after reentry
without retaining an alternate resolver interface or callback owner. Preserve
the root input's exact compiler eligibility and identity; wrapper admission is
not broadened by this refactor. The exact original relation is used
for costing; the winning hidden relation is resolved once through the same
ordinary method. Preserve the fresh original/current/publication proofs and
both real engine pair checks.

Keep the root's one-time claim, scan/tag/version association, duplicate/64-item
bound and disabled-on-overflow behavior in the existing QueryBuilder. Clear
borrowed references and input on success, error, cancellation and panic via the
same lexical defer. Nested SELECT, EXPLAIN, prepared FIELD/rebind and retries
must not claim an earlier root or inherit its records. No plan stores a native
relation, transaction operator, statement owner or callback.

Collapse the default-argument-only planning/prepared/frontend forwarding APIs
into their canonical implementations. Migrate call sites, function values and
builder callbacks to the final signatures, including explicit absent inputs
where needed. Preserve argument evaluation order and evaluate each existing
argument once. Remove the old callback adapter as well; callbacks implement the
new input contract directly. Real authorization/namespace/protocol entry points
retain their policy and invoke the one canonical implementation.

## Candidate, execution and reuse

Candidate provenance is a generic property of a necessary storage predicate.
Keep its wire number and all copy/fold/reader/transport consumers consistent.
It is independent of authorization and independent of whether the scalar
endpoint is refreshed. Do not mark new SQL shapes or LIKE cases merely to
demonstrate another producer. Reader support already handles generic OR,
unknown and seek behavior and must retain their controls.

Execution intent remains an immutable value snapshot of current clock, zone,
placement and offload facts. Its owner is the existing
statement/root planning/compile attempt, not a second temporal lifecycle. Use
the ordinary scheduler for local placement and retain fixed-zone admission,
ForceOneCN, sticky candidate topology, reset rejection and retry/rebuild fences.
Clock or zone drift before physical execution follows the existing unscoped
rebuild path; do not add a new Run state machine or extend context budgets.

The ordinary Compile constructor becomes the only constructor entry. Its final
setup mode expresses the three existing sequences: immediate publication with
the ordinary offset boundary; deferred publication and deferred offset after
preflight; or definition retry, which establishes its offset before allocating
the new Compile while deferring Process publication. Remove both specialized
constructors and the retry-forwarding wrapper. Keep sequence-retry state,
message-board publication, preserved existing offsets, labels, panic cleanup
and pool/reset ownership at the exact original checkpoints. Invalid internal
setup modes reject before allocating resources. The mode represents an actual
remaining construction contract, not a selector for an old implementation.

Cache policy distinguishes three questions: ordinary generation reuse,
prepared logical-template rebuild and prepared physical-compile reuse. Derive
the answers at one frontend cache policy owner from existing facts, computing
only the requested layer so unrelated dependency walks are not added. Keep the
current differences for FK actions, view/UDF/subscription/index coverage,
DDL defaults, percentile/EWM execution configuration and Iceberg snapshots.
Move their existing checks into this owner rather than inventing a second
dependency registry. `Node.NotCacheable` is not equivalent and is not used as
a replacement. Preserve prepared SELECT 1 and permanently ineligible query
reuse. Preserve existing time-zone cache invalidation and AST ownership.

`ExecutionDependentAccess` is set for a potentially eligible BASE as well as
an INDEX: a later clock can change cost and make an index worthwhile. A fresh
runtime endpoint cannot recost a previously selected BASE. Do not broaden
function safety/admission or rebuild every statement-stable expression.

`NewPlanningInput` admits only parameter-free parser roots; compile-time intent
matching independently rejects parameterized roots. Parameter admission is not
a copied flag on execution intent. Prepared templates and bound execution plans
are identified by the planner's existing reusable-plan state and cannot produce
temporal candidates or an execution-dependent temporal lifetime. This distinction
also applies when no optional input exists: removing a prepared input must not
disable prepared plan reuse.

Prepared execution retains its ordinary parameter binding, declaration history,
schema and time-zone checks, scheduling, binary metadata, authorization and
definition-change retry. It carries no candidate input, execution intent,
candidate-only validation/rebuild path or candidate retry snapshot. The bound
planner and frontend APIs take only their actual binding inputs. Ordinary
candidate retries continue to carry the execution intent through the existing
Compile owner. Remove prepared-only marker/intent tests while retaining real
prepared and ordinary candidate behavior and resource/error oracles.

## Compatibility and costs

No catalog/on-disk format, SQL syntax or result/error contract changes. These
candidate fields are introduced only by the unmerged source PR; generic names
retain their existing protobuf tags and boolean values. Regenerate code and
descriptors together. Preserve the repository's stopped-upgrade contract;
rolling mixed-version candidate execution is not introduced.

Raw stamp calls keep their lock behavior; binding/matching are bounded scalar
operations. Non-opted resolver calls add no read work. Optional resolution
still performs one ordinary lookup and one winning hidden lookup, retaining
the existing 64-record and expression-work bounds. Cache policy is stack-local
derived data, with no persistent cache, queue, goroutine or global registry.
Check ordinary/prepared constructor cost and allocations separately from SQL
latency. Retained native metadata scans keep existing bounds and Try locks.

## Alternatives

1. Retain the source implementation. Safe baseline, but leaves a temporal
   read representation, resolver side channel and scattered cache policy.
2. Rename temporal types/fields only. Rejected: it retains those independent
   representations and decisions without transferring guarantees.
3. Return a completely new descriptor result from every ordinary Resolve call.
   This forces over 100 descriptor consumers to destructure a new result even
   when they cannot use borrowing. The explicit optional output preserves the
   same ordinary override while closing the side channel with less churn.
4. Build a general expression/dependency framework or use NotCacheable for all
   layers. Rejected: no such framework is needed for the current consumers,
   and existing cache layers have deliberately different policies.

## Validation and acceptance

First validate the shared owner with lightweight tests: wrapped operator
identity, snapshot comparison modes, visibility/revision drift, optional Try
failure; ordinary resolver override/forward/decline, output clearing, no extra
lookup, reentry and root cleanup; and cache-layer truth tables with temporal
and non-temporal consumers. Existing negative cases remain named and executed.

Then run the affected owner/consumer normal and race suites, protobuf
descriptor/copy/transport checks, existing temporal SQL integration with full
IGNORE INDEX controls, ordinary/prepared/time-zone and sparse/dense transitions,
and relevant view/index authorization and two-CN placement/transport controls.
Use the repository CGo wrapper and disk temp directory. Run selected SCA for
the complete intended new-PR delta and affected consumers.

Compare the imported feature baseline and final executable with identical
build/native/toolchain, fixture, settings and interleaved order, full-row
controls and physical-plan observations. Cover shuffled/ordered sparse/dense,
same-domain and ordinary/prepared paths. Report p50/p95, work and allocation
scope, same-source variability and any unresolved regression signal. Historical
source-PR benchmark reports do not certify this implementation.

Before delivery, read the complete new-PR change map, review all new owner
closures and reconcile inherited source changes and valid evidence. Record
design and final self-review honestly; no independent reviewer is implied.
Deliver a new fork feature branch and PR targeting main. Do not update or close
#31944, merge a PR, push a default branch, or claim pending CI passed.

## Integration with current physical-index authorization

Before delivery, main advanced to `bfbb2645c03422a4c5d3a77a9e56942710944643`
(#32032). This changes an actual dependency: ordinary resolution now denies
explicit physical-index access, generated SHOW and index-maintenance sources
have authorized resolver routes, and catalog-generation checks have a separate
raw identity-validation entry. Integrate these owners before publication; the
previous binary evidence does not certify this new combination.

Ordinary `Resolve` and its real namespace variant must pass through the current
`resolveUserRelation` admission. Optional borrowed output is cleared before work
and cannot escape a denied resolution. The metadata-only catalog validator
keeps its authorization-neutral identity purpose, uses the final callback
signature directly and declines borrowed output. Root scans route through the
current generated-source/maintenance authorization owner; its ordinary branch
may forward the same optional output to `Resolve`, without another lookup,
adapter or bypass. Generated SHOW sources retain `ParentObjRef`; they do not
inherit a user scan's borrow. Reentry, post-resolution read and final base/index
pair checks remain necessary after authorization.

Move main's new temporary-DDL reuse restrictions into the existing `planReuse`
owner. Preserve its compile retry maintenance specification, literal snapshot,
trusted caller provenance and publication/cleanup ordering alongside the three
canonical construction checkpoints. Do not reintroduce removed temporal APIs.
Reserve main's released function ID 633 for `MO_IS_PHYSICAL_INDEX`; append the
two unmerged endpoint families at 634/635 and renew the exact registry oracle.
Regenerate the combined pipeline schema, retaining both main's maintenance
messages and this feature's fixed-offset zone fields without changing tags.

Validation renews the function/planner/frontend/compile and generated/wire
closures in normal and race modes, current hidden-index authorization and
existing temporal/view/two-CN public SQL, plus scoped static analysis. Renew
paired measurements using the same original implementation integrated with
this main revision as the immediate pre-refactor baseline, and the final
combined binary. Reuse typed math, reader and client evidence only where their
semantic inputs are unchanged. Original PR #31944 stays untouched.

## Complete comparison and physical-cost integration

Design gate: gpt-6.1-terra/max approved this continuation against
`545426c9d680e0d52ac65da258e4d1da4469769b`, base
`647eaf9dd36d4b8521114e5dedb9d99d4c148c68`, session
`01a1b2f1-23e5-7033-a552-beaa87385b9b`. This section supersedes earlier
allowances for separate temporal cost/block pipelines. PR #32341 implements
this design; #31944 remains unchanged.

The common comparison owner classifies an oriented column/bound/operator as
raw, losslessly normalized, inverse necessary range or unsupported. Exact
serialization and equality-pinning callers accept only their existing exact
cases. A shared `types.ComparisonOp` replaces `TemporalOp` without aliases and
preserves internal endpoint numeric values. The type inverse supplies bounded
logical arms to the same direct/serialized physical-leaf lowering used by
ordinary single-component equality and ranges. Full residuals, prefix/NULL
rules, selectivity and collation remain owned by ordinary index construction.

QueryBuilder captures the bounded original scan proof before folding for both
safe literal and mixed comparisons. Conservative execution-dependency discovery
remains independent of current optional input and selected BASE/INDEX. The
regular-index loop collects necessary descriptors alongside ordinary matching,
then observes them only if ordinary matching owns neither a winner nor a
fallback. FORCE, small-table, sorted-scan and spatial policies stay unchanged.
There is one scorer and one covering-scan constructor/publication owner.

The copied-statistics range-union estimate and cached block observation have a
second real consumer: an already eligible ordinary signed/unsigned integer
comparison with one source-free, exactly representable literal. One matching
final block filter, safe original/current shape, local execution intent and
current borrowed read are required. The scored covering winner is rechecked
against complete observed BASE work using the common block-granular estimate;
its existing filter/output estimate is never reduced. An incomplete observation
preserves the ordinary choice. This is a controlled physical-cost expansion,
not expanded SQL or index admission.

Ordinary tag allocation and hidden relation resolution happen at their existing
error checkpoint, before optional costing can choose BASE. Reuse that resolved
metadata and revalidate after reentry; never add a lookup or suppress resolver
errors. Necessary ranges retain private tag/map/residual construction, both
real relation-pair checks, fixed-zone/local placement and final cancellation
checks before any publication. Remove the separate resolved-index callback.

`IndexBlockInputRequest` carries column, observed stats, copied scalar bytes,
scalar type, comparison scale/operator and zone. `ObserveIndexBlockInput` uses
the ordinary scalar zone-map comparator also used by reader execution. Mixed
conversion is private to that comparator. Preserve decimal bound scales,
unknown propagation, native ordinary BETWEEN, sorted seek and bloom restrictions.
The disttae index-planning owner performs the cache-only bounded walk with all
existing Try locks, iterator cleanup, zero-partial-output and final identity,
subscription, transport, stats and cancellation checks. The new optional
ordinary observation suppresses non-cancellation observation errors; the
existing necessary-range path keeps its existing resource-error contract.

Delete the nine superseded temporal original-analysis, candidate-admission,
candidate-expression/index/index-only, and CBO access/admission/cost/scalar
production files after their responsibilities move. No aliases, adapters,
callback owner, parallel publication route, evaluator, cache or worker remains.
The 4,096-unit original proof and 65,536-unit work budgets stay bounded.
Scan-free literal-only SELECT roots decline before scheduling-intent construction
and ParameterCount formatting; uncertain roots retain complete checking.

Validation adds actual integer BASE/INDEX/incomplete-observation controls,
ordinary and mixed real cached blocks, shared comparator operator/decimal/unknown
and reader seek/bloom controls. Retain all original provenance, diagnostic,
prepared, authorization, snapshot, cancellation, contention and no-partial-result
oracles. Run affected normal/race/public SQL/static checks. The mandatory
unmodified-main/final matched mixed campaign includes scan-free SELECT and
ordinary reader hot paths. Historical feature-baseline measurements remain
separate evidence and cannot close that performance gate.

## Avoid paying for losing candidates

The matched main/final campaign found repeatable planning overhead on ordered
sparse BASE scans. Allocation profiling attributes part of it to constructing
an executable candidate solely for pricing before discovering it cannot win.
The common selector first charges the existing per-index traversal and scores
the observed estimate with zero fixed predicate work. A valid lower bound that
cannot strictly beat BASE or the current best needs no candidate construction,
provided the existing comparison/input checks still prove it constructible.
Invalid scores retain the existing pricing/error path. Possible winners retain
actual topology pricing and a second call to the same scorer, preserving its
floating-point addition order, rejection rules and ties. Candidate state no
longer copies the estimate or BASE work already owned by its observation.

Construction is prepaid only when performed. An incomplete attempted price
still discards optional enumeration; saved work may allow a later winner within
the same fixed budget. Cancellation is checked around pricing and before
enumeration completes and takes precedence over optional errors. Ordinary
resolution, lifetime discovery and final publication checks remain unchanged.

The final-reader equality proof retains bounded shape/size checks and protobuf
equality for every non-statistical field. It compares shallow expression and
function copies recursively, ignoring only Ndv/Selectivity, without cloning
two complete trees or mutating their sources. The shared scalar matcher selects
its raw operator at compilation. The reader likewise selects its minimal block
closure whenever no early stop or bloom work is possible. Sorted stop/seek,
decimal carriers, unknown results, mixed semantics and bloom errors keep their
existing owners. This optimization was reviewed by gpt-6.1-terra/max before
implementation and requires renewed normal/race/public/static and matched
main/final performance evidence.

## Preserve regular-index errors and remove unused reader state

Regular index construction returns errors through the existing planner chain.
Ordinary mandatory resolution and column-map failures are returned instead of
being converted to panics. The index-merge selector and builder return errors
to the same caller; they check cancellation on a declined merge and before
publishing branches. A recursive rewrite updates a parent child ID only after
the child succeeds. Optional ordinary binding errors still decline that access,
but cancellation does not become a silent decline.

For ordinary access, the request cause supplies an otherwise absent error or
replaces its exact context cancellation sentinel. A distinct substantive error
is preserved. If an error wraps that sentinel and the request has a distinct
cause, the returned error retains both. Necessary-candidate construction keeps
its existing cancellation precedence. No recovery shim or builder error state
is introduced. Before publication, failures do not publish scans, caller column
maps or hints. A consumed ordinary binding tag remains private to the failed
builder; after the commit point the existing final cancellation check prevents
the private plan from being returned or cached.

Remove the unused production index-only wrapper; tests register their indexes
and drive the real selector, or exercise its actual matcher and mapping owners.
Remove the reader's empty-filter state, which has no real leaf producer: empty
or unsupported OR input falls back to the existing non-pruning filter. This
also preserves conservative handling for compound cluster keys and independent
membership hints. Temporal candidate emptiness remains type-owned and is widened
before the reader sees a pruning bound.

Validate cancellation and deadlines after successful relation resolution,
later merge-branch failure, concurrent substantive errors and the frontend's
existing error classification. Cover parent/child publication boundaries and
the reader's PK and cluster-key fallback and cleanup. Rerun affected planner,
reader, frontend and public SQL checks, static analysis and matched integrated
performance controls on the final source.
## Final ownership of access proof and execution requirements

PR #32341 continues the complete refactor of #31944 and #32318. Its planner,
frontend, storage, reader and scheduler boundaries, serialized plan field and
execution/retry lifetime trigger mandatory design-first review. This section
supersedes conflicting original-analysis, candidate and reader decisions above.
It was developed against `9614729778b36950cff38821d0a228899850cd39`; the design
review must identify the exact revision containing this amendment before the
implementation is delivered.

### Problem and invariant

A separate temporal dependency walk and exclusion proof repeat analysis of the
original bound scan. A BASE choice must still be replanned when another execution
could choose an INDEX, while ordinary unrelated and prepared plans must keep
their existing reuse policy. Unknown evidence is not a proof of no dependency.

The current candidate marker conflates two independent facts: an early storage
filter is safe, and the physical access was admitted under a particular
execution. A folded or removed expression does not cancel the second fact. The
existing SQL residual remains authoritative. Authorization, tenant, snapshot,
error ordering, deadlines, resource ownership and retry publication are not
weakened. Type inversion retains DATETIME/TIMESTAMP semantics, fixed-offset
conversion, precision, NULL, sentinels and out-of-domain tails.

### Original-plan owner

Use the bounded unary path and original proof already owned by QueryBuilder for
both positive optional access admission and plan lifetime. Publish only the
existing `ExecutionDependentAccess` query fact, independently of the optional
borrow and BASE/INDEX winner; never clear a prior true value. Keep the current
4,096-unit, 64-expression and depth-64 positive proof. Run the potentially
recursive safety check only after a fitting single original proof.

For a live context, a fitting non-single proof excludes the current candidate.
For a fitting single proof, inspect actual column and bound types, including a
typed NULL, then determine the old leading-part exclusion and any-index-part
membership in one bounded traversal. Preserve regular-index classification,
aliases and all invalid metadata semantics. The leading exclusion spends the
positive proof's remaining budget; membership has at most 4,096 units. A
complete exclusion or complete membership miss means no new dependency; a
witness or unknown means dependency. A malformed earlier index must not hide a
later suffix witness.

If the positive proof is incomplete, the same owner uses a bounded recursive
fallback over original FilterList roots, function arguments, lists and retained
literal sources. Recognize oriented comparison, BETWEEN/IN_RANGE and IN,
including unproved T_any/parameter sources. Unsupported expression variants,
cycles, omitted suffixes, malformed or oversized names, depth or work exhaustion
are UNKNOWN, never a negative proof. Use at most 4,096 charged units, depth 64,
64 index definitions, 16 parts per index and 256 bytes per inspected name or
algorithm; charge before traversing children and names. A completed inspection
showing no matching temporal indexed column is a negative proof. A small
unrelated integer query on a table with a temporal index remains reusable. No
bitset, second full-plan walk, callback registry or retained dependency object
is added. If context is nil or stopped, clear positive admission and do at most
this same bounded, nonblocking classification; an actual witness or unknown is
dependent. The owner does not replace cancellation or substantive errors. The
bound-only view-planning route can return without the ordinary final cancel
checkpoint, so it must not infer absence from a stopped proof.

Delete `potentialContextDependentIndexPath`, `contextDependentIndexPair` and
`originalIndexPathExcludesIndex` after their behavioral assertions migrate to the
original owner and public-plan tests. For complete, live, in-budget inputs the
new result equals the old `P && !E`; for unsupported inputs only conservative
false-to-true lifetime changes are allowed.

### Independent physical facts

`Expr.CandidateFilter` means an independently derived necessary storage
condition. Its producer proves that early evaluation and skipping rejected row
and ancestor work preserve SQL results and diagnostics, and that the hint is
safe to evaluate; the exact original predicate remains as a residual. It grants
no authorization, local placement, captured execution, raw-key compatibility
or ordered-seek proof. The existing mixed-time producer has the required
original/current-path proofs. Ordinary and composite predicates stay unmarked
unless their real producers establish this contract. Mark the root of an
independent `BlockFilterList` entry, and retain the proof through exact copies,
folding, remapping, statistics recomputation and transport; discard a hint when
its equivalence or owning scan cannot be preserved. Marked and unmarked
block filters must not deduplicate one another. Preserve all marked independent
hints and send them to the ordinary storage-filter route only after its existing
eligibility checks and the reader/PK admission below. No new producer or second
storage path is introduced.

Add `IndexScanInfo.requires_captured_execution` at tag 7. It says the physical
index access was admitted under the captured `ExecutionIntent` and may compile
only after the existing exact process/clock/zone, fixed-offset, non-offloaded,
pinned-local scheduler checks succeed. It is a restriction, never authorization.
Set it only on publication after the real base/hidden relation pair, borrow,
root and reentry proofs succeed; ordinary integer cost observation does not set
it. Move frontend validation and unscoped rebuild checks, compiler setup,
preflight, scheduling, retry and Reset from candidate provenance to this fact.
Malformed flagged nodes must be rejected even when not a physical index scan.
Carry one compile-owned sticky copy across folding and retry, clear it on pool
reset, and delete the old candidate-topology field and marker-based access
helpers rather than retaining compatibility routes. Copy the new scan field in
`DeepCopyNode` and regenerate its protobuf marshal, unmarshal and descriptors.
`ForceOneCN` alone cannot prove the current coordinator was selected.

The input/intent, one-shot root claim, ordinary resolver output, transaction
stamp and actual relation-pair check remain: they represent different existing
attempt, authorization and lifecycle boundaries, not another temporal cache.
No table lookup, subscription, relation cache, scheduler, background worker or
execution pipeline is added. Existing substantive errors and cancellation
precedence remain owned by their current operation. Failed optional observation
discards partial evidence; failed preflight publishes neither message board nor
deferred transaction offset. A drift retry rebuilds from the original statement
without the scoped planning input and reauthorizes the replacement.

### Reader and encoded-key admission

Unmarked OR keeps its current metadata-load/error behavior. A marked condition
may prune earlier only with an independently valid storage-domain proof. Missing
or unknown arms must retain data and prevent an unsafe early stop; malformed OR
declines optional compilation. Ordered seeks require a compatible, known
physical domain and compatible bounds for every relevant block; an unknown
lower proof starts at zero, and an unknown upper proof retains the lower seek.
Unify the two existing seek implementations in the reader with an explicit
known-proof requirement; ordinary lower seeks retain their established work and
ordering. Unsupported or malformed marked leaves decline optional pruning.

Keep this admission narrow and concrete: the existing temporal producer uses
OR, direct scalar equality/BETWEEN and serialized prefix equality/BETWEEN.
Those forms may also operate on proved ordinary integer and decimal domains;
use existing scale-aware zone-map comparisons. Float, unsupported/opaque types,
marked IN/vector leaves and unproved forms fail open until an actual producer
and a corresponding safe physical contract exist. Do not add a speculative
vector validation framework. Byte-domain bounds that exceed the stored 30-byte
zone-map prefix and start with 30 `0xff` bytes cannot authorize prefix
exclusion: a real matching key can otherwise be excluded by the truncated
maximum. The current serialized temporal endpoints are at most ten bytes and
start with their type code, so this restriction does not suppress that path.
Existing Unicode collation and serialized-key admission stay in force.

PK/memtable filtering compares raw payloads and needs a stronger encoding proof
than zone-map comparison. Propagate candidate provenance through the existing
sort-key filter builder; for a marked scalar use the existing exact raw-runtime
filter pair check, including decimal-scale and actual bound type, plus ordered
and prefix safety where applicable. Unsupported marked filters decline; AND may
keep independently safe arms and OR must fail open. The normal unmarked PK path
and temporary-vector ownership remain unchanged.

### Compatibility, alternatives and validation

Existing table/index storage, SQL syntax and configuration do not change.
Stopped 4.2 upgrade is the supported contract; mixed-version execution and
downgrade are out of scope. Saved query-result metadata does persist plan bytes
used for privilege checks. A new optional field must default safely when absent;
validate a plan encoded using the actual v4.2.8 schema through the saved-plan
reader and authorization consumer, including an index-scan node. This fixture
is format evidence, not a claim that a full stopped upgrade has run.

Retaining the coupled marker was rejected because reader proof, captured
execution and ordered encoding are independent. Replanning all SELECTs was
rejected because unrelated plans lose reuse. DuckDB's local optional filter and
ART access provide a useful residual-predicate precedent, but do not replace
MO's hidden relation authorization, transaction identity, distributed
placement, covering projection and retry. A new dependency framework, generic
runtime hidden-table route, artificial producer or blanket broadening of OR
pruning would add cost or change errors without the needed proof.

Validate actual owner behavior: bounded/canceled original proof, both operand
orientations, typed NULL, aliases, leading and suffix indexes, missing metadata,
unproved and oversized inputs, and ordinary/prepared reuse; full-result BASE to
INDEX and INDEX to BASE transitions. Exercise proof preservation through stats,
dedup in both orders, folds, copying and wire transport, plus an ordinary
integer/decimal synthetic marked reader control. Check no-intent, folded/removed
hint, invalid flagged node, wrong CN, clock/zone drift, reentry, retry, Reset,
cleanup and authorization. Include decimal scale mismatch, unknown block,
truncated `0xff` prefix, NaN/unsupported domain and an unmarked OR injected
metadata error. Use real reader and saved-result entry points where applicable.

Run affected normal and race owner/consumer tests, relevant public SQL and BVT,
repository formatting and incremental static analysis after the requested
rebase. Measure matched old/new executables with identical main base, native
libraries and fixtures, A/A controls and interleaved rounds: selective and dense
temporal scans, ordinary integer, prepared and scan-free paths, p50/p95,
allocations and scanned rows/blocks. Explain reproducible regressions relative
to measured variability. Do not claim unexecuted upgrade, mixed-version or
skipped CI coverage.
