# Existing owners for index planning and plan reuse

Status: design-first decision for the replacement of PR #31944 and issue
#32318. The user requested a complete transitive refactor in a new PR while
leaving #31944 unchanged. Source is `db225c815a0cfb38cde193e49fd1891ee737204a`;
the imported feature baseline is `eb5f432079e24748cfe3718a279eca12183474ff`,
on main `aeab8787f2b8acd4a80d47a2f23fd263d6f0ef2d`. The implementation PR will
link this reviewed design revision. This is an ownership refactor, not an
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
| Transaction/workspace/snapshot/visibility identity | `engine.TemporalRead` wraps `client.CatalogReadStamp`; frontend, view provider and disttae repeat comparisons | Bind the actual caller operator and sampled visibility to `client.CatalogReadStamp`; its matching methods implement shared identity comparisons. Delete `TemporalRead`. |
| Ordinary descriptor and borrowed relation came from one authorized lookup | Separate `BorrowingTableResolver`, frontend injection, `ResolveBaseForPlanning` and `ResolveIndexForPlanning` | Optional caller-owned `TableResolution` output on ordinary `CompilerContext.Resolve` and `ResolveIndexTableByRef`. Delete the richer interface, both side-entry methods and resolver injection. |
| Statement/context and placement still match | Temporal intent/input types and scheduling wrapper | One execution intent passed through the existing root builder/frontend/compile attempt, with placement checked by the existing scheduler. Delete the temporal scheduling type and temporal-only input route names. |
| Necessary predicate provenance | `Expr.TemporalCandidateFilter` throughout copying, pruning and compile | A generic `Expr.CandidateFilter` fact consumed by existing filter/index compilation. Preserve residuals, OR unknown behavior, bounded seek and physical-index placement checks. |
| Execution values affect access selection | `Query.TemporalIndexNeedsReplan` and scattered frontend decisions | Publish `Query.ExecutionDependentAccess`; the existing frontend cache owner derives a single reuse policy for ordinary generations, prepared logical plans and physical compiles. Delete the temporal policy helper and duplicated case-specific cache decisions. |

`TemporalCandidate`, comparison operators, bound evaluation, typed cost/range
analysis and temporal block observations remain justified specialization.
`BorrowedTable` is synchronous evidence owned by the ordinary transaction; it
does not become a relation cache or gain a Close operation.

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

## Candidate, execution and reuse

Candidate provenance is a generic property of a necessary storage predicate.
Keep its wire number and all copy/fold/reader/transport consumers consistent.
It is independent of authorization and independent of whether the scalar
endpoint is refreshed. Do not mark new SQL shapes or LIKE cases merely to
demonstrate another producer. Reader support already handles generic OR,
unknown and seek behavior and must retain their controls.

Execution intent remains an immutable value snapshot of current clock, zone,
parameter provenance, placement and offload facts. Its owner is the existing
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
