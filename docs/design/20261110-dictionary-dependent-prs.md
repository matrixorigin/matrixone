# Dictionary dependent PR delivery design

Owner: #32379. Proposed follow-ups: #32385, #32386, #32387, #32388.
Status: combined consumer and scan/filter producer design approved by the requester
on 2026-11-10; implementation in progress. #32405 keeps its accepted boundary-only scope.

## Baseline and invariant

Build on #32405's producer-owned DictionaryOutputState. Ordinary OperatorBase
and vm.Exec retain main's layout and path. Producers retire their previous
private output at Call entry and normalize their final projected output before
publication when the destination requires flat data. Reset/free/cancellation
retain that ownership contract. No wrapper opcode, global registry or cache.

Logical values, row order, repeated rows, NULL/grouping and string/prepared
metadata must match ordinary execution. Selections are immutable, accounted,
bounded and compose to one base. The full retained backing remains charged
until its last owner releases it. Allocation refusal uses the existing accounted
fallback; query errors and cancellation retain their meaning.

Unknown consumer capability stays flat. An operator may declare dictionary
input only after every reachable physical/native access is either converted or
explicitly normalized at its owning boundary. A dictionary-aware forwarder must
also declare possible dictionary output so an unsupported next edge is safe.

## Combined PR, part A: consumer migration (#32385)

Depends on #32405. Implement this consumer part before enabling part B.
The same implementation PR covers #32385 and #32386; no producer is enabled
until its complete consumer/normalization path is covered.

- Resolve fixed/varlen read views once per input batch. Preserve existing direct
  flat/constant loops and scalar broadcast; selected loops index the immutable
  base and use logical validity/metadata.
- Convert scalar expression parameter wrappers and the predicate/projection
  kernels needed by the enabled paths. Unconverted functions/native calls use
  explicit executor-owned normalization with the existing pool/account.
- Convert key preparation and hash-map probe readers, including selected keys,
  constants, NULLs and composite keys. Hash row ordinals remain logical.
- Convert vector/batch gather/union and join payload copying to read selected
  source rows directly into existing output. Never flatten a complete probe
  input merely to copy a few matched rows.
- Convert aggregate input reads used by these paths; aggregate state/output
  remains ordinary owned storage. Unconverted aggregate contracts stay flat.
- Make shuffle a safe consumer: route selected logical keys, preserve whole
  batch forwarding, and copy selected payload directly into existing flat
  partitions on mixed routes. This does not yet introduce dictionary partition
  production. Forwarding projection/shuffle output follows the boundary owner.
- Gate join capability by proven mode/type/residual support. Unsupported modes
  use the established flat boundary; no blanket declaration for all joins.

Before coding, inventory physical access by expression, key, payload, aggregate
and lifecycle closure in a versioned consumer matrix. Each capability declaration
must name its converted functions and residual normalization points.

Validation: tiny flat/constant/dictionary cases with repeated/reordered/empty
rows, NULL/grouping, inline/area strings and metadata; compare to independently
specified logical outputs. Exercise selected keys and payload separately,
function fallback, failed copy, reset/reuse, stopped cancellation and actual
join/aggregate callbacks. Run affected owning packages; race only shared/async
closures. The completed combined PR uses part B's Q12 speedup and all-query regression gate;
do not run a separate neutral acceptance campaign between these commits.

## Combined PR, part B: scan/filter/projection selections (#32386)

Shares one PR with A. Primary target Q12. Recheck the current physical plan before
implementation and qualification. The retained current plan is:

    LINEITEM reader with pushed predicates -> shuffle -> hash build
    ORDERS reader -> shuffle -> hash join probe -> group

There is no standalone Q12 filter/projection-to-probe chain. Implementing only
that hypothetical path would not establish Q12 activation.

Add minimal explicit selected-output capability to the existing ReadWithFilter
request/result contract. The request is made only for a supported useful consumer
edge. Readers that cannot safely retain their decoded backing return ordinary
owned output. Reuse the selection already computed by predicate evaluation;
retain decoded payload rather than gathering it into an intermediate flat batch.
Do not introduce another predicate engine or read every payload twice.

TableScan owns the generation-local producer state, base snapshots and one
shared row mapping across columns. A capability/ownership/budget refusal retains
the ordinary path. Direct projections preserve the selected view; computed
columns may be owned flat vectors in the same logical batch. Grouping-set and
other unconverted paths stay ordinary.

The useful Q12 path is selected reader output through dictionary-aware shuffle
and HashBuild consumers. Mixed-route shuffle may still produce flat partitions;
whole-batch forwarding can keep the view. HashBuild performs its existing
necessary retained copy/key preparation directly from logical selected rows.
The intended saving is the earlier reader/filter gather, not the required owned
join storage. Producer admission must avoid building a view solely to flatten it
immediately. Expose the needed destination decision through producer-owned state,
without changing ordinary OperatorBase or vm.Exec.

Work proof: count originated selected rows, avoided reader/filter gather bytes,
whole-batch forwarding, downstream materialization bytes, retained physical peak
and fallback reasons. Require real Q12 activation and attributable work removal,
then stable SF10/SF100 memory latency benefit plus all-query/Total checks. If the
latest main has already removed this gather, return for rescoping before coding
another mechanism or selecting a different target after measuring it.

## PR C: shuffle and join output views (#32387)

Depends on A/B's stable view and capability contracts; primary target Q9.
Can develop as a separate stacked draft while B is reviewed.

Freeze an input snapshot once and produce accounted partition selections over
it. Preserve the existing whole-batch move path. Each receiver owns its header;
bounded queues retain references rather than mutable source headers. Producer
reuse performs COW and never waits for an all-consumer acknowledgement barrier.

Start join output views with residual-free unique INNER and probe-only SEMI/ANTI
cases. Compose logical selections onto one base. Gather build payload where the
result cannot be represented by a single base; keep other join outputs flat.
Do not enable outer, mark, ASOF, residual or nonunique output views without a
separate closed ownership/semantic proof.

Validate queue capacity, early consumer stop, partial send, cancellation,
duplicate cleanup, source reuse and final backing release using deterministic
barriers. Count bytes/copies avoided and retained peak. Confirm Q9 activation and
SF10/SF100 memory benefit; run the all-query/Total memory/disk checks.

## PR D: reader early filtering and late payload (#32388)

Depends on A/B; does not inherently depend on C. Reader and shuffle/join work can
proceed in separate files/branches once shared contracts are fixed. Target Q19.

B establishes selected reader output. D moves ready membership/predicate work
to early columns and delays remaining payload work using existing reader paths.
Preserve original-row mapping, residual evaluation/error order and snapshot
identity. Retain safe decoded backing or materialize required payload once.
Do not change codecs, cache replacement/tiering, SQL types or join placement.
Block codecs may still decode each touched block; do not equate reduced gathers
with reduced decoding or I/O.

Validate reordered selections, residual errors, row/block/runtime-filter
composition, held/evicted backing, pressure and refusal fallback. Count decoded
bytes, gathered bytes, native reads and physical pins independently. Confirm Q19
activation/benefit at both scales and all-query/Total behavior.

## Delivery and future verification protocol

Submit one templated draft for combined A/B after real code and cheapest functional
proof exist; C and D remain separate PRs. Do not submit empty placeholders. Draft development may stack;
production enablement and final isolated qualification wait for prerequisites
to merge. #32405 merged as main `00da05219f` on 2026-11-10. Combined A/B was rebased
onto that main without inherited prerequisite commits.

The requester replaced the repeated-process protocol for future PRs:

- One process per build/engine, scale and memory/cache configuration.
- Load/open data once; execute Q1-Q22 six complete consecutive suites.
- Exclude suite1 from reported timing, profiling and accounting.
- Profile/account suites2-6 and retain all five warm results, per query and Total.
- No reload/restart between those suites and no additional five-process-pair
  layer. The five repeats describe within-process variation, not independent
  process samples; do not reuse the old independent-pair confidence claim.
- Preserve matched resource budgets, cold exclusion, no-spill/swap/OOM rules,
  software pins and no benchmark-time dataset/result verification.
- Use local SF10 (memory and disk-backed) and local SF100 (disk-backed), per
  the requester’s latest instruction. Do not use server129. Separate timing/work evidence from changes
  in warm cache residency and disclose profiling overhead consistently.

For producer PRs, require the predeclared target to show repeatable memory-profile
benefit at both scales with attributable copy/work reduction. Publish all22 and
Total warm results, CPU/instructions, I/O and retained memory. Investigate slower
queries before readiness; source changes require fresh affected evidence, not
numerical reruns until a result passes. No process-level statistical certainty
is inferred from one process. Do not introduce an unapproved noise tolerance.

Append results as issue/PR comments. Collapse genuinely superseded result tables
after publishing replacements; preserve source pins and raw evidence. Keep raw
datasets, and retain loaded seeds while active comparisons need them. Q15/Q20
rewrites, additional decimal SIMD, codecs and SF1000 remain outside this wave.

## Approved scope revision: shared scalar support and useful producer paths

The requester approved the revised combined #32385/#32386 coding plan after
reviewing the first SF10 regressions. Broaden comparisons/BETWEEN/Boolean/null,
fixed/string membership, ordinary numeric arithmetic/casts and shared string
kernels through existing read views/templates. Audit every specialized branch;
unknown and physical-only paths remain explicitly flat. Preserve flat/SIMD paths.

Use the selected required-column approach: an execution-lane-local input frame
owns fixed column slots and lazily bulk-gathers a requested contiguous operand
once per batch/selection generation. Sibling unconditional expressions share it.
Masked evaluation retains existing branch-local compaction and never shares
across masks. Reused batch addresses start a fresh generation. Free all owned
scratch before account terminal; never free borrowed source headers. No new
OLAP cache tier, cross-query cache, SQL type, wire format or configuration.

Complete ordinary join-shuffle selected INPUT consumption, retaining owned flat
partition outputs and existing bounded queue/handoff/cancel protocols. Preserve
accounting through pending writes and cleanup. Prove Q1/Q12 actual activation,
not only synthetic owned-complete-key shuffle support or faster wall times.
Skip absent metadata scans, preserve logical NULL/provenance and transactional
partial-column rollback. PR C/#32387 output views and PR D/#32388 earlier filtering
and late payload remain separate with the scope recorded above.

Qualification remains local SF10 memory/disk and SF100 disk, all32 CPUs, matched
cache/budgets, one cold plus five profiled warm full22 suites per process/config.
No spills/swap/OOM, concurrent builds/profile analysis or benchmark result/data
verification. Report per-query wall/CPU/instructions/allocation/I/O/retention and
Total. The earlier SF100 external-CPU exclusion is not acceptance evidence.
If a new regression appears, investigate read-only and present its cause and
proposed correction for approval BEFORE changing code. Publish results only as
comments; keep draft until attributable target gains and regression gates pass.

## Approved Q12 regression correction

The requester approved three corrections after reviewing the failed SF10
qualification: restore whole-bucket forwarding of existing selected input under
its admitted destination/lifetime contract; expose only current-predicate column
views over original surviving-row indices, caching immutable backing once per
source column for that evaluation; resolve typed primitive comparison views once
outside the row loop while preserving wrapper conversions when required.

The selector publishes final output columns only after the ordered predicate
chain finishes. Unknown executors retain the complete owned fallback. Partial
construction and optional account-capacity refusal clean all temporary headers
before the existing owned gather. The existing pool/handoff/cancel protocol and
mixed-partition flat output remain unchanged; this does not enable partition
view production. Tests distinguish empty EOF progress batches from forwarded
data, assert reference-only views/backing reuse, preserve masked/null/NaN behavior,
and exercise admitted/declined synchronous and asynchronous forwarding.

Q1 partial-aggregation admission is a separately identified gap and is not changed
by this three-part correction. No SQL precision, cache algorithm or join plan is
changed. Requalify all22 locally; a new regression requires read-only diagnosis
and requester approval before any further product correction.

## Approved ownership correction after retrospective

The requester approved ownership-first correction. Selected reader publication
must retain its original caller-owned decode vectors instead of freeing them.
The scan retires its published selection before restoring this fixed workspace.
Promoted MPool backing may return to its ORIGINAL vector without copying only
through a successful last-reference transition. Retain/disown race via CAS;
retained views force existing COW/replacement. Foreign immutable backing is never
reclaimed. Original-owner permission is revoked on release/COW, including header
reuse, and allocation provenance stays unchanged until physical free.

The new reader request workspace is local to one prepared scan and has at most
one original vector per schema column. Error/reset/free clean both workspace and
published output before account terminal. Already leased immutable inputs keep
their existing retained-owner path; this does not expose sealed ObjectIO cache
bytes through a new writable alias or add a cache tier.

Whole-bucket routing keeps forwarding through the existing producer boundary.
When the destination requires flat input, Shuffle opts into one reusable owned
normalization bank. BeginReusableCall ends the prior borrow and clears data;
ordinary BeginCall and terminal vm.Reset/vm.Free retain their releasing behavior.
Incompatible schemas, constants and auxiliary buffers use the existing owned
copy. A separately retained snapshot prevents reuse and remains immutable.

Proof: same native reader allocation across repeated publications; held-view COW;
no foreign/dead-owner reclamation; retain/disown race with exactly one effective
release authority; reusable flat-boundary pointer stability and terminal account
closure. Qualify TOTAL cost (wall/CPU/instructions, allocation/zeroing, bytes,
retained memory), not just eliminated compaction copies. All-query local protocol
and approval-before-correction on a new regression remain mandatory.

### Completion of approved private metadata reuse

The first ownership-only screen remained slower, so native-buffer pointer reuse
was not treated as full qualification. Complete the already approved metadata
step before another full campaign: reader and predicate owners keep fixed,
per-column EMPTY publication-header slots after the prior borrow ends. Vector
constructors take only exclusive, empty compatible slots; ordinary constructors
retain their original API/behavior. Consumed headers are cleaned on construction
failure; unconsumed slots remain caller-owned. Retained sibling snapshots are
never reused or mutated. Namespace/schema changes and terminal Free release slots.
Owned flat fallbacks are not retained as an extra header-cache payload bank.
No global pool/cache or new tier is introduced. Selected body inputs still share
one immutable backing; final publication remains transactional.

## Approved selected routing and bulk gather correction

After reviewing the completed ownership/header screen, the requester approved
restoring sorted-range endpoint checks and typed selected fixed-width gathers.
A dictionary key may use the original endpoint proof only when its existing
logical sorted flag is true, its length is positive and it has no NULLs. Endpoint
values resolve through the immutable mapping/constant base; partition functions
and range boundaries are unchanged. NULL or unproven ordering keeps enumeration.

Fixed-width selected appends retain the established preflight, logical length,
grouping and metadata publication. Dispatch on physical width once, then copy
typed selected values with constant and NULL decisions outside the non-null loop.
Respect offsets, flags and existing destination rows; restrict byte views to
logical payload lengths so allocator padding never becomes an extra value.
Varlen and unrecognized widths keep the previous path. No arithmetic, SQL type,
cache, queue or ownership changes are introduced. Validate all supported widths,
NULL/flags/offset cases, endpoint bypass and cancellation before the all22 gate.

## Approved retained cache-source boundary

The requester approved continuing with immutable cache-backed column views and
owned workspace fallback. Only ObjectIO's private validated immutable marker may
enter the initial borrow path; fixed/unmarked/version-ineligible entries keep
owned materialization. Keep reusable owned destination capacity instead of
replacing it to borrow. Borrow only full non-appendable columns before selection,
with no tombstones, row-id request, visibility filtering or Top-K. Other cases
preserve the existing reader path. No cache policy/tier or serialization change.

ObjectIO keeps the decoder header scoped, retains the cache data owner and
creates an immutable DictionaryBase with full physical pin admission. Only a
dictionary header escapes. The source's logical metadata is private; one
identity mapping is shared across borrowed columns in that load. Cache-owner and
pin references survive IOVector release and end at the final view. Unconverted
consumers use existing owned normalization/COW. Optional pin/map refusal declines
to owned copy while the same IOVector is still pinned, without another read.

Deterministic proof covers cache-payload address identity, no public Bytes clone,
last-view lifetime, COW cache isolation, pin-pressure refusal and an actual
no-tombstone reader callback receiving a dictionary before predicate evaluation.
Existing tombstone/late/visibility/Top-K cases remain fallback controls. Requalify
all22 locally and present any new regression before changing product code again.

## Approved correction after retained-cache qualification

The requester approved fixing the measured SF10 regressions on 2026-11-11.
Keep all other PR scopes above. No cache algorithm/tier or SQL typing change.

The cache reader now installs privately copied metadata and immutable counted
payload leases into its existing empty flat workspace. This removes the
intermediate identity row mapping and dictionary. Full cache capacity is
reserved before publication; the lease owns both cache retention and that pin.
The tracked flat header follows existing read-only/COW rules; no unowned legacy
alias escapes. Selected publication freezes this source and constructs the final
surviving-row map. The pin is shared within the same statement account, rather
than charged again for each snapshot. Original source headers are retired by the
existing reader workspace boundary; downstream immutable references keep their
own lifetime. Unmarked cache entries and reusable owned capacity keep copying.

A per-column snapshot slot and one row-map slot are owned by each prepared
reader/filter execution lane. A final-reference callback returns cleared
snapshot metadata (without payload) or bounded row-map scratch. Live borrowers
prevent reuse. Each slot holds at most one spare; concurrent final releases
that lose admission free their objects. Free atomically seals the slot, releases
its spare and makes later callbacks free their objects without reopening the
old generation. No global pool, background worker or waiting is introduced.

Non-null/non-grouping fixed selected keys resolve type, width and physical row
mapping once, then use typed loops into existing iterator scratch. Floats retain
the canonical codec, and nullable, grouping and constant inputs retain the
audited generic path. Flat-only scan consumers already reject selected output
at edge preparation, so they retain their direct owned path instead of creating
views for immediate normalization; this existing admission must remain intact.

Proof reuses the cached lifetime/COW/pin-refusal fixture and actual reader test,
adds held-borrower/retirement/close-race and row-map reuse controls, and reuses
key equality/float/NULL/grouping oracles. Run owning checks, targeted races, SQL
and pre-push SCA, then local Q1-Q22 six-suite qualification with accounting.
Any new regression again stops product edits for diagnosis and requester review.


## Approved owned-vector fast-path correction

After the `31c018976f` qualification, the requester approved guarding workspace
reclamation in `MaterializeOwned`. Invoke reclamation only for a live data/area
lease. Owned flat scalar destinations retain the existing no-copy return;
dictionary, borrowed validity and payload COW still follow their original paths.
This removes an observed new scalar-join cost, not an asserted complete cause of
Q21's regression. Reuse the ownership/COW controls, then requalify all22 locally.


## Approved direct predicate-row compaction

The requester approved direct compaction after `af9d97b151` qualification.
Reserve reusable original-row scratch once for the first nonconstant predicate;
write survivors there directly. Subsequent predicates compact those original
positions in place, with the write index bounded by the current read index.
Remove the temporary match list and the separate copy/composition traversal.
Keep ordered expression evaluation, identity/empty/NULL/constant semantics,
dictionary Boolean physical-row mapping and unknown-executor fallback unchanged.
This targets the line-attributed selection loop; it does not assert that all
Q20 or whole-suite regressions share that cause. Validate focused semantic
controls, the owning expression package, SQL and static checks, then the same
local six-suite Q1-Q22 qualification; regressions again require requester review.


## Approved shuffle admission correction — local only until accepted

The requester authorized fixing the verified build-then-flatten boundary and
explicitly prohibited further pushes until performance is fixed. Ordinary
shuffle now propagates its downstream output admission to its child. Separate
mechanical support from negotiated admission so representation discovery cannot
become circular or remain stuck false. Owned complete-key routing continues to
consume selected rows into its existing owned partitions. Keep normalization as
a safety fallback; do not change routing, buffering, lifetime or cancellation.

Reuse the existing synchronous/asynchronous hash/range routing fixture to prove
accepting -> flat -> accepting boundary reconfiguration, source admission,
unchanged results/accounting and fallback representation. Run owning checks,
then a local-only clean build and the same Q1-Q22 six-suite protocol against the
frozen matching main. No repeated full pre-push gate or remote push while this
local performance revision is unaccepted. Any remaining regression gets a
concrete diagnosis/proposal before further product changes.


## Approved selected H8 typed-store correction

The requester reviewed the detailed H8 design and authorized implementation.
Eligible fixed-width 1/2/4/8-byte nonconstant logical NULL-free dictionary keys
reuse the existing fillKeys native typed-store primitive and batched offset
update. Resolve physical backing and logical selection outside the row loop;
remove the selected generic per-value slice-copy helper. Keep canonical float,
nullable, constant-backed and varlen fallback behavior, owned key scratch and
existing flat loops/single-full-width bulk copy. No new unsafe import/function,
allocation, lease, materialization, wire or key-layout change is introduced.

Validate existing key equivalence/NULL/float/constant controls plus two-byte
composite keys, nonzero offsets, repeated/reordered selections and NULL-domain
prefixes. Inspect emitted selected loops for per-value memmove removal. Full
local Q1-Q22 timing and per-query resource acceptance remain mandatory; do not
infer no allocation/copy regression from suite totals or absent instrumentation.
Keep the entire correction local and do not push until all acceptance closes.


## Approved ownership-metadata correction — private predicate scratch

The requester authorized eliminating temporary ownership allocations while
keeping the no-push/all-query-resource gate. Private selected predicate inputs
now use execution-owned flat scratch, populated only for the current expression's
referenced columns. This removes per-predicate frozen bases, retained leases,
immutable row-map objects and temporary batch/header allocation. Existing native
flat kernels consume the same surviving rows in the same order. The extra narrow
column gathers must be counted, not assumed cheaper than retained views.

The frame alone owns its batch and column banks. Returned predicate batches are
borrowed only until the next predicate preparation. Boolean results are consumed
before reuse. Ordinary function parameters/column executors borrow arguments;
memo roots explicitly reset cached values before each Eval. Unknown executor
trees retain independent owned gather fallback; no unsupported-input error or
new public borrowed-vector/lease contract is introduced. Actual escaping scan
or filter outputs retain the existing immutable publication protocol.

On capacity refusal, release optional bank capacity and use the ordinary fresh
owned-gather fallback, so unused retained scratch cannot cause a new rejection.
Errors keep source values intact; End clears borrowed batch references and Free
releases every bank exactly once. Account/schema generation changes dispose old
scratch. Storage is bounded by prepared input width and batch/MPool limits; no
global pool, queue, worker, wait or native ownership change is introduced.

Reader publication/map/snapshot banks are initialized only after the downstream
edge actually admits selected output. Flat-only scans keep their previous path.
Held immutable sibling or output lifetime remains proved in vector ownership
tests; private predicate scratch now has an explicit borrowed/reuse oracle.
Validate original-row/error ordering, NULL/metadata, repeated memo roots,
capacity-refusal cleanup and private-bank reuse before local all22 timing and
per-query allocation/copy accounting. Keep regressions and coverage gaps blocking.


## Approved projection-aware column publication — local only

The requester approved classifying columns at the reader/projection handoff.
Prepared audited expression dependencies classify inputs as retained forwarded
outputs, owned computed-expression inputs, or predicate-only unused columns.
Forwarding takes priority when an input also occurs in a computation. Unknown
executors, absent projections and incompatible policy shapes preserve the old
all-retained behavior. Local-domain consumers keep all columns aligned; no
unused slot is cleared while whole-batch domain compaction can still occur.

Residual predicates see the complete original live-row input. After selection,
only escaping columns enter immutable retained construction. Proven unused early
columns clear their logical contents, while private unique nonconstant owned
computed inputs compact in place using existing Shrink metadata/COW protocols.
Borrowed, legacy-alias, dictionary and constant inputs keep safe retention/owned
fallback. Late payload materialization and appendable/tombstone/RowID/Top-K paths
remain unchanged. Selected-output length validation accepts an empty column only
when the proven local policy explicitly marks it unused. Projection receives
all of its dependencies; opaque clients keep the original full-schema contract.

Request-owned non-owning publication operands replace per-block temporary slice
allocations. Their capacity is bounded by scan width; every success/error path
clears source/destination references. Only Workspace owns displaced reader
headers; output owns the committed selected headers. Failed construction frees
its prefix and restores unconsumed spare headers. No ownership handle, cached
payload, goroutine, wire format or cache policy is added by this scratch.

Proof covers prepared-expression liveness, shared forward/computed precedence,
domain alignment, an actual persisted read dropping a predicate-only column,
retained immutable siblings, in-place owned header/buffer identity and failed
prefix cleanup. Existing fallback/visibility/Top-K controls remain. Run local
all22 timing and per-query accounting without pushing; any unresolved regression
or counter-coverage gap continues to block acceptance.


Single-batch dependency collection must match the actual executor binding.
ColumnExpressionExecutor treats every relation tag as input zero when exactly
one batch is supplied. The shared collector explicitly describes that mode and
ignores relation tags while still validating column positions. This keeps valid
scan projections/predicates from silently falling back because planner tags are
nonzero; it is not a multi-input dependency API. Nonzero-tag projection and
predicate controls prove this correspondence.


Column use must include the real consumer, not only the scan's local projection.
Runtime decision counters showed Q7's scan projection forwards all inputs and
therefore never requested the owned mode. Complete the approved handoff by
propagating a soft owned-input preference through the existing dictionary-boundary
configuration. Supported probe hash joins copy values into owned result buffers
and request owned input; projections with flat output request it as well. Existing
boundary state carries the preference through admitted edges without disabling
selected filtering. Unknown consumers retain default behavior. Scan forward uses
become owned under that preference; computed/unused classification is unchanged.
Borrowed/constant/non-private source fallback remains mechanically supported.

This is configuration metadata only, not a new vector kind, lifetime contract,
materialization requirement or allocation owner. Preference changes invalidate
cached column classification. Reconfiguration must clear stale preferences,
including when destination permission is removed. Routing, native kernels and
retaining consumers are unchanged. Validate propagation and existing selected
probe correctness before repeating local timing/resource evidence; no pushes.


Persistent predicate frames take original-row scratch from the existing selection
pool once per lane and return the caller's final slice at Free. End does not
expire the returned row mapping; its existing next-callback/reader-return borrow
contract remains intact. A nil/temporary frame leaves returned scratch caller-owned
and never returns it to the pool before consumption. Switching scratch owners or
account/schema generations retires the old pool ownership before reinitializing.
This avoids allocating a fresh full-block row map for every newly compiled scan;
it does not add a cache tier or change selection validation.


The admitted direct-column HashBuild path also requests owned ingress: its
existing budgeted CopyIntoBatches/spill-partition path already copies values into
owned storage. This completes the same real-consumer handoff on build roots,
including non-producer roots configured individually. It does not change dedup,
computed-key admission, retained batch format, spill, or build lifetime.


Honor the same eventual-column demand at the initial single pinned read, not
only after filtering. Owned/unused columns take the existing owned decoder path
instead of creating transient cache-owner/pin leases and then converting them.
Retained/unknown columns preserve admitted cache borrowing. A reusable request
mask aligns with the early-column list; no extra read or payload cache is added.
The nil-policy behavior remains unchanged; incompatible masks conservatively
copy owned. The real persisted-reader fixture verifies predicates still see full
input while owned/unused cache columns do not acquire borrowed backing. Restored
copying must remain measured against main; this is not a waived copy budget.


## Approved synchronous pinned-cache scope

The requester approved holding the existing IOVector pin through filtering and
owned publication instead of choosing between full-column copying and escaping
cache leases. Admit only a complete owned/unused policy, readonly Select callback,
non-appendable blocks, no tombstones/storage selection/RowID/Top-K/extra buffers.
Retaining/unknown consumers and unsupported storage modes keep existing paths.

ObjectIO owns reusable scoped decoder headers and binds only its private validated
immutable marker. Other entries materialize into the original owned destinations.
The loader keeps one IOVector pinned until consume returns; scoped aliases never
retain/release an additional cache owner or statement foreign pin. During consume,
the reader temporarily presents scoped inputs to the readonly predicate callback,
validates its original-row map, restores the original destination headers on all
terminal paths, and gathers only selected required columns into owned buffers.
Unused outputs clear their logical contents. Late payload reads keep the original
physical-row mapping. The consumer owns no scoped header or byte/metadata alias.

Scope cleanup runs before IOVector release, including decode/type errors, predicate
errors, malformed selection and partial owned-gather allocation failure. There is
no new goroutine, wait, lifetime handle or cached payload: retained metadata is
bounded by prepared column width and headers contain no live aliases after With.
Original owned destination capacity remains reusable. Standard expression/memo
argument borrows end before scope exit; actual escaping values are native-owned.

Tests prove payload address identity inside the scope, no public Bytes clone or
additional cache Retain, callback-error retirement, stable header reuse and owned
output survival after releasing the original cache owner. The existing persisted
fixture proves scoped input and owned selected varlen output. Requalify all22 and
per-query resources locally; keep uncovered native/runtime copies blocking, and
make no push while any query/resource regression remains.


## Approved flat-path repair — 2026-11-11

The requester approved repairing A/B's shared flat vector and key-encoding
costs from local c8c5ceab99. Preserve all preceding designs, including separate
C/D follow-ups. The membership extension and its verified Q12 activation stay
in A/B. Diagnostics with selection, accounting or normalization disabled are
not production remedies and cannot establish acceptance.

Restore flat Copy/UnionOne payload operands and typed stores; resolve selected
physical payload in private helpers after ordinary admission, without changing
logical metadata or ownership. Keep selected UnionMulti repetition out of the
flat broadcast frame. Restore the flat H8 encoder and route selected typed
stores separately; move selected string-key sizing out of the shared flat frame
without captured callbacks or new scratch. Retain bulk gather preflight, window
reuse, private backing reclamation, aliases/COW, NULL/grouping/string provenance,
canonical keys, error cleanup and full accounting. No schema, public interface,
cache policy, queue protocol, new pool or generic representation is approved.

Verify existing flat/constant/selected and unhappy-path fixtures, focused owning
packages and targeted static checks. Inspect generated flat code and component
controls against c8c5ceab99 and frozen main 553e16f52b; component gains never
replace TPC-H acceptance. Then use one fresh process per arm/configuration,
open once, run six full Q1-Q22 suites, discard cold and retain/profile/account
five warm suites. Local SF10 memory comes first, then SF10 disk and SF100 disk
only if the preceding gate clears; no server129. Preserve existing matched
schema/32 CPUs/cache/memory/spill settings, reject spill/swap/OOM, and perform
no benchmark-time data/schema/result verification. Every query and Total must
be reported. No unresolved per-query regression or missing copy coverage can
be waived by Total. New remedies outside this closure require design review.

Keep changes local and unpushed until accepted. Only then merge newest
authoritative main, rerun semantically affected evidence and complete the
mandatory self-review/pre-push gates. Results belong in comments, with genuinely
superseded tables collapsed after replacements are posted.


### Approved batch copy selection — 2026-11-12

The requester approved a narrow exception to the preceding public-interface
restriction: select stateless bulk-copy functions once per input generation.
`GetUnionMultiFunction` and `GetUnionBatchFunction` inspect the source batch;
only a complete flat/constant batch may bypass dictionary dispatch. Mixed,
missing or unknown sources retain the generic API. Returned functions capture
no vectors, buffers, leases or allocation accounts. All ordinary admission,
alias/COW, metadata, capacity and partial-failure rules remain in the existing
payload functions. No cache tier, pool or new representation is introduced.

LoopJoin binds probe copying on each new input assignment, including reused
headers. Build batches are immutable for their JoinMap lifetime; bind range
copying when the build batch changes and discard that identity on the next
probe generation, terminal probe, cancellation, failure, Reset and Free.
Continuation within the same input retains its binding. This change aims to
remove repeated representation dispatch from Q11's bulk-copy calls; inspect
actual call/frame overhead and measure all queries before claiming any gain.

The local all-query/resource gates above remain unchanged, as do the separate
C/D designs. No push is authorized before acceptance.


### Approved Boolean result validity correction — 2026-11-12

The requester approved boolean-validity-repair-design.md from the local evidence
wave after e910c58f15. Separate the current result's NULL proof from physical
input capability. Admit full, unmasked, accounted Boolean results for audited
AND/OR/XOR/NOT, homogeneous native numeric comparisons (including decimal), and
the existing capability-proven comparisons/BETWEEN/fixed-list membership. Check
actual logical argument NULLs every evaluation; grouping/prepared JSON, masks,
unknown overloads and unproved domains retain normal result admission. Do not
expand dictionary input declarations or change kernels, NULL synthesis, errors,
casts, planner types, cache tiers, allocation owners or designs C/D. Reuse the
existing FunctionResult[bool].PreExtendAndResetNonNull API.

Allocation diagnostics must distinguish MPool off-heap and Go-backed traffic.
GlobalStats covers off-heap only; report the two disjoint MPool domains and their
sum, never add that traffic to runtime Go Mallocs. The prior Q6 +160 native
requests exactly replaced 160 Go-backed literal requests; both complete-domain
counts were 926 and both byte counts 17,767,032. No new literal pool or accounting
bypass is warranted. Remaining Boolean bitmap growth in Q17/Q19 is real.

The six-suite, one-process-per-arm local protocol and all-query/resource gates
remain unchanged. Identical-executable control r30 itself flagged Q22 +5.4% and
measured Q7 +12.4%; it proves the diagnostic screen can misattribute timing
changes. It does not waive candidate slowdowns or authorize a gate/tolerance
change. Preserve all records, resolve timing comparability and missing copy
coverage before acceptance, and keep all changes unpushed meanwhile.


### Allocation profile attribution correction — 2026-11-12

The Boolean repair was implemented and locally validated as8565ccdaeb. r32
removed Q19's excess MPool requests. Q17's remaining64 validity allocations are
Decimal128 results, not Boolean results: the custom profiler merged generic
locations by erased function name/file/line and reused the first instantiation's
PC. A diagnostic-only key that includes frame.PC, resolved against its exact
executable in r35, establishes the Decimal128 carrier. Earlier Boolean-type
attribution for Q17 is invalidated. The proposed non-Boolean extension is in
local decimal128-validity-extension-design.md and still requires requester
approval. No new production behavior was added for it. Normal r32 and GC-off
r34 retain unresolved query latency/resource signals; neither is acceptance.


### Approved Decimal128 result validity extension — 2026-11-12

The requester approved local decimal128-validity-extension-design.md. The
current Q17 plan confirms cast(l_quantity AS DECIMAL128(38,2)) below a comparison
with 0.2 * avg(l_quantity). Extend only full, unmasked, accounted result admission
using the existing concrete FunctionResult[Decimal128] non-NULL preparation.
Admit the registered ordinary native decimal multiply overload with homogeneous
D64/D128 inputs, and implicit lossless D64->D128 casts with proven precision/
scale bounds, including the precision18 signed-carrier edge. The cast target is
a typed NULL metadata placeholder and is not a nullable value operand.

Check current logical value NULLs, grouping and prepared metadata each time;
unknown/narrowing/explicit/rounding casts, masks, nullable values and unproved
kernels keep ordinary admission. Preserve data capacity/account errors, overflow,
warnings, scalar broadcast, representation normalization and NULL reuse. Add no
public interface, planner/type change, arithmetic kernel, cache, literal pool or
new ownership mechanism. C/D designs and all local all-query/resource acceptance
requirements remain intact; the current PR stays unpushed until accepted.


### Approved scoped SUBSTRING predicate admission — 2026-11-12

The requester approved local scoped-substring-predicate-design.md after the
b703b87e8c validity repair. Admit only the existing CHAR/VARCHAR SUBSTRING
kernels (overloads0-3), with ordinary fixed scalar int64 offset/length executors,
to the synchronous predicate borrow allowlist. Keep the existing dictionary
capability, recursive argument proof and no-reset/no-free requirements. Unknown
shapes/overloads retain the owned bank. Do not change string kernels, Unicode
normalization, metadata semantics, public APIs, representation or accounting.

DictionarySelectionScope and the reader's existing IOVector pin own source
lifetime until End. SUBSTRING appends owned output bytes and metadata; fixed-list
IN retains no probe alias. Verify actual admission without a phone gather,
reordered/repeated/NULL domains, inline and area output, Unicode/binary metadata,
source reset, conservative fallback and capacity refusal/reclamation.

The r39 traces show similar frontend/join-map/shuffle waits in both arms; they
do not attribute r37's Q22 wall slowdown. The tiny r40 control proves the phone
gather is unnecessary, not its full latency cost. All normal all-query/resource
gates stay open, including copy-coverage gaps. Requalify locally with the existing
six-suite one-process/arm protocol and preserve all C/D content and old evidence.
Keep changes unpushed and the PR draft until acceptance is complete.
