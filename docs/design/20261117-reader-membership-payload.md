# Reader membership before payload construction

Owner: #32388; parent: #32379. Design revision 1, approved by the requester on
2026-11-17 before implementation. Independent of #32387; baseline main 47957ef450.
Selected reader output, leases and normalization through #32492 already exist.
This revision adds earlier filtering, not a second dictionary-output API.

## Behavior and interfaces

Extend the existing internal ReaderFilterRequest with optional membership key
positions and a read-only selection callback. Nil preserves ordinary execution.
Admit compatible filtered probe scans using the existing equality-lineage,
independent-source, capacity and profitability proofs. Only predicates whose
skipped evaluation cannot suppress errors or side effects admit this ordering.
Do not change join placement, persisted annotations or native filtering kernels.

Preserve storage visibility, tombstones and native storage-filter handling. Read
membership keys, apply the ready domain, read remaining predicate columns for
survivors, and evaluate the existing ordered residual chain. Compose mappings
back to original physical rows, then construct remaining payload once using the
existing retained-view/owned-output policy. Empty results skip payload reads.
Keep intermediate mappings reader-local and publish final output once. Reuse
existing low-rejection bypass rather than introducing another threshold.

Existing exact-filter, Top-K and unsupported reader paths retain their proven
implementations. Compatible exact programs consume the membership survivors as
an initial physical-row selection through the existing exact-read function;
this does not replace their native predicate/payload implementation. Fall back through the established route without reevaluating
predicates, changing their errors, or rejecting valid input. Default output stays
flat; view publication still requires existing account/capability admission.

## Ownership and boundedness

The prepared scan owns the sequential request and callbacks. Borrowed selections
are consumed before the next callback; reader-local composition scratch is at
most one block of row indexes. A scoped loader owns cached input until evaluation
and any final gather finish. Escaping backing requires the existing physical pin
lease; refusal uses an accounted selected-row gather. Restore private reader
headers on success/error/cancel and release unpublished state on all paths.
Retain the existing allocation, queue, cache and terminal protocols. No new cache,
background worker, user-facing flag, codec or wire format.

## Validation and delivery

Extend existing reader and planner fixtures for partial/all/empty membership,
storage selections, tombstones and physical-row composition, NULLs, reordered
input views, ordered residual errors, unavailable/unprofitable domains, pin
refusal/eviction, cancellation and prepared reuse. Verify full account release.
Run owning packages, focused lifecycle race checks and a small SQL fixture.

Count decoded, gathered and retained bytes and physical I/O separately: fewer
selected rows do not imply fewer decoded bytes for a touched block. Require a
component work benefit and cheap all-match/bypass behavior before full timing.
Qualify this branch alone against fresh main using local SF10 memory/disk and
SF100 disk. Use one process/build/configuration, one open and six full Q1-Q22
suites; discard cold suite 1 and profile/account suites 2-6. Require attributable
Q19 benefit and no concealed per-query/Total regression; no spill/swap/OOM or
competing builds. Keep configured budgets matched. Benchmark scripts do not
verify input/results. Append results as comments, use the PR template, and keep
draft until acceptance. Preserve raw datasets/evidence; remove task-owned run
clones at terminal completion. Material design changes need requester review.

## Approved revision 2: admission and generic predicate-first repair

The requester approved this repair before implementation. Preserve direct
BuildPlan annotations, then rederive ordinary optimized SELECT domains after the
existing folding pass and final node numbering, using the same compiler-context
statistics and session/per-statement disabled-hint policy. Keep prepare and
correlation paths conservative. A domain-specific totality wrapper accepts valid
n-ary AND/OR only when every operand passes the existing leaf/error proof. Do not
rewrite Boolean IR, broaden the global Aggregate/LIMIT proof or evaluate new
expressions.

Generic filtered readers evaluate their original ordered predicates first, test
ready membership on survivors, compose both maps through physical storage rows
and tombstones, and construct remaining payload once. Read a membership-only key
for predicate survivors and reuse predicate keys. Preserve native exact/Top-K
paths, bounded scratch, capacity/pin refusal fallback and low-rejection bypass.
Record input rows/bytes once across the two stages. Require real optimized Q19
reachability and Q10 component work improvement before isolated all22 timing.
Keep CPU-only local SF10 memory/disk and SF100 disk with one process/open, six
suites (first excluded), all existing resource/acceptance gates and draft status
until actual acceptance. No new public flag, type, wire contract or cache layer.

## Approved revision 3: local stream and scoped cached predicates

The requester authorized the remaining native closure design before code.
Admit a connector source only when its exact existing local WaitRegister reaches
an independently scheduled Merge -> HashBuild receiver with that source as its
only prerequisite, one local receiver, no probe/filter wait, remote/lazy edge or
cycle. Preserve all original pure-source checks; do not whitelist arbitrary
connectors. Source EOF completes its domain before terminal cleanup. A bounded
receiver drains independently while the probe waits; cancellation/error/early
receiver stop retain the existing typed terminal/spool cleanup protocol.

Restore the baseline WithColumnsDataInto borrowed-flat predicate scope for the
generic scoped-owned case. Keep its IOVector live through predicates, membership
and the final owned gather; skip unused predicate copies and restore original
headers on every exit. Begin with identity storage rows, owned/unused output
columns and membership-only keys; overlap, tombstone and retained-output shapes
keep the established accounted route. No borrowed header or cached bytes escape,
and no cache policy/kernel changes. Prove native Q19 activation and Q10 copied
work/instructions before another full qualification.

## Approved revision 4: proven-total membership scheduling

Requester approved this design with “go on” before implementation. All earlier scope and validation contracts remain except the explicitly revised scheduling/copy admission below.

### Proposed implementation

1. Carry an explicit, optional total-predicate permission in the existing reader membership request. Validate the actual final scan predicates and generated runtime predicates against the same domain-specific totality proof at compilation; do not infer it from selectivity or a query name. Preserve the current conservative prepared/correlated/remote/unsupported refusals. Ordinary/custom reader callbacks default to predicate-first. No protobuf/wire or public SQL option.
2. For the existing identity/no-tombstone scoped-owned reader subset, load and lend cached key/predicate headers once through WithColumnsDataInto. Keep the existing IOVector alive until all callbacks and final owned gathering finish. Never copy the full key or predicate column merely to test membership. Use only existing bounded schema operands and dictionary selection scope.
3. On that proven-total path, test complete membership on the original key domain. Bind the existing synchronous read-only selection scope over membership survivors, then run the unchanged ordered predicate chain on those rows. Consume its borrowed map, compose to original physical rows once, retire temporary views, and gather only final escaping outputs. Discard unused inputs. A function lacking native dictionary execution uses its existing normalization on the narrowed row set; no new function/vector kernel.
4. Empty/all membership, NULL keys and multiple domains preserve current semantics. Restore owned headers on all returns, end the temporary selection before releasing cached inputs, and retain normal scratch-admission refusal/cancellation/reset cleanup. Tombstones, storage selection, overlap/representation cases not covered by the scoped route retain their established implementation. Existing native exact/Top-K paths stay unchanged.
5. Keep the existing low-rejection bypass and ordinary schedule restoration. Do not add thresholds, global switches, a new cache, a work queue or a second membership structure.

## Verification and decision

First prove the permission is derived from the final executable predicates and refuses fallible/volatile counterexamples. Extend the existing persisted fixture for total key-first ordering, key/predicate overlap, no/all/partial selection, borrowed-header restoration and unused-input copies; preserve every predicate-first error oracle. Reuse unchanged topology/race evidence, rerunning only affected scopes.

Require actual native Q10/Q19 predicate-input reduction and lower instructions than current main before another full reader comparison. If that gate fails, retain the evidence and propose the next decision; do not push or relax acceptance. The independent join PR continues all22 local acceptance now.


## Approved revision 5: mask consumption and lazy identity coordinates

Requester approved this revision with “go on” before implementation. Earlier source, ownership, lifecycle and acceptance contracts remain. No native Bloom hash/vector kernel, cache policy, API or persistent state change.

## Verified contracts

cgo/bloom.c:bloomfilter_test_fixed explicitly writesfalse for NULL positions before hashing. CBloomFilter.testFixedVector passes the null bitmap, clears const-NULL output and broadcasts the physical result over the vector's logical length. selectLocalDomains nevertheless repeats Vector.IsNull for every returned byte. Its constant broadcast is redundant only when vector.Length equals batch.RowCount; the existing one-value constant header with a larger batch still requires the outer broadcast.

The reader already records an identity boolean when there are no storage selections or tombstones. It nevertheless calls deleted.Contains and writes row0..N into Physical for every row before either callback. Identity coordinates are derivable at the first selection composition. The total-reader path overwrites that initial map when membership is selective.

## Bounded implementation

1. Consume the NULL-correct mask from the unchanged Bloom API. Preserve unsupported-shape refusal and the short-constant-header broadcast. Remove the duplicate Go NULL scan. For the common single active domain, count rejected rows and collect selected logical indexes in one pass; preserve per-domain tested/rejected/bypass decisions and current-batch mask application. Multiple domains retain their order, per-domain statistics and AND semantics, combining counters and AND where possible. No new scratch, state, allocation, API or native kernel.
2. With no tombstones/storage selection, set live row count without testing or filling identity rows. At first composition, derive the original index directly from the logical index; keep Physical lazy until a real nonidentity selection exists. Native exact and predicate-first paths explicitly write their first composed map using the identity flag. In total membership-first execution, partial membership copies its selected original indexes; identity membership plus partial predicates copies the predicate indexes; both identity stages use existing full-column publication without reading map contents. Tombstone/storage maps retain their existing population. Keep allocation/refusal/ownership and I/O scope unchanged.
3. Preserve row and error order, NULL/constant semantics, original-row coordinates, low-rejection bypass and counters. Existing masks/maps remain lane-owned and cleared/freed by the same owners. Unsupported cache formats still decline before callbacks/copies.

## Evidence before delivery

Extend existing NULL/short-constant/multiple-domain controls and original-row mapping fixtures. Prefill reusable map storage with invalid indexes to prove identity cases do not consume stale contents; cover empty/all/partial stages and tombstones/storage rows. Require a component instruction/cycle win and unchanged allocations using the actual existing Bloom API. Reuse unchanged stream/cache lifetime evidence; run the changed owning packages and static closure.

Then screen Q7 and Q10 at both SF10 and SF100 against current main, not only the small scale. No full reader campaign until those foreground instruction/resource signals improve. Finish all22 qualification with the approved one-process six-suite protocol, preserve exclusions and keep draft if any introduced regression remains. The independent join PR continues separately.

## Review closure: projection policy, retirement and optional capacity

Handled membership reads use the original projection ColumnUses carried by the optional request; the outer request retains its aligned policy for unsupported/ordinary readers. Required domain-source keys remain owned. Policy caching is invalidated when membership enters or retires.

After a read returns, account its current late columns and retire completed membership before EOF/empty-result shortcuts. Low-rejection bypass restores ordinary scheduling immediately, including an empty residual result.

If optional dictionary metadata is refused, leave the pinned scope, release all membership maps/bitmaps, and reuse the ordinary scoped-owned predicate route. No selected wide-payload temporary is allocated before predicate evaluation. The optional stage passes and retires for that scan; the join still owns correctness. Preserve raw input/callback order and conservative fallback column policy.

Focused projection-policy, empty-result retirement and constrained-account nullable/wide predicate regressions pass; owning table_scan/blockio, focused race checks and incremental static checks pass. Existing performance measurements remain pinned to source59acc92f6a; these functional/resource review fixes are not represented as newly timed benchmark results.

## Approved review revision: ordinary-reader fallback scope

On 2026-11-18 the requester approved retaining staged pruning only for pinned,
identity-visible reads with a proven-total predicate chain. Generic predicates,
tombstoned/storage-selected blocks and native exact reads use their established
ordinary reader and the existing post-read local-domain filter. Do not mark an
unsupported route as capacity-bypassed: its post-read domain must remain active.

Decline before any predicate or membership callback, and release scratch retained
from an earlier admitted block before the ordinary reader allocates output.
Remove the unused generic/exact membership map composition and Logical vector.
The staged total path keeps its existing one-map selection, source pinning and
capacity-refusal contract. No replay API, new reservation layer, hidden row-map
allocation, predicate reordering or change to native exact/visibility semantics.

This scope gives up early payload pruning on those ordinary routes. The measured
Q19 path stays staged; older measurements are not retimed or relabeled. Verify
ordinary and optional generic/tombstoned/native-exact reads under identical small
accounts, one callback execution, genuine callback/source errors, stale-scratch
release, and the TableScan post-read domain application.

The ordinary reader owns visibility. Admission checks are free of I/O; staged
execution runs only after the existing tombstone lookup proves identity, while
nonempty masks continue through that same ordinary read. Unsupported/native
routes release prior scratch before required materialization. No second lookup
or visibility cache is added.

If post-read dictionary publication exhausts optional capacity, preserve the
already read batch for the exact join and record the publication fallback.
Successful publication still prunes; callback and I/O errors retain their usual
propagation. A failed publication never destroys or repeats the required read.

The same retryable-capacity classification covers optional maps, view metadata,
gathers and post-read publication. A proven-total predicate capacity refusal
with live optional physical-map storage first leaves the pinned scope and frees
that storage, then uses the existing ordinary predicate fallback. Capacity
errors without optional storage, semantic callback errors and I/O errors keep
their normal propagation. No unknown/fallible callback is made replayable.

Post-read dictionary-key normalization is optional probe work. On capacity
refusal, keep the completed batch, bypass that probe with the existing pass
state and allow any other healthy domain to prune. Mandatory source-building
normalization and non-capacity errors retain their existing behavior.

The prepared optimizer's constructor mode survives the public Optimize call
used by bound planning, even though that call passes isPrepareStmt=false.
Per-call prepared suppression is restored afterward rather than becoming sticky
when a normal optimizer is reused. Post-fold derivation remains ordinary-only.
