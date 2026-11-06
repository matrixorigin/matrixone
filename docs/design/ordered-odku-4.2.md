# Ordered ODKU execution and storage identity (4.2)

Design revision 5, 2026-11-07. This design covers PR 31979 against the 4.2-dev
storage and SQL contracts. It preserves direct primary/unique-key assignment
restrictions and generated-key moves already accepted by MatrixOne. It does
not promise general MySQL constraint compatibility or repair existing corrupt
rows/indexes. Revisions 3 and 4 clarify shutdown-upgrade and deferred cleanup
ownership below.

## Contract, evidence, and decisions

This change gives one ordered owner a sequence of insert or duplicate-update
actions. Every action observes the *effective* row and live key ownership left
by prior successful actions. A stored original is identified by RowID; its raw
original PK remains the writer's deletion/index payload. For each constraint,
the target is the first eligible current physical owner in table order. An
update cannot publish its proposed image, key claims, automatic timestamp, or
count before its checks pass. Failure rolls back the whole statement; retry is a
fresh capture and a fresh sequence.

Negations include: updating a SQL-equal but different physical key's row;
exposing a value from an action whose update was a no-op; counting only the
number of final written images; accepting a duplicate key after a key move;
losing a vacated key; leaking an intermediate image through a failed check; or
leaving an index pointing at an old PK after a generated PK moves.

The contract has **two distinct equality domains**:

1. **Physical constraint identity:** base PK and the PK of each hidden unique
   relation use the existing stored, typed key representation. Single FLOAT/
   DOUBLE keys preserve the bits of the assigned physical value; encoded
   composite/prefix keys compare their existing serialized bytes; ENUM uses the
   hidden relation's UINT16 ordinal; NULL unique keys own no key. The original
   PK is always retained raw for writer and lock input. Do not apply SQL's
   generic floating hash normalization to decide stored-key ownership.
2. **Whether an update is effective:** use MO's existing typed SQL NULL-safe
   equality operator `<=>` over the same applicable non-automatic columns as
   the current no-op guard, comparing the **immediately previous effective
   image**, not the statement's original image. This includes JSON's actual
   SQL equality and FLOAT scale behavior. It is intentionally distinct from
   constraint identity and FK comparison. Do not implement a new family of
   hand-written type comparators as a substitute.

Public probes against the 4.2 target and the pre-repair PR candidate show binary-protocol +0/-0 DOUBLE values can coexist across separate
INSERTs in a PK and a single-column unique index, although SQL equality is
true, and an existing negative-zero owner is reachable by ODKU. The original
baseline's same-batch unique case returns 1062 and rolls back; that candidate instead
silently combines two different physical identities into one row. The selected
repair uses physical identity consistently inside ordered ODKU and its directly
affected duplicate checks and index-maintenance comparison boundaries. For two
otherwise valid new distinct physical identities in one ODKU statement, the
required successful result is **two inserts**; baseline's 1062 is an observed
defect, not an acceptable alternative outcome. Never redirect to the other
physical owner. If a typed writer/TN test proves another documented constraint
actually prevents this result, record the concrete evidence and return for a
design decision; do not weaken the SQL assertion to accept both outcomes. Do not
change plain INSERT globally merely to make this example succeed. This is a
concrete storage-identity repair, not a promise
of general MySQL PK/UNIQUE compatibility, NaN SQL support, or a general change
to numeric equality. Base's direct PK and unique-column ODKU assignment
restrictions remain; generated-key moves already accepted by MO remain supported.

Those probes also show the pre-repair candidate fixes a baseline stale unique-index payload after a
generated-PK move and a wrong owner after a generated-unique key is vacated.
Keep these fixes. Action counterexamples identify the candidate's regression when an automatic ON UPDATE
column feeds a generated key: a true no-op can fail 1062. The PR's historical
results show a 16.43% batch no-op and 12.19% 512-byte insert regression on an
earlier candidate against the merge base; those historical timings do not measure the final implementation. Those historical numbers are not final-target performance evidence.

### Pre-repair causal path

- `bind_insert.go` appends implicit ON UPDATE expressions, then generated
  assignments and the composite PK. `odku_actions.go` calls
  `odkueval.EvaluateUpdate`, which evaluates all of them and update CHECKs.
  `applyODKUAction` then evaluates and resolves proposed keys, publishes the
  image and claims, and only much later does `bind_insert.go` filter final
  rows by original-versus-final `<=>`. Thus the filter is too late for a
  generated-key collision or the representation observed by the next action,
  and its original-versus-final comparison cannot count multiple actions.
- For a single-column FLOAT/DOUBLE unique key, `buildODKUInput` compares raw
  values with generic `=`; `odku_keys.go` uses generic hashmap iterators,
  whose floating encoders canonicalize signed zero and scaled FLOAT values.
  `statement_key_view.go` sets `physical: i == 0`, so unique-index
  relations use `sameStatementKey(false)` and the generic, promoted workspace
  hash index. The base relation already uses a binary raw-key workspace index.
  A residual comparison alone cannot repair a promoted hash index that merged
  two physical keys.
- Existing affected rows originate in physical `MultiUpdate` insert/delete
  counts. One final image may represent several ordered actions, or no final
  physical write despite a nonzero logical action count. Pipeline cleanup can
  Reset the owner before `Compile.Run` reads the final count.

## Chosen implementation

### 1. Planner phases and action-local comparison

In `bind_insert.go`, `odku_actions.go`, the plan proto and existing plan
visitors/codecs, describe the ordered assignment phases explicitly rather than
infer them from the generated RHS at execution:

- user assignments in AST order, including repeated targets; each RHS and cast
  executes exactly once and observes earlier assignments;
- generated columns that do **not** transitively depend on an implicit
  ON UPDATE column, in existing generated-column dependency order;
- implicit ON UPDATE assignments, with a column excluded when user-assigned;
- generated columns in the automatic dependency closure, in dependency order,
  then any composite physical PK helper. Generated expressions are constrained
  by MO DDL to be deterministic and not forward-reference a later generated
  column (`build_util.go`, `build_ddl.go`).

The first two phases produce the candidate for change comparison. The compared
set is the current guard's non-hidden written columns, excluding implicit
automatic columns and their generated dependency closure; preserve errors and
`<=>` semantics. Prepare typed comparison executors once, not a new expression
per action, and short-circuit evaluation only where doing so cannot suppress a
required expression error. Use the plan's existing relation-0 current image
and relation-1 incoming values and preserve them through liveness, expression
rewrites, deep-copy, protobuf and distributed codecs. Validate phase metadata
and layout before execution. If a required generated/check expression needs a
column not otherwise materialized, retain that dependency.

For each source ordinal:

1. Resolve the first eligible physical conflict, or allocate an insert owner.
   Evaluate the existing explicit-input CHECK prefix at its existing boundary;
   retain conversion, binding, RHS and error order. An insert runs insert
   CHECK/NOT NULL and is eligible for all applicable non-self FKs.
2. For an update, snapshot the full immediate before-image in stable scratch;
   run every explicit assignment once and evaluate generated values needed for
   the comparison. Determine change using the prepared SQL `<=>` comparison.
   If changed, evaluate each eligible implicit ON UPDATE expression exactly once
   and the remaining generated expressions. If unchanged, do **not** advance
   implicit ON UPDATE; form any generated candidate needed to perform the
   existing update CHECK/NOT NULL obligations with the old automatic values.
   Perform applicable update checks before publication even on a no-op. Do not
   suppress an expression, conversion, CHECK, NOT NULL, prior-lookup, or other
   typed error merely because a later comparison may say no-op.
3. A no-op restores the **whole prior table image**, including its byte
   representation and physical key, before any later action can observe it.
   It has no new key claim or key move, but it still advances the ordinal and
   carries required FK/final-validation and source metadata forward. In
   particular, a JSON value equal under `<=>` must not leak a different
   representation into a later action when this action is a no-op.
4. A changed update evaluates the complete post-automatic/generated image.
   Evaluate FK tuple eligibility against the immediate before and effective
   after image using the existing SQL null-safe FK semantics. Resolve *all*
   non-NULL proposed physical keys against current owners and the captured
   prior view before claiming any. An existing different owner gives the
   established typed duplicate error. Atomically transition all claims and
   publish the stable image; on allocation, cancellation, collision or partial
   mutation mark the attempt terminal until Reset. Append exact raw old/new/
   transient lock keys once. An original returned by the prior view is not an
   owner of a key it has since vacated.
5. Emit superseded eligible FK images via the existing prefix mechanism;
   final eligibility remains cumulative for a new row and is validated by the
   existing parent-lock/retry path before it can be discarded as a physical
   no-op. Pending/suspended output retains its target and must not rerun RHS,
   counts, or ordinals.

This phase ordering is a deliberate change from unconditional implicit update.
Keep the existing explicit-input CHECK boundary, and test both candidate checks
and errors from generated expressions under no-op. If an existing supported
expression requires a different order to preserve an observable error, do not
silently reorder it: update this design with that concrete counterexample.

### 2. Physical key boundary

Use the existing physical builder as the source of each raw key:
`buildUpdateMutationIndexKey`, prefix handling, composite serial form and
`bindIndexKeyStorageExpr`; do not recode persisted data. Fix the ODKU target
LEFT JOIN and its later unique-index duplicate and old-index lookup JOINs so
both sides compare the hidden relation PK's physical identity. For scalar
FLOAT/DOUBLE, `bindPrimaryKeyIdentityExpr` uses `serial` on both sides;
the packer uses raw IEEE bits, so it preserves signed zero and NaN payload bits.
Apply this as a **comparison expression only**. Keep raw typed values for
`Lookup`, writer/delete payload, error formatting and `lockop.LockRows`.
Non-floating and already serialized keys retain their existing representation.
Audit every ODKU index-maintenance join in this path, not only target arbitration.

For accepted-owner maps in `odku_keys.go`, retain raw vectors and normalize
only the map input for floating scalar keys into a bit-preserving identity
(e.g. owned/accounted binary or unsigned-bit scratch), consistently on insert,
find, release, transition and rebuild. The selected implementation must preserve
NULL and GROUPING metadata and constant/flat vectors. Do not use a numeric cast
or generic floating hashmap canonicalization. The raw value remains available
for duplicate formatting and raw lock-key history. Avoid a global hashmap change:
SQL joins and grouping intentionally have a different equality contract. Base
FLOAT PKs already use serialized identity as `CurrentKey`, but audit all code
paths rather than assuming every constraint uses the same type.

Every relation passed to `NewStatementKeyView` is probed by its own physical
primary key, including a hidden unique relation. Remove the false logical/physical
split there: use the existing raw-byte residual **and** existing binary scratch
for the promoted workspace map for every relation; keep raw typed lookup APIs.
The preliminary first-workspace scan and promoted path must return the same
owner. Reassess and remove dead logical-only scaled-float probing after this
change; retain the conservative packed zero range (both signed zeros as
candidates, exact physical residual) and exceptional full scans where needed.
Candidate pruning must not lose the actual key. Existing object/reader lifetime,
remote tombstone application and accounting remain intact.

Lock fetchers pack FLOAT/DOUBLE raw bits already. Verify that incoming, resolved
original, new and transient key locks all target the same physical table/key
domain as the writer. Preserve issue 31957 resolved-row locking. Existing
`sameODKUKey` raw comparisons and key group reclamation must agree with the
chosen normalized map representation; account scratch and preserve group
identity when a map switches for GROUPING. Rebuild and lock-history dedup must
not discard a physical key before it is locked.

The design claims exact representation for **typed keys after existing
assignment casting**, not that all possible float bit patterns can be supplied
through every SQL client. If TN or another existing downstream dedup path uses
a different equivalence, characterize it with a minimal typed/public witness
and repair the directly affected ODKU boundary without reinterpreting unrelated
SQL equality, changing the on-disk encoding, or silently redirecting a row.
If an independently documented writer/TN constraint would prohibit the required
two-insert outcome, return the evidence for a design decision before broadening
scope or changing that expected outcome.

### 3. Affected rows and physical writes

Maintain a uint64 logical count in the one action owner for the current attempt:
+1 for an insert, +2 for each effective changed duplicate update, +0 for a
no-op, in input order. A statement that changes an original and then changes
it back can report 4 even if its final image requires no write; inserting a new
owner and changing it once reports 3. Increment only after the action's checks
and publication succeed. Detect arithmetic overflow rather than wrapping. This
preserves 4.2's current zero-for-no-op behavior; `CLIENT_FOUND_ROWS` is
advertised but current 4.2 has no planner/compile support for it. Do not add a
new capability contract solely by copying main's broader implementation.

Use a **per-attempt Compile-owned result slot** for ordered ODKU, bound during
coordinator preparation to the one ordered `PreInsertUnique` owner for both
static and mutable keys. `compilePreInsertUk` already merges to one coordinator
or proves a single local inline scope; validate that invariant for every actual
owner, including empty input and prepared/retried plans. At terminal EOF, after
all input, lock/history validation and final emissions, the owner publishes its
completed count once into that attempt's slot. The slot outlives operator
Reset/Free and has explicit publication/synchronization; it is not
`Process.AffectedRows`, which holds previous-statement state. A retry installs
a fresh slot; callbacks cannot publish into a later generation. After
`runOnce` and all statement consumers succeed, `Compile.Run` uses this
slot for the keyed ODKU result instead of the writer-derived count. Missing or
multiple owner/EOF publication is an internal error, never an apparent zero.
Any later `Adjust`, sink seal, cancellation, rollback or other failure
returns an error and discards the count. Other DML, embedded internal SQL,
remote/S3/partition writer counters and client/session state keep their current
paths. This is preferable to altering all `MultiUpdate` count paths; especially
note that partitioned `doAddAffectedRows` currently directly increments.
Test local, multi-scope/remote writer, S3/partition if reachable, prepared reuse,
retry, failure, empty source and RETURNING/count as applicable.

Replace the final old guard with a final **physical write** decision distinct
from action counts. A newly inserted owner always writes. An original with no
effective changed action never writes. Otherwise compare the final values in
the **actual mutable stored-column closure** against their immutable originals,
including implicit ON UPDATE columns, generated columns and the composite PK
where applicable, and also compare the original and final raw physical PK.
Immutable stored columns need no comparison only when the planner and owner
prove they are retained unchanged. Exclude RowID, source ordinal, helper-only
and FK eligibility metadata from the stored-column comparison. An exact
stored-representation difference requires a write, even if SQL `<=>` says the
value is equal. Exact equality
permits elision only after all required CHECK/FK/lock validation. When a type's
physical comparison is uncertain, conservatively write. This ensures a
changed-then-reverted payload still persists a changed automatic timestamp and
a genuinely changed action can persist a SQL-equal JSON representation; a
true no-op restored the prior representation already. Compute this final flag
in the action owner against the retained immutable origin at final emission.
When a final FK obligation requires the row, including for a no-op, carry a
typed physical-write flag through the plan's output layout, liveness and codecs;
retain the row and flag through the final parent-lock/validation check, and
filter only afterwards. With no deferred FK obligation, the owner may suppress
a no-write final row at emission while still reaching EOF and publishing the
attempt count. No count-only row is needed. Keep all required FK intermediate
outputs.

### 4. Concurrency, writer and global dedup boundary

Source ordinals are strictly increasing across batches; singleton optimization
requires a statically proven singleton. Unordered INSERT SELECT has no new SQL
order guarantee. Maintain one current owner and raw original RowID/PK. Original,
incoming, new and transient physical keys must go through existing lockop;
post-lock view validation or a snapshot refresh retries the entire attempt.
All old regular-index reads and synchronous fulltext/IVF delete preparation
complete before any writer mutation. Final base and all index writers consume
the same committed effective image; a generated PK move updates every hidden
index's base-PK payload even if its index key did not change. Do not add an
all-deletes-first escape path.

The TN change advances separate data/tombstone dedup frontiers only after all
eligible persisted and in-memory source scans complete; cancellation, bloom,
storage and close errors must not advance the frontier. Inspect and retain only
the minimum correct common implementation. The PR's RPC normalization is wider:
for optimistic metadata it maps default IncrementalDedup to existing
FullSkipWorkspaceDedup while preserving explicit policies and borrowed request
ownership. Incremental skips old targets and persisted sources under assumptions
which include CN key locks; optimistic mode supplies no such lock. Thus a full
skip-workspace policy is a plausible necessary correctness fix for persisted
sources, and the configured `incremental-dedup=false` mode already uses it.
The acceptance evidence combines independent ordinary optimistic
INSERT/UPDATE/DELETE commits, same-transaction replacement, primary and unique
conflicts, successful disjoint writes and an explicit full-policy control with
the ODKU competition controls and a direct TAE persisted-object witness. The
shared per-domain scan loop and its terminal frontier advancement also require
source review. Additional tombstone-domain late-source faults, direct Bloom
faults and mixed-policy/DDL combinations are untested limits, rather than a
claim of exhaustive coverage. If
these expose an unrelated compatibility or performance regression, do not
paper over it or weaken tests: remove/narrow the RPC change and use an existing
per-write FullSkipWorkspace policy propagated for the ODKU transaction if its
lifetime and other writes' policy can be proven. That is a material design
deviation requiring review before implementation/delivery. No new globally
enabled policy is justified solely by the ODKU example.

## Ownership, bounds, lifecycle, compatibility

The action operator is the only owner of current images, constraints and the
local logical count. One retained original and current image per touched row;
one live key per non-NULL constraint and a bounded, charged history of distinct
old/new/transient keys until locks succeed. Immutable-key path keeps its JOIN
fast path and does not build a prior view; mutable-key path alone captures one
engine-owned snapshot of base and hidden relations. There is no second replay
owner in DedupJoin or HashBuild.

Budget retained vectors, hash cells/iterators, metadata, comparison/key scratch,
prior-workspace indexes, copied deletion selections, remote range frames, lock
history and output/writer fence with the existing attempt allocation and shared
query budgets. Vacated groups may be rebuilt only when they are not also
needed for lock history. Unique transient keys can be O(actions × constraints),
and row images O(touched rows × projected width); neither is assumed constant.
Admission is finite; return the existing resource error when exhausted. Output
is at most 8192 rows and a soft 1 MiB quota, with at most one row overshoot.
Existing bounded disk/FD spill ownership remains with materialized writer
sources. No per-query goroutine, unbounded queue or per-key cache. Revision 4 below
adds one lazy engine cleanup worker for charged detached objects.

Prior views retain pinned workspace batches/files and copied tombstone
selections; any promotion/index failure is terminal. Close readers and release
remote reservations with the view before sealing the attempt; pin cleanup hands
off to the existing bounded engine queue. Never hold the transaction mutex
across metadata lookup, internal SQL, I/O, reader close or wait. Cancellation,
partial initialization, duplicate, OOM, locking error, sender/receiver failure,
remote close error and reset/free follow existing typed primary-error and
terminal ownership rules. A retry closes the prior generation and takes a new
view, owner and count slot. Prepared reuse resets executors, all images/maps,
eligibility, ordinal and attempt count. Rollback invalidates the view without
waiting under the transaction mutex; shutdown joins existing bounded cleanup.
Locks/optimistic validation protect actual persisted ownership; FK and writer
dependencies complete before successful publication. Security/tenant boundaries
are unchanged; retained state is bounded per existing query/tenant budgets.

No catalog, disk, backup, checkpoint, or persisted key encoding changes; a new
binary reads older data after shutdown upgrade. Protocol/plan metadata additions
have matching producer/consumer/deep-copy validation but require **no new
per-feature MORPC gate**: repository policy supports shutdown upgrade only.
Remove PR-added `MORPCVersion12`, its latest-version bump, ordered-ODKU
capability checks and tests/instructions claiming version 12 is required;
retain structural shape/width validation, unrelated existing gates and any
unrelated use of older versions. Update the PR design document accordingly.
No downgrade or mixed in-cluster operation is claimed. Do not assert a persisted
plan incompatibility without a specific persisted surface and input.

## Alternatives

| Alternative | Correctness / compatibility | Cost and decision |
|---|---|---|
| Keep the earlier candidate and adjust the downstream no-op filter or add signed-zero special cases | Too late: keys and candidate images already published; still wrong for generated/JSON and promoted prior maps; special cases miss scaled floats and other representations | Reject: multiple semantic owners and regression recurrence |
| Serialize/replay every action through existing physical writers | Could mimic sequential committed actions but alters write ordering, error/rollback and lock behavior; old index payloads, checks and intermediate FKs still need a coordinator | Reject: expensive writes and broader distributed protocol; cannot guarantee current transactional writer guarantees |
| Global physical equality in generic SQL hashmaps, or an always-on prior-data view | Global hashmap change breaks SQL equality/grouping; an always-captured view has substantial memory/I/O for immutable keys and still needs effective-action handling | Reject: unrelated semantic and performance blast radius |
| Chosen: existing ordered owner, pre-publication effective action, localized physical identities, attempt-scoped count | Maintains source order and existing physical writer/storage contracts with explicit lock/FK boundaries | Slightly more planner metadata and per-action comparison; confines cost to affected paths and can retain the immutable fast path |

Per-output-row affected-count/physical-change metadata, as seen in main, is
credible precedent but would require preserving count-only rows through all
filters, intermediate FK output, materialization, remote/S3 and partition
writers. The Compile-owned attempt result slot is narrower on 4.2 because this
PR already enforces a single coordinator owner. If that invariant is not true
in a reachable plan, this decision must be revisited, not patched with a global
process counter.

## Optimization and cleanup

Compare the corrected implementation with target
`02fcfbc898f595b4f8eeb3545c6970e6d3711e85` using the same workload and plan.
Profile regressions before selecting an optimization. The merge base
`ebf03dc42b` remains the delivery-diff and incremental-SCA reference, not the
final performance baseline; no rebase is required for this comparison.
Measure CPU and allocations for source numbering/sort, initial lookup JOINs,
per-action expression execution, copies/varlen compaction, owner hash/key
encoding, allocation accounting, final filtering and writer fences. Reuse the
proven singleton ordinal bypass and static immutable-key path. Compile typed
comparison/phase executors once; use fixed-width direct operations only when
proven exactly equivalent to `<=>` on nulls and all type-specific semantics.
Reuse one-row scratch and skip retained image/key copies when bytes are
unchanged, without skipping RHS, CHECK, FK, key or lock obligations. Avoid
generic per-row serial conversion for non-floating keys. Do not add caches,
parallel action owners or new protocol machinery to recover benchmarks.

Delete the obsolete planner's post-publication original-vs-final no-op
predicate once its obligations are explicitly relocated; remove duplicate
state/flags/tests made redundant, logical-key-only prior-view paths, and the
PR-local v12 gate. Preserve structural validation and existing unrelated
protocol gates. Perform full final delivery-diff mapping: hunk -> contract,
owner/consumer, risk and required evidence. Avoid a second replay mechanism,
a second count, duplicate key histories, or a test that merely repeats a helper.

## Validation and acceptance map

Use the public probes above as baseline observations, not final pass evidence. Reuse exact-head
CI only when relevant inputs/mode remain identical; any changed production,
test, plan codec or dependency invalidates affected evidence. The existing
`TestODKUSystematicSQL` fixture in `pkg/tests/dml/ordered_odku_test.go`
already exercises the real frontend; reuse it instead of starting a new cluster.

| Risk / invariant | Cheapest proof and necessary public/consumer oracle |
|---|---|
| Action order, phase split, exact once RHS, auto/generated closure, checks, error/cancel before publication | Focused deterministic owner/planner UT: no-op, real change, repeated target, generated chain, explicit automatic assignment, expression failure, CHECK/NOT NULL error and reset. Public SQL on exact baseline/candidate for true no-op auto -> generated PK **and** UK; generated collision on a real change must still return 1062 and roll back. |
| SQL-equal representation | Typed SQL comparator UT against actual `<=>`, including NULL, CHAR, scaled FLOAT and JSON. Public two-action JSON witness where an SQL-equal representation differs observably; a later RHS must see the immediately retained effective value. Also a real change with SQL-equal JSON input and changed-then-reverted sequence to distinguish this from raw-byte comparison. |
| Counts versus writes | Public affected rows and committed rows for insert, duplicate change/no-op, repeated no-op/change, change->revert, insert->duplicate update, timestamp changed then reverted payload, and no-op after change. Exact automatic-time control via existing fake-clock/seam or deterministic values; no sleep. Verify final physical/index contents and no-op validation, not only reported count. |
| Count slot lifecycle | Compile/owner deterministic empty source, one owner, EOF once, downstream failure after EOF, retry with new generation, cancellation, reset/prepared reuse and internal SQL; public local plus multi-scope/remote writer and S3/partition/RETURNING only where reachable. Race test publish/consume boundary. |
| Physical-key correctness | Independent typed tests for base and hidden float identity, +0/-0, scaled FLOAT distinct bits, NULL, ordinary equal and distinct values; NaN/infinity payloads only through valid typed interfaces. SQL binary parameters on existing rows and same batch, both ordering directions, using base and forced-index queries and committed state. Assert two distinct inserts for the valid +0/-0 batch; do not accept 1062 as an alternative. Test prefix/composite/ENUM/NULL controls. No test may treat SQL equality as physical-owner proof. |
| Prior view | First sparse workspace probe, promoted index, flushed/workspace/committed reads, vacated key and hidden-index payload -> base RowID, raw bit residual and conservative zero/exception pruning; remote/sharded relation and deletes when applicable. Assert same physical owner before/after promotion; cancellation and typed primary error, close/reset/pin/account cleanup. |
| Key movement and concurrency | Real generated PK/UK move, reuse of vacated key, two-owner collision/rollback, NULL transition and unchanged-UK payload update; force base and each index. Barrier-controlled pessimistic resolved-row lock fix 31957 and optimistic competing original/final-key commits; prove retry or one rejected commit, never dual success. |
| CHECK/FK and writer order | Existing explicit-input CHECK order and update candidate checks; intermediate and final FK missing/valid/unchanged/no-op paths; writer dependency barrier, index producer failure before mutation; regular/fulltext/IVF consumers where affected. Preserve exact typed primary errors and rollback. |
| TN/global policy | Focused policy/frontier UT, real ordinary and ODKU optimistic commit controls, direct TAE persisted-object witness and shared-loop/error-propagation source review as described above. Nonzero explicit policies and pessimistic defaults unchanged; no borrowed request mutation. Additional fault/DDL combinations remain explicit limits. |
| Compatibility and shape | Typed plan phases/visitors/deep copy/codec; structurally invalid layouts fail before producers; normal homogeneous shutdown-upgrade assumptions and existing catalog data; remove only PR-introduced gate. No artificial mixed-version test. |
| Resources/operator | Fault injection at allocation, key transition, prior lookup, source close, writer wait and cancellation; assert account, reservations, reader/view cleanup, no partial emitted success, reusable Reset and bounded lock/key history. Focus race on credible shared-state/lifecycle paths, then owning packages once under `-race`. |

Use the repo Go version and controlled `mo-cgo-test` with matching native
artifacts. Run non-empty exact focused tests, then directly affected owning
packages and real consumers in normal and race modes, with terminal logs. Apply
the race skill's exact-test adaptive repetition for credible concurrency tests;
do not rerun equivalent green tests as a ritual. Derive incremental SCA from
the **entire final merge-base-to-delivery diff**, including staged/unstaged and
untracked Go files, and run gofmt, vet and configured incremental lint on the
actual package/consumer closure. Classify failures by code versus environment;
a pending/skipped command is not PASS. Final `mo-self-review` requires no
material blockers, including all PR hunks and the independent optimistic-policy
closure.

Performance gate: compare final head to current target
`02fcfbc898f595b4f8eeb3545c6970e6d3711e85` with identical native artifacts,
runtime configuration and hardware; at least 3 warmups and 7
measurements per workload, preferably interleaved/ABBA to reduce drift. Include
A/A with sufficient resolution (target A/A median spread <=2.5% for the
singleton 5% budget and <=5% for the batch 10% budget; if noisier, improve
isolation or sampling rather than declare PASS). Use correct INSERT, conflict
and no-op workloads, narrow and 512-byte/wide data, plus generated-key path as
a separately reported cost. Point-median regression must be <=5% on singleton
and <=10% on representative 8192-row batches; publish distributions, run
counts and peak memory, not one favorable run. Profile and optimize each
breach, rerun only affected benchmarks, and do not waive correctness or the
stated budgets.

## Decision log and implementation sequence

1. Physical key identity is a storage contract; SQL null-safe comparison is an
   update-effect contract. Do not infer MySQL compatibility or rewrite on-disk
   identities.
2. Preserve accepted generated-key moves and direct-key-update restrictions;
   resolve full action/check/FK lifecycle before publication.
3. Count logical actions in an attempt-scoped coordinator slot; choose physical
   writes independently, preserving automatic timestamps and stored bytes.
4. Preserve snapshot/locks/TN and index-writer ownership; independently prove
   or narrow the wider optimistic RPC policy before delivery.
5. Remove only PR-added protocol gating under shutdown-only upgrade.

Order of work after design acceptance: (a) add focused failing action/key/count
oracles and exact-base witness where needed; (b) phase/check/no-op handling
plus count slot and physical output guard; (c) physical-identity JOIN/map/view
closure and generated/index controls; (d) TN global-policy and lifecycle
closure; (e) remove dead guard and v12-only gate, update plan/docs/generated
files; (f) focused then mapped normal/race/SCA/public tests and performance;
(g) overall independent review, remediate, revalidate, commit/push/update PR.

Regression coverage must assert the two-insert same-batch signed-zero outcome
through the writer/TN, independent ordinary optimistic DML controls, and the
performance budgets above. If a documented writer/TN
constraint contradicts the selected outcome, return with that evidence for a
design decision before making broader semantic/storage changes.

### Design decisions

- The verified PR target `02fcfbc898` is the performance baseline;
  `ebf03dc42b` remains the merge base for the delivery diff and incremental
  SCA. Rebase is not required.
- Distinct +0/-0 physical identities in a valid same-batch ODKU must result in
  two inserts. Baseline 1062 and the earlier candidate's redirected update both fail this contract.
- Final write equivalence covers the proven mutable stored-column closure and
  raw PK, excluding non-stored execution metadata. No-op rows with final FK
  obligations carry a typed physical-write flag through the parent-lock check.
- The Compile-owned count slot requires a single local owner, a fresh attempt
  generation, terminal EOF and successful pipeline completion. Preserve
  4.2's currently advertised-but-unimplemented `CLIENT_FOUND_ROWS` behavior;
  do not add capability support in this PR.

## Design revision 3 clarification

The overall gpt-6.1-terra/max review of implementation 892cb69c96 approved
removing the PR-added RequireSameCommit build-ID gate as well as version 12.
Build identity is not the captured MVCC snapshot or RowID identity. Homogeneous
shutdown upgrades require neither check, including for builds with unknown
commit IDs. Keep pre-existing prepared-parameter compatibility checks, typed
response errors, real remote range pruning and transaction snapshot validation.
A generated-column DEFAULT assignment can leave an empty pre-change phase and
comparison set; that is a valid no-op, with required generated/check obligations
still evaluated. Fixed-width and variable-width final batches both stop at any
owner that requires no physical write. A missing-EOF result error closes views,
finishes the allocation account and aborts a RETURNING sink before returning.


## Design revision 4 — deferred object progress

Canceled views can enqueue detached rollback objects without another query
ever draining them; concurrent closes can also lose the last drain notification. One lazy worker per engine owns this existing quota-charged
queue, independently of SQL contexts and the ordinary GC pool. A buffered wake
and queue check under the same mutex prevent lost wakeups. Transfers append a
linked chain after transaction/pin locks are released. There is no per-object
generic job allocation or goroutine.

The worker deletes bounded chunks with a five-second IO context. Failed chunks
remain fully charged, move to the FIFO tail, and retry individually to isolate
a bad object; an interruptible exponential backoff bounds failure traffic.
Successful deletion alone returns the pin charge, once. Absent-name deletion
is idempotent, so an ambiguous partial failure can safely retry all its names.
No IO, retry wait or shutdown join holds transaction, view or queue mutexes.

View admission registers with the engine before capture. Its final close
atomically enqueues its detached objects and releases that registration after
all view resources are cleaned. Engine shutdown rejects new admission and
waits for zero live views, an empty queue and no in-flight delete, then stops
and joins the worker. A narrow idempotent CN shutdown hook runs after pipeline
handlers join and before IO teardown; Engine.Close uses the same hook.

Shutdown uses the existing bounded cleanup budget. A deadline marks terminal
failure before canceling IO, retains the error and all unconfirmed charges,
and reports failure without claiming a completed join. Late registered views
may still transfer records, but terminal shutdown never restarts a worker
against torn-down services. Repeated shutdown returns the stored failure.
After failed terminal shutdown, retained records require process/external
cleanup; this is not a live-engine retry or a successful cleanup claim.

Validation uses channel-controlled Delete, canceled captured views, closed and
saturated GC pools, empty/wake handoff, partial and persistent failure, FIFO
isolation, late releases, blocked cancellation and idempotent shutdown. Assert
physical object lifetime, typed SQL errors, exact quota ownership, single
Delete ownership and worker joining under normal and race instrumentation.


### Validation boundaries

SI commit responses need not wait for CN logtail application. Competing-commit
SQL fixtures fence on the actual CN client's nonempty commit frontier and wait
for a later applied timestamp before asserting exact base and forced-index
contents. A session choice alone is not the fixture's visibility guarantee.

The generated-key benchmark retains a strict forced-index result oracle. The
4.2 target fails that oracle for the generated-UNIQUE move workload, so its
runtime is not a valid performance comparator. Report the corrected path's
standalone cost separately; apply the matched A/A and ABBA budgets to the ten
common-correct singleton and batch workloads. Do not weaken the index oracle
or report a speed ratio against an incorrect result.

The cleanup tests enqueue during Delete and after observing an empty queue.
The exact internal empty-check-to-wait interval is covered by the common-mutex
and buffered-wake source proof rather than a production test hook.


## Design revision 5 — action comparison cost

Profiles of repeated hot-key actions identify allocation in the pure SQL
comparison executor. For bool, bit, signed/unsigned integers, enum and byte
string types, identical non-NULL stored bytes are a sufficient proof of the
existing MO `<=>` result; both NULLs are equal and one NULL differs. The action
comparison may short-circuit those proven cases after checking one-row shape
and matching actual OIDs and canonical physical sizes. Width/Scale do not
affect these approved SQL operators. Unequal bytes and every other type continue through
the existing typed executor, including FLOAT, JSON, arrays and temporal types.
This is a sufficient equality check, not a replacement comparator. It leaves
assignment, CHECK, FK, key/owner publication, source metadata, locks and counts
unchanged. Differential tests use the actual compiled `<=>` as the oracle and
observe fallback calls. The insert-path performance breach remains a separate
measurement/design question; speculative batch admission is not part of this
optimization.


The action owner also holds one stable two-batch RHS input frame after both
scratch batches are created. Evaluators synchronously borrow this frame and
still observe each earlier assignment through the image batch; they do not
mutate or retain the slice. Free executors before inputs and clear the frame
with the action state on Reset/Free. This removes per-RHS temporary slice
allocation, with only two bounded per-owner pointers and no new row state.
