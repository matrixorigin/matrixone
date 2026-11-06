# Ordered ODKU execution and storage identity (4.2)

Design revision 7. This document describes the current ordered ODKU contract
and its implementation boundaries on `4.2-dev`. Exact comparison revisions,
measurements and review results belong in the external validation record.

Direct primary/unique-key assignment restrictions remain. Generated-key moves
already accepted by MatrixOne remain supported. This change does not promise
general MySQL constraint compatibility or repair existing corrupt rows/indexes.

## Problem and responsibility

Each insert or duplicate-update action must observe the effective row and live
key ownership left by earlier successful actions in source order. Resolving all
conflicts from the initial snapshot, publishing a candidate before its checks,
or filtering no-ops only after publication breaks that contract. Writer counts
also cannot describe several logical actions coalesced into one final row.

`PreInsertUnique` owns ordered actions, current images and key claims. A stored
original is identified by RowID; its immutable raw PK remains the writer's old
identity. The planner owns expression phases and dependencies. `Compile` owns
the execution-attempt result and writer dependency fence. The engine owns the
captured workspace/snapshot view, pins and deferred-object cleanup. DedupJoin
and HashBuild have no second UPDATE replay owner.

Failure rolls back the statement. Retry creates a new capture, action owner and
result slot. No failed action may expose an image, key claim or successful count.

## Physical identity and SQL equality

These are separate contracts:

- **Constraint ownership** uses each relation's existing physical primary-key
  representation, including hidden unique relations. Scalar FLOAT/DOUBLE keys
  retain their assigned IEEE bits; composite/prefix keys use existing serialized
  bytes; ENUM uses the hidden relation's UINT16 ordinal. NULL unique keys own no
  key. Keep raw typed keys for lookup, duplicate messages, locks and writers.
- **Effective update** uses the existing typed SQL null-safe comparator `<=>`
  against the immediately preceding effective image. JSON equality and FLOAT
  scale semantics therefore apply to update effect, without redefining physical
  ownership or the separate FK comparator.

Valid distinct +0/-0 physical keys in one ODKU statement must produce two
inserts, even though SQL equality reports true. Never redirect an action to a
SQL-equal but physically different owner. This concerns typed keys after normal
assignment casting; it does not add SQL support for arbitrary NaN payloads or
change ordinary numeric equality or persisted encoding.

The planner uses existing physical key builders, prefix handling and storage
casts. For scalar floating comparisons, `bindPrimaryKeyIdentityExpr` serializes
both sides without altering the raw writer/lock payload. Target lookup, later
unique duplicate checks and old-index lookup JOINs use the same identity.

Accepted-owner maps preserve floating bits in their accounted map input and
retain NULL/GROUPING semantics across constant/flat vectors and grouping-map
promotion. SQL grouping/hash semantics remain unchanged. Every relation in a
`StatementKeyView` is probed by its own physical PK, with the same raw-byte
residual and binary workspace-map identity before and after promotion.
Conservative signed-zero ranges and exceptional full scans may admit extra
candidates; pruning must not discard the actual physical owner.

## Ordered evaluation and publication

The plan explicitly represents these phases and preserves them through column
liveness, rewrites, visitors, deep copy and codecs:

1. User assignments in AST order, including repeated targets.
2. Generated assignments independent of implicit ON UPDATE columns, in the
   existing dependency order.
3. Implicit ON UPDATE assignments, excluding explicitly assigned columns.
4. Generated assignments dependent on the automatic phase, followed by any
   composite-PK helper.

MO DDL already requires deterministic generated expressions without references
to later generated columns. The change-comparison set contains applicable
non-hidden written columns, excluding implicit automatic columns and their
transitive generated closure. A generated-column DEFAULT can legitimately
leave an empty pre-change phase and comparison set.

For each strictly increasing source ordinal:

1. Resolve the first eligible current physical conflict in constraint order,
   or allocate an insert owner. Preserve the existing explicit-input CHECK
   boundary, casting and error order. An insert runs insert CHECK/NOT NULL and
   is eligible for applicable non-self FKs.
2. For an update, retain the immediate before-image. Execute explicit RHSs
   exactly once in order; each sees earlier assignments. Evaluate the first
   generated phase and compare using prepared typed `<=>` executors. A changed
   action executes the automatic/dependent phase; a no-op keeps the automatic
   values. Required update CHECK/NOT NULL and generated-expression errors still
   apply before publication, including on a no-op.
3. A SQL no-op restores the whole preceding table image, including its bytes
   and physical keys. A SQL-equal alternative JSON representation must not leak
   to a later action. Advance source metadata and retain applicable final FK
   obligations without publishing a new key move.
4. For a changed image, evaluate FK tuple eligibility against the immediate
   before/after images using existing null-safe FK semantics. Resolve all
   proposed non-NULL physical keys against live owners and the captured prior
   view before claiming them. A different owner produces the typed duplicate
   error. Publish current images and all key transitions together; any partial
   allocation/mutation, collision or cancellation makes the attempt terminal
   until Reset.
5. Superseded eligible images use the existing FK prefix output. Final
   cumulative FK eligibility reaches the parent-lock/retry check before
   physical no-op filtering. Suspended output retains its target and cannot
   repeat RHS evaluation, ordinals or counts.

Ordinals add no SQL ordering guarantee to unordered INSERT SELECT. A singleton
shortcut requires statically proven singleton input.

Compile typed executors once. A stable borrowed two-batch RHS frame observes
image changes synchronously; executors neither mutate nor retain the frame.
Free executors before their inputs and clear the frame on Reset/Free.

For bool, bit, integers, enum and byte-string types, equal non-NULL stored bytes
are a sufficient proof of the existing `<=>` result after checking matching
actual OIDs, canonical physical sizes and valid row shape. Both NULLs are equal;
one NULL differs. Unequal bytes and every other type use the typed executor,
including FLOAT, JSON, arrays and temporal types. This is a sufficient equality
shortcut, not a second SQL comparator; differential tests use actual `<=>`.

A static no-op shortcut additionally requires immutable keys, no prior view,
FK/deferred output or automatic/generated phase, exact
incoming-column assignment plans/executors covering every comparison column,
native NOT NULL checks with valid mappings, and exactly the mapped table fields
plus source ordinal. Unknown/different operands fall back before any RHS runs.
On success every verified RHS still runs once in AST order, native checks run,
cancellation is checked, and an existing owner's ordinal uses the normal
accounted copy path. A newly resolved, unchanged original stays in the frozen
snapshot instead of acquiring a retained image and RowID entry. Later actions
can resolve it again: keys cannot move and physical writes wait until EOF.
Original validation and insert-claim conflict checks still precede the RHS.
Both paths share count/ordinal finalization and preserve image bytes and locks.

## Logical results and final writes

The action owner counts successful actions in a checked uint64: +1 insert,
+2 effective duplicate update, +0 no-op. A change then reversion can report 4
while requiring no final write; insert then update reports 3. Preserve 4.2's
current zero-for-no-op behavior. Although advertised, `CLIENT_FOUND_ROWS` lacks
planner/compile support on this branch; this change does not add that capability.

One Compile-owned result slot belongs to each execution attempt. Coordinator
preparation proves one ordered owner, including inline and empty-source plans.
After consuming input, lock/history validation and final output, the owner
publishes EOF/count once. The slot survives operator Reset/Free and has explicit
synchronization. Retry binds a fresh slot so an old callback cannot publish into
a new generation. After all consumers succeed, Compile uses that logical result
instead of physical writer counts. Missing/multiple publication is an internal
error. Later sink/adjust/cancel/rollback failures discard the count. Missing EOF
also closes views, finishes the allocation account and aborts a RETURNING sink.
Other DML and remote/S3/partition writer count paths retain their contracts.

Final physical writes are independent. New owners write. An unchanged original
does not. After an effective change, compare the final mutable stored-column
closure and raw PK with immutable originals, including automatic/generated and
composite-PK columns, excluding RowID, ordinal and execution/FK metadata. Exact
stored-byte differences require a write even if SQL-equal; uncertain physical
comparisons conservatively write. This preserves changed automatic timestamps
and changed actions carrying SQL-equal JSON bytes.

Required FK validation always precedes elision. A typed physical-write flag
carries no-op rows through final FK checks when needed; otherwise the owner can
suppress final no-write rows while still reaching EOF. Fixed- and variable-width
output stop at each no-write owner. No count-only rows or second count owner
are needed.

## Locks, storage views and writer dependencies

Incoming, resolved original, final and transient physical keys use existing
lockop domains; preserve resolved-row locking. A stored original returned for a
vacated key is not a current claim. Post-lock view validation/snapshot refresh
retries the entire attempt when necessary. SI commit response does not imply
CN logtail application; cross-CN result assertions fence on actual nonempty
commit frontiers and wait for application before reading base/forced indexes.

Immutable keys retain the snapshot JOIN path without a prior view. Mutable-key
attempts capture one engine-owned view of base/hidden relations and workspace
state. Lookup owns and closes remote readers; no I/O occurs while holding the
transaction mutex. Pin charges follow detached workspace batches/files and
copied tombstones until views and readers release them. Rollback invalidates
views without waiting under that mutex. Compile joins scopes and closes views
before sealing their account or entering rollback/retry. Prepared reuse resets
executors, images, claims, eligibility, ordinals and result generation.

All old regular-index reads and fulltext/IVF delete preparation complete before
any physical writer mutates data. Materialized producers must all succeed
before writers proceed. Base and every index consume the same effective final
image. A generated PK move updates hidden-index base-PK payloads even when their
index keys do not change. There is no all-deletes-first escape path.

The TN common scan advances separate data/tombstone dedup frontiers only after
all eligible in-memory and persisted sources complete. Cancellation, Bloom,
storage and close errors must not advance them. For optimistic RPC metadata,
default IncrementalDedup normalizes to existing FullSkipWorkspaceDedup; explicit
nonzero policies, pessimistic defaults and borrowed request ownership remain.
The wider default-policy correction needs ordinary optimistic DML, replacement,
PK/UK conflict, successful disjoint writes, explicit-policy and persisted-object
controls in addition to ODKU competition tests. Additional tombstone late-source,
direct Bloom-fault and mixed-policy/DDL combinations remain untested limits.
Any narrowing to an ODKU-only per-write policy requires a new lifetime/consumer
proof; ODKU alone cannot justify unrelated global policy changes.

## Resource bounds and deferred cleanup

Retain one original/current image per touched row and one live non-NULL key per
constraint. Distinct old/new/transient groups remain until Reset to deduplicate
lock history, including cycles. They are not compacted to a current-owner cap.
The `dead` counter validates reuse of vacated ownership. Row-image/key payload
areas compact dead varlen bytes independently from these groups.

Charge retained vectors, hash cells/iterators, metadata, key/comparison scratch,
workspace indexes, copied deletion selections, remote frames, lock history and
writer materialization to existing attempt/shared-query budgets. Images can be
O(touched rows × projected width), history O(actions × constraints). Exhausted
admission returns the existing resource error. Output has an 8192-row and soft
1 MiB limit, with at most one-row byte overshoot. Materialized sources own bounded
spill files/FDs. There is no per-query worker, unbounded queue or per-key cache.

One lazy engine worker owns the existing charged deferred-object queue,
independently of query cancellation and the ordinary GC pool. Transfers append
linked chains after transaction/pin locks are released. A buffered wake and
queue check under the same mutex prevent lost notifications; no per-object
job allocation or goroutine is needed.

Delete uses bounded chunks and five-second I/O contexts. Failed chunks retain
their full charge, move to the FIFO tail and retry individually with interruptible
exponential backoff. Successful deletion alone releases each charge once.
Absent-name deletion is idempotent, allowing ambiguous partial failures to retry.
No I/O, backoff or shutdown join holds transaction, view or queue mutexes.

View admission registers before capture. Final close transfers detached objects
and releases registration after cleaning all view resources. Shutdown rejects
new views, waits for no live views, queued records or in-flight delete, then
stops/joins the worker. The idempotent CN hook runs after pipeline handlers join
and before I/O teardown; Engine.Close uses the same hook.

The existing bounded shutdown budget applies. On deadline, terminal failure is
recorded before canceling I/O; retain the error and unconfirmed charges without
claiming a completed join. Late registered views may transfer records, but cannot
restart a worker against torn-down services. Repeated shutdown returns the stored
failure. Unconfirmed records then require process/external cleanup.

## Compatibility and alternatives

No catalog, disk, backup, checkpoint or persisted-key format changes. New plan
metadata has matching producers, consumers, deep copy and structural validation.
Upgrade is full-cluster shutdown only; no PR-specific MORPC/build-ID gate or
old-peer fallback is needed. Retain unrelated prepared-parameter compatibility
checks, typed response errors and captured-snapshot validation. No downgrade or
mixed in-cluster operation is claimed. Tenant/auth boundaries are unchanged.

Rejected alternatives:

- SQL `<=>` for constraint identity changes existing physical ownership.
- Post-publication no-op filtering cannot undo observable key moves or count
  individual actions.
- Restricting generated-key updates would remove existing supported behavior.
- A global Process count risks stale prior-statement state; per-output count
  rows would widen filtering, FK, materialization and writer contracts. The
  attempt slot is narrower while a single coordinator owner is proven.
- A second replay engine, speculative insert state machine, cache or protocol
  layer adds another ownership/validation path without established benefit.

## Validation and performance acceptance

Reuse existing fixtures and valid source/test/dependency-matched evidence. Run
focused owner tests and real consumers, then invalidated owning packages once
in normal and race modes with nonempty selections and terminal logs. Derive
incremental format/vet/configured lint from the complete final delivery diff
against the verified PR merge base, including intended local source/tests.
Record exact commands, source/test fingerprints, toolchain, native inputs and
terminal status outside tracked source. Pending/skipped/partial runs are not PASS.

| Contract | Required behavioral proof |
|---|---|
| Ordered phases / no-op | Exact-once repeated/volatile RHS, generated and ON UPDATE closure, empty generated DEFAULT, whole-image JSON restoration, CHECK/NOT NULL error ordering and reset |
| Physical identity | Typed/raw FLOAT keys including signed zero, composite/prefix/ENUM/NULL controls; real same-batch two-insert result and base/forced-index reads |
| Logical count / writes | Insert/change/no-op, repeated target, change→revert, automatic timestamp and JSON representation; EOF, retry, empty input, prepared reuse, failure and RETURNING |
| Mutable keys / concurrency | Generated PK/UK movement, vacated reuse, two-owner collision, unchanged-index PK payload, distinct transient locks, pessimistic resolved locks and optimistic memory/persisted competition |
| Prior views / resources | Workspace promotion/capture/flush/deletes, remote/sharded reads, cancellation/typed primary errors, partial admission, reader/pin/account cleanup and reusable Reset |
| CHECK/FK / writers | Explicit-input and candidate checks, intermediate/final/no-op FK obligations, all-producer success fence and failed writer rollback, regular/fulltext/IVF consumers |
| TN policy / frontiers | Ordinary optimistic DML and explicit-policy controls, direct persisted replacement/concurrent commits, scan error propagation; record the untested limits above |
| Deferred cleanup | Closed/saturated GC pool independence, queued/empty wake handoff, partial/persistent delete failures, FIFO isolation, cancellation, late views, bounded/idempotent shutdown and joined ownership |
| Shared operators / codecs | Batch boundaries, aliases, NULL/grouping metadata, spill/admission failures; fallback versus optimized paths, structural plan validation and schema regeneration |

Compare final candidate against the verified current PR target using identical
native artifacts, fixture, runtime configuration and hardware. Use at least
three warmups and seven measurements per workload; choose sufficient samples
to cover warm-engine behavior and prefer interleaved/ABBA process order. Include
INSERT, conflict and no-op, narrow and 512-byte data, representative 8192-row
batches and hot keys. Keep reset/setup/verification outside timed SQL execution.
The common-correct gate includes all ten maintained cases in
`BenchmarkOrderedODKU`; the generated-key case is reported separately.

Both target and candidate same-version process-median spread must be <=2.5%
for singleton workloads and <=5% for batches. Point-median regression must be
<=5% for singletons and <=10% for representative batches. Publish distributions,
sample counts, tails, process order and peak memory. Noise or paging does not
waive the budgets; stopped sequences and favorable incomplete subsets cannot
establish acceptance. Profile a reproducible breach before retaining an
optimization, and prove its benefit against the pre-optimization candidate.

The generated-key workload retains a strict forced-index result oracle. When
the target fails that oracle, report only the corrected path's standalone cost;
a ratio against an incorrect result is invalid. Record historical observations
as historical. Performance/merge acceptance requires qualifying final-revision
evidence independently from correctness and ordinary draft publication.
