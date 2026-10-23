# Issue #25590: Workspace Statements Bounded by the Live Working Set, and Lossless Failure-Path Cleanup — Design

- Owning issue: [matrixorigin/matrixone#25590](https://github.com/matrixorigin/matrixone/issues/25590)
  ([Bug] AI-WALK (Txn workspace): repeated mutations degrade super-linearly in long transactions)
- Implementation series: pull request [#30988](https://github.com/matrixorigin/matrixone/pull/30988)
  from branch `agent/issue-25590-6d5e84a3`, 55 changed paths, built on main
  `be663676872` after the r12 post-merge rebase recorded below; the design-review
  rounds and the 2026-10-23 implementation-review series ran against the original
  parent `83222fa8371007c7969d3aeca3ab8ced90695f4c`, which main absorbed before the
  rebase.
- Document version: **r12** (2026-10-23; independent review outcomes r8-r12 recorded
  2026-10-24 in the closing §10 sections below). Revision r1 was the round-17 design-gate
  artifact; r2 revised §5.1, §5.3, §5.5, §6, §7, §8, §9 and the §10 review record to
  resolve the round-18 pre-PR review's two blocking findings (B6/B7 in §10): capacity
  must be reserved per persisted OBJECT rather than per dump, and every recorded
  object a rollback can hand to the queue — direct-S3 records and successfully
  dumped objects alike — must enter the bound at creation. r3 revised §2, §4, §5.3,
  §5.4, §5.5, §6, §7, §9 and the §10 review record to resolve the round-19 pre-PR
  review's two blocking findings (B8/B9 in §10): the dirty-object compaction must
  complete all fallible preparation and admission BEFORE any original metadata
  retires — with worker failures propagated instead of panicked into the ants
  pool — and the direct-S3 insert writer must reserve capacity BEFORE its objects
  persist and hand its unrecorded persisted names to a cleanup owner on unwind.
  r4 revises §4 (§4.5), §5.3, §5.5, §6, §7, §9 and the §10 review record to resolve
  the round-20 pre-PR review's four findings (F1–F4 in §10): the compaction pass
  must publish its replacement records ATOMICALLY across every replacement (an
  earlier fully-staged record retracts with a failed pass, so nothing survives
  underneath the failed admission's statement boundary to be duplicated by the
  retry); every multi_update S3 producer (insert sinkers, temporary table writers,
  tombstone writers) binds the reservation surface and its teardown hands
  unrecorded names to the cleanup ownership; a record's batch copy completes BEFORE
  its admission commits name membership or converts reservations; and the failed
  pass releases the staged re-publication batches its staging loop allocated.
  The exact reviewed revision identity is the sha256 of this complete file, recorded
  in the round-20 handler review record and to be linked by the implementation PR.
  r5 revises §1 (I2/I4 wording, new I5), §2, §5.3, §5.5, §5.6, §7, §9 and the §10
  review record to resolve the round-21 pre-PR review's four findings (G1–G4 in §10):
  publication-in-flight names are classified only at consumer-quiesced boundaries —
  a producer teardown that races ahead of its recording consumer may never move the
  name to the deletion queue inside the recording statement, and every boundary's
  handoff rechecks recorded membership; the capacity classification of recorded names
  follows the commit's TERMINAL outcome (no release at prepare); the standalone
  DELETE tombstone producer joins the reservation and ownership-protocol surface;
  and every workspace a remote run constructs outside the coordinator's session
  receives exactly one terminal settlement at the run's end — and the
  coordinator's transport intake ADOPTS every relayed object name through the
  same admission reservation the writers use BEFORE the fragment's settlement
  can release anything, with every refusal failing the WHOLE run so refusal
  can never grow a backlog some engine does not release itself.
  `rN` changes require re-review of the affected decisions before implementation
  review resumes.
- Status: **Approved** — r1 by the distinct design-first review of 2026-10-21; r2 by
  the round-18 re-review of the §5.3 decisions; r3 by the round-19 re-review of the
  §5.3/§4 producer-ordering decisions; r4 by the round-20 re-review of the
  §4.5/§5.3 atomicity, ordering and producer-coverage decisions; r5 by the round-21
  re-review of the boundary-classification, commit-terminal-capacity,
  producer-closure and cross-CN-settlement decisions; r6 by the round-22 re-review
  of the remote-transfer adoption and two-engine capacity-admission decisions;
  r7 by the round-23 re-review (Decision: PASS at r7, §10); r8, r9 and r10 were
  each returned REQUEST_CHANGES by the round 24-26 independent design reviews and
  their blockers were resolved by their successor revisions (exact reviewed content
  hashes and the findings in the closing §10 record); the operative approval is
  **PASS at r11** by the round-27 independent design-only review of 2026-10-22
  (reviewed content sha256
  74edf0d9ba9c2e1711467ef718b08f76b724c1a513b92c716db9547021e5c836); r12 is the
  recorded post-merge conflict resolution under that PASS, independently covered
  together with the implementation by the six complete-worktree reviews of
  2026-10-23, the sixth approving with no blocking findings (§10 closing record).
  Implementation PR: #30988.

### Revision r6 (round-22 pre-PR review, findings H1/H2)

r6 revises §1 (I4/I5), §2, §5.3, §5.5, §5.6, §7, §9 and the §10 review record
to resolve the round-22 pre-PR review's two blocking findings. **H1 (remote
ownership released before adoption):** the r5 settlement classified transfer
from the run's error alone, so a clean fragment released every reservation and
retained name on the wire's word while the coordinator had classified nothing —
cancellation between the run's clean end and any record, decoded-but-discarded
metadata batches, and record-preparation failures left physical files enumerated
by neither workspace; repeatable per fragment, so the residue accumulated
cumulatively across runs. r6 makes adoption precede release structurally: the
relayed S3-info batches already carry the object names in band (the delete name
column and the insert's serialized BlockInfo), so the coordinator's transport
client ADOPTS every decoded name at its intake (before forwarding, before the
ack) through `AdoptRemoteTransferredObjectNames`, the fragment's settlement
moves behind the message's batch-flow drain — the adoption barrier: the client
adopts before it acks, so a drained flow proves every relayed name reserved and
enumerable on the recording workspace — and the recording side covers the
remaining end classes at its own terminals: every record-preparation failure
fail-open-retains its reservation-less name instead of only the admission's
refusal, and every terminal's reservation drain adopts unconverted
reservations instead of abandoning them. **H2 (the capacity bound across
refused remote records):** the r5 refusal semantics let a saturated coordinator
book one fresh retain per refused record (+1 per round behind a bound nobody
releases with the drain stalled) while a clean fragment recycled its own
engine's capacity and kept feeding — an unbounded aggregate the round-21 test
even asserted (9 → 10 → 11 → 12 behind limit 8). r6 closes the feed at its
source: the intake adoption uses the SAME `reserveUnpublishedPendingObject`
admission the writers hold, so a refusal fails the whole run and the fragment
settles through its own failure terminal instead of releasing — the fragment
engine's population then saturates its own bound and its writers refuse
further spills, so the two-engine aggregate obligation population stays
bounded by the SUM of the per-engine bounds however long refusal sustains with
deletion blocked; the recording side's remaining fail-open retains keep the r19
last-owner doctrine for the residual compat class whose in-tree feed the gates
leave empty, and the growth-accepting test assertions are replaced by bounded
sustained refusal coverage. The first r22 validation round refined two
implementation details within these decisions (§5.3's refusal bullet): the
preparation-failure guard retains the record's extracted fresh names rather
than the caller's fileName variant, and the local file service completes the
not-found swallow the drain ladder's tolerance requires at its Remove step.

## 1. Problem, evidence, and invariant

Observed (issue evidence, reproduced in round 1 of this series): inside ONE
explicit transaction, repeatedly superseding the same primary-key row by
separate statements — `UPDATE point_t SET v=v+1 WHERE id=1` × 5k–30k, likewise
REPLACE and DELETE+INSERT pairs — grows per-statement latency super-linearly
(last/first-quarter ratio 5.18x at 30k; commit only 41.6 ms). The reported CN
profile attributes the growth to per-statement rescans of the transaction-wide
write slice: `Transaction.IncrStatementID`, `mergeTxnWorkspaceLocked`,
`forEachTableHasDeletesLocked`, `ForEachTableWrites`, and
`LocalDisttaeDataSource.filterInMemUnCommittedInserts` /
`workspaceDeleteEntriesLocked`.

Root behavior on the base (verified against merge-base sources): superseded
rows inside the transaction are recorded as pending delete selections
(`batchSelectList`, fed by `deleteTableWrites`) and are shrunk from their
batches at statement admission, but a batch left with zero rows survives as
an inert entry and retired/empty entries are never removed — there is no
compaction anywhere on the base. `txn.writes` therefore grows by one inert
entry per mutation, while `IncrStatementID` runs `mergeTxnWorkspaceLocked`
and the readers above scan the whole slice once per statement: aggregate work
Θ(N²) in mutation count at a live working set of one row. Functional results
are correct on base; the defect is CPU amplification and history-bounded
statement latency. Separately (found while instrumenting the fix), a failed
statement admission returned BEFORE appending its statement boundary, so the
frontend's rollback of the failed statement truncated the PREVIOUS
statement's boundary — a correctness hazard under repeated failed admissions.

Secondary root (rounds 5–16 of this series): persisted-object cleanup on the
failure paths was unbounded or blocked on user-visible paths — failed
publications deleted storage synchronously inside the statement's admission,
a background lane carried unbounded backlog, and `Engine.Close` could
conclude over in-flight transfers. None of these change query results; they
change latency, leakage, and shutdown liveness under failure.

### Invariants (the design's contract)

- **I1 — live-set bounding.** Aggregate per-statement workspace work is
  O(live working set), never O(superseded history): `len(txn.writes)` and
  the pending-selection map stay proportional to live entries after every
  statement boundary that an admission (successful or failed) publishes.
- **I2 — lossless, storage-free ownership moves.** Every
  persisted-but-unpublished object name is owned by exactly one enumerable
  owner at every instant (transaction `pendingObjectCleanup` → engine queue
  → physical deletion), no ownership move performs storage I/O on the
  statement's critical path, and a name whose publication is still IN FLIGHT —
  a producer teardown that raced ahead of its recording consumer inside the
  recording statement's epoch — keeps transaction ownership until the first
  consumer-quiesced boundary after it: it is handed to the deletion queue only
  at a boundary whose statement epoch is later than the retaining statement,
  and only when the boundary's recorded-membership recheck still classifies it
  unrecorded. A name a later record covered never re-enters the queue through
  the pending handoff: its record's own lifecycle (publish, rollback transfer)
  owns the decision.
- **I3 — bounded shutdown completion.** While any name is owned or any
  producer (admitted GC caller, in-flight orphan handoff) is registered,
  `Engine.Close`'s final drain cannot conclude behind a stale observation;
  all shutdown drain work fits one overall deadline, and whatever remains at
  deadline expiry is exactly the crash-mid-queue equivalence class (§6),
  never a stranding of detected-dead names on healthy storage behind a
  concluded drain.
- **I4 — bounded retained backlog.** The engine's retained
  unpublished-object population (queued + live + admitted-in-flight) is
  capped; the cap refuses NEW persistence (fail-closed) and reopens as
  drains retire work; names already owned stay exactly as owned; the
  classification of recorded names against the cap follows the commit's
  TERMINAL outcome — through commit preparation and the commit RPC window
  the prepared transaction's recorded names stay counted, so no competing
  transaction can consume the unit a parked commit still needs, and a
  definite failure transfers owned→queued without increasing the total.
  r6 adds the remote intake to the same gate: a relayed object name joins
  the recording engine through the SAME per-object admission reservation
  the writers hold, and a refusal fails the whole remote run instead of
  booking a fresh obligation behind a saturated bound — so no engine can
  grow another engine's backlog, and under a sustained stalled drain the
  aggregate obligation population across every participating engine stays
  bounded by the sum of their per-engine limits (round-22 finding H2).
- **I5 — adoption precedes remote release, one terminal classification
  per constructed workspace.** Every workspace a statement run constructs
  outside the coordinator's session — the remote-run fragment workspaces
  built by `NewTxnWorkSpace` under a mirror txn operator — receives exactly
  one terminal settlement, and on the negotiated stream protocol the
  transferred classification requires adoption established first: the
  coordinator's transport client adopts every relayed object name at its
  intake — before forwarding the batch into its pipeline, before the ack —
  and the fragment's settlement runs behind the message's batch-flow drain,
  so a transfer implies every relayed name already holds an enumerable
  per-object owner on the recording workspace. A transferred classification
  then releases its reservations and owned membership without deleting
  anything the recording side's adoption owns, and a failed, cancelled,
  receiver-stopped or drain-failed classification retains the fragment's
  unrecorded names through the fragment-local pending list into the LOCAL
  engine's lossless queue. The recording side owns every end class its
  terminals can meet: a cancelled or discarded relay keeps its adopted
  reservation until the terminal's reservation drain adopts it as its last
  reachable owner, and a record whose preparation fails fail-open-retains
  its reservation-less name (round-22 finding H1). No reservation, owned
  membership or pending name outlives the run or transaction that
  constructed or adopted it.

Facts (profile, base-source inspection, parent-base red/green differentials)
are separated from the issue's wording: the Θ(N²) term, the
failed-admission boundary hazard, and the failure-path gaps are treated as
established because the reproduction and the round-by-round NC differentials
evidence each one.

### Measurable success criteria

- Same-PK 2,000-statement UT workload: workspace entries occupied by the
  one-row live working set ≤ 8 at every boundary (including shrink/merge
  slack), independent of statement count; boundary-drained-reader visibility
  exact (`pkg/vm/engine/test/issue_25590_workspace_scaling_test.go`).
- SQL level: the repeated-mutation shapes (UPDATE / REPLACE / DELETE+INSERT)
  stay functionally exact across COMMIT, ROLLBACK, and statement rollback
  after a failed admission
  (`test/distributed/cases/dml/{update,delete,replace}_workspace.*`).
- Failure paths: every injected failure (write/sync/post-persist/close,
  delete stalls, admission saturation, shutdown straddles) keeps I2–I4 and
  returns bounded (the regression battery below; parent-base red → fixed
  green per behavior).

## 2. First owners and consumers

| State / resource | First owner | Enumeration points |
|---|---|---|
| `txn.writes` entries, `batchSelectList`, `txn.offsets` boundaries | the `Transaction` (workspace mutex) | statement admission, readers, `Commit`, `Rollback`, dump |
| persisted-but-unpublished names of a dump in flight | the writing `Transaction` via its dump writer (`CNS3Writer` / `Sinker.PersistedNames()`) | failed-publication epilogue, `Commit`, `Rollback` |
| txn-owned orphan names (`txn.pendingObjectCleanup`) | the owning `Transaction` | next boundary's pre-gate handoff, terminal handoff, live accounting |
| owned obligation names (`e.unpublishedPending.ownedNames` / `live`, r2) | the `Engine` (queue mutex) | record choke at creation, failed-publication epilogue conversion, enqueue moves, commit release, GC settle |
| queued orphan names (`e.unpublishedPending`) | the `Engine` (queue mutex) | enqueue, drain attempts, GC tick, admission snapshot, shutdown triple |
| per-object admission reservations (`inFlight`, r2 — one name per spilling object) | the `Engine` | admission snapshot and discharge (record conversion, epilogue sweep, terminal abandon) |
| producer registrations (`e.gcProducers`) and the drain pipeline (`drainCtx/drainIdle/drainScheduled`) | the `Engine` (gcMu; queue mutex per §5.4 order) | producers, `Close` |
| a dirty object's physical rewrite during compaction (Phase A, r3/r4) | the compaction pass's worker result slot; its persisted names stay in the rewriter's sinker until the parent retakes or records them | Phase B STAGES (r4), failure back-out retraction + retake, staging loop |
| a direct-S3 insert statement's writer (`pkg/sql/colexec/insert`, bound to its workspace r3) | the pipeline's `Insert` operator until mergeBlock's record consumes the metadata, its writer's `PersistedNames()` snapshot thereafter | the record choke, `Insert.Reset`/`Insert.Free` unwind retake |
| a multi_update producer's persisted objects (r4) — insert sinkers and temporary data/tombstone writers | the delegate (`s3WriterDelegate`): each writer's names via `PersistedNames()`, the temp writers' names collected before their deferred close | the workspace retainer at `reset`/`free`, the record choke for the names a record covered |
| a standalone DELETE producer's tombstone objects (r5, `pkg/sql/colexec/deletion`'s flush path) | the `Deletion` operator's container: each flush's writer names collected before the in-flush deferred close, until the merge-delete consumer's `ObjectMeta_ObjectStats` record resolves them | the record choke; `Deletion.Reset`/`Free` handoff to the workspace retainer (r5) |
| publication-in-flight retained names (r5): a producer teardown's unrecorded names, stamped with the retaining statement's epoch | the retaining `Transaction` — the stamp keeps them away from every quiesced=false handoff | mid-statement dumps (`quiesced=false`), boundary admission and terminal handoffs (recorded-membership recheck + epoch filter), the drain |
| a remote fragment's persisted names and per-object reservations (r5; adoption-ordered r6) | the remote run's fragment workspace until its settlement — which runs behind the batch-flow drain, so the coordinator's intake adoption (engine reservation + recording reservation map) has taken every relayed name first | the transport client's `AdoptRemoteTransferredObjectNames` at its intake, `FinalizeRemoteFragmentWorkspace` after the drain (transferred → released, nothing deleted; failed/cancelled/stopped → local queue), the recording side's record choke (admission/convert for adopted names) and terminal reservation drain (never-recorded adoptions, preparation failures) |

(r3) Two consumers above changed ownership shape that revision: the
compaction rewriter no longer records inside its ants worker behind a
local mutex — the worker owns only a result slot while the PARENT, holding
the transaction lock the admission grants, records every finished
replacement and only then runs the infallible tail (§4.5); and the
direct-S3 insert writer joins the reservation surfaces its dump and
compaction siblings already carried, so its spilling objects hold
in-flight units before they persist and its unwind transfers unrecorded
names instead of dropping the last reference to persisted files (§5.3).
(r4) The compaction's Phase B no longer appends per record: every
replacement's record STAGES (owning nothing in `txn.writes` until the
pass's infallible tail publishes them all), and the staging choke's
name membership is retractable while unpublished — the failed pass's
back-out retracts it so the retake can reach the names. The multi_update
producers join the same reservation and retake surfaces: their files
hold in-flight units before they persist, and the delegate's teardown
transfers the unrecorded ones to the cleanup ownership.

Consumers whose cost or liveness this design changes: statement admission and
statement rollback (`frontend` + `compile` + workspace), readers via
`LocalDisttaeDataSource`, RC snapshot/tombstone transfer, `Commit`/`Rollback`,
`GCObjsByIdxRange`/`gcFiles`/`gcFilesToOwner`, `Engine.Close`, and the CN
file service (deletion-only usage).

## 3. Design A — retire and compact superseded entries at statement boundaries (I1)

### 3.1 Mechanism

1. `deleteTableWrites` (marking path, unchanged) records rowids superseded by
   UPDATE/REPLACE/DELETE of transaction-inserted rows as per-batch selections
   in `txn.batchSelectList`.
2. `shrinkWorkspaceEntryBatchLocked` — run at statement admission by
   `mergeTxnWorkspaceLocked` — applies the selections; NEW in this change, a
   batch left with zero rows is retired via `releaseWorkspaceEntryBatchLocked`
   into a nil slot (the base kept zero-row entries alive forever). Residual
   pending marks are consumed and removed there.
3. NEW `compactNilWorkspaceSlotsLocked` (stable-order in-place compaction)
   removes retired nil slots at exactly two call sites — statement admission
   (`IncrStatementID`) and `Commit` — in both cases AFTER the fallible
   dump/merge of the same admission and immediately BEFORE the next statement
   boundary is appended. Recorded statement boundaries therefore never point
   into pre-compaction coordinates, and no statement error is observed
   mid-compaction.

### 3.2 Linearization and state-transition points

- Statement boundary publish: `txn.offsets ← len(txn.writes)` after merge,
  dump, and compaction; compaction is the only writer that moves slots.
- `forEachTableHasDeletesLocked` / `ForEachTableWrites` / reader scans keep
  their `[0, writeOffset)` contracts; their cost collapses from
  O(total history) to O(live entries).
- RC statement path: per-statement transfer state
  (`statementTransferState`) is saved before snapshot advance (base
  semantics kept; re-verified by the RC sweep).

### 3.3 Rollback invariants (review finding B1)

- **Statement-rollback fidelity under failed admission.** The boundary is
  appended even when the admission's merge or dump FAILS
  (`admissionInterrupted = true`), so the frontend's `RollbackLastStatement`
  pops THIS statement's boundary — including its compaction — instead of a
  previous successful statement's. On the base a failed admission returned
  before appending, so the failed statement's rollback truncated the
  PREVIOUS statement's writes (correctness hazard fixed here).
- **Pending-mark retention.** On failure, marks that belong to earlier
  statements must survive; `admissionInterrupted` keeps
  `RollbackLastStatement` from wiping pending-delete state the next
  admission still has to apply.
- **Transaction rollback.** Unchanged truncation-by-boundary with batch
  release; debug-gated accounting asserts
  (`assertWorkspaceAccountingLocked`, base `checkWorkspaceAccountingLocked`
  machinery retained) guard nil-slot and boundary integrity after the new
  paths.

### 3.4 Hot-path and memory budget

- Marking O(sels); shrink O(rows-in-batch); retirement O(1); compaction
  O(len(writes)) per admission, amortized O(1) per retired slot and bounded
  by live entries afterwards. Every consumer scan (admission merge, readers,
  delete enumeration, tombstone transfer) is O(live entries): the Θ(N²)
  aggregate term is removed.
- Memory: workspace batch memory tracks live rows plus the same per-statement
  staging as base; the history-growth term is removed. Per-entry byte
  accounting is base machinery, retained; the full-scan consistency check is
  `DoIfDebugEnabled`-only.

### 3.5 Alternatives (workspace)

- **A-0 status quo.** Θ(N²) CPU at correct results; rejects I1. Rejected.
- **A-1 per-table index rewrite.** Replace `txn.writes` with per-table index
  structures so consumers pay O(live) without compaction (the direction the
  issue text sketches). Correct, but it re-authors every consumer's
  coordinate arithmetic (statement boundaries, dumps, rollback, RC
  transfer) against a new structure — a much wider correctness surface for
  the same asymptotic end state. Rejected.
- **A-2 (selected) retire + compact at published boundaries.** Keeps the
  single array and every existing `[0, boundary)` contract; changes only
  where history lives. Minimal correctness surface: one compaction writer,
  failure-atomic publish, one rollback rule.
- **A-3 compact per mutation.** Compaction during statement execution would
  move slots whose coordinates an in-flight executor holds and pays O(N)
  per mutation chasing an amortized constant. Rejected.
- Draft PR #27013 (cited by the selector) benchmarks different-row growth,
  not same-PK supersede chains; its results do not close this issue's
  reproduction, which A-2 targets directly.

## 4. Design B — atomic dump/statement publication (rounds 5–8, 14–15)

1. **Staged merge with dry-run** (`mergeTxnWorkspaceLocked`): the combined
   batch is fully built — anchor copy, every column append, rowid
   regeneration (`allocateNewRowIds`) — BEFORE any workspace mutation
   publishes it; a mid-merge failure leaves every original source intact and
   the merge re-runnable. The base merged pairs in place and allocated the
   combined batch's rowids after destroying the sources.
2. **Retained staged sources on dump failure:** a failed collection/dump
   keeps its staged batches retryable instead of releasing them with
   half-written published metadata; delete-dump failures keep tombstones
   staged; the publication entry is removed only when the metadata path
   rejects it, so an object never becomes reachable-but-orphaned, and a
   staged object always has a reportable owner.
3. **Ownership snapshot without deletion** (writer side):
   `Sinker.PersistedNames()` returns `DeletePersisted`'s exact snapshot
   (pipeline drained first, same name assembly and
   `normalizeUnpublishedObjectNames`), and `DeletePersisted` is refactored
   over the same assembly with unchanged behavior; `CNS3Writer.PersistedNames`
   mirrors it. The failed-publication epilogue and terminal paths consume
   the snapshot and transfer names; the synchronous `DeletePersisted` is
   gone from the statement path.
4. **Failure-atomic epilogue order:** the failed-publication epilogue takes
   `PersistedNames()`, closes the writer, appends the names to
   `txn.pendingObjectCleanup`, and enters them into the engine's live-orphan
   accounting under the queue mutex (one
   `WORKSPACE-PENDING-CLEANUP-RETAKE` warning) — zero storage calls under the
   statement's locks. The original publication error returns immediately;
   collection happens through the boundary/terminal handoff and the queue's
   drain (§5).
5. **Object compaction keeps the same discipline across every phase (r3,
   review finding B8; r4, review finding F1).**
   `compactDeletionOnObjsLocked` is a strict
   fallible-prefix / infallible-tail split again: Phase A workers run ONLY
   the dirty object's physical rewrite (the reader, the per-block copies, the
   spilling writer — which carries the transaction's capacity option, so
   every replacement object reserves its in-flight unit BEFORE it persists)
   and return `(bat, fileName, persistedNames, err)` to the parent; worker
   failures are DATA, never panics. Phase B (r4) — the parent, under the
   transaction lock statement admission already holds — STAGES every
   finished replacement's record instead of appending it: each staged
   record completes the record body's whole fallible prefix (§5.3's
   epoch fence, the workspace's own batch copy, the record's admission
   choke) into a `stagedWorkspaceRecord` that has committed no workspace
   mutation. THE PASS PUBLISHES NOTHING UNTIL EVERY RECORD STAGED: only
   then does the infallible tail run — the staged records append
   back-to-back, the staged clean-object re-publications publish
   back-to-back, the originals' batches retire, the original objects
   unprotect and GC, and the pending marks clear. A refusal anywhere
   earlier — the hook on a spilling replacement object, a later record's
   own preparation (fence, copy, choke), a filesystem failure inside a
   worker — fails the WHOLE admission with the workspace exactly as
   found: marks and originals intact, every staged record's admission
   RETRACTED (§5.3's recorded-name retraction unwinds the staged choke's
   merge/conversion so the re-takeable names become visible to the
   retainer again), every prepared copy and every staged re-publication
   batch RELEASED (F4: those batches never entered `txn.writes`, so no
   transaction cleanup can free them — the pass itself must, and repeated
   refused admissions may accumulate nothing), and every name the failed
   pass persisted (a finished rewrite whose record never ran, or a
   commit-ambiguous half-rewrite) transferred to the transaction's
   asynchronous cleanup ownership through the same
   `WORKSPACE-PENDING-CLEANUP-RETAKE` move the dump epilogue uses. The
   pre-r3 worker recorded through `writeFileLocked…Known` WITHOUT the
   transaction lock, converted the record's admission error into
   `logutil.Panic`, and the ants pool recovered that panic while
   `waiter.Done` still ran — the swallow-tail GC'd the original objects,
   wiped the marks and returned nil. The pre-r4 parent appended each
   record the MOMENT its own call returned (F1): with two dirty objects
   an earlier successful record survived a later refusal as a live
   workspace entry next to an un-retired original — the failed
   admission's statement boundary sits above such an append, so the
   rollback could not remove it, and the retried compaction re-rewrote
   the same object and committed its surviving rows twice (§5.6
   alternative H rejects the per-record-append shape and its rollback
   compensation).

Alternatives (publication): synchronous deletion on the failed statement
(the base shape for dump-path deletes) — rejected: a stalled or
context-blind deletion parks the statement, cancellation, and the terminal
rollback behind the workspace lock; a trash/double-write namespace —
rejected: doubles S3 I/O on healthy paths to cover a failure case that
transfer alone already bounds; leaving orphans to the published-object GC —
rejected: unpublished objects never enter visible metadata, so that GC has
no enumerable owner for them — exactly the vacuum this subsystem fills.

## 5. Design C — engine-owned lossless cleanup subsystem (rounds 9–16)

### 5.1 The queue (I2)

`Engine.unpublishedPendingQueue` (one queue mutex owns its invariants):
`names` + `nameSet` membership index (kept in lockstep with retirements and
re-admission, giving O(1) duplicate suppression — a later owner can hand the
same name exactly once); `ownedNames` — the live-name ledger of the admission
bound (r2): a name is a member for exactly as long as it neither published
at commit, was physically deleted, nor been enqueued here, with
`live == len(ownedNames)` kept in lockstep, so the set-keyed membership makes
every ownership transition idempotent and duplicate-safe even though the same
name can arrive from several producers (a failed publication's orphan, a
record entry's object, a handed-off GC caller tail); `inFlight`
(per-object admission reservations, r2 — one unit per name a spilling
writer is about to persist); `arrivalSeq` (enqueue version, one bump per
call that adds at least one name); `drainScheduled`/`drainCtx`/`drainIdle`
(one coalesced drain pipeline). Every accounting transition and the whole
transfer window run inside one queue-mutex section: the subsystem's
linearization point. The conservation law (r2): every obligation name
enters the ledger exactly once at creation — a spilling writer's reservation
converted at its record (or swept by its failed publication's epilogue), or
a recorded object admitted fail-closed at its recording write — and leaves
it exactly once (the enqueue moved it to queued ownership, or the commit
published it, or a settle deleted it, or an abandoned reservation's object
never existed).

### 5.2 The drain ladder (liveness under failure)

- `deleteQueuedUnpublishedPendingOnce` partitions the queued snapshot into
  independent batches of `unpublishedPendingDeleteBatch = 1000` names: a
  failing batch keeps exactly its own names and never walls later-queued
  healthy names behind every retry (poisoned names are quarantined, the
  pipeline is not).
- One pool-worker drain task runs at most
  `unpublishedPendingDrainRounds = 8` rounds within
  `unpublishedPendingDrainBudget = 10 minutes` per task, on the engine-owned
  cancellable drain context; a task that exhausts rounds/budget leaves its
  names queued (ownership untouched).
- `gcUnpublishedPendingObjectTicker = 1 minute` paces the engine GC
  scheduler's retry of the backlog with no new orphan — storage recovery
  alone produces cleanup. Scheduling is opportunistic and
  ownership-neutral: a refused/stopped schedule leaves names queue-owned.

### 5.3 Admission bound (I4) — per-object capacity ledger (r2; producer ordering r3)

`unpublishedPendingAdmissionLimit = 500_000` names. The bound counts every
OBLIGATION NAME the engine retains toward the cleanup queue, in object units:

- **Every producer is admitted.** (r2, finding B7.) The pre-r2 gate admitted
  only the workspace dump, so objects a ROLLBACK could hand to the queue —
  direct-S3 metadata records reaching `txnTable.Write` without any dump,
  and successfully dumped objects recorded before the terminal — entered
  behind the bound, and repeated INSERT/ROLLBACK transactions could grow
  the retained backlog without limit while deletions stalled. Now a single
  record choke (`writeFileLocked…Known` on both record bodies) admits every
  recorded object AT CREATION: the record's distinct object names are
  admitted fail-closed before any workspace mutation, so a refusal leaves
  the workspace untouched and no obligation class bypasses the bound.
- **Dumps reserve in object units.** (r2, finding B6.) A dump's writers
  carry the engine's capacity hook (`ioutil.WithUncommittedCapacityHook`):
  `syncFileSinker` consults it with the object's name right before the sinker
  makes the object durable, and `reserveUnpublishedPendingObject` takes ONE
  in-flight unit per object — a writer that spills three objects holds three
  units, and the dump is no longer one slot behind which any number of
  objects could pass. The failed-publication epilogue can therefore no
  longer `live += len(files)` past a bound its single-slot check passed:
  every persisted name already holds its own reservation, and the epilogue
  converts exactly those reservations into ledger membership
  (`convertDumpedReservationsLocked`).
- **Every producer persists behind the reservation surface (r3, finding
  B9).** The capacity hook no longer guards only the workspace dump. The
  direct-S3 insert operator resolves its transaction's workspace through the
  transaction operator (structural `ioutil.SinkerOwner` — the operator's
  package imports no engine) and appends the workspace's option to its writer's
  options; the compaction rewriter's writer carries the same option from its
  transaction; and the sink pool's sync workers consult the hook before a
  pipelined job's `Sync`, so a pipelined writer cannot push objects past a
  bound its non-pipelined sibling respects (a refused pool job persists
  nothing and surfaces only through the writer's Sync). A workspace without
  the owner surface keeps an unhooked writer — the pre-r3 shape: an unhooked
  insert writer persisted its objects at a saturated bound, the record then
  refused them, and the operator's teardown dropped the only surviving
  reference to the files — repeated refused statements leaked one object
  each behind a ledger whose count never included them. With the binding,
  the FIRST gate the insert object passes is its own reservation: a saturated
  bound refuses the spill with nothing half-persisted behind it.
- **Every multi_update producer persists behind the same surface (r4,
  finding F2).** The multi_update S3 delegate constructs THREE more writer
  classes, and r4 binds them all: the persistent per-table insert sinkers
  (`ensureInsertSinkers` — they spill during `append`, long before
  `multi_update.go`'s `source.Write` records the objects) and the temporary
  table writers of `sortAndSyncOneTable`, in both the data and the tombstone
  shape. All resolve the workspace's option through
  `colexec.WorkspaceSinkerCapacityOption(proc)` — the same
  `ioutil.SinkerOwner` structural resolution the insert operator uses,
  shared through one colexec helper instead of an engine import — so no
  producer class reaches its record's choke without passing its own
  per-object reservation first. The pre-r4 delegate built all of these
  unhooked: a slipped object persisted at a saturated bound, the record
  refused the METADATA instead and left the file recorded nowhere, and
  rollback — which enumerates only recorded workspace entries — could not
  list it: repeated large UPDATE/REPLACE failures leaked one object each
  behind a bound whose count never included them, the exact shape the
  review cited.
- **Unwinding writers hand their unrecorded names over (r3, finding B9).**
  Persistence and recording are separated by the pipeline's whole consumer
  chain (writer Sync → mergeBlock → `relation.Write`), so a stream can die
  between them. The operator teardown (`Insert.Reset`/`Free`) re-asserts the
  retainer surface (`ioutil.UnrecordedSinkerObjectsRetainer`) and hands the
  writer's `PersistedNames()` snapshot to the workspace BEFORE the close
  drops the tracked references: `disttae.Transaction.RetainUnrecordedS3Objects`
  filters out every name whose record DID land (its deletion is owned by the
  record's lifecycle — rollback's queue handoff or commit's release) and
  takes over only the UNRECORDED remainder into `txn.pendingObjectCleanup`
  plus the capacity ledger — converting each persisted name's reservation
  into owned membership and retaining hook-less extras fail-open, because
  the only alternative to counting an already-physical object is dropping
  its last cleanup owner. The compaction back-out reuses the same filter:
  a finished rewrite whose record never ran transfers its names whole,
  a committed-side record's names never re-enter. Double teardown is
  harmless (the writer nils after the first close) and the WARN vocabulary
  stays §6's `WORKSPACE-PENDING-CLEANUP-RETAKE` with the unwind reason.
- **The multi_update delegate's teardown keeps the same ownership (r4,
  finding F2).** Its temporary writers CLOSE inside `sortAndSyncOneTable` —
  the metadata record runs only in the caller, after the function returned
  — so the delegate collects each temp writer's `PersistedNames()` before
  the deferred close drops the tracked references (registered before the
  close, LIFO) and HOLDS them until its `reset`/`free`, the only teardown
  left running after the record attempt resolved, hands the union — the
  collected temp names plus every insert sinker's snapshot — to the
  workspace's retainer BEFORE resetting or closing them. Names a
  successful statement's records covered are filtered out by the
  workspace itself (its recorded-name filter owns the decision), so an
  operator reused by a later statement never double-owns a live object,
  and the second teardown hands nothing. A workspace without the retainer
  surface keeps the pre-fix bare teardown: collections drop silently and
  no panic fires — the same compat shape the insert operator keeps.
- **Record bodies STAGE fallibly, publish infallibly, and their admission
  is retractable (r4, findings F1/F3).** Both record bodies
  (`writeFileLocked…Known` and the CCPR skip-transfer twin) are now a
  stage/publish pair: the stage completes the epoch fence, the
  workspace's own batch copy, and the record's admission choke — in that
  ORDER — without touching any workspace state, and only the infallible
  publish (the CN-segment registry, the cnObjs summary, the workspace
  entry append) mutates state, so a single-record caller keeps its exact
  pre-existing behavior while the compaction pass (§4 item 5) may hold
  several fully staged records and publish them back-to-back. The
  copy-BEFORE-choke order is the F3 fix: an allocation failure between
  persistence and recording leaves NO recorded-name membership and
  converts NO reservation — the failed record's persisted objects stay
  reachable through the unwinding retainer, their in-flight units settle
  at the retake or the terminal drain, and the file is deleted with the
  ledger balanced. The pre-r4 order registered the name and converted
  the reservation BEFORE the copy: a failed copy left the object marked
  as recorded while no workspace entry existed for it, so the retainer's
  filter skipped it, no rollback enumeration listed it, and the terminal
  dropped its ledger membership without deleting it — a stranded file no
  owner could reach. A refused choke AFTER a successful copy cleans the
  prepared copy, consuming nothing. The staged choke's merge remains
  RETRACTABLE while its record is still staged: the compaction pass's
  back-out retracts every staged record's admitted names from
  `recordedUnpublishedObjectNames` (the retraction is safe because the
  names are freshly persisted replacements of this same pass and the
  transaction lock serializes the choke against the back-out) before the
  general retake runs — the owned LEDGER membership the staged
  conversions created stays in place, exactly the cleanup obligation the
  queue's drain discharges after the re-take.
- **Reservation bookkeeping under concurrent hooks (r3).** The hook now fires
  on writer goroutines that hold NO transaction lock — compaction workers,
  the insert pipeline, the sink pool — so the transaction's private
  reservation map (`unpublishedDumpReservations`) gained its own small guard
  (`unpublishedDumpReservationsMu`): every section is short, never reaches
  back to the transaction lock or the engine queue, and one lock order holds
  everywhere (`txn.Lock → reservations guard → engine queue mutex`). The
  capacity LEDGER and its bound check stay exactly where r13/r18 put them —
  behind the engine queue's single mutex, the linearization point every
  accounting transition shares; the record choke's split became read-only,
  deleting the reserved names only in the same move that converts them,
  so a refused admission still consumes nothing.
- **Admission check.** Admit iff `queued + owned + inFlight(+fresh) ≤
  limit`, all inside the one queue-mutex section. The dump case counts the
  single object being reserved (`queued+owned+inFlight` after the increment);
  the record case counts only the names that are neither members nor queued
  (duplicate records spend capacity once). No torn total is observable;
  concurrent spilling writers and recording transactions share the remaining
  capacity through their per-name reservations instead of racing past the
  bound.
- **Conversion, release, and settle.** A successful dump's record converts
  its reservations into owned membership (the choke); a failed publication's
  epilogue converts exactly the names it persisted (and retains any persisted
  name whose reservation it never saw — an impossible-in-production
  robustness path); the commit publishes this transaction's still-owned
  recorded names out of the ledger; an ordinary rollback's queue handoff
  moves them owned→queued (lossless, capacity neutral); the CCPR branch
  deliberately releases to the txn cache's shared ownership; a GC caller's
  finished batch settles its names out of the ledger; and a transaction
  ending with reservations still outstanding — the r6 case the round-21
  protocol abandoned — leaves them to the terminal's reservation DRAIN
  (r6, finding H1): CCPR abandons its shared units exactly, while every
  other transaction's leftovers (a dump interrupted between spill and
  record, an adopted relay whose record the coordinator never reached)
  adopt the terminal as their last reachable owner — the drain retains
  them into the pending list and converts their in-flight units into
  owned membership, the terminal's handoff carries them to the engine's
  queue, and the drain's not-found tolerance retires them. No unit can
  outlive its transaction; no member can double-count.
- **Refusal surface.** Refusal fails the object's writer spill or the
  recording write with the internal error "workspace admission paused: N
  retained unpublished storage objects reached the admission bound" and
  logs `WORKSPACE-UNPUBLISHED-PENDING-ADMISSION` with retained/live/inflight
  counts (objects key for records, object key for spills); admission
  reopens automatically as drains retire work. All names already owned —
  including per-transaction ones — remain owned and hand off losslessly
  until then. A saturated engine therefore degrades exactly like a slow GC:
  bounded memory, bounded blast radius, self-healing. r6 splits the refusal
  feed by class (finding H2): RESERVED names refuse only where they already
  hold an in-flight unit — the reservation surface gated their persistence,
  so a refusal converts or keeps them without growing anything; RELAYED
  names refuse AT THE COORDINATOR'S INTAKE with the whole run failing, so
  nothing books and the fragment engine saturates its own bound instead of
  recycling capacity per clean round; and the record choke's fail-open
  retain stays exactly the r19 last-owner doctrine for the
  already-physical reservation-less class — whose in-tree feed the two
  gates leave empty (every in-tree writer reserves; every relayed name
  adopts) — so on this tree a sustained stalled drain pins the aggregate
  population at the engine bounds instead of counting refusal rounds up. Two
  findings of the first r22 validation round refine the implementation within
  the same decisions: the preparation-failure guard retains EXACTLY the
  record's extracted reservation-less names (the choke's own fresh-name
  filter) rather than the caller's fileName argument — a compaction staged
  replacement passes a delta-location variant whose retention would have
  created a phantom cleanup member beside the canonical replacements its
  writers already reserved — and the local file service's `deleteSingle`
  completes its own documented not-found contract at the Remove step (a
  concurrent deletion between the Stat and the Remove now reads as the
  not-found outcome it is): the drain ladder's not-found tolerance — the r10
  doctrine the two concurrent scheduled drains this design legitimately
  creates — depended on that swallow holding under the race.
- **Classification waits for the consumer to quiesce (r5, finding G1).** A
  producer teardown CAN run before its recording consumer finishes: the
  pipeline tears a sender scope down while batches it emitted still sit in
  the channel a receiving scope has not yet written into
  (`pkg/vm/pipeline/types.go` `cleanupSenderReceiverPipeline` runs the
  sender root's Reset concurrently with the receiver's merge teardown;
  `process_spoolr.go`'s deferred spool release acknowledges no completion),
  so `Insert.Reset`/`Free` and the delegate's `reset`/`free` retain names
  that the SAME statement's consumer may still record a moment later. Two
  guards separate those in-flight names from definite orphans. First, the
  retain stamps every name with the retaining statement's epoch, and a
  mid-statement handoff (`dumpBatchLocked` at `offset≥0`, i.e. inside the
  recording statement) is `quiesced=false`: it moves NOTHING stamped with
  the current epoch — those names keep transaction ownership exactly as
  long as their consumer could still record them. Second, every handoff
  that CAN run at a quiesced boundary (the next statement's admission, the
  commit-prepare dump at `offset<0`, and every terminal) rechecks the
  transaction's recorded-name set first: a name whose record has landed —
  during the statement, or during the commit-prepare window — stays with
  its record's lifecycle (publish on commit, rollback's queue transfer) and
  never enters the deletion queue through the pending handoff; a name still
  unrecorded at a quiesced boundary is a definite orphan because no
  recording consumer can run for it anymore. The engine drain, in turn,
  only ever deletes names that entered a queue — and every queue entry now
  enters behind one of those two boundary classifications or behind a
  rollback/terminal transfer, so the drain never needs a recorded-set
  probe of other transactions' state. The round-19 seam test exercised a
  failed teardown against a stub retainer, so the pre-r5 tree moves
  exactly the wrong shape at the mid-statement dump — the in-flight name
  reaches the queue blind, and the very next drain deletes a live or
  committed object.
- **Capacity classification follows the commit's terminal outcome (r5,
  finding G3).** The workspace's commit preparation (`Transaction.Commit`:
  `genWriteReqs`) only DRAFTS the payload; `operator.go` still runs the
  commit RPC and classifies the outcome after it, and the r18/r20 prepare
  released every recorded name's ledger membership right after the draft —
  so at `limit=1` a parked commit freed its unit, a competing transaction
  consumed it, and the first commit's definite failure then enqueued its
  own object: two retained obligations behind a limit of one. r5 keeps the
  prepared names counted through the entire prepare/RPC window and settles
  them ONLY at the terminal classification the caller already performs:
  `FinalizeCommit` releases the recorded names as published (no deletion);
  `Rollback` — the definite-failure path the operator drives on a rejected
  commit — transfers owned→queued exactly (its `gcObjsByIdxRangeToOwner`
  sweep and `handoffUnpublishedPendingLocked` already move the names in one
  queue-mutex section) and then drains the remaining reservations; and
  `FinalizeCommitWithUnknownResult` releases the membership WITHOUT any
  deletion (the commit may have landed at TN — the pre-existing ambiguity
  doctrine). Every terminal drains the transaction's outstanding dump
  reservations, so no in-flight unit can outlive its transaction no matter
  which outcome class it takes.
- **The standalone DELETE tombstone producer joins the protocol (r5,
  finding G4).** A public DELETE path the r3/r4 bullets did not cover:
  `compileDelete`/`newDeleteMergeScope` ships a `Deletion` operator whose
  `container.flush` builds a TOMBSTONE writer per partition
  (`newDeletionTombstoneWriter`), persists at its own threshold
  (`SetCNFlushDeletesThreshold`), and closes the writer INSIDE the flush —
  while the flush's consumer is the downstream merge-delete scope whose
  `ObjectMeta_ObjectStats` batches reach the coordinator's
  `txnTable.Delete` record path only after the flush returned. r5 binds
  that constructor through `colexec.WorkspaceSinkerCapacityOption(proc)`
  (the sink persists behind its own reservation), makes the flush collect
  the writer's `PersistedNames()` BEFORE the in-flush deferred close (the
  delegate's collect-then-close shape), and drives the collected names
  through the workspace retainer at `Reset` and `Free` — under the SAME
  epoch stamp and boundary classification as every other producer, so a
  flush whose stats the merge-delete consumer still owes stays in flight
  until the consumer quiesces, and a refused record leaves the file to the
  unwind handoff exactly like the delegate's writers. The pre-r5 tree
  leaves this producer unhooked: a saturated bound lets the tombstone
  persist, the refused record leaves no workspace entry for rollback to
  enumerate, and the in-flush close drops the only name reference — the
  exact pre-r3 insert leak shape on the delete side.
- **Remote-CN producers settle behind the adoption barrier (r5 finding G2;
  ordering tightened r6, findings H1/H2).** A remote fragment re-throws the
  r3/r4 machinery onto a second engine: `remoterunServer` rebuilds a MIRROR
  txn operator per run message (`NewWithSnapshot`, never registered in the
  remote client's active set), binds a FRESH `disttae.NewTxnWorkSpace` to it,
  and runs the fragment's writers through the r19/r20 bindings — so the
  per-object reservations the hook takes land on the REMOTE engine's ledger,
  while the metadata records run on the COORDINATOR (the relay batches ride
  the pipeline's own data stream: `multi_update`'s `UpdateWriteS3` emitters
  build the five-vector `[action, tableID, rowCount, name, serializedBatch]`
  output, and the coordinator's `UpdateFlushS3Info` consumer records through
  the coordinator's relation and workspace; only the coordinator's workspace
  payload reaches TN). The r5 protocol classified transfer from the run's
  error alone, which the round-22 review rejected twice over. First (H1),
  the wire's acknowledgment only proves the recording side DECODED each
  batch — a consumer-less ack, `receiverDone` discards included — so a clean
  fragment freed reservations and retained names while cancellation between
  the clean end and any record, or a record's own preparation failure
  (epoch fence, keyed stage refusal, batch copy), or a discarded metadata
  batch left the physical file enumerated by no workspace at all, per run
  and cumulatively across runs. r6 orders the ownership instead: the
  coordinator's transport client extracts the relayed names from every
  decoded flush-info batch (`multi_update.TryExtractRelayS3InfoObjectNames`
  — the delete name column plus the insert's serialized BlockInfo, exactly
  the names the recording consumer derives) and ADOPTS them at its intake
  BEFORE forwarding and BEFORE the ack, through
  `AdoptRemoteTransferredObjectNames`: every name not already recorded or
  reserved takes one `reserveUnpublishedPendingObject` unit (the same
  fail-closed admission the writers pass — H2's gate below) plus a booking
  in the recording transaction's reservation map, so from the moment the
  bytes leave the wire an ENUMERABLE owner holds the name. The fragment's
  terminal settlement then moves behind the message's batch-flow drain —
  the adoption barrier: the client adopts before it acks, so a drained flow
  proves every relayed name adopted; a clean, non-stopped, drained run
  TRANSFERS (releases the fragment's reservations and owned membership
  without deleting anything — the recording side's adoption and its records
  own the fate: publish on commit, rollback transfer on failure), and every
  other ending — run failure, drain failure (cancellation), a
  receiver-stopped stream whose trailing batches never reached the
  coordinator — settles through the ordinary rollback terminal and keeps a
  reachable deletion owner on the fragment's LOCAL engine for exactly the
  names whose records can still land nowhere. The settlement runs before the
  run message's deferred `Compile` release because a non-transferred
  settlement is a full workspace `Rollback`. The recording side closes its
  own end classes at its own terminals: a cancelled or discarded relay's
  adopted-but-never-recorded name reaches `FinalizeCommit` /
  `FinalizeCommitWithUnknownResult` / `Rollback` with its reservation still
  outstanding, and the r6 terminal reservation drain adopts it as the
  terminal's last reachable owner (retains it into the pending list, hands
  it to the recording engine's queue, converts its in-flight unit exactly)
  instead of abandoning it; every record-preparation failure — the epoch
  fence, the keyed stage refusal, the batch copy — fail-open-retains the
  record's reservation-less names before it returns (the same last-owner
  doctrine r19 set), and CCPR keeps the shared-file exemption throughout.
  A stream that negotiated no batch flow (a pre-negotiation peer) keeps the
  r5 clean-run classification: the peer cannot prove adoption, and the
  cross-version residual stays §5.6's legacy scope.

The r1 shape (one in-flight slot per dump batching the whole publication,
epilogue crediting every persisted file into `live`) is the rejected
alternative (C-E in §5.6): it kept the linearization simple but let any
multi-object publication overdraw the ledger by its extra objects —
the pre-PR review's counterexample: at `limit−1` retained names, one
admitted three-object dump exceeded the cap by two once its reservation
released, with nothing refused in between.

### 5.4 Producer protocol and coordinated shutdown (I3)

- **Producers.** Admitted GC callers (`gcFiles`/`gcFilesToOwner` while names
  are between snapshot and owner) and transaction-owned orphan handoffs
  (`registerOrphanHandoffProducer`) both increment `e.gcProducers` under
  `gcMu` for exactly the window in which their names are enumerable by no
  owner. Registration happens BEFORE the window; the decrement runs strictly
  AFTER the enqueue, on every handoff path.
- **Transfer window.** The orphan handoff's clear-to-enqueue section runs
  WHOLE under the queue mutex (`handoffUnpublishedPendingLocked`), with
  scheduling outside the lock. Queue-mutex readers therefore observe a
  transfer pre-registration (names still transaction-owned), mid-registration
  (producers ≥ 1, the reader excluded from the window), or post-enqueue
  (names queued, arrival horizon bumped) — never a half-state.
- **Shutdown observation.** `Engine.snapshotShutdownObligations` reads
  `(producers, queued, arrivals)` as one consistent triple — queue mutex,
  then `gcMu` (the package's only nesting of the two; audited deadlock-free:
  no site acquires the queue mutex while holding `gcMu`). `Engine.Close`'s
  final drain consumes that triple for every decision, and its no-progress
  rule decides on a FRESH triple taken after its own deletion attempt,
  continuing when the attempt succeeded, retired something, or saw arrivals
  move (`attemptErr == nil || remaining < queued || arrivalsAfter !=
  arrivals`) and concluding only on a fresh no-progress triple with
  `producersAfter == 0`. A handoff registering or an enqueue arriving
  mid-attempt therefore holds the drain open instead of being concluded over
  from a stale count. All shutdown drain work — cancel, bounded join of a
  running pipeline, pool release, and the final attempt loop — fits
  `unpublishedPendingCloseDrainTimeout = 10 s` from `Close`'s entry; at
  expiry the remaining work detaches with ownership intact (§6).

### 5.5 Resource bounds

| Resource | Bound | Enforcer |
|---|---|---|
| queue names/set memory | O(unique admitted names) | admission §5.3 + dedup §5.1 |
| owned-name set (live ledger) + per-name reservation map | O(unique admitted names in flight of ownership) | admission §5.3 |
| retained obligation population | 500,000 names — queued, owned, and in-flight alike | `unpublishedPendingAdmissionLimit` |
| objects per admitted producer (dump, compaction rewrite, direct-S3 insert, multi_update insert sinker or temp data/tombstone writer, standalone DELETE flush writer (r5), pipelined pool job) | the producer's own remaining capacity | per-object hook §5.3 |
| staged per-pass mpool footprint (compaction re-publication batches, staged record copies, prepared record batches) | zero across arbitrarily repeated refused admissions | failure back-out releases every staged batch and copy before the return (§4 item 5); pinned by the CurrNB equality asserts of the round-20 regressions |
| in-flight publication names held through a recording statement (r5) | the statement's own retained names, epoch-stamped and re-checked per boundary | `quiesced=false` handoffs move nothing of the current epoch; quiesced boundaries recheck recorded membership (§5.3 G1) |
| capacity behind a parked commit (r5) | the parked prepared transaction's own recorded names — one unit each, for its whole prepare/RPC window | commit-terminal classification (§5.3 G3): release at `FinalizeCommit`, owned→queued transfer at `Rollback`, release-without-deletion at unknown |
| remote fragment owned names and reservations (r5; intake-ordered r6) | each remote run's own fragment footprint, settled exactly once, only after the batch-flow drain proved the coordinator's intake adoption | `FinalizeRemoteFragmentWorkspace` behind the run's drain (§5.3 remote bullet / I5) |
| relayed names adopted at the recording intake (r6) | the recording engine's remaining capacity — one `reserveUnpublishedPendingObject` unit per adopted name before the first ack | `AdoptRemoteTransferredObjectNames` at the transport client's intake (§5.3 remote bullet, finding H1) |
| aggregate obligation population across the fragment and recording engines under sustained refusal with deletion stalled (r6) | the SUM of the per-engine admission limits — refusals fail whole runs, the fragment keeps its own units, and the saturated fragment's writers then refuse further spills | the intake admission gate + the transacted reservation surface (§5.3 refusal surface, finding H2) |
| pool workers engine-wide | 5 | `GCPoolSize` |
| concurrent drain pipelines | 1 (coalesced) | `drainScheduled` |
| rounds per drain task | 8 | `unpublishedPendingDrainRounds` |
| drain task wall time | 10 minutes | `unpublishedPendingDrainBudget` |
| names per batched delete attempt | 1000 | `unpublishedPendingDeleteBatch` |
| retry pace | 1 tick per minute | GC scheduler ticker |
| shutdown drain work per close | one 10 s deadline (test-pinnable) | `unpublishedPendingCloseDrainTimeout` |

### 5.6 Alternatives (cleanup/shutdown)

- **C-A status quo.** Synchronous or dropped failure-path cleanup:
  statement-synchronous deletes stall admission and rollback behind storage;
  dropped names leak without an owner; `Close` concludes over in-flight
  transfers — violates I2/I3. Rejected.
- **C-B per-failure goroutines.** One deferred goroutine per orphan set:
  unbounded goroutine count, no single owner after engine teardown, no bound
  on retained backlog — violates I4, harder to test deterministically.
  Rejected.
- **C-C queue with the statement path waiting for capacity.** Bound at
  admission but parks the user statement behind queue retirement (an
  unbounded in-statement wait) — violates I2's storage-free statement path
  and moves the stall from GC into the user path. Rejected.
- **C-E (r1's, now superseded) one slot per dump.** The first admission
  shape reserved one in-flight unit per `dumpBatchLocked` call, independent
  of how many objects the publication actually persisted, and credited
  every persisted name into `live` on failure. Simple, but any multi-object
  publication overdrafts the ledger by its extra objects, and recorded
  obligations not behind a dump were never counted at all (findings B6/B7).
  Rejected at r2.
- **C-F admission without the sinker hook (per-DUMP check with epilogue
  repayment).** Rejected: the epilogue cannot return capacity for objects
  it was never checked against, and any post-hoc accounting keeps the same
  overdraft window between reservation and persistence.
- **C-G (r19's findings, now superseded) swallowed worker panics and unbound
  insert writers.** Letting the compaction worker panic its record refusal
  away into the ants pool (which recovers the panic) with a nil-returning
  tail keeps failure-atomicity UNREACHABLE: the tail had already retired the
  originals' metadata before the record could fail, and nothing after the
  panic can restore it — the rejected replacement's surviving rows simply
  vanish (finding B8). Deleting the refusals' persisted objects is no
  better; the marks needed for retry were wiped with them. Likewise,
  binding the capacity ledger at the RECORD while the insert writer persists
  freely behind it leaves the refused statement's physically persisted
  objects ownerless at the teardown's close — a leak the bound's count
  never sees and a rollback can never enumerate (finding B9). Both shapes
  rejected at r3: all fallible preparation and admission must precede any
  original retirement, worker failures must travel as data, and every
  direct-S3 writer must reserve before it persists or hand its unrecorded
  names over at unwind.
- **C-H (r20's shapes, rejected) per-record append inside Phase B,
  record-after-copy, unreleased staging.** Appending each replacement
  record the moment its own call returned — with the back-out merely
  skipping the already-recorded entry — keeps Phase B trivial but destroys
  the pass's atomicity across REPLACEMENTS (finding F1): with two dirty
  objects an earlier record survives a later refusal as a live entry next
  to an un-retired original, the failed admission's statement boundary
  sits above the append so the rollback cannot remove it, and the retried
  compaction commits the same object twice. Compensating at rollback by
  walking past the failed boundary, or undoing an appended entry by
  index, was considered and rejected: the boundary arithmetic is shared
  with the frontend's statement ceremony, and index-level undo
  re-introduces the crash-window discipline the staged publication removed.
  Likewise rejected: registering a record's names BEFORE its batch copy
  (finding F3) turns a copy failure's objects into recorded-but-ownerless
  files no enumeration can reach, and leaving the staged re-publication
  batches unreleased on a failed pass (finding F4) accumulates one
  allocated batch per clean object per refused admission inside the
  workspace's own mpool. All three shapes are replaced by the staged
  record + retraction + release back-out of §4 item 5 and §5.3.
- **C-D (selected).** One engine-owned queue with per-batch liveness,
  per-OBJECT fail-closed admission covering every producer (dumps via the
  sinker capacity hook, records via the creation-time choke), producer-
  registered queue-mutex-atomic handoffs, and a one-deadline coordinated
  shutdown. Costs one queue struct, one coalesced worker, one owned-name
  set, and a bounded close tail; buys each of I2/I3/I4 individually pinned
  by dedicated deterministic regressions (§7).
- **C-I (r21, rejected as a WIRED protocol; r6 closes the residual without
  it).** Extending the write-back stream with a per-batch or per-object
  consumption/denial acknowledgment protocol — the producer releasing its
  names only on the coordinator's reply — would close the clean-fragment
  residual the round-21 record documented, but it adds a
  serialization/protocol surface in `pipeline` internals and a remote-side
  wait on the fragment's critical path, exactly the class of blocking the
  queue design removes. r6 closes the residual WITHOUT any wire change —
  still rejected: the relayed S3-info batches already carry every object
  name in band, so the recording client adopts at its own intake
  (`AdoptRemoteTransferredObjectNames`, fail-closed through the same
  admission reservation the writers hold), and the existing batch-flow
  drain — the ack the negotiated protocol already sends — becomes the
  adoption barrier the fragment's settlement waits behind. The wired
  variant recorded in r21 would add a protocol surface to buy what the
  r6 ordering owns structurally; it stays rejected.
- **C-J (r21, rejected) global lease registry keyed by name.** Having the
  drain consult a cross-transaction, cross-engine "possibly
  recorded-anywhere" registry before deleting would let the queue admit
  unsafe names and filter at delete time, but the registry's truth would
  still be distributed and its staleness would re-create the very
  membership race G1 removes. r5 keeps the classification at the
  retaining transaction's boundaries, where the recorded set is local and
  lock-ordered. Rejected.
- **C-K (r21, rejected) recorded-set probing at drain time.** Keeping the
  mid-statement handoff and having `deleteQueuedUnpublishedPendingOnce`
  probe every workspace for recorded membership before each batch deletes
  requires the drain to hold every transaction's lock in an arbitrary
  order — the engine outlives transactions, so the probe set is not even
  enumerable — and still races a record against the probe's decision. The
  epoch stamp plus the boundary recheck remove the need for any probe:
  the queue only ever receives definite orphans. Rejected.

## 6. Compatibility, restart, and operations

- **No durable-contract change.** No catalog, schema, wire, protocol, or
  on-disk format change. Unpublished-object names are CN-local file-service
  objects that were unreachable garbage on base and remain unreachable
  garbage here; the subsystem changes who reliably deletes them, and when.
- **Upgrade/downgrade/mixed version.** The state is process-local; a
  rollback or downgrade leaves at most the crash-class leak (below) —
  harmless on storage. No migration, no launch-phase coupling.
- **Restart semantics (review finding B2).** Queue and transaction lists are
  process-local; at process exit, names never handed off/queued/collected
  leak on the file service. This leak class is identical to a crash mid-queue
  and to the pre-existing behavior for any process killed mid-publication on
  base: accepted garbage that no metadata path can reach. The design
  contracts every non-crash terminal path in-process; it deliberately does
  NOT add a startup listing scan (the cooperative equivalents — the writer
  epilogue, the GC tick, and the admission WARN family — are the reachable
  remediations; a listing scan would add a new startup I/O contract for no
  correctness gain).
- **Configuration.** The bounds of §5.5 are source constants; the deadline
  budgets and the admission limit are package vars (test-pinnable) so CI and
  soak deployments can pin them. No new user-visible knobs in this change
  (an exported knob is an open decision, §8).
- **Observability.** A stable WARN family:
  `WORKSPACE-PENDING-CLEANUP-RETAKE` (an unwound or failed publication's
  orphans transferred at a boundary or teardown — r3 adds the direct-S3
  insert writer's unwind and the compaction back-out as retake reasons
  under the same key), `WORKSPACE-UNPUBLISHED-PENDING-KEEP` (a failed
  delete batch retained), `WORKSPACE-UNPUBLISHED-PENDING-ADMISSION`
  (bound hit: an object's spill or a recording write errors while the
  backlog drains — r2 broadened the refusal surface from whole dumps to
  single objects and records, r3 extends it to every spilling writer
  including the direct-S3 insert writers and pipelined pool jobs, so the
  same family tags both producer classes),
  `WORKSPACE-UNPUBLISHED-CLEANUP-SCHED`/`NOPOOL`/`DRAIN-JOIN-TIMEOUT`.
  Operator actions: wait for GC retirement (minutes scale; staged per-batch
  retries), fix the storage-side failure if KEEP lines persist, restart the
  CN only as a last resort (leak class above).
- **Security/tenancy.** Object names are random UUID-based; no user data in
  names or logs; the admission bound doubles as per-CN DoS containment; no
  new trust boundary is created.

## 7. Validation criteria (cheapest deterministic coverage per invariant)

- **I1 — scaling.** The issue workload's exact shape at component level with
  the production statement drive (`StartStatement → IncrStatementID →
  UpdateSnapshotWriteOffset → ops → EndStatement`): 2,000 same-PK updates,
  1,000 delete+insert pairs, ≤8-entries live-set cap at every boundary,
  drained-reader visibility oracle
  (`pkg/vm/engine/test/issue_25590_workspace_scaling_test.go`) — red on the
  base (entry count grows with statements), green after. SQL-level truth of
  the same shapes across COMMIT/ROLLBACK and statement rollback after a
  failed admission: `test/distributed/cases/dml/{update,delete,replace}_workspace.*`
  (result files in the diff; generated results were each inspected against
  the intended semantics).
- **I2 — publication/ownership.** Staged-merge dry-run and retained staged
  sources pinned by the dump sweep; `PersistedNames`'s snapshot semantics
  pinned by the sinker package's own test; the failed-publication epilogue's
  zero-storage-calls contract pinned by
  `InsertDumpPostPersistFailureCleansOrphans`, `FailedPublicationAdmitsWhileDeletionStalls`
  (insert and delete legs: the statement returns while the deletion stays
  parked), and the dump/rollback family.
- **I3 — handoffs/shutdown.** Deterministic parking-barrier stagecraft
  (context-blind FS delete stalls, one-shot failures, seams parked
  mid-window; small bounded oracles only, no wall-clock-dependence for
  verdicts): `CloseWaitsForParkedOrphanHandoff` (window holds the queue
  mutex; Close cannot pass it),
  `LateOrphanRegistrationCollectedBeforeShutdown` (registration behind the
  drain's captured count must still be collected before shutdown),
  `CloseCancelsBlockedDrainWithinBudget`, `CloseDetachesUncancellableCleanupWithinBudget`,
  `TerminalAndCloseReturnWhileGCAdmissionStalls`, and the engine lifecycle
  suite (`pkg/vm/engine/test`). Standard of record for each: the identical
  test run against the previous-round production files (NC differential)
  must fail at a cited interleave with the finding's message, and pass on
  the fixed tree — every round's red/green is archived with sha-verified
  restores.
- **I4 — admission.** Saturation and refusal
  (`FinalizationPromptUnderSaturatedGCPool`, `RejectedScheduleRecoversOnEngineGCTick`,
  `ExhaustedLadderRecoversOnEngineGCTick`), the retained-backlog bound
  (`AdmissionBoundsRetainedOrphanBacklog`), concurrent admissions sharing
  remaining capacity (`ConcurrentAdmissionsCannotBypassTheBound` — r2
  per-name reservation seam: 8 racers against a held boundary unit), and the
  queued-vs-live accounting identity across ownership transfers
  (`AdmissionPreservesTotalAcrossOwnershipTransfers` — r2 reservations
  converting through the failed-publication epilogue). The r2 producer
  coverage (findings B6/B7): `DumpReservesCapacityPerPersistedObject`
  (boundary leg: a real multi-object dump at `limit−1` — the first object
  takes the last unit, the second spill is refused before anything of it
  persists, the epilogue converts exactly the one persisted object, and the
  ledger sits EXACTLY at the limit; concurrent leg: two one-object dumps
  racing one free unit — exactly one records, the ledger never crosses), and
  `RollbackProducersEnterTheCapacityBound` (record-time admission: eight
  one-block records fill the bound, the ninth is refused at the recording
  write with the workspace untouched; rollback hands exactly the eight;
  sustained record/rollback rounds add nothing; a drain retirement re-opens
  admission; the commit's settle retires its released names exactly once).
  NC standard of record: both regressions' r18 message text must be red on
  the round-17 production files (the NC probes drive the same
  publication/record shapes through pre-r18 surfaces) and green on the fixed
  tree.
  The r3 producer-coverage additions (findings B8/B9):
  `CompactionRefusalRetainsDirtyObjectSources` (white-box: a real mixed
  metadata batch + pending marks; the pinned bound refuses the dirty
  replacement's persistence, the admission error propagates, the marks and
  every original object stay intact, nothing published/recorded/retained;
  the retried pass under the restored bound completes — republications,
  replacement record, retirement, mark clear, ledger shape),
  `CompactionAdmissionRefusalPreservesRowsAndCommits` (engine level: the
  full statement/rollback ceremony around the refused admission, exact
  surviving-row reads before and after, retry compaction, commit, and a
  fresh transaction's exact read),
  `DirectWritesReserveAndRetainOnUnwind` (the writer's binding probe;
  repeated saturation rounds persist NOTHING; the persisted+recorded leg
  converts its own reservation; the unwind leg hands ONLY the UNRECORDED
  name to the cleanup ownership and the drain deletes the physical orphan
  while the recorded object's file stays untouched), and the insert
  operator seam tests (bind-on-Prepare, retake-on-Free with its idempotent
  second Free, compat without the owner surface). NC standard of record
  for r19: the compaction probe must be red on the round-18 files (nil
  admission error behind the ants-recovered panic, marks wiped, original
  metadata batch published) and the insert-leak probe red there (the
  persisted object without ANY cleanup owner after the refused record and
  the dropped writer), both green-or-excised on the fixed tree with the
  same staging.
  The r4 additions (findings F1–F4):
  `TwoDirtyObjectCompactionIsAtomicAcrossReplacements` (white-box: a
  three-object batch with TWO dirty objects under a bound granting ONE
  reservation; the sequential single-slot compaction worker makes the
  first rewrite persist and the second refuse while the first record is
  FULLY staged — NO append survives the failed pass, the staged admission
  retracts to the dump-only recorded set, the clean object's staged
  re-publication batches release with mpool CurrNB equality across three
  repeated refusals, the orphaned replacement joins the cleanup
  ownership and the drain deletes its file while every original stays
  kept; the retry rewrites both dirty objects exactly once),
  `RecordFailureAfterStagedReplacementRetracts` (the record-failure leg:
  two tables' rewrites both persist, the first table's record fully
  stages, the SECOND table's record fails its preparation through the
  keyed `FJ_CNWorkspaceRecordStageFailed` seam — writes stay at exactly
  the two metadata entries, both retracted/unrecorded names join the
  cleanup ownership and drain physically, the disarmed retry records
  exactly two fresh replacements), and
  `TwoDirtyObjectRefusalRollsBackRetriesAndCommits` (engine level: the
  pinned one-reservation bound over two dirty objects fails the
  admission; rollback keeps both marks; the post-rollback, post-retry and
  post-commit reads return the EXACT surviving pk set, which no duplicate
  live replacement entry could satisfy). For F3:
  `FailedRecordCopyKeepsObjectRetrievable` (the record's own copy fails
  at a pinned allocation cap: the name never registers, the reservation
  stays outstanding, the retake converts it, the queue deletes the file;
  plus the refused-after-copy leg whose prepared copy must release
  exactly) and for F2 the multi_update producer suite (hook on the
  insert sinker and both temp writer shapes; collect-then-close
  hand-off at Reset/Free with the idempotent second teardown; saturation
  fails the writer's Sync with the refusing hook's error and nothing
  persists; compat without the owner surface) and the white-box tombstone
  leg (`TombstoneWriterReserveAndRetainOnUnwind`) draining the tombstone
  object physically. NC standard of record for r20: the two-dirty
  compaction regression, the record-failure regression and the record-
  copy regression must be red on the archived round-19 txn.go (an
  appended earlier record surviving the failed pass; the missing staging
  refusal; a name registered before its failed copy) and the
  multi_update NC probes red on the archived round-19 delegate (no hook
  on any multi_update writer, no name hand-off, no saturation
  propagation), both green on the fixed tree with sha-verified restores.
  The r5 additions (findings G1–G4): the real-producer in-flight
  publication regressions — the INSERT leg drives the production
  `Insert.Reset` teardown over a real writer and the real workspace
  retainer with the consumer's record held back until after the teardown,
  then releases it, commits, and verifies the object's physical survival
  and the drain's non-interference, and the multi_update leg drives the
  real delegate teardown under the same interleave — plus the epoch-guard
  regression (a mid-statement dump may not move the current epoch's
  retained name to the queue) and the boundary-recheck regression (a name
  recorded between retain and the next boundary never enters the queue);
  the commit-terminal capacity regressions (a parked prepared commit keeps
  its unit, so a competing transaction's admission is refused through the
  whole prepare/RPC window; the park's definite failure transfers
  owned→queued without exceeding the bound; the success and unknown
  classifications retire exact); the standalone DELETE producer regression
  (the production flush path with a pinned threshold: saturation refuses
  the tombstone spill, the persist-then-refused-record files join the
  cleanup ownership and drain physically, and the merge-delete consumer's
  recorded stats keep their files live), and the remote settlement
  regressions (two engines, small bound: a transferred fragment releases
  its reservations and ownership with nothing deleted; a failed or
  cancelled fragment retains through the local queue and drains; a
  coordinator-side refusal of a clean fragment's names fail-open-retains
  them on the recording engine; repeated executions stay bounded). NC
  standard of record for r21: the in-flight interleave, the epoch guard,
  the parked-commit admission, the refuse-retain of reservation-less
  names, the DELETE-flush binding, and the remote release must each be
  red on the archived round-20 production files at the invariant the
  finding names, and green on the fixed tree with byte-verified restores.
- **r6 — intake adoption and the two-engine bound (round-22, findings
  H1/H2).** The recognizer's contract is pinned where the layout lives
  (`multi_update`): the five-vector relay layout, delete-name derivation,
  insert-name derivation through the serialized BlockInfo, the unknown-
  action/ordinary-batch rejections, and the pipeline-codec wire round trip
  (`TestIssue25590RelayS3InfoNameExtraction`). The adoption-before-release
  contract is pinned across two real engines
  (`TestIssue25590RemoteIntakeAdoptsBeforeRelease`): the adopted object
  survives the recording transaction's commit with every ledger balanced;
  a cancellation between adoption and any record keeps the released file
  enumerated on the recording side and deletes it physically through its
  terminal reservation drain; a record whose own preparation fails keeps
  the reservation as the owner and retires the file physically; a
  saturated recording engine refuses the intake before anything books and
  the fragment settles through its failure terminal. The sustained
  refusal bound is pinned by `TestIssue25590SustainedTwoEngineRefusalBounded`
  (deletion blocked, two engines, bound 2+2): clean rounds fill the
  recording queue, refused intakes fail whole runs so the fragment keeps
  its own obligations, the saturated fragment's writers then refuse their
  own spills, and the aggregate obligation population stays exactly pinned
  at the sum of the per-engine bounds for however long refusal sustains —
  replacing the r21 growth-accepting assertions (the reworked
  `TestIssue25590RollbackProducersEnterTheCapacityBound` holds the
  population flat behind a stalled drain through refused rounds that book
  nothing). NC standard of record for r22: on the archived round-21
  production files the adoption capability (no
  `AdoptRemoteTransferredObjectNames` on the recording workspace), the
  preparation-failure retain, and the terminal reservation-drain
  adoption must each be red at the invariant the finding names, with the
  H2 aggregate probe red on the round-21 production protocol (the
  fresh-refuse loop's per-round growth), and every probe green on the
  fixed tree with byte-verified restores; the compile-package suites
  cover the moved settlement's drain ordering and the client intake.
- **Concurrency.** Race mode over the focused suites (covering the
  queue-mutex transfer window and the triple's lock order) plus the engine
  race suite — zero reports.
- **CI spine.** `go test ./pkg/vm/engine/disttae/` (full package),
  `./pkg/vm/engine/test/` (issue suite + RC sweep),
  `./pkg/objectio/ioutil/`, `./pkg/sql/colexec` (S3 sweep),
  `./pkg/sql/colexec/insert/` (operator seam, r3), both race suites,
  gofmt/vet — the round-by-round recorded battery ran all of these green at
  the current revision (latest full run, round 19: full package 1,260
  tests / 0 failures — r18's 1,258 plus the two r19 white-box regressions —
  flake sweep 264/264 — 22 queue/admission tests × 12 counts including all
  four r19 regressions — WB 48/48, engine 9/9, insert package ok, RC 16/16,
  dump sweep 7/7, both race suites zero reports); the original issue's
  30k-statement CPU profile remains the issue-side human confirmation flow,
  superseded for CI by the deterministic tests above.

## 8. Drawbacks and open decisions

Accepted tradeoffs are recorded in §9. Drawbacks: a bounded shutdown can leak
the deadline-detached work class; failed publications log the retake warning
at each retaining boundary; admission refusal surfaces one statement error
per object-creating statement (a spilling dump or a recording write) for the
duration of a pathological backlog, with the WARN family as the diagnostic;
the per-name reservation map adds O(objects-per-dump) bookkeeping to every
dump (memory-bounded by the same limit, §5.5). Open decisions (non-blocking):
export the
close-drain deadline and admission limit as configuration knobs (owner:
performance team; decision point: soak telemetry shows a site saturating
near the limit); add a queue-depth metric (owner: observability; decision
point: operators ask for backlog diagnosis more than once).

## 9. Decision log

1. **Retire-and-compact (A-2) over per-table index rewrite (A-1).** Both meet
   I1; A-1 re-authors every consumer's coordinate arithmetic for the same
   asymptotic end state. Accepted: A-2's minimal invariant surface.
2. **Boundary published even when merge/dump fails.** Required so the
   frontend's `RollbackLastStatement` always dominates THIS statement's
   writes; realized with `admissionInterrupted` for pending-mark retention.
3. **No storage calls on the statement's failure path.** Ownership is
   transferred (pendingObjectCleanup → live accounting) and collection is
   deferred to the boundary/terminal handoff and the queue's drain.
4. **Fail-closed admission (refuse the dump) over waiting for capacity.**
   One statement error now, GC autonomy intact, unbounded user-path waits
   avoided.
5. **One queue + coalesced pipeline (C-D) over per-failure goroutines (C-B)
   or admission waits (C-C).** Bounded workers and backlog, deterministic
   tests.
6. **Producer window whole under the queue mutex + one atomic shutdown
   triple (round 16).** Closes the conclude-over-registration race; the
   remaining teardown-time arrival belongs to the accepted crash class.
7. **Crash-mid-queue leak class accepted as the restart clause.** Every
   non-crash terminal path is contracted in-process; at exit only garbage
   the base could also leave behind remains.
8. **Per-object capacity ledger over one-slot-per-dump (r2, findings
   B6/B7).** Every name that can reach the cleanup queue joins the bound at
   its creation: spilling writers reserve one unit per object through the
   sinker capacity hook, and the record choke admits every recorded object
   (direct-S3 and compaction records fail-closed; a dump's recorded names
   convert their own reservations). The alternative (C-E) kept the check
   simple but overdrafted the ledger on every multi-object publication and
   never counted recorded-at-creation obligations at all. The duplicated-
  name cases (a record naming one object twice; two entries of one object)
   are absorbed by the set-keyed ownedNames ledger and the choke's distinct-
   name extraction, so capacity is spent per object, never per mention.
9. **Fallible-prefix compaction and bound-at-persistence producers over
   panicked workers and record-only admission (r3, findings B8/B9).** The
   compaction's Phase A workers own nothing but their result slots: their
   rewrite is the physical fallible step, the PARENT records under the lock
   admission grants (the last fallible step), and the infallible tail
   (publish, retire, GC, clean) never runs behind a possible failure.
   Every direct-S3 writer — insert writers, compaction rewriters, pipelined
   pool jobs — reserves one unit per object before it persists, and a writer
   teardown hands its unrecorded persisted names to the transaction's
   asynchronous cleanup ownership through the retainer surface with the
   recorded-name filter. The rejected shapes (C-G): swallowed worker
   panics whose tail retired originals a refused record could no longer
   reach, and unbound writers whose refused statements leaked persisted
   objects no owner could enumerate. The transaction's private reservation
   map gained its own guard so hooks firing on lock-free writer goroutines
   cannot race the choke's split (lock order: transaction lock →
   reservations guard → queue mutex).
10. **Atomic record publication across all replacements, copy-before-admission
   record bodies, and multi_update producer closure (r4, findings F1–F4).**
   The compaction pass STAGES every replacement's record (fence, workspace
   copy, choke) and publishes an all-or-nothing batch; a failed pass
   retracts every staged admission from the recorded-name set (ledger
   membership stays as the cleanup obligation), releases every staged
   re-publication batch and prepared copy, and hands every unrecorded
   persisted name to the retainer — nothing survives under the failed
   admission's statement boundary for a retry to duplicate. Record bodies
   complete their fallible batch copy BEFORE committing name membership or
   converting reservations, so an allocation failure leaves the persisted
   object re-takeable instead of recorded-but-ownerless. Every multi_update
   S3 writer (insert sinkers, temporary data and tombstone writers) binds
   the workspace's capacity option through the shared colexec resolution
   helper, collects its persisted names before its deferred close, and its
   Reset/Free hands the unrecorded collection to the workspace retainer.
   The rejected shapes (C-H): per-record append with rollback
   compensation, record-side registration before the copy, and unreleased
   staging on failure.
11. **Boundary classification, commit-terminal capacity, DELETE producer
   closure, and remote settlement (r5, findings G1–G4).** A retained name
   is classified recorded-or-orphaned only at a boundary its recording
   consumer can no longer affect: the retain stamps the retaining
   statement's epoch, mid-statement handoffs move nothing of the current
   epoch, and every quiesced handoff rechecks recorded membership — the
   queue never sees an in-flight publication and the drain never needs a
   cross-transaction probe. Recorded names keep their capacity through
   commit preparation and the commit RPC, and are settled only at the
   caller's terminal classification (success publishes, definite failure
   transfers owned→queued exactly, unknown releases without deleting).
   The standalone DELETE flush writer reserves through the shared surface,
   collects before its in-flush close, and hands the unrecorded names over
   at teardown. A remote fragment's workspace receives exactly one
   settlement at its run's end — transferred releases without deleting,
   failed/cancelled retains through the local queue — and a refused
   coordinator-side record fail-open-retains the refused
   reservation-less names on the recording engine (CCPR records exempt).
   Rejected shapes: C-I wired acknowledgment, C-J global lease registry,
   C-K drain-time membership probing.
12. **Intake adoption before remote release, and the two-engine bound
   (r6, findings H1–H2).** The coordinator's transport client adopts every
   relayed object name at its intake — before forwarding and before the
   ack — through the same fail-closed per-object admission reservation
   the writers pass; the fragment's single settlement waits behind the
   message's batch-flow drain, so a transfer implies every relayed name
   already has an enumerable recording-side owner, and every refusal
   (drain failure included) fails the whole run so the fragment settles
   through its own failure terminal instead of releasing. The recording
   side covers its own end classes: every terminal's reservation drain
   adopts unconverted reservations (adoption-then-queue, CCPR exempt) as
   the last reachable owner instead of abandoning them, and every
   record-preparation failure fail-open-retains the record's EXTRACTED
   reservation-less names (the choke's fresh-name filter, one checkpoint
   earlier) before returning, with the local file service completing the
   not-found swallow at its Remove step so the concurrent scheduled
   drains keep the r10 tolerance the ladder's doctrine names. Sustained refusal with deletion stalled
   therefore pins the aggregate obligation population at the sum of the
   per-engine admission bounds — the refused-intake rounds book nothing,
   the saturated fragment's writers refuse their own spills — and the
   record choke's fail-open retain keeps only the r19 last-owner doctrine
   for the reservation-less compat class whose in-tree feed the two gates
   leave empty. The receiver-stopped clean run classifies
   non-transferred: batches after the stop never reached the recording
   intake. Rejected shapes: the wired C-I variant stays rejected — r6
   needs no wire change.

## 10. Design review record

```text
Round-17 record (revision r1):
Change scope: complete #25590 series (16 implementation paths at HEAD
  83222fa837 plus this design document); one PR - workspace statement
  bounding + failure-path cleanup subsystem.
Trigger: size - 1,934 additions / 245 deletions of non-generated production
  code across 6 owning production files (txn.go +1,494), exceeding the
  500-line default; complexity - introduces a long-lived stateful
  abstraction (unpublishedPendingQueue, coalesced drain pipeline, admission
  controller), materially changes GC ownership, retry/idempotency and the
  shutdown protocol, and changes a hot statement path. The ordinary-fix
  exemption does not apply to the complete series.
Design: docs/design/20261021-issue-25590-workspace-scaling-and-cleanup-ownership.md,
  revision r1, sha256 recorded in the round-17 handler review record; status
  approved on 2026-10-21 by the distinct design-first review phase.
Blocking findings: B1 - statement-rollback fidelity under failed admissions -
  resolved by preallocated failure-atomic boundaries + admissionInterrupted
  (§3.3; rollout guarded by the WB statement-rollback cases); B2 - post-restart
  queued names - resolved as the accepted crash-mid-queue equivalence class
  with observable consequences and operator actions defined (§6); B3 - the
  admission refusal surfaces as a statement error - accepted with the WARN
  family and self-reopening semantics (§5.3). No blocking finding remains.
Decision log: §9.
Decision: PASS
Implementation deviations: none at approval time. Any deviation - including
  changes to the churn space of §3.3, the triple/producer ordering of §5.4,
  or the fail-closed form of §5.3 - requires revising this document and
  re-reviewing the affected decisions before implementation approval.

Round-18 record (revision r2, superseding r1's §5.3 decisions):
Trigger: the pre-PR implementation review of r1 REQUEST_CHANGES'd the
  series with two P2 findings against §5.3's admitted-capacity model - the
  mandatory gate's deviation rule fired on the fail-closed form of §5.3.
Finding B6 - capacity was reserved per DUMP, not per persisted OBJECT: the
  writers can spill several objects per publication
  (sinker trySpill/syncFileSinker), and the failure epilogue credited every
  persisted name into live accounting, so at limit-1 an admitted
  multi-object dump exceeded the cap by its extra objects after its
  reservation released; the concurrent-admission regression tested
  reservation calls without multi-object persistence and could not
  establish I4.
Finding B7 - the capacity model was not closed over every queue producer:
  the sole admission call sat in the dump path, while Rollback hands ALL
  recorded workspace objects to the same queue (gcObjsByIdxRangeToOwner ->
  recordedWorkspaceNames), direct-S3 inserts reach txnTable.Write's
  metadata branch with no gate, and successfully dumped objects awaiting
  the terminal were absent from accounting - sustained INSERT/ROLLBACK
  could grow the backlog past the limit while deletions failed.
Resolutions: §5.3 (r2) - the per-object capacity ledger: the sinker
  capacity hook reserves one unit per object before it goes durable
  (ioutil.WithUncommittedCapacityHook; nil for every caller without the
  ledger); the single record choke on both record bodies admits every
  recorded object at creation, fail-closed; the epilogue converts exactly
  the persisted names' reservations; commit publishes, rollback moves
  owned->queued losslessly, CCPR releases to the cache, settles retire
  deleted names, terminals abandon outstanding reservations; the ownedNames
  set-keyed ledger spends one unit per object independent of duplicate
  mentions. Validation: §7's I4 additions name the two r18 regressions
  (multi-object boundary and concurrent leg; record-time admission with
  the sustained rollback loop) plus the NC red/green standard on the
  round-17 files.
Decision log: §9 entry 8.
Decision: PASS at r2. The §5.1 ledger, §5.5 bounds, §6 WARN vocabulary and
  §8 drawback wording were revised in lockstep; §5.6 records the rejected
  C-E/C-F alternatives.
Implementation deviations: none at approval time. The same deviation rule
  continues from r1: any change to the churn space of §3.3, the
  triple/producer ordering of §5.4, or the per-object/admission form of
  §5.3 requires revising this document and re-reviewing the affected
  decisions before implementation approval.

Round-19 record (revision r3, superseding r2's §5.3 producer coverage and
  extending §4's failure-atomicity contract to the compaction pass):
Trigger: the pre-PR design review of r2 (sha256
  3e4fbd00f55520e6e3e8060792223b017df403f7ad799fbae686a0f88b8de898)
  REQUEST_CHANGES'd the series with a P1 and a P2 finding - the mandatory
  gate's deviation rule fired on the ownership and failure-atomicity
  decisions of §4/§5.3.
Finding B8 (P1) - dirty replacements did not complete all fallible
  preparation and admission before the original metadata retired: the
  compaction worker's rewrite persisted through the unhooked writer, its
  record could fail at the choke (txn.go:2679 on the r18 tree), yet
  compactDeletionOnObjsLocked had already retired the original metadata
  (releaseWorkspaceEntryBatchLocked), and the worker converted the
  admission error into a panic that the ants pool recovered while
  waiter.Done still ran - the parent then garbage-collected the original
  objects, cleared the pending deletions and returned nil, so the rejected
  replacement's surviving rows vanished from the workspace (and from
  commit), and the physically persisted replacement leaked with no
  cleanup owner.
Finding B9 (P2) - the direct-S3 insert producer persisted before any
  admission and dropped its unrecorded objects at teardown: the insert
  writer (insert/insert.go) carried no capacity hook, its Sync persisted
  objects before MergeBlock recorded them through relation.Write, the
  record refusal at saturation left the persisted names recorded nowhere,
  and Insert.Free only closed the writer - repeated refused inserts each
  leaked one physical object behind a ledger that never counted it,
  invisible to the recorded-entries-only rollback enumeration.
Resolutions: §5.3 (r3) - every producer persists behind the reservation
  surface (the workspace's sinker option binds the insert writer at
  Prepare through the structural SinkerOwner interface; the compaction
  rewriter carries the same option; the sink pool consults the hook before
  a pipelined job's Sync), unwinding writers transfer their UNRECORDED
  persisted names to the transaction's asynchronous cleanup ownership
  (RetainUnrecordedS3Objects with the recorded-name filter; the compaction
  back-out reuses it), and the transaction's private reservation map gained
  its own guard for hooks firing on lock-free writer goroutines; §4.5 -
  the compaction restructure: Phase A workers run only the physical
  rewrite and return (bat, fileName, persisted, err) to the parent, Phase B
  records every finished replacement on the parent under the transaction
  lock, and only the infallible tail (publish republications, retire
  originals, unprotect+GC, clear marks) runs after the last fallible step
  succeeds. Validation: §7's r3 additions name the four regressions
  (white-box compaction refusal + retry, the engine-level E2E, the
  direct-writer reserve/retain/physical-delete legs, and the insert
  operator seam tests) plus the NC red/green standard on the round-18
  files.
Decision log: §9 entry 9.
Decision: PASS at r3. The §2 consumers table, §4.5, §5.3, §5.5, §5.6
  (rejected C-G), §6 WARN vocabulary, §7 validation and §8 drawback wording
  were revised in lockstep.
Implementation deviations: none at approval time. The same deviation rule
  continues: any change to the churn space of §3.3, the phase ordering of
  §4.5, the triple/producer ordering of §5.4, or the per-object/admission
  form of §5.3 requires revising this document and re-reviewing the
  affected decisions before implementation approval.

Round-20 record (revision r4, superseding r3's per-record compaction
  publication and closing §5.3's producer coverage):
Trigger: the pre-PR design review of r3 (sha256
  2ce30d71aa57f83adde976bef0d45fc2c8439d00f65e3df3a2132ab860dcb51f)
  REQUEST_CHANGES'd the series with four findings (P1 + three P2) - the
  mandatory gate's deviation rule fired on the pass-atomicity, producer-
  closure and record-ordering decisions of §4 item 5 and §5.3.
Finding F1 (P1) - compaction publication was not atomic across all
  replacements: txn.go:4094-4123 recorded each finished rewrite's
  replacement the moment its own record call returned, so with two dirty
  objects an earlier successful record was appended BEFORE the pass
  inspected the later rewrite/record errors; the back-out preserved
  recorded replacements, and types.go:971-978 then appended the failed
  admission's statement boundary after these writes - RollbackLastStatement
  could not remove an append below that boundary, and the retried
  compaction left the earlier replacement as an additional live entry
  duplicating its surviving rows at commit.
Finding F2 (P2) - multi_update's persistence paths were outside the
  shared reservation and ownership protocol: sortAndSyncOneTable's
  temporary data and tombstone writers (and ensureInsertSinkers'
  persistent sinkers) were constructed without the capacity hook; they
  persisted before their metadata reached source.Write, the record's
  choke could refuse it, and the delegate's teardown never transferred
  PersistedNames - rollback enumerates only recorded workspace entries,
  so repeated large UPDATE/REPLACE failures leaked ownerless files
  outside the advertised bound.
Finding F3 (P2) - record bodies registered their objects before the
  fallible copy completed (txn.go:2691-2700 and 3110-3119): an
  inputBat.Dup failure left the name in recordedUnpublishedObjectNames
  and the reservation converted, with no workspace entry existing, so
  retainUnrecordedS3ObjectsLocked excluded the name as already
  recorded, rollback could not enumerate it, and the terminal removed
  its ledger membership without deleting the stranded physical file.
Finding F4 (P2) - the compaction's staged re-publication batches
  (txn.go:3999-4017) were not released on the later rewrite/record
  failure branch (4126-4154): the batches never entered txn.writes, so
  no transaction cleanup could free them, and each refused admission
  stranded one allocated batch per clean object inside the mpool's
  registry.
Resolutions: §4 item 5 (r4) - the staged-record compaction: Phase B stages
  every finished replacement's record (fence, copy, choke) into a
  stagedWorkspaceRecord that commits no workspace mutation; the pass
  publishes nothing until EVERY record staged; the failed pass retracts
  the staged admissions from the recorded-name set, releases every
  prepared copy and staged re-publication batch, and hands every
  unrecorded persisted name to the cleanup ownership. §5.3 (r4) - the
  record bodies reorder to copy-BEFORE-choke with prepared-copy cleanup
  on a refused choke; every multi_update writer binds the workspace's
  capacity option (shared colexec.WorkspaceSinkerCapacityOption
  resolution), the temp writers collect their PersistedNames before the
  deferred close, and the delegate's Reset/Free hands the union to the
  retainer before closing; the staged choke's merge is retractable while
  unpublished (freshly persisted names, transaction-lock serialized).
  §5.5 - the staged per-pass mpool footprint is zero across repeated
  refused admissions; §5.6 - rejected alternative C-H records the
  per-record-append, register-before-copy and unreleased-staging shapes.
  Validation: §7's r4 additions name the seven regressions (two-dirty
  white-box atomicity with the mpool equality across repeated refusals,
  the record-failure retraction leg, the engine-level two-dirty
  refusal-rollback-retry-commit with exact reads, the record-copy
  both-legs probe, the multi_update producer suite and its compat shape,
  and the tombstone reserve/retain/drain leg) plus the NC red/green
  standard on the archived round-19 files.
Decision log: §9 entry 10.
Decision: PASS at r4. The §4 item 5, §5.3, §5.5, §5.6 (rejected C-H), §6
  WARN vocabulary, and §7 validation sections were revised in lockstep.
Implementation deviations: none at approval time. The same deviation rule
  continues: any change to the churn space of §3.3, the staged-publication
  phases and retraction of §4 item 5, the triple/producer ordering of
  §5.4, or the per-object/admission/retraction form of §5.3 requires
  revising this document and re-reviewing the affected decisions before
  implementation approval.

Round-21 record (revision r5, superseding r4's boundary, commit-lifecycle,
  producer-closure and cross-CN ownership decisions):
Trigger: the pre-PR design review of r4 (sha256
  6f43ec5455cc9e6d7d9a36848e999c281971d2d533faae93f809796d9ed3a3fa)
  REQUEST_CHANGES'd the series with four findings (one P1, three P2) - the
  mandatory gate's deviation rule fired on §5.3's boundary classification,
  commit-state capacity, producer coverage and the cross-CN ownership
  extension of the admission/cleanup protocol.
Finding G1 (P1) - §5.3 classified producer output as orphaned without
  waiting for downstream publication. Evidence verified on the r4 tree:
  insert/types.go:157-162 and s3writer_delegate.go:963-999 retain
  unconditionally at producer teardown; pipeline/types.go:192-242 runs the
  sender scope's Reset concurrently with the receiver's merge teardown and
  process_spoolr.go:313-335 acknowledges no relation.Write completion, so
  "MergeBlock received the metadata, producer Reset ran, then source.Write
  recorded" is a reachable in-statement interleaving;
  retainUnrecordedS3ObjectsLocked (txn.go:3150-3168) filters recorded
  names only at handoff time; txn.go:3156-3166 placed the in-flight name in
  pendingObjectCleanup while the record path (txn.go:2981-3045) never
  removes it; the mid-statement admission dump hands the list to the
  engine queue blind (967-995, 1712-1714) and the drain
  (1556-1601) deletes without any recorded-membership recheck - the next
  drain can delete a live or committed object. Resolution: the r5
  classification of §5.3 (epoch-stamped retains; quiesced=false mid-
  statement handoffs; recorded-recheck at every quiesced boundary; the
  queue receives only definite orphans; I2's new clause).
Finding G2 (P2) - no ownership closure across remote CN producers and
  coordinator records. Evidence verified on the r4 tree: the remote run
  rebuilds a mirror txn operator per message (NewWithSnapshot,
  txn/client/operator.go:810-815; never registered in the remote client's
  active set) and binds a fresh disttae.NewTxnWorkSpace to it
  (remoterunServer.go:588-592); the fragment's writers reserve on the
  REMOTE engine's ledger while the metadata records run in the
  COORDINATOR's workspace (compile.go:9834-9836 merge scope on the
  coordinator; mergeblock.go:85-89 source write into the coordinator's
  relation; txn_table.go:2201-2213), so the coordinator's choke inspects
  its own reservations and can neither see nor retire the remote unit;
  Compile.clear (compile.go:470-525) and proc.Free (process2.go:289-303)
  release scopes and process resources only, and no remote path calls
  any workspace terminal - the fragment's teardown converts its
  reservations into remote owned membership plus a pending list no
  boundary drains, so successful distributed writes permanently consume
  remote capacity (only the coordinator's workspace payload reaches TN)
  until healthy remote writes are refused. Resolution: the r5 settlement
  protocol of §5.3 (one terminal classification per fragment workspace at
  the run's end; transferred releases without deleting, failed/cancelled
  retains through the local queue; the coordinator record choke as the
  acknowledgment, with refusal fail-open-retaining the refused
  reservation-less names; CCPR exemption; invariant I5; the residual
  documented window and rejected-alternative C-I).
Finding G3 (P2) - recorded-object capacity was released before the commit
  outcome was terminal. Evidence verified on the r4 tree: txn.go:4622-4635
  releases all recorded-name membership immediately after genWriteReqs
  drafted the commit payload, while operator.go:1463-1486 still runs the
  commit processing and rolls back the prepared workspace on definite
  failure - with the bound at one, a competing transaction could consume
  the released unit during the parked commit's RPC window and the park's
  failure then enqueue its own object: two retained obligations behind a
  limit of one, contradicting the capacity-neutral rollback guarantee.
  Resolution: the r5 commit-state model of §5.3 (membership and
  reservations retained through prepare and the RPC window; the release
  relocates to the terminal classification the caller already performs -
  FinalizeCommit publishes, Rollback transfers owned→queued exactly, the
  unknown-result terminal releases without deleting; I4's new clause).
Finding G4 (P2) - the standalone DELETE tombstone producer sat outside the
  ownership protocol. Evidence verified on the r4 tree:
  deletion/types.go:102-109 constructs an UNHOOKED tombstone writer
  (newDeletionTombstoneWriter reserves nothing), the flush persists at
  its own threshold (SetCNFlushDeletesThreshold; 295-298) and defers only
  the writer Close inside the flush (262-266), and Reset/Free discard the
  metadata without any retainer handoff; the path is publicly reachable
  (compile.go:9530-9541 compileDelete / newDeleteMergeScope), and the
  round-20 tombstone regression manually supplied the capacity option
  (txn_issue25590_test.go:6868-6871), so the missing production binding
  was invisible to it. At saturation the tombstone persists before
  txnTable.Delete reaches the gated record path
  (txn_table.go:2416-2439); the refused record leaves no workspace entry
  for rollback to enumerate and the in-flush close drops the only name
  reference - files leak outside the bound. Resolution: the r5 DELETE
  producer closure of §5.3 (constructor binds
  colexec.WorkspaceSinkerCapacityOption; flush collects
  PersistedNames before the in-flush deferred close; Reset/Free hand the
  collection to the workspace retainer under the same epoch-stamp and
  boundary machinery as every other producer).
Validation: §7's r5 additions name the real-producer in-flight
  publications (insert and multi_update), the epoch-guard and
  boundary-recheck regressions, the commit-terminal capacity regressions
  (parked unit retained / refused competitor / exact transfer on
  definite failure), the standalone DELETE production-path regression
  (pinned threshold, saturation refusal, persist-then-refused-record
  physical drain, merge-delete-consumer survival), and the remote
  settlement regressions over two engines (transferred release, failed
  and cancelled retention, refused-record fail-open retain, repeated
  execution boundedness), plus the NC red/green standard on the archived
  round-20 files.
Decision log: §9 entry 11.
Decision: PASS at r5. §1 (I2/I4 wording, new I5), §2, §5.3, §5.5, §5.6
  (rejected C-I/C-J/C-K), §7 validation and §9 were revised in lockstep.
Implementation deviations: none at approval time. The same deviation rule
  continues: any change to the epoch-stamp/boundary classification of the
  pending handoff, the commit-terminal release points, the DELETE
  producer closure, or the remote fragment settlement requires revising
  this document and re-reviewing the affected decisions before
  implementation approval.

Round-22 record (revision r6, superseding r5's remote-transfer and
  refusal-capacity decisions):
Trigger: the pre-PR review of design r5 and its round-21 implementation
  REQUEST_CHANGES'd the series with two P2 findings against §5.3's remote
  settlement classification and refusal semantics - the mandatory gate's
  deviation rule fired on the transfer ordering and the fail-open-retain
  feed of §5.3, §5.5.
Finding H1 (P2) - an enumerable cleanup owner was not established before
  fragment ownership and capacity were released. Evidence verified on the
  r5 tree: remoterunServer.go:654 classified transfer solely from
  runErr==nil and settled inside the run case, and
  txn.go:4969-4974 then cleared the pending names and released capacity
  before the transport drain at remoterunServer.go:158-171; the
  coordinator's forwarding (remoterunClient.go:372-381) acknowledges
  channel delivery - including batches a stopped local receiver
  discards - rather than workspace recording; record preparation returns
  on epoch-fence/keyed-fault/batch-copy failure before the refusal
  retainer runs (txn.go:2818-2840); Repeating clean fragments followed by
  cancellation therefore accumulates files nobody can enumerate, and the
  r5 document's per-fragment bound does not bound cumulative leakage.
  Resolution: the r6 adoption ordering of §5.3 (transport-client intake
  adoption through AdoptRemoteTransferredObjectNames in band - the relay
  layout's delete-name column and insert BlockInfo - with the fragment
  settlement moved behind the batch-flow drain as the adoption barrier;
  transferred requires a clean, non-stopped, drained run; the recording
  side's terminal reservation drain adopts never-recorded adoptions
  instead of abandoning them, and every preparation failure fail-open-
  retains its reservation-less name; I5's reworded contract).
Finding H2 (P2) - refusals grew the coordinator backlog instead of
  enforcing a capacity bound. Evidence verified on the r5 tree:
  txn.go:3112-3132 answers a capacity refusal by retaining every fresh
  name, retainUnpublishedPendingNames at txn.go:1462-1478 increments
  ownership without any capacity check, and Rollback merely transfers the
  additions into the queue - so with deletes stalled, remote fragments
  persisted under their separate engine's capacity, released it on clean
  completion, and added one more refused object to the saturated
  coordinator per round; the round-21 regression even asserts the growth
  (txn_issue25590_test.go:4424-4462 asserts totals 9 then 10/11/12 at
  limit 8 while admission logs exceed the limit), contradicting design
  I4 and §5.5's cap over queued+owned+inFlight. Resolution: the r6
  refusal-feed split of §5.3 (the intake adoption uses the SAME
  reserveUnpublishedPendingObject admission, so a refusal fails the run
  and books nothing; the fragment then keeps its own units until its own
  engine's bound refuses its writers' spills) plus the two-engine
  aggregate bound of §5.5 (the sum of the per-engine limits under
  sustained stalled deletion), with the record choke's fail-open retain
  reduced to the r19 last-owner doctrine for the reservation-less compat
  class whose in-tree feed the two gates leave empty; the
  growth-accepting assertions are replaced - the reworked capacity-bound
  test pins the population flat behind a stalled drain, and the new
  sustained two-engine test requires the aggregate to stay bounded with
  deletion blocked.
Validation: §7's r6 additions name the recognizer unit contract with its
  pipeline-codec round trip, the adoption-before-release regressions over
  two real engines (committed adoption survival with balanced ledgers,
  cancellation-between-adoption-and-record, record-preparation failure,
  saturated-intake refusal with nothing booked), the sustained two-engine
  bounded-refusal test with deletion blocked, and the reworked
  capacity-bound sustained phase, plus the NC red/green standard on the
  archived round-21 files (adoption capability, preparation-failure
  retain, terminal reservation-drain adoption, and the H2 aggregate
  probe run against the round-21 production protocol).
Decision log: §9 entry 12.
Decision: PASS at r6. §1 (I4/I5), §2, §5.3, §5.5, §5.6 (the C-I note),
  §7 validation and §9 were revised in lockstep.
Validation-round refinements (r6's own first battery, same decisions, no
  re-review needed): the FLAKE stage FAILED on
  TestIssue25590RecordFailureAfterStagedReplacementRetracts (12/12) with a
  phantom third pending member — the round-20 compaction keyed-fault drive's
  record passes a DeltaLocation fileName variant while its canonical
  replacements (reserved by their writers) are the real object set, so the
  preparation-failure guard now retains EXACTLY the record's extracted
  fresh names (the choke's own filter: `fileName`-less, reservation-filtered)
  and the compaction drive's pending list stays its canonical two. One
  FLAKE count also failed TestIssue25590UnrecordedRetainedNameDrainsAtBoundary
  when the engine's own scheduled drain won the race against the test's
  manual drain and the LocalFS deleteSingle surfaced a raw
  Stat-vs-Remove ENOENT: `deleteSingle` now completes its documented
  not-found contract at the Remove step, which is precisely the
  not-found tolerance the drain ladder's r10 doctrine named as its
  foundation under concurrent scheduled drains. Both refinements stay
  inside the recorded decisions (the preparation guard's scope narrows to
  the already-recorded fresh class; the file-service change completes an
  existing swallow, no consumer-visible contract change), so the r6 PASS
  stands and the full battery reruns from the top.
Implementation deviations: none at approval time. The same deviation rule
  continues: any change to the intake adoption gate, the
  settlement-behind-the-drain ordering, the terminal reservation-drain
  adoption, or the preparation-failure retain requires revising this
  document and re-reviewing the affected decisions before implementation
  approval.
```

The implementation reviewers must verify the code against this revision
(`r6`; sha256 recorded in the round-22 handler review record) before approving
the implementation; the validation mapping in §7 is the required
executable-evidence spine.

---

## Revision r7 (round 23): authoritative producer provenance and the complete producer-format registry

The round-23 pre-PR review of the r6 implementation returned REQUEST_CHANGES
with three findings. All three were first re-verified against the code, then
executed as red probes on the r6/r22 tree (all five probes RED with their
exact finding messages, /tmp/r23_red_probes.log), then fixed under this
revision.

F1 (P1) - Distinguish ownership metadata from ordinary query results. The r22
intake recognized relayed metadata purely from its five-column shape, a
shape an ordinary query result carries, and adopted the delete-class rows'
fourth column as a storage-object name: an ordinary SELECT naming an
existing committed file would book that file for deletion at the recording
transaction's terminals. F2 (P2) - the adopt the complete canonical set
finding: the delete rows' fourth column is the writer's flush key
(raw object-ID bytes plus '|type', s3writer_delegate.go fillDeleteBlockInfo),
never a storage name, and the insert tail carries EVERY persisted object
(FillBlockInfoBat accumulates every sync) while the r22 recognizer decoded
only the first BlockInfo - adoption covered fewer objects than the writer
persisted, and undecodable tails were skipped instead of rejected. F3 (P2) -
close adoption over the standalone producers: the standalone INSERT result
stream (AllocCNS3ResultBat's [BlockMeta_BlockInfo, ObjectMeta_ObjectStats]
two-vector layout) and the standalone DELETE remote stream
(makeDelRemoteBatch's [Delete_ID, DeltaLoc, Type, Partition, Deletes_Length]
layout, tombstone ObjectStats serialized inside DeltaLoc rows of the
FlushDeltaLoc class) were both unrecognized, yet their clean, drained
fragments still settled transferred and released their local owners.

### Decisions

- D1 Producer-provenance gate (F1). Rows are ownership-candidate metadata
  only when the RECEIVED SCOPE'S OWN OPERATOR TREE - the exact plan the
  coordinator built and sent - contains one of the registered persistence
  producers (vm.MultiUpdate/PartitionMultiUpdate, vm.Insert/PartitionInsert,
  vm.Deletion/PartitionDelete; scopeRemoteObjectAdmission in
  pkg/sql/compile/remote_object_admission.go). Ordinary query fragments
  never reach any recognizer, whatever their column shapes. The dispatcher
  adoptRelayedS3ObjectsAtIntake runs this gate first in all three receive
  loops; ordinary rows book nothing, and AdoptRemoteRunIntakeMetadata
  exports the same path for the transport regression.
- D2 Canonical tail extraction shared with recording (F2). The four scalar
  columns never name an object. Every name derives from the serialized
  metadata tails alone: decode the tail batch and enumerate EVERY row of its
  ObjectStats column through the same ObjectStats-to-ObjectName derivation
  the workspace record choke uses (recordedUnpublishedObjectNamesLocked's
  formula). Row-only deletes own nothing (the producer's own
  PhysicalAddr_Attr marker).
- D3 Fail-closed metadata (F2). A tail that cannot decode, decodes without
  its ObjectStats column, or carries a short/empty stats value rejects the
  ACK: the run fails while the fragment still holds its local owner, and a
  stream can never claim ownership for fewer objects than its writer
  persisted.
- D4 The complete producer-format registry (F3). Three intake classifiers
  now exist, one per registered producer family:
  multi_update.TryExtractRelayS3InfoObjectNames (the relay action rows,
  rewritten per D2/D3), insert.TryExtractS3InsertObjectNames (the standalone
  insert result layout), deletion.TryExtractRemoteDeleteObjectNames (the
  standalone delete remote layout, FlushDeltaLoc rows only).
- D5 Settlement boundary (F3). settleRemoteFragmentWorkspaceAfterDrain
  permits the transferred classification only when the decoded fragment
  scopes contain a registered producer
  (remoteRunScopesAdmitAdoption(runCompile.scopes)); every other fragment
  keeps a reachable local owner on every ending, so a future producer class
  cannot release files the intake does not cover until it registers AND
  contributes a classifier.
- D6 Regressions. The r23 transport regression
  (TestIssue25590OrdinaryQueryRowsCannotBecomeCleanupObligations: an
  ordinary five-column batch naming a REAL committed witness file, with the
  machinery-live danger control first, then COMMIT and ROLLBACK legs asserting
  no booking anywhere and the file untouched through both terminals) and the
  production sender/receiver proofs for the standalone formats
  (TestIssue25590StandaloneProducerFormatsAdoptBeforeRelease: insert and
  delete format batches through the real classifiers, cancellation before
  any record retiring physically through the recording terminal, and the
  sustained refused intake with deletion stalled pinning the population at
  the bound and retiring only after the drain can delete). Per-producer
  sender-fidelity units live in each producer package
  (relay_info_names_test.go, relay_object_names_test.go x2); the registry
  walk and settlement predicate are pinned by
  pkg/sql/compile/remote_object_admission_test.go.

### Validation (round 23)

- Red probes on the r6/r22 tree (all RED at their finding asserts,
  /tmp/r23_red_probes.log): ordinary five-column rows adopted; multi-object
  insert tail truncated to one; the delete rows' flush-key string adopted
  instead of the tombstone name; undecodable metadata acknowledged with no
  liability; no recognizer covering the standalone insert/delete formats.
- Green on the fixed tree: the three classifier unit tests, the compile
  registry test, and the two composed disttae regressions (logs
  /tmp/r23_relay_test.log, /tmp/r23_producer_tests*.log,
  /tmp/r23_new_tests*.log); gofmt/vet clean across the five touched
  packages; the r5-r22 regression ring and the full disttae package run in
  the round's battery (see the handler record for the exact commands and
  counts).

## 10 design review record - round 23 (r7)

Reviewer: this handler's recorded design re-review per the repository
contract's revision order. Findings 1-3 verified against the code first
(file:line evidence above), executed as RED probes on the r22 base, then
fixed. The blocking questions - ordinary rows cannot reach a recognizer
without a registered producer in the coordinator's own plan; every adopted
name derives from the serialized ObjectStats tails through the recording
side's own derivation, complete and fail-closed; every registered producer
format adopts before the ack so a drained clean fragment transfers only
behind proven adoption - are resolved by D1-D6 and their statuses above.
Decision: PASS at r7.

---

## Revision r8 (round 24): borrowed-object preservation, mode-authenticated provenance, adoption-capable transfer, and the dispatcher-level proof plan

The round-24 pre-PR review rejected design r7 at the design gate with four
findings. All four were re-verified against the current tree before this
revision; none is stale:

F1 (P1) - Preserve clone-shared objects when metadata recording fails.
Verified: TableMetaReader.addCloneSharedFile (table_meta_reader.go:85, used at
:327) registers clone-borrowed committed source names (and txn-local shares)
in engine.cloneTxnCache; filterGCNamesLocked (types.go:1189) is the only
protection for those names and it runs ONLY inside gcFiles' scope admission
(types.go:1229) for clone transactions. The round-20..22 retention machinery
treats every reservation-less, non-CCPR name as a FRESH orphan: the
preparation-failure retain retainFreshRecordPreparationFailureLocked
(txn.go:~2822, three early returns), the admission-refusal retain in
registerUnpublishedObjectRecordLocked (txn.go:~3218), and the boundary
handoff handoffPendingObjectCleanupLocked (txn.go:~1015 -> the moved split)
all enqueue names with no clone-shared consultation - and both retain sites
funnel through retainUnrecordedS3ObjectsAtLocked -> txn.pendingObjectCleanup
-> the engine's unpublished queue, whose drain deletes physically. A clone
transaction's Write records carry the borrowed source names (TableClone
passes the reader's metadata to dstRel.Write), so a preparation failure or a
capacity refusal there, or the pending handoff at any boundary, enqueues
COMMITTED SOURCE DATA for physical deletion. CCPR's shared-file exemption
(the isCCPRTxn guards) is the only borrow-aware exemption today; clone
shares have none.

F2 (P2) - Authenticate metadata output rather than operator-family presence.
Verified: remote_object_admission.go's admitOpTree flags stream eligibility
from the op FAMILY (vm.MultiUpdate/PartitionMultiUpdate, Insert/
PartitionInsert, Deletion/PartitionDelete) without the producing mode:
multi_update.go update() (Action==UpdateWriteTable) returns its input batch
UNCHANGED (return input, nil), and compile builds UpdateWriteTable-writer
fragments normally - such a fragment's ordinary result rows reach the relay
recognizer through the r7 gate, where matching types/actions either fail a
legitimate ordinary stream fail-closed (the r7 D3 rule) or adopt embedded
object stats. The registry test covers only UpdateWriteS3. Family presence is
not provenance of a metadata output edge.

F3 (P2) - Negotiate ownership-adoption capability before interpreting ACKs
as transfer. Verified: settleRemoteFragmentWorkspaceAfterDrain's barrier is
drain + receiver-stop + registered-producer, but supportsFinishAck
(remoterunServer.go:1072) checks only teardown mode and message type, and
the assigned base's receive loops (HEAD remoterunClient.go) forward or
discard batches and ACK them without any adoption - so an OLD coordinator
with a NEW fragment satisfies the barrier while acquiring nothing;
cancellation between the ACK and the coordinator's record recreates the
ownerless-file path the r5..r7 protocol was built to close. MOProtocolVersion
is the tree's existing rollout gate (compile.go:8638-8659;
handlePipelineProtocolCheck carries it in the protocol-check handshake,
remoterunServer.go:226) and is the natural carrier of adoption capability.

F4 (P2) - Exercise the actual intake and settlement boundary in the
required proof. Verified: txn_issue25590_test.go's composed tests call the
insert/deletion classifiers and AdoptRemoteTransferredObjectNames directly
(the disttae package cannot import compile or multi_update - import cycle),
never the multi_update intake nor AdoptRemoteRunIntakeMetadata;
remote_object_admission_test.go covers predicates only. The receive loops'
adoption-before-ACK and the settle's transfer classification - the central
distributed invariant - have no executable proof.

### Decisions (r8)

D1 - The borrowed-object exemption (closes F1). The ownership contract now
distinguishes three classes a record's name can belong to: (a) NEWLY
persisted objects of this transaction (writers' reservations and record
choke own them - the r18/r21 machinery is for exactly these); (b) CCPR
transaction-local shared files (the isCCPRTxn exemption); (c) CLONE-BORROWED
objects: names engine.cloneTxnCache holds as SharedFile (committed source
state) or TxnLocalSharedFile for this txn - objects this transaction did
NOT create, whose fate belongs to the clone-GC scope rules
(filterGCNamesLocked inside gcFiles), never to the orphan machinery.
INV1 (the new invariant): a clone-borrowed name never enters
txn.pendingObjectCleanup nor the engine's unpublished queue through ANY
retain/handoff path; specifically both retain funnels
(retainUnrecordedS3ObjectsAtLocked's unrecorded split) and the handoff
(moved split) must drop or keep-without-enqueue exactly the clone-shared
names the cloneTxnCache reports for THIS txn (isCloneTxn && IsSharedFile ||
IsTxnLocalSharedFile - the same predicates filterGCNamesLocked applies,
lifted into a txn-scoped helper so the two admission sites - gcFiles and
the round-20+ orphan machinery - share ONE predicate owner). Names that are
neither reserved nor borrowed remain definite orphans exactly as r21/r22
proved.
Regresions (cheapest): a clone txn with a real committed witness file
registered through the TableMetaReader/addCloneSharedFile path (positive
control via IsSharedFile) - (leg A) retainFreshRecordPreparationFailureLocked
carrying the borrowed name asserts pendingObjectCleanup holds NOTHING for
it while a genuinely fresh name in the same batch still lands (both
directions pinned); (leg B) the admission-refusal retain (bound saturated,
admitUnpublishedPendingRecordedNames refuses) asserts the same split; (leg
C) handoffPendingObjectCleanupLocked preloaded with one borrowed and one
fresh name moves ONLY the fresh one to the queue; (leg D) end-to-end
preservation: roll the clone txn back, drain the queue, assert the witness
file still exists (StatFile ok) and the source table still reads its rows -
the review's physical-survival and readability requirement.

D2 - Mode-authenticated metadata output edges (closes F2). The registry
gate flags a stream for ownership interpretation ONLY when the producer
operator itself reports a metadata-bearing output mode:
MultiUpdate/PartitionMultiUpdate with Action==UpdateWriteS3;
Insert/PartitionInsert with ToWriteS3 && !ToExternal;
Deletion/PartitionDelete with RemoteDelete. Vehicle: one exported predicate
per operator family (structural interface, e.g.
ProducesRemotePersistedObjectMetadata() bool) on BOTH the raw and the
partition-wrapper types (the wrapper's unexported raw field is visible
inside its package), and admitOpTree asserts the interface - no compile-side
field reaching into wrappers. Scopes whose write operators all run ordinary
modes (UpdateWriteTable passthrough, workspace insert, external insert,
local delete) admit NOTHING and pass ordinary data through untouched;
their fragments also settle transferred=false via the existing r7
predicate. Regressions: negative legs for every ordinary mode (UpdateWriteS3
absent) with ordinary five-column rows through the real dispatcher - no
adoption, no error; positive legs for all three metadata modes with
production-built streams; the registry unit gains all mode legs including
nested/wrapper placement.

D3 - Adoption-capable transfer (closes F3). The transferred classification
requires, in addition to clean + drained + not-stopped + registered
producer: the peer coordinator has DEMONSTRATED adoption capability, carried
by the MOProtocolVersion rollout gate's handshake - a minimum protocol
version V_adopt (the first version whose receive loops adopt every decoded
relay batch before its ACK). Below V_adopt (including the assigned base's
client), transferred=false and the fragment keeps and rolls back its local
owner - the pre-r5 safe behavior: the cancelled run's files are deleted
locally, no window exists. Compat matrix: old-coordinator/new-fragment ->
kept+rolled-back (proven by a forced-below-V_adopt settle fixture with
cancellation between ACK and record: the file survives the coordinator's
state and the fragment's terminal retires it physically); new-coordinator/
old-fragment -> the coordinator's intake still adopts every RECEIVED batch
pre-ACK (client-side, version-independent - strictly stronger than base);
the unreceived-tail residual under cancellation remains the OLD server's
own base behavior, outside this tree's control and documented as the mixed
rollout residual beside the no-batch-flow residual. Regressions: settle-leg
fixtures with the negotiated version forced above V_adopt (transfer path
green) and below (kept path + physical local retirement after
cancellation), plus the capable-peer transfer pass already pinned by
r22's legs reused untouched.

D4 - The dispatcher-level proof plan (closes F4). The r8 validation ladder
moves the central proof to the COMPILE package, where every layer is
reachable (compile imports disttae, multi_update, insert, deletion - the
import cycle that forced the disttae-side tests to bypass the dispatcher
does not exist there): (1) AdoptRemoteRunIntakeMetadata integration: real
scopes (ordinary query, UpdateWriteTable, UpdateWriteS3, ToWriteS3 insert,
ToExternal insert, RemoteDelete deletion, partition wrappers, nested
PreScopes) x real batches (ordinary five-column rows naming an existing
file; the three production metadata formats; malformed metadata; short
stats) asserting adoption, error and byte-silence per D1/D2 - including the
multi_update relay intake the r23 tests skipped; (2) receive-loop ACK
ordering: the three receive loops against a stub sender - adoption happens
before forwarding and before the ACK, a refusal fails the run before the
ACK, a stopped receiver still ACKs after adoption; (3)
settleRemoteFragmentWorkspaceAfterDrain through real runCompile/receiver
fixtures: capable peer transfer, incapable peer keep, drain failure, stop,
cancellation-retirement; (4) NC red/green differential against the assigned
base: archive the r24 files, swap the BASE (HEAD) versions of the four
compile/multi_update leaf files in with the new tests aside or trimmed to
the base surface, assert the base fails the dispatcher invariants (no gate,
no adoption, transferred without capability), restore byte-verified; (5)
reuse r23's unchanged evidence (the classifier units, the ring, the race
belt, the full package) where inputs are unchanged, per the
validation-evidence contract; rerun only the compile package in full and the
dispatcher/settle focus ring. BVT and multi-CN public-path proof: no
SQL-visible surface changes this revision (internal ownership/transport
contracts); the public multi-CN path is exercised by the engine-lifecycle
suite and the receiver/settle fixture legs above, and the full-package
battery reruns last in CI.

### Design-only checkpoint scope

This revision changes production contracts (D1's shared exemption funnel,
D2's interface predicates, D3's version gate) and therefore returns a
design-only checkpoint per the repository contract: no production change,
no implementation validation this round. The code-verified owner paths
above are the traceability the independent reviewer validates; the r8
validation ladder is the cheapest discriminating plan for issue #25590.

## 10 design review record - round 24 (r8)

Reviewer: this handler's design revision prepared for independent review
per the repository contract's checkpoint order. Findings 1-4 were verified
against the current tree first (file:line traceability above); D1-D4
resolve them with one new invariant (INV1: clone-borrowed names never enter
the orphan machinery), mode-authenticated admission, capability-negotiated
transfer, and the compile-package dispatcher proof ladder. Blocking
questions for the independent reviewer: (1) is dropping clone-borrowed
names at BOTH retain funnels plus the handoff the complete set of enqueues
into the orphan machinery; (2) is the per-family predicate interface the
right mode carrier across partition wrappers; (3) is binding the transfer
gate to MOProtocolVersion's V_adopt (with the old-server residual
documented) sufficient for the mixed-rollout matrix. Decision: r8 is
submitted for independent design review; this handler does not
self-approve the design gate, and implementation proceeds only after the
reviewer's PASS on r8.

---

## Revision r9 (round 25): refuse-before-persistence for incapable peers, an explicit capability advertisement, and the scoped clone exemption

The round-24 pre-PR review rejected design r8 at the design gate with three
findings. All three were re-verified against the current tree before this
revision; none is stale, and where a finding contradicted r8 the
contradicted r8 wording is RESCINDED here, traceably.

G1 (P1) - Do not delete successful writes when the coordinator lacks
adoption capability. Verified: FinalizeRemoteFragmentWorkspace(false)
routes to txn.Rollback (txn.go:5088-5090), which queues the workspace's
retained/persisted names for physical deletion, and the run-case terminal
response (remoterunServer.go:174-179) sends sendEndMessage whenever
handlerErr stays nil - so r8 D3's "below-V_adopt -> transferred=false ->
keep-and-rollback" applied to CLEAN runs deletes the just-persisted files
while reporting SUCCESS: an old coordinator receives the metadata, records
it through its normal consumer, and COMMITs references to files the
fragment deleted underneath it. The keep path is a delete path; r8's rule
is unsafe and is RESCINDED.

G2 (P2) - Specify how coordinator capability is advertised to the fragment.
Verified: proto/pipeline.proto:78 - "protocol_version = 19 // Request:
minimum required version. Response: actual receiver version." - the
existing field carries the CALLER'S FLOOR about the receiver, never the
caller's own capability; requestStreamProtocols (remoterunClient.go:726-733)
advertises only teardown mode and credit settings;
handlePipelineProtocolCheck (remoterunServer.go:211-230) returns the server
side's version and retains nothing about the caller. r8's assumption that
the handshake already demonstrates coordinator adoption capability is
WRONG and is RESCINDED; a forcing fixture cannot stand in for the missing
propagation.

G3 (P2) - Preserve cleanup scope when sharing the clone-protection
predicate. Verified: filterGCNamesLocked (types.go:1195-1201) removes
IsSharedFile names unconditionally but removes IsTxnLocalSharedFile names
ONLY for the cloneGCIntermediate scope; table_meta_reader.go:321-327
requires txn-local shared objects to be DELETED on full transaction
rollback, and TestCloneTxnRollbackGCDeletesTxnLocalSharedObjects
(table_meta_reader_test.go:446) pins that behavior. r8 D1's union of both
classes as "this transaction did not create" is WRONG for the txn-local
class and is RESCINDED.

### Decisions (r9)

D1' - The scoped clone exemption (replaces r8 D1, closes round-24 F1
without breaking G3). INV1 restated: a CLONE-COMMITTED-BORROWED name -
engine.cloneTxnCache.IsSharedFile for this transaction: objects owned
outside this transaction - never enters txn.pendingObjectCleanup nor the
engine's unpublished queue through ANY retain/handoff path (the two
retain funnels' unrecorded splits and the handoff's moved split apply one
txn-scoped predicate, borrowsCommittedCloneShare, reusing the exact
IsSharedFile semantics gcFiles' filter already applies). TRANSACTION-LOCAL
shared names (IsTxnLocalSharedFile - objects THIS transaction materialized
and later clone/ALTER writes may share) KEEP the r21/r22
definite-orphan semantics through every retain/handoff path unchanged:
statement-failure atomicity still owns them, and the terminal full
rollback still deletes them, exactly as the existing
filterGCNamesLocked scoping and TestCloneTxnRollbackGCDeletesTxnLocalSharedObjects
require. Terminal owners per class: committed-borrowed - the clone
completion and gcFiles scope rules own them; the orphan machinery never
touches them; txn-local - the r20 orphan machinery (retain -> handoff ->
drain) owns their retirement, deleting on terminal failure exactly like
fresh names. Validation additions demanded by the review: (control A) the
txn-local INTERMEDIATE-SURVIVAL control - an intermediate retain (the
preparation-failure class) still OWNS a txn-local shared name (it lands on
pendingObjectCleanup; no leak when the statement fails but the txn
continues) while the committed-borrowed name in the SAME batch never
lands; (control B) the FULL-ROLLBACK-DELETION control - that owned
txn-local name is physically deleted by the terminal drain after rollback
(the existing test's guarantee re-proven through the orphan-machinery
path, running alongside the untouched existing test); (controls C/D from
r8, unchanged) the committed-borrowed never-enqueued proof at both retain
funnels and the handoff, plus physical survival and source-table
readability after rollback and drain.

D3' - Refuse-before-persistence for incapable peers (replaces r8 D3,
closes round-25 G1). A remote run whose decoded scope registers a
persistence producer (remoteRunScopesAdmitAdoption(runCompile.scopes)) is
executed ONLY when the request's advertised coordinator capability meets
V_adopt. Below V_adopt - including absent/zero advertisement from an older
coordinator - the server REFUSES the run BEFORE pipeline execution: runErr
is set to a definite unsupported-capability error while no writer has
persisted anything, the terminal response takes the sendError path
(remoterunServer.go:174-179), the statement fails definitively on BOTH
sides, the settle's transferred stays false with an EMPTY rollback
(nothing persisted, nothing destroyed), and the coordinator never
receives metadata it might commit references to. This is the review's
demanded safety property: local deletion happens only under definite
statement failure propagated to the coordinator - and for refused runs
there is nothing to delete. Capable peers keep the r6 barrier untouched
(clean + drained + not-stopped + registry-covered and now
capability-proven -> transferred; the r22 transfer and
cancellation-between-ACK-and-record legs remain the evidence). The
new-coordinator/old-fragment direction is unchanged by this decision: the
old server ignores the unknown capability field (proto3 unknown-field
behavior), runs its base semantics, and the coordinator-side intake
adoption of every RECEIVED batch stands; the unreceived-tail residual
under cancellation remains the old server's own base behavior, documented
beside the no-batch-flow residual.

D3'' - The capability advertisement (closes round-25 G2). The pipeline
execution message grows one backward-compatible field:
`int64 coordinator_protocol_version = 20;` - the coordinator's OWN build
protocol version, absent/zero for older clients. The client sets it from
the same source readProtocolVersion/plainlyRequest uses today
(moruntime.MOProtocolVersion), at request build in requestStreamProtocols'
call site (it is a run-request property, independent of the FinishAck
gate). The receiver stores the advertised value when the execution request
is parsed (a receiver field populated per request - this codebase builds a
fresh receiver per pipeline message, so no cross-request carry; if a stream
is reused the per-request parse IS the reset), the run path consults it
for D3''s refusal, and settleRemoteFragmentWorkspaceAfterDrain's
transferred classification consults the STORED request value
(capability must reach settlement through the request, never through a
test-forced fixture). V_adopt is a named constant equal to the first
coordinator build whose receive loops adopt at intake (this branch).
Proto regeneration goes through the repository's standard proto toolchain;
the field is additive, so older senders and receivers both remain
wire-compatible. Validation: message-level tests prove the advertisement
reaches the receiver and the settlement for BOTH mixed directions - a
request advertising >= V_adopt reaches the transferred path through the
real receiver, and a request with the field absent/zero triggers the
refusal-before-persistence with a definite error terminal
(the sendError path, no persisted files, nothing recorded) - plus the
capable-peer success/COMMIT/readback regression the review demands:
the run persists, the coordinator records and COMMITS, the source rows
read back through the committed references, and the files survive;
cancellation-between-ACK-and-record follows the r22 legs for capable peers.

D4' - Validation plan (extends r8 D4; layers unchanged from r23 remain
reused per the validation-evidence contract, since no production or test
input changed in rounds 24-25). New legs: the D1' controls A-D in the
disttae package (the clone fixtures already exist there: the AddSharedFile
and AddTxnLocalSharedFile registration paths and the existing
rollback-deletes-txn-local test to run beside control B), the D3'/D3''
refusal, capability-propagation and success/COMMIT/readback legs in the
compile package through the real receiver + settle, and the receiver
absent-field/default path. Cheapest discriminating order: the D1' retain
control first (one fixture, both directions, the P1's exact splits), then
the capability propagation (message-level), then the refusal and readback
legs, then the compile dispatcher/settle rings from r8 D4, then the NC
differential, then the full-package rerun once for CI parity.

### Design-only checkpoint scope (round 25)

Rounds 24-25 changed no production or test input; this revision again
changes ONLY the design document. The P1 live in the tree (committed
clone source data deletable through the orphan machinery) remains gated on
this checkpoint's independent PASS exactly as the repo contract orders;
the round-25 P1 (clean below-capability runs deleting successful writes)
existed only in the r8 PROPOSAL and is now designed out before any code
carried it - no implementation of r8 D3 was ever written.

## 10 design review record - round 25 (r9)

Reviewer: this handler's design revision prepared for independent review
per the repository contract's checkpoint order. Round-25 findings G1-G3
were verified against the tree first (file:line traceability above);
r8's contradicted wordings (the D1 union, the D3 keep-and-rollback-on-clean,
the handshake-capability assumption) are rescinded, and r9 carries the
safe rule set: refuse-before-persistence for incapable peers, a real
per-request capability advertisement, and the committed-borrowed-only
orphan exemption with txn-local semantics preserved. Blocking questions
for the independent reviewer: (1) the two retain splits plus the handoff
split are the complete orphan-machinery enqueue set found by the audit -
confirm no third exists; (2) the refusal-before-persistence rule must sit
in the run case BEFORE any fragment pipeline step executes - confirm that
placement cannot bypass an earlier persistence path; (3) the additive
proto field with an absent-is-incapable default is the intended
backward-compatible default direction. r9 is submitted for independent
design review; this handler does not self-approve the design gate, and
implementation proceeds only after the reviewer's PASS on r9.


## Revision r10 (round 26): the public and mixed-version validation matrix (D4'')

The round-26 pre-PR review rejected design r9 at the design gate with one
finding (P2): complete the public and mixed-version validation matrix. The
finding was verified against the tree and the document before this
revision was written:
- The no-public-change rationale is obsolete, as the review states: r9's
  compatibility paragraph (r9 lines 2015-2024) claims no SQL-visible
  surface changes, but D3' (r9 lines 2128-2139) introduces exactly such a
  change - a remote write planned by an incapable coordinator becomes a
  definite statement failure - and the repository testing contract demands
  public-path proof for public claims (references/validation-evidence.md
  line 20).
- The D4' fixture layer cannot prove public multi-CN behavior, as the
  review states: the engine test harness rides a mock that cannot
  enumerate CN services (test/testutil/util.go:118 builds the logtail
  agent on NewMockLogtailAgent; test/testutil/disttae_engine.go:562-574
  mockMOCluster.GetCNService has an empty body), so receiver/settle
  fixtures establish protocol behavior only inside one process.
- r9's advertised mixed directions (r9 lines 2171-2176) both exercise the
  NEW receiver; r9 had no leg exercising the actual
  new-coordinator/old-fragment direction.

RESCISSION: the r9 paragraph ending "BVT and multi-CN public-path proof:
no SQL-visible surface changes this revision (internal ownership/transport
contracts); the public multi-CN path is exercised by the engine-lifecycle
suite and the receiver/settle fixture legs above, and the full-package
battery reruns last in CI." is rescinded and superseded by D4'' below.
D1', D3', D3'' and their mechanisms are UNCHANGED - the review confirms
they address the previous findings; this revision completes only the
validation plan and the mixed-version proof matrix.

D4'' - Public and mixed-version validation matrix (replaces r9 D4'; the
r9 D4' compile-package legs stay as the layer below; execution remains
pending until the implementation turn, per the review's own boundary).

Fixture - in-repository public SQL, run by the unmodified shared mo-tester
harness against a handler-owned real cluster:
- Topology - the issue's own: 1 logservice, 1 TN, 2 CNs built from the
  worktree, client endpoint pinned to CN1's MySQL port (exactly as the
  issue's traffic used CN1), launched per the validation-services contract
  (isolated runtime directory, non-overlapping port plan, bounded
  readiness SQL, owned-PID teardown with listener-release confirmation,
  isolated report directory).
- One new in-repo distributed case file beside this session's existing
  workspace battery (test/distributed/cases/dml/, the update_workspace/
  delete_workspace/replace_workspace family): update_remote_fragment_
  capability.sql with its generated-then-reviewed .result. The case uses
  only stock harness features - the "@session:id=2" tag for the
  fresh-session readback connection, "@ignore:0" plus "@regex(...)" for
  the dynamic cn-uuid statements - and generation mode is followed by
  statement-by-statement review of the generated result (no
  generated-result trust; the mo-tester repository and its shared
  configuration stay untouched).
- Forcing conditions for REMOTE metadata-producing DML on this topology:
  seed volume large enough that (a) the plan routes as multi-CN
  (ExecTypeAP_MULTICN with len(cnList) > 1; scope placement across the
  scheduleQueryWorkers worker set, compile.go:5517-5527 built at
  compile.go:1455), so fragment scopes land on CN2 and dispatch through
  Scope.RemoteRun (compile.go:648) into CN2's handlePipelineMessage
  (remoterunServer.go:355); and (b) the per-statement row volume crosses
  the workspace-spill threshold so the fragment's registered producer
  family (multi_update/insert/deletion) persists S3 metadata objects
  (remoteRunScopesAdmitAdmission-scoped) rather than staying in memory.
  Exact row counts are calibrated once at implementation and recorded in
  the case file.

Version controls - existing mechanisms only; no new knobs, no harness
change:
- V_adopt is the branch's new named defines constant (the next protocol
  constant after MORPCVersion113, pkg/defines/const.go:152): the first
  coordinator build whose receive loops adopt at intake. The
  implementation adds it and points MORPCLatestVersion at it, so fresh
  binaries advertise >= V_adopt and every pre-merge binary - parent HEAD
  83222fa8 included - has latest = MORPCVersion113 < V_adopt: a real old
  coordinator advertises 113 exactly.
- Old-coordinator simulation is the shipped SQL ctl knob:
  `select mo_ctl('cn','setprotocolversion', concat(regexp_substr(
  mo_ctl('cn','getprotocolversion',''), '<standard 36-char uuid hex
  pattern>',1,1), ':113'));` - identical shape for the second live CN
  uuid (regexp_substr occurrence 2; handleGetProtocolVersion enumerates
  the CN services first and appends the TN last, cmd_rpc_version.go:27-43,
  so the uuid occurrences select CNs only). regexp_substr is a stock SQL
  function (pkg/sql/plan/function/func_builtin_regexp*), so the case is
  launch-independent: no hard-coded uuids, no preprocessing, CI-runnable.
  The knob path is entirely existing code: ctl service/method dispatch
  (ctl.go:100-101, ctl/types.go:99-100) -> handleSetProtocolVersion
  (cmd_rpc_version.go:93-140) -> queryservice handleSetProtocolVersion
  (query_service.go:106, rpc_version.go:44-56) -> per-service
  runtime.ServiceRuntime.SetGlobalVariables(MOProtocolVersion) -
  process-local and non-persisted. In-repo pin/restore precedent on this
  exact global: pkg/frontend/computation_wrapper_test.go:5613-5650; the
  deployment semantic it serves ("deployment keeps MOProtocolVersion at
  the oldest live service during a rolling upgrade") is stated at
  dbutils/runtime.go:228. The new D3'' client-side advertisement reads
  the same coordinator-runtime global at request build
  (remoterunClient.go:725-733 requestStreamProtocols is the set-site),
  so the pin flips the advertisement itself - the only D3' input for the
  refusal.
- Old-fragment simulation is binary skew, not the knob: leg P3 below.
- Version-state readback and restore verification:
  `select mo_ctl('cn','getprotocolversion','')`
  (cmd_rpc_version.go:24-80) enumerates every CN service address with its
  CURRENT value; @ignore masks the dynamic uuid text while @regex
  asserts the value pattern (pinned 113s vs restored current) and the
  membership count.

Leg P1 - new/new public COMMIT and fresh-session readback:
- Both CNs fresh: no pin; the default runtime carries the build's
  MORPCLatestVersion (runtime.go:60-61), which post-bump advertises
  >= V_adopt.
- Steps: create the working table; seed the volume; run the mass UPDATE
  inside an explicit transaction; COMMIT; then the "@session:id=2"
  connection reads the rows back (fresh session, same CN1 endpoint).
- Assertions: the fresh session reads every committed row value through
  the transferred metadata objects (the public twin of the review's
  capable-peer success/COMMIT/readback regression); COMMIT order and all
  deterministic rows exact-match in the reviewed .result; the ordinary
  repeated-mutation battery (r20-r23 BVT) stays green on the same
  cluster run.

Leg P2 - old-coordinator/new-fragment rejection with the documented
rollback outcome and no partial effects:
- Version state: pin BOTH live CNs to 113 (the pre-merge advertisement)
  with the mo_ctl knob, then read back both values. Pinning both is the
  documented oldest-live-service rollout state (each CN's own runtime
  also gates receiver-side feature dispensation, remoterunServer.go:1322-1331,
  so this leg matches a real mixed cluster, not a partial fiction); the
  refusal itself consults only the REQUEST advertisement, so the
  coordinator's pinned value drives it either way.
- Steps: the identical fixture and the same mass UPDATE in an explicit
  transaction.
- Assertions: the statement fails with the D3' documented class - the
  sendError terminal's definite unsupported-capability error (the
  implementation pins the exact moerr class; the existing ErrNotSupported
  family, error.go:67/:457, is the default candidate) - and the reviewed
  .result records the exact client-visible text; the transaction takes
  the documented outcome (ROLLBACK after the failed statement);
  the "@session:id=2" fresh-session readback shows every target row
  unchanged: the refusal is pre-pipeline (nothing persisted, empty
  rollback), and the statement is atomic across the coordinator's local
  portion through the definitive failure propagated to the coordinator,
  so no partial effects survive anywhere in the table.
- Routing evidence (the review's demand): the refusal is itself the
  discriminator - only a fragment REMOTELY SENT below an adopted
  capability can produce this error class (a purely local execution
  consults nothing, so the expected .result cannot pass without the
  remote fragment path); corroborated by the GetProtocolVersion
  membership readback (two CN services on the same cluster the DML
  addressed), CN2's service log (the handlePipelineMessage receive with
  its debug message, remoterunServer.go:355 and :111), and the IN-CASE
  POSITIVE CONTROL: restore both CNs through the same knob and rerun the
  IDENTICAL mass UPDATE in a second transaction - it succeeds and
  commits; the same case file, only the version input flipped, separates
  the capability boundary from volume and topology noise.
- Hygiene: the restore clauses sit immediately after each refused leg's
  readback, and CI's per-job fresh cluster bounds the failure window
  (a case interrupted mid-pin leaves a process-local, non-persisted
  value that a CN restart clears).
- P1 and P2 share the file: the never-pinned COMMIT leg runs first, the
  refused leg with its positive control follows.

Leg P3 - the actual new-coordinator/old-fragment compatibility leg
(handler-executed differential as the r22 NC cycle is; CI runs uniform
builds):
- Version control: an NC hybrid of the binary - the fragment-side
  receiver is swapped byte-exact to the parent base HEAD 83222fa8 (the
  pre-fragment-machinery server), while the coordinator-side client and
  the regenerated pb keep the new additive field; the hybrid tree must
  compile (evidence the receiver additions are additive); both CNs run
  the one hybrid binary and only CN2 actually receives remote fragments.
  The expected minimal revert set is pkg/sql/compile/remoterunServer.go;
  if symbols force more receiver-side files, the implementation turn
  identifies and records the set as built. Shard-hash, swap, build, run,
  restore, byte-verify - the r22 NC cycle discipline.
- Steps: CN1 fresh (advertises >= V_adopt); run leg P1's exact case
  through COMMIT; fresh-session readback.
- Assertions: the old receiver ignores the unknown capability field
  (proto3 unknown-field behavior) and runs base semantics; the new
  coordinator's intake adoption of every RECEIVED batch stands (D3's
  unchanged direction, r9 lines 2145-2150); COMMIT and fresh-session
  readback succeed on the identical fixture. Residual, documented beside
  the review: in this direction the unreceived-tail under cancellation
  is the old server's own base behavior and narrows only as the rollout
  advances (r9's unchanged-direction paragraph).

Layering and order - cheapest discriminating first; the
validation-evidence contract still reuses every unchanged r23 layer:
1. the r23 evidence layers unchanged (classifier units, ring, race belt,
   full package) - reused with no rerun, inputs unchanged;
2. the r9 D4' compile-package legs unchanged, as the layer BELOW the
   public legs (message-level both-direction propagation, refusal,
   settlement, receiver absent/zero default);
3. leg P1 (the public path, the forcing volume, the readback shapes);
4. leg P2 (pin, refusal, no-partial-effects readback, restore, in-case
   positive control);
5. leg P3 (the NC hybrid cycle, run, restore, byte-verify);
6. the D1' disttae controls and the r8-D4 dispatcher rings where they
   share fixtures, and one full-package rerun last for CI parity.
The public legs are ADDITIVE to the existing battery; they replace no
fixture layer.

### Design-only checkpoint scope (round 26)

Rounds 24-26 changed no production and no test input; this revision again
changes ONLY the design document. The remaining live hole (the round-24
P1: committed clone-borrowed source objects deletable through the orphan
machinery in the r23 tree) still gates on this checkpoint's independent
PASS exactly as the repository contract orders; nothing in r10 is
implemented, and no test or code artifact changed in rounds 24-26.

## 10 design review record - round 26 (r10)

The round-26 finding was verified accurate first at each cited anchor:
the obsolete no-public-change rationale against D3''s SQL-visible
rejection, the mock-cluster CN-enumeration limit (util.go:118,
disttae_engine.go:562-574), and the new-receiver-only advertisement in
r9's mixed directions. The r10 response is D4'' above: the public fixture
(one in-repo distributed case file beside the session's existing battery,
driven by the unmodified shared harness against a real handler-owned
2-CN cluster with the issue's own topology), the version controls (the
shipped mo_ctl SetProtocolVersion/GetProtocolVersion pair,
regexp_substr-composed so no hard-coded uuids, per-process and
non-persisted; the V_adopt constant paired with the MORPCLatestVersion
bump; the P3 NC hybrid for the real old-fragment binary), the routing
evidence (the refusal itself as discriminator, the membership readback,
the CN2 receive log, and the in-case positive control), and the
assertions (P1 commit and fresh-session readback; P2 documented rollback
outcome with no partial effects; P3 forward-compatible commit/readback
with the documented residual). Blocking questions for the independent
reviewer: (1) confirm the mo_ctl pin pair is the right in-repo
version-control surface for the old-coordinator legs, with the both-CN
pin as the documented oldest-live-service rollout state and the stored
request advertisement as the only D3' input; (2) confirm the P3 NC-hybrid
discipline (minimal receiver-side revert set, compile-checked, recorded
as built, byte-verified restore) is acceptable evidence for the
new-coordinator/old-fragment direction; (3) confirm the forcing
conditions (multi-CN scope placement plus the spill threshold) together
with the in-case positive control constitute the required routing
discriminator. r10 is submitted for independent design review; this
handler does not self-approve the design gate, and implementation
proceeds only after the reviewer's PASS on r10.


## Revision r11 (round 27): real old-fragment binaries and the default-policy failure contract

The round-27 pre-PR review rejected design r10 at the design gate with two
P2 findings. Both were verified against the tree and the document before
this revision was written; both are accurate.
- Finding 1 (P3 does not exercise an actual old fragment): r10's leg P3
  (lines 2392-2405) reverted only remoterunServer.go inside one hybrid
  binary while keeping the regenerated protobuf, so the hybrid receiver
  DECODES the new field - the claimed old-decoder unknown-field behavior
  was never tested; the hybrid also kept the new producer reservation and
  teardown-retention machinery (s3writer_delegate.go:274-279 - the sinker
  capacity options at writer creation; :963-988 -
  retainUnrecordedSinkObjects; both absent from the parent file) while
  removing their settlement call, which exists only in the worktree
  server (settleRemoteFragmentWorkspaceAfterDrain, worktree
  remoterunServer.go:270/:732; the parent file has neither). A cluster of
  such hybrids combines mechanisms no parent binary ever had; a
  successful COMMIT/readback through it proves nothing about parent
  compatibility. Confirmed at every cited anchor.
- Finding 2 (statement atomicity must be proven before the explicit
  rollback): r10's leg P2 assertions (lines 2359-2369) inspect rows only
  after an explicit full ROLLBACK, an oracle that also passes if the
  refused statement leaves local writes in the transaction - the explicit
  rollback would wipe them. The default-policy chain verified: the
  whole-transaction rollback list is the twelve infrastructure codes in
  wholeTxnRollbackCodes (pkg/common/moerr/txn.go:17-20) and contains
  nothing for ErrNotSupported; sessionRollsBackTxnOnError
  (pkg/frontend/util.go:2266-2307) consults the
  mo_rollback_txn_on_error system variable, whose default is 0
  (pkg/frontend/variables.go:1687-1693); so with the proposed
  ErrNotSupported class the frontend's error path takes the
  statement-only branch (pkg/frontend/txn.go:1148-1170, Case2;
  RollbackLastStatement at :1163) and leaves the transaction open. The
  review's prescription - a default-policy leg with an earlier successful
  write, in-transaction verification before the rollback, then COMMIT and
  fresh-session readback - is required, with the full-ROLLBACK leg kept
  separately.

RESCISSION of r10 P3's hybrid claim: the paragraph at r10 lines 2392-2405
claiming the receiver-only hybrid proves the old-decoder direction is
rescinded. It survives ONLY as the optional ablation below, with its true
scope recorded: a diagnostic for the capability-call surface over a new
decoder, never evidence of parent-binary compatibility.

P3' - The actual new-coordinator/old-fragment leg, run with two separate
real binaries (replaces r10 P3's one-hybrid-binary leg; ci runs uniform
builds, so this remains a handler-executed leg with the r22 NC-cycle
discipline applied to TWO trees):
- Binaries: CN1 (the coordinator the case connects to) runs the PROPOSED
  build, compiled from the worktree. CN2 - the fragment receiver - runs
  the UNCHANGED PARENT BUILD, compiled from a pristine tree of parent HEAD
  83222fa8 (the branch's own base commit), extracted with git archive into
  a scratch build directory outside the tracked diff, so the worktree and
  its 36 paths are untouched. The parent binary carries its own protobuf
  (the parent pipeline proto stops at protocol_version = 19,
  proto/pipeline.proto:79, and has no coordinator field - verified: no
  coordinator_protocol_version anywhere in the parent file), its own
  producer/workspace machinery (no sinker capacity options, no
  retainUnrecordedSinkObjects, no settlement call), and its own receive
  path (parent handlePipelineMessage at its :295 with the same
  debug-message receive log at its :111).
- Topology: the same isolated cluster shape and port plan as legs P1/P2 -
  logservice, TN and CN1 on the proposed build - with exactly ONE
  variable: CN2's mo-service process is launched from the parent binary
  against the same isolated cluster endpoints. This isolates the
  new-coordinator-over-old-fragment contract from every other mixed
  interaction; if a failure surfaces that the reach evidence traces to a
  proposed-CN-to-parent shared-service interaction unrelated to the
  fragment contract, the implementation may scope shared services to the
  proposed build and keep ONLY CN2 parent, recording the choice and the
  evidence - CN2's parent binary is the non-negotiable requirement the
  review states.
- Steps: on the CN1 endpoint, leg P1's exact case - the mass
  metadata-producing DML inside an explicit transaction, COMMIT, then the
  "@session:id=2" fresh-session readback. No version pins: the skew is
  real. The GetProtocolVersion readback records it - CN1 at the bumped
  fresh value, CN2 at the parent's 113.
- Assertions and reach evidence (the review's demands): CN2's
  parent-binary service log shows the received pipeline fragment execution
  for the case's DML (the parent's debug-message receive line and its
  handlePipelineMessage execution) - the tested DML REACHED CN2; COMMIT
  succeeds; the fresh-session readback is exact, so the objects the old
  fragment persisted COMMitted as readable data through the new
  coordinator's intake - readable transferred objects. The only additive
  wire element in this direction is coordinator_protocol_version = 20:
  the parent decoder skips the unknown field per proto3 and runs base
  semantics; the new coordinator's intake adoption of every RECEIVED
  batch stands (D3's unchanged direction). If any other proposed-build
  change is wire-incompatible with the parent receiver, this leg fails
  and reports a real defect - that is its discriminating purpose. The
  unreceived-tail-on-cancellation residual remains the parent server's
  own behavior, as r9 recorded.
- Optional ablation (kept from r10, rescoped): the receiver-only revert
  hybrid may still be run for diagnosis of the capability-call surface
  over a new decoder; its result is never parent-compatibility
  evidence, and the review's caveat (new pb, kept producer mechanisms)
  is recorded beside any such result.

P2' - The refused-statement failure contract, defined, plus the
default-policy oracle (refines r10 P2; legs P2a and P2b below share the
fixture and in-case positive control with r10 leg P2):
- Defined contract (fixed in the design, no longer an implementation
  choice): the refusal error class is moerr.ErrNotSupported - 20105
  (error.go:67, message format at :457; construction NewNotSupportedf at
  :1079) - with the message naming the required coordinate capability
  (V_adopt) and the statement class; the transaction disposition under
  the DEFAULT policy is statement-only rollback: ErrNotSupported sits
  outside wholeTxnRollbackCodes (pkg/common/moerr/txn.go:17-20 - the
  twelve infrastructure codes), the mo_rollback_txn_on_error variable
  defaults to 0 (pkg/frontend/variables.go:1687-1693;
  sessionRollsBackTxnOnError at util.go:2266-2307), and the frontend
  therefore takes the Case2 statement-only branch
  (pkg/frontend/txn.go:1148-1170, RollbackLastStatement at :1163),
  leaving the transaction open and commitable. Applications that opt into
  mo_rollback_txn_on_error = 1 receive the whole-transaction rollback
  they asked for. This default disposition is a DESIGNED property of D3':
  an incapable-coordinator refusal must not doom the transaction.
- Leg P2a (default-policy statement-atomicity leg - the review's
  demanded oracle): BEGIN; an earlier successful in-transaction write on
  sentinel rows distinct from the refused statement's target (e.g. an
  UPDATE on a marker row, visible in-session); then the refused
  distributed statement fails with the documented unsupported-capability
  error text (the reviewed .result records it, and the harness continues
  because the error is the expected result); SAME-TRANSACTION
  verification BEFORE any rollback: the sentinel write is still visible
  (statement-only rollback; the transaction is open) and every row the
  refused statement targeted is unchanged (no local writes and no remote
  effects survive the statement rollback - this is the step that fails
  if the refusal path leaks workspace state); then COMMIT succeeds; then
  the "@session:id=2" fresh-session readback asserts the earlier write
  SURVIVED the commit and NO effect of the refused statement committed.
- Leg P2b (retained full-ROLLBACK leg, kept separately): BEGIN; the same
  refused distributed statement fails with the same documented error;
  the transaction takes an explicit full ROLLBACK; the fresh-session
  readback shows every row unchanged. This leg keeps r10's property
  checks - the refusal is pre-pipeline (nothing persisted at the
  fragment, the empty rollback needs no deletion) - while no longer
  carrying the atomicity proof, which P2a now owns.
- In-case positive control: restore both CNs' versions through the
  mo_ctl knob after P2b, rerun leg P2a's exact sequence in a fresh
  transaction - both the sentinel write and the mass DML commit, and the
  fresh-session readback sees both; the same case, only the version
  input flipped, remains the routing discriminator (the refusal class
  cannot occur without a remotely sent fragment advertising below
  V_adopt), corroborated by the GetProtocolVersion membership readback
  and CN2's receive log.
- Pins and hygiene unchanged from r10: both-CN mo_ctl pin to the
  pre-merge 113 for P2a/P2b, restores immediately after the legs,
  process-local values a CN restart clears.

D4'' layering and order with P2'/P3' (cheapest discriminating first; all
unchanged layers stay reused per the validation-evidence contract):
1. the r23 evidence layers unchanged (classifier units, ring, race belt,
   full package) and the r9-D4' compile-package legs +
   D1' disttae controls - reused as planned in r10;
2. leg P1 (the public path, the forcing volume, the readback shapes);
3. legs P2a, P2b and the in-case positive control (the defined error
   class and default disposition, the in-transaction oracle, the
   retained full-ROLLBACK property leg);
4. leg P3' (the two separate real binaries; CN2's parent build compiled
   from the pristine parent tree, byte-verified build provenance, run,
   restore);
5. the optional receiver-only ablation, only if diagnosis needs it;
6. the dispatcher rings, the NC differential and one full-package rerun
   last for CI parity.
The .result for the new legs is generated then reviewed
statement-by-statement; the mo-tester repository and shared
configuration stay untouched.

### Design-only checkpoint scope (round 27)

Rounds 24-27 changed no production and no test input; this revision again
changes ONLY the design document. The remaining live hole (the round-24
P1: committed clone-borrowed source objects deletable through the orphan
machinery in the r23 tree) still gates on this checkpoint's independent
PASS. Nothing in r11 is implemented; no test or code artifact changed in
rounds 24-27.

## 10 design review record - round 27 (r11)

Both round-27 findings were verified accurate first at every cited
anchor: the r10 P3 hybrid's regenerated pb, retained producer mechanisms
(s3writer_delegate.go:274-279, :963-988) and removed settlement call
(remoterunServer.go:732; the parent has none of them), and the
default-policy chain (wholeTxnRollbackCodes, mo_rollback_txn_on_error,
txn.go's Case2). The r11 response: P3' runs the coordinator on the
proposed build and the fragment receiver on the unchanged parent build
including its protobuf and producer/workspace dependencies - with
reach evidence (CN2's parent-binary receive log) and committed
readable transferred objects (post-COMMIT fresh-session readback) as
assertions, and the receiver-only hybrid demoted to an optional
ablation with its true scope recorded; P2' fixes the error class
(ErrNotSupported, outside the whole-transaction list) and the default
transaction disposition (statement-only rollback; the transaction stays
open) in the design itself, and adds the default-policy oracle - an
earlier successful transaction write, the refused distributed
statement, same-transaction verification before any rollback, COMMIT,
then fresh-session readback asserting the earlier write survives and
no effect of the refused statement commits - while the full-ROLLBACK
leg is retained separately as P2b. Blocking questions for the
independent reviewer: (1) confirm the two-binary topology isolating
exactly one variable (CN2 on the pristine parent build, shared
services proposed, with the recorded fallback if an unrelated
shared-service interaction blocks the leg); (2) confirm the fixed
public failure contract - ErrNotSupported with statement-only default
disposition and the mo_rollback_txn_on_error opt-in for
whole-transaction rollback; (3) confirm the P2a oracle and CN2
receive-log reach evidence close the round-27 gaps. r11 is submitted
for independent design review; this handler does not self-approve the
design gate, and implementation proceeds only after the reviewer's
PASS on r11.

## Revision r12 (post-merge conflict resolution): V_adopt renumbering to MORPCVersion130 and ownership weaves

The implementation branch rebased onto `be6636768` (2026-10-23 main),
which allocated MORPC versions 114-129 to other capabilities since the
original branch point. The decisions below resolve the merge without
changing any r11 contract; they renumber the adoption line and carry
main's new workspace-ownership invariants (#26796) into this design's
structures.

- V_adopt renumbers from 114 to MORPCVersion130 (pkg/defines), the first
  free version after main's 129: MORPCLatestVersion = 130, and every
  r11-D3'' reference to "114"/"113" above becomes "130"/"129" in the
  code (coordinatorIntakeAdoptionVersion), the UT fixtures, and the BVT
  case (coordinator pinned to the pre-adoption 129; the refusal text now
  reads "capability 130 or newer (coordinator advertised 129)"). The
  fail-closed principle is unchanged: only a build that carries the
  adoption intake can advertise >= V_adopt, and pre-rebase binaries
  (latest = 129 and below) remain incapable coordinators exactly as
  before.
- Main's #26796 workspace ownership (per-entry exact-key index with
  budget bytes, per-entry insert key-range envelope) now exists on the
  rebased base, so every batching mutation this design owns refunds the
  index through the same choke point main defined
  (unaccountInsertKeyOffsetsLocked) and maintains the envelope:
  - the staged merge's publication (Design A/r6) unaccount's the anchor
    index before the batch swap and unions each source's envelope into
    the survivor before retiring the source (the same invariants
    main's in-place merge helper carries);
  - both dump publications' retirement compactions (Design B) refund the
    retired entries' indexes alongside their workspace accounting, and
    their truncation now clears the vacated backing-array slots through
    main's clearVacatedWriteSlots (issue #26796's stale-reference
    contract applies to the retirement compaction's truncation exactly
    as to every other forward compaction);
  - the shrink pass and the entry release gate already had the calls
    (they come from main entirely), and compactNilWorkspaceSlotsLocked
    now also clears the vacated slots' stale references through main's
    clearVacatedWriteSlots, preserving main's memory-retention fix under
    this design's deferred (post-dump) compaction point;
  - upstream #26796 tests that pinned main's observables follow the
    boundary sequence this design owns (addBatchSelections ->
    mergeTxnWorkspaceLocked -> compactNilWorkspaceSlotsLocked), and the
    fully-superseded entry's observable is the retired nil slot, not a
    lingering zero-row batch (ensureInsertKeyOffsetsLocked fails closed
    for the nil-batch entry through the same provably-empty branch).
- Engine.Close merges main's alpha-stats shutdown reordering (stats
  producers joined before pClient) ahead of r11's single-budget drain;
  the drain deadline still covers waitUnpublishedPendingDrains, the GC
  pool release and the final deletion attempt. A failed pool release
  joins into closeErr through main's errors.Join but never skips the
  tail: the parked, context-blind worker is exactly what the detached
  final deletion attempt exists for (design A/B producers coordinate
  with the release's freed callers), so a release error keeps the
  attempt running under the same deadline exactly as before the rebase.
- dumpInsertTableSelectionLocked (main's extraction of the selection
  sizing) replaces the inline sizing the branch inherited from the
  original parent, and dumpInsert/dumpDelete keep Design B's staging
  semantics (mpIdxs/mpRows, stagedEnd, retire-only-published) with no
  inline retirement — main's keepElement model is obsolete here.
- mergeWorkspaceEntryBatchesLocked (main's upgraded in-place merge)
  stays in the tree as the isolated two-entry merge unit validating the
  shared key-index/envelope rules (issue #26796 envelope contract
  test); the production statement-boundary merge pass remains this
  design's staged flow, unchanged.
- The pipeline Message keeps its additive coordinator_protocol_version =
  20 wire field; pipeline.pb.go is regenerated from the merged proto
  with the repository's pinned protoc 3.21.1 + protoc-gen-gogofast
  toolchain.

Validation delta: the r11-D4''/D4' legs execute identically with the
renumbered constants (the UTs were renumbered to the same intent), and
the full r11 primary proof set is re-run on the rebased tree (focused
UT set + the BVT case legs); no new test surface is required by this
revision beyond the renumbering already present in those suites.

## 10 design review record - rounds 24-27 outcomes (r11 independent PASS)

This closing section records the independent review decisions the loop
prior to 2026-10-24 kept only in its private review artifacts, while the
submission records above ended at their "submitted for independent
review" states. The sequence, verified against those artifacts:

- Round 24 returned REQUEST_CHANGES for the r8 submission (reviewed
  content sha256
  5e8d032249eda255f8dabe4cbe2dcb518214d1a94242c44d29a25a3a924873c6),
  with three design blockers: the adoption-capable transfer must not
  delete successful writes when the coordinator lacks adoption
  capability; the design must specify how the coordinator advertises
  its capability to the fragment; the clone-protection predicate must
  keep its cleanup scope when shared. r9 resolves exactly these three
  (refuse-before-persistence for incapable peers, the per-request
  capability advertisement, the committed-borrowed-only orphan
  exemption) and rescinds r8's contradicted wordings recorded in its
  round-25 section above.
- Round 25 returned REQUEST_CHANGES for the r9 submission (reviewed
  content sha256
  0105fd23c57196ecb9ed7fbd3d9315b3ac871680a948b34c09d47494ce37e5d5)
  with one blocker: the public and mixed-version validation matrix
  remained incomplete. The reviewer confirmed the revised ownership and
  capability mechanisms ("address the previous three design findings").
  r10's D4'' resolves it: the in-repo public fixture over a real
  handler-owned 2-CN cluster with the shipped mo_ctl
  SetProtocolVersion/GetProtocolVersion controls and the routing
  discriminators.
- Round 26 returned REQUEST_CHANGES for the r10 submission (reviewed
  content sha256
  d9cd157031dc4f7bb098018ebcab47d33604cdf44dd5bf87795dcc338534580e)
  with three validation blockers: P3 does not exercise an actual
  old-fragment binary; statement atomicity must be proved before the
  explicit rollback; the UPDATE forcing conditions and fixture cost need
  correction. r11's P3' (the coordinator on the proposed build, the
  fragment receiver on the pristine parent build, with reach and
  committed-readability assertions), P2a (the default-policy oracle
  with statement-only rollback, P2b retained separately) and the
  corrected forcing matrix resolve them.
- Round 27 returned **PASS** for the r11 submission on 2026-10-22:
  Decision: **PASS at r11**, on the complete submitted document with
  content sha256
  74edf0d9ba9c2e1711467ef718b08f76b724c1a513b92c716db9547021e5c836.
  The reviewer's recorded basis: "P3' and P2a resolve the prior
  blockers", closing the three submitted questions (the isolated
  two-binary P3' topology; the fixed public failure contract -
  ErrNotSupported with statement-only default disposition and the
  mo_rollback_txn_on_error opt-in for whole-transaction rollback; the
  P2a oracle with the CN2 receive-log reach evidence). One nonblocking
  note was recorded: the legs must explicitly enable the parent
  receive-log witness by arming the existing inject_send_pipeline fault
  point with a substring identifying the target DML, assert its marker
  in the receiver's parent-binary log, and restore the fault state at
  teardown. That note is folded into the shipped cases: both
  update_remote_fragment_capability case files arm inject_send_pipeline
  with the 'rfc_t' substring and remove it in teardown. The reviewer
  bounded the approval's own scope: it "covers this exact design only;
  D1' remediation, implementation, and executable proof require
  subsequent full review."

The operative design approval for PR #30988 is this r11 PASS. This
handler does not self-approve the design gate; the decisions above are
the independent reviewer's, recorded here for repository traceability
after the fact.

## 10 design review record - r12 (post-merge conflict resolution) and the 2026-10-23 implementation approval

r12 above was authored after the r11 PASS, during the 2026-10-23 rebase
of the implementation branch onto main `be663676872` (main had absorbed
the design-review parent `83222fa837` and allocated MORPC versions
114-129 past the original branch point). Under the deviation rule it
changed no r11-approved decision: the adoption line renumbers to
MORPCVersion130 under the unchanged fail-closed rule; main's #26796
workspace-ownership invariants are carried through the same choke points
main defined; the regenerated pipeline protobuf only re-carries the
additive coordinator field alongside main's message changes; and
Engine.Close merges main's shutdown ordering ahead of the r11 drain.
No design decision was reopened, so none required r11 re-review.

The rebase-resolution content was independently covered by the
2026-10-23 implementation-review series: six consecutive complete-worktree
reviews against the cumulative diff from parent `83222fa837`, each
reusing the exact r11 PASS identity recorded above. The five
REQUEST_CHANGES rounds placed blocking findings precisely in the
r11/r12 territory - the intake's mode authentication before adoption,
the capability advertisement and mo_ctl pins running on the renumbered
V_adopt (with the additive protobuf generation verified), the
adoption-precedes-settlement transport proof, clone-materialization
admission before persistence, post-rollback reservation recycling, and
the failed-merge pending-cleanup handoff - each confirmed, fixed and
re-reviewed. The sixth review approved the complete implementation:
"Source, parent/head scaling evidence, r34 red/green recovery,
package/race results and BVT/mixed-version evidence support the fix. No
blocking findings.", recording two nonblocking test-hygiene notes
(synchronize on completed deletion rather than the call counter before
asserting file absence - the pre-existing load-sensitive window this PR
documents as a known risk; barrier the GC scheduling around the
transfer-snapshot inspections) which the approval carried as accepted,
documented tradeoffs.

Decision: r12 stands as a recorded conflict-resolution revision under
the operative r11 PASS, with its content independently covered by the
approved 2026-10-23 series above. The r12 document as shipped in PR
#30988 head `9694277ba9` carried content sha256
8cb0dab2bb76421fe7a227f4d9d0ec9a41f5cd595f43a3406d626bbf3cb5405c; this
closing record is documentary only - it records decisions already made
by the independent reviews and changes no design decision. The
deviation rule stands: the first change that alters any r11-approved
decision requires updating this document and re-reviewing the affected
decisions before implementation approval resumes.

## 10 design review record - round 35 (implementation follow-up): the reused writer's capacity admission rebinds to the current workspace

The 2026-10-23 implementation approval was followed by one further
blocking review (round 35, against head `604f6e4551`) that confirmed a
defect in the r4/f2b producer binding's reuse path and blocked on it:
the multi_update S3 delegate's persistent insert sinkers outlive their
transaction exactly as designed - a cached TP plan keeps the delegate
and its sinkers alive across executions for their buffer pools and
arenas - but the per-object capacity admission landed on them only at
construction time, so a reused delegate entered its next execution
still wearing the FIRST transaction's hook. Every object its next
spill persisted then reserved an in-flight unit through the FINALIZED
first workspace (the review's delegate probe observed first-owner
calls=2, second-owner calls=0; its real-workspace probe ended with
queued=0, live=0, inFlight=1), and no terminal of a later transaction
can reach such a reservation: repeated reuses permanently consumed
engine-wide admission capacity until unrelated persistence was
refused. §5.3's own invariant decides this - a reservation is
settleable only by the workspace whose statement persisted the object
- so the defect is an implementation gap inside the r11-approved
admission contract, and no design decision changes here.

The deliberate repair (the reviewer's first suggested option): a
reused delegate REBINDS its persistent sinkers' admission to the
CURRENT statement's workspace at every Prepare, before any spill of
the next execution can run. `Sinker.RebindUncommittedCapacityHook`
installs (or clears) the per-object hook on a live sinker;
`CNS3Writer.RebindWorkspaceSinkerCapacity` resolves the current
workspace through the same `WorkspaceSinkerCapacityOption` rules as
construction - a proc without a transaction operator, or a workspace
without the `SinkerOwner` surface, ends exactly as a freshly built
unhooked writer (never on the previous workspace's stale hook) - and
`MultiUpdate.Prepare` performs the rebind in the reuse branch beside
`refreshSelectorState`. Temporary writers, the deletion tombstone
writer and the insert operator were re-checked and construct fresh per
execution or per spill, so the persistent insert sinkers were the only
stale-hook surface.

Proof (commands run with the repository CGo wrapper
`.agents/skills/mo-dev/scripts/mo-cgo-test` on this tree):

- New focused regressions, red on parent `604f6e4551`, green after the
  fix - `go test -run TestMultiUpdatePrepareReuse
  ./pkg/sql/colexec/multi_update/`: pre-fix exit 1 with the review's
  exact shape (the second execution's spill consulted the stale first
  workspace's hook, second-owner calls=0; an owner-less reuse kept the
  stale hook), post-fix exit 0.
- Engine-level settlement through real workspaces and the real ledger -
  `TestIssue25590ReusedWriterReservationsSettleUnderReboundWorkspace`
  reconstructs the reviewer's probe on one shared engine: transaction 1
  spills, its commit settles its own unrecorded object to the cleanup
  queue, the drain deletes it; transaction 2 rebinds the SAME writer and
  spills - the reservation rides the current workspace's
  `unpublishedDumpReservations` (nothing on the finalized one), its
  commit hands the object to the queue, and the ledger closes at
  queued=0, live=0, inFlight=0 with both objects drained physically.
  This is the unit the pre-fix stale hook stranded.
- The refusal still gates through the rebound hook: a saturated
  CURRENT workspace refuses the reused sinker's Sync before anything
  persists (the saturated-round case inside the same multi_update
  regression), and an owner-less reuse restores the fresh-construction
  unhooked shape while the finalized workspace's hook consults
  nothing further.
- Packages touched: `./pkg/objectio/ioutil/`, `./pkg/sql/colexec/multi_update/`,
  `./pkg/sql/colexec/insert/` all green with the new tests included
  (`ok` each, exit 0), and `go vet` over the changed package closure
  exits 0.

Decision: round 35 is recorded as an implementation defect of the
r11-approved admission binding, repaired inside the unchanged decision
set (the binding now follows the execution rather than the writer's
construction, which is what the reservation-settlement invariants of
§5.3 and the producer protocol of §5.4 already required). No r11- or
r12-approved decision is altered, so the deviation rule opens no
design re-review; the two new sinks that reached validation with this
round - the inverse-resolution clear (an owner-less reuse must end
unhooked, not stale) and the reused-sinker refusal gate - are
decision-compliant consequences of the existing owner-surface rules,
pinned by the regressions above.
