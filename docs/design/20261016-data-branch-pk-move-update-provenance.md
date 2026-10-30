# Data Branch pk-move update provenance (#28026)

## 1. Revision and scope

| Field | Value |
| --- | --- |
| Owning issue | matrixorigin/matrixone#28026 |
| Implementation | PR [matrixorigin/matrixone#31629](https://github.com/matrixorigin/matrixone/pull/31629) (branch `agent/issue-28026-688adec6` on fork `gouhongshen/matrixone`), single PR |
| Current revision | r104, 2026-10-31, supersedes r103 |

| Revision | Date | Note |
| --- | --- | --- |
| r1-r71 | 2026-10-16 - 2026-10-24 | Initial protocol design and the first pre-PR review rounds. |
| r72-r101 | 2026-10-25 - 2026-10-30 | Post-review repair rounds; each round's disposition is recorded per-decision in section 11. |
| r102 | 2026-10-30 | Editorial compaction only — no protocol or behavior-design change. Answers the PR #31629 review by aunjgr: the tracked artifact now keeps the durable content (the problem/invariant protocol of sections 2-9, the invariant validation anchors of section 10, and the complete 222-decision register with its refinement chain, open items and approval trace of section 11) and drops the transient material the repository delivery contract excludes (per-run verification logs and walkthroughs, temporary local paths, one-off proof narratives, and full review transcripts). The Implementation field is corrected to name PR #31629. The superseded full text of r101 remains reachable as linked external evidence at the pre-compaction blob: <https://github.com/matrixorigin/matrixone/blob/430914c128ee5afd36880afd8f44d242bcc50f7f/docs/design/20261016-data-branch-pk-move-update-provenance.md> |
| r103 | 2026-10-30 | Editorial only — no protocol or decision change. Answers the PR #31629 design-gate review by aunjgr (2026-10-30T13:43:12Z): section 11 now identifies the independent design approval of record for the delivered revision with its reviewed content hash, public byte link, reviewer identity, and a durable artifact link. No completed review was rerun; no review transcripts are added to the repository. |
| r104 | 2026-10-31 | Editorial only — no protocol or decision change. Answers the PR #31629 Codex automated follow-up review (2026-10-30T20:27:13Z, non-blocking P2): sections 3, 7 and 10 now name the delivered v4_0_19 tenant-upgrade vehicle and its real test package instead of the retired 4.0.8 design-time placeholder; the D149 register entry already states the version-neutral forward-placement rule, and no register entry changed. |
## 2. Problem, evidence, and invariants

A pk-moving `UPDATE` executes in `multi_update` as a delete of the old row
plus an insert of the new row, with no durable operation identity.
`CollectChanges` therefore exports the old (rowid, pk) as a tombstone and
the new (rowid, pk) as independent data, byte-identical to a genuine
`DELETE` followed by an `INSERT`. `findDeleteAndUpdateBat` correlated the
halves only by primary-key value, so once the key moved the halves replayed
as `DELETE`+`INSERT` on the destination: the parent delete violates the
foreign key's implicit RESTRICT check in the window before the replacement
insert lands, and ER-generating cascades would run as DELETE cascades
instead of UPDATE cascades.

Concretely reproduced on issue #28026: `data branch merge merge_src into
parent_t when conflict accept` after `update merge_src set id = 3 where id
= 2` failed with ERROR 20101 (`Cannot delete or update a parent row: a
foreign key constraint fails`) while a native `update parent_t set id = 3
where id = 2` cascades. Matching replay halves by commit timestamp,
transaction membership, or iteration order would conflate independent
`DELETE k; INSERT k2` with updates and could fire unintended cascades, so
recovery needs explicit per-row old->new provenance.

### Invariants

- I1 (identity): a tombstone+insert pair folds into an update only when a
  *committed* provenance chain proves row continuity from the tombstone's
  (old pk, old rowid) to one final replacement row. Unknown, ambiguous, or
  contradictory provenance always degrades to the legacy delete+insert
  replay, which fails loudly on FK-referenced parents and never
  re-parents child rows silently.
- I2 (counterexample): an independent `DELETE k; INSERT k'` in one
  source transaction records no link and must keep delete+insert replay,
  including the FK error. Provenance must never be inferred by correlation
  alone.
- I3 (precedence): provenance resolution runs before legacy same-key
  matching, so an unrelated new row sitting at the moved-from key is
  preserved as its own insert and cannot suppress the move.
- I4 (conflict lineage): conflict handling must resolve the *destination's*
  lineage of a contended row. Under ACCEPT a source move applies to the
  destination row that the destination itself moved; an apply whose exact
  key predicate matches zero destination rows aborts loudly instead of
  reporting success.
- I5 (window): replay and PICK key selection consider only moves relevant
  to the diff window. A move that predates the destination's fork never
  admits keys or folds pairs for this diff.
- I6 (ownership/bounds): the account that updates a table owns the table's
  link rows; the rows retire with the table's protected delta stream —
  immediately on drop for unbranched tables, and only when the whole
  branch subtree is deleted for lineage participants. Reader memory
  is bounded by the keys a diff actually probes, never by the whole
  history of the catalog.
- I7 (atomicity): proving, conflict resolution, apply, and the FK actions
  the apply triggers all commit or abort in the MERGE/PICK transaction.
  Link writes commit atomically with the UPDATE that produced them.
- I8 (owner-scoped reads): every provenance read for a diff segment
  executes under that segment's authorized physical owner - the identity
  the routing layer opened the segment's change stream under. A recorded
  move is visible to every supported operand route, including
  subscriptions; a read never crosses an account the routing layer did
  not authorize.

### Non-goals

- REPLACE INTO / INSERT ... ON DUPLICATE KEY UPDATE / multi-target UPDATE
  paths keep delete+insert semantics; expanding their coverage requires a
  new eligibility review (see §11).
- Sibling pre-existing gap: an ordinary stable-key source update, or a
  plain source delete, whose destination counterpart *moved keys* resolves
  neither lineage today (pre-existing on main; stable-key update
  vs destination move can apply a stray insert at the vacated key). The
  zero-match guard in §6.3 makes the regression-free subset loud; the full
  family is follow-up work under this design's destination-lineage
  resolution.
- Garbage collection of link history for live tables beyond DROP/TRUNCATE
  reclaim (see §9.3, open question O1).

## 3. Why this requires a design review

Size: ~700 production lines added across the complete feature in r2.
Complexity triggers (any one suffices):

- crosses planner, multi_update executor, frontend branch diff/apply,
  bootstrap tenant-upgrade, and restore subsystems;
- changes a catalog / persistent contract
  (`mo_catalog.mo_data_branch_update_link`), including its key schema;
- affects tenant upgrade (v4.0.19, D149's forward version),
  mixed-version operation (tenants
  without the table), restart, backup/restore (table skipped on restore);
- adds a collaboration protocol between statement-time writes and
  async-time reads with identity invariants (I1/I2) that section §5 makes
  exact.

## 4. Standards, precedent, and alternatives

### Precedent used

- Change-data-capture convention that an update is a (before-image,
  after-image) pair *joined by identity*, not by value; MO's own
  `table_changes` streams expose commit timestamps with rows, which §5
  reuses as birth evidence.
- Write-ahead / provenance logs: the recorder writes the linkage at the
  moment the operation executes, in the same transaction; consumers replay
  it verbatim instead of re-deriving it. This mirrors how MO persists
  auto-increment metadata (`mo_catalog.mo_increment_columns`, per-account,
  table-id keyed, reclaimed on drop) and merge settings
  (`mo_catalog.mo_merge_settings`), including the drop-time reclaim hook in
  `Scope.dropTableSingleResolved`.
- Foreign-key action semantics: preserving the operation class (UPDATE)
  lets the existing FK engine run `ON UPDATE` actions natively; no new FK
  logic is introduced.

### Alternatives considered

| Alternative | Verdict |
| --- | --- |
| A1: infer pairs consumer-side by commit ts / txn id / iteration order | Rejected (pre-existing finding in the issue): a genuine `DELETE k; INSERT k2` in one txn is stream-identical; inference fires false cascades and violates I2. |
| A2: persist `new_row_id`, giving each hop an exact successor identity | Accepted in modified form (r3): the operator cannot know the replacement row id (the workspace assigns it during staging, and the operator's staged batch is a txn-private copy it never sees), so the operator writes the column NULL and the disttae workspace back-fills it from the same transaction's already-staged inserts when the link insert itself is staged. The r2 objection — engine code paths may rewrite insert rowids before commit — materialized in r4: the commit pipeline reassigns every staged insert's rowid at precommit, so the recorded id is a same-transaction chaining credential only, and the fold runs on the commit-timestamp tie (D14). Rows the workspace never stages (the S3 update path writes no links at all) keep NULL and still fold single-shot. |
| A3: operator read-back of the final rowids through a snapshot reader after each insert | Rejected: introduces a new engine-reader dependency into `multi_update`, double memory/latency on a hot path, and still races commit-time reassignment. |
| A4 (r2's selection): persist (old pk, old rowid, new pk, write_ts) and validate chains consumer-side against the committed change stream via occupancy order | Superseded by r3: the review proved the r2 walk's one residual mis-association (NF1) reachable with ordinary DML, so occupancy alone can no longer be the identity argument. The r2 decision record remains below for history (D6, superseded by D8). |
| A5 (r3 selection): durable row identity — persist (old pk, old rowid, new pk, new_row_id, write_ts) and walk chains by following the recorded departures of the lineage's own row id | Accepted, amended by r4 (D14): the recorded id proved to be a pre-commit workspace placeholder rather than the committed row's id, so claims run on the commit-timestamp tie between the deleting tombstone and the landed row; the placeholder still pins same-transaction netted hops. Nothing is inferred from key occupancy alone (I1). |

## 5. Logical model

### 5.1 Catalog contract

```sql
create table mo_catalog.mo_data_branch_update_link(
    link_id    binary(64)      not null comment 'be table_id(8) | sha256(old_pk)(32) | old_row_id(24)',
    table_id   bigint unsigned not null comment 'table whose row was updated',
    old_pk_sha binary(32)      not null comment 'sha256 of the packer-encoded old pk',
    old_row_id binary(24)      not null comment 'row id of the row before the update',
    old_pk     varbinary(65535) not null comment 'packer-encoded primary key before the update',
    new_pk_sha binary(32)      not null comment 'sha256 of the packer-encoded new pk',
    new_pk     varbinary(65535) not null comment 'packer-encoded primary key after the update',
    new_row_id binary(24)      null comment 'row id of the replacement row; NULL when identity was not recorded',
    write_ts   bigint unsigned not null comment 'physical ns of the updating txn creation timestamp',
    primary key(link_id)
);
```

- The schema deliberately carries no secondary index. Rows are written by a
  raw `Relation.Write` side effect of the root `multi_update` node, and raw
  relation writes do not maintain secondary index tables, so a declared
  index would sit empty and silently mis-answer every predicate the
  optimizer routes through it. All probes therefore ride the `link_id`
  primary-key prefix: exact/prefix range conditions over
  `cat(table_id, sha256(pk))` (old-pk probes) or `cat(table_id) + 1` window
  scans with in-memory `new_pk_sha` filtering (PICK closure and per-table
  range scans).

- One row per row whose pk actually changed, written by the root
  single-target UPDATE's `multi_update` node via a synthetic link
  `UpdateCtx` (a marker/schema carrier with no column refs), inserted
  through the same transaction workspace as the user write (I7).
- The single-column `link_id` primary key packs `(table_id,
  sha256(old_pk), old_row_id)` so a per-chunk probe
  (`table_id = ? AND old_pk_sha IN (...)`) resolves through one
  primary-key prefix range instead of a three-column composite scan.
- `old_pk`/`new_pk` use the exact `types.Packer` encoding of the
  data-branch hash map keys, so stored bytes probe hash keys verbatim.
  `*_sha` columns bound key/index width; equality decisions always
  re-check the full stored `old_pk`/`new_pk` bytes as defense against
  hash collision.
- `new_row_id` is the workspace-stage identity of the replacement row
  (r4, D14): a pre-commit placeholder minted in
  `colexec.TxnWorkspaceSegment`, durable only as evidence the moving
  transaction itself recorded — the committed stream row carries the id
  TN assigns at precommit instead (§1). The operator writes it NULL —
  only the txn workspace knows the id it assigned — and the disttae
  workspace back-fills it in
  `Transaction.writeBatchWithAutoIncrEpochKnown` when the link insert is
  staged, resolving the newest staged insert of the same `(account,
  table, new_pk)`. Ordering is guaranteed: the planner places the
  synthetic link ctx directly after the owning main-table ctx and both
  execute inside one statement, so the replacement insert is staged
  first. Its sole consumer role is chaining same-transaction netted
  generations (§5.2); the fold itself runs on the commit-timestamp tie,
  so a NULL that survives staging (e.g. the replacement went straight to
  S3) still folds single-shot and only netted continuations become
  unprovable. The `UpdateWriteS3` operator path writes no link rows at
  all (fail closed, legacy replay).
- `write_ts` is the *creation* timestamp of the moving transaction
  (`CreateTS` of the txn operator), in physical nanoseconds. The create
  timestamp is constant for the whole transaction, which the
  same-transaction chain match (§5.2) requires: under read-committed the
  statement executors refresh `SnapshotTS` per statement, so two
  pk-moving UPDATEs of one transaction would record *different*
  snapshots and could never chain. It still lower-bounds the commit
  timestamp (commit > create), so window probes keep their one-sided
  sound filter (D2).
- ROLLBACK removes link rows with the user rows (workspace removal);
  the commit atomically publishes move + link (I7).

### 5.2 Chain resolution (commit-timestamp tie, r4)

The r2 occupancy-order walk derived hop identity from snapshot-ordered
first succession, which left one reachable mis-association (NF1): after
`UPDATE 2->3; DELETE 3; INSERT (3, 'unrelated')`, the surviving unrelated
row satisfied every occupancy check and was folded as the move's
replacement, re-parenting destination children to an unrelated row. r3
pinned every hop to a recorded durable *row id*; r4 keeps that
"never infer continuity from occupancy or ordering" stance but replaces
the hop credential, because the recorded id turned out to be a pre-commit
workspace placeholder (§1, D14) rather than the committed stream row's
id. The r4 credential is the commit timestamp the stream itself carries
on every data and tombstone row.

Two stream facts make the timestamp tie an identity proof:

- F1 (pk uniqueness at every committed instant): one key holds at most
  one committed live row at any instant, so at most one live row under a
  key can be born at one commit timestamp.
- F2 (one committed image per key per transaction): a transaction's
  workspace nets its own insert/delete churn on one key before commit
  (born-died rows never reach the stream; re-inserts collapse), so a
  commit timestamp names at most one row version per key.

Definitions, all available at diff time from the already-materialized
per-side hash maps plus catalog probes:

- `live(K)`: data rows present under encoded key `K` in the data hash
  map (each carries row id + trailing commit ts `birth`).
- `tombs(K)`: tombstone rows under `K` (each carries deleted row id +
  commit ts).
- `departed(R)`: the birth commit ts of the version the in-window
  tombstone `R` deleted, captured while both were present (one
  pre-pass `capturePkMoveStreamEvidence` runs inside
  `buildHashmapForTable` ahead of its per-key reconciliation, which
  otherwise nets the pair out of the maps — §5.4). The pairing is by
  birth order, never by row id (r6): the deleted version is the unique
  data row under the same key with the greatest birth strictly below
  the tombstone's commit ts — unique by committed pk uniqueness at
  every instant — while row ids are storage-attributed and diverge for
  one logical version across a flush (S3, §5.4). Two data rows sharing
  that greatest birth are corruption: nothing is recorded and hops
  over the deletion fail closed.
- `link(K0, R0)`: the unique catalog row with `old_pk = K0` and
  `old_row_id = R0` (the exact hop-1 anchor).
- `succ(K, R)`: the catalog rows with `old_pk = K` and `old_row_id = R`
  — recorded departures of that exact row version. Zero means "no
  recorded departure"; several (reincarnation), or any uncertainty about
  probe completeness, mean "unprovable". All three fail closed.

Walk for one LCA-hit tombstone `(K0, R0)`:

1. `L1 = link(K0, R0)`, else fail closed. The anchor names the deleting
   tombstone's own row id, so an independent `DELETE/INSERT` reusing
   `K0` can neither start a chain (I2) nor be mistaken for one.
2. Seed the birth certificate: the unique tombstone under `K0` naming
   `R0` commits at `T` (zero matches or two matches fail closed —
   anything else is stream corruption this fold refuses to interpret).
   `K = L1.new_pk`, snapshot `S = L1.write_ts`, inhabitant
   `I = L1.new_row_id` (possibly a workspace placeholder or NULL).
3. Repeat (cap 32 hops, visited-key set seeded with `K0`):
   - CLAIM: if `live(K)` holds a row with `birth = T`, it is the
     lineage's version (F1+F2); rows born at other timestamps are
     unrelated occupants and keep their plain insert meaning. Two rows
     born at `T` under one key are a corrupt impossibility: fail closed.
     The claim stands when `succ(K, claimRowID)` answers zero with
     complete evidence (ambiguous or incomplete evidence fails closed);
     the walk then ends: fold (tombstone, row) into one key-moving
     update. A candidate with its OWN recorded departure is not the
     lineage end: it defers to the departure steps below, which bind
     the hop to that version's tombstone when the departure is
     in-window and fail closed otherwise.
   - CROSS-TRANSACTION DEPARTURE: otherwise, a tombstone under `K`
     committing at `T' > T` whose departed version was itself born at
     exactly `T` (`departed(R) = T`) and that pairs with the unique
     `succ(K, R)` continues the walk: `K = L'.new_pk`, `T = T'`,
     `S = L'.write_ts`, `I = L'.new_row_id`. Tombstones at or before
     `T` name strictly older versions of the key, tombstones whose
     departed birth is missing or differs from `T` belong to an
     occupant's own lineage, and tombstones without paired links are
     unrelated churn: all three are skipped. Two continuing departures
     from one landed key fail closed.
   - SAME-KEY TAIL (r7, D16): otherwise, a tombstone under `K`
     committing at `T' > T` whose departed version was born at exactly
     `T` but that pairs with NO link (`succ(K, R)` answers zero with
     complete evidence) records the lineage version dying without a
     move: the key kept the row. `T = T'` and the walk re-verifies the
     re-born image at the SAME key (one re-entry exempt from the
     visited-key guard, per tail step; `T` strictly increases so the
     hop cap still bounds the walk). The claim then accepts only a
     live row born at exactly `T'`: a stable-key update folds into the
     lineage, and so does a same-transaction `DELETE`+`INSERT` pair at
     the same key — the two are indistinguishable in the stream, and
     the legacy stable-key update matcher already folds that pair as an
     update (#27996 semantics, kept consistent here). A plain delete
     finds no such row and fails closed; a re-insert committed by any
     later transaction has the wrong birth and fails closed. The
     consumed link's inhabitant is cleared: it named the version that
     just died, and no same-transaction continuation may chain off it
     afterwards. A second tombstone under `K` whose departed birth also
     equals `T` contradicts committed one-image-per-key (F1+F2): fail
     closed.
   - SAME-TRANSACTION CONTINUATION: otherwise, the landed generation was
     netted out of the stream by its own transaction. The unique
     `succ(K, I)` may continue the walk only if `I` is a
     workspace-segment placeholder (the harvested pre-commit identity)
     and `L'.write_ts = S` (one transaction create timestamp). `T` and
     `S` carry over; `I = L'.new_row_id`. A NULL inhabitant or a non-
     workspace identity cannot chain netted hops: fail closed.
   - Landing on a previously visited key by a cross-key hop (cycle,
     including reincarnation `2->3->4->3`) fails closed even when a
     longer walk could in principle resolve it. Same-key tail
     re-entries carry a strictly newer birth certificate and are the
     only exemption.

Soundness argument: the anchor identifies one physical row version and
the anchor tombstone identifies the one transaction that deleted it.
Every hop either (a) claims the unique version that transaction's commit
timestamp could have born at the landed key (F1+F2), with recorded
departures of that exact row required to be absent — proof the lineage
ends here; (b) crosses one recorded departure of a version proven to be
the one born at the running certificate `T`; (b') advances the
certificate to the death timestamp of that proven version when no move
was recorded, re-verifying at the same key — the departed-birth match
certifies the deleted version was the lineage's own, and the final
claim re-verifies the re-born image, so only the deleting transaction's
same-key successor can ever be claimed; or (c) crosses one
same-transaction netted generation through the placeholder identity the
moving transaction itself recorded. Links of any *other* row holding the
same key never match the departure evidence (their own births differ
from `T`), so an impostor occupant (NF1), its own moves, and any
same-key reincarnation cannot be followed or claimed. Every unprovable
shape (NULL identity mid-chain, ambiguous successor, cycle, missing
stream identity evidence, duplicate births at `T`, duplicate lineage
departures, probe budget exhaustion) degrades to the legacy
delete+insert replay, which stays loud on referenced parents.

Claimed finals join a per-diff claimed-key set (computed in one
classification pass before any residual tombstone is emitted): an
unpaired tombstone at a claimed key belongs to the displaced previous
occupant of that key and is absorbed (skipped) instead of replayed as a
delete that would kill the lineage row the fold just installed.

### 5.3 Reader paths and bounds (I6)

- Provenance is scoped by *physical table* and *time window*, not by the
  endpoint table id alone: `constructChangeHandle` records one
  `(table_id, endPhys)` window per relation on each side's branch path,
  so a multi-generation branch (src forked from parent, leaf forked from
  src) resolves moves no matter which generation recorded them. Every
  probe of one window carries `write_ts <= endPhys`, and the PICK closure
  additionally carries its `write_ts >= lowerBound`, so moves outside a
  diff's collected commit interval can neither fold nor admit keys (I5,
  NF6/NF7). A source read with an LCA-timestamp endpoint therefore never
  sees post-endpoint history.
- Pairing loads links on demand: per tombstone chunk, prefetch per window
  over `link_id` prefix ranges for the chunk's keys; continuation hops
  probe per key lazily and cache. Negative answers are remembered per
  `(table_id, key)` so a key is probed once per window.
- Every SELECT is `LIMIT <remaining row budget + 1> ORDER BY link_id`, so
  arbitrarily large history is never materialized (NF8): rows are counted
  as they decode, the fired limit is *visible*, and crossing the budget
  disables the provider for this diff — unresolved verdicts aborting the
  apply per D51/D56, anchors with independently proven absence keeping
  legacy replay — with its caches released so a disabled provider holds no
  catalog history alive.
- Incomplete evidence is a distinct state from complete absence (NF5):
  successors answer `(found, complete)` and a truncated probe or a
  disabled provider reports `complete = false`, which the walk treats
  exactly like unproven identity — including at the terminal claim, so a
  walk that already possesses its anchor can never turn a mid-walk budget
  event into a successful fold.
- The PICK key closure iterates an admitted-key frontier to a fixpoint
  per window inside the same `[lower, endPhys]` interval with the same
  LIMITed reads; a truncated link read stops expansion early (fewer
  dependency keys only, never fabricated ones), and the admission cap
  guards against corrupt topology.
- Provenance reads are scoped by physical table and time window *and by
  the segment's physical owner* (I8): windows projected from a
  `collectRange` carry the segment identity the routing walk recorded,
  and PICK closure windows resolve their owner by the same
  ordered-candidate winner rule as the reopen walk (walking endpoint
  identity first, then the opposite side; equal accounts collapse).
  Whole-window probes, per-key overflow fallback reads, the D156
  scope-to-commit capture, and the per-account catalog existence check
  all execute under the resolved owner identity; a pre-upgrade owner
  degrades to complete absence for that account's windows only, and a
  local-local diff collapses to the caller identity byte-identically.

### 5.4 Change-stream shape (r5): preserving born-and-died generations

Two independent net-effect passes can erase exactly the §5.2
cross-transaction evidence, and a third hazard distorts the identity
the evidence is keyed by; the design has to defeat all three:

- S1 (collector coalescing): `PartitionChangesHandle` buffers one range
  and `logtailreplay.filterBatch` coalesces every buffered
  (data, tombstone) pair to the last operation per primary key, *unless*
  the caller supplied `engine.WithCollectChangesPreserveAllVersions`.
  Under the default coalesced stream, a version born and deleted inside
  the collect window vanishes entirely — its insert and its delete never
  reach the diff — and this holds across transactions. Verified live:
  `mo_ctl flush` and global checkpoints between the source updates do
  not resurrect the pair, because the coalescing is a read-time net
  effect over buffered operations, not an artifact of in-memory
  locality. A mid-chain generation's tombstone is therefore absent from
  a coalesced stream in principle, and the §5.2 cross-transaction
  departure step is dead code against it. `constructChangeHandle` now
  opts apply-capable diffs with a real primary key
  (`copt.expandUpdate && pkKind != fakeKind`) into
  `WithCollectChangesPreserveAllVersions`. Diff output is unchanged:
  `buildHashmapForTable`'s own per-key reconciliation (S2) collapses
  versions back to the same net rows for plain replay. The cost is
  holding every in-window version of high-churn keys during the build
  instead of their coalesced image.
- S2 (hash-map reconciliation): `buildHashmapForTable` ends with a
  per-key pass that keeps only the latest tombstone per key, pops data
  rows older than it, and drops the tombstone itself when stale data
  rows consume it ("the net effect is a pure INSERT"). This parallel
  `ForEachShardParallel` iteration also owns the tombstone map
  exclusively, so chain walks may not query it then. The evidence the
  walk needs is therefore captured by `capturePkMoveStreamEvidence`
  inside `buildHashmapForTable` *before* that pass (one legal
  pre-iteration pre-pass building `pkMoveStreamEvidence.tombs` and
  `pkMoveStreamEvidence.departed`), and rides alongside the maps to
  `findDeleteAndUpdateBat`. A walk then binds a cross-transaction hop
  to a tombstone the reconciliation later netted away, with the
  departed version's birth already paired. Direct callers without
  threaded evidence fall back to a degraded inline capture against the
  post-reconciliation maps (unit fixtures; mid-chain hops are then
  simply unprovable, fail closed).
- S3 (storage-identity divergence, r6): row ids in the change stream
  are storage-attributed. After `mo_ctl flush` rewrites a version's
  block, the stream names that version's *export* identity while the
  delete of the same version names the flushed-block identity it
  targeted; verified live, the two differ for one logical version
  (tombstone row id != data row id under the same key, births one
  commit apart). Pairing tombstone to version by row id therefore
  silently records nothing and every cross-transaction hop over the
  flushed generation fails closed. Commit timestamps carry no storage
  attribution, and link `old_row_id` stays consistent with its
  tombstone (one transaction reads and deletes the same identity), so
  the birth-order pairing of §5.2 `departed(R)` is the sound join:
  birth timestamps identify the version, row ids only the storage
  image. A claimed lineage end needs no such join at all: a data row
  survives S2 only when its birth is at or after its key's newest
  tombstone, so a claimed version provably has no later tombstone and
  its recorded-departure probe space is complete.

With both in place, the D/F reachability classes fold as designed:
a cross-transaction chain (`2->3` committed, then `3->5` committed)
surfaces the `(3, T2)` tombstone and pairs it with the `T1` birth, and
a mixed chain (same-transaction netted generation followed by a
cross-transaction departure) passes through both hop kinds. Anything
short of that (impostor re-occupation, ambiguous successors, straddling
transactions whose links pre-date the window) still fails closed to the
loud legacy replay.

### 5.5 Diff/apply changes

- Ordering (I3): within each tombstone chunk the LCA-hit rows run the
  §5.2 walk *before* legacy same-key matching (`PopByVectorsStream`).
  Claimed tombstones (marked seen) and popped finals are invisible to the
  legacy pass; untouched rows take the historical path verbatim.
- Output: folded moves apply as
  `UPDATE <dst> SET <all writable incl. pk> WHERE <exact where key> LIMIT 1`.
  The where key defaults to the link-proven OLD key; I4 overrides below
  replace it when the destination moved the same row.
- New-key conflict resolution (NF2): a folded move whose NEW key an
  independent destination row holds is a conflict like any other, decided
  by the configured mode *before* anything is consumed or applied. FAIL
  aborts the diff with the conflict error; SKIP drops the move (its
  tombstone half is already inert); MERGE-ACCEPT applies the move and
  deletes the destination occupant inline at apply time; PICK-ACCEPT
  routes the occupant through the conflict pipeline as the victim pair.
  Folded rows the conflict machinery already owns (both-moved, LCA
  inserts) are marked resolved so this probe never double-counts them,
  and nil-conflict diffs keep the legacy shape (at most one destination
  row can hold a key, and nothing in the diff window deleted it).
- Single-pass claimed classification (NF3): displaced-occupant
  absorption (§5.2) is classified once from the final claimed-key set
  after all folds, before remaining tombstones are emitted — never
  interleaved with the fold loop — so a claimed tombstone can never be
  both absorbed and replayed.
- PICK selection by proven lineage (NF4): the closure's admitted keys
  travel in a dependency set distinct from the literal selection, and a
  folded move may only be produced for a literally selected anchor
  (`pairPkMovingTombstones` skips dependency-only anchors). An unrelated
  lineage whose key collides with a dependency key is never applied,
  and neither are its children.
- Zero-match guard (I4): immediately before each key-moving UPDATE, the
  apply executor runs `SELECT count(*) FROM <dst> WHERE <same predicate>`
  in the same transaction and aborts MERGE/PICK unless the count is
  exactly 1. Key-moving UPDATEs execute inline before staged
  deletes/inserts, so source rows inserted at a vacated key cannot claim
  it first.
- Destination-lineage override (I4): while collecting destination-side
  (base) batches, the conflict layer remembers, per key-moving
  destination row, `old key -> destination's NEW key`. When a source
  key-moving insert meets a destination key-moving delete with the same
  old key (both sides moved the same LCA row), ACCEPT applies the source
  version with `WHERE <destination's current key>`; SKIP already keeps
  the destination move (delete-half suppression propagates to the insert
  half by old key); FAIL reports the conflict on the old key. PICK flows
  through the same machinery (ACCEPT with preserved conflict markers)
  and inherits the same behavior plus its victim-delete rule for an
  independent destination row claiming the NEW key.

## 6. Lifecycle, failure, and atomicity

### 6.1 Commit points

- Statement: user UPDATE + link rows commit atomically (workspace);
  the link write executes inside the user UPDATE statement's own
  transaction through a raw relation write, and a failed link write
  aborts the statement rather than dropping provenance silently
  (fail-injection UT: `TestInsertUpdatePkMoveLinkRowsWritePath`).
  ROLLBACK removes both link rows and user rows (workspace removal).
  Identity back-fill happens at staging time inside the same transaction
  (disttae workspace, holding `txn.Lock`, before the entry is formed), so
  commit and rollback need no extra handling.
- DDL: DROP TABLE / TRUNCATE reclaim link rows in the dropping account,
  inside the drop transaction (see §9.3): immediately for tables without
  branch lineage, and — for lineage participants — for exactly the
  tables whose complete branch subtree the protect-snapshot flow retires
  in the same transition, so a dropped intermediate generation keeps its
  links while live descendants can still diff against its stream.
  Reclaim failure leaves harmless orphans (never consumed, keyed by a
  dead table id) and is logged, not fatal. A rolled-back drop leaves
  links intact.
- MERGE/PICK: diff, guard counts, key-moving UPDATEs, staged
  delete/insert flushes, and their FK actions commit or abort as one
  transaction (executor runs with forced pessimistic RC); a RESTRICT
  violation, zero-match guard failure, or any SQL error aborts
  everything.

### 6.2 Failure, cancellation, retry, duplicate delivery

- Diff producer cancellation releases the same ret-pool batches as
  before; validator failure modes are error-return or fail-closed
  degrade, never partial folding.
- MERGE is naturally re-runnable: a successful MERGE removes the
  difference, so a retry diff is empty; an aborted MERGE replays from
  scratch because provenance is read-only during apply.
- Statement-level retry of a pk-moving UPDATE rewrites identical link
  PKs; duplicate key on the link PK cannot occur across distinct
  committed moves because old rowids are never re-killed (a rowid dies
  once).
- The `UpdateWriteS3` write path records no links (§5.1); large
  intra-statement S3 flushes of the owning table resolve `new_row_id` to
  NULL. The no-link shape degrades to legacy replay; the NULL-identity
  shape still folds single-shot by the commit-timestamp tie (D14) and
  only same-transaction netted continuations degrade.

### 6.3 Zero-match guard as loud failure

The guard converts r1's silently-successful no-op UPDATE into an abort
covering: destination row deleted between diff and apply (impossible in
one txn but defensive), destination lineage unresolved by I4 (sibling
gap in §2), and catalog/stream inconsistencies. The guard runs in the
MERGE/PICK transaction, so abort semantics are I7-atomic.

## 7. Compatibility, mixed-version, restore

- Upgrade: a NEW forward tenant-upgrade version NEWER than the
  target branch's final registered version creates the r2 table
  idempotently (`CheckTableDefinition` guard) — D149: 4.0.19 as
  delivered on PR #31629 (upstream #31497 folded and removed the
  unpublished 4.0.8-4.0.17 handlers, upstream #31489 published
  4.0.18 and the 4.0.19 sysview identity refresh that this branch's
  4.0.19 vehicle folds with), the
  number re-derived as predecessor-final-plus-one at landing, all
  existing migrations untouched. New accounts get it from
  predefined DDL. Nothing backfills history; links accrue from
  first enabling binary onward.
- Mixed version: a pre-fix CN never writes links (no writer), and never
  reads them (no consumer); a fixed CN facing a tenant whose table is
  missing (upgrade not yet applied) disables link writes per statement
  batch with a one-time warning and keeps legacy replay. Schema drift can
  only exist on clusters bootstrapped by mid-development binaries; both
  sides degrade safely: the workspace back-fill leaves a batch without
  `new_row_id` untouched, and rows whose identity stayed NULL fold only
  by the commit-timestamp tie (D14) — same-transaction netted
  continuations stay unprovable (I1).
- Downgrade: an older binary ignores the table; replay falls back to
  historical behavior with no data-integrity risk.
- Snapshot / PITR / restore: the link table is skipped on restore
  (`systemCatalogRestoreSkip`) because its keys reference the
  pre-restore incarnation's table ids and engine rowids, which a restore
  invalidates. Post-restore, new moves record new links; stale history
  is absent rather than wrong (I1).
- Restart: links are ordinary catalog rows (durable); r1 live-verified
  survival across restart.

## 8. Performance and capacity model

- Write path: +1 tiny catalog row per moved row only for eligible
  UPDATEs (§11); non-moving pk-touching updates pay one pk-encoding
  compare per row. REPLACE/ODKU/multi-target paths pay nothing.
- Read path (r8, D17): one whole-window range scan per touched table
  generation reads the window's entire link_id prefix range
  (`link_id >= prefix(table_id) and link_id < prefix(table_id+1)`,
  bounded by `write_ts` and a LIMIT of remaining-budget+1), filters to
  the requested keys in Go, and marks the generation fully cached, so
  anchors, continuation hops, and successor probes resolve from memory
  with no further SQL. This replaced per-chunk point/range probes after
  live measurement on a 40,000-link window (2026-10-17, single-CN
  cluster): a 512-term OR of (sha, sha+1) prefix ranges planned for
  78.58 s per query (planning cost grows superlinearly with the range
  count), a 512-sha IN-list took 28.96 s, and even a 2-range OR took
  0.19 s, while the single whole-window scan returns the same 40,000
  rows in 0.315 s — the scan serves every later key of the window at
  zero incremental query cost, which the point probes never could. The
  scan rides the link_id primary key; the writer inserts through raw
  relation writes, so no secondary index exists and none is needed.
  Fail-closed semantics are unchanged: the budget+1 LIMIT keeps
  truncation observable (provider disables, fold degrades to legacy
  replay past 2^20 cached rows), conflicting (row_id, old_pk) identities
  poison the window, and rows cache under full old-pk bytes so sha
  aliases cannot merge; the walk is ≤ 32 hops per anchor. PICK closure
  volume is bounded by frontier caps. Reader memory is
  O(window links + probed keys), bounded by the same cap as before; the
  claimed-key set is O(folded moves).
- Apply path: one extra SELECT count per moved row (moved rows are rare
  in real diffs; the issue-grade workloads are per-row moves).
- Capacity: catalog rows grow with the number of pk moves of live,
  non-dropped tables. §9.3 defines the owner and the reclaim that exists
  today; O1 bounds the residual growth question.

## 9. Security, tenancy, ownership

### 9.1 Isolation

The table is per-account like `mo_increment_columns`: each tenant reads
and writes only its own copy; cluster tables and system databases never
record links (eligibility predicate). Per-account WRITE isolation is
unchanged: capture rides the mover's account, subscription routes never
write links, and no link row ever crosses an account. The one
authorized cross-account READ (I8, D220) is a subscription-resolved
segment's provenance reads executing under the physical owner identity
the routing layer opened that segment's change stream under - read-only
catalog probes keyed by that segment's own physical table id. The leak
surface is old and new primary-key values, row ids and commit instants
of pk-moving updates, a strict subset of what the base subscription
design (B8) already exposes to the same consumer; subscription
revocation closes routing before any probe can run; no rows are copied
into the subscriber's catalog; every non-subscription route keeps its
exact pre-existing single-account behavior.

### 9.2 Privilege surface

The synthetic ctx is hidden from EXPLAIN and from privilege enumeration
(dedicated walker predicates), so the internal write neither leaks into
user-visible plans nor requires user grants. User-visible read access to
the catalog table follows the existing system-table conventions.

### 9.3 Retention and ownership

Owner of a link row: the account that ran the move; keyed by the moved
table's id. Reclaim lifetime equals the protected delta-stream lifetime,
so a link can never be deleted while a live branch diff can still fold
it. `Scope.dropTableSingleResolved` (DROP TABLE, DROP DATABASE via
per-table routing, TRUNCATE via drop+create, DATA BRANCH DELETE) probes
`mo_branch_metadata`: tables with no lineage participation are reclaimed
immediately, and lineage participants defer to the protect-snapshot flow
(`Compile.reclaimBranchProtectSnapshots` and the lifecycle-RC closure),
which releases exactly
`databranchutils.ComputeBranchReclaimTableIDs(dag, deadTIDs)` — the
dropped roots whose subtrees are fully deleted plus the ancestors that
retire with them — and now reclaims those tables' link rows in the same
transaction. A dropped intermediate generation with live descendants
therefore keeps its links (multi-generation folding still resolves),
and dropping the last live descendant reclaims the deleted chain above
it. Compacted ALTER generations are reclaimed with their lineage rows
(both the plain-route and the lifecycle-RC compaction). Each reclaim
issues `DELETE FROM mo_catalog.mo_data_branch_update_link WHERE link_id
>= x'<big-endian table_id>' AND link_id < x'<table_id+1>'` — a half-open
range on the `link_id` primary-key prefix — under the session account,
guarded so a missing table (pre-upgrade tenant) skips silently.
Riding the primary key is mandatory for the same reason as for reads
(§5.1): links enter through a raw relation write that maintains no
secondary index, so any predicate the planner routes through such an
index — a `table_id =` probe or a delete against an accidentally
declared one — silently matches nothing while the rows persist; the
catalog table therefore ships without a secondary index and every
reader/reclaimer keys off `link_id`. Orphans beyond reclaim: reclaim
covers every table-drop path; the documented residuals are
cross-account ancestors (a retired ancestor owned by another account is
reclaimed when that account drops its own lineage, since the delete
rides the session account and table ids are globally unique) and
`SkipDataBranchReclaim` internal paths, which skip link reclaim exactly
as they skip snapshot reclaim; reclaim-window failures are likewise
harmless (see §6.1).

## 10. Validation map

Every invariant maps to the cheapest deterministic evidence:

| Invariant / behavior | Evidence (cheapest) |
| --- | --- |
| I1 identity: commit-tie claims (unique vs duplicate births at the anchor ts), impostor at intermediate key (with/without continuation), impostor's own links never followed, NULL identity folds single-shot but cannot chain netted hops, workspace-placeholder same-txn hops (shared snapshot required), contradictory departure, ambiguous successors, cycle/reincarnation, hop cap, same-key tail (single and chained stable-key tails, tail after a cross hop then linked continuation, tail never follows a foreign departure's successor, plain-delete and separate-txn re-insert tails fail closed, duplicate lineage departures fail closed) | UT of the ts-tie walk over fabricated providers (`TestResolvePkMoveChain`, `TestPkMoveSuccessorOf` in `pkg/frontend/data_branch_pk_move_link_test.go`) |
| I1 live equivalence: MERGE/PICK of genuine single + multi-hop moves cascades ON UPDATE CASCADE; impostor at a vacated key survives as its own insert | live smoke battery + BVT `merge_foreign_key_pk_move`, `pick_foreign_key_pk_move` |
| I2 counterexample: independent DELETE/INSERT (autocommit and explicit `begin; ...; commit;`) keeps DELETE semantics, FK error, atomic state | BVT tx sections + live battery S1/S7 |
| I3 precedence: provenance folds before legacy same-key matching | UT ordering witness + BVT scenario |
| I4 conflict lineage: both-moved ACCEPT/SKIP/FAIL; dst-deleted vs moved at every mode, MERGE and PICK | live matrix + BVT both-moved sections |
| New-key occupant conflicts (NF2): independent destination row at the move's NEW key with referencing children — FAIL aborts, SKIP keeps destination, MERGE-ACCEPT replaces victim inline, PICK-ACCEPT routes victim through the conflict pipeline | live battery NK scenarios; legacy nil-conflict shape covered by existing BVT |
| I5 lineage selection (NF4): reused intermediate keys admit only the selected lineage; unrelated parent and child rows unchanged | pair consumer UT (`TestPairPkMovingTombstones/PICK_dependency-only_anchor_never_folds`) + live reused-key PICK regression |
| I5 windows (NF6/NF7): multi-generation branches fold through ancestor tables; retired intermediate tables still resolve; historical endpoints and PICK BETWEEN read only in-window moves | live battery MG/HW scenarios + probe SQL bound assertions (`TestPkMoveLinkProbeBudgetAndBounds`) |
| I5 staleness: a move after the selected snapshot endpoint never contaminates the historical apply | live battery HW scenario |
| I6 bounds (NF8): probe SQL carries `write_ts <= endPhys` and `LIMIT budget+1`; overflow disables the provider, releases caches, and never caches a truncated prefix; cache over budget disables before SQL; mid-walk truncation fails closed (NF5) | `TestPkMoveLinkProbeBudgetAndBounds` (SQL capture + truncation + disable-before-SQL + injected cap-boundary walk) |
| Writer protocol: NULL-identity staging; link batch layout matches the catalog schema; eligibility/fail-closed paths | `pkg/sql/colexec/multi_update/pk_move_link_test.go` |
| Writer atomicity (I7): injected link-write failure aborts the statement; degraded ctx skips silently; healthy write stages exactly one NULL-identity row | `TestInsertUpdatePkMoveLinkRowsWritePath` |
| Identity back-fill: harvests map pk bytes to the freshest staged row id; account/table/type filters; NULL and malformed entries skipped; null identity column left untouched on schema drift | `pkg/vm/engine/disttae/pk_move_link_fixup_test.go` |
| Pair consumer: proven anchors fold and claim their replacement (durable-identity and NULL-identity links alike, via the commit-timestamp tie); unproven/dependency-only anchors stay deletes; replacement leaves the stream exactly once | `TestPairPkMovingTombstones` |
| I7 rollback/restart: ROLLBACK removes link rows with user rows; links survive a clean restart and keep resolving | live battery RB/RS scenarios |
| I6 tenancy: a second tenant writes/reads only its own links; a tenant without the table degrades to legacy replay | live battery TI scenario |
| I6 reclaim: DROP TABLE / DROP DATABASE / TRUNCATE remove the table's links; a dropped intermediate generation keeps its links while descendants are live and them both retire when the last descendant drops; global audit shows zero orphans | BVT reclaim probes + live counts + `TestComputeBranchReclaimTableIDs` |
| Upgrade path and idempotence (D149): an existing tenant at the target's final registered version (4.0.7 on the landing target) lacking the table upgrades to the new forward-handler version and gains `mo_data_branch_update_link` with the §3 schema; a repeated upgrade run is a no-op; a tenant at the new version selects no further handler; DDL/schema agreement between `predefined.go`, plan TableDef, and writer batch | UT in `pkg/bootstrap/versions/v4_0_19`, `pkg/sql/plan`, plus the selection-witness and idempotence cases in the new forward version's package — the schema-agreement half owns the fresh-cluster/new-account predefined-DDL path (bootstrap at the final version, no handler selection), while the selection-witness, repeated-upgrade, and upgrade-path cases own existing-tenant reachability; post-upgrade MERGE/PICK pk-move cascade parity runs live on an upgraded cluster |
| I7 apply atomicity: RESTRICT rejects loudly; MERGE aborts atomically (parent/child unchanged) | BVT RESTRICT cases (MERGE and PICK) |

The per-round, per-run evidence rows that followed this table in r101 were
transient proof narratives; they are removed from the tracked artifact per
the repository delivery contract and remain reachable in the r101 blob
linked from section 1. The current validation surface of PR #31629 is:

- Focused UTs: `pkg/frontend` (`data_branch_pk_move_link_test.go`,
  `data_branch_link_retention_test.go`, `data_branch_hashdiff_test.go`,
  `data_branch_pick_test.go`, `data_branch_owner_scoped_d220_test.go`,
  fencing/retire suites), `pkg/frontend/databranchutils`
  (`pk_move_fencing*_test.go`, `pk_move_stage_owner*_test.go`,
  `pk_move_owner_coverage_test.go`, `pk_move_retention_lockkey_test.go`),
  `pkg/sql/colexec/multi_update` (`pk_move_link_test.go`,
  `pk_move_link_location_ownership_test.go`, `pk_move_genesis_lock_test.go`),
  `pkg/sql/compile` (`pk_move_*_test.go`, `ddl_drop_lifecycle_test.go`),
  `pkg/sql/plan` (`data_branch_update_link_test.go`),
  `pkg/txn/client` (`operator_expected_conflict_test.go`),
  `pkg/vm/engine` (`change_handle_visible_state_test.go`),
  `pkg/vm/engine/disttae` (orphan-GC and catalog visibility tests),
  `pkg/bootstrap` (v4_0_19 upgrade tests), and the service-level cells
  under `pkg/tests/service` (`data_branch_fencing_cells_test.go`,
  `data_branch_retire_cells_test.go`).
- BVT (mo-tester, `test/distributed/cases/git4data/branch/`):
  `merge/merge_foreign_key_pk_move`, `merge/merge_link_retention_fail_atomic`,
  `merge/merge_link_retention_linkfree`, `pick/pick_foreign_key_pk_move`;
  subscription routing: `publication_subscription/pub_sub_data_branch`
  (including the B13 owner-resolution case); plus the regenerated
  information-schema / show / privilege result sections the new catalog
  rows touch.
- Live batteries against a launched service: the MERGE and PICK pk-move
  matrices (single-hop and multi-hop moves, both-moved ACCEPT/SKIP/FAIL,
  destination-deleted at every mode), the RB/RS rollback-and-restart
  scenarios, the TI tenancy scenario, and the RED→GREEN subscription
  owner-resolution battery whose unanchored-probe RED (PICK abort 20101)
  motivated D222.

Each CI run of the PR re-executes the UT and BVT surfaces above; per-run
logs live on the PR checks page, not in this document.

## 11. Decision log and review record

The 222 numbered design decisions, in revision order. Each entry keeps its
revision tag and refinement/supersession/amendment stamps, so the chain
from an early decision to its current form is traceable without the
per-round narratives removed in r102; the full text behind any entry is
reachable in the r101 blob linked from section 1.

- D1 (superseded by D8): consumer-side occupancy validation (A4) over stored successor ids.
- D2 (amended by D15): `write_ts` = move-txn *create* ts.
- D3: catalog PK keyed by (table_id, old_pk_sha, old_row_id) with full old/new pk bytes stored and always equality-probed.
- D4: link-eligibility stays single-target root UPDATE on plain non-partitioned, non-temporary, non-cluster user tables.
- D5: restore skips the table.
- D6 (superseded by D8): occupancy-order chain walk of r2.
- D7: zero-match guard = `SELECT count(*) = 1` in-txn before the key-moving UPDATE (not affected-rows plumbing), keeping the apply path's SQL-only shape and file-output mode unchanged.
- D8 (superseded by D14): durable row identity (r3, NF1).
- D9: new-key conflicts resolve per mode before consumption (r3, NF2).
- D10: PICK selection is by proven lineage (r3, NF4).
- D11 [refined by D91+D100]: provenance windows cover every physical table on both branch paths with per-window `write_ts <= endPhys` bounds (r3, NF6/NF7), and every budget executes as a SQL `LIMIT` at read time with truncation observable (r3, NF8).
- D12: reclaim absorbs protected consumers via reclaim failure tolerance (§6.1/§9.3): a reclaim failure leaves orphans keyed by a dead table id that no reader can reach, logged, never fatal.
- D13: link lifetime equals protected-stream lifetime (r3, NF6).
- D14: commit-timestamp tie (r4).
- D15 (r5) [refined by D85]: cross-transaction evidence must be *preserved*, not just consulted (§5.4).
- D16 (r7): a lineage version's link-less departure is a same-key continuation, not a lineage end (§5.2 SAME-KEY TAIL).
- D17 (r8): link probes scan each touched window's whole link_id range once and filter in Go.
- D18 (r9, review F1) [refined by D87+D94]: a same-transaction hop consults the recorded workspace successor set (unique, complete, same `write_ts`) BEFORE any commit-timestamp terminal claim.
- D19 (r9, review F2): fold emission order is keyed on chain-final keys with vacate-first direction.
- D20 (r9, review F3): a cyclic fold plan is detected before materialization and excluded, replaying through the legacy DELETE+INSERT path.
- D21 (r9, review F4): dead-tail pruning is a fixpoint over the whole chain.
- D22 (r9, review F5): pruning runs once per commit, under the transaction lock, BEFORE `mergeTxnWorkspaceLocked` and `dumpBatchLocked`.
- D23 (r9, review F7): the shared evidence budget is reserved atomically per admitted entry under the provider mutex.
- D24 (r9, review F8) [refined by D41]: whole-window probes (D17) carry the `[startPhys, endPhys]` write-ts bounds in the scan predicate plus a per-table window signature.
- D25 (r10, review F1): key-moving emission order and apply ranks are computed over the EFFECTIVE destination-lineage WHERE key of each fold (`pickKeyMovingWhereRow`), i.e. the predicate the apply will actually run after conflict resolution retargets rows — never over the raw source-side `old` key.
- D26 (r10, review F2) [refined by D44]: transaction-local row identities get a durable transaction namespace.
- D27 (r10, review F3): provenance is preserved BEFORE every statement-time workspace rewrite, not repaired at commit.
- D28 (r10, review F4) [refined by D87+D94]: genuine UPDATE identity is carried through stable-key workspace replacements by CONTINUITY MARKERS, while independent DELETE+INSERT semantics are untouched.
- D29 (r10, review F5): the S3 producer route is proven end-to-end by a controllable repository-owned fixture at bounded scale.
- D30 (r10, review F1's self-found sibling, the compile gate): the r9-era `compileMultiUpdate` S3 route/fault-gate was DEAD CODE.
- D31 (r10, review F6): final-tree verification evidence is produced from the complete worktree (tracked + untracked files) with non-empty test selection and recorded build configuration/revision: the review-named disttae tests and the r10 additions run verbosely against the final tree, the owning packages pass whole, `-race` passes on the parallel subset, and `go vet`/`gofmt -l` are clean.
- D32 (r11, review F1): replacement liveness is tracked across the deletion of UNCOMMITTED S3-route rows.
- D33 (r11, review F2) [refined by D92]: continuity retargeting is statement- reversible.
- D34 (r11, review F3): the harvest cache is invalidated EXPLICITLY on statement rollback.
- D35 (r11, review F4) [refined by D88]: durable origins and heal bindings ride the workspace re-key even when NO link row exists yet.
- D36 (r11, review F5): self-dependencies no longer count in effective-key apply ranking.
- D37 (r11, defect found while answering): `fillPkMoveLinkNewRowIDsLocked` pass 1 (durable-origin rewrite of `old_row_id`) ran BEFORE pass 2 (continuity-marker retarget), so it rewrote the marker rows' own `old_row_id` cells and destroyed the pre-image pass 2 matches on.
- D38 (r11, evidence integrity): every server-dependent evidence step gates on PORT-OWNED.
- D39 (r12, review F1): an unresolved NULL-identity intermediate is departed BY KEY inside the recording transaction's write_ts namespace, mirroring the commit-time prune's own continuer grouping (D32 keeps a NULL link alive exactly while an alive same-transaction departure of its new key exists), never by the bare commit-timestamp tie.
- D40 (r12, review F2): replacement liveness is a pk-level question whenever the recorded identity can change hands.
- D41 (r12, review F3): transaction identity is no longer conflated with commit-window membership.
- D42 (r12, review F4): one synchronization contract covers every provenance-map accessor: the caller MUST hold `txn.Lock`.
- D43 (r13, review F1): the NULL-identity by-key hop screens the landed key's tombstones BEFORE adopting a same-transaction departure.
- D44 (r13, review F2; amends D26): the durable transaction id is the transaction namespace.
- D45 (r13, review F3): statement undo bookkeeping is constant-time per record.
- D46 (r13, review F4): the ts-tie claim at a chain end tolerates a post-endpoint committed successor departure when that departure binds no in-window tombstone.
- D47 (r13, review F5; refines D13) [refined by D83+D86]: row-less parent link reclaim is additionally gated on snapshot coverage.
- D48 (r13, review F6): boundary tests drive the production locking boundary, not its interior.
- D49 (r14, review F1): S3-route same-key edits record which durable lineage the key descends from, and a later move of that row heals its recorded old identity through it.
- D50 (r14, review F2): UpdateWriteS3 producers COLLECT provenance and ferry it through the S3 tail output.
- D51 (r14, review F3): provenance verdicts are tri-state, and an unresolved verdict aborts the apply instead of degrading to legacy DELETE+INSERT replay.
- D52 (r14, review F4): provenance bookkeeping failures travel the ordinary statement-error route.
- D53 (r14, review F5; refines D45): per-marker continuity work indexes its staged-link scans instead of re-scanning the staged set.
- D54 (r14, review F6): the conflict-side apply ranker schedules ready folds through a min-heap seeded at in-degree zero and updated only on vacated-key unlocks, against in-window effective destination WHERE keys.
- D55 (r14, review F7): the consumer's transaction-identity collision handling carries its own deterministic proof.
- D56 (r15, review F1): capped evidence must reach the abort of D51, not quietly bypass it.
- D57 (r15, review F2): fold plans with a genuine key-rotation cycle cannot be executed without semantics loss by either replay strategy, so plan assembly refuses them: dependency-order draining (Kahn, seeded by folds whose landing key holds no later-moving fold) leaves a residue exactly when a cycle exists, and that residue aborts the apply statement-atomically with an actionable rework hint instead of falling through to same-key matching.
- D58 (r15, review F3): workspace compaction remap failures travel the same error channel as every other provenance bookkeeping failure (D52).
- D59 (r15, review F4): provenance candidates are relevance-filtered before the D51 unresolved-verdict abort, because a walk that cannot resolve is only an apply hazard when its lineage can reach the destination.
- D60 (r15, review F5): S3-route provenance retention is bounded by an explicit byte budget with a spill lifecycle, not by the statement's row count.
- D61 (r16, review F1): the per-transaction death ledger keeps SETS of statement ids, so repeated same-key workspace edits and mid-transaction statement rollback preserve lineage continuity instead of overwriting it.
- D62 (r16, review F2): link-relation resolution downgrades only the explicitly supported missing-table condition.
- D63 (r16, review F3) [refined by D69]: coordinator-side provenance retention is admission-bounded inside the committing workspace, the layer D60's producer budget cannot reach.
- D64 (r16, review F4) [refined by D73]: every temporary provenance spill object has exactly one cleanup owner.
- D65 (r16, review F5) [refined by D67+D68]: statement-boundary workspace dumps keep every link owner table's staged inserts in memory, so the workspace identities recorded on a link survive for the WHOLE transaction and same-transaction chaining cannot sever.
- D66 (r16, review F6): fold planning excludes self-dependencies the way the apply ranker already does, so a return to the original key is plan shape, never a key-rotation cycle.
- D67 (r17, review F1, extends D35) [refined by D109]: a durable row's origin stays pinned across a statement-boundary workspace spill.
- D68 (r17, review F2, refines D65): the keep decision is per batch by row-id membership, never per table.
- D69 (r17, review F3, refines D63): consumed marker storage is released before provenance admission.
- D70 (r17, review F4): a PROVEN same-transaction return to the original key resolves.
- D71 (r17, review F5): proven TERMINAL deletion is a defined verdict, not incomplete provenance.
- D72 (r17, review F6): terminal spill cleanup is cancellation-safe and failure-atomic.
- D73 (r17, review F7, refines D64): durable cleanup ownership is established BEFORE the object can persist.
- D74 (r18, review F1, extends D35/D67) [refined by D82]: a by-key lineage binding dies through the PRODUCTION deletion boundary, not only through staged DELETE entries.
- D75 (r18, review F2, refines D29/D30) [refined by D81]: workspace-to-S3 continuity is REPRESENTED EXPLICITLY, and replacement harvesting excludes rows the statement has already logically deleted.
- D76 (r18, review F3, refines D64/D73) [refined by D80]: spill-object cleanup ownership TRANSFERS to the consumer on successful ferry publication.
- D77 (r18, review F4, refines D63/D69): the coordinator retention budget counts ALL retained provenance bookkeeping, not only retained link batches.
- D78 (r19, review F1, refines D30/D44) [refined by D111]: an edit-first lineage switching to the S3 route composes the lineage's already-recorded durable origin instead of binding the workspace pre-image literally.
- D79 (r19, review F2, refines D67/D77) [refined by D84+D95]: workspace dumps keep exactly the rows staged provenance references, and post-fill bookkeeping growth is budget-checked at the write boundary.
- D80 (r19, review F3, refines D76): spill ownership state is execution-scoped and protection leases re-mint after expiry.
- D81 (r19, review F4, refines D75): the workspace-to-S3 NULL retarget rides the same continuity index as the resolved-marker path.
- D82 (r20, review F1, refines D74): raw workspace deaths retire lineage bindings through the shared deletion boundary.
- D83 (r20, review F2, refines D47) [refined by D98+D153]: direct unbranched DROP honors snapshot grounding exactly like the orphan-parent flow, resolved from caller-known names.
- D84 (r20, review F3, refines D79) [refined by D90+D95]: pin-retained workspace bytes are charged to the coordinator budget at the dump gate.
- D85 (r20, review F4, refines D15): stream evidence builds one version index per key instead of rescanning per tombstone.
- D86 (r20, found while answering r20 F2 and the BVT authoring; not a review finding) [refined by D98+D153]: three boundary facts r20 established.
- D87 (r21; refines D18/D28) [REJECTED — no behavior change ; refined by D156]: the r20 review asked to persist explicit continuity so a same-key delete-then-insert in one transaction stays distinguishable from a same-key UPDATE across commits; verified at its cited loci, then rejected because (a) a committed same-transaction same-key delete+insert emits exactly the change-stream signature that the upstream-merged #27996 series defines as native UPDATE replay, and (b) emission order is DML-order dependent, so no ordering survives without new durable identity.
- D88 (r21, review F1, refines D35) [refined by D93]: provenance remaps across the workspace shrink boundary.
- D89 (r21, review F3) [refined by D96+D97]: restored moving rows carry their old-key sidecars through emit and apply.
- D90 (r21, review F4, refines D84): pin charges derive at admission instead of a cached field.
- D91 (r21, review F5, refines D11): the whole-diff anchor probe is bounded and link-aware.
- D92 (r21, review F6, refines D33): statement rollback restores staged-link cells through one lazy index.
- D93 (r22, review F1, refines D88): the shrink remap builds the new origins from an immutable snapshot.
- D94 (r22, review F2) [REJECTED — no behavior change]: the birth-equals-deletion-ts continuity tie stays.
- D95 (r22, review F3, refines D79/D84) [refined by D99]: the fill-end budget check gates on participation.
- D96 (r22, review F4, refines D89) [refined by D97]: restored rows route through the landed-key occupant arbiter before emission.
- D97 (r23, review F1, refines D89/D96) [refined by D101]: restored inserts drain after the surviving moves of the same diff.
- D98 (r23, review F2, refines D47/D83+D86) [refined by D102+D153]: snapshot-grounded reclaim extends to branch DAG members.
- D99 (r23, review F3, refines D95) [refined by D103+D112]: participation gating extends through the delete and dump budget boundaries.
- D100 (r23, review F4, refines D11's overflow discipline) [refined by D107]: window overflow narrows to per-key probing instead of disabling every lookup.
- D101 (r24, review F1, refines D97's conflict preservation) [refined by D104]: SKIP decisions close over folded-move dependencies.
- D102 (r24, review F2, refines D98): snapshot coverage matches the table's physical identity.
- D103 (r24, review F3, refines D99) [refined by D105+D106+D137]: continuity/origin state rides the statement-boundary dump instead of pinning user batches.
- D104 (r25, review F1, refines D101): PICK's consumer-side SKIP decisions close over move dependencies too.
- D105 (r25, review F2, refines D103) [refined by D108+D111]: provenance rides deletion-driven object rewrites, not only statement-boundary dumps.
- D106 (r25, review F3, refines D103) [refined by D110]: update links stay aligned with the RC tombstone transfer, atomically.
- D107 (r25, review F4, refines D100): PICK closure expansion completes for the selected frontier across unrelated-history overflow.
- D108 (r26, review F1, refines D105) [refined by D115+D118]: delete-compaction workers can no longer swallow an abort.
- D109 (r26, review F2, refines D67's edit-first origin discipline): the provenance pk vector resolves independently of duplicate-check eligibility.
- D110 (r26, review F3, refines D106) [refined by D113+D139+D142]: provenance rides the PERSISTED tombstone transfer route too.
- D111 (r26, review F4, refines D78 and D105): the D78 by-key lineage BINDING's durable-origin hop rides the same identity migration as every other provenance cell.
- D112 (r26, review F5, refines D99 and D103) [refined by D119]: ordinary non-key updates now hold provenance continuity memory BOUNDED to one entry per live lineage generation.
- D113 (r26, F3 residual found by the era-20 live gates, refines D110) [refined by D114+D116+D117]: the transfer must REPLACE, not twin.
- D114 (r27, review F1, refines D113): every persisted-DELETE scan honors the nil-slot lifecycle.
- D115 (r27, review F2, refines D108) [refined by D120+D121+D123]: a delete-compaction wave is failure-atomic across statements.
- D116 (r27, review F3, refines D113): partial retirement detaches before it records.
- D117 (r27, review F4, refines D113): TransferFlow.Close owns the leftover batch across every error path.
- D118 (r27, review F5, refines D108) [refined by D122]: persisted replacement objects have a cleanup owner until workspace publication.
- D119 (r27, review F6, refines D112) [refined by D120]: finished-continuity sweeps cap lineage history at the live binding per key.
- D120 (r28, review F1, refines D115 and D119) [refined by D123+D124+D127]: the statement boundary is established before any history retirement, so a boundary failure can roll back only the failed boundary itself.
- D121 (r28, review F2, refines D115): clean sibling objects join the same atomic publication wave as dirty replacements.
- D122 (r28, review F3, refines D118) [refined by D125]: replacement-file ownership survives cancelled and failed cleanup.
- D123 (r29, review F1, refines D120 and D115) [refined by D126+D141]: the statement boundary treats the completed compaction wave's destructive tail.
- D124 (r29, review F2, refines D120): the diverted-deletion ledger records statement ownership and survives a failed boundary.
- D125 (r29, review F3, refines D122) [refined by D128+D138]: unpublished replacement files keep a cleanup owner when the failed wave's own reclamation also fails.
- D126 (r30, review F1, refines D123) [refined by D129]: original-entry release at adoption is by batch IDENTITY, not by a workspace position captured at wave time.
- D127 (r30, review F2, refines D120) [refined by D129]: the rollback cut widens only after EVERY fallible RC step succeeds.
- D128 (r30, review F3, refines D125) [refined by D130+D131]: close-time reclamation whose submission or delete fails keeps its cleanup owner.
- D129 (r31, review F1, refines D126+D127) [refined by D137+D138]: the whole boundary maintenance transition.
- D130 (r31, review F2, refines D128) [refined by D132+D133]: the durable orphan registry bounds retention and retry work.
- D131 (r31, review F3, refines D128) [refined by D134]: cleanup ownership survives Engine.Close.
- D132 (r32, review F1, refines D130): an overflow admission pins its registry entry across storage I/O.
- D133 (r32, review F2, refines D130) [refined by D135+D136+D140]: cleanup ownership is reserved BEFORE production, so sustained deletion failure cannot grow retained state past a hard bound.
- D134 (r32, review F3, refines D131): the terminal close pass is a single bounded flight over a finite snapshot, coordinated with the storage's active sweep.
- D135 (r33, review F1, refines D133): every reservation transferred into registerDurableOrphanGC settles exactly once, whether direct reclamation succeeds or retention adopts the name.
- D136 (r33, review F2, refines D133): a rollback resurrection releases the reservations of the retired files it returns to publishing workspace ownership.
- D137 (r34, review F1, refines D129+D103) [refined by D139]: the boundary dump's provenance translation is owned and restored by the maintenance window itself, independently of any dirty-object wave.
- D138 (r34, review F2, refines D129+D125) [refined by D140]: the dump path holds cleanup ownership of its persisted output from a successful Sync until the workspace registration takes it.
- D139 (r35, review F1, refines D137+D110): dump-rollback restoration composes with completed in-window transfer rekeys instead of replacing them.
- D140 (r35, review F2, refines D138+D133): bounded producer admission moves to the persistence choke point.
- D141 (r36, review F1, refines D123): rollback ownership of the compaction wave's provenance re-key moves from a wholesale staged-snapshot restore to an exact inverse application against the current maps, so the discard composes with D139 instead of overwriting it.
- D142 (r37, review F1, refines D110+D113) [refined by D143+D144]: retained in-memory rekeys and discarded object-result rekeys get different rollback owners, and the object-route carry gets one at last.
- D143 (r38, review F1, refines D142) [refined by D145]: the rollback owner for carried provenance leaves the maintenance window for a transaction-level, statement-tagged chronological carry journal, because the window scope has a reachable hole.
- D144 (r38, review F2, refines D142) [refined by D146+D150+D151]: journal retention becomes effective-only, coalesced, budget-charged, and admitted before mutation.
- D145 (r39, review F1, refines D143+D113) [refined by D147]: adoption becomes a recorded eligibility transition applied uniformly to both roll-forward ledgers, because the seq arithmetic r38 assumed does not exist.
- D146 (r39, review F2, refines D144) [refined by D148]: admission charges the COMPLETE retained growth of the carry plus its potential inverse, because the staged mutation is not free.
- D147 (r40, review F1, refines D145): settlement must never move a record OUT of the collection the close drain owns.
- D148 (r40, review F2, refines D146): rollback reclamation is CHOSEN.
- D149 (r41, review F1, refines §7's upgrade placement): the link-table catalog migration moves to a FORWARD tenant-upgrade version NEWER than the target branch's final registered version, because upgrade selection is strictly greater-than.
- D150 (r43, review F2, refines D144 part (2), invariant (ii), and planned test (c)): the carry journal preserves every round's undo until that round completes.
- D151 (r43, review F1, refines D144 as clause (v)): EVERY carry admission boundary — boundary-dump origin translation (pk_move_link_fixup.go:2855) and object-transfer re-key (transfer_util.go:475) alike — applies the shared participation rule: with no live staged link rows, the pair-list integrity checks and effective-pair computation still run and the journal append still happens, but the budget refusal is skipped, because a transaction without staged links commits no provenance.
- D152 (r45, implementation review P1, refines D47/D83/D86+D98) [refined by D153+D155]: the historical-retention gate that every link-reclaim probe consults covers ACTIVE PITR scopes exactly like user snapshots, through ONE shared check.
- D153 (r46, design review r45 P2, refines D152/D98/D83/D86/D47) [refined by D154+D159]: links retained under the D152 union for a table that then disappears gain a terminal lifecycle.
- D154 (r47, design review r46 findings P1 + four P2, refines D153): the retained-dead-table lifecycle gets its ownership, discovery, exceptional-path, and restore contracts corrected.
- D155 (r48, design review r47 findings P1 + two P2, refines D152 and D154(C)/(D)/(E)): the retained-dead-table lifecycle closes the sys-owned owner topology, hardens the owner-removal error contract, and splits the restore contract.
- D156 (r50, design answer to r60 review F1 (P1), refines D87's unrecorded-replacement boundary, executes D2/D15's pre-authorized commit-exact direction without a catalog shape change): failed-scope attribution is derived from the link catalog's OWN committed change stream, not from approximations over the data stream.
- D157 (r50, design answer to r60 review F2 (P2), refines the evidence-construction cost contract accompanying D156) [refined by D158]: arrival-build evidence construction is one pass per provider generation, and every evidence loop observes cancellation.
- D158 (r54, round-4 review F1 (P1), refines D102) [refined by D159+D160]: one member, two identities - the coverage contract matches the OWNER's logical identity, never the link's physical address.
- D159 (r55, round-5 review F1 (P2), refines D153/D158(B)) [refined by D161+D168+D169]: the genesis-admission lifecycle of the retention ledger.
- D160 (r55, round-5 review F2 (P2), refines D158(B)/(C)) [refined by D162]: the retention ledger's storage format and compatibility boundary.
- D161 (r56, round-6 review F1+F2 (P2), refines D159(A)/(B)/ (E)/(F)) [refined by D163+D164]: ledger lifetime and registration-state correctness.
- D162 (r56, round-6 review F3 (P2), refines D160(B)/(E)/(F)): capability derivation WITHOUT a persistent cache - the r55 'cached per process' phrase is deleted outright.
- D163 (r57, round-7 review F1 (P2), refines D159(B)/(F) and D161(A.1)/(B)) [refined by D165]: re-registration after a successful prune cancellation - cancellation and subsequent admission are transitions of ONE per-(transaction, address) registration state.
- D164 (r57, round-7 review F2 (P2), refines D161(A.1)) [refined by D166]: registration cancellation survives workspace spills.
- D165 (r58, round-8 review F1 (P2), refines D159(B) and D163(A)/(C)/(D)) [refined by D167+D168+D174+D175]: the committed-row probe-hit outcome is a first-class registration state, defined separately from an owned staged registration.
- D166 (r58, round-8 review F2 (P2), refines D164(A)/(D)/(F) and D159(E)) [refined by D169]: every entry admitted to the bounded-retention lane is transaction-owned, so the admitted set is bounded exactly by the D164(D) domination argument.
- D167 (r59, round-9 review F1 (P2), refines D165(C) and D159(E)): same-transaction funnel retirement is a JOURNALED state transition of the registration state, not an invisible vanishing.
- D168 (r60, round-10 review F1 (P2), refines D165(A)/(B)/ (C)/(E) and the D159(E) paired-delete callers) [refined by D170+D171+D174]: registration use and retirement share one serialization protocol, so the link/ledger identity invariant holds for every successful interleaving of a registration-using transaction and a retiring transaction.
- D169 (r60, round-10 review F2 (P2), refines D159(E), D166(A), and the D163(A)/D165(A) transition sets): a KEEP/enrichment refresh of a present ledger row whose address is TRACKED in the refreshing transaction PRESERVES ownership of the registration instead of replacing it with an unowned funnel batch.
- D170 (r61, round-11 review F1 (P2), refines D168(B)/(C)/ (D) and D165(B)) [refined by D172]: the post-lock visibility that the D168(B) critical section requires is delivered by an explicit snapshot mechanism, never by the lock op's conditional advance.
- D171 (r61, round-11 review F2 (P2), refines D168(D)): the D168 protocol's conflict outcomes are modeled by the repository's OWN failure taxonomy - successful waits, statement-retriable RC snapshot retries, and transaction-fatal lock failures - preserving the repository's existing rollback policy exactly.
- D172 (r62, round-12 review F1 (P2), refines D170(B)(2)/ (B)(3)/(E.2)/(H)) [refined by D173]: an EXPLICIT clock-fence snapshot advance under the held exclusive ledger lock, executed by the retiring code between the lock grant and the re-probe, replacing D170(B)(3)'s retracted self-refresh premise.
- D173 (r63, round-13 review F1+F2 (P2), refines D172(B)/ (G)/(H)) [refined by D174]: the advance's fence is the CAUSAL COMMIT FRONTIER surfaced by the exclusive ledger-key grant itself (result.Timestamp) - never a local clock sample - and the install contract states the waiter's actual exclusive-next semantics.
- D174 (r64, round-14 review F1+F2+F3 (P2), refines D173(C)/(H), D165(B)/(C), D168(E)/(F)/(H), D170(B)(2)/ (E.2), D172(H.1)) [refined by D175]: the retirement's committed frontier is made valid ACROSS BINDING GENERATIONS by a committed fencing transaction with generation-checked acceptance, proxy-enabled deployments are closed by writer-fair admission of the protocol's only shared ledger lock (the shared acquisition retired by D199 at r85), and writer-commi...
- D175 (r65, round-15 review F1+F2+F3 (P2), refines D174(B.1)/(B.2)/(D)/(H.1)/(H.5) and retracts the D165(C) r64 exception) [refined by D176]: frontier acquisition under caller-held ledger locks, the withheld-logtail fencing schedule, and the proxy cell's origin topology.
- D176 (r66, round-16 review F1+F2 (P2), refines D175(B.1)/(B.2)/(D)/(E.10)/(G)/(H.1)/(H.2)) [refined by D177]: the fencing write and the rejection/restart boundary.
- D177 (r67, round-17 review F1+F2 (P2), refines D176(B.1)/(C.8)/(D)/(H.7)) [refined by D178]: fencing-round ownership, sweep eligibility, and helper-commit outcome separation.
- D178 (r68, round-18 review F1+F2+F3 (P2), refines D175(B.1), D176(B.1)/(D)/(F), D177(B.1)/(B.3)/(C.10)/(D)/(G)/(H.8)/(H.9)) [refined by D179]: scratch identity moves OUT of the catalog id space, round lifetime becomes enforceable, and residue recovery observes durable state instead of trusting a resolver status that does not exist.
- D179 (r69, round-19 review F1+F2 (P2), refines D178(B.2) enforcement and D178(H.10)(a); D165(C) remains absolute) [refined by D180]: the deadline owner moves off the blocked thread onto a driver-owned asynchronous execution lane, and the RED arm becomes a schedule ordinary DML locking actually permits.
- D180 (r70, round-20 review F1+F2+F3 (P2), refines D179(B.1) and D179(H.10); D165(C) remains absolute) [refined by D181]: lane resources become a globally budgeted pool whose owner survives statement exit, the pool's drain is placed inside the CN close order, and the expiry proof blocks the awaited helper itself.
- D181 (r71, round-21 review F1+F2+F3 (P2), refines D180(B.2) and D180(H.10)(e); D165(C) remains absolute) [refined by D182]: the fencing drain stops joining lanes on a phase snapshot, the detachment boundary is rebuilt on a COMPLETE enumeration of the helper tail with content-guarded residue, and a named driver-owned FINAL DRAIN BARRIER after lockService.Close certifies actual exit and zero accounting before CN.Close completes.
- D182 (r72, round-22 review F1+F2+F3 (P2), refines D181(B.1)(v), D181(B.2), D181(B.3), and D181(H.10)(e)/(h); D165(C) remains absolute) [refined by D183]: the fencing drain keeps every r71 mechanic, but its safety is re-derived from the REAL write-bearing protocol - a per-stage micro-shape replaces the empty-workspace guard, the residue contract is re-enumerated per finalization class over the actual workspace, timeout ownership follows the platform's own fail-stop dr...
- D183 (r73, round-23 review F1+F2+F3+F4 (P2), refines D182(B.1)(ii), D182(B.2), D182(B.3), and D182(H.10)(h.2)/(h.3); D165(C) remains absolute) [refined by D184]: the fencing drain keeps every r72 mechanic, but the cleanup certificate becomes a LIFECYCLE (gate check PLUS prepared-state re-check), the finalization class set gains the known-rejection class C-KR, barrier certification is narrowed to lane exit with continuation bodies argued separately, and the validation order e...
- D184 (r74, round-24 review F1+F2 (P2), refines D183(B.1), D183(B.4), D183(F)/(G), and D183(H.10)(j)/(k)/(h.2); D165(C) remains absolute) [refined by D185]: the fencing drain keeps every r73 mechanic, but the cleanup certificate's enforcement moves from an un-interceptable in-flight prepared-state re-check to prevention-time certification on an exported predicate plus driver-exact shape floors, and the persistence validation schedule is rebuilt so every arm mints REAL o...
- D185 (r75): CERTIFICATION-PREDICATE AND PERSISTENCE-REACHABILITY REPAIR (round-25 review F1/F2; supersedes D184(B.1)(i)'s mechanism and restructures D184(B.2)/(H.10)(j)/(k); every other D184 element stands, noted in place).
- D186 (r76): LANE-TERMINAL PUBLICATION, T_f1 OWNERSHIP AMALGAMATION, GENERATION-SCOPED POOL, SUBSTRATE ALLOCATOR LIFETIME, AND PARTIAL-REAP ROLLBACK PROOF (round-26 review F1+F2+F3+F4+F5 (P2); refines D185(B.1)(iv) and the D181/D182/D183/D184/D185 shutdown-safety family, refines D180(B.1)/(B.2) and the D176(D)/D179 per-stage lane-charge model for the T_f1 pair ONLY, respecifies D155-C's injection mechanism ONLY; every other element stands, noted in place; D165(C) remains absolute).
- D187 (r77): PER-ROUND OWNER TOKEN AND PRE-INVOCATION TERMINAL-OWNERSHIP SELECTION FOR THE T_f1 OWNER (round-27 design review F1+F2 (P2, both against D186(B.2)'s T_f1 ownership mechanics); refines D186(B.2)(i)/(ii)/(iii) and re-proves (C.23) ONLY; D186(B.1)/(B.3)/(B.4)/(B.5) and every earlier decision stand, noted in place; D165(C) remains absolute).
- D188 (r78): OBSERVED-CLOSE COMPLETION BOUNDARY FOR THE T_f1 OWNER (round-28 design review F1 (P2, against D187(B.2)(iii)/(iv) and (C.23')); refines D187(B.2)(ii)/(iii)/(iv) and re-proves (C.23') ONLY; D187(B.1) token mechanics, (C.27)/(C.28), and every earlier decision stand, noted in place; D165(C) remains absolute).
- D189 (r79): SHARED STAGE-OPERATOR OWNERSHIP AND THE RETAINED-OWNER DRAIN CONTRACT (round-29 design review F1 (P2, against D186(B.2)'s T_f1-only accounting scope as retained by D188) and F2 (P2, against D188(B.1)(iii)/(C.29)'s retention bound); extends D186(B.2)'s amalgamation scope and refines D188(B.1)(iii)/(C.29) ONLY; every D187/D188 token, selection, boundary and exactly-once decision stands, noted in place; D165(C) remains absolute).
- D190 (r80): CANCELLATION-REACHABLE SHUTDOWN ORDER AND THE TWO-PHASE RETENTION MODEL (round-30 design review F1 (P2, against D189(B.2)(iv)'s order authority) and F2 (P2, against D189(H.10)(u)/(C.29')(b)'s token-held geometry); amends the stopRPCs segment order and re-seats the retention/completion claims ONLY; D186-D189 ownership, accounting, registry and exactly-once decisions stand, noted in place).
- D191 (r81): T_F2 RETENTION/SATURATION CELL PHASE RE-SEAT (round-31 design review F1 (P2, against the carried (H.10)(t) schedule that D190(H) declared unchanged)).
- D192 (r82): WATERMARK-PROBE DISPOSAL OFF THE STATEMENT THREAD (round-32 pre-PR review F1 (P2), refines D189(B.1)(i)'s probe premise; the D186/D187/D188/D189/ D190/D191 ownership, accounting, registry, saturation and shutdown-order decisions stand verbatim).
- D193 (r82): POST-GRANT REGISTRATION REVALIDATION FOR THE LOCKING PROBE (round-32 pre-PR review F2 (P2), delivers D168(D)'s grant-side re-read for the production call shape; refines D170(C)'s EXTERNAL-side claim for the r81 admission shape; the D168(B) protocol, the D170(B)/D172/D173/D174 retirement-side mechanics, the D171 taxonomy and D186(B.4)(a)'s OWNED exclusive anchor stand).
- D194 (r83): FIXED-SNAPSHOT ADMISSION GATE FOR THE NON-RC VERDICT (round-33 pre-PR design review F1 (P2), supersedes D193(B.4)'s falsified premise for the uncontended-grant schedule; D193(B.1)'s RC frontier mechanism, the (B.2) matrix, (B.3)'s memo consumption and every D168/D170-D174 decision stand verbatim).
- D195 (r83): ONE ABSOLUTE FOLD DEADLINE WITH FULL CONTEXT PROPAGATION (round-33 pre-PR design review F2 (P2), supersedes D192(B.3)'s bounded-termination claim for the pre-loop and in-probe acquisition waits; D192(B.1)/(B.2)/(B.4)/(B.5)'s disposal split, accounting and owner-pool bound stand verbatim).
- D196 (r83): PROMOTION REFUSAL PROPAGATION AND FRESH-TRANSACTION RECOVERY (round-33 pre-PR design review F3 (P2), correcting D193(B.2)(ii)'s retry sentence and re-seating its C.3 oracle; the (B.2)(i)/ (iii)/(iv) resolutions, the quiescent-window promotion itself and D168(D)'s OWNED re-registration contract stand verbatim).
- D197 (r84): THE PRODUCER VERDICT ANCHOR CARRIES NO ADMISSION RESULT ON EITHER ISOLATION SHAPE (round-34 pre-PR design review F1 (P2), supersedes D194(B.1)/(B.3) for the producer path; every retirement-side admission consumer and the RC API contract stand verbatim).
- D198 (r84): COMMIT-ADJUDICATED VERDICT SEAL - CROSS-GENERATION COVERAGE WITHOUT LOCK-DOMAIN WATERMARKS (round-34 pre-PR design review F2 (P2), supersedes D194(B.2)'s coverage premise; D168-D174 retirement-side mechanics, D195 and D196 stand verbatim).
- D199 (r85): ONE ACQUISITION, ONE MODE - THE HIT-LANE SHARED ANCHOR IS RETIRED AND THE SEAL'S ORDINARY EXCLUSIVE IS THE PRODUCER'S ONLY LEDGER-KEY ACQUISITION (round-35 pre-PR design review F1 (P2), supersedes D198(B.5)'s belt sentence and D197(B.1)'s preserved-shape assertion for the anchor; the OWNED lane's D186(B.4)(a) first-touch exclusive stands verbatim).
- D200 (r85): THE SEAL'S TWO-CAUSE COMMITTED-CONFLICT MODEL - SEAL-VERSUS-SEAL ABORTS ARE BOUNDED BENIGN SERIALIZATION ABORTS UNDER ONE RECOVERY CONTRACT (round-35 pre-PR design review F2 (P2), supersedes D198(B.2)'s no-false-positive sentence, D198(B.6)'s absolute availability sentence and the byte-identity wording of D198(C.7); D198(B.1)'s seal shape stands).
- D201 (r86): THE SEAL-REFUSAL TRANSITIONS - ACQUISITION (T1), IN-STATEMENT HEAL (T2) AND COMMIT (T3) ARE THREE DISTINCT TRANSITIONS WITH NAMED CLEANUP OWNERS AND SURVIVING STATE (round-36 pre-PR design review F2 (P2), supersedes the commit-only refusal wording of D198(B.2), D199(B.3)/(C.1)/(C.3) and D200(B.1), and refines D171(B)'s three-outcome model with a FOURTH outcome for the seal acquisition).
- D202 (r86): THE FRESHNESS ENVELOPE FOR THE RETRY BOUND - THE PAIRWISE AT-MOST-ONCE (ALPHA) BOUND IS GROUNDED IN THE PRODUCTION TRANSACTION-CREATION APPLIED-FRONTIER WAIT UNDER THE DEFAULT FRESHNESS-PRESERVING PROFILE AND IS PROGRESS-CONDITIONED UNDER THE SACRIFICING- FRESHNESS PROFILE (round-36 pre-PR design review F1 (P2), supersedes D200(B.1)/(B.4)'s 'latest-commit learning' premise).
- D203 (r87): CAUSAL FRESHNESS PER REFUSAL TRANSITION - the corrected convergence accounting (round-37 pre-PR review F1 (P2), refines D202(A)/(B.1)/ (C.1) and the D200(B.4)/(C.3) oracles; D200's two-cause model, D201's transitions and D202's profile honesty stand).
- D204 (r87): THE COLD-GENERATION RC COMMIT REFUSAL (T3-RC) - closing the heal-gate escape and reconciling D198-D201 (round-37 pre-PR review F2 (P2), refines D201(B.2)/(B.3)/(C.3); D198(C.3)'s retained 'same adjudication' RC twin stands as the consistent precedent).
- D205 (r87): PRE-BIND CREATION-CANCELLATION OWNERSHIP - separating the never-bound probe's disposal from post-bind PH-2 retention (round-37 pre-PR review F3 (P2), refines D195(B.4)/(C.1)/(C.2); D192's bound- operator cells stand and gain the re-seated oracle).
- D206 (r88): THE EXPECTED-CONFLICT AVAILABILITY CONTRACT - a mark-aware checker consult making every fabric-reached internal serialization conflict survivable under enable-check-rc-invalid-error (round-38 pre-PR review F1 (P2), revises the protocol's error contract and amends D204's check-on oracles in place; D198's seal shape, D200-D203 and D205 stand).
- D207 (r88): THE RESTART-AWARE CLOCK-CHAIN CONTRACT - the per-generation causal freshness ground and the two closure channels (round-38 pre-PR review F2 (P2), supersedes D203(B.1)'s response-lift sentence and the D203(B.4) envelope pin (ii); D201-D206 stand).
- D208 (r89): THE REFRESH-WRITER MARKING COMPLETION - the owner-removal writer joins the expected-conflict contract (round-39 pre-PR review F1 (P2); completes D206(B.1)'s inventory; D206's mark mechanism, consult sites, parity boundary and cells stand; D198-D205, D207 stand).
- D209 (r89): THE CHANNEL-SEPARATED RESTART FRONTIER PROTOCOL - the frontier-cover certificate is delivered L >= r, never coverage of the conflicting commit (round-39 pre-PR review F2 (P2); supersedes D207(C.1)'s ARMs (ii)/(iii) and sharpens (B.4); D207's in-generation chain, window, channel-C and per-generation accounting stand).
- D210 (r90): THE REAP MARK'S BACKGROUND OPERATOR RESOLUTION - site (4) marks the transaction that OWNS the refresh writes, obtained directly from the DROP's background executor (round-40 pre-PR review F1 (P2); supersedes D208(B.2)'s reach sentence; D208's placement, cells and D206-D209 stand).
- D211 (r90): THE COMPILE-SIDE KEEP WRITER MARKING COMPLETION - the repo-wide kind=0 inventory and SITE (5) at the KEEP funnel (round-40 pre-PR review F2 (P2); completes D208(B.1)/(C.2); D206-D210 stand).
- D212 (r91): THE COMPILE-SIDE RETIREMENT DELETE WRITER ACCOUNTING - the CRUD-complete inventory, SITE (6) and the validated grant-based exemption (round-41 pre-PR review F (P2); completes D211(B.1)/ (C.2); D206-D211 stand).
- D213 (r92): THE FENCED-PATH OUTCOME CORRECTION AND THE TWIN'S REHOMING (round-42 pre-PR review F (P2); supersedes D212(C.1)'s oracle; amends D212(A)/(B.3)/ (B.4)/(C.1); D206-D212 stand).
- D214 (r93): THE RC STATEMENT-LEVEL FRESHNESS CORRECTION - the pinned-isolation adjudication venue (round-44 implementation-adjacent self-finding; supersedes the venue of D206(C.2)/(C.4), D208(C.1), D211(C.1), D212(C.2)/(C.3) and the choreography of D204(A.2), each amended in place; D206(C.1) and D213(C.1) stand UNCHANGED).
- D215 (r94): THE LIFECYCLE-ADMISSION BOUNDARY - separating the lower-level conflict proof from the public DROP proof (round-45 pre-PR design review REQUEST_CHANGES on r93, one P2; supersedes the venue of the DROP-carried cells D211(C.1) and D212(C.2)/(C.3), vacates the D213-rehomed drop-carried Fatal twin, and CONFIRMS D206(C.2)/(C.3)/(C.4)'s r93 venue with the verified carrier record; D208(C.1), D206(C.1) and D213(C.1) stand).
- D216 (r95): THE PUBLIC SNAPSHOT/PITR ADMISSION ORACLES AND THE KEEP BRANCH STATES (round-46 pre-PR design review REQUEST_CHANGES on r94, TWO P2 findings; supersedes D208(C.1)'s r94 admission-witness control and D211(C.1)'s r94 KEEP-arm oracle; D212's DROP-TABLE controls and every r94 D215 landing beyond those two oracles stand).
- D217 (r96): THE REAL-RETENTION NAMESPACE IN THE KEEP FIXTURE ORACLES (round-47 pre-PR design review REQUEST_CHANGES on r95, ONE P2 finding; supersedes D216(B.3)'s and D211(C.1)'s r95 amendment row_kind=1 literals, corrected IN PLACE; every other r95/r94 landing stands).
- D218 (r97): THE ACTUAL-MOVE ADMISSION GATE FOR RETENTION GENESIS AND SEALING (round-48 pre-PR implementation review on the r96-era implementation, ONE P1 finding; continuity markers preserved; every r96 landing stands).
- D219 (r99, CI run 40797678570 repair): the fencing-retirement critical section gets a pre-round gate on its own transaction's locked view.
- D220 (r100, 2026-10-30, owner-scoped provenance reads for subscription-routed segments): `pkMoveLinkTableWindow` gains routing-layer segment identity from `collectRange.idents`; every provenance read derives its context via `identityCtx(base, window.ident)` (the provider probes, the D156 scope-to-commit capture, and the PICK closure probes), so a subscription-routed diff (subscriber clone plus publisher pk move) reads the PUBLISHER's link catalog; the PICK pre-collect window owner is resolved by the reopen walk's ordered-candidate rule `dedupeBranchIdentities(walking, opposite)`; a per-account existence probe degrades per-account; failure semantics and the write path are untouched.
- D221 (r100, 2026-10-30, re-bind the retention relation handle per execution under prepared-compile reuse): in `Prepare`'s per-context loop, a non-nil `info.retentionSource` is re-bound with `Reset(proc.GetTxnOperator())` exactly like `info.Source` (multi_update.go:115-119), and a re-bind error aborts the statement; `resetMultiSources` (multi_update.go:1010) clears retentionSource so the lazy resolver re-resolves under the executing transaction; the sticky `pkMoveRetentionDisabled` degrade is unchanged; the disttae side is untouched.
- D222 (r101, 2026-10-30, historical owner resolution for PICK closure windows after physical-generation retirement): the pre-collect PICK window owner probe anchors at each window's historical transition snapshot (`{MO_TS = '<phys-logical TS>'}`), following `buildSideCollectRange`/`reopenDataBranchNodeByID` precedence (pathTS[i+1] for intermediate windows, endpointSP for the endpoint; an empty snapshot keeps the unanchored probe), so a retired physical generation's owner is resolved as of the transition instead of against the current catalog; the ordered-candidate rule and BETWEEN narrowing are unchanged.

- O1 (owner: issue assignee; decision point: first maintenance window
  after merge): time-based or branch-DAG-aware GC of link history for
  long-lived tables. The DAG-aware horizon — a link is dead once every
  descendant branch forked after its commit — needs branch-metadata
  analysis that this PR deliberately defers; §8 quantifies the residual
  growth and §9.3 owns the reclaim that exists today. r46 D153 owns the
  dead-retained-table lifecycle (ledger + final-owner reap); O1 remains
  scoped to links of LIVE long-lived tables, plus the D154(D) defensive
  residue (empty-name ledger rows from admission+ledger misses), which
  reaps never auto-delete and O1's maintenance may discharge.
- O2 (owner: data-branch maintainers; decision point: first follow-up
  issue): destination-lineage resolution for the sibling family in §2
  (stable-key source update / plain source delete vs destination move).

Approval trace (compact; the full transcripts are transient and remain in
the r101 blob linked from section 1):

| Revisions | Decision(s) | Venue / outcome |
| --- | --- | --- |
| r1-r7 | D1-D17 | Independent design and implementation reviews; F1-F7 (replacement identity, same-key precedence, destination-moved ACCEPT replay, PICK window over-expansion, retention/reader bounds, unproved contracts, missing design) and NF1-NF9 resolved; this document created. |
| r8-r21 | D18-D87 | Pre-PR review F1-F8 (D18-D24: intermediate-key reuse, topological edges, cyclic fold plans, dead-tail pruning, prune placement, truncated BVT results, shared-budget race, unbounded window probe) and the identity/lineage/conflict-matrix rounds; cycle refusal (D20, D57) and effective-key ranking (D25) pinned; D87's persist-explicit-continuity request verified and REJECTED with repository-established grounds (see register). |
| r22-r74 | D88-D184 | Workspace carry/journal integrity, budget ownership and staging discipline (the every-admission-boundary participation rule settled at D144/D150/D151); fencing, retention genesis/sealing and lifecycle admission; the physical-generation retirement protocol assembled. |
| r75-r97 | D185-D218 | Round-25 to round-48 pre-PR design and implementation review findings (each register entry names its round and the elements it refines); all closed before PR opening. |
| r98-r99 | D219 | CI run 40797678570 repair: fencing-retirement pre-round gate on the transaction's own locked view. |
| r100 | D220, D221 | PR #31629 review rounds 49-50: owner-scoped provenance reads for subscription-routed segments; retention relation handle re-bound per execution under prepared-compile reuse. |
| r101 | D222 | PR #31629 review (aunjgr inline): historical owner resolution for PICK closure windows after physical-generation retirement — the owner probe anchors at each window's transition snapshot `{MO_TS = '<phys-logical TS>'}`. |
| r102 | — (editorial) | PR #31629 review (aunjgr, doc scope): tracked artifact compacted to the durable protocol, decisions and this trace; Implementation field corrected; transient history moved to the linked r101 blob. |

Review record:

```text
Change scope: whole feature (single PR on #28026), PR matrixorigin/matrixone#31629
Trigger: catalog persistence change; cross-subsystem protocol; upgrade/restore impact
Design: docs/design/20261016-data-branch-pk-move-update-provenance.md, revision r102 (this document)
Blocking findings: the r1-r101 rounds above; every finding is resolved by the
  numbered decision in the register, except D87 which was verified and rejected
  with grounds, and O1/O2 which remain open items with named owners
Decision log: D1-D222 above, with refinement/supersession stamps; the approvals at
  r1-r4, r8-r15, r16-r21, r22-r46, r47-r74, r75-r97, r98-r101 all PASS after their
  respective blocking findings closed
Decision: the PR #31629 implementation follows sections 5/6 and the section 10
  validation map, and stands for the independent reviewers of record
Implementation deviations: none known; section 5/6 invariants gate the code
```

Approval traceability for the delivered revision (added r103; per the mo-dev
contract, "the reviewed revision and approval decision must be traceable"):

- Approval of record: independent **DESIGN PASS on r100**, recorded
  2026-10-30T00:41:36Z. Reviewed content MD5
  `ca2d0d19b2a39b16c8f1103555b0d128` at worktree HEAD
  `bcc8b099d457ddf88cb9a10d1d47939b6cbc91fa`; the exact reviewed bytes are
  public at PR commit `56c390d567a249e498a906d1d162fe384593951e`
  (and `99dd9c6651a0de20e9730814918feaa8388ed580`):
  <https://github.com/matrixorigin/matrixone/blob/56c390d567a249e498a906d1d162fe384593951e/docs/design/20261016-data-branch-pk-move-update-provenance.md>
- Reviewer identity: the issue-fix-loop controller's independent local
  reviewer — a read-only, ephemeral Codex review session under the
  fix-issue-loop-local-reviewer contract, provider-separated from the
  implementing handler (reviewer model gpt-6.1-sol, reasoning xhigh; handler
  Grok CLI, magpie/tass/kimi-k3). `approved=true` approves that exact design
  revision only; implementation and executable proof are a separate review.
- Delivered-lineage check: r101 = r100 + D222 only (the r100→r101 document
  diff is 68 added lines, 0 deletions, all D222-scoped); D222 was demanded by
  this design-gate review's round-51 finding and confirmed resolved in the
  2026-10-30T13:43:12Z review. r102 = r101 with sections 2-9 byte-identical
  (block MD5 `102bf6b88108c5b1c8f3ed490e71a3fc` on both sides); delivered
  r102 content MD5 `483aaf9da027ac7fec1ed832ecb88acf` at commit
  `053b5ffbac87bcc69613cd2667c4edff46d82919`. r104 = r103 with the three
  placement references (§3, §7's D149 prose, §10's anchor) corrected to the
  delivered v4_0_19 vehicle; editorial only, no protocol or decision change.
- Durable decision record and verbatim artifact:
  <https://gist.github.com/gouhongshen/5e3e7f1c103a8312e31c684676bf0cad>
  (decision record README plus the r100 PASS artifact JSON, MD5
  `29a610e69e0d27e802428bf0c8999b1a`, copied verbatim).
