# Data Branch pk-move update provenance (#28026)

## 1. Revision and scope

| Field | Value |
| --- | --- |
| Owning issue | matrixorigin/matrixone#28026 |
| Implementation | PR [matrixorigin/matrixone#31629](https://github.com/matrixorigin/matrixone/pull/31629) (branch `agent/issue-28026-688adec6` on fork `gouhongshen/matrixone`), single PR |
| Current revision | r113, 2026-11-02, supersedes r112 |

| Revision | Date | Note |
| --- | --- | --- |
| r1-r71 | 2026-10-16 - 2026-10-24 | Initial protocol design and the first pre-PR review rounds. |
| r72-r101 | 2026-10-25 - 2026-10-30 | Post-review repair rounds; each round's disposition is recorded per-decision in section 11. |
| r102 | 2026-10-30 | Editorial compaction only — no protocol or behavior-design change. Answers the PR #31629 review by aunjgr: the tracked artifact now keeps the durable content (the problem/invariant protocol of sections 2-9, the invariant validation anchors of section 10, and the complete 222-decision register with its refinement chain, open items and approval trace of section 11) and drops the transient material the repository delivery contract excludes (per-run verification logs and walkthroughs, temporary local paths, one-off proof narratives, and full review transcripts). The Implementation field is corrected to name PR #31629. The superseded full text of r101 remains reachable as linked external evidence at the pre-compaction blob: <https://github.com/matrixorigin/matrixone/blob/430914c128ee5afd36880afd8f44d242bcc50f7f/docs/design/20261016-data-branch-pk-move-update-provenance.md> |
| r103 | 2026-10-30 | Editorial only — no protocol or decision change. Answers the PR #31629 design-gate review by aunjgr (2026-10-30T13:43:12Z): section 11 now identifies the independent design approval of record for the delivered revision with its reviewed content hash, public byte link, reviewer identity, and a durable artifact link. No completed review was rerun; no review transcripts are added to the repository. |
| r104 | 2026-10-31 | Editorial only — no protocol or decision change. Answers the PR #31629 Codex automated follow-up review (2026-10-30T20:27:13Z, non-blocking P2): sections 3, 7 and 10 now name the delivered v4_0_19 tenant-upgrade vehicle and its real test package instead of the retired 4.0.8 design-time placeholder; the D149 register entry already states the version-neutral forward-placement rule, and no register entry changed. |
| r105 | 2026-10-31 | Editorial placement re-derivation — no protocol or behavior-design change. Upstream #31488 folded the unpublished 4.0.8-4.0.19 identities into the unpublished 4.0.7 handler and deleted their packages; docs/design/20261028-catalog-upgrade-407.md forbids registering those identities as upgrade steps. The two provenance catalog creations therefore ride as the tail tenant entries of the unpublished 4.0.7 handler (offset 41 -> 43), still strictly forward of every supported upgrade source; sections 3, 7 and 10 and the D149 register note name the re-derived placement, and the incompatible-unreleased-layout rejection for main-era 4.0.7-offset-41 clusters is recorded. All decisions, invariants and validation anchors stand. |
| r106 | 2026-11-02 | Protocol repair — one new decision (D223). Answers the PR #31629 review by aunjgr (2026-11-02T09:41:16Z, blocking P2): the unanchored provenance probe read the apply statement's snapshot, so an ancestor transaction begun before a fork but committed after its lineage window ended was visible to that window's probe and poisoned a valid child-generation anchor on the same inherited row identity before stream commit-membership screening could exclude it. D223 snapshot-anchors every provider-window catalog read at the window's full end timestamp and degrades pre-catalog windows to complete absence; section 5.3 and the section 10 validation map record the repaired bounds. REJECTED at independent design review before implementation (see r107); D223 never shipped and is superseded by D224. |
| r107 | 2026-11-02 | Protocol repair — D223 withdrawn, one new decision (D224). The independent design review of r106 (the issue-fix-loop controller's local reviewer, 2026-11-02, REQUEST_CHANGES, blocking P2) verified that D223's `{MO_TS = window.end}` reads of the link catalog require retained historical storage versions that no protection channel covers: TN's all-snapshot protection names only the three core catalog tables, table-level snapshots propagate only to non-system tables of the same database (system tables are skipped), and the mo_catalog PITR channel carries only user-created PITRs, so catalog-object rewriting and reclamation could delete the very version an anchored read queries while its logical links remain live. No CN-expressible mechanism can establish that retention, and adding one in TN is outside this PR's scope. r107 therefore repairs the straddler defect without any historical catalog-state dependency: provenance probes keep the unanchored current-snapshot posture, probed links keep their per-(table, window) attribution, and identity conflict checking is deferred until commit membership is established through the D156 scope-to-commit capture (D224). Section 9.3 gains the storage-version retention audit and section 10 gains the demanded lifecycle validation scenario (table-scoped branch protection only, no account/cluster snapshot or PITR, catalog rewriting/reclamation, restart, exact MERGE/PICK cascade assertions). |
| r108 | 2026-11-02 | Protocol repair — D224 withdrawn, one new decision (D225). The independent design review of r107 (the issue-fix-loop controller's local reviewer, 2026-11-02, REQUEST_CHANGES, blocking P1 + P2) found: (P1) D224(f)'s single-candidate exemption let a lone out-of-window candidate start a chain without membership proof — an ancestor straddler's link can splice onto a sibling branch's independent same-transaction DELETE/INSERT sharing the inherited row identity, because the claim binds by commit instant alone and never binds the candidate's recording transaction to the deleting transaction (verified at the reviewed head's chain walk: the claim accepts any row born at the seeded instant and the durable `new_row_id` is never compared, so the destination count guard passes and a wrong UPDATE cascade executes); (P2) D224's membership screening made valid anchor resolution depend on the D156 capture, which reads the link catalog's committed change history — reclaimable under the §9.3 audit — so after ordinary catalog rewriting/reclamation a valid retained branch could no longer resolve, contradicting D13 (link lifetime equals protected-stream lifetime). r108 repairs both without any historical catalog-state dependency and without consulting reclaimable change history on any resolution path: the link row preserves its recording transaction's full-precision commit instant (new `commit_ts` column, D15's preservation executed in its commit-exact form), stamped by a CN-side commit-completion step that observes the TN-assigned commit timestamp; every candidate that starts or continues a chain must present `commit_ts` equal to the departure tombstone's instant (exact-transaction binding; certified window membership by construction; no single-candidate exemption); the D156 scope-to-commit capture is retired from every resolution path (the arrival tie re-attributes from the preserved column). Section 5.1 gains the column and stamping contract, section 5.2 the commit-exact gate, section 5.3 the revised reader bounds, section 9.3 the rewritten retention audit (the reclaimable-history reader class is removed), and section 10 the revised validation map — including the review's demanded singleton-straddler independent DELETE/INSERT oracle (DELETE semantics, FK rejection, unchanged destination parent/child rows, MERGE and PICK) and the lifecycle oracle restoring exact cascade assertions after catalog reclamation and restart. Submitted for fresh independent design approval before implementation; D224 never shipped. |
| r109 | 2026-11-02 | Protocol repair — D225 amended, one new decision (D226). The independent design review of r108 (the issue-fix-loop controller's local reviewer, 2026-11-02, REQUEST_CHANGES, blocking P1 + P2) found: (P1) D225(d) treated `commit_ts` equality as sufficient window membership and discarded per-window attribution, so ONE transaction — an RC transaction begun before a fork whose per-statement snapshot advance (`handleRCSnapshot`, `pkg/vm/engine/disttae/types.go:2022-2031`) lets it write both branches — can perform the ancestor move AND the sibling's independent same-transaction DELETE/INSERT in a single commit; the ancestor window's unanchored probe returns that link (`write_ts` = create ts), its `commit_ts` equals the sibling anchor's departure instant (one transaction, one commit instant), and the equality-only gate admits it and claims the sibling's unrelated row — the destination count guard passes and a wrong UPDATE cascade executes (verified at the reviewed head: windows are relation-specific, `pkg/frontend/data_branch.go:2628-2653`; the identity merge omits the table, `data_branch_pk_move_link.go:1067-1081`; the terminal claim checks birth time only, `:3246-3301`; guard `data_branch_output.go:2200-2241`). Transaction identity does not establish WHICH table's operation a link records. (P2) r108's two-step publication (commit first, stamp later) left a durable gap: a stamping failure, cancellation, lost commit response (`FinalizeCommitWithUnknownResult`, `pkg/vm/engine/disttae/txn.go:4004-4011`), or CN crash leaves `commit_ts` NULL permanently, and r108 made that state abort forever — an otherwise valid committed branch permanently losing MERGE/PICK provenance after a transient failure — while the completion event joins callback errors into the post-commit result (`pkg/txn/client/operator.go:1187-1191`), leaving COMMIT-result semantics unclosed. D226 repairs both: per-(table, window) candidate attribution is restored and a candidate's preserved `commit_ts` must fall INSIDE its own attributed window in addition to equalling the departure instant; and NULL `commit_ts` becomes a recoverable durable intent — the commit instant is re-derived at resolution from the protected user-table stream (the link's `old_row_id` row version dies exactly once; I7 ties the link to that commit) with same-transaction inheritance for placeholder chains, a best-effort re-stamp, and a closed COMMIT-result rule (stamping never alters the user transaction's own outcome). Section 5.1's stamping contract, section 5.2's admission gate (plus the new recovery rule), section 5.3's reader bounds, section 6.2's failure semantics, section 9.3's audit consequences, section 10's validation map (the review's demanded one-transaction ancestor-move + sibling DELETE/INSERT oracle for MERGE and PICK, and deterministic crash/failure checkpoints requiring eventual exact cascades with rollback leaving no provenance), and this register updated. Submitted for fresh independent design approval before implementation; D225's r108 form never shipped. |
| r110 | 2026-11-02 | Protocol repair — D226 amended, one new decision (D227). The independent design review of r109 (the issue-fix-loop controller's local reviewer, 2026-11-02, REQUEST_CHANGES, blocking P1 + P2) found: (P1) D226's recovery rule classified every non-workspace `old_row_id` as a committed stream-bound version and excluded a NULL-stamped candidate with no in-window death, but uncommitted S3 rows also carry non-workspace object identities whose same-transaction deletes are diverted without publishing a tombstone (`pkg/vm/engine/disttae/txn.go:2544-2569`: the delete lands in the per-block list, the batch is shrunk, and the object is rewritten minus the row at commit), so under a stamping failure the second hop of an S3-netted chain — the reviewed schedule: one transaction moves 2->1000->1001 through a forced small-object flush and independently reinserts 1000 (`test/distributed/cases/git4data/branch/merge/merge_foreign_key_pk_move.sql:879-907`) — is wrongly excluded, the terminal timestamp claim then adopts the unrelated reinserted row born at the chain instant (`data_branch_pk_move_link.go:3206-3301`), the destination count guard passes (`data_branch_output.go:2200-2241`), and the apply cascades onto the impostor key instead of the lineage's final key; and (P2) the recovery rule text excluded a candidate with no in-window death while the soundness argument and section 5.3 classified the same shape (no death, no bound sibling) as a loud abort — an unstamped successor committed after a historical endpoint (`test/distributed/cases/git4data/branch/pick/pick_foreign_key_pk_move.sql:822-855`) would abort the apply and block the valid earlier in-window move, contradicting D226's own promise that out-of-window candidates cannot poison valid applies; the stamped form already resolves by ending the lineage at the claimed version (the terminal exclusion path, `data_branch_pk_move_link.go:3303-3322`). D227 repairs both: recovery becomes a single outcome table keyed by identity class — heal from the unique in-window death; inherit the bound instant across the candidate's own transaction for transaction-local netted identities (workspace placeholders AND uncommitted S3 object identities — a missing death carries no window information for either, and window absence is never inferred from it); exclude on proven window absence (stream-bound identity, NF5-complete stream, no in-window death — never loud); abort loudly only on incomplete or corrupt evidence (a truncated/disabled stream, a duplicate in-window death, or an unbound transaction-local candidate). The established continuation checks and per-(table, window) attribution are preserved, and the two-condition gate still evaluates the inherited instant against the candidate's own window. Section 5.1's NULL-identity bullet is aligned with the recorded S3 continuity (D32/D49/D50, D75-D81), section 5.2's same-transaction continuation records both shipped chain forms and the recovery rule is rewritten as the outcome table with the soundness argument aligned, sections 5.3/5.4/6.2/9.3 name the table, section 10 gains the demanded MERGE/PICK stamping-failure and crash twins of both S3-chain scenarios (asserting the exact child key and the preservation of the unrelated intermediate row) plus the NULL-stamped future-successor and cross-fork cases requiring the valid in-window move to cascade, and this register updated. Submitted for fresh independent design approval before implementation; D226's r109 form never shipped. |
| r111 | 2026-11-02 | Protocol repair — D227's continuation rule amended, one new decision (D228). The independent design review of r110 (the issue-fix-loop controller's local reviewer, 2026-11-02, REQUEST_CHANGES, blocking P1 + P2) found: (P1) D227(d)'s key-chained continuation established continuity from same transaction, same key, and an absent tombstone — but the reviewed attack distinguishes nothing between the landed generation's own departure and an EARLIER unrelated departure of a different holder: one transaction inserts an unrelated row B at 1000, spills it to an uncommitted S3 object, moves B 1000->1001 (the delete is diverted per `pkg/vm/engine/disttae/txn.go:2544-2569` and publishes no tombstone), then moves A 2->1000 with an identity-less S3 replacement; both links survive the commit fixup (B's death at 1000 predates A's staging statement, so the fixup's `od < birth` post-mortem rule ignores it — `pkg/vm/engine/disttae/pk_move_link_fixup.go`; B's replacement survives), D227 heals A's link at the death of 2, lets B's link inherit that same instant, and the walk's key-chain rule (`pkg/frontend/data_branch_pk_move_link.go:3224-3234`) follows B to 1001 instead of ending A at 1000 — the destination count guard passes (`data_branch_output.go:2200-2232`) and children cascade onto the unrelated B; the existing vacate-before-arrival coverage uses a COMMITTED B (`merge_foreign_key_pk_move.sql:1055-1084`) whose tombstone masks the uncommitted form; and (P2) the submitted public acceptance projection's limitations preserved the rejected r109 reading — a stream-bound candidate with no unique in-window death on retained evidence classified as unprovable provenance that aborts loudly — contradicting the same projection's own exclusion semantics and D227(c)'s proven-window-absence row. D228 repairs both: same-transaction continuity stops being INFERRED at the walk and becomes RECORDED at commit — over the converged post-prune link set, the commit-time fixup pairs each retained link arriving at a key with the same-transaction retained link departing that key at the smallest later statement ordinal (sound by the existing fixup invariants: one row per key per instant; prune-on-unexplained-death; the `od < birth` post-mortem rule), persists the pairing on the link row (new `succ_link_id`/`stmt_seq` columns riding the same unpublished upgrade entry and predefined DDL — the table is unshipped, so an in-place schema addition rides the same 4.0.7 tail), and the walk follows ONLY the recorded pairing (exact link_id point probe; scope equality, key match and ordinal order verified; identity match and the tombstone screen demoted to consistency cross-checks whose contradiction is corrupt evidence; a NULL pairing on a link whose replacement provably died in the transaction is loud). In the review's schedule B's departure (earlier statement) never pairs with A's arrival (later statement), so A's chain ends at 1000 and B remains at 1001 as its own plain insert — stamped and stamp-pending alike. The r110 key-chain acceptance is retired. The acceptance projection's limitations are restated as the four outcomes consistently (unique death heals; proven window absence excludes; proven transaction-local continuity inherits; incomplete, corrupt or unbound evidence aborts) with no blanket no-death abort. Section 5.1 gains the credential columns and the aligned continuity bullets, section 5.2's same-transaction continuation is rewritten to follow the recorded pairing and its recovery row names D228, sections 5.3/6.2/7/9.3 record the pairing's reader bounds, storage form and schema-drift posture, section 10 gains the review's demanded MERGE/PICK twins of the uncommitted vacate-before-arrival schedule (stamped AND stamp-pending, asserting A's child ends at 1000 and unrelated B remains at 1001) plus the pairing/fixup/walk UTs, and this register updated. Submitted for fresh independent design approval before implementation; D227's key-chain form never shipped. |
| r112 | 2026-11-02 | Protocol repair — D228's revisit clause amended, one new decision (D229). The independent design review of r111 (the issue-fix-loop controller's local reviewer, 2026-11-02, REQUEST_CHANGES, blocking P2) found: r111 recorded that the cycle guard applies unchanged to pairing-followed hops, so a pairing-followed hop landing on ANY previously visited key fails closed — but D228 itself pairs the two links of a one-transaction `2->3->2` return (the first link's arrival at 3 pairs with the second link's departure of 3 at the greater statement ordinal), and the pairing-followed hop lands on the already-seeded anchor key 2, so the specified guard aborts a valid apply, contradicting retained D70 (a PROVEN same-transaction return to the original key resolves). Verified at the cited loci: the shipped parser rejects only strictly backward birth certificates and continues equal-certificate homecomings (`pkg/frontend/data_branch_pk_move_link.go:3148-3161`, `:3173-3178`), and the shipped success oracles prove returns resolve — the one-transaction `2->3->2` and `2->3->4->2` walk UTs (`pkg/frontend/data_branch_pk_move_link_test.go:1420-1469`) and the changed/unchanged-payload MERGE and PICK BVTs (`test/distributed/cases/git4data/branch/merge/merge_foreign_key_pk_move.sql:1514-1561`, `test/distributed/cases/git4data/branch/pick/pick_foreign_key_pk_move.sql:1088-1133`). A single lineage returning home is distinct from a multi-row key rotation. D229 repairs the rule: a verified pairing-followed hop landing on the lineage's own previously visited key is a proven return and continues under the parser's birth-certificate order (scope equality fixes one commit instant — an equal certificate; the strictly greater `stmt_seq` proves the recorded links progress; a strictly backward certificate fails closed as corrupt evidence), termination holds because `stmt_seq` strictly increases within the committing transaction's statement count and the hop cap bounds the walk regardless, the terminal NULL pairing ends the chain at the returned key and the fold planner's `oldKey == finalPK` path folds changed/unchanged payloads (D66), and the fold planner still rejects true multi-row key rotations (D57); non-pairing cross-key revisits keep the existing fail-closed posture. Section 5.2's guard is rewritten, section 5.1's `stmt_seq` bullet records the termination argument, D228's register entry gains the amendment stamp and D229 registers the repair, section 10 carries the existing changed/unchanged-payload MERGE/PICK return-home success oracles into the persisted-pair validation plan (stamped AND stamp-pending forms, with the persisted pairing asserted) plus the walk UTs, the acceptance projection preserves the proven-return behavior, and this register updated. Submitted for fresh independent design approval before implementation; D228's blanket-revisit clause never shipped. |
| r113 | 2026-11-02 | Protocol repair — D229's revisit clause amended, one new decision (D230). The independent design review of r112 (the issue-fix-loop controller's local reviewer, 2026-11-02, REQUEST_CHANGES, blocking P2) found: r112 admitted revisits only through recorded same-transaction pairings or same-key tails, so two SEPARATELY committed updates `2->3` at T1 then `3->2` at T2 > T1 abort when the cross-transaction departure returns to the seeded anchor key 2 — contradicting retained D66, whose approved r100 text (commit `56c390d567a249e498a906d1d162fe384593951e`, lines 2040-2054) explicitly requires exactly this schedule to fold as an in-place UPDATE (changed payload) or no-op (unchanged payload): "Two separately committed source updates `2 -> 3 -> 2` resolve through the advancing-timestamp chain into one fold whose departure and arrival share key 2". Verified at the cited loci (the deciding code is unchanged from r111): the walk binds the departure to the lineage's own landed version through the departed-birth-pinned pairing index (`pkg/frontend/data_branch_pk_move_link.go:2879-2929`), advances the birth certificate at every cross-transaction hop (`:3381-3393`), rejects only strictly backward certificates on revisit (`:3173-3178`, with the later-certificate cross-transaction return documented at `:3148-3161`), resolves the terminal claim at the chain instant (`:3290-3301`), and aborts an unresolved destination-held anchor (`:3241-3244`); the fold planner excludes the self-dependency (`pkg/frontend/data_branch_hashdiff.go:3423-3435`, whose comment names the two-committed-moves `2->3->2` shape); and the shipped success oracles cover the separate-transaction changed/unchanged-payload returns (`test/distributed/cases/git4data/branch/merge/merge_foreign_key_pk_move.sql:1431-1476`, `test/distributed/cases/git4data/branch/pick/pick_foreign_key_pk_move.sql:1006-1051`). D230 repairs the rule: a hop landing on a previously visited key follows the parser's birth-certificate order for EVERY hop class — a strictly backward certificate fails closed as corrupt evidence; an equal certificate continues only through the verified recorded same-transaction pairing (D229's homecoming); and a strictly newer certificate is a PROVEN cross-transaction return whenever the hop is ATTRIBUTABLE to this lineage — the departure tombstone binds the lineage's own landed version (departed-birth equals the running certificate) and the successor link passes D226's windowed admission gate (its `commit_ts` equals the departure instant AND falls inside its own attributed window) — so the lineage itself returned to a key it vacated earlier, never a foreign reincarnation (a foreign row's departure names a different departed birth and is never served to this walk). Termination holds: cross-transaction hops strictly advance the commit instant (one transaction one instant over the committed stream), pairing-followed hops strictly advance `stmt_seq`, same-key tails strictly advance the instant, and the hop cap bounds the whole walk regardless. The backward-certificate rejection, the hop bounds, the D57 genuine-rotation rejection, and the D228 recorded-pairing mechanism all stand. Section 5.2's guard is rewritten to the three-way order, section 5.1's `stmt_seq` bullet gains the cross-transaction termination argument, D229's register entry gains the amendment stamp and D230 registers the repair, section 10 gains the separate-transaction changed/unchanged-payload MERGE/PICK return successes and the attributable `2->3->4->3` returns in stamped AND stamp-pending forms plus the walk UTs, the acceptance projection admits the proven cross-transaction return, and this register updated. Submitted for fresh independent design approval before implementation; D229's same-transaction-only revisit clause never shipped. |
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
- affects tenant upgrade (the unpublished 4.0.7 handler's tail tenant
  entries, D149's forward placement),
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
    txn_id     binary(16)      not null comment 'recording transaction identity; namespaces workspace-minted link ids and same-transaction matching',
    commit_ts  binary(12)      null comment 'full types.TS byte form of the recording transaction commit instant; NULL until commit-completion stamping lands',
    succ_link_id binary(64)    null comment 'link_id of the same-transaction continuation paired by the commit-time fixup over the converged post-prune set; NULL when this generation is terminal (D228)',
    stmt_seq   bigint unsigned not null comment 'ordinal of the staging statement inside the recording transaction (D228)',
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
  first. Its consumer role is chaining same-transaction netted
  generations (§5.2); the fold itself runs on the commit-timestamp tie,
  so a NULL that survives staging still folds single-shot. A
  replacement the statement flushed to an uncommitted S3 object keeps
  no usable staged identity (the column stays NULL or names a
  rewrite-erased placeholder), yet the netted generation still chains:
  the commit-time fixup pairs the transaction's retained links over its
  own converged post-prune set and persists each continuation on the
  arriving link row (`succ_link_id`, D228), and §5.2's
  same-transaction continuation follows ONLY that recorded pairing —
  one mechanism for the workspace-placeholder form and the S3-netted
  form alike, replacing r110's inferred key-chain acceptance
  (D32/D49/D50, D75-D81's recording-side S3-route continuity stands as
  the fixup's input evidence). The `UpdateWriteS3` operator path
  writes no link rows at all (fail closed, legacy replay).
- `write_ts` is the *creation* timestamp of the moving transaction
  (`CreateTS` of the txn operator), in physical nanoseconds. The create
  timestamp is constant for the whole transaction, which the
  same-transaction chain match (§5.2) requires: under read-committed the
  statement executors refresh `SnapshotTS` per statement, so two
  pk-moving UPDATEs of one transaction would record *different*
  snapshots and could never chain. It still lower-bounds the commit
  timestamp (commit > create), so window probes keep their one-sided
  sound filter (D2).
- `txn_id` is the recording transaction's durable 16-byte identity
  (D26); an all-zero value marks "identity unknown" (defensive — the
  recorder always has its operator id) and falls back to the
  create-timestamp namespace.
- `commit_ts` preserves the recording transaction's COMMIT instant
  (r108, D225 — D15's preservation executed in its commit-exact form):
  the full 12-byte `types.TS` byte form (physical + logical), because a
  physical-only stamp cannot order two transactions committed inside
  one physical instant — the HLC logical component distinguishes them,
  and the walk compares stream instants with full precision. The moving
  statement stages the row with `commit_ts` NULL — the commit instant
  is unknowable before TN assigns it at commit — and a CN-side stamping
  step runs after the recording transaction's successful commit: the
  commit-completion event observes the TN-assigned timestamp
  (`pkg/txn/client` operator completion), and one bounded internal
  transaction updates exactly this transaction's staged link rows by
  their exact `link_id` set, carried from the transaction's own bounded
  staging bookkeeping. No CN write can ride the same commit batch
  (entries are applied before the response returns the assigned
  timestamp), so the stamp is necessarily a second, small transaction.
  Publication and recovery contract (r109, D226): `commit_ts` NULL is
  a durable *stamp-pending intent*, not an error state. The eager
  stamping step is BEST-EFFORT and never alters the user-visible
  COMMIT result — the completion event joins callback errors into the
  post-commit result path, so the stamping callback swallows and logs
  its own failures instead of returning them: a committed move always
  reports committed, and a failed/rolled-back move has no links to
  stamp (I7). A stamping failure, its cancellation, a CN crash in the
  stamping window, or a lost commit response
  (`FinalizeCommitWithUnknownResult` — the CN releases its local state
  without learning the outcome) leaves `commit_ts` NULL, and the
  §5.2 recovery rule (r110, D227's single outcome table) re-derives the
  commit instant at resolution time from the protected user-table
  stream — never from reclaimable catalog change history and never
  from GC-able TN txn records — so committed moves keep resolving
  exactly, including chains that net generations through uncommitted
  S3 objects: their diverted deletes publish no tombstone, so the
  instant is inherited across the transaction's own recorded
  continuity — D228's persisted `succ_link_id` pairing — and never
  inferred from a missing death. Because link lifetime equals
  protected-stream lifetime (D13), both the pending intent and its
  recovery evidence remain available for exactly the protected
  lineage's lifetime without any reclaimable catalog change history.
- `succ_link_id` is the transaction's RECORDED same-transaction
  continuation credential (r111, D228): over the converged post-prune
  link set of one committing transaction, the commit-time fixup pairs
  each retained link `L` arriving at key `K` (staging statement
  ordinal `a`) with the same-transaction retained link departing `K`
  at the smallest ordinal `d > a`, and persists the paired link's
  `link_id` here. The pairing is sound by the fixup's own invariants:
  one row per key per committed instant; a retained link whose landed
  generation died unexplained inside the transaction is pruned; and a
  continuer whose old key's newest death predates its staging (the
  fixup's post-mortem rule) is reincarnation lineage and never
  registered — so the smallest later departure of `K` is `L`'s own
  continuation and an EARLIER unrelated departure of a different
  holder can never be paired. The value is computed unconditionally
  at commit, so a D228-era NULL always means TERMINAL (this generation
  is the chain's end), never "unknown". Pairing is stamp-independent:
  it derives from the transaction's own statement ledger, so it holds
  for stamped and stamp-pending links alike.
- `stmt_seq` is the ordinal of the link's staging statement inside the
  recording transaction (r111, D228), recorded when the link is
  staged. The walk verifies a followed pairing is strictly increasing
  (`succ.stmt_seq > link.stmt_seq`), which with transaction-scope
  equality bars cross-transaction splicing of a pairing. The strict
  increase also bounds the walk when a proven return revisits a key
  (r112, D229; r113, D230): pairing-followed hops are bounded by the
  committing transaction's statement count, cross-transaction hops
  strictly advance the commit instant (one transaction, one commit
  instant over the committed stream, so each admitted hop consumes a
  distinct instant), and same-key tails strictly advance the instant —
  so repeated keys cannot cycle, and the hop cap bounds the whole
  walk regardless.
- ROLLBACK removes link rows with the user rows (workspace removal);
  the commit atomically publishes move + link (I7); the
  commit-completion stamping above then fills `commit_ts` — it touches
  no user rows and is itself one atomic transaction.

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
   WINDOWED COMMIT-EXACT ADMISSION (r109, D226): candidates keep
   their per-(table, window) attribution — each hop consults the
   candidate pool of the window whose stream carries that hop's
   departure tombstone, never a cross-window merged identity map —
   and `L1` must satisfy BOTH (i) `commit_ts` inside its own
   attributed window's `[start, end]` and (ii) `commit_ts = T`.
   The link insert committed atomically with the move's own
   delete+insert (I7), and at one committed instant at most one
   transaction deletes a given row version and at most one births a
   given key (F1+F2), so a link whose preserved commit instant equals
   the anchor tombstone's instant is provably the tombstone's author.
   Window membership (i) is a separate requirement, not a consequence
   of equality (ii): one transaction can write several lineage
   generations in a single commit (an RC transaction's per-statement
   snapshot advance lets a pre-fork transaction write the parent and
   the post-fork sibling alike), so the ancestor window's unanchored
   probe can return that transaction's ancestor-table link whose
   `commit_ts` equals the sibling anchor's instant — but the
   ancestor's segment of THIS source lineage ended before that commit,
   condition (i) fails, and the candidate is excluded before any
   identity comparison: it can neither bind nor poison this anchor
   (the r108 review's P1 counterexample — one transaction performing
   both the ancestor move and the sibling's independent
   DELETE/INSERT). Equality (ii) excludes the classic straddler
   (created before a fork, committed after its lineage window ended):
   its commit instant differs from the anchor's. With no admissible
   candidate the anchor is proven link-less and keeps legacy DELETE
   semantics. `commit_ts` NULL triggers the §5.2 recovery rule below
   FIRST — the commit instant is re-derived from this window's
   protected stream or inherited across the candidate's own
   transaction (D227's single outcome table) — and the gate then
   admits, excludes (proven window absence), or fails closed LOUDLY
   (D51/D56) exactly as the table rules: a loud verdict follows only
   from incomplete or corrupt evidence, never from a stamp-pending
   candidate alone and never a silent legacy downgrade. Every
   candidate passes this gate; there is no single-candidate exemption.
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
     `succ(K, R)` continues the walk under the same windowed
     commit-exact admission (D226): the paired link is drawn from the
     candidate pool of the window carrying that tombstone, and its
     `commit_ts` must fall inside that window's `[start, end]` AND
     equal `T'`, the departure tombstone's own instant. A paired
     candidate with `commit_ts` NULL first takes the §5.2 recovery
     rule (D227): a stream-bound identity with no in-window death on
     a complete stream is excluded as proven out-of-window history —
     never loud — and only an unrecoverable candidate (corrupt,
     truncated, or unbound transaction-local evidence) poisons the
     hop, failing closed loudly; a candidate outside
     its window or with `commit_ts != T'` is excluded and the
     tombstone is treated as unpaired (a straddler's out-of-window
     link never chains off a deletion its recorder did not author);
     two DISTINCT admitted links pairing one departed version fail
     closed loudly (at one instant at most one transaction deletes a
     row version, so duplicates are genuinely ambiguous catalog
     evidence). On admission: `K = L'.new_pk`, `T = T'`,
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
     netted out of the stream by its own transaction, and the walk
     follows the departure the transaction itself RECORDED at commit
     (r111, D228): the incoming link's `succ_link_id` names the paired
     continuation's exact `link_id`, persisted by the commit-time
     fixup over the transaction's converged post-prune link set (§5.1).
     The walk fetches exactly that link by `link_id` point probe and
     verifies the pairing: the fetched link is present; scope equality
     holds (`txn_id`/`write_ts` equal the incoming link's — one
     transaction, D18/D26); its `old_pk` equals the running `new_pk`;
     and its `stmt_seq` is strictly greater than the incoming link's.
     A verified pairing continues the walk; `T` and `S` carry over,
     `I = L'.new_row_id`. The pre-D228 evidence is demoted to
     CONSISTENCY CROSS-CHECKS over the fetched link: when the incoming
     link's inhabitant `I` is a workspace-segment placeholder, the
     fetched link's `old_row_id` must equal it; and an in-window
     stream tombstone naming the fetched link's `old_row_id` (the old
     key-chain's vacate-before-arrival screen) contradicts the
     recorded pairing. Either contradiction is corrupt catalog
     evidence and fails closed loudly — the fixup's invariants make
     both unreachable over evidence it wrote itself. A NULL
     `succ_link_id` means TERMINAL (the fixup computes the pairing
     unconditionally, so NULL never means unknown): the chain ends at
     the landed generation — and a retained same-transaction link
     departing the landed key at a greater `stmt_seq` while the
     incoming link's pairing is NULL contradicts the deterministic
     pairing rule and fails closed loudly. Because the pairing is
     recorded over the transaction's own post-prune set in statement
     order, an EARLIER unrelated departure of a different holder of
     the key never pairs with a LATER arrival (the r110 review P1's
     schedule: B's departure statement precedes A's arrival statement,
     so A's link pairs with nothing), and the departed-then-replaced
     key's new holder keeps its own identity — stamped and
     stamp-pending alike. This REPLACES r110's inferred acceptance
     forms (the workspace-identity match and the S3-route key-chain
     with its tombstone-only screen): same transaction, same key, and
     an absent tombstone could not distinguish the landed generation's
     own departure from an earlier unrelated departure of a different
     holder — the diverted delete publishes no tombstone
     (`pkg/vm/engine/disttae/txn.go:2544-2569`), so the screen caught
     only the committed-holder form (the r110 review's P1). The
     continuation's `commit_ts` equals the running chain's by
     construction (one transaction, one commit instant), so netted
     hops inherit the anchor's commit-exact binding instead of
     re-proving it — and a NULL `commit_ts` on a netted hop heals by
     that same inheritance for the workspace-placeholder identity and
     the netted S3 object identity alike (D227's recovery rule: their
     diverted deletes publish no tombstone, so a missing death carries
     no window information for either class). The cycle guard below is
     amended for proven returns (r112, D229; r113, D230): a verified
     pairing-followed hop landing on the lineage's own previously
     visited key is a PROVEN same-transaction return (D70), and an
     attributable cross-transaction hop landing on a previously
     visited key with a strictly newer birth certificate is a PROVEN
     cross-transaction return (D66) — both continue under the parser's
     birth-certificate order.
   - Landing on a previously visited key follows the parser's
     birth-certificate order for EVERY hop class
     (`pkg/frontend/data_branch_pk_move_link.go:3173-3178` rejects
     only strictly backward certificates; the later-certificate
     cross-transaction return is documented at `:3148-3161`).
     (i) A STRICTLY BACKWARD certificate is corrupt evidence and
     fails closed, whatever the hop class. (ii) An EQUAL certificate
     continues only through the verified recorded same-transaction
     pairing (r112, D229): scope equality fixes one transaction and
     one commit instant (D18/D26) and the strictly greater `stmt_seq`
     proves the recorded links progress — D70's same-transaction
     homecoming (a one-transaction `2->3->2`, or longer, returning
     to the anchor key). The chain then ends at the terminal link's
     NULL pairing; the claim accepts the re-birthed row born at the
     chain instant, and the fold planner's `oldKey == finalPK` path
     folds a changed payload as an in-place update and an unchanged
     payload as a no-op (D66 — a return to the original key is plan
     shape, never a rotation cycle). (iii) A STRICTLY NEWER
     certificate continues through either proven channel (r113,
     D230): the verified recorded same-transaction pairing (a return
     to a key the lineage itself vacated in an earlier generation,
     reached at the recording transaction's own instant — D229), a
     same-key tail re-entry (the tail step above), or an
     ATTRIBUTABLE cross-transaction departure — the departed-birth-
     pinned pairing index serves the hop only when the departure
     tombstone names the lineage's own landed version (the departed
     birth equals the running certificate,
     `pkg/frontend/data_branch_pk_move_link.go:2879-2929`), the
     successor link passes the windowed admission gate (D226: its
     `commit_ts` equals the departure instant AND falls inside its
     own attributed window), and the certificate advances to that
     instant (`:3381-3393`) — so the lineage itself returns to a key
     it vacated earlier, which is D66's approved semantics (two
     separately committed updates `2->3->2` fold as one in-place
     UPDATE or no-op; an attributable `2->3->4->3` return folds
     identically). A FOREIGN row's departure is never attributable:
     its departed birth differs from the running certificate, the
     pairing index never serves it to this walk, and the claim then
     finds no row born at the chain instant and fails closed — the
     unproven cross-transaction reincarnation posture is unchanged.
     Termination holds with repeated keys: `stmt_seq` strictly
     increases on every pairing-followed hop within the committing
     transaction's statement count, the commit instant strictly
     increases on every cross-transaction hop and same-key tail
     (one transaction, one commit instant over the committed
     stream), and the hop cap bounds the whole walk regardless. A
     true multi-row key ROTATION (distinct rows exchanging keys)
     still resolves each row's own walk — recorded pairings cannot
     cross rows, and a cross-transaction hop binds only the
     lineage's own landed version — and the fold planner rejects
     the exchange as a rotation cycle (D57).

Recovery rule (r110, D227 — amends r109's D226 rule): an unstamped
`commit_ts` is a pending intent, healed from evidence the protocol
already retains. A candidate link whose `commit_ts` is NULL takes
this rule before the admission gate evaluates it, and every outcome
is exactly one row of a single table — heal, inherit, exclude, or
abort — keyed by the identity class of the link's `old_row_id` and
the completeness of the candidate window's collected stream.

Identity classes. The `old_row_id` of a committed link names either
a STREAM-BOUND identity — a durable committed row version (a
pre-existing row's durable id, or any version the committed stream
itself carries) — or a TRANSACTION-LOCAL identity — a version the
recording transaction itself produced and netted inside that same
transaction: a workspace-segment placeholder (the harvested
pre-commit identity), or an uncommitted S3 object identity (the row
was flushed to an uncommitted CN object by an intra-transaction S3
flush, and its later same-transaction delete was DIVERTED —
`pkg/vm/engine/disttae/txn.go:2544-2569` moves it to the per-block
delete list and shrinks it out of the outgoing batch, the object is
rewritten minus the row at commit, and NO tombstone is ever
published). Both transaction-local shapes leave no trace in the
committed stream, and the S3 shape carries no workspace-segment
mark — the r109 rule misclassified it as stream-bound and excluded
its link on a missing death that proves nothing (the r109 review's
P1).

Outcomes for a NULL-stamped candidate attributed to window `W`:
(a) HEAL (stream-bound death): the candidate's `old_row_id` version
has exactly one death in `W`'s collected stream. That version dies
exactly once in its physical table's committed stream (§6.2: a rowid
dies once), and I7 ties the link to the very commit that killed it,
so the death's instant IS the recording transaction's commit
instant: `commit_ts` := that instant, and the admission gate
evaluates it like a stamped value. The lookup rides the same
protected per-window user-table stream the walk already collected —
recovery adds no reader class and no storage dependency.
(b) INHERIT (transaction-local identity): the candidate's
`old_row_id` is a workspace placeholder, or it is an uncommitted S3
object identity the walk reached through the transaction's own
recorded continuity — D228's persisted `succ_link_id` pairing (the
same-transaction continuation above: exact `link_id` point probe,
scope equality by `txn_id`/`write_ts`, D18/D26, key match and
strictly increasing `stmt_seq`). A missing death carries NO window
information for this class — the row never publishes a tombstone no
matter when its transaction commits — so window absence is never
inferred from it.
The candidate inherits the commit instant of any admitted link of
the SAME transaction (one transaction, one commit instant): a
stamped sibling, a sibling healed by (a), or the running chain's
anchor binding. The established continuation checks and the
per-(table, window) attribution are preserved unchanged: after
inheriting, the candidate still faces the two-condition gate against
its OWN attributed window, so an inherited instant that falls
outside that window excludes the candidate exactly as a stamped
mismatch does.
(c) EXCLUDE (proven window absence): the candidate's `old_row_id`
is stream-bound, `W`'s collected stream is complete (NF5
certified), and the version has NO in-window death. A complete
stream records every committed in-window death, so the recording
transaction provably did not commit this move inside `W` — the
candidate is excluded before identity comparison, never loud, and
never heals INTO a window. This is the single outcome for the
shape (the r109 review's P2): an unstamped successor committed
after a historical endpoint (its linked version dies after the
window's end) and an unstamped cross-fork straddler both exclude,
so the valid in-window move the walk already holds still cascades —
exactly the stamped path's verdict at the terminal claim exclusion.
(d) ABORT LOUDLY (incomplete or corrupt evidence): `W`'s stream or
probe is truncated or disabled (NF5 completeness not certified); the
linked version shows two or more in-window deaths (corrupt: a rowid
dies once); or the candidate is transaction-local with NO bound
same-transaction sibling (an unbound netted chain — nothing proves
when its transaction committed, and death-absence proves nothing
for this class). All are unprovable provenance failing closed
through D51/D56 — never a wrong fold, never a silent legacy
downgrade.
(e) RE-STAMP: a candidate healed by (a) or inherited by (b) is
re-stamped in the catalog by one small bounded internal transaction
over its exact `link_id` set, so later resolutions read the durable
value; re-stamp failure is harmless — the value stays recomputable
from the retained stream and the transaction's own recorded
continuity.
(f) POSTURE: because every heal source is the protected stream (D13
lifetime) or the transaction's own recorded continuity, a stamping
failure, cancellation, CN crash in the stamping window, or lost
commit response can no longer strand an otherwise valid branch:
committed moves keep resolving exactly — through workspace-netted
and S3-netted chains alike — and loud aborts remain only for row
(d)'s genuinely incomplete or ambiguous evidence. A rolled-back
move commits no links and no stream entries (I7), so no provenance
outlives its move and nothing recovers toward it.

Soundness argument: the anchor identifies one physical row version and
the anchor tombstone identifies the one transaction that deleted it;
windowed commit-exact admission (D226) proves the followed link was
recorded by THAT transaction AND inside that transaction's own
window of this lineage — the preserved commit instant equals the
tombstone's and falls inside the candidate pool's attributed window,
and at one committed instant at most one transaction deletes a given
row version or births a given key (F1+F2) — so a link can no longer
splice onto a departure its recorder never authored: an ancestor
straddler's out-of-window link is excluded, and a sibling branch's
independent same-transaction DELETE/INSERT is proven link-less and
keeps DELETE semantics — even when ONE transaction authored both the
ancestor move and the sibling's pair in a single commit, because the
ancestor's segment of this lineage ended before that commit (the
r107 review's P1 counterexample and the r108 review's P1
one-transaction counterexample).
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
same-transaction netted generation through the continuity credential
the recording transaction's commit-time fixup paired and persisted
(D228's `succ_link_id` — the workspace-placeholder identity match
demoted to a cross-check, the inferred key-chain acceptance retired). Links of any *other* row holding the
same key never match the departure evidence (their own births differ
from `T`), so an impostor occupant (NF1), its own moves, and any
same-key reincarnation cannot be followed or claimed. Every unprovable
shape (NULL identity mid-chain, ambiguous successor, cycle, missing
stream identity evidence, duplicate births at `T`, duplicate lineage
departures, probe budget exhaustion) degrades to the legacy
delete+insert replay, which stays loud on referenced parents. NULL `commit_ts` is no longer one of
the loud shapes (r109, D226; r110, D227): it is a durable
stamp-pending intent, and the recovery rule above resolves it to
exactly one outcome before any verdict is reached — heal from the
unique in-window death, inherit across the transaction's own netted
generations (workspace-placeholder and uncommitted-S3 identities
alike), exclude on proven window absence, or abort. A loud D51/D56
abort follows only for the table's abort row — a duplicate in-window
death (a corrupt stream), a truncated/disabled stream, or an unbound
transaction-local candidate — while a stream-bound candidate with no
in-window death on a complete stream is EXCLUDED as proven
out-of-window history, so an unstamped successor committed after a
historical endpoint or an unstamped cross-fork straddler never blocks
the valid in-window move the walk already holds: a transient stamping
failure, its cancellation, a CN crash in the stamping window, or a
lost commit response can never permanently strand a committed move,
and the verdict still never guesses author-or-straddler.

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
- Every provider-window catalog read stays on the apply statement's
  CURRENT snapshot (the r105 unanchored posture; r106's D223 snapshot
  anchoring and r107's D224 membership screening are both withdrawn
  before implementation — see §9.3's storage-version retention audit):
  `write_ts` is the recording transaction's CREATE time, so a
  straddling transaction created before a window ended but COMMITTED
  after it remains VISIBLE to the probe — but visibility no longer
  decides the verdict, and no commit-membership screening is needed.
  The probe preserves each candidate's per-(physical table, window)
  attribution (D226): a link probed for one hop enters only the
  candidate pool of the window whose collected stream carries that
  hop's departure tombstone, and conflict checking happens only among
  the admitted candidates of that one pool. Every fold verdict gates
  on BOTH preserved facts (D225's equality, D226's membership): a
  candidate may start or continue a chain only when its `commit_ts`
  equals the departure tombstone's instant AND falls inside its own
  attributed window's `[start, end]`. Equality alone does not imply
  membership — the departure instant is a collected stream entry of
  the ANCHOR's window by construction, while the candidate's own
  window is a different segment when one transaction spans several
  generations in a single commit (an RC transaction begun before a
  fork can move a row on the parent and independently DELETE/INSERT
  on the child, committing both at one instant: the parent window
  ended before that commit, so condition (i) excludes the parent link
  even though condition (ii) matches — the r108 review's P1
  counterexample); conversely a straddler whose commit instant lies
  outside its window fails (ii) for any in-window departure. Either
  failure excludes the candidate BEFORE any identity comparison or
  arrival attribution: it can neither bind nor poison an anchor on an
  inherited row identity. Zero admissible candidates is proven
  absence (NF5 completeness unchanged); exactly one binds; two or
  more DISTINCT admitted identities fail closed loudly — at one
  committed instant at most one transaction deletes a given row
  version (§5.2 F1+F2), so admitted duplicates are genuinely
  ambiguous catalog evidence. A needed candidate whose `commit_ts`
  is NULL first takes the §5.2 recovery rule (D227's single outcome
  table) — the commit instant is re-derived from the linked version's
  unique in-window death, inherited across the candidate's own
  transaction for a transaction-local netted identity (a workspace
  placeholder or an uncommitted S3 object identity reached through
  D228's recorded `succ_link_id` pairing; a missing death
  carries no window information for either, so absence is never
  inferred from it), or the candidate is excluded as proven
  out-of-window history when its stream-bound version has no
  in-window death on a complete (NF5-certified) stream; a healed or
  inheriting candidate is then admitted or excluded by the same
  two-condition gate — so only an UNRECOVERABLE NULL (a duplicate
  death, a truncated/disabled stream, or an unbound transaction-local
  candidate) aborts loudly through D51/D56, never a silent legacy
  downgrade (r108 review P2, r109 review P2). There is no single-candidate exemption — every
  followed link presents the same proof (r107 review P1). The D156
  scope-to-commit capture is retired from every resolution path: its
  remaining r100 role, arrival-tie attribution, now reads the same
  preserved column under the same windowed gate (a predecessor move
  justifies an arrival exactly when its `commit_ts` equals the
  arrival instant AND falls inside the predecessor's own attributed
  window; an unhealed NULL stays undecidable per instant; distinct
  justifiers stay ambiguous — all under the unchanged whole-window
  completeness requirement), so no verdict anywhere consults the
  link catalog's reclaimable committed change history (r107 review
  P2). The recovery rule's stream lookups ride the same protected
  per-window user-table change stream the walk already collected, so
  they add no reader class and read no historical catalog state.
  Windows with an empty end timestamp (unit fixtures, the MaxInt64
  endpoint fallback) keep the "read to now" posture, and walk
  fixtures fabricate links with their intended commit instants (a
  fixture may leave `commit_ts` NULL to prove the recovery lanes and
  the unrecoverable loud lane); the catalog-existence check per
  owner account is unchanged (D220 (B.4)), and the PICK key-closure
  probes keep the wider unanchored read and never consult
  `commit_ts`, because over-admission only widens the dependency-key
  set while every fold decision stays behind the commit-gated walk.
- Pairing loads links on demand: per tombstone chunk, prefetch per window
  over `link_id` prefix ranges for the chunk's keys; cross-transaction
  continuation hops probe per key lazily and cache, and a D228 recorded
  same-transaction continuation is one exact `link_id` equality probe
  for the persisted `succ_link_id` (plus the per-key probe feeding the
  NULL-pairing cross-check). Negative answers are remembered per
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
short of that (impostor re-occupation, ambiguous successors) still
fails closed to the loud legacy replay; a straddling transaction whose
link commits outside the collected windows is excluded by the
windowed commit-exact gate (§5.2, D226) — and so is a link whose
commit instant equals the departure yet lies outside the candidate's
own attributed window — proven absent for that anchor, never a
poison; an unstamped link earns the same exclusion from the recovery
rule's proven-window-absence row (D227), stamped and stamp-pending
candidates landing on one verdict.

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
  shape still folds single-shot by the commit-timestamp tie (D14), and
  its same-transaction netted continuations ride D228's persisted
  `succ_link_id` pairing — the fixup pairs by key and statement ordinal
  over the converged post-prune set, so a missing replacement identity
  no longer degrades the chain.
- Provenance stamping (D225/D226, recovery by D227) is best-effort:
  its failure, cancellation, a CN crash in the stamping window, or a
  lost commit response never changes the user-visible COMMIT outcome —
  the stamping callback reports no error into the completion event,
  whose path otherwise joins callback errors into the post-commit
  result — and never strands provenance, because a NULL `commit_ts`
  resolves through the §5.2 recovery rule's single outcome table at
  resolution: heal from the unique in-window death, inherit across the
  transaction's own netted generations (workspace placeholders and
  uncommitted S3 object identities alike — their diverted deletes
  publish no tombstone, so absence is never inferred from a missing
  death), exclude on proven window absence, abort only on incomplete
  or corrupt evidence. A rolled-back move commits no links and no
  stream entries (I7), so no provenance outlives its move and nothing
  recovers toward it.

### 6.3 Zero-match guard as loud failure

The guard converts r1's silently-successful no-op UPDATE into an abort
covering: destination row deleted between diff and apply (impossible in
one txn but defensive), destination lineage unresolved by I4 (sibling
gap in §2), and catalog/stream inconsistencies. The guard runs in the
MERGE/PICK transaction, so abort semantics are I7-atomic.

## 7. Compatibility, mixed-version, restore

- Upgrade: the r2 table and its retention-ledger sibling are created
  idempotently (`CheckTableDefinition` guard) by forward tenant-upgrade
  placement — D149 as landed on PR #31629: upstream #31497 folded and
  removed the unpublished 4.0.8-4.0.17 handlers, upstream #31489
  published and then upstream #31488 deleted the unpublished
  4.0.8-4.0.19 identities, folding every unpublished migration into the
  unpublished 4.0.7 handler (docs/design/20261028-catalog-upgrade-407.md),
  whose contract forbids registering those identities as upgrade steps.
  The two catalog creations therefore append to the 4.0.7 tenant entry
  list and raise its offset (41 -> 43), strictly forward of every
  supported upgrade source, all earlier migrations untouched. Main-era
  clusters stamped 4.0.7 at offset 41 are incompatible unreleased
  layouts and are rejected by the offset check, per the same contract.
  New accounts get the tables from predefined DDL. Nothing backfills
  history; links accrue from first enabling binary onward.
- Mixed version: a pre-fix CN never writes links (no writer), and never
  reads them (no consumer); a fixed CN facing a tenant whose table is
  missing (upgrade not yet applied) disables link writes per statement
  batch with a one-time warning and keeps legacy replay. Schema drift can
  only exist on clusters bootstrapped by mid-development binaries; both
  sides degrade safely: the workspace back-fill leaves a batch without
  `new_row_id` untouched, and rows whose identity stayed NULL fold only
  by the commit-timestamp tie (D14) — same-transaction netted
  continuations stay unprovable (I1). The D228 credential columns ride
  the same unpublished upgrade entry and predefined DDL, so a supported
  cluster never holds pre-credential rows; under schema drift a row
  lacking `succ_link_id` keeps its netted continuations unprovable
  (I1), while a D228-era NULL `succ_link_id` always means TERMINAL —
  the fixup computes the pairing unconditionally at commit, so NULL
  never means unknown.
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

Storage-version retention audit (added r107, answering the independent
design review of r106; rewritten r108 after the independent design
review of r107): the protocol's catalog readers divide into
version-dependency classes.

- (a) Provenance probes — whole-window scans, per-key overflow fallback,
  LIMIT-1 existence probes, and the PICK closure — read the apply
  statement's CURRENT snapshot. They depend only on the catalog's LIVE
  logical state, which this section's retention rule already guarantees:
  a live branch lineage's links are never deleted while a live branch
  diff can still fold them, so no historical storage version is
  required for their correctness.
- (b) The D222 owner-resolution probe reads `mo_tables` at
  `{MO_TS = transition snapshot}`. `mo_tables` is one of the three core
  catalog tables that TN's snapshot application protects for EVERY
  snapshot (verified at the reviewed head: the all-snapshot flatten set
  covers mo_database/mo_tables/mo_columns only), so the branch-protect
  snapshots already cover its historical versions.
- (c) REMOVED (r108, D225): r107's third class — the D156 scope-to-
  commit capture reading the link catalog's COMMITTED CHANGE STREAM —
  is retired from every resolution path. The r106/r107 reviews verified
  that this history is unprotectable: table-level snapshots propagate
  only to non-system tables of the same database (the per-table
  application skips system tables, so the branch-protect snapshot on
  the user table never reaches `mo_data_branch_update_link`), the
  mo_catalog PITR channel carries only user-created PITRs
  (`pitr.MinTS()`), and with no account/cluster snapshot or PITR
  `ObjectIsSnapshotRefers` permits reclamation of the catalog's retired
  objects. No CN-expressible mechanism can establish retention for
  those versions — creating a PITR would be a broader, user-visible
  protection this design rejects, and extending TN's protected catalog
  set or propagation rules is a TN-side change outside this PR's
  authorized scope. D225 removes the need instead of protecting the
  versions: the membership/binding evidence travels INSIDE the live
  link row (the preserved `commit_ts`, and D228's persisted
  `succ_link_id`/`stmt_seq` continuity credential), whose retention
  this section's rule already guarantees for the protected lineage's
  lifetime (D13), and the commit-completion stamping step that fills
  `commit_ts` is a current-snapshot point update needing no historical
  version.

Consequences recorded for the protocol: r106's D223 anchored reads and
r107's D224 membership screening are both withdrawn before
implementation; the revised reader (D225, windowed by D226) keeps
every probe in class
(a), reconstructs no historical catalog STATE, and leaves no reader in
the reclaimable class at all. After catalog-object rewriting and
reclamation, while the branch lineage's links and its protected
user-table stream stay live, every provenance verdict keeps resolving:
the §10 lifecycle oracle requires the exact MERGE/PICK cascade
assertions to hold after reclamation and restart on otherwise valid
retained branches (r107 review P2). Loud aborts remain only for
genuinely invalid or incomplete evidence — a truncated or disabled
probe (NF5), duplicate admitted identities (corruption), or an
UNRECOVERABLE stamp-pending candidate (recovery row (d): a duplicate
in-window death, a truncated/disabled stream, or an unbound
transaction-local candidate) — never a wrong fold and never a silent
downgrade to legacy replay; a stamp-pending candidate whose
stream-bound version provably did not die in the window is EXCLUDED
as proven out-of-window history, so an unstamped successor committed
after a historical endpoint never blocks the valid in-window fold.
A NULL `commit_ts` is not in the loud list (r109, D226; r110, D227):
the recovery rule resolves it from the same protected per-window
user-table stream the walk already reads plus the transaction's own
recorded netted-generation continuity — D228's persisted
`succ_link_id` pairing travels inside the live link row (class (a)),
so chain resolution after catalog rewriting, reclamation and restart
follows the same recorded pairs and adds no reader class — keeping
this section's lifecycle guarantee: a transient stamping failure, its
cancellation, a crash in the stamping window, or a lost commit
response cannot degrade a valid apply, through workspace-netted and
S3-netted chains alike.

## 10. Validation map

Every invariant maps to the cheapest deterministic evidence:

| Invariant / behavior | Evidence (cheapest) |
| --- | --- |
| I1 identity: commit-tie claims (unique vs duplicate births at the anchor ts), impostor at intermediate key (with/without continuation), impostor's own links never followed, NULL identity folds single-shot and chains netted hops ONLY through the recorded `succ_link_id` pairing (D228 — an unrecorded departure is never followed), workspace-placeholder and S3-netted same-txn hops alike riding the one recorded pairing, contradictory departure, ambiguous successors, proven same-transaction returns through the recorded pairing (the equal-certificate homecoming resolves, a strictly-backward certificate fails closed — D229), proven cross-transaction returns through the attributable departed-birth-pinned hop (a strictly newer certificate resolves — D230; a foreign row's departure is never served and fails closed), cycle/reincarnation, hop cap, same-key tail (single and chained stable-key tails, tail after a cross hop then linked continuation, tail never follows a foreign departure's successor, plain-delete and separate-txn re-insert tails fail closed, duplicate lineage departures fail closed) | UT of the ts-tie walk over fabricated providers (`TestResolvePkMoveChain`, `TestPkMoveSuccessorOf` in `pkg/frontend/data_branch_pk_move_link_test.go`) + arrival-tie attribution re-pointed to the preserved commit instant under the windowed gate (predecessor justified iff `commit_ts` equals the arrival instant AND falls inside the predecessor's own attributed window; unhealed NULL undecidable per instant; distinct justifiers ambiguous) — the retired D156 capture's coverage |
| I1 live equivalence: MERGE/PICK of genuine single + multi-hop moves cascades ON UPDATE CASCADE; impostor at a vacated key survives as its own insert | live smoke battery + BVT `merge_foreign_key_pk_move`, `pick_foreign_key_pk_move` |
| I2 counterexample: independent DELETE/INSERT (autocommit and explicit `begin; ...; commit;`) keeps DELETE semantics, FK error, atomic state | BVT tx sections + live battery S1/S7 |
| I3 precedence: provenance folds before legacy same-key matching | UT ordering witness + BVT scenario |
| I4 conflict lineage: both-moved ACCEPT/SKIP/FAIL; dst-deleted vs moved at every mode, MERGE and PICK | live matrix + BVT both-moved sections |
| New-key occupant conflicts (NF2): independent destination row at the move's NEW key with referencing children — FAIL aborts, SKIP keeps destination, MERGE-ACCEPT replaces victim inline, PICK-ACCEPT routes victim through the conflict pipeline | live battery NK scenarios; legacy nil-conflict shape covered by existing BVT |
| I5 lineage selection (NF4): reused intermediate keys admit only the selected lineage; unrelated parent and child rows unchanged | pair consumer UT (`TestPairPkMovingTombstones/PICK_dependency-only_anchor_never_folds`) + live reused-key PICK regression |
| I5 windows (NF6/NF7): multi-generation branches fold through ancestor tables; retired intermediate tables still resolve; historical endpoints and PICK BETWEEN read only in-window moves | live battery MG/HW scenarios + probe SQL bound assertions (`TestPkMoveLinkProbeBudgetAndBounds`) |
| I5 staleness: a move after the selected snapshot endpoint never contaminates the historical apply | live battery HW scenario |
| I5 lineage visibility (D226): a transaction begun before a fork but committed after its lineage window ended stays visible to the unanchored probe yet is excluded by windowed commit-exact admission (`commit_ts` != the departure instant), so its link can neither bind nor poison a descendant generation's anchor on the same inherited row identity (the reviewed straddler schedule); a candidate whose `commit_ts` EQUALS the departure instant yet falls OUTSIDE its own attributed window is likewise excluded before identity comparison or arrival attribution (the r108 review P1's one-transaction cross-branch schedule); a straddler committed INSIDE its window still resolves; two DISTINCT admitted links sharing one row identity fail closed loudly; a NULL `commit_ts` resolves by the D227 outcome table (heal, inherit, exclude on proven window absence) and only an unrecoverable NULL fails loudly; there is no single-candidate exemption | multi-generation/sibling fabric UT (the reviewed two-candidate straddler: out-of-window ancestor excluded, in-window child binds and resolves) + commit-gate UTs (admit on equal instant inside the attributed window, exclude on instant mismatch, exclude on window mismatch even with equal instant, NULL heals via the unique in-window death, NULL with proven window absence excludes — the unstamped future-successor and cross-fork straddler forms — transaction-local netted identities inherit, unbound transaction-local loud, duplicate admitted poison, no singleton exemption, same-transaction netted hops inheriting the anchor's binding) + live BVT merging sibling branches with a cross-fork straddling pk-moving transaction, MERGE and PICK twins |
| I2 lineage visibility (D226; the r107 review P1's and r108 review P1's demanded oracles): a sibling branch's independent same-transaction DELETE/INSERT that shares an inherited row identity with an out-of-window straddler's recorded move is proven link-less — the out-of-window candidate is rejected by windowed commit-exact admission, so MERGE and PICK keep DELETE semantics, the FK implicit RESTRICT check rejects loudly, and the destination parent/child rows stay unchanged (no UPDATE cascade onto the unrelated row) — including the r108 review's harder shape where ONE RC transaction begun before the fork performs BOTH the ancestor move and the sibling's independent DELETE/INSERT in a single commit, so the candidate's `commit_ts` equals the departure instant yet lies outside its own attributed window | live BVT MERGE and PICK twins of the straddler + independent DELETE/INSERT schedules (the two-transaction singleton form and the one-transaction form) asserting DELETE semantics, FK rejection, and unchanged destination parent/child rows + fabric UT of the walk verdict (candidate excluded on window membership; zero admissible -> proven absence -> legacy replay) |
| I6 bounds (NF8): probe SQL carries `write_ts <= endPhys` and `LIMIT budget+1` on the apply statement's current snapshot (D225: no historical anchor, no membership capture); overflow disables the provider, releases caches, and never caches a truncated prefix; cache over budget disables before SQL; mid-walk truncation fails closed (NF5) | `TestPkMoveLinkProbeBudgetAndBounds` (SQL capture + truncation + disable-before-SQL + injected cap-boundary walk + `commit_ts` decode assertions) |
| Storage-version lifecycle (r107/r108 reviews): with table-scoped branch-protect snapshots ONLY — no account/cluster snapshot, no PITR — force catalog compaction/checkpoint rewriting and object reclamation, restart, then run the exact MERGE/PICK pk-move cascade assertions: no resolution-path reader depends on reclaimable catalog change history (the D156 capture is retired), so while the lineage's links and its protected user-table stream stay live every cascade assertion holds exactly after reclamation and restart — including links whose `commit_ts` was left NULL by an injected stamping failure, which heal from the protected stream and still resolve exactly; loud aborts are asserted only for genuinely incomplete evidence (a truncated protected stream, duplicate admitted identities) — never a wrong fold, never a silent downgrade to legacy delete+insert replay | live lifecycle battery (catalog rewrite + reclamation + restart + exact MERGE/PICK cascade assertions on otherwise valid retained branches, with unstamped links healing to exact cascades; truncated-stream fixtures assert the loud-abort posture) |
| Commit/stamping failure and crash protocol (D226; the r108 review P2's demanded checkpoints): stamping-failure injection, stamping cancellation, a crash between commit and stamping (fault point), a lost commit response (`FinalizeCommitWithUnknownResult`), and restart — each followed by exact MERGE/PICK cascade assertions for the committed move, because the NULL stamp heals from the protected stream at resolution; rollback leaves zero links and no provenance; the user-visible COMMIT result equals the user transaction's own outcome in every injection | live fault-injection battery over the completion hook with exact MERGE/PICK cascade assertions + fabric UTs driving the walk over unstamped fixtures + unknown-result finalize UT |
| Stamp recovery over netted S3 generations (D227, continuity by D228; the r109 review P1's demanded variants): a pk-move chain whose intermediate generations live in uncommitted S3 objects (their same-transaction deletes diverted, no tombstone published) keeps resolving exactly when the chain's links are left NULL-stamped — the first hop heals from its durable old version's unique in-window death and every netted hop inherits the transaction's bound instant through the persisted `succ_link_id` pairing (a missing death is never read as window absence for this identity class); the chain fold lands on the exact final key, and an unrelated row reinserted at the vacated intermediate key keeps its own identity as a plain insert | live MERGE/PICK twins of BOTH S3-chain scenarios — the plain 2->1000->1001 chain and the chain plus an unrelated reinsert at the vacated key (the `fj/cn/flush_small_objs` fixtures) — each under stamping-failure injection and under a crash between commit and stamping, asserting the exact child key (the cascade lands on the lineage's final key) and the preservation of the unrelated intermediate row (its own payload survives as a plain insert) + fabric UTs driving the walk's recovery over unstamped netted S3 identities (inherit when chained through the recorded pairing, exclusion never inferred from a missing death, unbound transaction-local loud) |
| Recorded continuity over uncommitted same-key holders (D228; the r110 review P1's demanded variants): one transaction inserts an unrelated row B at the move's target key, spills it to an uncommitted S3 object (`fj/cn/flush_small_objs`), moves B off that key (its delete diverted, no tombstone published), then moves A onto the key with an identity-less S3 replacement — over the converged post-prune set the fixup pairs A's arrival with NOTHING (B's departure statement precedes A's arrival statement), so A's chain ends at the target key, B keeps its own identity at its final key, and the walk never follows B's earlier same-key departure — stamped and stamp-pending alike | live MERGE and PICK twins of the uncommitted vacate-before-arrival schedule, each in a stamped form AND a stamp-pending form (stamping-failure injection), asserting A's child cascades onto A's row at the target key and unrelated B remains at its own final key as its own plain insert + fixup UTs (arrival-to-departure pairing by statement ordinal over the converged post-prune set; an earlier departure never pairs with a later arrival; terminal NULL recorded unconditionally) and walk UTs (only the recorded `succ_link_id` is followed; scope equality, key match and strictly increasing `stmt_seq` verified; identity-match and tombstone-screen contradictions fail loudly; a NULL pairing coexisting with a retained later same-transaction departure fails loudly) |
| Proven same-transaction returns through the recorded pairing (D229, restoring D70 consistency; the r111 review P2's demanded oracles): one transaction moves a row `2->3->2` (or longer) — over the converged post-prune set the fixup pairs the first link with the second (the arrival at 3 pairs with the departure of 3 at the greater statement ordinal) and records terminal NULL on the last, the walk follows the verified pairing onto the already-seeded anchor key (an equal birth certificate, valid under the parser's strictly-backward rejection), the terminal claim accepts the re-birthed row born at the chain instant, and the fold lands at the original key: a CHANGED payload folds an in-place native UPDATE, an UNCHANGED payload folds an in-place no-op — stamped and stamp-pending alike; a strictly backward certificate on a pairing hop fails closed, the hop cap still bounds, and a genuine multi-row key rotation still aborts at fold planning | the existing shipped success oracles carried into the persisted-pair plan — the walk UTs of the one-transaction homecomings (`pkg/frontend/data_branch_pk_move_link_test.go:1420-1469`, the `2->3->2` and `2->3->4->2` recorded-hop cases) re-pointed to the persisted `succ_link_id`/`stmt_seq` pairing, plus live MERGE and PICK twins of the changed-payload return (an in-place native UPDATE at the original key) and the unchanged-payload return (an in-place no-op) of `test/distributed/cases/git4data/branch/merge/merge_foreign_key_pk_move.sql:1514-1561` and `test/distributed/cases/git4data/branch/pick/pick_foreign_key_pk_move.sql:1088-1133`, each in a stamped form AND a stamp-pending form (stamping-failure injection) with the persisted pairing asserted (the first link pairs to the second, the last records terminal NULL) + walk UTs (a verified pairing hop landing on the lineage's own previously visited key with progressing `stmt_seq` resolves; a strictly-backward certificate fails loudly; the hop cap still bounds; the two-row rotation exchange still aborts at planning) |
| Proven cross-transaction returns under the windowed admission gate (D230, restoring D66 consistency; the r112 review P2's demanded oracles): SEPARATELY committed moves return a row to a key the lineage itself vacated — `2->3` committed at T1 then `3->2` committed at T2 > T1, and the longer attributable `2->3->4->3` form — the departed-birth-pinned pairing index serves the returning hop (the departure tombstone names the lineage's own landed version), the successor link passes the windowed admission gate (its `commit_ts` equals the departure instant AND falls inside its own attributed window), the strictly newer birth certificate continues the walk, the terminal claim accepts the re-birthed row born at the chain instant, and the fold lands at the vacated key: a CHANGED payload folds an in-place native UPDATE, an UNCHANGED payload folds an in-place no-op — stamped and stamp-pending alike; a strictly backward certificate fails closed, a foreign row's departure at a vacated key is never served to the walk (unproven reincarnation fails closed), the hop cap still bounds, and a genuine multi-row key rotation still aborts at fold planning | the existing shipped success oracles retained — the live MERGE separate-transaction changed-payload return (an in-place native UPDATE at the original key) and unchanged-payload return (an in-place no-op) of `test/distributed/cases/git4data/branch/merge/merge_foreign_key_pk_move.sql:1431-1476` and their PICK mirrors of `test/distributed/cases/git4data/branch/pick/pick_foreign_key_pk_move.sql:1006-1051`, each additionally run in a stamp-pending form (stamping-failure injection) with the window-admitted successor asserted, PLUS new MERGE and PICK twins of the attributable `2->3->4->3` return (changed and unchanged payloads, stamped AND stamp-pending) + walk UTs (a cross-transaction hop landing on a previously visited key with a strictly advancing certificate resolves when the successor is window-admitted; the same hop with the successor outside its attributed window fails closed; a foreign row's departure at the vacated key is never followed; a strictly-backward certificate fails loudly; the hop cap still bounds; the two-row rotation exchange still aborts at planning) |
| Stamp recovery at window boundaries (D227; the r109 review P2's demanded cases): an unstamped successor committed after a historical endpoint, and an unstamped cross-fork straddler begun before the fork and committed after its lineage window ended, are EXCLUDED as proven out-of-window history (stream-bound identity, complete stream, no in-window death) — never loud — so the valid in-window move still cascades exactly | live PICK between snapshots with the future successor's stamping suppressed (the fold still ends at the in-window key and the child cascades there) and its MERGE twin, plus the cross-fork straddling-transaction MERGE/PICK pair with the straddler link's stamping suppressed (the in-window child move still applies as a native UPDATE) + fabric UTs of the outcome table's exclude row against its loud rows |
| Writer protocol: NULL-identity staging; NULL `commit_ts` staging; link batch layout matches the catalog schema; eligibility/fail-closed paths | `pkg/sql/colexec/multi_update/pk_move_link_test.go` |
| Writer atomicity (I7): injected link-write failure aborts the statement; degraded ctx skips silently; healthy write stages exactly one NULL-identity row | `TestInsertUpdatePkMoveLinkRowsWritePath` |
| Identity back-fill, continuity pairing, commit stamping, and stamp recovery (D225/D226, recovery by D227, pairing by D228): harvests map pk bytes to the freshest staged row id; account/table/type filters; NULL and malformed entries skipped; null identity column left untouched on schema drift; the commit-time fixup pairs each retained link's arrival with the same-transaction retained link departing its new key at the smallest later statement ordinal and persists the pairing (`succ_link_id`/`stmt_seq`) unconditionally over the converged post-prune set — a terminal generation records NULL and the pairing is stamp-independent; the commit-completion step observes the TN-assigned commit timestamp and stamps exactly the transaction's staged `link_id` set with the full-precision commit instant; a stamping failure, its cancellation, a CN crash in the stamping window, or a lost commit response leaves `commit_ts` NULL and never alters the user-visible COMMIT result; ROLLBACK stages nothing, pairs nothing and stamps nothing; recovery resolves a NULL stamp by the single outcome table (D227): heal from the linked version's unique in-window death; inherit the bound instant across the candidate's own transaction for transaction-local netted identities (workspace placeholders and uncommitted S3 object identities — a missing death carries no window information for either, so absence is never inferred from it); exclude on proven window absence (stream-bound identity, complete stream, no in-window death — the unstamped future-successor and cross-fork forms); fail loudly only on a duplicate in-window death, a truncated/disabled stream, or an unbound transaction-local candidate; and re-stamp healed links best-effort | `pkg/vm/engine/disttae/pk_move_link_fixup_test.go` + pairing persistence UTs (smallest-later-departure pairing over the converged post-prune set, terminal NULL recorded unconditionally, stamp independence, schema-drift leaves the credential columns untouched) + commit-completion stamping UTs (exact link set, full-precision instant, failure-leaves-NULL, rollback-noop, COMMIT-result unaffected by stamping failure) + recovery UTs (heal in-window death, exclude on proven window absence, transaction inheritance for workspace and S3 identities, unbound transaction-local loud, duplicate-death loud, truncated-stream loud, best-effort re-stamp of the healed link set) |
| Pair consumer: proven anchors fold and claim their replacement (durable-identity and NULL-identity links alike, commit-gated by the preserved `commit_ts`); unproven/dependency-only anchors stay deletes; replacement leaves the stream exactly once | `TestPairPkMovingTombstones` |
| I7 rollback/restart: ROLLBACK removes link rows with user rows; links survive a clean restart and keep resolving | live battery RB/RS scenarios |
| I6 tenancy: a second tenant writes/reads only its own links; a tenant without the table degrades to legacy replay | live battery TI scenario |
| I6 reclaim: DROP TABLE / DROP DATABASE / TRUNCATE remove the table's links; a dropped intermediate generation keeps its links while descendants are live and them both retire when the last descendant drops; global audit shows zero orphans | BVT reclaim probes + live counts + `TestComputeBranchReclaimTableIDs` |
| Upgrade path and idempotence (D149): an existing tenant lacking the tables walks forward into the tail entries of the unpublished 4.0.7 handler and gains `mo_data_branch_update_link` (carrying `commit_ts` and D228's `succ_link_id`/`stmt_seq` credential columns, added in place to the same unpublished entry and the predefined DDL — a tenant that executed a pre-column shape is an unreleased dev layout under the offset hard-error rule) and `mo_data_branch_link_retention` with the §3 schema; a repeated upgrade run is a no-op; a tenant at the completed handler offset selects no further entries; DDL/schema agreement between `predefined.go`, plan TableDef, and writer batch | UT in `pkg/bootstrap/versions/v4_0_7` (`pk_move_provenance_upgrade_test.go`), `pkg/sql/plan`, plus the offset-bump service cases in `pkg/bootstrap` — the schema-agreement half owns the fresh-cluster/new-account predefined-DDL path (bootstrap at the final version, no handler selection), while the entry-registration, idempotence, and upgrade-path cases own existing-tenant reachability; post-upgrade MERGE/PICK pk-move cascade parity runs live on an upgraded cluster |
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
  `pkg/bootstrap` (v4_0_7 upgrade tests), and the service-level cells
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

The 223 numbered design decisions, in revision order. Each entry keeps its
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
- D14: commit-timestamp tie (r4) [made commit-exact by D225: the tie is verified against the followed link's own preserved commit instant, not by instant visibility alone].
- D15 (r5) [refined by D85; commit-exact preservation executed by D225]: cross-transaction evidence must be *preserved*, not just consulted (§5.4).
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
- D70 (r17, review F4) [restated for recorded-pairing walks by D229]: a PROVEN same-transaction return to the original key resolves.
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
- D149 (r41, review F1, refines §7's upgrade placement; landing placement re-derived at r105): the link-table catalog migration moves to a FORWARD tenant-upgrade placement strictly newer than every supported upgrade-source state, because upgrade selection is strictly greater-than; as landed it rides the tail of the unpublished 4.0.7 handler (offset 41 -> 43), since upstream #31488 folded the unpublished 4.0.8-4.0.19 identities into 4.0.7 and forbids their registration as upgrade steps (docs/design/20261028-catalog-upgrade-407.md).
- D150 (r43, review F2, refines D144 part (2), invariant (ii), and planned test (c)): the carry journal preserves every round's undo until that round completes.
- D151 (r43, review F1, refines D144 as clause (v)): EVERY carry admission boundary — boundary-dump origin translation (pk_move_link_fixup.go:2855) and object-transfer re-key (transfer_util.go:475) alike — applies the shared participation rule: with no live staged link rows, the pair-list integrity checks and effective-pair computation still run and the journal append still happens, but the budget refusal is skipped, because a transaction without staged links commits no provenance.
- D152 (r45, implementation review P1, refines D47/D83/D86+D98) [refined by D153+D155]: the historical-retention gate that every link-reclaim probe consults covers ACTIVE PITR scopes exactly like user snapshots, through ONE shared check.
- D153 (r46, design review r45 P2, refines D152/D98/D83/D86/D47) [refined by D154+D159]: links retained under the D152 union for a table that then disappears gain a terminal lifecycle.
- D154 (r47, design review r46 findings P1 + four P2, refines D153): the retained-dead-table lifecycle gets its ownership, discovery, exceptional-path, and restore contracts corrected.
- D155 (r48, design review r47 findings P1 + two P2, refines D152 and D154(C)/(D)/(E)): the retained-dead-table lifecycle closes the sys-owned owner topology, hardens the owner-removal error contract, and splits the restore contract.
- D156 (r50, design answer to r60 review F1 (P1), refines D87's unrecorded-replacement boundary, executes D2/D15's pre-authorized commit-exact direction without a catalog shape change) [retired by D225: the arrival tie re-attributes from the preserved `commit_ts` column, so no resolution path reads the catalog's reclaimable committed change stream]: failed-scope attribution is derived from the link catalog's OWN committed change stream, not from approximations over the data stream.
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
- D223 (r106, 2026-11-02, snapshot-anchored provenance reads; refines D11/D24 window bounds) [SUPERSEDED by D224 before implementation — REJECTED at the independent design review of r106]: PR #31629 review (aunjgr inline, 2026-11-02T09:41:16Z) found that the provider's catalog probes read the apply statement's CURRENT snapshot with only `write_ts <= endPhys` (write_ts is the recording transaction's CREATE ts, per D2), so an ancestor transaction begun before a fork but committed after its lineage window ended — a straddler whose provenance this source lineage never saw — entered `mergeProbedLinksLocked` and poisoned the shared `(old row id, old pk)` identity before stream commit-membership screening could exclude it, aborting a conflict-free merge. The failure is fail-closed only: every manifestation routes through D51's unresolved-verdict abort, and birth-certificate binding bars a misattributed fold, so the defect never produces wrong data. Repair: every provider-window catalog read — the whole-window scan, the per-key overflow fallback, and the LIMIT-1 existence probe — is snapshot-anchored at the window's full `end` types.TS via `{MO_TS = '<phys-logical TS>'}` (the D222 anchor form), so the probe exposes exactly the links committed at or before the window's end; the missing lower bound on `write_ts` is unchanged, since a transaction created before a window's start but committed inside it remains visible at the anchor. Sufficiency without per-window cache attribution: the link_id prefix range already restricts each probe to the window's physical table, and on a linear lineage an in-window move retires its (row id, old pk) occupant from every descendant generation's inherited state, so after anchoring two DIFFERENT visible links can share one row identity only through genuinely ambiguous catalog evidence (duplicate committed rows naming one inherited identity with different successors) — the poisoning path stays fail-closed for exactly that case. An empty `end` (unit fixtures, the MaxInt64 endpoint-relation fallback) keeps the unanchored posture, where "now" is the intended horizon. Anchored existence: a window whose end predates the link catalog's creation in its owner account degrades to complete absence for that window only (the anchored `mo_tables {MO_TS = window.end}` existence check extends D220 (B.4)), so historical endpoints preceding the upgrade read as empty instead of erroring on the absent catalog. The PICK key-closure probes keep the wider unanchored read: closure windows carry endPhys only, and over-admission there widens only the dependency-key set while every fold decision stays behind the provider's anchored, membership-screened walk. The rejected alternative — per-(table, window) attribution of the byRow cache with conflict checking deferred until commit membership is established — repairs the same defect but reworks the cache keying and every verdict accessor for no additional behavioral guarantee. Supersession record: the independent design review of r106 (the issue-fix-loop controller's local reviewer, 2026-11-02, REQUEST_CHANGES, blocking P2) verified that the anchored reads require retained historical storage versions of the link catalog that no snapshot/PITR channel covers (see §9.3's storage-version retention audit), so catalog-object rewriting and reclamation could remove the very version an anchored read queries while the logical links remain live; no CN-expressible protection mechanism exists, and establishing one in TN is outside this PR's authorized scope. D223 never shipped. D224 adopts the per-window attribution alternative this entry had rejected, because the retention finding reverses that trade-off: the cache/verdict rework buys the decisive guarantee that no provenance verdict depends on reconstructing a historical catalog state.
- D224 (r107, 2026-11-02, per-window provenance attribution with commit-membership-deferred conflict checking; supersedes D223; refines D11/D24 window bounds) [SUPERSEDED by D225 before implementation — REJECTED at the independent design review of r107]: repairs the same reviewed straddler defect (aunjgr inline, 2026-11-02T09:41:16Z) without any historical catalog-state dependency. (a) Reads: every provider-window catalog probe keeps the unanchored current-snapshot posture with `write_ts <= endPhys` and the row-budget LIMIT; no `{MO_TS}` anchor is placed on the link catalog. (b) Attribution: probed links are cached WITH their (table, window) attribution — one window per physical table id at construction — instead of merging into a single shared `(old row id, old pk)` identity map at probe time; the merge no longer poisons. (c) Screening before comparison: at verdict time each candidate is first tested for commit membership — the D156 scope-to-commit capture, built from the window's own committed change stream under its certify-complete-or-poison-wholesale contract, answers whether the candidate's recording transaction committed inside its own window's collected [start, end]; a candidate committed outside every collected window of this source lineage (the straddler) is excluded before identities compare, so it can neither bind nor poison an anchor. Membership is unique by construction: a transaction commits exactly once, and the link's table id is the updated table (D4), so one candidate admits through at most one collected window. (d) Comparison: only membership-admitted candidates enter the identity comparison — zero candidates is proven absence (completeness semantics of NF5 unchanged), exactly one binds the anchor, and two or more DISTINCT identities poison (fail closed), which on a linear lineage is genuinely ambiguous catalog evidence: an in-window move retires its (row id, old pk) occupant from every descendant generation's inherited state. (e) Failure posture: a window whose membership evidence cannot be certified complete poisons that window's screening — the D156 wholesale rule extended from the arrival tie to the anchor conflict check — so reclamation of the catalog's change history (§9.3 class (c)) degrades to the loud D51/D56 abort, never a wrong fold and never a silent downgrade to legacy replay. (f) Cost: the capture arms lazily for anchor screening exactly as it already does for the arrival tie, bounded by the collected windows' ranges (D156/D157: one pass per provider generation, budget-limited, cancellation-observed); a verdict holding a single candidate may follow it directly because the walk's stream-side birth-certificate binding already bars a wrong fold — screening is mandatory exactly where identities conflict and where an arrival tie is consulted. (g) Fixtures: empty-end windows keep the "read to now" posture, and a provider without a capture source keeps the adopted complete-empty index whose candidates pass unscreened (r50 (D)), so the walk fixtures are untouched; the catalog-existence check per owner account (D220 (B.4)) and the wider unanchored PICK key-closure read (over-admission widens only the dependency-key set; fold decisions stay behind the screened walk) are unchanged. Supersession record: the independent design review of r107 (the issue-fix-loop controller's local reviewer, 2026-11-02, REQUEST_CHANGES, blocking P1 + P2) verified (P1) that (f)'s single-candidate exemption admitted an unscreened out-of-window candidate: a singleton ancestor straddler's link splices onto a sibling branch's independent same-transaction DELETE/INSERT sharing the inherited row identity, because the claim bound by commit instant alone and never bound the candidate's recording transaction to the deleting transaction — the destination count guard then passes and a wrong UPDATE cascade executes; and (P2) that (c)'s membership screening made valid anchor resolution depend on the D156 capture, which reads the link catalog's reclaimable committed change history (§9.3 class (c)), so ordinary catalog rewriting/reclamation would strand otherwise valid retained branches — contradicting D13 and leaving the original straddler scenario unresolved after reclamation. D224 never shipped. D225 repairs both findings by preserving the commit instant inside the live link row (executing D15's preservation in its commit-exact form), gating every candidate on it with no exemption, and retiring the capture from every resolution path.
- D225 (r108, 2026-11-02, preserved commit-exact provenance: the link row carries its recording transaction's commit instant; supersedes D224 before implementation; refines D14/D15; retires D156's capture; refines D11/D24 window verdicts) [AMENDED by D226 before implementation: admission gains per-window membership alongside departure equality, per-(table, window) attribution is restored, and the NULL `commit_ts` posture moves from the loud abort to D226's recovery rule; the `commit_ts` column, the CN commit-completion stamping hook, the D156 capture retirement, the equality binding, and the no-exemption posture stand]: repairs the reviewed straddler defect (aunjgr inline, 2026-11-02T09:41:16Z) and both r107-review findings without any historical catalog-state dependency and without any reclaimable-history reader. (a) Catalog: `mo_data_branch_update_link` gains `commit_ts binary(12) null` — the recording transaction's full-precision `types.TS` byte form; physical-only precision cannot order two transactions committed inside one physical instant (the HLC logical component distinguishes them), and the walk compares stream instants with full precision. The column is added in place to the same unpublished 4.0.7 tail upgrade entry and the predefined DDL: no release carries this table, and a tenant that executed a pre-column entry shape is an unreleased dev layout under the existing offset hard-error rule. (b) Writer/stamping: the moving statement stages rows with `commit_ts` NULL (I7 atomicity unchanged — the commit instant is unknowable before TN assigns it); after the recording transaction's successful commit, a CN-side commit-completion step — the `pkg/txn/client` operator completion event already observes the TN-assigned timestamp — runs one bounded internal transaction updating exactly this transaction's staged link rows by their exact `link_id` set, carried from the transaction's bounded staging bookkeeping. Entries are applied before the commit response returns the assigned timestamp, so no CN write can ride the same commit batch; the second transaction is small and attempted synchronously, and a stamping failure or CN crash leaves `commit_ts` NULL. (c) Resolution gate: a candidate link may start or continue a fold chain at a departure instant `T` only when `commit_ts = T`. This is exact-transaction binding: the link insert committed atomically with the move (I7), and at one committed instant at most one transaction deletes a given row version or births a given key (§5.2 F1+F2), so the admitted link's recorder is provably the departure tombstone's author. `commit_ts` NULL on a material candidate is unprovable evidence — the verdict fails closed LOUDLY (D51/D56), never a silent legacy downgrade; `commit_ts != T` EXCLUDES the candidate before any identity comparison — it can neither bind nor poison; zero admissible is proven absence (legacy semantics, NF5 completeness unchanged); two or more DISTINCT admissible identities fail closed loudly (impossible absent corruption, since one row version dies once). There is NO single-candidate exemption (r107 review P1): every followed link presents the same proof. (d) Membership subsumption: every departure instant the walk consults is a collected stream entry of this source lineage by construction, so an admitted candidate provably committed inside the collected windows — certified window membership with no separate evidence — and the reviewed straddler (created before a fork, committed after its lineage window ended) fails the equality and is excluded; D224's per-window attribution and deferred screening are never built. (e) Capture retirement: the D156 scope-to-commit capture is removed from every resolution path; the arrival tie (its remaining r100 role) re-attributes from the preserved column — a predecessor move justifies an arrival exactly when its `commit_ts` equals the arrival instant, NULL stays undecidable per instant, distinct justifiers stay ambiguous — under the unchanged whole-window completeness requirement. No verdict anywhere reads the link catalog's reclaimable committed change history (r107 review P2); §9.3's reader class (c) is removed. (f) Same-transaction/netted chaining unchanged: one transaction's links share one commit instant, so netted continuations (scope equality by `txn_id`/`write_ts`, D18/D26) inherit the anchor's binding instead of re-proving it. (g) PICK unchanged: the key closure keeps its wider unanchored read and never consults `commit_ts` (over-admission widens only the dependency-key set); fold decisions stay behind the commit-gated walk. (h) Cost: one small stamping transaction per pk-moving transaction (bounded by the existing coordinator budget) replaces the capture's per-generation change-stream pass; the probe/budget posture (I6/NF8) is unchanged. (i) Failure posture and lifecycle: after catalog-object rewriting and reclamation, while the lineage's links and its protected user-table stream stay live, every verdict keeps resolving — the §10 lifecycle oracle restores exact MERGE/PICK cascade assertions after reclamation and restart on otherwise valid retained branches; loud aborts remain for genuinely invalid or incomplete evidence only (truncated/disabled probes per NF5, duplicate admissible identities, unstamped material links). (j) Fixtures: walk fixtures fabricate links with their intended commit instants, and a fixture may leave `commit_ts` NULL to prove the loud lane; empty-end windows keep the "read to now" posture. The r107 review's P1 counterexample (singleton straddler plus independent DELETE/INSERT, MERGE and PICK, asserting DELETE semantics, FK rejection, and unchanged destination parent/child rows) and the P2 lifecycle oracle enter the §10 validation map.
- D226 (r109, 2026-11-02, windowed commit-exact admission and durable stamp recovery; amends D225; refines D11/D24 window verdicts and D13's evidence lifetime) [AMENDED by D227 before implementation: the recovery rule becomes a single outcome table keyed by identity class — transaction-bound inheritance extends to netted S3 object identities, a stream-bound candidate with no in-window death on a complete stream is excluded as proven window absence rather than aborted, and loud aborts confine to incomplete or corrupt evidence and unbound transaction-local candidates; the windowed admission gate, the publication protocol, the re-stamp, and the failure posture stand]: repairs both r108-review findings (the issue-fix-loop controller's local reviewer, 2026-11-02, REQUEST_CHANGES, blocking P1 + P2). The r108 review verified (P1) that D225(d)'s membership subsumption conflated the anchor's window with the candidate's own window: windows are relation-specific (one per physical table, `data_branch.go`'s collectRange projection), so ONE transaction — an RC transaction begun before a fork advances its snapshot per statement (`handleRCSnapshot`) and can write both generations — that moves a row on the parent and independently DELETEs/INSERTs on the child commits both at one instant; the parent link's `commit_ts` then EQUALS the child anchor's departure instant while lying OUTSIDE the parent window's `[start, end]`, so equality-only admission splices the parent's link onto the child's independent pair, and the destination count guard passes a wrong UPDATE cascade; and (P2) that a NULL `commit_ts` after a stamping failure, its cancellation, a CN crash in the stamping window, or a lost commit response permanently strands an otherwise valid committed branch under D225's loud-abort posture, with the txn-operator completion event joining callback errors into the post-commit result path. (a) Attribution restored: the probe preserves each candidate's per-(physical table, window) attribution — one candidate pool per collected window, the pool of the window whose stream carries the hop's departure tombstone — and conflict checking happens only among the admitted candidates of one hop's pool; candidates of different windows never poison one another. (b) Windowed admission: a candidate may start or continue a chain at a departure instant `T` only when its stored `commit_ts` falls INSIDE its own attributed window's `[start, end]` AND equals `T`, checked before any identity comparison or arrival attribution; the one-transaction cross-branch counterexample fails condition (i) (the parent window ended before the single commit), the classic straddler fails condition (ii), and a valid in-window move passes both. (c) The arrival tie is amended identically: a predecessor justifies an arrival exactly when its `commit_ts` equals the arrival instant AND falls inside the predecessor's own attributed window. (d) Publication/recovery protocol: the eager commit-completion stamping is best-effort and never alters the user-visible COMMIT result — the stamping callback swallows and logs its own failure rather than reporting it into the completion event; a NULL `commit_ts` is a durable stamp-pending intent, and resolution heals it lazily from the protected per-window user-table stream the walk already collected: the linked `old_row_id` version dies exactly once in its physical table's committed stream, and I7 ties the link to that commit, so the death's instant IS the recording transaction's commit instant; a placeholder `old_row_id` inherits the instant from any stream-bound or stamped link of the same transaction by D18/D26 scope equality; exactly one in-window death heals, no in-window death excludes the candidate, and a duplicate in-window death, an unbound placeholder chain, or a truncated/disabled stream aborts loudly; a healed candidate is best-effort re-stamped by one small bounded internal transaction over its exact `link_id` set, and re-stamp failure is harmless because the instant is recomputable; lost commit responses (`FinalizeCommitWithUnknownResult`), cancellation, crash and restart all land on the same NULL-heals lane; a rolled-back move commits no links and no stream entries (I7), so no provenance outlives its move. (e) Failure posture: loud aborts remain only for genuinely incomplete or ambiguous evidence — a truncated/disabled probe (NF5), duplicate admitted identities, or an unrecoverable stamp-pending candidate; no permanent NULL-induced aborts remain. (f) The D156 capture retirement, the equality binding, the no-single-candidate-exemption posture, and PICK's wider unanchored read are unchanged from D225. (g) Cost: recovery rides the already-collected protected stream (no new reader class, no historical catalog state), plus at most one optional re-stamp transaction per healed link. (h) Validation: the §10 map gains the one-transaction cross-branch MERGE/PICK oracle (FK rejection and unchanged destination parent/child rows), the window-mismatch commit-gate assertions, the recovery UTs, and the deterministic crash/failure checkpoints (stamping failure, cancellation, crash between commit and stamping, lost commit response, restart — each followed by exact MERGE/PICK cascades for committed moves, with rollback leaving no provenance).
- D227 (r110, 2026-11-02, single recovery outcome table; transaction-bound stamp recovery extended to netted S3 object identities; amends D226; refines D32/D49/D50's S3-route continuity and D13's evidence lifetime) [CONTINUATION RULE AMENDED by D228 before implementation — REJECTED at the independent design review of r110: the (d) key-chain acceptance is retired (same transaction, same key and an absent tombstone cannot distinguish the landed generation's own departure from an earlier unrelated departure of a different holder), and same-transaction continuity becomes the commit-time fixup's persisted pairing (`succ_link_id`/`stmt_seq`) followed by exact link_id probe with the identity match and tombstone screen demoted to loud cross-checks; the outcome table (a)-(c), the no-absence-inference posture, the boundary behavior, the re-stamp, and the failure posture stand]: repairs both r109-review findings (the issue-fix-loop controller's local reviewer, 2026-11-02, REQUEST_CHANGES, blocking P1 + P2). The r109 review verified (P1) that D226's recovery rule classified every non-workspace `old_row_id` as a committed stream-bound version and excluded a NULL-stamped candidate with no in-window death, but uncommitted S3 rows also carry non-workspace object identities and their same-transaction deletes are diverted without publishing a tombstone (`pkg/vm/engine/disttae/txn.go:2544-2569`: the delete lands in the per-block list, the batch is shrunk, the object is rewritten minus the row at commit), so under a stamping failure the second hop of an S3-netted chain (the reviewed schedule: one transaction moves 2->1000->1001 through a forced small-object flush and independently reinserts 1000, `merge_foreign_key_pk_move.sql:879-907`) is wrongly excluded, the terminal timestamp claim then adopts the unrelated reinserted row born at the chain instant (`data_branch_pk_move_link.go:3206-3301`), the destination count guard passes (`data_branch_output.go:2200-2241`), and the apply cascades onto the impostor key instead of the lineage's final key; and (P2) that D226's rule text excluded a candidate with no in-window death while its soundness argument and section 5.3 classified the same shape (no death, no bound sibling) as a loud abort — an unstamped successor committed after a historical endpoint (`pick_foreign_key_pk_move.sql:822-855`) would abort the apply and block the valid earlier in-window move, contradicting D226's own promise that out-of-window candidates cannot poison valid applies, while the stamped form already resolves by ending the lineage at the claimed version (the terminal exclusion path, `data_branch_pk_move_link.go:3303-3322`). (a) Single outcome table: recovery resolves every NULL-stamped candidate to exactly one of heal (the linked stream-bound version's unique in-window death is the commit instant), inherit (a transaction-local identity — a workspace placeholder or an uncommitted S3 object identity reached through the transaction's own recorded continuity — takes the commit instant of any admitted same-transaction sibling, one transaction one commit instant, the established continuation checks and per-(table, window) attribution preserved, and the two-condition gate still evaluated against the candidate's own window), exclude (proven window absence: stream-bound identity, NF5-complete stream, no in-window death — never loud, never heals into a window), or abort loudly (incomplete or corrupt evidence: a truncated/disabled stream, a duplicate in-window death, or an unbound transaction-local candidate). (b) No absence inference for netted identities: window absence is never inferred from a missing death for transaction-local identities, because their diverted deletes publish no tombstone regardless of when the transaction commits; their exclusion comes only from the bound instant failing the candidate's own window. (c) Boundary behavior: an unstamped successor committed after a historical endpoint and an unstamped cross-fork straddler both land on the exclude row, so the valid in-window move still cascades — one verdict for stamped and stamp-pending candidates. (d) The same-transaction continuation records both of its shipped forms (identity-chained through the workspace placeholder; key-chained through the transaction's recorded S3-route departure, screened against foreign vacate-before-arrival departures by the in-window tombstone), and section 5.1's NULL-identity bullet is aligned with the recorded S3 continuity (D32/D49/D50, D75-D81). (e) The D226 windowed admission gate, the best-effort publication protocol with the closed COMMIT-result rule, the best-effort re-stamp, the rollback-no-provenance posture, and the no-historical-catalog-reader posture are unchanged; recovery still adds no reader class and no storage dependency. (f) Validation: the section 10 map gains the MERGE/PICK stamping-failure and crash twins of both S3-chain scenarios asserting the exact child key and the preservation of the unrelated intermediate row, the NULL-stamped future-successor and cross-fork cases requiring the valid in-window move to cascade, and the outcome-table recovery UTs (inherit when chained, exclusion never inferred from a missing death, unbound transaction-local loud).
- D228 (r111, 2026-11-02, recorded same-transaction continuity: the commit-time fixup pairs arrivals with later departures over the converged post-prune set and persists the pairing on the link row; amends D227's continuation rule; refines D32/D49/D50/D75-D81's S3-route continuity and D53's fixup bookkeeping) [REVISIT CLAUSE AMENDED by D229 before implementation — REJECTED at the independent design review of r111 (P2): (c)'s recorded cycle guard rejected every pairing-followed hop landing on a previously visited key, aborting D70's proven same-transaction return that D228 itself pairs; the pairing recording and credential (a)-(b), the walk's pairing verification and loud cross-checks (c), the one-mechanism, stamp-independence, PICK and lifecycle posture (d)-(g), and the validation/acceptance consistency (h) stand]: repairs both r110-review findings (the issue-fix-loop controller's local reviewer, 2026-11-02, REQUEST_CHANGES, blocking P1 + P2). The r110 review verified (P1) that D227(d)'s key-chained acceptance established continuity from same transaction, same key and an absent tombstone, which cannot distinguish the landed generation's own departure from an EARLIER unrelated departure of a different holder: one transaction inserts an unrelated row B at 1000, spills it to an uncommitted S3 object, moves B 1000->1001 (the delete is diverted per `pkg/vm/engine/disttae/txn.go:2544-2569` and publishes no tombstone), then moves A 2->1000 with an identity-less S3 replacement; both links survive the commit fixup (B's death at 1000 predates A's staging statement, so the `od < birth` post-mortem rule ignores it — `pkg/vm/engine/disttae/pk_move_link_fixup.go`), D227 heals A's link at the death of 2, lets B's link inherit that same instant, and the walk's key-chain rule (`pkg/frontend/data_branch_pk_move_link.go:3224-3234`) follows B to 1001 instead of ending A at 1000 — the destination count guard passes (`data_branch_output.go:2200-2232`) and children cascade onto the unrelated B, while the existing vacate-before-arrival coverage uses a committed B (`merge_foreign_key_pk_move.sql:1055-1084`) whose tombstone masks the uncommitted form; and (P2) that the submitted public acceptance projection's limitations preserved the rejected r109 reading — a stream-bound candidate with no unique in-window death on retained evidence classified as unprovable provenance that aborts loudly — contradicting the same projection's own exclusion semantics and outcome row (c). (a) Recording-side pairing: over the converged post-prune link set of one committing transaction, the commit-time fixup pairs each retained link `L` arriving at key `K` (staging statement ordinal `a`) with the same-transaction retained link departing `K` at the smallest ordinal `d > a`. The pairing is sound by the fixup's own invariants: one row per key per committed instant, so between `a` and the smallest later departure the arriving row itself holds `K` and that departure is its own; a retained link whose landed generation died unexplained inside the transaction is pruned (and the fixpoint cascades), so no retained link faces an unexplained death of its replacement; and a continuer whose old key's newest death predates its staging is reincarnation lineage and never registered — so an earlier unrelated departure of a different holder can never be paired with a later arrival. (b) Persisted credential: the link row gains `succ_link_id binary(64) null` (the paired continuation's exact `link_id`, computed unconditionally — a D228-era NULL always means TERMINAL, never unknown) and `stmt_seq bigint unsigned not null` (the staging statement's ordinal), riding the same unpublished 4.0.7 tail upgrade entry and the predefined DDL (the table is unshipped; a pre-column shape stays an unreleased dev layout under the offset hard-error rule). (c) The walk follows ONLY the recorded pairing: the continuation hop is one exact `link_id` point probe; the walk verifies presence, scope equality (`txn_id`/`write_ts`, D18/D26), `old_pk` equal to the running `new_pk`, and strictly greater `stmt_seq`, then applies the D227 recovery and the D226 windowed gate to the fetched hop; the workspace-identity match and the tombstone screen are demoted to consistency cross-checks whose contradiction is corrupt evidence failing loudly, and a NULL pairing coexisting with a retained later same-transaction departure of the landed key fails loudly; r110's inferred acceptance (the identity match and the key-chain rule) is retired; pairing-followed hops follow D229's revisit rule — a verified hop landing on the lineage's own previously visited key is a proven return and continues under the parser's birth-certificate order — and the cycle guard otherwise stands. (d) One mechanism covers both shipped chain forms — the workspace-placeholder form and the S3-netted form — because pairing keys on the transaction's statement ledger, not on any replacement identity. (e) Stamp independence: the pairing derives from the transaction's own statement ledger at commit, so it holds for stamped and stamp-pending links alike, and the r110 review's oracle (A's child ends at the target key, unrelated B remains at its own final key) holds in both forms. (f) PICK follows the same recorded pairs; its key-closure probes are unchanged. (g) The credential travels inside the live link row (§9.3 class (a)): chain resolution after catalog rewriting, reclamation and restart follows the same recorded pairs, adding no reader class and no storage dependency. (h) Validation and acceptance consistency: the section 10 map gains the r110 review P1's demanded MERGE/PICK twins of the uncommitted vacate-before-arrival schedule in stamped AND stamp-pending forms (A's child ends at the target key; unrelated B remains at its own final key as its own plain insert) plus the fixup pairing-persistence and walk UTs; and the public acceptance projection restates its limitation as the four outcomes consistently — a unique in-window death heals, proven window absence excludes, proven transaction-local continuity inherits, and only incomplete, corrupt or unbound evidence aborts — with no blanket no-death abort (the r110 review's P2).
- D229 (r112, 2026-11-02, proven same-transaction returns through the recorded pairing resolve; amends D228's revisit clause; restores D70 consistency) [REVISIT CLAUSE AMENDED by D230 before implementation — REJECTED at the independent design review of r112 (P2): (c)'s non-pairing revisit clause barred EVERY cross-transaction return to a vacated key, aborting the separately committed `2->3->2` schedule that retained D66's approved r100 text requires to fold as an in-place UPDATE or no-op; the equal-certificate pairing homecoming (a), the resolution shape (b), the rotation rejection in (c), and the validation/acceptance consistency (d)-(e) stand, with attributable cross-transaction returns admitted by D230]: repairs the r111-review finding (the issue-fix-loop controller's local reviewer, 2026-11-02, REQUEST_CHANGES, blocking P2). The r111 review verified that §5.2's recorded cycle guard rejected ANY pairing-followed hop landing on a previously visited key while D228 pairs the two links of a one-transaction `2->3->2` return — the pairing-followed hop lands on the already-seeded anchor key, so the specified guard aborts a valid apply, contradicting retained D70; the shipped parser rejects only strictly backward birth certificates (`pkg/frontend/data_branch_pk_move_link.go:3173-3178`, equal-certificate homecomings documented at `:3148-3161`), and the shipped success oracles prove returns resolve (`pkg/frontend/data_branch_pk_move_link_test.go:1420-1469`, `test/distributed/cases/git4data/branch/merge/merge_foreign_key_pk_move.sql:1514-1561`, `test/distributed/cases/git4data/branch/pick/pick_foreign_key_pk_move.sql:1088-1133`). A single lineage returning home is distinct from a multi-row key rotation. (a) Revisit rule: a verified pairing-followed hop landing on the lineage's own previously visited key is a PROVEN return and continues whenever the birth-certificate order is valid — the parser's rule: strictly backward fails closed as corrupt evidence; an EQUAL certificate is D70's same-transaction homecoming (scope equality fixes one transaction and one commit instant, so every pairing hop of the anchor's own transaction revisits at the anchor's instant); a strictly newer certificate is a return to a key the lineage itself vacated in an earlier generation, reached at the recording transaction's own instant. The recorded links progress by construction (strictly greater `stmt_seq` per hop), so termination holds even with repeated keys: `stmt_seq` is bounded by the committing transaction's statement count, and the hop cap (`pkMoveLinkMaxChain`) bounds the whole walk regardless. (b) Resolution shape: the terminal link's NULL pairing ends the chain at the returned key, the claim accepts the re-birthed row born at the chain instant, and the fold planner's `oldKey == finalPK` path folds a changed payload as an in-place update and an unchanged payload as a no-op (D66 — a return to the original key is plan shape, never a rotation cycle). (c) Rotation posture unchanged: a true multi-row key ROTATION (distinct rows exchanging keys in one transaction) still resolves each row's own walk — recorded pairings cannot cross rows — and the fold planner rejects the exchange as a rotation cycle (D57); non-pairing cross-key hops landing on a previously visited key follow D230's three-way order — a strictly backward certificate fails closed, an attributable cross-transaction hop with a strictly newer certificate RESOLVES (the separately committed `2->3->2` and `2->3->4->3` returns; same-key tail re-entries likewise), and only an UNPROVEN revisit (a foreign row's departure the departed-birth-pinned pairing never serves to this walk) keeps the fail-closed posture. (d) Validation: the section 10 map carries the existing changed/unchanged-payload MERGE/PICK return-home success oracles into the persisted-pair validation plan — MERGE and PICK twins of the one-transaction `2->3->2` return asserting the changed-payload fold lands as an in-place native UPDATE at the original key and the unchanged-payload form applies as an in-place no-op, each in a stamped form AND a stamp-pending form (stamping-failure injection), with the persisted pairing asserted (the first link pairs to the second, the last records terminal NULL) — plus walk UTs (a verified pairing hop landing on the lineage's own previously visited key with progressing `stmt_seq` resolves; a strictly-backward certificate fails loudly; the hop cap still bounds; the two-row rotation exchange still aborts at planning). (e) Acceptance consistency: the public acceptance projection preserves the proven-return behavior — a proven same-transaction return resolves through the recorded pairing (changed payload as an in-place native UPDATE, unchanged payload as an in-place no-op), while a genuine multi-row key rotation remains a loud planning abort.
- D230 (r113, 2026-11-02, proven cross-transaction returns with strictly advancing birth certificates resolve under the windowed admission gate; amends D229's revisit clause; restores D66 consistency): repairs the r112-review finding (the issue-fix-loop controller's local reviewer, 2026-11-02, REQUEST_CHANGES, blocking P2). The r112 review verified that §5.2's guard and D229(c) admitted revisits only through recorded same-transaction pairings or same-key tails, so two SEPARATELY committed updates `2->3` at T1 then `3->2` at T2 > T1 abort when the cross-transaction departure returns to the seeded anchor key 2 — contradicting retained D66, whose approved r100 text (commit `56c390d567a249e498a906d1d162fe384593951e`, lines 2040-2054) explicitly requires exactly this schedule to resolve "through the advancing-timestamp chain into one fold whose departure and arrival share key 2" and to apply as a native in-place UPDATE (changed payload) or in-place no-op (unchanged payload). The deciding shipped code is unchanged from r111: the departed-birth-pinned pairing index binds the departure tombstone to the lineage's own landed version (`pkg/frontend/data_branch_pk_move_link.go:2879-2929`), the hop advances the birth certificate (`:3381-3393`), the revisit rule rejects only strictly backward certificates (`:3173-3178`, with the later-certificate cross-transaction return documented at `:3148-3161`), the terminal claim resolves at the chain instant (`:3290-3301`), an unresolved destination-held anchor aborts (`:3241-3244`), and the fold planner excludes the self-dependency (`pkg/frontend/data_branch_hashdiff.go:3423-3435`, whose comment names the two-committed-moves `2->3->2` shape); the shipped MERGE/PICK success oracles cover the separate-transaction changed/unchanged-payload returns (`test/distributed/cases/git4data/branch/merge/merge_foreign_key_pk_move.sql:1431-1476`, `test/distributed/cases/git4data/branch/pick/pick_foreign_key_pk_move.sql:1006-1051`). A single lineage returning home across commits is distinct from a foreign reincarnation and from a multi-row key rotation. (a) Revisit rule (complete three-way order, every hop class): a hop landing on a previously visited key compares its birth certificate against the newest certificate the walk recorded at that key — STRICTLY BACKWARD fails closed as corrupt evidence (unchanged); EQUAL continues only through the verified recorded same-transaction pairing (D229(a)'s homecoming); STRICTLY NEWER continues through any proven channel — the verified recorded pairing (D229's return to a self-vacated key at the recording transaction's own instant), a same-key tail re-entry, or an ATTRIBUTABLE cross-transaction departure: the departed-birth-pinned pairing index serves the hop only when the departure tombstone names the lineage's own landed version (departed birth equals the running certificate), and the successor link passes D226's windowed admission gate (`commit_ts` equals the departure instant AND falls inside its own attributed window) — so the lineage itself returns to a key it vacated earlier, which is D66's approved semantics for the separately committed `2->3->2` and the longer attributable `2->3->4->3` forms. A FOREIGN row's departure is never attributable: its departed birth differs from the running certificate, the pairing index never serves it to this walk, the claim finds no row born at the chain instant, and the walk fails closed — the unproven cross-transaction reincarnation posture is unchanged. (b) Termination: cross-transaction hops strictly advance the commit instant (one transaction, one commit instant over the committed stream, so each admitted hop consumes a distinct instant), pairing-followed hops strictly advance `stmt_seq` within the committing transaction's statement count, same-key tails strictly advance the instant, and the hop cap (`pkMoveLinkMaxChain`) bounds the whole walk regardless. (c) Resolution shape: the terminal claim accepts the re-birthed row born at the chain instant, and the fold planner's `oldKey == finalPK` path folds a changed payload as an in-place native UPDATE and an unchanged payload as an in-place no-op (D66); a genuine multi-row key ROTATION still resolves each row's own walk — recorded pairings cannot cross rows, and a cross-transaction hop binds only the lineage's own landed version — and the fold planner rejects the exchange (D57). (d) Validation: the section 10 map retains the existing separate-transaction changed/unchanged-payload MERGE/PICK return successes (`merge_foreign_key_pk_move.sql:1431-1476`, `pick_foreign_key_pk_move.sql:1006-1051`), each additionally run in a stamp-pending form (stamping-failure injection) with the window-admitted successor asserted, gains new MERGE and PICK twins of the attributable `2->3->4->3` return (changed and unchanged payloads, stamped AND stamp-pending), and gains walk UTs (an attributable cross-transaction revisit with a strictly advancing certificate resolves; the same hop with the successor outside its attributed window fails closed; a foreign row's departure at the vacated key is never followed; a strictly backward certificate fails loudly; the hop cap bounds; the rotation exchange still aborts at planning). (e) Acceptance consistency: the public acceptance projection admits the proven cross-transaction return — separately committed moves returning a row to a key the lineage itself vacated fold as an in-place native UPDATE (changed payload) or an in-place no-op (unchanged payload) under the windowed admission gate, while an unproven reincarnation and a genuine multi-row key rotation remain loud.

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
| r106 | D223 | PR #31629 review (aunjgr inline): snapshot-anchored provenance reads — an out-of-window straddler commit can no longer poison a valid descendant anchor; submitted for independent design approval before implementation and REJECTED there (historical link-catalog storage versions are unprotected); never shipped. |
| r107 | D224 (supersedes D223) | Independent design review of r106 (issue-fix-loop controller's local reviewer, REQUEST_CHANGES, P2): D223's anchored reads required historical link-catalog storage versions no snapshot/PITR channel retains; r107 withdraws them for per-window attribution with membership-deferred conflict checking, adds the §9.3 storage-version retention audit and the §10 lifecycle validation scenario; submitted for fresh independent design approval before implementation and REJECTED there (P1 singleton exemption admits an unscreened out-of-window candidate; P2 membership screening depends on reclaimable catalog change history); never shipped. |
| r108 | D225 (supersedes D224; retires D156's capture) | Independent design review of r107 (issue-fix-loop controller's local reviewer, REQUEST_CHANGES, P1+P2): D224(f)'s single-candidate exemption let a singleton out-of-window straddler link splice onto a sibling branch's independent DELETE/INSERT (no transaction/commit binding), and D224's membership screening depended on the reclaimable link-catalog change history, stranding valid branches after reclamation; r108 withdraws D224 for preserved commit-exact provenance (`commit_ts` column + CN commit-completion stamping + the commit-exact admission gate with no exemption), retires the D156 capture from every resolution path, rewrites the §9.3 audit, and updates the §10 validation map with the review's demanded oracles; submitted for fresh independent design approval before implementation and REJECTED there (P1: equality-only admission conflated the anchor's window with the candidate's own window — one RC transaction can author both the ancestor move and the sibling's independent DELETE/INSERT in a single commit; P2: permanent NULL-induced aborts strand valid committed branches after a transient stamping failure, and the COMMIT-result semantics were unclosed); never shipped. |
| r109 | D226 (amends D225) | Independent design review of r108 (issue-fix-loop controller's local reviewer, REQUEST_CHANGES, P1+P2): r109 restores per-(table, window) candidate attribution and gates admission on the candidate's `commit_ts` falling inside its own attributed window in addition to departure equality, and replaces the permanent NULL-abort posture with the durable publication/recovery protocol (best-effort stamping with a closed COMMIT-result rule, NULL as a stamp-pending intent healed lazily from the protected stream, same-transaction inheritance for placeholder chains, best-effort re-stamp); the §5.1-§5.4 and §6.2 protocol text, the §9.3 consequences, the §10 validation map (window-mismatch gate assertions, the one-transaction cross-branch MERGE/PICK oracle, recovery UTs, deterministic crash/failure checkpoints), and this trace updated in step; submitted for fresh independent design approval before implementation and REJECTED there (P1: the recovery rule misclassified netted uncommitted-S3 object identities as stream-bound and excluded their unstamped links on a missing death that carries no window information, so an S3-netted chain folded onto an unrelated reinserted row; P2: the rule text carried two inconsistent outcomes — exclude versus loud abort — for one evidence shape, letting an unstamped post-endpoint successor block a valid in-window apply); never shipped. |
| r110 | D227 (amends D226) | Independent design review of r109 (issue-fix-loop controller's local reviewer, REQUEST_CHANGES, P1+P2): r110 makes recovery a single outcome table (heal from the unique in-window death / inherit across the transaction's own netted generations / exclude on proven window absence / abort only on incomplete or corrupt evidence), extends transaction-bound inheritance to netted S3 object identities with the established continuation checks and per-(table, window) attribution preserved, aligns the §5.1 NULL-identity bullet with the recorded S3 continuity, records both same-transaction chain forms in §5.2, aligns the soundness argument, the §5.3 reader rules, and the §6.2/§9.3 consequences, and updates the §10 validation map (MERGE/PICK stamping-failure and crash twins of both S3-chain scenarios asserting the exact child key and the preserved unrelated intermediate row; NULL-stamped future-successor and cross-fork cases requiring the valid in-window move to cascade; the outcome-table recovery UTs) and this trace in step; submitted for fresh independent design approval before implementation and REJECTED there (P1: the key-chain acceptance followed an earlier unrelated departure of a different key-holder — an uncommitted vacate-before-arrival whose diverted delete publishes no tombstone — cascading children onto the unrelated row instead of ending the chain at the landed key; P2: the submitted acceptance projection's limitations contradicted the proven-window-absence exclusion rule); never shipped. |
| r111 | D228 (amends D227's continuation rule) | Independent design review of r110 (issue-fix-loop controller's local reviewer, REQUEST_CHANGES, P1+P2): r111 replaces inferred same-transaction continuity with recorded continuity — the commit-time fixup pairs each retained link's arrival with the same-transaction retained link departing its new key at the smallest later statement ordinal over the converged post-prune set and persists the pairing on the link row (`succ_link_id` computed unconditionally, NULL meaning terminal, plus `stmt_seq`), the walk follows ONLY the recorded pairing by exact link_id probe with scope equality, key match and ordinal order verified (identity match and the tombstone screen demoted to loud consistency cross-checks; a NULL pairing coexisting with a retained later same-transaction departure loud), one mechanism covering the workspace-placeholder and S3-netted forms stamp-independently, and the r110 key-chain acceptance retired; §5.1 gains the credential columns, §5.2's continuation and recovery text rewritten, §5.3/§6.2/§7/§9.3 record the reader bounds, storage form and drift posture, §10 gains the uncommitted vacate-before-arrival MERGE/PICK twins in stamped AND stamp-pending forms (A's child ends at the target key; unrelated B remains at its own final key) plus the fixup pairing and walk UTs, the acceptance projection restates the four outcomes consistently with no blanket no-death abort, and this trace updated in step; submitted for fresh independent design approval before implementation and REJECTED there (P2: the recorded cycle guard rejected every pairing-followed hop landing on a previously visited key, aborting D70's proven same-transaction return that D228 itself pairs — the shipped parser rejects only strictly backward birth certificates and the shipped oracles resolve the `2->3->2` homecoming); never shipped. |
| r112 | D229 (amends D228's revisit clause; restores D70 consistency) | Independent design review of r111 (issue-fix-loop controller's local reviewer, REQUEST_CHANGES, P2): r111's recorded cycle guard rejected every pairing-followed hop landing on a previously visited key, so D228's own pairing of a one-transaction `2->3->2` return aborted a valid apply, contradicting retained D70; r112 makes the walk specification and D228 consistent with D70 — a verified pairing hop landing on the lineage's own previously visited key is a proven return and continues under the parser's birth-certificate order (a strictly backward certificate fails closed as corrupt evidence), strictly increasing `stmt_seq` plus the hop cap bound the walk, the terminal NULL pairing ends the chain at the returned key and the fold planner's `oldKey == finalPK` path folds changed/unchanged payloads, and the fold planner's true key-rotation rejection stands; §5.2's guard rewritten, §5.1's `stmt_seq` bullet aligned, D228's register entry amended and D229 registered, §10 gains the changed/unchanged-payload MERGE/PICK return-home oracles over persisted pairs in stamped AND stamp-pending forms (carried from the shipped success oracles) plus the walk UTs, the acceptance projection preserves the proven-return behavior, and this trace updated in step; submitted for fresh independent design approval before implementation and REJECTED there (P2: the revisit rule admitted only recorded same-transaction pairings and same-key tails, so two separately committed updates `2->3` then `3->2` aborted when the cross-transaction departure returned to the seeded anchor key, contradicting retained D66 — the approved r100 text requires exactly that schedule to fold as an in-place UPDATE or no-op, the shipped parser rejects only strictly backward birth certificates and admits the later-certificate cross-transaction return, and the shipped MERGE/PICK oracles resolve the separate-transaction returns); never shipped. |
| r113 | D230 (amends D229's revisit clause; restores D66 consistency) | Independent design review of r112 (issue-fix-loop controller's local reviewer, REQUEST_CHANGES, P2): r112 admitted revisits only through recorded same-transaction pairings or same-key tails, so two separately committed updates `2->3` then `3->2` aborted when the cross-transaction departure returned to the seeded anchor key, contradicting retained D66's approved r100 text (commit `56c390d567a249e498a906d1d162fe384593951e`, lines 2040-2054 — two separately committed `2 -> 3 -> 2` updates resolve through the advancing-timestamp chain into one in-place UPDATE or no-op); r113 makes the walk specification consistent with D66 — a hop landing on a previously visited key follows the parser's birth-certificate order for every hop class: strictly backward fails closed as corrupt evidence, equal continues only through the verified recorded same-transaction pairing (D229's homecoming), and strictly newer continues through any proven channel including an ATTRIBUTABLE cross-transaction departure (the departed-birth-pinned pairing index serves only the lineage's own landed version, `pkg/frontend/data_branch_pk_move_link.go:2879-2929`, and the successor passes D226's windowed admission gate), while a foreign row's departure is never served and fails closed; termination holds (strictly advancing commit instants and `stmt_seq`, plus the hop cap), and the fold planner's true key-rotation rejection stands; §5.2's guard rewritten to the three-way order, §5.1's `stmt_seq` bullet gains the cross-transaction termination argument, D229's register entry amended and D230 registered, §10 retains the separate-transaction changed/unchanged-payload MERGE/PICK return successes and gains the attributable `2->3->4->3` twins in stamped AND stamp-pending forms plus the walk UTs, the acceptance projection admits the proven cross-transaction return, and this trace updated in step; submitted for fresh independent design approval before implementation. |

Review record:

```text
Change scope: whole feature (single PR on #28026), PR matrixorigin/matrixone#31629
Trigger: catalog persistence change; cross-subsystem protocol; upgrade/restore impact
Design: docs/design/20261016-data-branch-pk-move-update-provenance.md, revision r113 (this document)
Blocking findings: the r1-r101 rounds above; every finding is resolved by the
  numbered decision in the register, except D87 which was verified and rejected
  with grounds, and O1/O2 which remain open items with named owners
Decision log: D1-D230 above, with refinement/supersession stamps; the approvals at
  r1-r4, r8-r15, r16-r21, r22-r46, r47-r74, r75-r97, r98-r101 all PASS after their
  respective blocking findings closed; r106 (D223), r107 (D224), r108 (D225),
  r109 (D226), r110 (D227), r111 (D228) and r112 (D229) were REQUEST_CHANGES at
  independent design review and never shipped;
  r113 (D230) is submitted for independent design approval with this revision
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
  r105 = r104 with the placement re-derived onto the unpublished 4.0.7
  handler's tail tenant entries after upstream #31488 deleted the v4_0_19
  vehicle; editorial only, no protocol or decision change. Delivered r105
  content MD5 recorded in the PR #31629 ledger entry for this head.
  r106 = r105 plus the D223 protocol repair (snapshot-anchored provenance
  reads, with the section 5.3 bounds bullet, the section 10 validation-map
  row, and this trace updated in step); r106 changes a documented decision
  and therefore requires a fresh independent design approval covering D223
  before its implementation proceeds, and the delivered r106 content hash
  will be recorded in the PR #31629 ledger entry for its head. That
  independent review returned REQUEST_CHANGES (2026-11-02): D223's anchored
  reads required historical link-catalog storage versions that no
  snapshot/PITR channel retains, so r106 was never implemented.
  r107 = r106 with D223 withdrawn and superseded by D224 (per-window
  provenance attribution with commit-membership-deferred conflict
  checking), the section 5.3 reader-bounds bullet rewritten to the
  unanchored posture, the section 9.3 storage-version retention audit
  added, the section 10 validation map updated (the D224 lineage-visibility
  and bounds rows plus the demanded storage-version lifecycle scenario),
  and this trace updated in step; r107 changes documented decisions and
  therefore requires a fresh independent design approval covering D224
  before its implementation proceeds, and the delivered r107 content hash
  will be recorded in the PR #31629 ledger entry for its head. That
  independent review returned REQUEST_CHANGES (2026-11-02, P1+P2): the
  single-candidate exemption admitted an unscreened out-of-window
  candidate (a singleton straddler link spliced onto a sibling branch's
  independent DELETE/INSERT), and the membership screening depended on
  the link catalog's reclaimable committed change history, stranding
  valid retained branches after reclamation — so r107 was never
  implemented.
  r108 = r107 with D224 withdrawn and superseded by D225 (preserved
  commit-exact provenance: the `commit_ts` column added in place to the
  unpublished catalog DDL, CN commit-completion stamping, the
  commit-exact admission gate with no single-candidate exemption, and
  the retirement of the D156 scope-to-commit capture from every
  resolution path), the section 5.1 catalog contract and section 5.2
  walk updated, the section 5.3 reader-bounds bullet rewritten, the
  section 9.3 retention audit rewritten (the reclaimable-history reader
  class removed), the section 10 validation map updated (the D225
  lineage-visibility rows including the r107 review's
  singleton-straddler independent DELETE/INSERT oracle, the bounds row,
  the stamping rows, and the lifecycle oracle restored to exact cascade
  assertions after reclamation and restart), and this trace updated in
  step; r108 changes documented decisions and therefore requires a
  fresh independent design approval covering D225 before its
  implementation proceeds, and the delivered r108 content hash will be
  recorded in the PR #31629 ledger entry for its head. That
  independent review returned REQUEST_CHANGES (2026-11-02, P1+P2):
  equality-only admission conflated the anchor's window with the
  candidate's own window (one RC transaction can author both the
  ancestor move and the sibling's independent DELETE/INSERT in a
  single commit), and the permanent NULL-abort posture stranded valid
  committed branches after a transient stamping failure — so r108 was
  never implemented.
  r109 = r108 with D225 amended by D226 (windowed commit-exact
  admission: per-(table, window) candidate attribution restored and
  window membership required in addition to departure equality; and
  the durable publication/recovery protocol: best-effort stamping
  with a closed COMMIT-result rule, NULL as a durable stamp-pending
  intent healed lazily from the protected stream, same-transaction
  inheritance for placeholder chains, and best-effort re-stamping),
  the sections 5.1-5.4 and 6.2 protocol text, the section 9.3
  consequences, the section 10 validation map (the window-mismatch
  gate assertions, the one-transaction cross-branch MERGE/PICK
  oracle, the recovery UTs, and the deterministic crash/failure
  checkpoints), and this trace updated in step; r109 changes
  documented decisions and therefore requires a fresh independent
  design approval covering D226 before its implementation proceeds,
  and the delivered r109 content hash will be recorded in the PR
  #31629 ledger entry for its head. That independent review returned
  REQUEST_CHANGES (2026-11-02, P1+P2): the recovery rule
  misclassified netted uncommitted-S3 object identities as
  stream-bound and excluded their unstamped links on a missing death
  that carries no window information (the S3-netted chain then folded
  onto an unrelated reinserted row), and its text carried two
  inconsistent outcomes — exclude versus loud abort — for one
  evidence shape, which would let an unstamped post-endpoint
  successor block a valid in-window apply — so r109 was never
  implemented.
  r110 = r109 with D226 amended by D227 (the recovery rule becomes a
  single outcome table keyed by identity class: heal from the unique
  in-window death, inherit the bound instant across the transaction's
  own netted generations — workspace placeholders and uncommitted S3
  object identities alike, with window absence never inferred from a
  missing death — exclude on proven window absence, abort only on
  incomplete or corrupt evidence), the section 5.1 NULL-identity
  bullet aligned with the recorded S3 continuity, section 5.2's
  same-transaction continuation recording both shipped chain forms
  and the recovery rule rewritten with the soundness argument
  aligned, the sections 5.3/5.4/6.2/9.3 consequences updated, the
  section 10 validation map gaining the MERGE/PICK stamping-failure
  and crash twins of both S3-chain scenarios and the NULL-stamped
  future-successor and cross-fork cases, and this trace updated in
  step; r110 changes a documented decision and therefore requires a
  fresh independent design approval covering D227 before its
  implementation proceeds, and the delivered r110 content hash will
  be recorded in the PR #31629 ledger entry for its head. That
  independent review returned REQUEST_CHANGES (2026-11-02, P1+P2):
  the key-chained same-transaction acceptance followed an EARLIER
  unrelated departure of a different holder of the key (an
  uncommitted vacate-before-arrival whose diverted delete publishes
  no tombstone), cascading children onto the unrelated row instead of
  ending the chain at the landed key; and the submitted public
  acceptance projection's limitations preserved the rejected r109
  blanket no-death abort, contradicting its own exclusion semantics —
  so r110 was never implemented.
  r111 = r110 with D227's continuation rule amended by D228 (the
  commit-time fixup pairs each retained link's arrival with the
  same-transaction retained link departing its new key at the
  smallest later statement ordinal over the converged post-prune set
  and persists the pairing on the link row — `succ_link_id` computed
  unconditionally so NULL means terminal, plus `stmt_seq`; the walk
  follows ONLY the recorded pairing by exact link_id probe with scope,
  key and ordinal verification, the identity match and tombstone
  screen demoted to loud consistency cross-checks; one mechanism for
  the workspace-placeholder and S3-netted forms, stamp-independent;
  the r110 key-chain acceptance retired), section 5.1 gaining the
  credential columns and aligned bullets, section 5.2's
  same-transaction continuation rewritten and its recovery row
  aligned, sections 5.3/6.2/7/9.3 recording the pairing's reader
  bounds, storage form and drift posture, the section 10 validation
  map gaining the uncommitted vacate-before-arrival MERGE/PICK twins
  in stamped AND stamp-pending forms (A's child ends at the target
  key, unrelated B remains at its own final key) plus the fixup
  pairing and walk UTs and the acceptance-contract consistency repair,
  and this trace updated in step; r111 changes a documented decision
  and therefore requires a fresh independent design approval covering
  D228 before its implementation proceeds, and the delivered r111
  content hash will be recorded in the PR #31629 ledger entry for
  its head. That independent review returned REQUEST_CHANGES
  (2026-11-02, P2): the recorded cycle guard rejected every
  pairing-followed hop landing on a previously visited key, so
  D228's own pairing of a one-transaction `2->3->2` return aborted
  a valid apply, contradicting retained D70 — the shipped parser
  rejects only strictly backward birth certificates
  (`pkg/frontend/data_branch_pk_move_link.go:3173-3178`) and the
  shipped oracles resolve the homecoming — so r111 was never
  implemented.
  r112 = r111 with D228's revisit clause amended by D229 (proven
  same-transaction returns through the recorded pairing resolve: a
  verified pairing hop landing on the lineage's own previously
  visited key continues under the parser's birth-certificate order,
  strictly increasing `stmt_seq` plus the hop cap bound the walk,
  and the fold planner's true key-rotation rejection stands),
  section 5.2's guard rewritten, section 5.1's `stmt_seq` bullet
  aligned, D228's register entry amended and D229 registered, the
  section 10 validation map gaining the changed/unchanged-payload
  MERGE/PICK return-home oracles over persisted pairs in stamped
  AND stamp-pending forms plus the walk UTs, and this trace updated
  in step; r112 changes a documented decision and therefore requires
  a fresh independent design approval covering D229 before its
  implementation proceeds, and the delivered r112 content hash will
  be recorded in the PR #31629 ledger entry for its head. That
  independent review returned REQUEST_CHANGES (2026-11-02, P2): the
  revisit rule admitted only recorded same-transaction pairings and
  same-key tails, so two separately committed updates `2->3` then
  `3->2` aborted when the cross-transaction departure returned to
  the seeded anchor key, contradicting retained D66 — the approved
  r100 text requires exactly that schedule to fold as an in-place
  UPDATE or no-op, the shipped parser rejects only strictly backward
  birth certificates (`pkg/frontend/data_branch_pk_move_link.go:3173-3178`,
  the later-certificate cross-transaction return documented at
  `:3148-3161`), and the shipped MERGE/PICK oracles resolve the
  separate-transaction returns — so r112 was never implemented.
  r113 = r112 with D229's revisit clause amended by D230 (proven
  cross-transaction returns with strictly advancing birth
  certificates resolve under the windowed admission gate: a
  strictly backward certificate fails closed as corrupt evidence,
  an equal certificate continues only through the verified recorded
  same-transaction pairing, and a strictly newer certificate
  continues through any proven channel including an attributable
  cross-transaction departure — the departed-birth-pinned pairing
  index serves only the lineage's own landed version and the
  successor passes D226's windowed admission gate — while a foreign
  row's departure is never served and fails closed; strictly
  advancing commit instants and `stmt_seq` plus the hop cap bound
  the walk, and the fold planner's true key-rotation rejection
  stands), section 5.2's guard rewritten to the three-way order,
  section 5.1's `stmt_seq` bullet extended, D229's register entry
  amended and D230 registered, the section 10 validation map
  retaining the separate-transaction changed/unchanged-payload
  MERGE/PICK return successes and gaining the attributable
  `2->3->4->3` twins in stamped AND stamp-pending forms plus the
  walk UTs, and this trace updated in step; r113 changes a
  documented decision and therefore requires a fresh independent
  design approval covering D230 before its implementation proceeds,
  and the delivered r113 content hash will be recorded in the PR
  #31629 ledger entry for its head.
- Durable decision record and verbatim artifact:
  <https://gist.github.com/gouhongshen/5e3e7f1c103a8312e31c684676bf0cad>
  (decision record README plus the r100 PASS artifact JSON, MD5
  `29a610e69e0d27e802428bf0c8999b1a`, copied verbatim).
