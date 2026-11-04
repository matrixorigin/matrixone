# Disttae workspace point-read bounding and the per-batch key index

- Status: **proposed** — revision 1, 2026-10-21. Design review is required
  before implementation approval on the referencing PR; the approval record at
  the end of this document is the trace of that decision.
- Owning issue: [#26796](https://github.com/matrixorigin/matrixone/issues/26796)
  (severity/s0 performance record: latency of row-wise primary-key point
  UPDATEs growing with the in-transaction modification count).
- Implementation PR: [#30685](https://github.com/matrixorigin/matrixone/pull/30685).
  This revision describes implementation head
  `f4fd8cfc4eff6d682ff13b7eeace96fbe4944a59`. The PR description links the
  exact blob revision of this document.
- Gate: [.agents/skills/mo-dev/references/feature-design-review.md](https://github.com/matrixorigin/matrixone/blob/f4fd8cfc4eff6d682ff13b7eeace96fbe4944a59/.agents/skills/mo-dev/references/feature-design-review.md),
  applied by review
  [5593891733](https://github.com/matrixorigin/matrixone/pull/30685#pullrequestreview-5593891733).
  Implementation approval on PR #30685 stays blocked until this revision's
  design review passes.

## Change classification and gate record

```text
Change scope: single PR #30685 (disttae transaction-local read path).
Trigger: complexity — introduces a transaction-lifetime exact-key index (a
  stateful cache with hydration, invalidation, charge and refund lifecycle) on
  the workspace read hot path, and works a mandatory resource-accounting
  contract (spill sizing) into that lifecycle. The default size trigger also
  applies: 647 changed non-generated production lines (85 + 324 + 25 + 48 + 1
  + 164 additions-plus-deletions across the six production files), above the
  500-line threshold; 1,843 unit-test and 708 BVT lines are excluded.
Design: this document, revision 1 (2026-10-21), status proposed.
Blocking findings: pending design review of this revision.
Decision log: section "Design decisions" below.
Decision: proposed — needs design approval before implementation approval.
```

## Problem and evidence

Inside one transaction, every statement that can read the transaction's
uncommitted writes (`LocalDisttaeDataSource.filterInMemUnCommittedInserts`)
walked every in-memory workspace insert entry and applied
`MemPKFilter.FilterVector` to it: the statement re-encoded the entire
accumulated primary-key column of every batch, per read. For primary-key
equality probes (single-row UPDATE/DELETE/SELECT), a batch either cannot
contain the probed key or contains a handful of rows, but the per-statement
work still grew linearly with the transaction's total modified rows, so a
transaction of N row-wise point statements paid quadratic total
encoding work. Statement-boundary merging packs shuffled keys into large
shared batches, so a batch-level sorted order or a batch-level min/max cannot
help alone: most fat batches' key ranges cover the whole active domain.

Measured at base `eb18dbfa03`, on the workload recorded in issue #26796:

- 30,000 single-row point UPDATEs in one transaction: 1,000-statement window
  medians grow 3.628x between the first and the last window.
- Mid-transaction CPU profile: `MemPKFilter.FilterVector` occupies 31.06%
  cumulative CPU.

Facts above are recorded measurements from the issue and the PR validation
runs. The causal attribution (the read path dominating late-transaction
statement latency) is confirmed by the post-fix profile, where `FilterVector`
and `MemPKFilter` disappear from the cumulative report and the growth trend
vanishes.

### The contract this design adds

The cost of one workspace point probe must be bounded by what the probe can
possibly touch: batches whose key envelope provably misses the probe cost one
comparison; a batch that may contain the key is answered through an exact
per-batch structure whose construction is amortized over the batch's layout
lifetime; the point statement no longer re-encodes the transaction's whole
accumulated primary-key column per statement. Range statements keep the
original unpruned path whenever exactness cannot be proven. No observable
result may change.

Non-goals: no change to committed-data reads, snapshot or logtail reads,
workspace merging or spilling policy, DELETE/tombstone semantics, row
visibility, or any persisted protocol. No in-transaction statistics,
optimizer hints, or configuration knobs.

## Invariants

- I1 (envelope soundness): an initialized `Entry.pkRange` is a byte-wise
  superset of the canonical-encoded primary keys an entry's live rows ever
  have. In-transaction row deletions only remove rows; deletions never widen
  the envelope. Merging replaces the destination envelope with the union of
  both, or disables it when the source's is unknown; a failed merge append
  disables the destination's because a half-appended batch can hold rows
  outside the old envelope.
- I2 (index/layout agreement): a hydrated `Entry.keyOffsets` describes the
  row offsets of the entry's current batch layout at all times. Every batch
  content mutation (merge destination and source, shrink, release, dump)
  drops the map through the single invalidation choke point
  `Transaction.unaccountInsertKeyOffsetsLocked`.
- I3 (singleness of charge): a hydrated map contributes exactly
  `Entry.keyIndexAccountedSize` bytes to `Transaction.workspaceSize` (and to
  `approximateInMemInsertSize` for in-memory user-table INSERT entries), and
  exactly one refund of the same size exists. The charge is taken before the
  map is allocated; the refund and the pointer drop happen atomically in the
  choke point; no duplicate or transfer ever shares a charge with its source.
- I4 (fail-closed read gate): the envelope skip answers only when
  `MemPKFilter.MayMatchKeyRange` proves disjointness; the exact-key index
  answers only a validated single-column EQUAL probe on the same column the
  index encodes and only when no independent membership filter is attached;
  every other filter shape, unmodeled operator, disabled envelope, zero-row
  entry, or failed proviso keeps the original `FilterVector` (plus membership
  filter) path. Skipped batches are provable misses; answered batches are the
  definitive hit/miss for that key in that batch.
- I5 (honest spill sizing): every decision that consumes workspace size —
  the dump gate, per-table spill quota and selection, and the debug accounting
  assertion — uses the same total (batch plus charged index bytes) that the
  budget charged, so a workspace whose retained indexes exceed a threshold
  can never appear small enough to stay.
- I6 (retirement): no reachable retired state: slices truncated by the four
  workspace compactions clear their vacated tail slots (`clearVacatedWriteSlots`),
  transaction teardown refunds every entry's index, and released entries drop
  their envelope.
- I7 (concurrency): readers hold the workspace mutex across each workspace
  scan; every state transition (envelope computation at write, hydration,
  invalidation, charge, refund, transfer) happens under that same mutex, so
  no concurrent mutation can observe a hydrated map without its charge or an
  index answering a stale layout.
- I8 (observability stays out of production logic): the read-side counters
  in `pkg/vm/engine/readutil` (rows encoded, index builds, index probes) are
  test-only atomics; production code writes but never reads them; no new
  runtime metrics, logs, or operator surfaces are introduced.

## Design

### Layer 1: write-time key envelope

When an INSERT workspace entry is created
(`Transaction.writeBatchWithAutoIncrEpochKnown` under the workspace mutex),
`computeInsertKeyRangeLocked` resolves the entry's primary-key column
(`pkCheckPos`/`pkCheckReady`, the metadata the unique-key check path already
resolves) and encodes the column once with
`readutil.EncodePrimaryKeyVector` — the same canonical encoder
`FilterVector` uses — recording cloned min/max key bytes on the entry.
Entries whose primary-key column the write path cannot resolve
(`pkCheckReady` false, bad position, empty vector), or whose keys contain
NULLs, keep `pkRange.inited == false` and stay on the unpruned path for
their lifetime.
Envelope bytes are bounded by two key lengths per batch and are not charged
to the budget.

### Layer 2: lazy exact-key index

The first statement that reaches an entry with a qualifying exact probe
hydrates `ensureInsertKeyOffsetsLocked`: one encode of the batch's
primary-key column, one charge of the deterministic upper-bound estimate, one
map from copied key bytes to all live row offsets of repeated keys. The map
describes the current layout (I2) and is dropped — map plus refund — by the
choke point on every content mutation; the next qualifying probe rebuilds
it. Hydration is freely declined (`built == false` returns the caller to
`FilterVector`), including for zero-row entries, so no probe can be made
slower or less honest by the index's existence.

### Layer 3: read gate

For each in-memory insert entry of the probed table (unchanged entry gate:
live in-memory rows), in order:

1. If the filter can prove the whole batch misses (I4), skip the batch:
   one `MayMatchKeyRange` call, no encoding.
2. Otherwise, if the filter is a validated single-key EQUAL probe on the
   entry's primary-key column and no membership filter is attached, answer
   from the entry's hydrated index: unbuilt index means one hydration;
   definitive miss skips the batch; hits produce exactly the offsets whose
   keys equal the probe (tombstones still apply in the unchanged shared tail,
   so deleted rows keep not surfacing).
3. Everything else takes the original `FilterVector` path, bit-for-bit the
   same as before this change.

### State ownership and lifecycle

| State | Created by | Invalidated/retired by | Bounded by |
|---|---|---|---|
| `Entry.pkRange` (<= 2 key clones) | write path, once per entry, under mutex | release; failed merge append disables; successful merge unions | two key lengths per entry; uncharged |
| `Entry.keyOffsets` | reader's first qualifying probe, under mutex | `unaccountInsertKeyOffsetsLocked` at merge/shrink/release/dump/drop, ALTER duplicate (nils the copy), teardown | estimated charge (below); lives <= one layout epoch |
| `Entry.keyIndexAccountedSize` | `accountInsertKeyOffsetsLocked` before the map is allocated | the same choke point, exactly once | mirrored by charge/refund equality; validated by the debug accounting assertion |
| vacated `writes` tail slots | pre-existing compactions | cleared at all four compaction sites | retired entries unreachable |

ALTER-in-transaction: `transferWorkspaceWritesForAlterLocked` duplicates the
entry's batch and deliberately transfers neither the hydrated map nor the
charge; the duplicate re-hydrates under its own charge and the original
retains both until its (now fully deleted) batch is released. Two owners of
one charge are impossible by construction, not by discipline.

### Memory bounds and accounting

The estimate `insertKeyOffsetsEstimateSize` charges, per hydrated entry:
`64 + sum over rows of (len(key) + 48 + 4)` bytes — copied key bytes, one
amortized map bucket and the string header per row, one uint32 offset slot
per row, the map base. It is a deterministic amortized upper bound used so
charge/refund are exact mirrors without walking the built map; it is budget
arithmetic, not heap instrumentation. Worst case charges the additional
space `O(PK-column bytes of the transaction's live insert rows + 52/row +
64/batch)`; that is bounded above by roughly the primary-key column's own
in-memory size, and the same spill thresholds that protect the batches now
protect the indexes (I5) — a workspace whose indexes push it over a
threshold spills; nothing new needs a separately tuned limit. Index lifetime
is bounded by the transaction; there is no cross-transaction state of any
kind.

### Failure, error and terminal paths

- Merge append error: destination envelope is disabled (fail-open), both
  batch charges are restored from actual state, and the stale destination
  index has already been dropped before the append; the operation fails as
  before, with no partially indexed layout reachable.
- Hydration requires one map allocation on the read path; the charge is
  taken before that allocation, so any allocation failure aborts the
  transaction the way any allocation failure on this path does today, and
  the charged bytes go away with the transaction. A freely declined
  hydration (`built == false`) only costs the unpruned path that was the
  status quo.
- Commit: surviving file-backed entries leave the in-memory path; drained
  entries' indexes are refunded at the drop points; teardown refunds any
  remainder. Rollback: teardown refunds everything. Either way the debug
  accounting assertion ("workspace accounting mismatch") plus the per-entry
  consistency check (`keyOffsets != nil` iff charge nonzero) would catch a
  leak or double refund in debug builds.

## Cost model

- Point-update workload (issue #26796): per statement, O(1) byte comparisons
  for provable misses, O(1) hash probe per candidate batch plus at most one
  amortized build per layout epoch; no per-row encode. Measured post-fix:
  10,000 parameterized single-row point UPDATEs in one transaction,
  1,000-statement window medians 0.81–1.70 ms with late/early growth ratio
  1.040–1.111 (noise-scale), COMMIT 0.13–0.20 s, exact post-commit
  validation (count/distinct/sum/bad = 10000/10000/10000/0); at base the
  same window measurement grew 3.628x late/early.
- Ordinary read-mostly workloads: no workspace inserts means no state is
  ever created; the loop's per-entry work is one comparison only when the
  filter is valid and the entry holds live in-memory rows.
- Write-heavy workloads without in-transaction point re-reads: one extra
  primary-key column encode per insert batch at write time — the same
  encode the first read previously paid — plus two key clones. This is the
  recorded honest cost of the design; the commit-latency measurement above
  covers it.
- Range and IN statements: the envelope layer can now also skip provable
  misses per operator (`MayMatchKeyRange` models EQUAL, IN, closed ranges
  (BETWEEN and the half-open variants), and the strict comparison operators;
  everything else fails open), at the cost of one bounded comparison per
  batch.
- ALTER, spill, and teardown add only refund arithmetic per entry (O(1)
  each).

Precedent inside MatrixOne: committed-data reads already bound block-level
probes with zonemap/bloom metadata before per-row checks. This design gives
the transaction-private batches the same two tiers — zonemap-style envelope
skip and exact probe — replacing the last unbounded per-statement scan of a
transaction's own writes.

## Alternatives considered

1. Status quo (per-statement full `FilterVector` over every batch). The
   defect itself; rejected on the measurements above.
2. Eager transaction-global key-to-entry index maintained at write time.
   Answers probes in one hop, but would charge memory and maintenance
   against workloads that never perform in-txn point reads, must be kept
   consistent at every merge/shrink/dump/ALTER anyway, and charges for keys
   the transaction may never probe. Rejected: lazy hydration pays nothing
   until a probe proves the need, and rides the same invalidation points
   that exist today.
3. Envelope-only pruning (no exact index). Two key clones per batch and one
   comparison per read, but fat merged batches usually cover the whole
   domain, so point statements would still re-encode nearly all rows. Kept
   as layer 1; insufficient alone, which the merged-batch observation shows.
4. Per-entry bloom filter (precedent: committed block metadata). Same write
   cost as layer 1, but answers only "maybe present"; hits still need the
   full `FilterVector` pass, so the per-statement asymptotics for point
   probes stay the same as status quo. Rejected: an exact structure costs
   the same one encode `FilterVector` would pay once, and removes the work
   rather than probabilistically filtering it. (Membership-filter scans,
   which keep their bloom path unchanged, are exactly the case that fails
   closed here.)
5. Sorted-copy or binary-search over each batch's keys. Equivalent memory
   to layer 2 but rebuilds on every layout mutation and complicates
   repeated-key handling; no advantage over a hash map for single-key
   equality.

Chosen composition: write-time envelope + lazy exact-key index + single
choke-point invalidation + fail-closed gates — the only alternative that
bounds both the miss side (envelope) and the hit side (exact probe) without
inventing a new eager structure or weakening any existing path.

## Compatibility and operations

The new state is entirely transaction-local and derived: no catalog,
on-disk, wire, configuration, or upgrade surface changes; restart/backup/
restore see no difference; rollback is revert. Existing spill thresholds
are unchanged — only the size they sum now includes retained index bytes,
so an index-heavy workspace by design spills no later than an equivalent
batch-heavy one (accepted operational consequence, exercised in the live
benchmark). No new background work, goroutines, locks, waits, metrics or
knobs. Security and multi-tenant boundaries are untouched: the index holds
copies of bytes already resident in the transaction's own memory, and
charging it to the workspace budget keeps bounded memory honest under the
existing spill containment.

## Validation plan (mapped to executed evidence)

| Behavior / invariant | Cheapest deterministic proof | Result |
|---|---|---|
| I1 envelope lifecycle | `TestIssue26796EnvelopeWrittenOnceAndKeptByLifecycle`, `TestIssue26796PruningKeepsReadSemantics` | pass at head |
| I2 invalidation across merge/shrink/release/dump | `TestIssue26796KeyIndexLifecycle` | pass at head |
| I3 charge/refund singleness incl. teardown | `TestIssue26796KeyIndexMemoryAccounting`, `TestIssue26796TeardownClearsKeyIndexesWithDebugAccounting` (red at base: debug-accounting panic) | pass at head |
| I4 fail-closed gate incl. membership filter | `TestIssue26796MembershipFilterGatesExactProbePath` | pass at head |
| I5 spill sizing sees index bytes | `TestIssue26796SpillSizingSeesHydratedKeyIndexBytes` (red at base: batch-only numbers) | pass at head |
| I6 compaction clears retired slots; dump-all hole panic | `TestIssue26796WorkspaceCompactionClearsRetiredIndexSlots`, `TestIssue26796DumpAllClearsVacatedSlotsThroughFullSlice` (red at base: index out of range) | pass at head |
| ALTER never clones ownership | `TestIssue26796AlterTransferResetsKeyIndexOwnership` (red at base: accounting mismatch) | pass at head |
| The contract itself (bounded per-statement work) | `TestIssue26796PointReadWorkStaysBounded`, `TestIssue26796WorkspaceMaintenanceWorkBounded`, `TestIssue26796WorkspaceMergingBoundsEntryCount` (red at base: 64/512/400 rows encoded vs bound 8; first-statement failure) | pass at head |
| SQL-visible behavior unchanged | BVT `test/distributed/cases/disttae/disttae_filters/issue_26796_long_txn_point_updates` (328/328) | pass at head |

Recorded commands and outcomes (implementation head): focused suite
`go test -run 'Issue26796' ./pkg/vm/engine/disttae` 13/13 pass; package
suites `disttae`, `disttae/cache`, `disttae/logtailreplay`, `disttae/route`,
`disttae/sidecar`, `readutil` ok; mo-tester replay of the BVT case
328/328; live 10k-statement benchmark as in the cost model; CI on head:
build, Ubuntu UT, CN e2e BVT, coverage, Iceberg tidy all green. Broader
suites (multi-CN compose BVT, upgrade) remain on CI; they do not change
behavior family beyond what the focused proofs and BVT already cover, and
no exclusion was weakened to obtain them.

## Drawbacks, risks, open decisions

- Write-side encode for every insert batch (the ordinary-workload cost
  above) is real even for transactions that never point-read in-txn;
  measured negligible on the commit-latency record, but a dedicated
  write-only benchmark would separate it further (non-blocking; owner:
  disttae reviewers, decision point: only if a future write-heavy
  regression report implicates `computeInsertKeyRangeLocked`).
- The budget estimate is amortized upper-bound arithmetic, not allocator
  truth; it exists so charging is a mirrored pair. Making it exact heap
  tracking would add instrumentation the accounting does not need
  (non-blocking; same owner, revisit only with evidence of drift affecting
  spill behavior).
- Predicates outside the modeled operator set (prefix ops, disjunctions
  beyond per-disjunct evaluation, membership filters) keep the unpruned path
  - correctness in favor of optimization; extension needs its own evidence
  first.
- Draft PR #27013 (workspace redesign) touches the same structures; textual
  rebase conflicts are expected at merge time and will be resolved then.
- Known implementation-risk surface already covered by tests: dump-all with
  retained DROP/TRUNCATE holes (the panic fixed by the full-length clear),
  ALTER transfers, and teardown under commit/rollback debug accounting.

## Design decisions

- Lazy per-batch exact index charged to the existing workspace budget, over
  an eager global structure: charge only what a real probe needs; spill
  protection stays uniform instead of adding a second accounting domain.
- Envelope and index coupled to the same write-time soundness predicates
  (`pkCheckReady`, resolved column, no NULLs): each layer proves
  encodability once instead of two gating regimes.
- One invalidation choke point owning map drop plus refund, consumed by
  every mutation and terminal path, over distributed "remember to nil it"
  conventions; the debug accounting assertion backstops it in debug builds.
- Exactness over filters: the index answers only provably exact single-key
  probes; everything else fails closed.
- Honest spill sizing (`inMemEntryDataSize`) over separate index thresholds:
  one budget, one behavior to operate.

## Design approval record

| revision | reviewer | decision | date |
|---|---|---|---|
| 1 (this document) | pending | awaited before implementation approval | 2026-10-21 |

Per the gate contract, a decision on this revision (or a follow-up revision
linked here) is required before the implementation on PR #30685 is approved.
