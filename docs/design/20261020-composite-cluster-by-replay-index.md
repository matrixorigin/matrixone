# Design v1: Composite CLUSTER BY secondary index for CN logtail replay state

| Field | Value |
|---|---|
| Status | `pending design review` (revision 1, 2026-10-20) |
| Owning issue | [#26996 — unflushed CLUSTER BY time-series reads linearly scan CN logtail state](https://github.com/matrixorigin/matrixone/issues/26996) |
| Implementation | PR [#30617](https://github.com/matrixorigin/matrixone/pull/30617) (single-PR series; `fix(disttae): prune unflushed composite CLUSTER BY reads in CN logtail replay state`) |
| Proposed design approvers | disttae/replay owners among the PR's requested reviewers: `LeftHandCold`, `aptend`, `aunjgr` |
| Places this design under review | `feature-design-review` contract in `.agents/skills/mo-dev/references/feature-design-review.md`; raised as a blocking finding by the mo-bot reviews of 2026-10-19T23:47Z and 2026-10-19T23:48Z on head `4a87cdc39e` |

## 0. Classification record

```text
Change scope: whole change = PR #30617 (one PR; no stacked/split series)
Trigger: both the default size trigger (936 non-generated production lines
         across 9 production files: +803/-133, over the 500-line threshold)
         and complexity triggers (new long-lived stateful abstraction -- a
         per-partition secondary index retained with the replay state; change
         to a material hot path -- every replayed insert and every committed
         in-memory read).
Class:     not an ordinary fix. The PR label is "BUG", but the contracted
           rule classifies real scope, not the label: the root-cause repair
           introduces the secondary index and era state, so the design-first
           gate applies.
Design:    this document, revision 1, embedded in the implementation PR.
           Per the contract's review order, implementation approval remains
           blocked until this revision passes design review; the mo-bot
           reviews of 2026-10-19 have already blocked on exactly that gate.
Decision log: section 6.
Decision:  PASS | REQUEST_CHANGES — to be recorded by the design approver.
```

## 1. Problem, evidence, and invariants

### 1.1 Observed problem and evidence

Issue #26996 reports, with a reproduction on commit `c3ba9c7df1`: a
`CLUSTER BY(tenant_id, device_id, metric_id, event_ts)` table without a primary
key ingests 100,000 committed, unflushed rows; a selective predicate returning
2 rows scans all of them through the CN logtail replay state
(`EXPLAIN ANALYZE`: `inputRows=100000, outputRows=2, ReadSize=0`), stable over 60
seconds of polling, median latency ~44-45 ms. After an explicit flush, the same
query scans one persisted block (8,192 rows) in ~19-27 ms. `ReadSize=0` shows the
slow path is in-memory replay data, so persisted-object pruning does not help
unflushed strong-consistency reads.

### 1.2 Current behavior and violated contract

The CN query-side `PartitionState` (`pkg/vm/engine/disttae/logtailreplay`)
retains unflushed committed row versions in a `rows` B-tree ordered by
`BlockID`, `RowID`, commit timestamp — an order that does not support
cluster-key seeking. `HandleRowsInsert` additionally maintains one secondary
index, `rowPrimaryKeyIndex`, and the committed-read fast path
(`LocalDisttaeDataSource.filterInMemCommittedInserts`,
`materializedSnapshotDataSource.buildCommittedInMemBatches`) only takes an
indexed iterator when the predicate converts to a primary-key `MemPKFilter`.
A composite cluster-by table has no primary key, and its sort key never
converts to a PK filter in that path, so reads fall back to `NewRowsIter`,
which visits every visible row version. The violated contract: **a read whose
predicate is a prefix/range predicate on the table's declared sort key must not
scan data unrelated to the predicate.** Persisted objects honor this through
block zone maps and bloom filters; the replay state has no equivalent for the
compound cluster key.

### 1.3 Restored invariant, goals, non-goals

**Invariant (correctness authority).** Every read returns exactly the set of
row versions visible at its snapshot timestamp that satisfy the predicate. The
`rows` tree remains the single visibility authority (MVCC versions, physical
deletes, in-flight vs committed, snapshot TS). A secondary index may only
*propose candidate row identities*; it may never decide visibility, never be
the only home of a version, and never remove a row from consideration that the
rows tree retains.

Fail-open requirement: any case in which the index cannot *prove* complete
coverage of the queried sort key falls back to the general `NewRowsIter`
exhaustive path. The cost of every guard failure is the pre-fix behavior
(a linear scan), never a wrong result or a missing row.

**Goals.**

- Give composite cluster-by tables seek-based pruning of committed unflushed
  rows for equality, prefix-equality, IN, prefix/range and OR-of-range
  predicates, on tables with or without a primary key.
- Reuse the existing encoded-key domain, filter parsers and iterator
  machinery shared with the primary-key path instead of inventing a parallel
  stack.
- Keep replay and reads lock-free for readers and single-mutator for writers,
  on the existing `Partition.MutateState` copy-and-publish model.

**Non-goals.**

- Single-column `CLUSTER BY` in-memory pruning: the hidden serialized column
  exists only for compound keys, so single-column cluster-by tables keep the
  current read path. (Recorded as a non-blocking open question, not silently
  dropped.)
- Persisted-object pruning, block zone maps and bloom filters: unchanged.
- Primary-key read semantics: unchanged and separately guarded; the cluster
  filter never participates in PK exact-hit shortcuts.
- Any change to the logtail protocol, DN write path, catalog, on-disk format,
  wire format, or configuration contract.

### 1.4 First owners of every new state

| New state / resource | Owner |
|---|---|
| `PartitionState.rowClusterIndex` (per-partition B-tree of cluster-key entries) | `PartitionState`, created in `NewPartitionState`, mutated only inside `Partition.MutateState` windows (single replacer), published by CAS like the sibling `rows` and `rowPrimaryKeyIndex`, removed whole when the partition goes away |
| `RowEntry.ClusterIndexBytes` (encoded compound key of an inserted version) | The row entry that carries it; same GC lifecycle as `PrimaryIndexBytes`; freed by `deleteRowIndexes` when the version is physically removed |
| Era descriptor: `clusterIndexAttrName`, `clusterIndexIncomplete`, `clusterIndexSawPlainBatch` | Three scalar fields (one string, two bools) beside the index; updated only by `trackClusterIndexEra` inside `HandleRowsInsert`; copied verbatim by `PartitionState.Copy` into published snapshots |
| `MemClusterKeyFilter` (read-side filter, value inside `MemPKFilter`) | Per-read, allocated by `readutil.NewReader`, dropped when the reader is closed; attaches to `MemPKFilter` so existing per-read lifetime applies |

### 1.5 Assumptions (verified in code)

- Replayed insert batches carry the hidden compound column as a real schema
  attribute whose name starts with `catalog.PrefixCBColName`
  (`__mo_cbkey_…`), positioned among the user columns after `rowid`/commit-ts,
  typed VARCHAR, and the name is derived from the ordered clustered column
  list (`pkg/sql/util.BuildCompositeClusterByColumnName`; DDL/ALTER rebuild it
  in `build_ddl.go` / `build_alter_add_column.go` /
  `build_alter_change_column.go`), so a name change means the key domain
  changed.
- Encoded key bytes are stable clones (`types.Packer.Bytes()` returns
  `slices.Clone`), so retained index bytes do not alias packer scratch space.
- `PartitionState` indexes use `github.com/tidwall/btree` v1.7.0, whose
  `Copy()` is copy-on-write and O(1): publication cost matches the existing
  `rowPrimaryKeyIndex` exactly.
- Publication is single-mutator: `Partition.MutateState` copies the loaded
  state and publishes with `CompareAndSwap`, panicking on concurrent mutation;
  published copies are immutable for readers.

## 2. Standards, precedent, and alternatives

### 2.1 Applicable precedent

There is no external interoperability standard to satisfy: the replay state
is node-internal. The applicable precedent is internal architecture plus
well-understood industry patterns:

- **Internal precedent (primary source).** `rowPrimaryKeyIndex` is the exact
  structural precedent: a per-partition secondary B-tree keyed by an encoded
  sort key whose entries point at row identities (`PrimaryIndexEntry`:
  `Bytes`, `RowEntryID`, `BlockID`, `RowID`, `Time`, `Deleted`), validated
  against the authoritative `rows` tree through `isPKItemValid`. The design
  deliberately reuses the entry type, the `Less` ordering, the iterator type
  (`primaryKeyIter`), the union iterator, and the publication/GC integration,
  so the new index inherits a proven concurrency and lifecycle model rather
  than adding a second one.
- **Industry precedent.** Nonunique secondary indexes in MVCC stores
  (InnoDB secondary indexes; LSM iterators over sorted runs) share the same
  shape: a nonunique index proposes candidates; an authoritative record
  decides visibility. Open interval bounds are walked with successor seeks
  rather than linear stepping in standard index practice; the design encodes
  the successor as `key || 0x00` (full successor) and last-byte increment
  (prefix successor), which are the standard byte-string successors.

MatrixOne differs from the InnoDB precedent in two ways the design respects:
the replay state is copy-on-write-per-publication (no lock-coupling iteration,
so no new locks are needed), and retention is defined by object GCK truncation
rather than a page cleaner, so index bounds ride the existing GC exactly.

### 2.2 Alternatives considered

- **A0 — status quo (linear scan).** Correct, zero extra cost, but leaves the
  reported defect: every selective composite-cluster-by query pays O(retained
  rows). Rejected: the issue's evidence shows 100k rows scanned for a 2-row
  result.
- **A1 — no index; vectorized predicate pushdown over the `rows` tree.** Keep
  the rows tree as-is, but evaluate the compound cluster column per visited
  row/batch before materializing rows. Cheaper per row, still O(retained rows)
  per query and per row version; does not restore the invariant that a
  selective query touches data proportional to its selectivity. Rejected: it
  re-baselines the constant, not the asymptotics.
- **A2 — per-batch zone-map metadata (min/max per memtable batch).** Bounded
  memory O(#batches), but pruning granularity is a whole batch; the target
  workload is explicitly out-of-order ingestion, which widens per-batch
  min/max ranges toward uselessness, and equality probing is impossible.
  Rejected as the mechanism. A non-blocking open question records it as a
  possible complement later.
- **A3 — lazy index construction on first cluster query.** Zero replay cost
  until a cluster query arrives. But compiled against the architecture it is
  incoherent: published `PartitionState` copies are immutable, so a lazily
  built index must either mutate published state (breaking the single-writer
  publication contract under concurrent readers) or be rebuilt per read
  (O(retained rows) per query, which is A1 with more steps). Rejected.
- **A4 — eager per-version secondary index on the replay state (selected).**
  Mirror `rowPrimaryKeyIndex` one-to-one: one entry per retained inserted
  version, keyed by the already-serialized compound value the logtail
  carries. Reads become O(log N + matches + interval width); costs are eager
  replay maintenance plus retained key bytes, both bounded by the same
  object-GC rule that already bounds the rows tree (section 3.4), and a
  completeness state machine that fails open (section 3.2).

**Decision log (why A4 wins).** A4 is the only alternative that restores the
selectivity invariant; its costs are bounded by structures that already
exist row-for-row (`rows`, `rowPrimaryKeyIndex`), it needs no new locks,
workers, or protocol, and its failure mode degrades to A0 behavior (fail
open), which is the status quo rather than a regression. Serialization cost at
replay is avoided by indexing the logtail's pre-serialized compound column
instead of re-encoding the parts.

## 3. Logical and architectural closure

### 3.1 Definitions and end-to-end flow

End-to-end trace of one inserted version and one pruned read:

1. **Filter construction (read side, per reader).** `readutil.NewReader` builds
   the unchanged PK filter and, in parallel, `NewMemClusterKeyFilter` from the
   same scan expression. It is valid only when the table's sort key is a
   composite cluster-by column whose compound column is VARCHAR and no
   clustered part uses a native Unicode collation (such parts keep collation
   semantics on the residual path); otherwise it is invalid and the read is
   exhaustive, as today. Parsing and encoding share
   `constructBaseSortKeyFilter` / `encodeMemSortKeySpecs` with the PK filter,
   so both keys live in the same encoded-key domain and spec algebra (EQUAL,
   PREFIX_EQ, IN, PREFIX_IN, ranges, prefix ranges, IN_RANGE, OR of these).
2. **Replay ingestion (write side, per insert batch).** `HandleRowsInsert`
   scans the batch attribute names by prefix: expected outcome is exactly one
   attr carrying `__mo_cbkey_`, positioned after `rowid`/commit-ts, with a
   VARCHAR vector (`encodeClusterIndexKeys`). `trackClusterIndexEra` then
   decides (a) whether this batch's keys are indexable at all and (b) whether
   the batch ends the index's claim to completeness.
3. **Indexing.** For each retained row version: `p.rows.Set(entry)` (existing,
   authority), `p.rowPrimaryKeyIndex.Set` (existing), and — when the era
   check passed — `p.rowClusterIndex.Set(entry)` with
   `Bytes = ClusterIndexBytes[i]`, sharing the `PrimaryIndexEntry` identity
   `(Bytes, Time desc, RowEntryID desc)`. Physical deletes carry no cluster
   bytes and never enter the index; a deleted version disappears from `rows`,
   so validation at read time rejects its stale index candidate, exactly like
   the PK index.
4. **Publication.** `Partition.MutateState` → `PartitionState.Copy()` →
   `CompareAndSwap`. `Copy()` adds the three era fields verbatim and the index
   via O(1) copy-on-write `btree.Copy()`, identical in shape and cost class to
   `rowPrimaryKeyIndex`.
5. **Pruned read.** Both committed in-memory datasources choose, in priority:
   valid PK filter → PK iterator (unchanged semantics); valid cluster filter
   → `PartitionState.NewClusterKeyIterWithFilters(ts, colName, specs)`;
   otherwise → `NewRowsIter` (exhaustive, unchanged). The cluster iterator
   walks B-tree order with the match spec (equality uses the nonunique
   `ExactNonUnique`, which never stops at the first hit), yields candidate
   entries, and `isPKItemValid` resolves each candidate against the `rows`
   tree at the read timestamp. Residual SQL predicates, workspace
   tombstones, persisted tombstones, and the no-early-exit rule (one replay
   match must not end the query while persisted matches remain) are outside
   the change and unchanged.

### 3.2 Era state machine: schema transitions and completeness

Per-state fields: `clusterIndexAttrName`, `clusterIndexIncomplete`,
`clusterIndexSawPlainBatch`. Pruning through the index is licensed only by
`ClusterIndexReadyFor(colName)`: `clusterIndexIncomplete == false` and
`clusterIndexAttrName == colName` (both the query's and every retained
version's key were built under the same compound name).

Transitions (all inside `HandleRowsInsert`, single-writer):

| # | Batch shape while state is… | Result |
|---|---|---|
| 1 | fresh state (`attrName=""`, not incomplete, no plain seen) sees one valid `__mo_cbkey_*` attr | `attrName := name`; batch's rows indexed |
| 2 | `attrName == name` | indexed, no state change |
| 3 | `attrName != ""`, batch's compound name differs (a clustered part was renamed/reordered/added/dropped — the ALTER/DDL builders rebuild the name) | `incomplete := true` (sticky); batch not indexed |
| 4 | `attrName != ""`, batch has no prefixed attr (compound column disappeared) | `incomplete := true` (sticky) |
| 5 | fresh state, batch has no prefixed attr (ordinary table, cluster column not yet seen) | `sawPlainBatch := true`; nothing indexed |
| 6 | `sawPlainBatch == true` and `attrName == ""`, batch now carries a compound attr | `incomplete := true` (sticky) — earlier retained versions can never be indexed |
| 7 | more than one prefixed attr (a user column legally shadows the reserved prefix) | `incomplete := true` (sticky) |
| 8 | prefixed attr is not VARCHAR (a user-typed column with the reserved prefix, e.g. GEOMETRY) | treated as case 4/5 (batch unindexed); key encoder's limited type domain is never exercised on user data |
| 9 | prefixed attr sits before `rowid`/commit-ts or lacks its vector (malformed batch) | treated as case 7: ambiguous, sticky incomplete |

**Sticky semantics are deliberate and sound**: `PartitionState` is append-only
— versions already retained without an index entry can never be recovered
after the fact, so the first completeness violation permanently disqualifies
that state (and every published copy; `Copy` carries the flags) from
pruning. Stickiness also skips maintenance cost for later batches. The
correctness direction is fails-closed; the cost of stickiness is bounded by
the worst-case fallback to the pre-PR exhaustive read (case A0). After a CN
restart the state rebuilds from checkpoint + logtail, so a table whose replay
history is consistent under one schema revision regains pruning; a table
whose history genuinely spans eras stays conservative until restart, which is
the price of never having to prove per-version "is this version indexed"
bookkeeping.

The queried side of the contract is likewise verifying: the filter's
`ColName` comes from the table definition, and `ClusterIndexReadyFor` refuses
to prune unless it matches the replayed name exactly, so a query under a new
schema revision cannot seek in a stale key domain.

### 3.3 Idempotence, duplicate delivery, partial state, reset/reuse, restart

- **Repeated delivery / idempotence.** `HandleRowsInsert` indexes what the
  `rows` tree retains. The entry identity `(Bytes, Time, RowEntryID)` and the
  per-row `p.rows.Set` are the same identity model the PK index has used
  since its introduction, so replaying a batch twice produces the same
  rows-tree/index relationship as replaying it once
  (`TestClusterKeyIndexRepeatedReplayIdempotent`).
- **GC (four physical-removal paths).** `handleDataObjectEntry`,
  `handleTombstoneObjectEntry`, `HandleDataObjectList`,
  `HandleTombstoneObjectList` each end in `p.rows.Delete`; all four call the
  centralized `deleteRowIndexes`, which removes the PK entry (as before) and,
  when `ClusterIndexBytes` is non-empty, the cluster entry. No removal path
  can delete a row without deleting both indexes' entries.
  `TestClusterKeyIndexGCTrackedWithRows` proves the index drains with the
  rows tree.
- **Snapshot copies.** Published snapshots see the index through O(1) COW;
  later mutation of the new mutator copy cannot retro-change a snapshot
  (`TestClusterKeyIndexSnapshotCopyIsolation`).
- **Restart / reset.** No persistence: the index and the era fields are
  rebuilt from the logtail after restart; dropping the partition drops all
  of them with the state. There is no partial-initialization mode: a state is
  either complete for a name or sticky-incomplete, and the readers honor it
  from the first batch.
- **Errors and cancellation.** Replay insert errors abort the mutate window
  before publication (no state swap), as before. Reader-side cluster
  iterators use the existing `primaryKeyIter`/`RowsIter` close protocol
  (`TestClusterKeyIterCloseIsIdempotent`); materialized-snapshot read errors
  clean up iterators (`TestMaterializedSnapshotTombstoneErrorCleansUp`).

### 3.4 Resource bounds and backpressure

- **Index size** is exactly proportional to retained inserted versions
  carrying a valid compound key: one `PrimaryIndexEntry` (fixed struct) per
  version plus the retained encoded key bytes (owned `slices.Clone` memory)
  plus the `RowEntry.ClusterIndexBytes` slice header. Deleted versions are
  never indexed. The bound owner is the existing object-GC truncation: when a
  memtable's appendable objects are flushed and truncated out of the replay
  window, the four removal paths delete the rows and their index entries. The
  index therefore has no independent growth path — it is bounded by the same
  quantity that already bounds `rows` and `rowPrimaryKeyIndex`, roughly
  doubling the per-version retained metadata of an *insert* version (not of
  deletes or persisted objects).
- **No queues, workers, timers, goroutines, files, or metrics are added.**
- **Insert hot path CPU.** Per batch: one attribute-name prefix scan —
  `O(#attrs)` short string-prefix comparisons, no allocations — plus, only
  for indexed batches, one pass of the existing `EncodePrimaryKeyVector` over
  the compound VARCHAR vector and one `BTreeG.Set` per row (`O(log N)`).
  Tables without the compound column (ordinary tables, PK-only tables,
  single-column cluster-by) never pay encoding or index writes.
  `BenchmarkClusterIndexInsertMaintenance` pins this cost as a benchmark.
- **Publication CPU.** COW `Copy()` is O(1) per tree, same class as before.
- **Read cost.** `O(log N + candidates)` where candidates are bounded by the
  interval width and match count; the iterator's `advances/surfaced/validated`
  counters make the cost observable at each boundary, and tests assert
  seek-vs-scan behavior at 1k/10k/100k rows with a nil-filter control
  (`TestClusterKeyIndexSeeksRatherThanScans`), not just wall-clock timing.
  Open/prefix interval bounds jump equal-key runs with a single successor
  seek (`fullKeySuccessor`, `prefixSuccessor`, `seekPastKeyRun`) instead of
  the previous linear `Next()` stepping. The same successor-seek fix is
  applied, behavior-preserving, to the *existing* PK `BetweenKind` specs,
  which previously walked equal-key runs step by step; entry positions
  reached are identical, so no PK read result changes
  (`TestClusterKeyIterSeeksPastOpenLowerBoundDuplicates`,
  `TestPrimaryKeyFilterUnchangedByClusterPath`).
- **Security / trust boundaries.** No new trust boundary: key bytes derive
  from logtail data the CN already trusts. A user column shadowing the
  reserved prefix cannot poison the index or inflate its cost beyond stickiness
  (cases 7–8), because name-and-type identity is checked and any ambiguity
  permanently disables pruning.

### 3.5 Precise contracts

- **API (in-Repo, package-local).** Replay/prod:
  `PartitionState.NewClusterKeyIterWithFilters(ts, colName, specs) RowsIter`,
  `NewClusterKeyIter(ts, op, keys)`, `ClusterIndexReadyFor(colName) bool`;
  read side: `readutil.NewMemClusterKeyFilter`,
  `ConstructBaseClusterFilter`, `MemPKFilter.SetClusterKeyFilter`,
  `ClusterKeyValid/Specs/TS/ColName`. No cross-service API, wire, or config
  contract is added or changed.
- **Key domain contract.** Pruning equality requires the query and the index
  to agree that keys are byte strings produced by `types.Packer` from the
  serialized compound VARCHAR column. Native Unicode-collation clustered
  parts keep collation semantics out of this domain by refusing the filter
  (residual path), mirroring the same refusal the PK filter performs today.
- **Nonunique semantics.** `ExactNonUnique` returns *every* equal-key entry;
  no exact-hit/affects-single-row shortcut may fire from a cluster or replay
  match (`TestClusterOneReplayMatchNeverArmsExactHit`) — `MemPKFilter.Exact`
  and `RecordExactHit` remain PK-only by construction.

## 4. Compatibility, delivery, and operations

- **Persisted/distributed state.** None. Nothing is written to disk, catalog,
  S3, wire, or configuration. Consequently:
  - **Upgrade / downgrade / mixed version / migration**: unchanged; a CN
    running this code masters its own replay state, peers of any version are
    unaffected.
  - **Restart / rollback / backup / restore**: the structure is rebuilt from
    the logtail after restart and vanishes on rollback (revert the patch);
    backups, restores and upgrade tests are unaffected structurally.
- **Defaults, gating, rollout, containment.** No feature flag or config knob:
  the era-completeness guard *is* the gate, and it is per-partition and
  self-reverting (each new state recomputes it). Failure containment is
  structural: every unproven case keeps the exhaustive path, so the worst
  deployed behavior equals the pre-PR behavior. Rollout is a single PR behind
  the normal CI gates (UT, SCA, BVT multi-CN compose pessimistic + PROXY,
  coverage).
- **Fallback and removal plan.** Failing open (`NewRowsIter`) is the
  automatic fallback. Removal is the mirror of the change: deleting the index
  fields and the guarded iterator selection restores today's read shape with
  no data or compatibility debt.
- **Observability and diagnosis.** Existing datasource summary buffers print
  the iterator kind (`cluster-key` beside the existing `primary-key`) so
  support can tell pruning from exhaustive reads in EXPLAIN-level traces; no
  new metrics are introduced (a non-blocking open question records an optional
  counter for sticky-incomplete states if maintainers want one).
- **Performance and capacity model, summarized for review.** Affected
  workloads: selective prefix/range reads on composite cluster-by tables
  change from O(retained rows) per query to O(log N + candidates)
  (issue repro: 100k visited rows, ~45 ms → matching-candidate visits; the
  datasource golden shows exactly 2 candidates examined out of 100,000
  retained rows, and the real-logtail integration shows 13 blocks pruning to
  the matching block's prefix keys). Ordinary workloads: tables without a
  compound key pay only the per-batch prefix scan during replay
  (allocation-free, O(#attrs)); nothing else in their write or read path
  changes. Cluster tables pay the indexed-ingestion cost above, bounded by
  retained versions, verified by the maintenance benchmark and the
  allocator-return-to-zero assertions in the integration test.
- **DoS posture.** The only new per-request work is building the cluster
  filter from an already-parsed expression; malformed or non-representable
  shapes simply invalidate the filter. No unbounded retry, queue, or fan-out
  exists in the design.

## 5. Verification

Validation follows the cheapest-deterministic-proof rule; each invariant maps
to a test already present in the implementation (commands and full outcomes
are recorded in the PR description; committed CI on the exact head has UT,
SCA, coverage, PROXY and pessimistic multi-CN BVT green):

| Invariant / risk | Deterministic proof |
|---|---|
| Reported defect reproduced & fixed | Parent/base: `TestClusterReplayDataSourcePrunesToMatchingCandidates` fails (`expected 2, actual 100000`); same test passes on the head with 2 candidates examined (nil-filter control: 100,000, sum mismatch proves the control path scans). Also `TestClusterReplayDataSourceReturnsAllDuplicateClusterKeys` (parent `expected 9, actual 10`) |
| Real logtail, not synthetic batch only | `TestClusterByRealLogtailIntegration`: production `HandleSyncLogTailReq` producer → `HandleLogtailEntry` consumer over genuinely committed unflushed TAE data; real `FlushTable`; block filtering 13→1; allocator returns to zero |
| Nonunique correctness (counterexamples to "first match wins") | `ExactNonUnique` + duplicates/multi-batch/NULL-part tests + 17×7 indexed-vs-exhaustive differential `TestClusterKeyIterMatchesRowsIter(RandomReplay)` |
| Visibility authority stays with `rows` | `isPKItemValid` reuse; `TestClusterReplayDataSourceDeletesAndSnapshots`, `TestMaterializedSnapshotClusterEntryCoversPruningAndFallback` |
| Era/schema-transition safety | `TestClusterReplayDataSourceFallsBackAcrossSchemaEras`, era matrix + ambiguous/shadow/non-VARCHAR cases (`…IgnoresAmbiguousCompoundAttrs`, `…FallsBackWhenUserColumnShadowsClusterPrefix`, `…IgnoresNonVarCharPrefixedUserColumn`), position independence (`…ResolvesCompoundAttrIndependentlyOfPosition`) |
| Index never drifts from `rows` across lifecycle | GC/Copy/idempotence trio (`…GCTrackedWithRows`, `…SnapshotCopyIsolation`, `…RepeatedReplayIdempotent`) |
| Pruning is seek-shaped | seek-vs-scan counters at 1k/10k/100k + nil-filter control; open-bound successor seeking test; maintenance benchmark |
| Filter/planner plumbing | `readutil` filter shape/invalid/cleanup tests, `TestReportedClusterBySQLEmitsPrefixInRangePredicate` (byte-exact merged `prefix_in_range`), `TestNewReaderAttachesClusterFilter`, `TestPrimaryKeyFilterUnchangedByClusterPath` |
| User-visible SQL contract | BVT `test/distributed/cases/disttae/cluster_by_logtail_pruning.sql`: reported predicate (count=2/sum=39), duplicate keys, NULL components, update/delete lifecycle, rollback, commit, non-leading-prefix counterexample; passed live mo-tester comparison twice on one instance |

Acceptance for this design gate: (1) owners judge the invariant and ownership
model above sound; (2) remaining verification debt stated explicitly — full
CI suites beyond the focused sets are delegated to the PR's required checks,
which are green on the current head.

## 6. Drawbacks, risks, open questions, decision log

**Accepted tradeoffs.**

1. **Eager maintenance over lazy or no index** (A4 over A0–A3): indexed
   ingestion cost and retained key bytes in exchange for selectivity-shaped
   reads; bounded by the rows tree's own GC.
2. **Sticky permanence of `clusterIndexIncomplete`** within a state's
   lifetime: after a real era change (e.g. ALTER on a clustered table rebuilds
   the compound name), that partition stays conservative (exhaustive) until
   the state is rebuilt (restart or new state), even if pre-era versions are
   later GC'd. Chosen over per-version bookkeeping because the missing-proof
   direction is fail-safe and the cost equals pre-PR behavior.
3. **Indexing the pre-serialized compound column** rather than re-encoding
   parts: avoids replay-side re-serialization and keeps one key domain, at the
   price of trusting the DDL-era naming contract (guarded by the name check).
4. **Successor seeks also replace linear stepping in the existing PK
   BetweenKind specs**: behavior-preserving scope extension so the shared
   traversal has one implementation; covered by open-bound tests and
   `TestPrimaryKeyFilterUnchangedByClusterPath`.

**Risks.**

- Index/rows drift (missing or stale entries) — countered by the single
  `deleteRowIndexes` removal path at every `rows.Delete` site, the
  insert-side era checks, the GC/Copy/idempotence tests, and the
  differential tests; drift-to-the-right (extra stale entries) fails safe
  via rows-tree validation.
- Unmodeled compound-column shapes in future logtail changes — countered by
  the fail-open guards and the named counterexample tests.

**Open questions (non-blocking; owners needed).**

1. Single-column `CLUSTER BY` in-memory pruning (out of scope here; file a
   separate issue if wanted). Owner: disttae maintainers.
2. Per-batch cluster zone maps *in addition* to the row index (A2 as a
   complement) — only if profiles show the seek cost dominating on very wide
   predicates. Owner: performance maintainers.
3. Optional observability: a metric/trace field for sticky-incomplete
   partitions. Owner: observability maintainers.
4. Era recovery: whether to un-stick a state once every pre-era version is
   provably GC'd (cost: per-version bookkeeping). Recommended default is to
   keep the current, simpler fails-closed rule. Owner: disttae maintainers.

**Decision log.** A4 selected (section 2.2); rows tree kept as sole visibility
authority; sticky-incomplete chosen over era recovery; no new locks, workers,
protocol, config, or persisted state; design and implementation agree — any
material deviation from this document requires updating this document and
re-reviewing the affected sections before approval.
