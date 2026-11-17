# Nullable leading keys and deep-offset residual work in ordered pagination pruning

Status: approved-then-implemented revision 2.1 (design PASS in pre-pr review artifact
`state/artifacts/issue-32469/pre-pr-review-1794907250227.json` for revision 2, sha256
`e5e1c0926cdd628a8a5250045f429b063ff4ca5c8bf4e5d929fc9de247f7d838`; the only
post-approval edit applies that review's P3 advisory to the pseudocode below, which
reordered the placement checks to put NULLS FIRST first exactly as order.go's
comparator and the prose already resolved it; no invariant, rule, or decision changed)
Owning issue: [#32469](https://github.com/matrixorigin/matrixone/issues/32469) (split from [#32061](https://github.com/matrixorigin/matrixone/issues/32061))
Prior approved design: [20261108-ordered-pagination-pruning.md](20261108-ordered-pagination-pruning.md), approved at `89ca3dbbf6d594d7614b8e1ca80f6b0300ed3cd2`. That document non-goaled nullable leading keys ("a general NULLS FIRST solution requires a proof about NULLs in unread blocks") and OFFSET-prefix skipping ("separate complexity and product decision"). This revision reopens only those two deferred decisions and everything they strictly require; all other decisions of the approved design remain in force.
Design baseline: MatrixOne `0c8bd5e5f3be4ce5822015131611181ddca37d6d` (worktree `agent/issue-32469-main-ad59df17`).

## Problem

### 1. Nullable leading keys are excluded from the pagination cutoff

The approved mechanism reduces `ORDER BY lead LIMIT L OFFSET O` scans after
K = L + O accepted rows: `Order` publishes one conservative leading-key
cutoff; ordered readers then skip blocks whose zone map proves every stored
key strictly worse. The report's workload (`big8m.k2 INT` nullable, offsets
100000/800000/9000000) is refused at every gate:

| # | Gate | Location | Effect for a nullable leading key |
|---|------|----------|-------------------------------------|
| G1 | `order.SupportsScanCutoffType` requires `typ.NotNullable` | `pkg/sql/colexec/order/scan_cutoff.go` | compile admission returns no scan/tag |
| G2 | `order.ScanCutoff.Observe`/`batchBound` reject any batch with `v.HasNull()` and disable the cutoff | same file | even a wrongly admitted execution fails open |
| G3 | `RemoteDataSource.SetOrderBy` drops the whole order unless `Expr.Typ.NotNullable` | `pkg/vm/engine/readutil/datasource.go` | remote block lists stay unordered; no bound can skip |
| G4 | `ScanCutoff.Observe` counts NULL-free rows only, and the two supported NULL placements need different witness rules | `pkg/sql/colexec/order/scan_cutoff.go` | central design gap this document resolves |

The `LocalDisttaeDataSource` counterpart (`pkg/vm/engine/disttae/local_disttae_datasource.go`) keeps the order for nullable keys but, with G1/G2, never receives a bound. Existing executable proof of exclusion today: `pkg/sql/compile/scan_cutoff_test.go` `TestPaginationCutoffFromSQL{name: "nullable key"}` (column `tpch.returning_bool.n`, `types.T_int32`, nullable = true) requires `paginationCutoffScan` to return nil.

Measured on the design baseline (local single-CN launch, database
`repro32469.big1m`, 1,000,000 rows, deterministic session
`execType=2`, `query_max_workers=1`, `max_dop=1`; loader and logs under
`state/artifacts/issue-32469/repro/`): every `ORDER BY k2 LIMIT 10
OFFSET ...` arm (asc 100,000/500,000/900,000, desc 500,000; head-concentrated
NULLs) and both `ORDER BY k2s ...` arms (scattered NULLs) sorted exactly
1,000,000 Sort input rows with pruning enabled, identical to the
disabled arm — G1/G2 keep the whole report workload at full scan + full
sort cost.

The exclusion is justified for one half of the NULL-ordering space and overly
broad for the other half (proof in "Invariants"): when NULLs trail the
requested head, a NULL key can never occupy a head position once K non-NULL
witnesses exist, so zone-map skipping is as safe as the non-null case; when
NULLs lead the head, zone maps alone can never exclude a block, but the
already-recorded per-block, per-column physical null count can. Rejecting
both halves keeps the report's `ORDER BY k2 LIMIT ... OFFSET ...` pages at
full scan + full sort cost.

### 2. Deep-offset residual work and the inherent head region

After PR #32355, Sort input for an eligible scan is bounded by the *head
region*: every block whose zone map proves it worse than the published
K-row boundary is skipped, but the K = L + O rows themselves must still be
read and ordered, because with overlap-free proof of global order unavailable,
the boundary value at position K is only known after K rows have been
witnessed. Costs of a deep page therefore decompose as: read+sort K rows
(head region, inherent unless storage proves global order and exact visible
prefix counts — the exact-prefix alternative rejected as a product decision
in the approved design), skip the remaining blocks (delivered), and, for
nullable keys under this revision's rules, keep any head-leading block that
may contain NULLs. Keyset-level latency for a deep OFFSET page is not
achievable this way because SQL LIMIT/OFFSET semantics do not expose the
boundary value; the keyset form (`WHERE lead < last ORDER BY lead LIMIT L`)
remains the bounded alternative and is unchanged.

Random or overlapping block ranges keep the conservative per-block rule but
hold the skip rate near zero: when early blocks carry zone maps overlapping
the boundary region, blocks stay kept individually; the effect must not be
reverse-engineered into an unsafe prefix rule.

Measured on the design baseline (same launch and session as above):
pruning enabled, Sort inputRows for the eligible NOT NULL key measured
106,496 at `offset 100,000`, 508,192 at `offset 500,000`, 905,648 at
`offset 900,000` — ≈ K rounded up to 8,192-row block boundaries, i.e. the
head region — and 8,192 for `LIMIT 10` alone; pruning disabled
(`optimizer_hints = 'execType=2,sendMessageFromTopToScan=1'`) measured
1,000,000 Sort input rows in every arm, including `LIMIT 10`. The keyset
control `WHERE k > 500000 ORDER BY k LIMIT 10` needed 8,192 rows on the
same data: the O(K) prefix the boundary mechanism cannot skip is exactly
the work the keyset form avoids, while the randomized key (`krand`)
measured 1,000,000 Sort input rows with pruning enabled and disabled —
no skip survives overlapping block metadata, so no stronger head-region
promise is available from zone maps alone.

### 3. NULL boundaries under the shared TopValue boundary of the LIMIT-only path

The same receiver boundary already serves the LIMIT-only (no OFFSET) plan:
`Top.getTopValue` (`pkg/sql/colexec/top/top.go:1310`) publishes
`TopValueMessage{TopValueZM}` once the heap holds `limit` rows, and the
planner attaches the message for single-key nullable leading columns today
(`pkg/sql/plan/message.go` rejects only the multi-key nullable case). Two
NULL defects are **measured live on the design baseline** with
two-block counterexample tables (`hza`, `hzb`; loaders and query logs under
`state/artifacts/issue-32469/repro/hz_load.sql`, `hz_probe*.log`):

- **D1 — wrong page under NULLs-lead placement, no NULL root involved.**
  `getTopValue` publishes the heap-root row's raw slot without a NULL check;
  with a non-NULL root the published point bound is a valid value (e.g. 10
  for `LIMIT 10`). The receiver then loses rows: block zone maps are built
  from non-NULL extrema only (`Vector.GetMinMaxValue` skips NULL rows,
  `vector.go:11816`; `BatchUpdateZM`, `zm.go:1892`), and the keep rules
  (`LocalDisttaeDataSource.needReadBlkByZM`,
  `local_disttae_datasource.go:219-227`; `UsableOrderZoneMap`,
  `readutil/datasource.go:70`, checks type/scale/truncation but not
  nullability) compare intervals only. A block whose non-NULL interval lies
  entirely above the bound is skipped although `ColumnMeta.NullCnt() > 0`
  proves it stores NULL rows that rank into the head under ASC default
  placement. `hza` (block 1: two NULLs + values 3..8192; block 2: five
  NULLs + values ≥ 9008198): `SELECT k3 FROM hza ORDER BY k3 LIMIT 10` read
  only block 1 (`inputBlocks=1`) and returned `[NULL,NULL,3,4,5,6,7,8,9,10]`
  while the 2-key full-sort control returns
  `[NULL,NULL,NULL,NULL,NULL,NULL,NULL,3,4,5]`; `LIMIT 3` returned
  `[NULL,NULL,3]` vs control `[NULL,NULL,NULL]`. This is a wrong-result
  defect of already-merged #32355 code — reachable by plain SQL on a
  single-node launch through the local data source; the remote data source
  drops nullable orders (gate G3), and this revision deliberately keeps that
  gate, so D1 does not and will not fire on the remote reader.
- **D2 — garbage publish under an all-NULL heap root.** The same
  `getTopValue` raw-slot read publishes zero-valued bytes (fixed-width) or a
  garbage length payload (varlen) when the root row's key is NULL.
  `hzb` (block 1 all 8,192 rows NULL; block 2 values 193..8384):
  `... ORDER BY k3 LIMIT 10` read only block 1 and returned ten NULLs —
  exact only because all needed NULLs live in block 1; the garbage bound
  drove skipping of a still-populated value block. A NULL heap root must
  publish nothing and fail open.
- **D3 — deep-offset Top with K-resident heaps.** The compile-side cutoff
  admission still requires NotNullable (G1), so no deep-offset nullable plan
  reaches this machinery today — measured: nullable deep-offset arms sorted
  exactly 1,000,000 input rows, identical to pruning disabled. D3 (a heap
  filled with NULLs while delivered non-NULL rows stay below K publishes a
 garbage bound that drops head rows, including page-bearing rows when the
  table's total non-NULL population is below K) becomes reachable only when
  this revision admits nullable keys for deep offsets, so the publication
  guard and placement-aware receiver rules are prerequisites of the
  admission, not optional polish.

The DESC arms measured conservative-but-exact on the same probes (`hza` DESC
returned the value block first and matched its control; `hzb` DESC read every
block before answering), consistent with NULLs-trail placement having no
value-blind improvement window in these arrangements; correctness there is
still established by the witness rules below, not by absence of a
counterexample. Consequence for the invariants: under NULLs-lead placement a
block is skippable only when its physical null count is 0 **and** the
interval rule already skips it, and no publication may originate from a
NULL root. Both publishers (`top.go`, deep path `scan_cutoff.go`) obey the
same rules at the shared `SetFilterZM` boundary, whose enforcing receiver
for nullable plans is the local `disttae` reader; the remote reader keeps
its existing NotNullable order gate, so nullable plans cannot consume a
bound remotely at all (see Change map).

## Invariants and proof

### Effective NULL placement

Define for a single leading `OrderBySpec` `F` (values only; direction bit and
NULLS bits; secondary keys are irrelevant and stay out of scope):

```
nullsLast(F) := false                 if F has NULLS FIRST  // checked first, as
                                     //                        in order.go: a
                                     //                        both-bits flag
                                     //                        resolves here
              := true                 if F has NULLS LAST
              := (F has DESC)         otherwise          // order.go default
nullsLeadHead(F) := !nullsLast(F)
```

`order.go` resolves sorting exactly this way (NULLS FIRST, then NULLS LAST,
then `nullsLast := desc`), so a both-bits flag sorts as NULLS FIRST and the
rule mirrors it (`nullsLeadHead = true`). Single-key eligible plans clone one
spec through planner, compile and readers, so placement agrees everywhere;
compile admission and scan-scope pairing must still fail closed on any
mismatch, as they already do for the DESC bit.

### Witness and skip rules

For a scope with summary limit K, direction ASC or DESC, and placement P =
nullsLeadHead:

- Rule L (P = true, NULLs may lead the head): a batch witnesses its NULL
  rows always, and its remaining rows only when its outward extremum was
  usable under the "Certified witnesses" classes below; every skipped-block
  row ranks after every certified witness (ASC: NULL < each non-NULL
  candidate; DESC+NULLS FIRST is symmetric under reversal). A published
  bound B is valid iff it comes from a usable batch and
  `certifiedWitnesses(B) >= K`. Readers may skip a block only if its zone map
  proves every stored key strictly worse than B **and** the block's physical
  column null count is 0.
- Rule T (P = false, NULLs trail the head): a batch witnesses only its
  non-NULL rows, and only when its outward extremum was usable. A published
  bound B is valid iff `certifiedWitnesses(B) >= K`. NULL keys can never
  occupy a head position after that (DESC: every NULL ranks after all
  non-NULL; ASC+NULLS LAST symmetric), and a K-witness bound excludes every
  remaining non-NULL row beyond it, so readers skip on the zone map alone,
  ignoring null counts, exactly as the non-null case does.

### Certified witnesses

A row may count toward K only when its rank is provable relative to the
candidate bound B; row counting without that certificate is unsound. Batches
therefore fall into three classes:

- (a) **Usable batches** — a conservative outward extremum on the
  publication side exists: the non-NULL extrema materialize
  (`GetMinMaxValue` ok, or the const/sorted fast paths when the batch
  contains no NULLs), the zone-map point initializes, and the ASC side
  carries no `MaxTruncated` marker (the pinned base's exact refusal
  predicates, with `!zm.IsInited()`, `(!desc && zm.MaxTruncated())`, scale
  and grouping checks). Under L every row counts (non-NULL rows certify
  through batch extremum <= B; NULL rows certify structurally, since a NULL
  row ranks no later than every row). Under T exactly the non-NULL rows
  (rowcount minus nullcount) certify, always through the extremum.
- (b) **All-NULL batches** — no non-NULL extremum exists: their NULL rows
  count under L (structural rank), zero under T. All-NULL batches contribute
  no bound.
- (c) **Unusable non-NULL batches** — non-NULL rows exist but the batch
  fails the refusal predicates (representation-truncated outward extrema,
  a zone map that cannot initialize, e.g. decimal256, structural mismatch,
  length mismatch): their NULL rows count under L, their **non-NULL rows
  never count**, and they contribute no bound. Counting those rows would
  over-certify: with K = L + O large, two batches whose stored maxima are
  truncations of a repeated huge value preceding a mid-value batch would
  otherwise publish the mid-value bound with fewer than K rows provably at
  or head-ward of it, and a later zero-null block entirely beyond that
  bound would be skipped although it supplies the true page. The pinned
  base is safe today only because `Observe`/`batchBound`
  (`pkg/sql/colexec/order/scan_cutoff.go`) disables the whole cutoff on any
  unusable batch; this revision removes that wholesale disable (usable
  batches may still publish) but never the certification requirement, which
  preserves the base's safety while keeping the cutoff alive.

Empty batches (rows = 0) witness nothing. Publication semantics: a summary
may publish only a bound built from class (a) extrema; if the certification
count never reaches K, it simply never publishes (fail open), and the sort
itself always consumes every row of every batch exactly as today regardless
of batch class.

Proof sketch for both directions and placements: ASC bounds are the maximum
of class-(a) batch maxima, so every certified witness of a class-(a) batch
ranks no later than B; blocking a
block requires every stored key strictly greater than B's outward side
(DESC: mins/lower side symmetric). Under L, every skipped row ranks after
K certified witnesses because NULL rows are impossible in the block
(null count 0) and non-NULL rows rank after the witnesses, where the
witnesses are certified per the classes above — NULL rows structurally,
class-(a) rows through their batch extremum, and never a class-(c)
non-NULL row whose rank is uncertifiable. Under T, every skipped row
ranks after K certified non-NULL witnesses; NULL keys additionally rank
after all non-NULL rows, so they also rank after the witnesses. Blocks
with unusable zone maps, appendable blocks, and in-memory rows are never
skipped and follow the unchanged visibility filter. Zone-map equality
keeps blocks exactly as today (secondary keys and ties). In-memory and
workspace rows always flow to `Order`, so they witness and participate
without any metadata assumption.
The physical null count is an upper bound over any tombstone/snapshot state
(deletes only remove rows), so null count 0 proves no NULL can be visible;
the same metadata is already load-bearing for `IS NULL` block pruning in
`expr_filter.go`. Because bound-construction is conservative by taking the
outward extremum of each usable batch, arbitrary reader interleavings and
batch-order changes cannot invalidate a publication; an unusable batch
(class (c)) suppresses only its own non-NULL witnesses and never a later
usable batch's publication, while a structurally invalid scope (nil,
mismatched, or grouped key vector) disables the cutoff outright, as today.

## Change map and ownership

All changes extend owners inside the existing boundary; no new component,
state, protocol, or persistence is introduced.

| Boundary | Change |
|---|---|
| `pkg/sql/colexec/order/scan_cutoff.go` | `SupportsScanCutoffType` drops NotNullable (domain admission only). `ScanCutoff` gains the effective placement and the certified-witness counters of the "Certified witnesses" section: usable batches contribute (L: rows; T: rows minus nulls), all-NULL batches (L: nulls; T: 0), unusable non-NULL batches (L: nulls; T: 0) and never a bound. `batchBound` keeps the base's refusal predicates — `!zm.IsInited()`, outward-side `MaxTruncated()` under ASC, `GetMinMaxValue !ok`, structural mismatch/grouping — and keeps const/sorted fast paths only when the batch contains no NULLs; a failed batch no longer disables the summary for later usable batches (the pinned base's wholesale disable is what this row removes; the certification rules are never relaxed). Publication requires a usable initialized bound and `certifiedWitnesses >= K`; a stream whose certification never reaches K never publishes (fail open). |
| `pkg/sql/compile/scan_cutoff.go` | `paginationCutoffScan` and `initScanCutoff` compare effective placement between sort and scan specs (fail closed, mirroring the DESC check) and construct the summary with it. |
| `pkg/vm/engine/readutil/datasource.go` (+ shared helper) | One exported `OrderHeadNullsLead(plan.OrderBySpec flag) bool` beside `UsableOrderZoneMap` (one definition, shared with the local reader). `RemoteDataSource.SetOrderBy` **keeps** its NotNullable gate: remote nullable orders stay unordered exactly as today, so nullable plans never consume a bound remotely (`SetFilterZM` still returns early on the empty order) and remote scans stay exact-but-unpruned. Remote enablement remains closed per the prior approved design's condition (round-trip, remote-reader, cancellation/failure and multi-CN snapshot validation, incl. scopes with fewer than K candidates on one CN — 20261108-ordered-pagination-pruning.md), which this loop's single-node venue cannot supply; the gate gains a UT pin so the retained fallback is regression-checked. `orderBlocks`/`Next` keep the existing zone-map-only rule for admitted NOT NULL orders; no remote null-count loading is introduced. |
| `pkg/vm/engine/disttae/local_disttae_datasource.go` | `getBlockZMs` also collects the column null counts; `sortBlockList` permutes them with the zone maps; `needReadBlkByZM` keeps a block whose null count is non-zero when placement leads. This is the correctness repair for defect D1 on the local reader (the `hza` wrong-page path measured in Problem §3) — the only receiver path where D1 can fire, because the remote reader keeps its nullable gate — and it is independent of, and prerequisite to, the nullable deep-offset admission. |
| `pkg/sql/colexec/top/top.go` | `getTopValue` refuses a NULL heap root (returns no value): a NULL root has no comparable key, and its raw slot bytes must never become a pruning bound. Fail-open: no message, exact path. This is the correctness repair for defect D2 on the LIMIT-only publisher, reaching the same receivers through the shared message. |

Planner message attachment for multi-key nullable orders (message.go line
~99) stays excluded. LIMIT/OFFSET admission thresholds (`K >
mergeTopResidentPlanThreshold`, prepared-TP reuse exclusion, safe-filter
rules, `SQL_CALC_FOUND_ROWS`, runtime-filter and lock paths) are untouched.

## Cost model

- Per observed batch before publication: one `IsNull` bitmap cardinality pass
  when NULLs are present (bitmap words, not rows) plus the existing extremum
  scan; after publication the existing atomic stop applies. Rule L adds one
  counter per batch (all rows of usable batches, nulls of unusable/all-NULL
  batches), Rule T one subtraction (rows minus nulls of usable batches);
  per-batch cost is bounded by today's extrema scan plus a bitmap popcount.
- Per ordered block entry: one uint32 slot copied from already-loaded column
  metadata; no extra object reads. `orderBlocks`/`getBlockZMs` already
  deserialize the same `ColumnMeta` for the zone map.
- Reader skip loop: one extra branch for leading placements only.
- Memory: unchanged per-execution bounded state.

## Compatibility

- No persisted format, message, plan-shape, or protocol change. `NullCnt` is
  an existing on-disk per-column block statistic set by every writer
  (`SetNullCnt(vec.GetNulls().GetCardinality())`) and already trusted by the
  `IS NULL`/`IS NOT NULL` block filters; this revision reads it, never writes
  it.
- Immutable persisted blocks keep a stable null count = upper bound of
  visible NULLs for any snapshot/tombstone overlay; appendable and unknown
  blocks never skip, unchanged.
- Behavior changes are confining: nullable-key LIMIT+OFFSET queries that
  previously fed Sort the entire eligible input may skip blocks on the
  local disttae receiver (correctness identical by the witness rules);
  remote plans keep today's fallback (unordered scan, exact pages, no
  bound consumed); the LIMIT-only Top path may publish fewer bounds when
  the heap root is NULL — strictly fail-open.

## Alternatives

1. **Status quo** (keep G1-G4): the report's nullable pages keep full scan +
   sort cost; the shared-boundary NULL-root hazard stays. Rejected: half of
   the placement space is provably safe to prune now, meaning the exclusion
   is stricter than correctness requires.
2. **Metadata gate only** (admit nullable keys, require null count 0 for
   every skipped block in both placements): correct, but it forfeits the
   trailing placement entirely and keeps the effectiveness of rule L limited
   by NULL-dense tails. Rejected: weaker than the witness-counting split at
   the same cost.
3. **Counting only** (witness non-NULL rows and ignore null metadata):
   correct for trailing placements only. Rejected: unsafe for leading
   placements, so it cannot be admitted alone.
4. **Exact-prefix skip** (DuckDB row-group style): skips the OFFSET prefix
   using exact visible prefix counts; needs a global order proof and
   synchronized OFFSET state; closed as a product decision in the approved
   design and remains closed here; keyset SQL remains the recommended bounded
   pattern.
5. **Read the null bitmaps of candidate blocks** at publication time: turns
   the read path into metadata reads per block per query and duplicates a
   statistic that the object writer already records. Rejected.

## Verification plan

- **Invariant oracles (UT, order package)**: randomized mixed-NULL batches,
  both directions, both placements, K boundary and overflow, all-NULL and
  const-NULL batches, truncated (`MaxTruncated`) and non-initializable
  (decimal256) extrema, and class-(c) batches appearing both before the
  first usable bound and after it — no publication without a usable bound
  and `certifiedWitnesses >= K` (the witness inequality asserted directly
  against the counters), every published page equal to an independent
  full-sort oracle, and counting exactly per the classes (T never counts
  NULL; L never counts a class-(c) non-NULL row); a stream that never
  certifies K never publishes; structural scope invalidity still fails
  open.
- **Admission (UT, compile package)**: nullable now admitted; placement-
  mismatched specs rejected; DESC/ASC x default/NULLS FIRST/NULLS LAST
  matrix; multi-key nullable and unsafe shapes unchanged.
- **Reader rules (UT, disttae) and gate pin (UT, readutil)**: real block
  metadata written with NULL payloads via the object writer; skip/keep per
  placement per direction; null-count-carrying usable-zone-map blocks kept
  under L even when strictly worse; skipped under T; appendable and
  unknown blocks unaffected; share-input not mutated; one readutil UT pins
  `RemoteDataSource.SetOrderBy` still dropping nullable single-key orders
  (the retained remote fallback of the Change map).
- **Remote admission stays closed (UT, readutil)**: this change's whole
  remote surface is that gate pin plus the unchanged zone-map-only keep
  rule; no multi-CN or remote-reader enablement is part of it. Reopening
  requires the prior approved design's validation matrix (round-trip,
  remote-reader, cancellation/failure and multi-CN snapshot cases,
  including scopes with fewer than K candidates on one CN) and stays a
  conditioned open item, not this PR.
- **Top-boundary guard (UT, top package)**: NULL heap root never publishes;
  non-null roots publish as before.
- **D1/D2 counterexample regressions (UT + BVT)**: the `hza`/`hzb`
  two-block cases of Problem §3 become deterministic regressions at both
  levels: `ORDER BY k3 LIMIT 10` over `hza` must equal
  `[NULL×7, 3, 4, 5]` and `LIMIT 3` must equal `[NULL, NULL, NULL]`
  against 2-key full-sort controls; `hzb` ASC stays `[NULL×10]` and
  `hzb` DESC stays `8384..8375` while never relying on a wrong skip; reader
  UTs cover the same block/null-count/zone-map matrices. Note: the SQL parser
  on this baseline rejects explicit `NULLS FIRST/LAST` (`ERROR 1064 ...
  not supported in MySQL syntax`), so BVT exercises the default placements
  (ASC lead, DESC trail); explicit bits are covered at the colexec UT API.
- **Public behavior (BVT)**: extend
  `test/distributed/cases/optimizer/ordered_pagination_cutoff.sql` with
  nullable single-key pages for both directions (default placements),
  head-concentrated vs scattered NULL data, all-NULL key data, deep and
  beyond-input offsets, deleted-row interactions, disabled-off controls, and
  full-sort controls that must agree row-for-row where keys are unique.
- **Paired performance (local reproduction)**: the 1,000,000-row deep-offset
  matrix (section "Problem") before/after this change per key kind, pruning
  on/off, warm; Sort inputRows and wall time recorded in the implementation
  report; the head-region model verified by inputRows ≈ K at the measured
  offsets.

## Decisions, risks, open items

- Defects D1/D2 of Problem §3 are measured, pre-existing correctness failures
  of the merged #32355 LIMIT-only boundary, not regressions of this change.
  The reader-side null-aware keep rules and the NULL-root publication guard
  are therefore a correctness repair the change must carry regardless of the
  deep-offset admission decision; evidence paths:
  `state/artifacts/issue-32469/repro/{load.sql,measure_on.*,
  measure_off.*,pages_control.*,hz_load.sql,hz_probe*,*.log}`.
- Rule L's skip effectiveness is data-dependent: trailing blocks of
  NULL-sparse but regularly-NULL columns (report's `%17` pattern) mostly
  carry non-zero null counts, so ASC deep pages over them prune little while
  remaining exact. Rule T prunes those same columns under DESC. The BVT must
  assert exact pages for such columns and pruning work only where provable
  (head-concentrated nulls, DESC cases).
- Multi-key nullable stability, OFFSET-prefix skipping, collation domains,
  floats, decimal256, JSON/geometry and prepared-TP reuse remain outside
  scope; the approved design's closures hold.
- Round-1 review (REQUEST_CHANGES, two P2 findings) resolved in this
  revision. (1) Witness counting is now certificate-based ("Certified
  witnesses"): the earlier formulation counted rows of batches without a
  usable bound, which would over-certify K when a truncated-extremum batch
  precedes the first usable bound and let an unsafe bound skip a
  page-supplying block; non-NULL rows of class-(c) batches never count, and
  the truncation/base refusal predicates stay. (2) Remote nullable admission
  is explicitly retained as a fallback — `RemoteDataSource.SetOrderBy` keeps
  its NotNullable gate and gains a UT pin — aligning the change map with this
  document's own scope, the prior approved design's remote-enablement
  condition, and the handler public contract, instead of enabling remote
  nullable ordered reads without the multi-CN validation matrix.
- Open (non-blocking): whether the compile admission should also deliver the
  placement to the remote CN instruction set explicitly. Remote `Order`
  reconstruction derives placement from its serialized spec today, so no
  change is required; revisit only if a round-trip test shows drift. That
  open item is strictly about placement plumbing for already-admitted NOT
  NULL orders; enabling nullable orders remotely remains closed and
  additionally requires the conditioned multi-CN validation matrix above.
