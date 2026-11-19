# Valid-observation windows and reset-aware cumulative state

- Revision: r5 (supersedes r4, sha256
  8b11f62de7d48face876fdea11c503c0f3f82c682c30038fdb642cacfa4dfa73; r4
  superseded r3, sha256
  156719be9d169aa94a82e9555cecff9adb0ae83a2fa25b064d2eb556e17f5a75; r3
  superseded r2, sha256
  74eb0dd2b5319b22aa1b19bf14734b5b6b74ea196100dad2abddd7a1da37f846; r2
  superseded r1, sha256
  5aae8b3c701a04c23c5e3472c419aefac83bed909f9d741341ae183ffc699230)
- Owning issue: https://github.com/matrixorigin/matrixone/issues/29948
- Evidence base: worktree HEAD 2b94111625 (`fix(python-sdk): preserve query
  execution semantics (#30961)`), branch base `origin/main`

## Revision history

r5 changes, mapped to the two P3 advisory findings in the r4 design
approval (artifact
`state/artifacts/issue-29948/pre-pr-review-1792811008440.json`: PASS on
r4, sha256
8b11f62de7d48face876fdea11c503c0f3f82c682c30038fdb642cacfa4dfa73, no
blocking findings; the two advisory actions are part of that approval):

1. The valid-observation template projects `t.id` in its final SELECT,
   agreeing with `id` entering `sequenced`, with I1's source-column
   preservation, and with the source-id-keyed BVT assertions (approval
   finding "Retain the source identifier in the valid-window output").
   This changes projection width only; row cardinality, ordering, and
   every asserted value are unchanged, so all executed r1-r4 evidence
   remains valid.
2. The capacity model qualifies the join-back with partition/rid
   multiplicity, and the workload warning now covers total query
   cardinality (approval finding "Account for duplicate rid keys in
   join-back cost"); the validation plan gains a small many-partition
   plan plus execution check. The accepted migration strategy is
   unchanged.

r4 changes, mapped to the r3 review finding:

1. Both reset templates and their composition now derive the total-order
   ordinal `rid := ROW_NUMBER() OVER (PARTITION BY p ORDER BY o,
   <tie-breaks>)` once in a `sequenced` stage and use `rid` consistently in
   every downstream stage: `LAG`, boundary accumulation (with an explicit
   `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` frame), the
   cumulative `SUM`, and the final ordering. The r3 forms ordered these
   stages by `o` only; with duplicate `o` values the default frame
   (`RANGE UNBOUNDED PRECEDING .. CURRENT ROW` whenever an `ORDER BY` is
   present, `pkg/sql/parsers/dialect/mysql/mysql_sql.y:168-177`) merges
   peers into one boundary frame, and intra-peer `LAG`/accumulation order
   is unspecified. The review's tied-order consecutive-reset counterexample
   was executed on the r3-published shapes: two rows sharing `o` with
   flags [1,1] returned `(id 1 -> 5, id 2 -> 12)`, and with distinct keys
   returned `(id 1 -> 12, id 2 -> 7)` — merged runs plus order-fragile
   peer assignment (probes F1/F3a). The r4 templates return `5,7` for both
   forms (F2/F3b), the composition keeps tied peers merged when no
   boundary fires and splits them when one does (`5,12,11,24`, F6), and
   the issue fixture and E6 composition fixture are unchanged
   (`NULL,2,4,4,4,-1`; `5,12,11,13,17,19`, F4/F5). The BVT plan now
   includes the tied-order consecutive-reset counterexample asserted by
   source id for both predicate forms and the composition.

r3 changes (mapped to the r2 review findings, retained for the audit
trail):

1. The P-key declaration now agrees with the template: `R(r) := (prev_key =
   key) IS NOT TRUE`, i.e. a row opens a new run unless its key equals the
   previous row's key; probe E1 proves the declared predicate and the CASE
   indicator identical across all six NULL transitions.
2. The capacity model distinguishes candidate comparisons, emitted pairs,
   and retained state; the band stage's missing equality key
   deterministically selects LoopJoin (`pkg/sql/plan/join_order.go:194`,
   `pkg/sql/compile/compile.go:7189`, `pkg/sql/colexec/loopjoin/probe.go:478`),
   an O(N^2)-comparisons worst case in one all-valid partition, now
   explicitly accepted with the B1 revisit trigger; E7 EXPLAIN confirms
   the cond shapes.
3. The valid-window template derives `rid` and carries `(p, rid)` as row
   identity through grouping and join-back (E2 r2-shape `12,12` vs E3
   r3-shape `5,7` on the two-row tie counterexample).

r2 changes (mapped to the r1 review findings, retained for the audit
trail):

1. Reset semantics are defined by an explicit row predicate (I2), including
   current-row inclusion and NULL-predicate behavior; the consecutive-key
   boundary is one of two supported predicate forms. Discriminating cases
   D1/D2 (consecutive true flags with unchanged keys; key changes without a
   reset flag) were executed.
2. Both templates carry every required key and source column through every
   stage, restore row identity and source columns in the final projection,
   and end with an explicit outer `ORDER BY`. The r1 reset template failed
   binding (`column k does not exist`, probe A2).
3. The valid-observation template uses NULL-safe partition equality in both
   joins: probe pair A1/B2 shows the r1 template returning NULL and the
   corrected template returning the I1-required 5 on a NULL-partition row.
4. The r1 prefix self-join anchor (N(N+1)/2 pairs in an all-valid
   partition) was replaced by a running `COUNT(v)` anchor.

## Status

Design approved at r4: design-only review artifact
`state/artifacts/issue-29948/pre-pr-review-1792811008440.json`, PASS on r4
(sha256 8b11f62de7d48face876fdea11c503c0f3f82c682c30038fdb642cacfa4dfa73)
with two non-blocking P3 advisory findings. r5 implements exactly those
two advisory actions and changes no decision, invariant, or template
semantics. This approval covers the design only; it does not authorize a
commit, push, or PR. The implementation and its executable BVT proof are
reviewed separately.

## Classification and triggers

This issue (`kind/feature`) requests a user-visible SQL capability:
documented valid-observation-count windows and boundary/reset-aware
cumulative state. Under the repository design contract the request is a
feature. Recorded triggers for the complete requested scope (dedicated
operators):

- public API / compatibility contract: any dedicated operator adds new SQL
  surface with a documented semantics contract;
- crosses two or more ownership boundaries if dedicated operators are built:
  parser (`pkg/sql/parsers`), function registry/binder
  (`pkg/sql/plan/function`, `pkg/sql/plan/window_binder.go`), execution
  (`pkg/sql/colexec/window`, `pkg/sql/colexec/aggexec`), and remote plan
  dispatch gating (precedent for a new window-capable aggregate:
  `pkg/defines/const.go`, `pkg/sql/compile/compile.go`,
  `pkg/sql/compile/operator.go` in the quantileExactWeighted series, whose
  design doc is `docs/design/20261023-exact-weighted-percentiles.md`);
- the issue explicitly permits the migration-equivalent route, and its
  triage history records that dedicated support was already deferred once.

The selected scope below (documented staged-SQL migration equivalents plus a
BVT regression, no production change) consists of documentation and focused
tests, which the contract exempts from a mandatory design document; this
document is retained anyway because the route selection itself resolves the
feature request and must be reviewable.

## Problem, evidence, and current behavior

MatrixOne computes window frames over physical row positions (`ROWS`) or
`ORDER BY` value ranges (`RANGE`). `GROUPS` frames are rejected with
`20102 GROUPS in WINDOW FUNCTION condition is not yet implemented`
(`pkg/sql/plan/window_binder.go:1002`). No form of `IGNORE NULLS` exists in
grammar or execution (a `SUM(x IGNORE NULLS) OVER (...)` probe fails with
the parser error 1064). There is no frame whose membership counts valid
(non-NULL) observations and no cumulative aggregate whose state restarts at
a row predicate boundary.

Observed on the evidence base through a live single-node cluster of the
unmodified base build (probe transcripts in the handler evidence directory;
each run against `generate_series`-built or `UNION ALL` synthetic fixtures,
no persisted tables):

| Probe | Result |
|---|---|
| `movingWindowIndex(CAST(1 AS DOUBLE),3)` | error 20105 `not supported: function or operator 'movingwindowindex'`, 3/3 identical |
| `dynamicGroupCumsum(CAST(1 AS DOUBLE),1)` | error 20105 |
| `dynamicGroupCumcount(CAST(1 AS DOUBLE),1)` | error 20105 |
| issue's last-two-valid control, verbatim | `NULL, 2, 6, 8, 8, 3` (all values and NULL bits) |
| physical `ROWS BETWEEN 1 PRECEDING AND CURRENT ROW` on the same fixture | `NULL, 2, 6, 8, 4, -1` (differs at ids 5,6) |
| r3/r4 valid-window template on the issue fixture (E4) | `NULL, 2, 6, 8, 8, 3` |
| r4 reset template (P-flag form) on the issue fixture (F4a) | `NULL, 2, 4, 4, 4, -1` (matches the issue's verified reset profile) |
| r4 reset template (P-key form) on the issue fixture (F4b) | `NULL, 2, 4, 4, 4, -1` (coincides; tick boundaries equal flag rows) |

The 20105 rejection is raised by `getFunctionIdByName`
(`pkg/sql/plan/function/function.go:1069`, code defined at
`pkg/common/moerr/error.go:67`) because none of the three names exists in
`functionIdRegister` (`pkg/sql/plan/function/function_id.go`); the names
appear nowhere in the repository.

The fixture for the two profiles (from the issue; `v` is the aggregated
value, `tick` a session/regime key, `flag` the reset flag):

| id | tick | v    | flag |
|----|------|------|------|
| 1  | 0    | NULL | 1    |
| 2  | 0    | 2    | 0    |
| 3  | 1    | 4    | 1    |
| 4  | 3    | 4    | 1    |
| 5  | 3    | NULL | 0    |
| 6  | 8    | -1   | 1    |

On this fixture the flag rows {1,3,4,6} coincide with the tick-change
boundaries, so both predicate forms of I2 reproduce the issue's verified
reset control `NULL,2,4,4,4,-1`; the D1/D2 discriminators in the validation
section separate the two forms where they must differ.

Both profiles therefore execute correctly on unmodified current MatrixOne
SQL. The real gaps are (a) dedicated, documented declarative surface and
(b) explicitly documented state semantics.

## Goals and non-goals

Goals (selected scope, Phase A):

1. Define exact semantics for both profiles (invariants below).
2. Provide verified staged-SQL migration equivalents that users can copy.
3. Land one BVT regression case under `test/distributed/cases/window/`
   asserting complete values, NULL bits, and row alignment, with the
   physical-frame counterexample and the entry-name expected errors kept in
   the same case so named/API availability stays visible separately from
   mathematical rewrites (the issue's own acceptance split).

Non-goals (Phase B, deferred with explicit revisit triggers):

- Dedicated SQL functions or frame syntax (`movingWindowIndex`,
  `dynamicGroupCumsum/Cumcount`, `GROUPS`, `IGNORE NULLS`). Revisit only
  when a maintainer accepts the cross-boundary owner cost listed in the
  triggers, or when a workload exceeds the accepted capacity envelope of
  the staged-SQL band join (quadratic-comparison worst case, recorded in
  the capacity model).
- DolphinDB runtime- or all-parameter parity. The referenced DolphinDB
  documentation is semantic background only; the issue states the same.

## Invariants

I1 (valid-observation window). For a partition ordered by a deterministic
total order and value expression `v`, the size-`K` frame of row `r` is
exactly the last `K` ordinal positions `i <= r` with `v_i IS NOT NULL`. The
aggregate computes over exactly that set and emits NULL when the set is
empty. Exactly one output row per input row; row order and all source
columns are preserved. Partition equality follows `PARTITION BY` grouping:
NULL partition keys form one partition and are preserved (the engine groups
them natively; the manual joins in the template must match that semantics).

I2 (reset-aware cumulative). Within a partition ordered by a deterministic
total order, a reset predicate `R` is a boolean SQL expression over the
current row's columns, including previous-row values exposed through `LAG`.
A new run starts at row `r` iff `R(r)` is TRUE or `r` is the partition's
first row; FALSE and NULL both mean no boundary (SQL truth semantics: a
NULL predicate is not TRUE). Current-row inclusion: the boundary row is the
first member of the new run, so its own value enters the restarted state
immediately. Cumulative state starts empty at each run start; a non-NULL
value updates the state; a NULL value row emits the current state without
mutating it (carry); a run prefix with no non-NULL value yet emits NULL.
Exactly one output row per input row; row order and all source columns are
preserved.

Two predicate forms are supported and documented; each declaration is
byte-equivalent to its published indicator over every three-valued-logic
transition (probe E1):

- P-flag (explicit reset flag): `R(r) := (flag = 1)`; boundary indicator
  `CASE WHEN flag = 1 THEN 1 ELSE 0 END`; `flag IN (0, NULL)` never resets.
- P-key (derived consecutive-key boundary):
  `R(r) := (prev_key = key) IS NOT TRUE`, `prev_key := LAG(key)`; boundary
  indicator `CASE WHEN prev_key = key THEN 0 ELSE 1 END`. A row opens a
  new run unless its key equals the previous row's key. The first row
  (LAG yields NULL) and every NULL-key row test `IS NOT TRUE`, so both
  always open runs; an equal-key pair tests TRUE and never opens one.

`R` may combine both: `R(r) := (flag = 1) OR ((prev_k = k) IS NOT TRUE)`,
indicator `CASE WHEN flag = 1 THEN 1 WHEN prev_k = k THEN 0 ELSE 1 END`
(probes E6/F5). The issue's "reset-on-flag" profile maps to P-flag.

I3 (determinism and restrictions). Callers supply a total order per
partition: an order key `o` that must be NOT NULL, plus any tie-break
columns needed to make the order total (results under an un-tie-broken
non-total order are not contractual). Every template derives one
total-order ordinal `rid := ROW_NUMBER() OVER (PARTITION BY p ORDER BY o,
<tie-breaks>)` once and uses it in every downstream stage — grouping,
join-back, `LAG`, boundary accumulation, cumulative aggregate, and final
ordering — with explicit `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT
ROW` framing on cumulative stages, so no stage relies on a scalar-column
order or on the peer-merging default `RANGE` frame. `K` is a positive
integer literal. Partitions are independent: no frame, run, or state
crosses a `PARTITION BY` boundary.

I4 (no regression). Phase A adds no production behavior; every existing
window/aggregate result is unchanged. The equivalents use only the current
documented SQL surface (CTEs, `ROW_NUMBER`, `COUNT`/`SUM ... OVER`, `LAG`,
joins), and were executed for this design on the unmodified evidence base.

## Selected design: documented migration equivalents (Phase A)

### Valid-observation window ("last K valid") — staged pattern

Generalized over partition `p`, NOT NULL order key `o`, tie-break columns
(here `id`), value `v`, size `K`. One `sequenced` stage derives the unique
row ordinal `rid` and the running valid count `cur` over the same total
order; partition equality in the manual joins is NULL-safe (`PEQ`); row
identity `(p, rid)` is carried through the aggregate and the join-back:

```sql
WITH sequenced AS (
  SELECT o, p, v, id,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY o, id) AS rid,
         COUNT(v) OVER (PARTITION BY p ORDER BY o, id
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cur
  FROM t),
valid_rows AS (
  SELECT p, rid, v,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY rid) AS ord
  FROM sequenced
  WHERE v IS NOT NULL),
joined AS (
  SELECT a.p, a.rid, SUM(b.v) AS result
  FROM sequenced a
  LEFT JOIN valid_rows b
    ON ((b.p = a.p) OR (b.p IS NULL AND a.p IS NULL))
   AND b.ord BETWEEN a.cur - (K - 1) AND a.cur
  GROUP BY a.p, a.rid)
SELECT t.id, t.o, t.p, t.v, j.result
FROM sequenced t
LEFT JOIN joined j
  ON ((j.p = t.p) OR (j.p IS NULL AND t.p IS NULL)) AND j.rid = t.rid
ORDER BY t.p, t.rid;
```

Semantics check: `rid` is unique within a partition by construction, so
`GROUP BY a.p, a.rid` emits exactly one row per input row and the join-back
matches 1:1. `ORDER BY rid` in `valid_rows` is the same total order as
`ORDER BY o, id` because `rid` is monotone in it. `cur` equals the ordinal
of the most recent valid row at or before the current row (0 for a leading
all-NULL prefix, whose band matches no `ord >= 1` and therefore yields a
NULL SUM, satisfying I1's empty-set rule). Non-NULL `v` advances `cur`;
NULL `v` rows reuse the same band, which is exactly the
carry-across-missing-row behavior (the verified `8` at id 5 of the issue
fixture). Null-partition equality follows `PEQ` in both joins. The
explicit `ROWS` frame on `COUNT` makes the running count peer-independent.
The final projection restores the source columns, including `id`,
satisfying I1's source-column preservation and making every emitted row
assertable by source id.

### Reset-aware cumulative — staged pattern

Both forms derive `rid` once and use it in every stage (I3); results are
asserted by source id.

P-flag form (explicit reset flag; the issue's "reset-on-flag" profile):

```sql
WITH sequenced AS (
  SELECT o, p, v, flag, id,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY o, id) AS rid
  FROM t),
tagged AS (
  SELECT o, p, v, flag, id, rid,
         SUM(CASE WHEN flag = 1 THEN 1 ELSE 0 END)
           OVER (PARTITION BY p ORDER BY rid
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
  FROM sequenced)
SELECT id, o, p, v, flag,
       SUM(v) OVER (PARTITION BY p, grp ORDER BY rid
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
FROM tagged
ORDER BY p, rid;
```

P-key form (derived consecutive-key boundary; session/regime keys):

```sql
WITH sequenced AS (
  SELECT o, p, v, k, id,
         ROW_NUMBER() OVER (PARTITION BY p ORDER BY o, id) AS rid
  FROM t),
lagged AS (
  SELECT o, p, v, k, id, rid,
         LAG(k) OVER (PARTITION BY p ORDER BY rid) AS prev_k
  FROM sequenced),
tagged AS (
  SELECT o, p, v, k, id, rid,
         SUM(CASE WHEN prev_k = k THEN 0 ELSE 1 END)
           OVER (PARTITION BY p ORDER BY rid
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
  FROM lagged)
SELECT id, o, p, v, k,
       SUM(v) OVER (PARTITION BY p, grp ORDER BY rid
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS result
FROM tagged
ORDER BY p, rid;
```

Semantics check: `rid` encodes the caller's total order, so boundary
accumulation and the cumulative aggregate are deterministic under ties and
explicitly `ROWS`-framed (counterexample F1/F3a vs F2/F3b); `LAG` over
`rid` is likewise the contract order, never the peer-physical order.
`grp` is the running count of boundaries, so it labels exactly the runs I2
defines, with the boundary row included in its new run (the indicator
fires on the boundary row itself). The indicator's ELSE branch implements
`(prev_k = k) IS NOT TRUE`: equal keys take THEN, everything else (key
change, NULL on either side, first row) opens a run. `SUM(v)` skips NULLs
(carry) and an all-NULL run prefix yields NULL. `PARTITION BY p, grp`
delegates partition handling to the engine, which groups NULL partition
keys natively, so no manual equality is needed anywhere in this template.
Consecutive resets need no extra machinery: each fires another indicator.
Every stage carries the source columns plus `rid`; the final SELECT
restores all source columns with an explicit ordering, satisfying the
alignment half of I1/I2.

Template composition note: the boundary indicator is a slot. The combined
predicate `R(r) := (flag = 1) OR ((prev_k = k) IS NOT TRUE)` uses
`CASE WHEN flag = 1 THEN 1 WHEN prev_k = k THEN 0 ELSE 1 END` inside the
same `sequenced`/`lagged`/`tagged` structure over `rid` (probes E6/F5/F6).

### Why no dedicated operator in this phase

- The issue's verified controls run on the unmodified base (observed
  above): the missing piece is documentation and explicit state semantics,
  not engine capability.
- Dedicated support prices parser grammar, binder frame-model changes, a
  new executor state machine, aggregate-id/wire gating, and their UT/BVT
  matrix against an acceptance profile that the staged SQL already meets.
  Shipping it without maintainer demand contradicts the
  narrowest-complete-closure rule; triage already deferred this once.
- Standard-SQL extensions do not express the profiles either: `IGNORE
  NULLS` (absent here) applies only to value functions and never counts a
  frame over valid observations; `GROUPS` (NYI at
  `window_binder.go:1002`) counts peer groups, not validity, and addresses
  neither reset semantics nor valid-observation frames.

## Alternatives considered

- B1: ship dedicated functions now. Rejected for this phase; revisit
  triggers in non-goals. The owner path is the one the
  quantileExactWeighted series took (registry + aggexec + window admission
  + MORPC gating + design doc). A dedicated valid-window operator would
  hold O(K) state per partition and make one O(N) pass, removing the band
  join's comparison worst case; that is its only cost advantage over Phase
  A (capacity model below) and is not a correctness gap.
- B2 (selected): documented equivalents + BVT regression.
- B3: implement standards-style frame extensions (`GROUPS`, `IGNORE NULLS`)
  as a substitute. Rejected: neither expresses I1 or I2; same cost class as
  B1 without closing the requested semantics.
- r3's reset templates ordered by scalar `o`: superseded by the `rid`
  contract (F1/F3a defect pair); ordering by `o` alone under duplicate
  values merges peers in the default `RANGE` boundary frame and leaves
  intra-peer `LAG`/accumulation order unspecified.
- r1's anchor via prefix self-join (`MAX(b.ordinal) WHERE b.o <= a.o`):
  removed in r2; its match set is N(N+1)/2 pairs in an all-valid partition,
  while the running-`COUNT(v)` anchor computes the same ordinals from one
  window sort with no expansion.
- Status quo: leaves the acceptance criteria (documented semantics plus
  regression) unmet despite a zero-risk closure being available.

## Compatibility and operations

Phase A changes no persisted, wire, catalog, or configuration state; the
equivalents run on this version and on any version implementing the used
SQL surface. Existing queries are unaffected (I4). No rollout, fallback, or
upgrade consideration applies. Plan-shape visibility for users of the
pattern is standard `EXPLAIN`.

Capacity model (N = input rows, V = valid rows <= N, K = window size), with
three quantities kept separate (operator selection verified against
`pkg/sql/plan/join_order.go:194` `IsEquiJoinCondition`, which admits only a
top-level two-sided equality, and `pkg/sql/compile/compile.go:7178-7193`,
which constructs HashJoin when an equality key exists and LoopJoin
otherwise; LoopJoin's probe scans the build side for every probe row,
`pkg/sql/colexec/loopjoin/probe.go:389` and `:478-516`; EXPLAIN of the
published valid-window query on the live base shows the band cond as
`PEQ-OR + BETWEEN` with no equality conjunct and the join-back cond
carrying the pure `rid` equality; probe E7):

| Stage | Candidate comparisons (execution work) | Emitted pairs before reduction | Retained state |
|---|---|---|---|
| sequenced (window sort(s)) | O(N log N) | 0 | N rows |
| valid_rows | O(V log V) | 0 | V rows |
| band join + GROUP BY (LoopJoin: no equality key) | O(N*V) worst case — one all-valid partition => O(N^2) even for fixed K | <= N*K | N groups |
| join-back (pure `rid` equality present => hash-join eligible; the NULL-safe partition equality stays an OR residual) | O(N) expected when same-`rid` collisions across partitions are rare; P partitions sharing one `rid` value cost up to P^2 residual comparisons there (N singleton partitions => O(N^2), independent of V) | N matches | N rows |
| reset templates (no joins): rid derivation, (LAG), boundary accumulation, cumulative aggregate | O(N log N) window sorts | 0 | N rows |

- Join-back partition/rid multiplicity: the hash is built on `rid` alone,
  which restarts per partition, while the NULL-safe partition equality
  remains an OR residual (`pkg/sql/compile/operator.go:2902` separates
  equality conditions from residuals; `pkg/sql/colexec/hashjoin/join.go:921-1006`
  evaluates duplicate-key candidates). With P partitions sharing one `rid`
  value, the residual costs up to P^2 comparisons for that value; N
  singleton partitions (every row its own partition, every `rid = 1`)
  reach O(N^2) residual comparisons regardless of V — including when every
  `v` is NULL. Emitted matches stay N and retained state stays linear;
  only comparison work grows.

- Worst case accepted explicitly: the band stage's comparison count is
  quadratic in a single all-valid partition. Emitted pairs stay <= N*K and
  retained state stays linear, so memory is not at risk; comparison work
  is. This tradeoff is accepted for the documented migration profile:
  staged-series migration and drill-down queries of moderate N with small
  fixed K (the movingvalid profile), where setup cost dominates anyway.
- Workload implication / revisit trigger: judge total query cardinality —
  the sum of every stage's comparison work (the band stage's O(N*V) worst
  case plus the join-back multiplicity residual above). A workload driving
  N into the high hundreds of thousands inside one partition of one query,
  filling a query with singleton partitions, or latency-sensitive repeated
  evaluation should take the B1 dedicated operator (O(N) pass, O(K)
  state). This is an explicit non-goal trigger.
- No performance advantage over any alternative is claimed, matching the
  issue's acceptance text. The implementation turn records the EXPLAIN
  cond-shape of the band stage in the BVT comments so the
  no-equality/LoopJoin selection stays visible next to the assertions.

## Validation

Premise probes executed for this design revision on the live unmodified
base build (SQL `/tmp/issue-29948/probes_r4.sql`, r3-turn
`/tmp/issue-29948/probes_r3.sql`, r2-turn `/tmp/issue-29948/probes_r2.sql`;
transcripts and lifecycle logs in the handler evidence directory. Expected
values were hand-derived from I1/I2/I3 before execution:

| Probe | Purpose | Hand-derived expected | Observed |
|---|---|---|---|
| F1 | r3-published P-flag shape, tied `o`, flags [1,1], values [5,7] | defect: merged run, `(id 1 -> 5, id 2 -> 12)` | exact match (defect demonstrated) |
| F2 | r4 P-flag, same input | `(id 1 -> 5, id 2 -> 7)` | exact match |
| F3a | r3-published P-key shape, tied `o`, keys [9,8] | defect: merged run + order-fragile peers, `(id 1 -> 12, id 2 -> 7)` | exact match (defect demonstrated) |
| F3b | r4 P-key, same input | `(id 1 -> 5, id 2 -> 7)` | exact match |
| F4a/F4b | r4 both forms on the issue fixture | `NULL,2,4,4,4,-1` twice (issue-verified) | exact match |
| F5 | r4 composition, r3 E6 fixture | `5,12,11,13,17,19` | exact match |
| F6 | r4 composition on tied `o`, both directions (merge vs split) | `(id 1 -> 5, id 2 -> 12, id 3 -> 11, id 4 -> 24)` | exact match |
| r3 E1 | declared P-key predicate vs CASE indicator across all six NULL transitions | columns identical: 1,0,1,1,1,1 | exact match |
| r3 E2/E3 | r2 vs r3 valid-window shape on the two-row tie counterexample | `12,12` defect vs `5,7` | exact match |
| r3 E4/E5 | valid-window on issue fixture, NULL-partition, three partitions | `NULL,2,6,8,8,3`; `5`; `10,10,30,50,NULL,7,NULL,NULL` | exact match |
| r3 E6 | composed reset predicate (unique `o`) | `5,12,11,13,17,19` | exact match |
| r3 E7 | EXPLAIN of the published valid-window query | band cond without equality conjunct; join-back with `rid` equality | as expected |
| r2 A1/A2 | r1 templates as published | A1 NULL-partition collapse; A2 bind error | both demonstrated |
| r2 B1,C1,D1 | r2 templates on issue fixtures | `NULL,2,6,8,8,3`; `NULL,2,4,4,4,-1` twice | exact match |
| r2 C2/C3,D2/D3 | predicate-form discriminators D1/D2 | `5,7` vs `5,12` both ways | exact match |
| r2 C4,D4 | NULL flag; NULL partition/keys | `NULL,5,5,12`; `2,3,4,1,10` | exact match |

Service lifecycle (r4 round, same recipe as r3): pre-launch listener check;
owned plan MySQL 39601 / CN-TN port-bases 38500/38600 / LOG 38700-38702
(shifted in the r3 round after a foreign handler's service took the older
MySQL port; that service was identified by args and preserved); bounded
readiness (`8.0.30-MatrixOne-v29948114`); teardown after args re-check;
process exit and listener release confirmed (`R4_summary.txt`).

Implementation-phase checks (after design PASS, one focused BVT case under
`test/distributed/cases/window/`, `.result` hand-derived from I1–I3 and
re-inspected against the executed output — a generated result alone is not
evidence):

- the case pastes the published templates verbatim against a
  self-contained fixture and asserts complete values, NULL bits, and row
  alignment for both profiles, keyed by source id;
- partitions: at least two `PARTITION BY` groups plus a NULL partition and
  an all-NULL partition;
- both profile assertions are keyed by source `id`, which the
  valid-window final projection now carries (approval finding P3-1);
- a small many-partition check (approval finding P3-2): singleton
  partitions — including a NULL partition and rows with NULL `v` — so
  every join-back hash key collides at `rid = 1`; complete results and
  row cardinality are asserted exactly and the plan shape of this shape
  is recorded in the case comments;
- the executed discriminators: consecutive true flags with unchanged keys
  (D1: P-flag `5,7` vs P-key `5,12`) and key changes without a reset flag
  (D2: P-flag `5,12` vs P-key `5,7`);
- the tied-order consecutive-reset counterexample asserted by source id
  (F1/F3a fixture: two rows sharing `o`, flags [1,1], values [5,7] ->
  `5,7`; keys [9,8] -> `5,7` for both forms), plus the F6 composition
  case (`5,12,11,24`);
- the declared-vs-indicator case of probe E1 (P-key predicate agreement
  across NULL transitions) and the two-row tie counterexample of the
  valid-window recipe (duplicate order values resolved by `rid` to `5,7`);
- explicit tie-break ordering, empty table, single row, single valid row,
  all-NULL value inputs;
- consecutive resets, NULL flags (no boundary), NULL group keys (each a
  singleton run), and a NULL-key run adjacent to a valid run;
- numeric/type boundaries: negative and large-magnitude DOUBLE values and
  an integer value column;
- the physical-`ROWS` negative counterexample retained in the same case;
- the three entry-name probes retained as expected-error statements;
- an EXPLAIN cond-shape check of the band stage recorded in the case
  comments (equality key absent => LoopJoin selection per the capacity
  model), executed during the implementation turn.

No production code changes means no new UT is required for Phase A; the
existing window UT/BVT suites plus CI remain the I4 regression net (this
turn does not re-run them).

## Acceptance criteria

1. explicit approval of this design revision recorded before implementation;
2. the BVT case above lands with hand-checked results matching I1/I2/I3,
   instantiating the published templates verbatim;
3. the entry-name availability probes remain visible as expected errors in
   the case, separate from the rewrites;
4. no change to any existing BVT/UT result (I4).

## Open questions

- Non-blocking: whether the user documentation site (separate docs repo)
  should carry a cookbook entry in addition to this document and the BVT
  comments. Owner: issue assignee; decision point: implementation PR
  review. The in-repo deliverables do not depend on it.
- Non-blocking: Phase B naming parity with the DolphinDB surface (e.g.
  exact function spellings) if B1 is ever triggered. Owner: future design.
