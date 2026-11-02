# Local Sliding-Window Ranks: `mrank`, `cumrank`, `tmrank`

## Performance amendment: cumulative and fixed-width ranks

The original direct-scan contract remains the fallback. For a materialized
partition, `cumrank` and `mrank` with a constant width may instead compress
the non-NULL, non-NaN values of a demanded prefix into ordered ranks and
maintain their frequencies in a Fenwick tree. The result remains one plus the count of values strictly
less than the current value. NULL consumes a physical frame position but is
never counted; a current NaN ranks 1, NaN inputs do not contribute, and signed
zeros compare equal. Width validation still precedes NULL-result handling.

The first index covers only the current output chunk. As consumption reaches
its horizon, the indexed prefix grows geometrically, covering at most twice
the currently requested prefix. Rebuilding seeds the tree with the already
consumed active rows; fixed-width eviction continues to count physical rows.
An initial short or all-NULL prefix can be reconsidered at the next horizon.
This bounds the additional work before first output even when a LIMIT stops
consumption early; the input partition itself remains materialized.

The window container owns partition state across output chunks. It discards
that state on partition completion/change, generation change, reset, free and
partial setup error. Replacement releases the old index before admission.
Scratch and retained arrays are charged to the existing analytic allocation
account before allocation, using at most 24*n + O(1) bytes simultaneously for
three 64-bit arrays, where n is the indexed prefix length. Size arithmetic is
checked. A resource-capacity rejection
releases partial reservations and falls back to the original scan; cancellation,
closed-generation, account-invariant and other errors propagate. Every O(n)
construction loop, seeding pass and sorting step has bounded cancellation checks. A
nonsequential fallback invalidates state before any subsequent fast-path use.
For fixed-width mrank, the first width is validated after the ordinary
cancellation check and before index construction. A capacity rejection keeps
the partition on the scan path, without repeated admission attempts. The
fixed-width path adds the entering row and removes the leaving row. If required
result growth is capacity-rejected after an index was admitted, the operator
releases the optional index, retries the unpublished fixed-width append once,
and scans subsequent rows. Cancellation and fatal account errors still
propagate. This local fallback does not guarantee headroom for arbitrary
downstream operators. A
per-row width may move its frame start backwards and therefore uses the original
scan; short partitions, all-NULL partitions and constant widths below 128 also
use the scan. All-NULL detection polls for cancellation; `tmrank` retains the
original resolver and scan, including disjoint
DST spans. Unsupported types and nonsequential evaluation fall back safely.

Geometric compression costs O(n log n) total time and O(n) memory per fully
consumed partition, then each rank costs O(log n). First-output construction is
bounded by the requested chunk. Rebuilding costs more than one eager sort and
must be compared against the direct scan for first and full consumption at
small and large partitions, several cardinalities and batch boundaries. Differential
tests cover ties, NULL, NaN, signed zero, width overflow, partition/reuse,
cancellation during construction, seeding and evaluation, capacity fallback,
output pressure, early reset/reuse, and
invariant-error propagation. The original reviewed design revision
below remains historical and is not represented as approval of this amendment.

- Owning issue: [matrixorigin/matrixone#29947](https://github.com/matrixorigin/matrixone/issues/29947).
- Implementation: [matrixorigin/matrixone#31020](https://github.com/matrixorigin/matrixone/pull/31020).
- Performance amendment: [matrixorigin/matrixone#31516](https://github.com/matrixorigin/matrixone/pull/31516).
- Original design approval and review history are preserved in the [original review record](https://github.com/matrixorigin/matrixone/blob/df8bfd602ecc036424ff7897187565657366919d/docs/design/20261023-local-sliding-window-ranks.md#9-review-record). The performance amendment above is a subsequent change.

## 1. Problem, evidence, and current behavior

Users of time-series alerting need "how unusual is the current value relative to the last few
observations". Partition-wide `RANK() OVER (ORDER BY ...)` ranks against the entire partition
history; a `ROWS` frame attached to `RANK` changes nothing (matrixone `RANK` ignores frames, as
it is rank-family `WIN_ORDER`). The issue's verified business profile is:

> 1-based ascending competition rank of the current non-NULL value in the last three physical
> rows, returning NULL when the current value is NULL.

Reproduction evidence at the assigned base `f8c8720016...`:

1. Entry-name probes are rejected before any math can be attempted ("entry-name availability
   only"). `SELECT mrank(CAST(1 AS DOUBLE),3);`, and likewise `tmrank`, `cumrank`, fail with
   `(20105, "not supported: function or operator '<name>'")`. Static proof of cause on the
   assigned base: none of the names exist in `functionIdRegister`
   (`pkg/sql/plan/function/function_id.go:899`), and unresolved names are raised from
   `getFunctionIdByName` (`pkg/sql/plan/function/function.go:1059`) or the frontend UDF fallback
   (`pkg/frontend/compiler_context.go:1063`) with exactly that message and code. A repository
   search finds no `mrank`/`tmrank`/`cumrank` identifier anywhere.
2. The requested computation itself has no engine entry point: the only ranking operators are
   the partition-wide `RANK`, `DENSE_RANK`, `PERCENT_RANK`, `CUME_DIST`
   (`plan.Function_WIN_ORDER` class) plus value-window `LAG`/`LEAD`/`*_VALUE`
   (`plan.Function_WIN_VALUE` class) in `pkg/sql/plan/function/list_window.go`. No function
   ranks within a row-local frame.
3. The issue supplies a working canonical-SQL control (self-join over ids `a.id-2 .. a.id`)
   whose complete verified values on the six-row synthetic fixture are `NULL, 1, 2, 2, NULL, 1`.
   This document adopts that control as the executable oracle for the `mrank` profile.

Live reproduction against a base `mo-service` built from the assigned base commit was executed
(evidence: `repro-evidence-base-f8c87200.txt`, service startup/teardown logs, all under the
issue's handler artifact directory). All three probes reproduced the issue's rejections:
`mrank` and `cumrank` returned exactly
`(20105, "not supported: function or operator '<name>'")`; the control query returned exactly
`NULL,1,2,2,NULL,1` with the NULL bits and id alignment verified. Two additional behaviors
observed on the live base shape this design:

1. `SELECT tmrank(CAST(1 AS DOUBLE), INTERVAL 3 DAY) result;` fails with
   `(20105, "not supported: standalone INTERVAL expression in TMRANK argument")`: the parser
   accepts an `INTERVAL n unit` inside a generic function argument list, and the binder's
   no-escape boundary for standalone interval expressions
   (`pkg/sql/plan/interval_expr.go:rejectIntervalFunctionArgs`, invoked from
   `base_binder.go` when builtin binding fails) classifies the call before any function
   resolution. A tmrank design must therefore consume the interval at binding time —
   and, because the operator must agree with user RANGE frames, normalize it with the
   window frame-bound grammar (§3.1 `D`, decision D14) — rather than carrying a
   `T_interval` argument into execution.
2. `SELECT rank();` (an already-registered window function without OVER) fails with a
   **parser** error (1064), not the binder message: in `pkg/sql/parsers/dialect/mysql/mysql_sql.y`
   the `function_call_window` productions require a mandatory `window_spec`
   (`RANK '(' ')' window_spec`, ...), and the generic `function_call_generic: ID '(' ... ')'`
   production cannot carry an OVER clause at all. Entry availability for the new names
   therefore reaches SQL only through new grammar productions, and the post-fix, no-OVER
   probe outcome is the same 1064 syntax-error class `rank()` has today.

The control values are re-derived independently below (`§2`) so the oracle is stated inside
this design.

## 2. Invariant and definitions

### 2.1 The one invariant

For every output row `j` of a local sliding window rank, with the rank input vector `v` and the
frame `F(j)` (a set of input-row positions defined per operator below):

```
result(j) = NULL                          if v[j] IS NULL
result(j) = 1 + |{ k ∈ F(j) : v[k] < v[j] }|   otherwise
```

(competition rank: ties share a rank and produce no inflation; ascending direction: the smallest
value in the frame gets rank 1; 1-based). NULL frame values never count, because
`NULL < x` is unknown and is excluded. `F(j)` contains `j` (the current row) on every
ordinary window, so a non-NULL current value that is the minimum of its frame ranks 1.
One documented exception exists for `tmrank` (decisions D15/D17): when the window width
normalizes to **NULL arithmetic** — a *successful* `NormalizeWindowInterval` outcome with
`null=true`, e.g. an extra-field compound spelling — the frame is **key-dependent,
exactly as the shared resolver computes it** (`window.go:2258-2265`: "NULL current keys
still form their ordinary peer group. Only a non-NULL temporal key has NULL interval
arithmetic and an empty frame.", asserted by `colexec/window/interval_test.go:117-158`):
a row whose temporal key `t(j)` is **non-NULL** gets the **empty frame**
(`F(j) = ∅`, not even the current row) and `result(j) = NULL` — mirroring the
aggregate-over-empty-frame semantics the equivalent user RANGE frame produces (verified
`NULL` sums in `test/distributed/cases/window/window_interval_parameters.result:22-29`,
which contains only non-NULL keys); a row whose temporal key is **NULL** keeps its
ordinary NULL-key **peer group** with the trailing positional cutoff applied
(`F(j) = { k ≤ j : t(k) IS NULL }`), so `result(j)` is computed by the ordinary
invariant — a single row with `t = NULL`, `v = 7` ranks **1**, never NULL. A
NULL *source value* for the width is a different outcome: it is an error (D15's outcome
matrix), never a NULL window.

### 2.2 Frames per operator

Rows are sorted by the operator (`ORDER BY` from `OVER`) and bounded by the partition
(`PARTITION BY` from `OVER`); `partStart(j)` is the first position of `j`'s partition. All frames
are trailing: they contain no position greater than `j`.

- `mrank(v, N)`: `F(j) = { k : max(partStart(j), j-(N-1)) ≤ k ≤ j }` — the last `N` physical
  rows including the current row; a partial frame near the partition start contains the available
  suffix (frame entry/exit). `mrank(v, 1)` = 1 for every non-NULL `v[j]`.
- `cumrank(v)`: `F(j) = { k : partStart(j) ≤ k ≤ j }` — cumulative prefix rank from the partition
  start through the current row.
- `tmrank(v, D)`: `F(j) = { k : partStart(j) ≤ k ≤ j and k satisfies the existing
  RANGE-interval bound semantics on t[k] within D of t[j] }` — a time-bounded trailing window,
  where `t = ORDER BY x` is the single ORDER BY expression restricted to the shared
  resolver's temporal key domain — **DATE, TIME, DATETIME or TIMESTAMP** (`§3.3` item 4,
  decision D21: YEAR and every other type are rejected at binding time) — and `D` is
  an interval
  expression (`INTERVAL n unit`). The window bound semantics — including calendar units
  (MONTH/YEAR/WEEK vary by date; they are never folded into a fixed microsecond count),
  unit normalization, DST and civil-time behavior, inclusive/exclusive edges, and NULL
  ordering — are inherited wholesale from MatrixOne's existing
  `RANGE BETWEEN INTERVAL D PRECEDING AND CURRENT ROW` machinery: the binder consumes the
  `INTERVAL n unit` argument in its name-keyed conversion switch (§3.1 `D`,
  §3.3 items 3-4) **normalized with the window frame-bound interval grammar** — the same
  `NormalizeWindowInterval` matrix user RANGE frame bounds normalize through
  (`resetWindowIntervalExpr`/`setWindowIntervalValue`, `window_binder.go`), preserving
  source type, spelling and sign until normalization — and the operator derives the
  trailing bound through the existing
  interval semantics (calendar units, civil-time rules, overflow checks) from the published
  `(count, unit)` arguments in their D18 published forms.
  Three `D` outcomes are distinct and all three are inherited exactly from that machinery
  (D15/D17): a NULL *source value* is an error (illegal-frame parse class); a *successful*
  normalization to NULL arithmetic (extra compound fields, e.g. `INTERVAL '00:01.5'
  MINUTE_SECOND` — `NormalizeWindowInterval` returns `null=true` without error, preserved
  as a null-marked constant by the binder) yields **the shared resolver's key-dependent
  NULL-arithmetic membership — non-NULL-key rows get empty frames with `result(j) =
  NULL`, and NULL-key rows keep their ordinary NULL-key peer group with the trailing
  positional cutoff, ranked by the ordinary invariant** (D17; `window.go:2258-2265`),
  never an error, matching the successful `NULL` sums of the equivalent user RANGE frame
  on non-NULL keys (`window_interval_parameters.result:22-29`) and its NULL-key peer
  retention; and a normalization
  *error* takes the shared helpers' own classes (negative → illegal-frame parse error,
  non-finite numeric → InvalidInput, overflow → the helpers' OutOfRange/InvalidArg classes).
  `D = 0` is
  valid and selects the rows whose timestamp is `t(j)` itself (an exact-timestamp trailing
  window), and an invalid string prefix (`INTERVAL 'abc' DAY`) succeeds as `D = 0` with no
  error, also per `NormalizeWindowInterval`'s zero rule. The right edge is the current
  *row position* (not the
  peer group): rows with an equal timestamp that sort after `j` are excluded because `tmrank` is
  a trailing moving operator — the acceptance tests distinguish this from later-peer inclusion
  with explicit equal-timestamp cases.

The six-row oracle from the issue, checked against `§2.1`:
fixture `(id,v)`: `(1,NULL),(2,2),(3,4),(4,4),(5,NULL),(6,-1)`;
`mrank(v,3) OVER (ORDER BY id)`:
`id1 v=NULL → NULL`; `id2 frame{1,2} less(2)=0 → 1`; `id3 frame{1,2,3} less(4)=2 → 2`;
`id4 frame{2,3,4} less(4)=1 → 2`; `id5 v=NULL → NULL`; `id6 frame{4,5,6} less(-1)=0 → 1`.
Result `NULL, 1, 2, 2, NULL, 1` — matches the verified control.

### 2.3 Declared conventions (as the issue requires them to be documented)

- Rank convention: competition (1-based, ties share ranks; e.g. frame values `{4,4,9}` rank
  `9` as 3). Dense, average, and percentile local-rank conventions are a declared non-goal of
  this revision (`§8`, decision D2).
- Direction: ascending (smallest in frame = 1). A descending or per-call direction parameter is
  a non-goal (D2). A numeric user can emulate descending locally by ranking `-v`.
- Window direction: frames always look backward (trailing). `F(j)` never contains rows after
  the current row.
- Rows are physical positions under the OVER `ORDER BY` (ROWS-frame-like). Peer groups (equal
  sort keys) do not collapse the frame.
- Partition rule: frames never cross partition boundaries; a partition is the maximal set of rows
  with equal `PARTITION BY` keys. Without `PARTITION BY`, the whole input is one partition.
- Explicit frames: the three operators derive `F(j)` from their own arguments, so an explicitly
  written frame in `OVER` is a parse-time error (`§3.3`). This is intentional: accepting a frame
  would create two conflicting window sources, the exact ambiguity the issue rejects.
- Without `ORDER BY`, row order is the operator's input order, the same indeterminate order the
  SQL standard assigns to `ROW_NUMBER()` without ORDER BY; result values then vary with that
  order. Not an error; documented behavior.

## 3. SQL contract

### 3.1 Signatures

```sql
MRANK(<numeric|temporal expr> v, <integer expr> N)            -- result type: UNSIGNED BIGINT
CUMRANK(<numeric|temporal expr> v)                            -- result type: UNSIGNED BIGINT
TMRANK(<numeric|temporal expr> v, INTERVAL <n> <unit> D)      -- result type: UNSIGNED BIGINT
```

- Name resolution is case-insensitive (matrixone lowercases function names before binding), as
  for every existing function.
- `v`: any numeric or temporal scalar type the frame can compare (`types.IsNumericOrTemporal()`
  domain: signed/unsigned/float/decimal and datetime/date/timestamp/time families). NULL values
  permitted. Strings are out of scope for this revision because an order contract for them needs
  the collation decision logged in D3; strings produce a binding-time "invalid argument" error.
- `N`: any integer-typed expression (as with `NTILE`'s bucket count and `LAG`'s offset). The
  per-row effective value must satisfy `N ≥ 1` and NULL is invalid; violations surface as
  execution-time InvalidInput errors named to the operator (mirroring existing
  `ntile bucket count must be positive`, `lag offset cannot be NULL/<= 0` behavior). The frame is
  clamped by the partition start, so an `N` larger than the partition is not an error.
- `D`: an interval expression (`INTERVAL n unit`, the `types.T_interval` pair the parser
  already produces inside function argument lists), consumed
  **inside the binder's name-keyed conversion switch** (`base_binder.go`
  `bindFuncExprImplByPlanExpr` case switch, the `date_add`/`uuid` site) **before overload
  resolution and before the publish-time no-escape postcondition**
  (`rejectIntervalArgs` after successful resolution, `base_binder.go:8214`) — but
  normalized **with the window frame-bound interval grammar, not the general DATE_ADD
  grammar** (decision D14): the value interpretation is exactly what user
  `RANGE BETWEEN INTERVAL ... PRECEDING` frame bounds receive (`resetWindowIntervalExpr`/
  `setWindowIntervalValue` in `window_binder.go`, whose literal entry is
  `types.NormalizeWindowInterval` — the grammar `types/window_interval.go` documents as
  deliberately separate from the DATE_ADD grammar that `bindStringIntervalExpr`/
  `NormalizeIntervalWithOverflow` implement, `base_binder.go:12544/:12556`) — and,
  **before any normalization or evaluation runs, the whole `D` source passes the
  shared pre-evaluation admission boundary (decision D22)**: (i) the
  **control-function prohibition** — `mo_ctl`/`fault_inject` anywhere in the source
  (direct, nested in arithmetic, or inside a nested window entry), refused with the
  frame-bound gate's exact `NotSupported` message, because the bound is *evaluated
  at bind time, before the frontend's mo_ctl sys-admin gate scans the built plan*,
  and the folded constant then replaces the call where a plan scan could see it
  (`makeWindowFrameConstValue`, `window_binder.go:1298-1308`, the #28985 contract;
  the recursive `HasMoCtrl` walk, `utils.go:3845`) — while the normalizer
  `resetWindowIntervalExpr` evaluates sources directly
  (`window_binder.go:1469-1474`) with no internal check of its own; (ii) the
  **foldability/marker admission** — a static shape walk where literal constants,
  the direct prepared magnitude marker (the D18 raw pair's source) and
  constant-foldable function spines with no column reference are admitted, and
  everything else — column-reference (non-foldable) magnitudes and indirect
  prepared forms — is refused pre-evaluation with the case's function-named
  invalid-argument error, giving the promised non-foldable rejection an explicit
  owner ahead of the eager evaluation instead of an emergent executor failure;
  **source type, spelling and sign are preserved until that normalization**: a
  string-literal source `INTERVAL '1.5' DAY` truncates to 1 day (window text grammar)
  instead of becoming 1.5 days as the DATE_ADD grammar would; a decimal/float literal
  with a time unit takes the frame-bound fractional semantics (rounded to microseconds,
  unit folded to MICROSECOND); other constant-foldable expressions (admitted by the D22
  boundary) fold through the
  frame-bound const-fold branch **exactly as the shared path implements it (decision
  D20)** — `window_binder.go:1453-1489`: the source is wrapped in the **same int64
  CAST the shared path applies** (`appendCastBeforeExpr` to `T_int64`) *before*
  evaluation, so a **fractional source value is resolved by the shared CAST, not
  rejected**: FLOAT sources round through the shared `floatToInteger`
  (`func_cast.go:3778-3800`, `math.Round`), exactly as a user RANGE frame bound with
  the same expression would — `INTERVAL SIN(1) DAY` keeps `SIN(1)` a FLOAT function
  expression until the cast, rounds `0.8415…` to the integer **1**, and publishes a
  1-DAY width; the published result is the **post-cast integer**, so there is no
  non-integral fold result to reject, and only a **NULL** fold result takes the
  illegal-frame parse error (`window_binder.go:1480-1481`). The outcome matrix for
  `D` inverts to three separate cases, each taken
  from the shared helpers verbatim (decision D15): (a) a **NULL source value** — a
  constant NULL literal, or an EXECUTE-d NULL marker value — is an **error** in the
  illegal-frame parse class (`newWindowFrameIllegalError` at bind,
  `pkg/sql/colexec/window/interval.go`'s NULL-evaluation branch at execution); (b) a
  **successful normalization to NULL arithmetic** (`NormalizeWindowInterval` returning
  `null=true` with no error — extra compound fields such as `INTERVAL '00:01.5'
  MINUTE_SECOND`, compound split errors such as `INTERVAL '1/2/3' MINUTE_SECOND`) is
  **not an error**: the binder preserves it as a null-marked constant
  (`window_binder.go:1380-1385`) and the materialization pipeline publishes a null-marked
  magnitude, so the window is **the shared resolver's key-dependent NULL-arithmetic
  membership (D17)** — non-NULL-key rows get the empty frame and `result(j) = NULL`
  (exactly matching the successful `NULL` sums of the equivalent user RANGE frame on
  its non-NULL keys, `window_interval_parameters.result:22-29`), while NULL-key rows
  keep their ordinary NULL-key peer group with the trailing positional cutoff
  (`window.go:2258-2265`, asserted by `colexec/window/interval_test.go:117-158`); (c) a
  **normalization error** takes
  the shared helpers' own error class — negative → the illegal-frame parse error
  (`windowIntervalNegativeError`), non-finite numeric sources (`NaN`/`Inf`/`+Inf`) →
  InvalidInput ("non-finite window interval", `window_interval.go:31-32`), digit/magnitude
  overflow → OutOfRange ("int64", "window interval",
  `window_interval.go:130-149`) or, through the bound-magnitude re-check
  `types.JudgeIntervalNumOverflow`, InvalidArg ("interval", num) — never a blanket
  illegal-frame parse error. Invalid string prefixes (`INTERVAL 'abc' DAY`) succeed as
  `D = 0` (NormalizeWindowInterval's zero rule), and invalid units are rejected.
  Calendar unit codes are preserved (MONTH/YEAR/WEEK never
  folded into microseconds; SECOND folds to microseconds and compound spellings fold to
  SECOND/MICROSECOND/MONTH per the window compound grammar), and the
  decimal-provenance `DecimalLiteralRequiresV82` protocol marker is applied exactly as
  `setWindowIntervalValue` applies it. The `T_interval` argument is replaced by two
  function arguments `(count, unit)`, and the
  **raw-versus-normalized representation follows the shared materialization's own durable
  discriminator (decision D18)** — the unit slot's literal kind, exactly what
  `materializeWindowInterval` distinguishes (`colexec/window/interval.go:80-96`: an
  int64-coded unit marks an immutable normalized bound whose int64-literal source —
  possibly null-marked, outcome (b) — is consumed directly and non-int64/non-literal
  sources are rejected; a MySQL-string unit marks a raw bound whose source runs the full
  normalization pipeline): **normalized constants publish `(int64 count const,
  int64 unit-code)`** and **retained prepared magnitudes publish `(retained source
  expression, unit-string literal)`** — the unit string travels beside the source, so
  the raw form survives parameter substitution (which replaces only the count slot: a
  substituted-literal source paired with the string unit still reads as raw, precisely
  the case the helper's own comment names — "a raw interval whose parameter was
  replaced by a literal"), projection refresh and cache restoration, without ever
  publishing a `T_interval`. The resulting **internal signature is
  `TMRANK(v, count, unit)`** with the unit slot accepting exactly the normalized int64
  code or the raw unit-string literal, and the count slot holding either the window-
  normalized (possibly null-marked, outcome (b) of the matrix above) int64 constant
  or, for a direct prepared magnitude (`INTERVAL ? unit`),
  the retained source expression itself — type, spelling and sign intact, mirroring
  `bindPreparedWindowFrameBound`'s marker retention for frame bounds
  (`window_binder.go`: "retain the original unit and scalar source for per-execution
  normalization. A temporal cast here would erase the offset's domain"); indirect
  prepared forms and non-foldable dynamic magnitudes are rejected exactly as frame
  bounds reject them, and no implicit cast may be inserted on either slot. Both the D16
  validate-discard owner and the operator's evaluation adapt this representation to
  shared materialization through one adapter (D18, §3.3 item 4): a raw
  `(source, unit-string)` pair runs the raw pipeline (evaluate, domain dispatch,
  `NormalizeWindowInterval`, overflow checks — outcomes per the matrix above, an
  invalid raw NULL erroring while a valid normalized NULL-arithmetic count succeeds);
  a normalized `(int64 const, int64 code)` pair runs the immutable check. A raw
  `T_interval` never survives in any published plan expression, so both the generic-function
  postcondition and every rebinding pass are clean. Direct three-int-argument public calls
  cannot occur (the grammar production only accepts the `INTERVAL n unit` form), while
  re-resolution of the saved internal form by name+arguments — prepared re-binding
  (`visit_plan_rule.go:3855`, `bindPreparedFuncExprImplByPlanExpr`) and projection refresh
  (`utils.go:7248-7271`, `refreshPreparedPlanProjectionExprType`, including refreshed
  prepared types) — resolves the same registered overload, so prepared statements
  with a changing value domain for `v` (e.g. `ABS(?)`) and repeated executions with changing
  interval magnitudes are first-class. **SQL-EXECUTE source-domain reconstruction
  (decision D19): generic parameter replacement substitutes most markers as TEXT
  transport (`visit_plan_rule.go:3917-3933` — "Most prepared parameters are
  intentionally replaced as TEXT to retain the historical SQL-EXECUTE behavior"), which
  would collapse the numeric/text width distinction, so the rebind path routes tmrank's
  retained count slot through the same source-domain reconstruction owner raw frame
  bounds use — `preparedRuntimeSourceExpr` in the `T_interval`-list branch
  (`visit_plan_rule.go:3940-3957`, whose comment fixes the contract: "Specialize its
  source domain without normalizing its value: cached plan restoration must continue to
  interpret the next value in the original unit"): a new tmrank case in the prepared
  parameter walk applies `preparedRuntimeSourceExpr` to the count-slot parameter
  position so numeric EXECUTE values keep the numeric domain (nearest-integer → `1.5`
  DAY is 2 days) and string values keep the text domain (truncation → `'1.5'` DAY is
  1 day), with cache-restoration provenance preserved.** The retained count slot's
  concrete value
  materializes per execution through the same raw-pair rules prepared frame bounds
  materialize through (`colexec/window/interval.go` `materializeWindowInterval`: evaluate
  the source, dispatch the value's domain — string domains take the window text grammar,
  numeric domains take MySQL's nearest-integer conversion, via
  `windowIntervalScalarText` — then `types.NormalizeWindowInterval` and the shared
  overflow rules `types.JudgeIntervalNumOverflow` + temporal-range conversion), with the
  same three-outcome matrix as constants; that per-execution materialization is
  **validated by the compile-time width-validation owner of §3.3 item 4 (D16) before any
  output row is produced**, so its outcomes are observable even on empty input or when
  LIMIT 0 eliminates the window operator.
  `D ≥ 0`, and `D = 0` is valid (exact-timestamp trailing window, §2.2). The
  standalone-interval no-escape boundary (`interval_expr.go`) stays active for every other
  argument position of these three operators and for all other functions.
- Used without `OVER`, the call fails with the framework's grammar-level window-function error:
  because the names become parser window-function tokens with mandatory `window_spec`, the
  statement gets the same 1064 syntax-error class `SELECT rank();` produces today (the grammar
  rule `function_call_window: <NAME> ... window_spec`). This changes the issue's probe from
  `(20105, "not supported: function or operator 'mrank'")` to that parser error, proving the
  name exists; the working `OVER` form is the availability proof of record.
  The three names are added as **non-reserved** window tokens following the existing
  `LAG`/`LEAD`/`FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE` pattern (`mysql_sql.y` lists them in the
  non-reserved keyword set while keeping their mandatory-window_spec productions): existing
  table, column and alias identifiers named `mrank`, `cumrank` or `tmrank` keep parsing
  unquoted, so the feature stays purely additive.
- `DISTINCT` inside these calls is rejected by the existing generic window-function DISTINCT rule.

### 3.2 Examples

```sql
-- the issue's business profile, self-contained
SELECT id, mrank(v, 3) OVER (ORDER BY id) FROM raw;
-- frame-local rank must remain frame-local; partition-wide rank changes each frame
SELECT id, mrank(v, 3) OVER (ORDER BY id), rank() OVER (ORDER BY v) FROM raw;
-- partitions bound the frame
SELECT gid, tick, mrank(v, 3) OVER (PARTITION BY gid ORDER BY tick) FROM ev;
-- cumulative variant
SELECT tick, cumrank(v) OVER (ORDER BY tick) FROM ev;
-- time-domain variant over irregular event times
SELECT tick, tmrank(v, INTERVAL 3 DAY) OVER (ORDER BY tick) FROM ev;
```

### 3.3 Binding rules (owning code paths)

1. Parser (`pkg/sql/parsers/dialect/mysql/keywords.go` + `mysql_sql.y`, regenerate with the
   repository's zero-conflict goyacc gate `make -C pkg/sql/parsers/dialect/mysql`): register
   `MRANK`, `CUMRANK`, `TMRANK` as **non-reserved** window tokens — the exact pattern
   `LAG`/`LEAD`/`FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE` already use (window productions plus a
   non-reserved keyword entry so identifiers by those names keep parsing) — and add
   `function_call_window` productions with a **mandatory** `window_spec`:
   `MRANK '(' expression ',' expression ')' window_spec`,
   `CUMRANK '(' expression ')' window_spec`,
   `TMRANK '(' expression ',' interval-shaped expression ')' window_spec`.
   The mandatory window_spec is what makes the names reject the OVER-less probe with the
   same 1064 class as `rank()`; the non-reserved entry is what keeps existing
   `mrank`-named table/column/alias identifiers valid. Extend the mysql parser round-trip
   tests (`mysql_sql_test.go`) for the new productions and for identifier compatibility.
   (`SELECT tmrank(v, INTERVAL 3 DAY)` already
   parses today through the generic expression rule; the new production keeps accepting that
   argument shape.)
2. `pkg/sql/plan/function/function_id.go`: add `MRANK`, `CUMRANK`, `TMRANK` IDs **616**, **617**,
   **618** — the smallest function-unused numbers after the base merge (603 and 604 are already
   allocated to `MO_VIEW_DEFINITION` and `MO_VIEW_CHECK_OPTION`; main's technical-indicator
   kernels #31017 claimed 605-615 and advanced the sentinel to 616 before this branch's
   conflict-resolution merge) — and advance `FUNCTION_END_NUMBER` to **619**, keeping the
   repo's documented allocation protocol (append before the sentinel, never renumber existing
   or already-on-main IDs; existing `mo_view_definition`/`mo_view_check_option` keep 603/604 so the
   `ViewMetadataFunctions` protocol feature detection in `pkg/pb/plan/string_literal_form.go`
   is untouched). Nothing but the rank IDs themselves moved: they are referenced by name
   everywhere (the file's own ONNX_RUN/LOAD_TEXT renumbered-on-main-merge precedent), so the
   ordered renumber is behavior-neutral. Register lowercase names in `functionIdRegister`; update `predefinedFunids`
   (`function_id_test.go`) with the exact new IDs alongside a registry-uniqueness check.
3. `pkg/sql/plan/function/list_window.go`: register three `FuncNew` entries in
   `supportedWindowInNewFramework` with class `plan.Function_WIN_VALUE` (value-window =
   needs direct access to other rows of the partition, which is precisely this feature; no
   protobuf enum change, so no wire-format impact), `layout: STANDARD_FUNCTION`, and per-function
   `checkFn` argument-shape rules matching the shapes that actually reach overload
   resolution. `mrank` exactly `(numericOrTemporal, integer)`, `cumrank` exactly
   `(numericOrTemporal)`, `tmrank` exactly `(numericOrTemporal, count, unit)` where
   the count slot is the **window-normalized (possibly null-marked, D15) int64 constant
   or, for the retained direct
   prepared magnitude only (§3.1 `D`, §3.3 item 4), the marker-typed source expression**,
   and the unit slot is the **normalized int64 unit-code constant or the raw
   unit-string literal** — the D18 discriminator mirroring `materializeWindowInterval`'s
   own raw-vs-normalized distinction (`colexec/window/interval.go:80-96`), so the
   internal three-argument shape the binder's tmrank case publishes remains
   self-describing through parameter substitution, projection refresh and cache
   restoration. This mirrors how
   `date_add` registers only its internal `(source, T_int64, T_int64)` overloads
   (`list_builtIn.go` DATE_ADD block) while its two-argument SQL syntax is converted
   during binding and needs no catalog entry of its own. **No implicit cast is inserted
   on tmrank's count or unit slot** — the count slot's domain must reach the operator for the
   window-grammar string-vs-numeric dispatch (D14), and the unit slot's literal kind must
   survive to mark raw-vs-normalized (D18); the binder's tmrank case is the only
   writer of the internal form (direct multi-argument spellings are rejected at the
   case, §3.3 item 4), so the accepted slot provenance is exactly what the case
   publishes. The public `(v, interval)` two-argument shape is therefore never
   overload-resolved and never re-resolved; every rebinding pass
   (`visit_plan_rule.go:3855`, `utils.go:7257`) sees a published shape the catalog
   accepts, which is what closes the round-3 binding finding. Return type:
   `aggexec.SingleWindowReturnType` (`types.T_uint64`), matching rank-family results.
   The AST→plan representation is therefore exactly:
   `INTERVAL n unit` AST → `types.T_interval` list pair `(source, unit-string)` →
   in-binding conversion through the **window frame-bound normalization matrix**
   (§3.1 `D`): constants are normalized at bind — unit string resolved to a
   `types.IntervalType` code with `types.IntervalTypeOf` and folded per the window
   grammar, count normalized with the source-preserving rules of the frame-bound entry
   (`resetWindowIntervalExpr` matrix: string text grammar, decimal/float
   microsecond folding, const-fold; negative/NULL illegal) — and a direct prepared
   magnitude keeps its source expression in the count slot → published function
   arguments `[v, count, unit]`. No raw `T_interval` survives in any published plan
   expression, and no published constant carries DATE_ADD-grammar values.
4. Binding-time rules in two owning files. `pkg/sql/plan/base_binder.go`
   (`bindFuncExprImplByPlanExpr`'s name-keyed conversion switch — the same site `date_add`
   at :7594 and `uuid` at :7634 use): a new `"tmrank"` case consumes the `INTERVAL`
   argument **inside binding, before overload resolution** — hence before the publish-time
   no-escape postcondition (`rejectIntervalArgs` after successful resolution,
   `base_binder.go:8214`), which a raw `T_interval` argument would otherwise trip. When
   the arguments are the public two-argument form `[v, T_interval]`, the case first
   routes the interval source through the **shared pre-evaluation admission boundary
   (decision D22)** — (i) the #28985 control-function prohibition, the exact
   `HasMoCtrl` gate `makeWindowFrameConstValue` runs *before* calling
   `resetWindowIntervalExpr` (`window_binder.go:1298-1308`): `mo_ctl`/`fault_inject`,
   direct or nested anywhere (the recursive `utils.go:3845` walk, including its
   window-entry recursion), is refused with the gate's `NotSupported` message before
   the normalizer can evaluate — `resetWindowIntervalExpr` evaluates sources directly
   (`window_binder.go:1469-1474`) with no internal check, and generic privilege
   scanning runs later on the built plan, where the folded constant has already
   replaced the call, so it cannot protect the bind-time evaluation; (ii) the static
   admission walk: literal constants, the direct prepared magnitude marker and
   constant-foldable spines (no column reference) are admitted; column-reference
   (non-foldable) magnitudes and indirect prepared forms are refused with the case's
   function-named error. The boundary is **one shared owner for both routes**:
   `makeWindowFrameConstValue` keeps its observable behavior by calling it in place
   of its inline gate (identical message and class, so `window_binder_moctl_test.go`
   stays valid), and `resetWindowIntervalExpr` gains the tier-1 check at its entry as
   defense-in-depth, making the normalizer safe by construction for every route to
   it (its sole production caller today is the frame-bound gate at
   `window_binder.go:1308`; the `projection_binder.go:185-187` wrapper has no
   production caller, and the entry re-check is a pure read-only walk). Only then
   does the case rewrite the
   interval list pair in place through the **window frame-bound normalization matrix**
   (§3.1 `D`: the `resetWindowIntervalExpr`/`setWindowIntervalValue` rules —
   `NormalizeWindowInterval` text grammar for string sources, microsecond folding for
   decimal/float time-unit literals, const-fold for other foldable expressions (the
   shared int64 CAST first, so fractional FLOAT sources round per `floatToInteger`
   — D20), `newWindowFrameIllegalError` for negative/NULL, `DecimalLiteralRequiresV82` on decimal
   provenance — preserving source type, spelling and sign until normalization, with
   indirect prepared forms and non-foldable magnitudes refused by the D22 boundary
   before any evaluation), yielding the
   published internal shape `[v, count, unit]` in its **D18 self-describing forms**:
   constant sources publish the normalized pair `(int64 count const, int64 unit-code)`
   (possibly null-marked for the D15 NULL-arithmetic outcome), and the direct prepared
   magnitude publishes the raw pair `(retained source expression, unit-string literal)` —
   the durable raw-vs-normalized discriminator that survives parameter substitution
   (`visit_plan_rule.go:3905-3939` replaces only the count slot), projection refresh and
   cache restoration. On the SQL-EXECUTE rebind walk, a new tmrank case applies the
   **source-domain reconstruction owner** (`preparedRuntimeSourceExpr`, the same
   entry the `T_interval`-list branch uses at `visit_plan_rule.go:3940-3957`) to the
   count-slot parameter position, so TEXT-transport substitution does not erase the
   numeric-vs-string width domain (D19).
   When the arguments are already the
   internal three-argument shape — the saved plan after the first bind, re-entered by
   prepared re-binding (`visit_plan_rule.go:3855`) or by projection-type refresh
   (`utils.go:7257`, both routing through `bindPreparedFuncExprImplByPlanExpr`, which
   passes `allowInternalFunctionArgs=true`) — the case passes them through unchanged,
   the `date_add` internal-form skip (`allowInternalFunctionArgs && len(args) == 3`,
   `base_binder.go:7620`): re-binding re-resolves the same catalog overload with refreshed
   argument types, so a `v` domain that changes with its parameter's EXECUTE value (e.g.
   `ABS(?)` text→numeric) and a count parameter that changes between executions stay
   first-class. Every other reachable argument shape (direct `tmrank(v, 5)` or
   `tmrank(v, 5, 3)` spellings) is rejected at the case with a function-named `InvalidArg`,
   exactly as `date_add`/`uuid` reject their direct internal spellings; the grammar's
   interval-shaped production makes those spellings unreachable from user SQL anyway. A
   **constant** NULL or negative count is rejected at bind with the window frame illegal
   parse error (`newWindowFrameIllegalError`, the frame-bound error class — not the
   DATE_ADD classes) — except where D15's outcome matrix says otherwise: extra compound
   fields and compound split errors `succeed` as null-marked constants (empty frames, NULL
   results, no error) and invalid string prefixes succeed as `D = 0`. A retained prepared
   count materializes per evaluation through the raw-pair rules
   (`materializeWindowInterval`: domain dispatch, window normalization, and the shared
   overflow re-check `types.JudgeIntervalNumOverflow` + temporal-range conversion) with
   the same D15 outcome matrix at execution: a NULL marker value errors in the
   illegal-frame parse class, a successful null-arithmetic normalization is published as
   a null-marked magnitude (empty frames, NULL results), and overflow or non-finite
   values take the shared helpers' own classes (InvalidArg via
   `JudgeIntervalNumOverflow` / OutOfRange via the temporal-range conversion /
   InvalidInput for non-finite) — never a blanket illegal-frame parse error.
   **Statement-constant width validation owner (decision D16).** Revision 4 placed all
   width validation in per-output-row evaluation, which this round's review showed is
   insufficient: `Window.Prepare` materializes only frames (`window.go:85-99`), so with
   empty input or a LIMIT 0 shortcut that eliminates the window operator no row
   evaluation happens and an invalid prepared width would go unchecked, breaking the
   declared execute-time contract. The design therefore extends the repository's
   existing owner for exactly this problem — `ValidateIntervalFrameBounds`
   (`pkg/sql/colexec/window/interval.go:34`, invoked at
   `pkg/sql/compile/compile.go:923-930`, whose in-repo comment blocks literal
   window-argument errors "even when LIMIT 0 removes their physical operator") — from
   inspecting only frame-bound `T_interval` expressions to also walking each
   `node.WinSpecList` entry's `window_func` expression (the WindowSpec message already
   carries `window_func = 1` beside `frame = 4`, `proto/plan.proto:771-777`): for every
   tmrank entry, the owner runs the D18 adapter over the entry's `(count, unit)` slots
   (raw unit-string pair → the raw materialization pipeline; normalized int64 pair →
   the immutable check) as a **validate-discard check** — the
   normalized copy is thrown away, the reusable plan is not mutated, and only the error
   propagates — the identical contract the function's own doc comment states for frame
   bounds ("Validation borrows the logical plan and discards normalized copies; Prepare
   still owns the operator's runtime frames"). Both retained sources and already-
   substituted literal count slots validate identically (the discriminator makes
   marker-versus-literal irrelevant), so a raw NULL width, a raw out-of-range width and
   a raw non-finite width all fail here on every execution attempt while raw valid
   string/numeric widths and normalized (possibly null-marked) constants pass;
   normalized constants were already validated at bind and pass this check trivially.
   This consumer is added to the change map (§6) and its
   row to the validation ladder. `pkg/sql/plan/window_binder.go` (`bindWindowSpec`, after
   `ensureDefaultWindowFrame` semantics are settled): for each of the three names, when
   `ws.HasFrame == true` (a frame the user wrote), fail with `InvalidInput`: "`<name>`'s
   window comes from its arguments and does not accept an explicit frame". No frame is
   synthesized on the window spec for any of the three names: `tmrank`'s width travels as
   its published `(count, unit)` function arguments and the operator derives each row's
   frame at evaluation time (§3.4); `mrank` and `cumrank` are positional arithmetic on the
   same published-argument route. For `tmrank`, the ORDER BY key domain is **restricted
   to the shared resolver's supported types (decision D21)**: exactly one ORDER BY
   expression whose plan type Oid is one of **`T_date`, `T_time`, `T_datetime`,
   `T_timestamp`**, checked at binding time with a named tmrank parse-error diagnostic
   for YEAR or any other type. The RANGE-N wording ("exactly one ORDER BY expression
   of temporal type") is too broad for this operator: `IsTemporal()` admits `T_year`
   (`pkg/container/types/types.go:750-757`) and YEAR columns are published with that
   Oid (`pkg/sql/plan/build_util.go:384-385`), while the shared resolver has no YEAR
   case and would fail at execution with the internal unsupported-type error
   (`pkg/sql/colexec/window/window.go`, the RANGE-search switch's `default` branch:
   "unsupported type %v for RANGE frame in window function"). `mrank`/`cumrank`
   accept any ORDER BY shape, including none.
5. Function-class routing already works through `getFunctionIdByNameWithoutErr` +
   `isWindowValue()`; the binder dispatch (`base_binder.go:3380` → `BindWinFunc`) and the
   operator dispatch (`window.go` `GetFunctionIsWinValueFunByName`) need no structural
   change beyond the new catalog entries. Every other window_SQL context rule (WHERE/GROUP BY/
   generated column/recursive CTE restriction, nesting rejection, alias/AST dedup keys) is
   generic and picked up automatically.

### 3.4 Execution rules (owning code path)

`pkg/sql/colexec/window/window.go`, `processValueFuncRange`: add named cases `"mrank"`,
`"cumrank"`, `"tmrank"` beside `"lag"`/`"lead"`/`"first_value"`/... reusing that machinery
(the value-window class already contains both flavors the family needs: `lag`/`lead`, whose
windows are argument-driven and frame-ignoring, and `first_value`/`last_value`/`nth_value`,
which are frame-relative through the same resolver `tmrank` reuses):

- `ctr.aggVecs[idx].Vec` already holds every evaluated execution argument (**`v`; for
  `mrank`, the `N` vector; for `tmrank`, the evaluated `count` and `unit-code` vectors**
  the binder of §3.3 item 4 publishes) across the materialized, sorted batch; `ctr.ps` /
  `buildPartitionInterval` already bounds partitions; the outer result-vector pattern
  (`allocation.NewVector`, per-row `checkCanceled`, `ReserveMetadata` scratch,
  `makeResultBatch`)
  already matches results to output rows. No new operator, plan node, or protobuf message.
- Per output row `j`: consume the row parameters per operator (`mrank`: `N_j` present,
  integer, ≥ 1, non-NULL; `tmrank`: **a null-marked count is the D15 NULL-arithmetic
  window with the D17 key-dependent membership — the operator does NOT short-circuit to
  a blanket empty frame: it passes the null-marked state through the shared resolver's
  own NULL-interval branch (`buildRangeIntervalWithLocation`, `window.go:2258-2265`),
  so a row with a non-NULL temporal key gets `F(j) = ∅` and `result(j) = NULL`, and a
  row with a NULL temporal key keeps its ordinary NULL-key peer group with the trailing
  positional cutoff (a lone NULL-key row with non-NULL `v` ranks 1) — no error**;
  a non-null
  count materialized under the shared rules flows on), compute the frame per `§2.2`
  (`mrank` arithmetic on positions; `cumrank` from `partStart(j)`; `tmrank` uses the
  window-normalized `(count, unit)` pair — constants arrive already normalized at bind
  (§3.1 `D`), and a retained prepared count materializes per execution through the D18
  adapter on the same raw-pair rules prepared frame bounds use (domain dispatch via
  `windowIntervalScalarText`, `NormalizeWindowInterval`,
  `JudgeIntervalNumOverflow`, temporal-range conversion) — and resolves its row span
  inside `[partStart(j), j]` by
  passing the resulting transient interval bound through the existing frame-row resolver
  helpers (`buildIntervalRows`/`buildInterval` at `window.go:1762` — the same civil/DST/
  fold-aware machinery the `first_value`/`last_value`/`nth_value` cases use at
  `window.go:1521`), with the right edge clamped to the current row position `j`), then
  count
  `v[k] < v[j]` over typed columns via one type switch over the argument
  vector's Oid family (numeric signed/unsigned/float/decimal; temporal cast-normalized through
  microsecond payloads like existing temporal comparisons). NULL `v[k]` rows are skipped
  naturally.
- Comparison domain note: equal values never count, so rank inflation cannot occur; both frame
  endpoints use row positions, so ties in `v` or in the ORDER BY key do not shift frames.
- Error contract: invalid widths never reach row evaluation — constants are validated at
  bind (§3.1 `D` outcome matrix) and retained statement-constant sources are validated by
  the compile-time owner of §3.3 item 4 (D16) before sources start, on every execution
  attempt, independently of row production and of operator presence; the operator keeps
  only a defensive backstop that aborts with the shared helpers' class for the failed
  route. A failed validation is a per-query error — its class is the shared helper's own
  (illegal-frame parse for NULL/negative, InvalidInput for non-finite, InvalidArg/
  OutOfRange for overflow), not a blanket function-named InvalidInput; D15's
  NULL-arithmetic outcome is a successful empty-frame case, never an error. No partial
  results are emitted for a failed output chunk (the operator aborts the
  pipeline, preserving existing failure atomicity).
- Evaluate per bounded output chunk. Direct scans poll for cancellation inside
  each frame. The cumulative and constant-width index path and its ownership,
  cancellation, admission fallback, and sliding updates are described in the
  performance amendment above.

### 3.5 Complexity and resource bounds

- Indexed `cumrank` and constant-width `mrank` use `O(n log n)` time and
  `O(n)` additional memory per partition. Short partitions and widths use
  direct scans, as do row-varying widths and `tmrank`.
- Direct `mrank` scans take `O(sum_j min(N_j, |partition_j|))`; `cumrank`
  fallback takes `O(n²)` in the worst case; `tmrank` takes `O(sum_j |F(j)|)`.
- All paths poll for cancellation during construction and/or scanning. The
  input partition may spill before evaluation; the optional index itself is
  not spillable and falls back on a capacity rejection.

## 4. Standards, precedent, and alternatives

- SQL:2023 window functions give no frame-local rank (ranking functions lock the frame to the
  partition; MySQL also documents that assignment of a ROWS clause to `RANK` changes nothing).
  The local-ranking requirement — and the specific trailing-frame formulation — comes from the
  time-series community; the issue cites DolphinDB m-functions (`mrank`, `tm*`, `cum*`) as
  reference UX. Per the issue text, no DolphinDB runtime exists for parity testing and no
  all-parameter parity is claimed; only the declared business profile of `§2.1` is contractual.
  The `OVER (PARTITION BY ... ORDER BY ...)` clause is matrixone's standard expression of what
  DolphinDB expresses as vector order, so the DolphinDB argument vector order maps to our
  ORDER BY semantics.
- Canonical SQL equivalent, adopted as the testing oracle: the issue's self-join control
  (`b.id BETWEEN a.id-2 AND a.id`, `1 + SUM(b.v < a.v)`), which is expensive (`O(n²)` join) and
  proves nothing about entry availability — it is the math oracle only.
- Alternatives evaluated:
  1. Plan-level rewrite to a self-join: rejected. It compiles the reference workload rather than
     the semantics: join inflation is quadratic and unpredictable, partition/frame combinations
     (PARTITION + ORDER BY + multiple local ranks in one query) multiply join fan-outs, and the
     named API requirements stay unmet.
  2. Compose the value from existing functions (e.g. `SUM(...) OVER (ROWS ...)`, `COUNT_IF`,
     or `LAG` chains): rejected. A window aggregate applies a row-local expression per row in
     the frame; it cannot reference both `v[k]` for a varying `k` and the fixed `v[j]` in a fixed
     arity expression, which is the same reason no vendor SQL expresses frame-local rank without
     a native operator. `LAG`-chain composition requires an upper bound on `N` per plan and
     deteriorates to another rewrite of the wrong shape. (No rewrite of this form is used.)
     — This proposal is the smallest general mechanism in the repo's own architecture, not a
     special-case trace of any frame size.
  3. Aggregator-executor placement: register the three functions as `aggexec` window executors
     (like `singleWindowExec`/`valueWindowExec`) and let the aggregate pipeline materialize
     every frame and drive `Fill(j, k)`/`Flush`. Rejected for this revision: the repository's
     current pattern for special window functions evaluates them directly in the window operator
     (`processOrderFuncRange` for the rank family, and the `lag`/`lead` cases of
     `processValueFuncRange`), reserving `aggexec` executors for the segment that needs them; the
     `aggexec` value-family executor buffers every frame's rows per output row
     (`valueWindowExec.frameValues`), which is strictly more state than the positional counting
     kernel needs here, and it drags serialization/spill-surface maintenance in with no semantic
     gain. Revisit only if a future consumer requires aggexec-level placement.
  4. Ship only the canonical SQL equivalent as documentation (no engine change). Rejected: the
     issue's accepted-behavior table requires the named operators (entry probes) and the
     acceptance criteria require named/API availability separately from the SQL math;
     documentation alone keeps every entry probe failing with the original 20105.
- The chosen design (native WIN_VALUE-class window functions evaluated in the window operator)
  is the same shape matrixone already uses for every non-aggregate window function today, which
  is why its integration cost and blast radius are minimal.

## 5. Compatibility, delivery, operations

- The change is purely additive user-visible SQL: three new function names, added as
  non-reserved tokens so existing identifiers by those names keep parsing. No table, catalog,
  persistence, wire, protobuf, configuration, or system-view change (class `plan.Function_WIN_VALUE`
  is reused, so no enum is added, so no plan-serialization change occurs), and existing
  function IDs — including the view-metadata functions `mo_view_definition` (603) and
  `mo_view_check_option` (604) and their protocol feature detection — are untouched.
- Mixed-version behavior: an older version without these functions rejects them with the same
  20105 unknown-function error; no other interaction exists, so serving a plan that references
  them to a mixed-version peer fails the same way any newer introduced function fails. There is
  no upgrade, downgrade, backup/restore, or restart impact, and no migration or feature gate to
  introduce: capability is name-availability, exactly like every prior additive function
  (precedent: the hyperbolic function family).
- Authorization: the only new eager evaluation (`tmrank`'s constant-foldable width, at
  bind time) is gated exactly like the existing one — frame bounds are evaluated there
  before the frontend's mo_ctl sys-admin gate scans the built plan, so the conversion
  routes through the shared pre-evaluation admission boundary (decision D22) and refuses
  `mo_ctl`/`fault_inject` outright, for every account including sys-admin (the #28985
  frame-bound contract). Everything else participates in existing checks: argument
  binding, privilege scanning, and view-planning (`windowValidationScans` handling
  already covers user window functions generically) — though generic scanning can no
  longer be assumed to cover the bind-time evaluation itself, because the folded
  constant replaces the call in the published plan before any scan runs, which is
  precisely why the boundary must own it. No data leave the query pipeline; the value
  operator receives only the columns the query already authorized.
- Observability: no new metrics; plan text (`EXPLAIN`), execution budget, and per-row cost
  accounting live in the existing window operator analyzer.
- Failure containment: an invalid `N`/`D` or non-numeric/non-temporal argument fails that one
  query; nothing persists. Compute-blast radius: the largest legal frame is the partition
  (`cumrank`, or `mrank` with a large `N` matches the existing running-frame quadratic
  envelope); no unbounded background work, queues, caches, or goroutines are introduced.

## 6. Verification plan (cheapest deterministic evidence)

Change map and risk levels (validation-evidence.md): one contract in six closures —
(a) catalog registration (`function_id.go`, `list_window.go`): public name/arity/type contract,
R2; (b) binder rules (`base_binder.go` name-keyed conversion case + `window_binder.go`
frame-bound normalization and frame/ORDER-BY rules): public parse-error,
public-to-internal binding, window-grammar normalization and raw-vs-normalized
representation contracts, R2;
(c) operator kernel
(`window.go` value path): public result semantics, R2; (d) statement-constant width
validation (the `compile.go:923-930` call site + `colexec/window/interval.go`
`ValidateIntervalFrameBounds` traversal over WinSpecList entries, adapting the published
representation through the shared materialization adapter) and the SQL-EXECUTE
source-domain reconstruction owner (`visit_plan_rule.go`'s `preparedRuntimeSourceExpr`
route extended to tmrank's flattened count slot, D19): execution-time
validation and source-domain contract for prepared and constant widths independent of
row production, transport protocol and operator presence, R2;
(e) re-binding and source-domain transfer (`visit_plan_rule.go:3855`,
`utils.go:7257`, the `:3940-3957` reconstruction precedent and its tmrank case):
prepared re-resolution contract, R2; (f) UT/BVT/doc artifacts: R1/R0
evidence-only. No persistence, protocol, security, concurrency, or background-work closures are
created (that is why R3 dimensions — race/restart/upgrade/topology/fault evidence — do not apply;
the operator stays partition-local and stateless per chunk).

Validation map per the testing contract (UT = internal invariant, BVT = public SQL contract):

| Behavior | Layer | Case |
|---|---|---|
| Parser acceptance/rejection | UT | the three `function_call_window` productions parse `mrank(v,3) OVER (...)`, `cumrank(v) OVER (...)`, `tmrank(v, INTERVAL n unit) OVER (...)` (round-trip formatting tests in `mysql_sql_test.go`); the OVER-less forms produce the 1064 parse error like `rank()`; **existing table/column/alias identifiers named mrank/cumrank/tmrank keep parsing unquoted** (non-reserved pattern, LAG/LEAD precedent); goyacc regeneration passes the repository zero-conflict gate |
| Function-id allocation hygiene | UT | `predefinedFunids` pinned to the exact new IDs 616-618 under `FUNCTION_END_NUMBER = 619` (renumbered from the drafted 605-607 when main's #31017 claimed that block; see the §9 merge note); registry uniqueness holds across the merged registry (no duplicate ids or names); `mo_view_definition`/`mo_view_check_option` keep 603/604 with their view-metadata regression suites green, and main's `SMA`..`CUMPOSITIVESTREAK` keep 605-615 |
| Local-rank invariant values/NULL bits/row alignment | UT | operator-level table cases vs an independent `O(n·W)` brute-force oracle implemented in the test (a self-contained re-derivation of §2.1), plus the six-row issue fixture verbatim |
| Frame entry/exit, ties, all-NULL frame | UT | table cases: partition-start short frames; adjacent equal values; all frame-role NULL combinations; `N=1`; `N ≥ partition size` |
| Partition boundaries | UT | multi-partition batches, reproducible partition order via ORDER BY; frames confined to partitions |
| Type domain and order dependence | UT | signed/unsigned ints, floats, decimals, datetime/time; DESC/nulls-sorted ORDER BY; **without ORDER BY the frame follows the operator's actual row order, so the oracle asserts against each supplied physical batch order** (trailing frames are order-dependent: e.g. mrank(v,2) yields 1,2,2 over rows 1,2,3 but 1,1,1 over 3,2,1 — no permutation-invariance claim) |
| Invalid inputs | UT | `mrank(v, 0)`, `mrank(v, -1)`, `mrank(v, NULL)`, `mrank(v)` (arg-shape bind error), `mrank(v, 1.5)`/string/N non-integer (bind error), `cumrank(a, b)` (bind error), explicit frame rejection, string-ranked `v` (bind error), `tmrank` without a supported ORDER BY key rejected at bind with the named tmrank diagnostic — YEAR keys included (`IsTemporal()` admits T_year, types.go:750-757, but the resolver has no T_year case), as are string/numeric/multiple/absent keys (D21), constant negative `D` rejected at bind with the illegal-frame parse error (`newWindowFrameIllegalError`), const-fold **NULL** `D` rejected at bind (illegal parse error) **while non-literal fractional sources are accepted through the shared int64 CAST** (D20: `INTERVAL SIN(1) DAY` folds to the 1-DAY width; only NULL results error), an EXECUTE-d NULL/negative prepared `D` rejected at execution-time validation (D16 owner; illegal-frame parse class), an EXECUTE-d non-finite numeric `D` rejected with InvalidInput ("non-finite window interval"), an EXECUTE-d overflow `D` rejected with the shared helpers' InvalidArg ("interval") / OutOfRange ("int64", "window interval") classes, direct or nested `mo_ctl`/`fault_inject` in the `D` source refused at the D22 admission boundary *before* evaluation with the #28985 `NotSupported` message (proven without evaluation by the nil-proc pattern of `window_binder_moctl_test.go`), indirect prepared magnitudes and non-foldable column-reference widths refused pre-evaluation by the boundary's static admission walk with the function-named error (D22), **`D = 0` accepted (exact-timestamp trailing window)**, **extra-field compound spellings succeed with the D15/D17 key-dependent NULL-arithmetic membership (non-NULL-key rows: empty frames, NULL results; NULL-key rows: ordinary NULL-key peer group with the trailing cutoff) — asserted successful, not errors** (moved out of the error list), `tmrank(v, 5)`/`tmrank(v, 5, 3)` direct internal spellings rejected with a function-named error at the binder case (the grammar's interval-shaped production keeps them unreachable from user SQL; case mirrors `uuid`'s direct-call rejection) |
| `tmrank` public-to-internal binding contract | UT | bind `tmrank(v, INTERVAL 3 DAY) OVER (...)` and assert the published function expression is exactly the internal shape — args `[v, count, unit]` with **no `T_interval` argument anywhere in the published plan** — and that published constants carry **window-normalized values, not DATE_ADD-grammar values** — and that the **D18 discriminator is asserted in the published shape (typed assertions distinguishing raw, normalized-NULL-arithmetic and valid raw forms)**: `INTERVAL '1.5' DAY` publishes the normalized pair (count 1, DAY code; text-truncated), `INTERVAL 1.5 DAY` as a decimal literal publishes the folded MICROSECOND pair (fractional frame-bound semantics), an extra-field compound spelling publishes the **normalized null-marked pair** (int64 count const + int64 unit code — the D15 NULL-arithmetic count, distinct from any raw form), compound spellings publish their folded units (e.g. `'0:01' MINUTE_SECOND` → 1 SECOND); unit-code preservation asserted for MONTH/YEAR/WEEK (calendar codes intact, never folded to microseconds); a direct prepared `INTERVAL ? DAY` publishes the **raw pair** — retained marker source + **unit-string literal** (D18) — and a substituted-literal count keeps the unit string (raw still readable after parameter replacement); a raw EXECUTE-d NULL source errors while the normalized NULL-arithmetic count succeeds (binder-level plan-assert harness, mirroring the existing `prepared_interval_test.go`/`base_binder_case_test.go` rebind cases); re-resolution of the saved internal form through both rebinding paths — prepared re-binding (the `visit_plan_rule.go:3855` route into `bindPreparedFuncExprImplByPlanExpr`) and projection refresh (the `utils.go:7257` route); repeated prepared executions covering a changing `v` value domain (a `PREPARE` bound once, `EXECUTE`-d with values that change the parameter's domain, e.g. text then numeric `ABS(?)` inputs) and changing interval values (`INTERVAL ? DAY`, EXECUTE-d with string values `'1'`, `'1.5'`, `'0:30'` and with numeric values `1`, `1.5`, `3`, `0`, and an overflow candidate) — each execution re-resolves the same internal overload and produces the correct frame-local results with the window grammar's string-vs-numeric semantics |
| SQL-EXECUTE width-domain transfer (D19) | UT + BVT | the source-domain reconstruction owner (`preparedRuntimeSourceExpr` routed to tmrank's flattened count slot during the rebind walk, the `visit_plan_rule.go:3940-3957` precedent) keeps numeric widths numeric despite the TEXT-transport substitution (`visit_plan_rule.go:3917-3933`): `PREPARE s AS SELECT tmrank(v, INTERVAL ? DAY) ...` executed via **SQL EXECUTE** with a text-protocol value `'1.5'` (a string) truncates to 1 day, while execution with the **numeric** value 1.5 (and via **COM_STMT_EXECUTE** with an explicit numeric domain) nearest-integers to **2 days**, asserted against the equivalent uploaded-RANGE frame on the same 30-hour witness (string spelling ranks the later row 1; numeric 1.5 covers both rows and yields rank 2) — typed rebinding assertions show the reconstructed source expression carries the numeric domain (int64/decimal typed literal, not TEXT) after substitution, and repeated executions of the same statement alternate string and numeric values with per-execution correct results, both protocols, with cache-restoration provenance preserved (a re-prepared/re-cached statement's next value is still interpreted in the original unit, per the in-repo comment at :3941-3944) |
| `tmrank` interval semantics, calendar units and boundaries | UT | month-end (JAN 31 + 1 MONTH), leap-year (FEB 29 + 1 YEAR), DST transitions, int64 overflow (`JudgeIntervalNumOverflow`/temporal-range conversion), irregular timestamps where the left edge needs a scan, and **equal-timestamp groups asserting trailing positional membership: equal-t rows after `j` are excluded while the exact-timestamp D=0 window counts the peers at or before `j`**; cases derived from existing RANGE-interval suites (`window_interval_parameters`, `window_negative_interval`, `window_timestamp_range_dst`, `window_temporal_range_bound`) reusing the same bound semantics |
| `tmrank` window-grammar normalization parity (D14) | UT + BVT | **the 30-hour counterexample**: two ascending DATETIME rows 30 hours apart with values 1, 2 — `tmrank(v, INTERVAL '1.5' DAY)` must compute the current row's rank as **1** (string spelling truncates to 1 day, so the earlier row is outside the window), which a DATE_ADD-grammar conversion (1.5 days = 36h) would miscompute as 2; fractional and compound spellings asserted against the equivalent user RANGE frame on the same data — for each bound spelling in {`'1.5' DAY` (text → 1 day), `1.5 DAY` decimal literal (frame-binding microsecond semantics = 36h), executed numeric marker 1.5 (nearest-integer → 2 days), `'0:30' MINUTE_SECOND`, `'1-2' YEAR_MONTH`, `'1.5' SECOND` (microsecond conversion), `INTERVAL ? DAY` executed with string-vs-numeric spellings} a `sum(v) OVER (ORDER BY at RANGE BETWEEN INTERVAL <same spelling> PRECEDING AND CURRENT ROW)` query fixes the expected membership and `tmrank`'s frame matches it exactly; **NULL-arithmetic spellings succeed with key-dependent membership (D15/D17)**: extra-field compounds (`'00:01.5' MINUTE_SECOND`, `'0 00:00:01.5' DAY_SECOND`, `'1/2/3' MINUTE_SECOND`) and the `'abc' DAY` zero rule are asserted **successful** — with **NULL-carrying fixtures** beyond the all-non-NULL `window_interval_parameters.result:22-29`: every NULL-key row keeps its ordinary NULL-key peer group with the trailing cutoff while every non-NULL-key row gets the empty frame (NULL result/sum), asserted with (a) a lone row with `t = NULL`, `v = 7` ranking **1** (the counterexample to the blanket-empty reading), (b) mixed-key tables (NULL and non-NULL timestamps interleaved, asc and desc, `nulls_first`/`nulls_last`), (c) all-NULL-key groups of several rows (trailing-cutoff peer ranks within the group), across date/time/datetime/timestamp keys, and (d) constant versus prepared NULL-arithmetic widths — each asserted against the equivalent user RANGE frames on the same data; **non-literal foldable fractional widths (D20)**: `INTERVAL SIN(1) DAY` (the source stays a FLOAT function expression until the shared int64 CAST rounds `0.8415…` to 1) is asserted to produce the **1-DAY width and the same membership as the equivalent user RANGE frame** carrying the identical bound expression on the same data, alongside foldable **NULL** (`INTERVAL ABS(NULL) DAY`-shaped folds) and **negative** (`INTERVAL 0-SIN(9) DAY`-shaped folds) controls that take the illegal-frame parse error; negative (`-1 DAY`, `'-1' DAY`) and NULL spellings produce the illegal-frame parse error, non-finite numeric markers produce InvalidInput, and overflow spellings produce the shared helpers' InvalidArg/OutOfRange classes — on both surfaces |
| `tmrank` ORDER BY key domain (D21) | UT + BVT | the bind-time key check accepts exactly `T_date`/`T_time`/`T_datetime`/`T_timestamp` keys and rejects everything else with the named tmrank diagnostic — typed-binding cases publish each plan type through the check (a YEAR column, `YEAR(now())`, a cast expression to YEAR, a varchar key, a numeric key, no ORDER BY, and multiple ORDER BY keys are each rejected at bind; the four supported OIDs, asc and desc, nulls first and last, are each accepted) — and public SQL boundary cases mirror them end to end: `CREATE TABLE k(y YEAR, d DATE, ...)`, `INSERT` a row, and `SELECT tmrank(v, INTERVAL 3 DAY) OVER (ORDER BY y)` fails at bind with the diagnostic while `... OVER (ORDER BY d)` executes; the fixture also documents the boundary's rationale (IsTemporal admits T_year but the resolver has no T_year case) with the unsupported-type error retained as the operator's defense-in-depth backstop for any key that slips the bind check |
| `tmrank` width admission boundary (D22) | UT + BVT | following `window_binder_moctl_test.go` (`TestMakeWindowFrameConstValueRejectsMoCtrl`, the #28985 pin): direct `mo_ctl('cn', ...)` and `fault_inject(...)` calls, and calls nested inside arithmetic (`1 + mo_ctl(...)`), in tmrank's width slot are each rejected with the boundary's `NotSupported` message BEFORE any evaluation — proven by the same pattern the repository's test uses (the converting bind runs with a nil proc, so reaching the eager executor would fail the test rather than pass it); non-foldable column-reference widths and indirect prepared forms are refused pre-evaluation with the function-named admission error, likewise never reaching the executor; the public SQL mirror (`SELECT tmrank(v, INTERVAL mo_ctl('cn','Task','...') DAY) OVER (...) FROM t`) is refused for every account including sys-admin and never fires the control action; and the frame-bound surface stays identical (the boundary call replaces `makeWindowFrameConstValue`'s inline gate verbatim, with `window_binder_moctl_test.go` and the existing window BVT group rerun green unchanged) |
| Prepared-width validation ownership (D16/D18) | UT + BVT | the compile-time owner (extended `ValidateIntervalFrameBounds`, `compile.go:923-930` call site) validates tmrank's published width representation **per execution, before sources start, independently of row production and of whether the window operator survives optimization**, running the D18 adapter over each tmrank entry's `(count, unit)` slots — raw unit-string pairs (retained marker or already-substituted literal, both) run the raw pipeline as validate-discard; normalized int64 pairs pass the immutable check: with a `PREPARE`d `tmrank(v, INTERVAL ? DAY)`, (a) EXECUTE with an invalid width (negative; NULL; overflow candidate; non-finite numeric) over an **empty table** fails at execution with the shared classes; (b) the same invalid EXECUTE under `LIMIT 0` (physical operator removed) still fails; (c) repeated **valid** EXECUTEs over the empty table and under `LIMIT 0` succeed (zero rows, no spurious error), including two consecutive identical executions of the same prepared statement; (d) a sentinel case asserts the reusable plan is not mutated by the validate-discard pass (a second identical EXECUTE behaves identically to the first, matching the in-repo contract "Validation borrows the logical plan and discards normalized copies; Prepare still owns the operator's runtime frames"); (e) typed cases distinguish the three published forms — a **raw NULL** failing at validation, a **normalized NULL-arithmetic** pair (null-marked int64 const + int64 code) passing validation and producing the D17 key-dependent runtime behavior, and **valid raw string/numeric widths** passing with their per-domain semantics; constant-width plans (normalized at bind) pass the owner trivially |
| Cancellation during frame scan | UT | deterministic KILL/cancel hook case proving a `cumrank`/large-N `mrank` scan responds within the `checkCanceled` polling cadence instead of finishing a full partition first |
| Cancellation/large-partition behavior (general) | UT | reuse the operator's existing cancellation hook pattern as existing tests do; not a new harness |
| Public SQL contract end-to-end | BVT | new self-contained case `test/distributed/cases/window/window_local_rank.sql` (`+.result`): issue fixture with complete values `NULL,1,2,2,NULL,1`, availability separation (OVER-less probe now shows the 1064 parse-error class like `rank()`, and the OVER form executes), math separation (mrank output compared in-query against the issue's self-join control; a difference query must return zero rows), partitions/ties/empty-partition/type boundaries, `tmrank` irregular times and D=0 equal-timestamp trailing behavior, teardown-clean database |
| Regression of existing window and view surfaces | BVT + UT | run the whole `test/distributed/cases/window/` group and existing `pkg/sql/colexec/window`, `pkg/sql/plan/function` (including `function_id_test.go` allocation checks), `pkg/sql/plan` window-binder and parser UT suites, plus the view-metadata function regressions (ids 603/604 untouched) |
| Red/green primary proof | BVT file | the same `window_local_rank.sql` executed against a base-f8c87200 instrument errors with the original 20105 vs. complete success after the change (recorded terminal exits; live reproduction on the base is captured during the design turn for the probe + control and re-captured as file-based evidence during implementation) |

Static analysis follows the incremental closure rule (gofmt/vet/golangci-lint over the changed
package closure only). `mo-cgo-test` is used for `pkg/sql/colexec/window` tests only if that
package is CGo-transitive in the current build graph (check `go list` deps at implementation
time; operator package has used the plain `go test` path historically).

Success criteria (measurable): every assigned issue entry probe produces the documented
post-fix behavior; the six-row fixture returns `NULL, 1, 2, 2, NULL, 1` from `mrank(v,3)` and
agrees with the in-file self-join control on the same rows; every spelling's `tmrank` frame
matches the equivalent user RANGE frame query on the same data, including the
NULL-arithmetic spellings succeeding with key-dependent membership on fixtures
carrying NULL keys (D15/D17); an invalid prepared width
fails at EXECUTE over empty input and under `LIMIT 0`, valid repeated executions
succeed with an unmutated reusable plan, and numeric widths stay numeric through
SQL-EXECUTE rebinds while string widths stay text (D16/D18/D19); the window BVT group and owning UT
suites are green with no result-file regressions; the same focused BVT fails on the parent base
(non-zero mo-tester exit with 20105 lines in the report) before the implementation is accepted.

## 7. Drawbacks and risks

- Adds three more named evaluators to the operator's `processValueFuncRange`
  switch; the switch is the established pattern, but every new name grows that function (a
  follow-up refactor to split per-function evaluators is recorded in D6 — out of scope here to
  avoid an unrelated refactor around a behavior-change PR).
- `cumrank` inherits the operator's known quadratic running-frame envelope (mitigation: none
  needed for correctness; profiled optimization is deferred behind real-workload evidence, D1).
- Vast ORDER BY flexibility (multiple keys, DESC, NULLS FIRST/LAST) is accepted for `mrank`/
  `cumrank` and affects only frame order, not counting; `tmrank` restricts ORDER BY to one
  ascending/descending DATE/TIME/DATETIME/TIMESTAMP key mirroring RANGE-N rules (D21: the
  shared resolver's supported domain; YEAR and every other type are deliberately rejected at
  binding time — widening the domain waits for a resolver `T_year` case).
- Local ranks can differ between adjacent rows by more than 1 (frame content changes), which
  reads differently from partition-wide `RANK`; the SQL function name family plus the doc is the
  only guard against user confusion, hence the explicit-frame rejection.
- Registering the three names as **non-reserved** window tokens (the
  `LAG`/`LEAD`/`FIRST_VALUE` pattern at `mysql_sql.y:16937-16941`) preserves existing
  table/column/alias identifiers by those names; the residual grammar risk is a goyacc
  conflict at regeneration, which the repository's zero-conflict gate turns into a
  build-time failure rather than a silent ambiguity (detected and reworked at
  implementation time).
- The function-ID allocation depends on the tail of `functionIdRegister` at merge time
  (`mo_view_definition`/`mo_view_check_option` already occupy 603/604 at the assigned base, and
  main may allocate more IDs before delivery): the implementation re-checks the first unused
  slot and documents the final numbers in `predefinedFunids`; the sentinel bump keeps the
  allocation protocol mechanical.
- The frame-bound normalization matrix is branch-dense (string literal vs decimal/float
  literal vs const-fold vs compound vs prepared-marker), and the outcomes differ per
  spelling (text `'1.5' DAY` → 1 day vs numeric `1.5` → 2 days vs decimal literal → 36h
  microseconds vs const-fold → the shared int64 CAST's post-round integer, so foldable
  fractional sources such as `SIN(1)` round through `floatToInteger` rather than reject
  (D20); extra compound fields → successful NULL arithmetic whose membership is
  key-dependent — non-NULL-key rows get empty frames, NULL-key rows keep their peer
  group — while NULL sources → errors: D15 keeps the three outcome classes distinct and
  D17 keeps the NULL-arithmetic membership aligned with the shared resolver's NULL-key
  branch); the parity,
  binding and ownership rows of §6 pin each branch and each outcome class so a future
  matrix change that misses tmrank fails the comparison rather than silently diverging.
- tmrank intentionally couples to six shared mechanisms (accepted trade-off,
  D13/D14/D15/D16/D17/D18/D19, with D20 keeping the const-fold branch on the same
  matrix and D22 adding the admission boundary):
  the binder's name-keyed conversion switch (`date_add`/`uuid` — any future refactor
  of that switch must preserve the tmrank case's internal-form passthrough exactly as it
  preserves `date_add`'s, or prepared rebinding silently breaks); the **window
  frame-bound interval normalization matrix**
  (`resetWindowIntervalExpr`/`setWindowIntervalValue`/`NormalizeWindowInterval` and the
  raw-pair materialization in `materializeWindowInterval` — tmrank's interval semantics
  stay identical to user RANGE frames by construction, including the successful
  NULL-arithmetic outcome and its key-dependent membership, and a refactor of the
  frame-bound normalization must keep it reachable from the function-argument conversion
  or the parity silently drifts); the **raw-vs-normalized representation discriminator
  and adapter** (the unit slot's literal kind that `materializeWindowInterval` itself
  reads, plus the shared-materialization adapter the validation owner and operator
  evaluation both use — a refactor must keep raw string-unit pairs readable or
  substituted raw sources would be misread as normalized, D18); the
  **statement-constant width-validation owner**
  (`ValidateIntervalFrameBounds` and its `compile.go:923-930` call site — a refactor
  must keep the WinSpecList `window_func` walk or empty-input/LIMIT-0 prepared widths
  go unchecked (D16)); the **source-domain reconstruction owner**
  (`preparedRuntimeSourceExpr` and its `visit_plan_rule.go:3940-3957` precedent — the
  rebind walk must keep the tmrank count-slot case or TEXT-transport substitution
  silently changes numeric widths' semantics (D19)); the **pre-evaluation admission
  boundary** (`HasMoCtrl`'s #28985 gate plus the static foldability admission — a
  refactor must keep both checks ahead of every eager evaluation of a
  window-interval source, or a control function hidden in a width fires its
  cluster-wide side effect at bind time before the frontend gate runs, and a
  non-foldable source reaches the eager executor; the boundary call sites are the
  tmrank case, `makeWindowFrameConstValue`'s replaced inline gate and the
  `resetWindowIntervalExpr` entry backstop (D22)); and the execution-side civil-time
  rules (`JudgeIntervalNumOverflow`,
  `timestampRangeSelection`/`buildIntervalRows` helpers), which keep calendar, DST and
  overflow semantics single-sourced so future fixes propagate automatically.
- The D22 boundary replaces the frame-bound inline gate in place (not beside it), so
  its message and class are shared by both routes; `window_binder_moctl_test.go` and
  the existing window BVT group pin the frame-bound surface, and a future message edit
  must update the shared owner so the safety test fails loudly rather than the routes
  drifting apart.

## 8. Decision log

- D1 (accepted): first revision computes each row's frame by direct positional scan
  (`O(min(N,j-partStart))` per row; `cumrank` is `O(n²)` worst). "Better than brute force"
  incremental structures are deferred behind profile evidence; the issue asserts no performance
  claim on these tests, and the window operator already tolerates quadratic running frames.
- D2 (accepted): only competition ascending rank ships. Dense/average/percentile and descending
  variants need their own conventions; a future overload (`mrank(v, N, rank_type)` or a direction
  argument) extends this framework without breaking the fixed-shape arity chosen here
  (see D5).
- D3 (open, non-blocking): collation-aware string ranking; owner: implementation PR; decision
  point: first user request or the collation contract workstream. Numeric/temporal covers the
  issue's declared profile now.
- D4 (accepted): reject explicit OVER frames for these operators rather than silently ignoring
  them (unlike LAG/LEAD); deterministic, prevents the two-window-source ambiguity, and is a
  bind-time parse error rather than a semantic surprise.
- D5 (accepted): windows are trailing only; the issue's profile is "most recent period" and a
  leading-side variant is unspecified in the issue.
- D6 (non-blocking maintenance note): extracting per-window-function evaluator functions out of
  `processValueFuncRange` belongs to a mechanical follow-up refactor PR, not this one.
- D7 (accepted): the probe `SELECT mrank(...)` (no OVER) intentionally remains an error,
  switching from 20105 to the same 1064 parse-error class `rank()` produces (the grammar's
  mandatory window_spec, verified live on the base). The issue states the
  probe checks entry availability only, and every SQL window keyword in matrixone requires
  OVER at the grammar level; consistency with `RANK` dominates inventing a scalar fallback.
- D8 (finding 1; accepted): function IDs are the first genuinely unused numbers — 616, 617,
  618 with `FUNCTION_END_NUMBER` raised to 619 — because 603/604 already belong to
  `mo_view_definition`/`mo_view_check_option` (base `function_id.go:797-799`) and those IDs
  participate in `ViewMetadataFunctions` protocol detection
  (`pkg/pb/plan/string_literal_form.go:995`). Never renumber existing functions; the
  validation ladder adds registry-uniqueness and view-function regression checks. Merge
  application (2026-10-24): the draft had taken 605-607/608; by PR #31020's conflict-resolution
  merge, main's #31017 had already claimed 605-615 with the sentinel at 616, so applying this
  decision to the merged base moves the never-on-main rank IDs to 616-619 exactly as recorded
  in §9 — the invariant (first genuinely unused numbers, existing functions untouched) is
  unchanged, only its realized numbers moved.
- D9 (finding 2; superseded in shape by D13): revision 2 specified `tmrank`'s interval
  as a **frame-bound representation** — a binding-synthesized internal `RANGE` frame with
  `[v]`-only executed arguments. The round-3 review showed that carrier cannot bind: the
  generic function path's publish-time postcondition rejects interval arguments
  (`base_binder.go:8214`) before any later lowering could run, and the lowered
  one-argument form has no resolvable rebinding signature on the saved-function
  re-resolution paths. Retained from D9: calendar units are never folded into
  microseconds, `D = 0` is valid (exact-timestamp trailing window), and overflow is
  re-checked through the shared interval rules. The carrier itself is replaced by D13.
- D10 (finding 3; accepted): cancellation is polled inside every frame scan with the
  shared `checkCanceled` cadence (1024-iteration batches), mirroring the aggregate
  frame-scan loops (`window.go:649-671`); a deterministic cancellation-during-scan test is
  part of the ladder. The complexity model states `tmrank` total CPU as
  `O(sum_j |F(j)|)` (quadratic when `D` spans the partition prefix) — the earlier
  `O(n log n)` claim counted only the left-edge search — and the memory model accounts for
  the resolver's bounded per-row selection/civil-index scratch instead of claiming
  unconditional `O(1)`.
- D11 (finding 4; accepted): the three names are registered as **non-reserved** window
  tokens following the existing LAG/LEAD pattern, so identifiers by those names keep
  parsing and the feature stays purely additive; parser tests assert identifier
  compatibility alongside the new productions.
- D12 (finding 5; accepted): the no-ORDER-BY oracle asserts against each supplied physical
  order (trailing frames are order-dependent; the permutation-invariance claim is
  withdrawn — mrank(v,2) yields 1,2,2 over rows 1,2,3 but 1,1,1 over 3,2,1), zero-duration
  `D` is valid per the `D ≥ 0` contract, and equal-timestamp cases pin trailing positional
  membership (later peers excluded).
- D13 (round-3 finding; accepted; its normalization clause is refined by D14; its
  conversion now opens with the D22 admission boundary):
  `tmrank`'s public-to-internal binding follows the
  repository's name-keyed in-binding conversion precedent. The `"tmrank"` case in
  `bindFuncExprImplByPlanExpr` (the `date_add`:7594 / `uuid`:7634 site) consumes the
  `T_interval` argument **before overload resolution** and before the publish-time
  no-escape postcondition (`rejectIntervalArgs`, `base_binder.go:8214`), publishing the
  internal signature `TMRANK(v, count, unit)` whose count slot carries the
  window-normalized constant or the retained direct-prepared source and whose unit slot
  carries the D18 discriminator (normalized int64 code or raw unit-string) (D14/D18).
  The catalog
  (`list_window.go`) registers exactly that internal shape (`date_add`'s
  `list_builtIn.go` registration pattern: only internal overloads, the SQL syntax
  converts during binding). The saved
  internal form passes through the case on re-entry via `allowInternalFunctionArgs`
  (`base_binder.go:7620`, entered with `true` from `bindPreparedFuncExprImplByPlanExpr`,
  `base_binder.go:7337`), so prepared re-binding (`visit_plan_rule.go:3855`) and
  projection refresh (`utils.go:7257`) re-resolve it with refreshed argument types —
  changing value domains (`ABS(?)`) and changing interval counts stay first-class. Direct
  internal spellings are rejected at the case (`uuid` direct-call pattern); no frame is
  synthesized on the window spec; the operator derives each row's frame at evaluation
  (§3.4). Validation adds the typed binding/lowering assertions and the repeated
  prepared-execution cases of §6.
- D14 (round-4 finding; accepted; error-class clause refined by D15, const-fold
  clause refined by D20): the conversion's
  **value interpretation uses the
  window frame-bound interval grammar, not the general DATE_ADD grammar**. The round-4
  review verified that revision 3's `resetIntervalFunctionArgs`-shaped wording routed
  string sources through `bindStringIntervalExpr` → `NormalizeIntervalWithOverflow`
  (`base_binder.go:12544/:12556`, the DATE_ADD grammar), while user RANGE frames
  normalize through `NormalizeWindowInterval` (`window_binder.go`, the grammar
  `types/window_interval.go:25-28` documents as deliberately separate). The divergence is
  observable: `INTERVAL '1.5' DAY` is 1.5 days under the DATE_ADD grammar but 1 day
  under the window text grammar (`window_interval_test.go`, `{"1.5", Day, text} → 1`),
  so two ascending DATETIME rows 30 hours apart with values 1, 2 would get current rank
  2 under the proposed conversion but rank 1 under declared RANGE semantics — and the
  resulting int64 count/unit cannot be re-checked to recover the source spelling.
  Revision 4 therefore normalizes through the frame-bound matrix
  (`resetWindowIntervalExpr`/`setWindowIntervalValue`): **source type, spelling and sign
  are preserved until normalization** — string literals take the window text grammar,
  decimal/float time-unit literals take the frame-bound microsecond folding, other
  foldable expressions take the const-fold branch, negative/NULL take
  `newWindowFrameIllegalError`, compound spellings take the window compound grammar,
  decimal provenance carries `DecimalLiteralRequiresV82` — constants are published as
  window-normalized int64 pairs, and the direct prepared magnitude is **retained**
  (`bindPreparedWindowFrameBound`'s marker-retention model, with the in-repo rationale
  "a temporal cast here would erase the offset's domain") and materialized per
  execution through the raw-pair rules (`materializeWindowInterval`:
  `windowIntervalScalarText` domain dispatch — numeric domains use MySQL's
  nearest-integer conversion — then `NormalizeWindowInterval` and
  `JudgeIntervalNumOverflow`). Indirect prepared forms and non-foldable magnitudes are
  rejected as frame bounds reject them. No implicit cast is inserted on the count slot.
  Validation adds the 30-hour counterexample and the fractional/compound
  spelling-comparison rows of §6.
- D15 (round-5 finding 1; accepted; its NULL-arithmetic membership clause is refined
  by D17): the interval contract's **three outcomes are
  explicitly separated**, because revision 4 conjoined NULL sources, NULL arithmetic
  and normalization errors into illegal-frame rejection, while the shared helpers
  distinguish them: `NormalizeWindowInterval` returns `null=true` **without error** for
  extra compound fields and compound split errors
  (`pkg/container/types/window_interval.go:37-46`), the binder preserves that NULL as a
  null-marked constant (`window_binder.go:1379-1385`), and the equivalent user RANGE
  query runs successfully producing NULL sums (extra-field compound frame queries in
  `test/distributed/cases/window/window_interval_parameters.result:22-29`). Revision 5
  therefore fixes the contract: (1) a **NULL source value** (constant NULL literal or
  EXECUTE-d NULL marker) is an **error** in the illegal-frame parse class;
  (2) a **successful normalization to NULL arithmetic** (extra compound fields etc.) is
  **not an error**: the count publishes null-marked and the frame membership is the
  shared resolver's key-dependent NULL arithmetic (D17's refinement: non-NULL-key
  rows get `F(j) = ∅` and `result(j) = NULL`; NULL-key rows keep their ordinary
  peer group with the trailing cutoff) — the `§2.1` invariant gains this documented
  exception and `§2.2`'s parity claim is reconciled (the `§6` parity row moved these
  spellings from the invalid-input list to successful cases);
  (3) a **normalization error** takes the shared helpers' own class — negative →
  parse error (`windowIntervalNegativeError`), non-finite numeric → InvalidInput
  (`window_interval.go:31-32`), digit/magnitude overflow → OutOfRange
  ("int64", "window interval", `window_interval.go:130-149`) or InvalidArg
  ("interval", num) via `JudgeIntervalNumOverflow`
  (`pkg/container/types/interval.go:451-460`), temporal-range conversion → OutOfRange
  (`colexec/window/interval.go:124-127`) — replacing the blanket illegal-frame
  expectation; invalid string prefixes still succeed as `D = 0`. The narrower tmrank
  domain already restricted `D` to constants and direct-prepared sources (D13/D14);
  D15 documents the outcome matrix as the complete `D` domain contract and adds the
  constant/prepared cases for extra compound fields, non-finite numeric values and
  overflow to `§6`.
- D16 (round-5 finding 2; accepted; its check adapts the published representation
  per D18): **a shared compile-time validation owner now
  guarantees tmrank's statement-constant width contract**. Revision 4 placed all width
  validation in per-output-row evaluation, which fails when no rows are evaluated:
  `Window.Prepare` materializes only frames (`window.go:85-99`), and the round-5 review
  verified the repository's own precedent — `ValidateIntervalFrameBounds`
  (`pkg/sql/colexec/window/interval.go:34-63`, invoked at
  `pkg/sql/compile/compile.go:923-930` with the in-repo comment that logical window
  arguments "remain execution-time errors when LIMIT 0 removes their physical
  operator") validates frame-bound `T_interval` expressions per execution attempt,
  before sources start, via a **validate-discard** pass whose doc comment fixes the
  immutability contract ("Validation borrows the logical plan and discards normalized
  copies; Prepare still owns the operator's runtime frames"). Revision 5 extends that
  same owner to walk each `node.WinSpecList` entry's `window_func` expression (the
  WindowSpec message already carries `window_func` beside `frame`,
  `proto/plan.proto:771-777`) and, for every tmrank entry, run the D18 adapter over
  the published `(count, unit)` slots as a
  validate-discard check — invalid prepared widths fail at EXECUTE even on empty input
  or under LIMIT 0, valid executions pass unchanged, and the reusable plan is never
  mutated. The consumer joins the change map (§6) and the ladder adds invalid-and-valid
  repeated executions over empty input and LIMIT 0.
- D17 (round-6 finding 1; accepted): **NULL-arithmetic membership is key-dependent, not
  blanket**. Revision 5 prescribed empty frames and NULL results for *every* row when
  normalization succeeds with `null=true`, but the shared resolver computes
  key-dependent membership: `buildRangeIntervalWithLocation`
  (`window.go:2258-2265`, comment verbatim: "NULL current keys still form their ordinary
  peer group. Only a non-NULL temporal key has NULL interval arithmetic and an empty
  frame.") returns the empty span only for non-NULL keys and lets NULL-key rows fall
  through to the ordinary peer-group machinery, asserted by
  `colexec/window/interval_test.go:117-158` (NULL key retains the row, non-NULL key
  empties the frame); the cited parity fixture
  (`window_interval_parameters.result:22-29`) carries only non-NULL keys and so cannot
  ground the broader claim. Revision 6 reconciles the invariant (§2.1's exception),
  execution rules (§3.4) and parity rows to that membership: a lone row with `t =
  NULL`, `v = 7` ranks 1 under the null-arithmetic window; the operator routes the
  null-marked state through the shared resolver's NULL-interval branch with the
  trailing positional cutoff instead of short-circuiting; the ladder adds constant and
  prepared fixtures with NULL keys, mixed non-NULL/NULL keys (asc/desc,
  nulls_first/nulls_last) and all-NULL key groups across date/time/datetime/timestamp.
- D18 (round-6 finding 2; accepted): **the published width representation is
  self-describing and durable**. Revision 5 published both normalized constants and
  raw prepared sources as `[v, count, int64 unit]`, and passed the internal form
  through unchanged at rebind, but the shared helper discriminates raw from normalized
  by the unit slot's literal kind (`pkg/sql/colexec/window/interval.go:80-96`: an
  int64-coded unit marks an immutable normalized bound — its branch rejects
  non-int64/non-literal sources and bypasses normalization; a MySQL-string unit marks a
  raw bound that runs the full pipeline), generic parameter replacement can turn a raw
  marker into a literal (`visit_plan_rule.go:3905-3939`), and a raw NULL must stay
  distinguishable from a valid normalized NULL-arithmetic count. Revision 6 adopts the
  shared helper's own discriminator for the function-argument carrier: normalized
  constants publish `(int64 count const, int64 unit-code)` and retained (or already
  substituted) raw sources publish `(source, unit-string literal)` — no `T_interval`
  anywhere, rawness readable after substitution, projection refresh and cache
  restoration — and defines one adapter to shared materialization used by BOTH the D16
  validate-discard owner and operator evaluation (raw pair → raw pipeline; normalized
  pair → immutable check; invalid raw NULL errors while the normalized null-marked
  pair succeeds). The catalog's unit slot accepts exactly these two shapes; the ladder
  adds typed assertions distinguishing raw NULL, normalized NULL-arithmetic and valid
  raw string/numeric widths.
- D19 (round-6 finding 3; accepted): **SQL-EXECUTE source-domain transfer is closed
  with the reconstruction owner frame bounds use**. Revision 5 required numeric
  prepared magnitudes to use nearest-integer conversion and string magnitudes text
  truncation, but the rebind walk's generic parameter replacement intentionally
  substitutes most markers as TEXT transport
  (`visit_plan_rule.go:3917-3933` — "Most prepared parameters are intentionally
  replaced as TEXT to retain the historical SQL-EXECUTE behavior"), and the owner that
  reconstructs each raw interval's source domain — `preparedRuntimeSourceExpr` in the
  `T_interval`-list branch (`visit_plan_rule.go:3940-3957`, comment: "Specialize its
  source domain without normalizing its value: cached plan restoration must continue
  to interpret the next value in the original unit") — sees only list-shaped intervals,
  which tmrank's flattened count slot bypasses: numeric `1.5 DAY` would arrive as text
  `'1.5'` and produce 1 day instead of 2, the wrong rank on the 30-hour witness.
  Revision 6 names and integrates that owner for tmrank: a new case in the prepared
  parameter walk applies `preparedRuntimeSourceExpr` to tmrank's count-slot parameter
  position (preserving cache-restoration provenance), the consumer joins the change
  map (§6), and the ladder plans SQL-EXECUTE and COM_STMT_EXECUTE checks for numeric
  1.5 versus string '1.5' with typed rebinding assertions and repeated executions.
- D20 (round-7 finding 1; accepted; refines D14's const-fold clause): **foldable
  fractional widths follow the shared CAST, not rejection**. Revision 6's const-fold
  wording ("NULL or non-integral results are illegal") prescribed rejecting
  non-integral constant-folded widths while promising identical RANGE behavior, but
  the shared path resolves fractionality *before* evaluation: the const-fold branch
  wraps the source in the same int64 CAST it applies (`window_binder.go:1453-1489`
  via `appendCastBeforeExpr` to `T_int64`) and evaluates the cast on an
  `EmptyForConstFoldBatch`; FLOAT-to-integer conversion rounds
  (`floatToInteger`, `func_cast.go:3778-3800`: `T2(math.Round(float64(v)))`),
  so a code-derived witness — `INTERVAL SIN(1) DAY`, where `SIN(1)` remains a FLOAT
  function expression — takes the shared path to a **1-DAY** width, and rejecting it
  would break RANGE parity. Revision 7 distinguishes the fractional *source value*
  from the post-cast integer *result*: foldable non-literal sources are cast first
  (rounding per the shared CAST), only a **NULL** fold result takes the
  illegal-frame parse error (`window_binder.go:1480-1481`), and no non-integral
  post-cast result exists to reject. §3.1's outcome matrix, §3.3 item 4's recap, the
  invalid-inputs row and the parity row are reconciled, and the ladder adds typed
  and SQL parity cases for `INTERVAL SIN(1) DAY` (alongside NULL and negative
  foldable controls).
- D21 (round-7 finding 2; accepted): **tmrank's ORDER BY keys are restricted to the
  resolver's supported domain**. Revision 6 admitted "exactly one ORDER BY expression
  of temporal type", but that predicate admits YEAR: `IsTemporal()` includes
  `T_year` (`pkg/container/types/types.go:750-757`) and YEAR columns are published
  with that Oid (`pkg/sql/plan/build_util.go:384-385`), while the shared RANGE
  resolver has no `T_year` case and falls to the unsupported-type internal error
  (the RANGE-search switch's `default` branch, "unsupported type %v for RANGE frame
  in window function") — a non-NULL YEAR key would bind but never execute. Revision
  7 lowers the accepted domain to the resolver's actual temporal key set — exactly
  `T_date`, `T_time`, `T_datetime`, `T_timestamp` — checked at binding time with a
  named tmrank diagnostic (parse-error class) for YEAR and every other type; adding
  a YEAR path to the shared resolver is deliberately out of scope for this feature.
  §2.2, §3.3 item 4, the invalid-inputs row and a new key-domain ladder row
  (typed-binding and public SQL boundary cases, including a YEAR column fixture
  rejected at bind while a DATE column executes) carry the restriction.
- D22 (round-8 finding; accepted): **every eager evaluation of a window-interval
  bind source is preceded by the shared admission boundary**. The review found
  that revision 7 routed tmrank's conversion through the named normalization
  helpers and leaned on generic privilege scanning, but the pre-evaluation
  control-function prohibition lives outside those helpers:
  makeWindowFrameConstValue runs HasMoCtrl before resetWindowIntervalExpr
  (window_binder.go:1298-1308, the #28985 gate whose comment records that a
  bound is evaluated HERE at bind time, before the frontend's mo_ctl sys-admin
  gate scans the built plan, with the evaluated bound then replaced by a
  constant the scan can never see), while the normalizer itself evaluates
  sources directly (window_binder.go:1469-1474) with no internal check, and the
  recursive HasMoCtrl walk (utils.go:3845, including its window-entry
  recursion) is inherited only by callers that run it first — so tmrank as
  specified would have eagerly evaluated a mo_ctl-bearing width, firing its
  cluster-wide side effect unauthenticated, and the promised non-foldable
  rejection had no pre-evaluation owner. Revision 8 makes the boundary one
  named shared owner with two tiers: (i) the control-function prohibition —
  mo_ctl/fault_inject, direct or nested, refused with the gate's exact
  NotSupported message — called by the tmrank case before conversion, by
  makeWindowFrameConstValue in place of its inline gate (identical behavior,
  window_binder_moctl_test.go stays valid), and again at
  resetWindowIntervalExpr's entry as defense-in-depth so the normalizer is
  safe by construction (its sole production caller today is the frame-bound
  gate at window_binder.go:1308; the projection_binder.go:185-187 wrapper has
  no production caller); (ii) the static admission walk — literal constants,
  the direct prepared magnitude marker and constant-foldable spines admitted;
  column-reference (non-foldable) magnitudes and indirect prepared forms
  refused pre-evaluation with the case's function-named error.
  Execution-time raw-pair materializations (the D16 owner and the operator
  path) never re-admit: after tier 1 walked the whole source at bind, no
  published source expression can contain a call, and EXECUTE-d marker values
  are data, not calls. The ladder adds the direct and nested mo_ctl/
  fault_inject rejection cases of section 6 (following
  window_binder_moctl_test.go, proven without evaluation by the nil-proc
  pattern), the non-foldable and indirect-prepared admission rejections, and
  the public SQL mirror refused for every account including sys-admin.

## 9. Review record

The original design reviews, merge notes and implementation-review history are
preserved in the [record at the original merged revision](https://github.com/matrixorigin/matrixone/blob/df8bfd602ecc036424ff7897187565657366919d/docs/design/20261023-local-sliding-window-ranks.md#9-review-record).
