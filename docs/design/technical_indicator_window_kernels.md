# Technical-indicator window functions and rolling risk analytics

- Status: gated — awaiting independent design approval before implementation;
  revision r7 incorporates the design-review findings recorded against r6
- Tracking issue: [matrixorigin/matrixone#29949](https://github.com/matrixorigin/matrixone/issues/29949)
- Owner: gouhongshen (issue assignee); investigation and design by the issue-29949 handler
- Base commit: `8111f870f5d8cee40c53fc48bcbcde8292da1fa5` (= current `origin/main` at worktree creation)
- Design revision: `technical-indicator-window-kernels-2026-10-24-r7`
- Superseded revisions: r1 (SHA256
  `89a4cb8a68b78f1c95693440788193a836c227ad8605b4502d8fe32e6902b78c`,
  REQUEST_CHANGES, four findings); r2 (SHA256
  `494ac76dfaecb1fad125b11a512a8fa44f4d421f24d7dea6902054961671830b`,
  REQUEST_CHANGES, two findings); r3 (SHA256
  `076f78bf5766acfc76b6a75b826baff6d5108ad6774157e580b49a128be8896b`,
  REQUEST_CHANGES, two findings); r4 (SHA256
  `52ce450c7813ba9e6daa96a2fdaa66b7f70b931358b75faffe091f3b49bd5865`,
  REQUEST_CHANGES, two findings); r5 (SHA256
  `ddf641cc73ea84f63c12ec26cfda364beb992ca5b23af2ccef70758b1`,
  REQUEST_CHANGES, two findings); r6 (SHA256
  `828a286502b4d7fa0adc9b98c4d51dd07abf56eebe33ec610995ee0f11343636`,
  REQUEST_CHANGES, one finding). Section 11 logs every finding and its
  disposition.
- Implementation PR: to be linked after design approval
- Last updated: 2026-10-24

## 1. Decision

MatrixOne gains a native family of SQL window functions for the
technical-indicator and rolling-risk operations requested in issue #29949:
the moving-average family `ema`, `wilder`, `kama`, `dema`, `tema`, `t3`,
`wma`, `sma`, `trima`, the bounded rolling drawdown operator `mmaxDrawdown`,
and the cumulative positive-streak operator `cumPositiveStreak`.

Design gates (mo-dev G-FEATURE-DESIGN) are triggered and this document is the
required versioned design artifact. Summary of the classification:

- **Trigger:** new user-visible SQL capability (public function surface);
  crosses the planner/binder and execution-engine boundary; expected to exceed
  500 non-generated production lines across at least three owning packages
  (`pkg/sql/plan/function`, `pkg/sql/plan`, `pkg/sql/colexec/window`).
- **Root cause of the requested behavior gap:** MatrixOne's function registry
  has no entries for any of these names, and the recursive members of the
  family (EMA-ratio recurrences, adaptive KAMA, Wilder smoothing, composed
  EMA chains DEMA/TEMA/T3, triangular convolution TRIMA) cannot be expressed
  by composing the existing window functions, because every existing window
  function derives row `i`'s result from frame *input* rows only, never from
  the *output* of row `i-1`. The two risk operators asked for in bounded form
  are likewise not window-composable: sliding-frame maximum drawdown needs a
  peak recomputed per outer-frame position (the frame anchor of an inner
  `MAX ... ROWS n-1 PRECEDING` is the inner row itself, not the outer frame
  start, so any single nesting overestimates peaks and understates drawdown),
  and a consecutive-run streak counter is only reachable through a
  four-level nested-window composition (sign, run-start via `lag`, running
  run-id, `row_number` per run) that no migration guide should ask users to
  port as the canonical expression of one DolphinDB/TA-Lib-style name.

The kernels are implemented as per-partition sequential state on the existing
window operator (`pkg/sql/colexec/window`), registered through the ordinary
builtin window-function path (`list_window.go`), with strict, documented
seeding/warmup/NULL profiles per operator. Operators that are already
expressible with ordinary SQL (cumulative maximum drawdown, percentage-change
"local trend", `avg`-over-`ROWS` simple moving average, bounded streak cap)
are delivered as documented and BVT-validated SQL migration recipes rather
than duplicate native entries, per the issue's own scoping
("documented SQL/migration equivalents"; "Validate named/API availability
separately from mathematical rewrites").

No claim of byte-for-byte TA-Lib or DolphinDB parity is made. Each kernel's
profile is defined exactly in section 3 and becomes the testable contract.

## 2. Problem, evidence, and root cause

### 2.1 Reported problem

Issue #29949 reports that financial/risk and telemetry workflows need
smoothed-indicator, drawdown, streak, and local-trend operators, and that
porting these formulas repeatedly across libraries creates validation cost.
The report requests documented, well-defined SQL profiles with explicit
warmup and NULL semantics for the missing
`EMA/DEMA/TEMA/T3/KAMA/Wilder/WMA/SMA/TRIMA` family plus rolling
drawdown/streak/trend operators, with the verified fixture `100,120,90,110`
whose cumulative maximum-drawdown ratios are `0,0,0.25,0.25`, and the
requirement that a bounded-frame drawdown use frame-local peaks rather than
the all-history peak.

### 2.2 Base evidence (this handler's investigation)

All evidence below was collected from the base binary built from
`8111f870f5d8cee40c53fc48bcbcde8292da1a5` and a fresh isolated single-node
MatrixOne instance (launch notes in the handler record). Probe files:
`/tmp/mo-issue29949/probes-entry.sh`, `/tmp/mo-issue29949/probes-expressibility.sh`.

**Entry availability probes** (each exact statement from the issue, executed
against the base binary):

| probe | observed on base |
|---|---|
| `SELECT mmaxDrawdown(CAST(1 AS DOUBLE),3) result;` | rejected — `not supported: function or operator 'mmaxdrawdown'` (error 20105) |
| `SELECT ema(CAST(1 AS DOUBLE),3) result;` | rejected — same class, `'ema'` |
| `SELECT kama(CAST(1 AS DOUBLE),3) result;` | rejected — same class, `'kama'` |
| `SELECT cumPositiveStreak(CAST(1 AS DOUBLE)) result;` | rejected — same class, `'cumpositivestreak'` |

(Observed exact outputs are preserved in the handler record; the failures are
name-resolution failures, consistent with the registry inventory: no
`sma`/`ema`/`dema`/`tema`/`t3`/`kama`/`wilder`/`wma`/`trima`/`mmaxdrawdown`/
`cumpositivestreak` names exist in `pkg/sql/plan/function/function_id.go`.)

**Working SQL control** (issue's cumulative-drawdown fixture): returns the
documented `0, 0, 0.25, 0.25` on the base binary, confirming ordinary
window composition already covers the *cumulative* drawdown profile.

**Expressibility probes** (documentation recipes validated on base):
the cumulative-drawdown control, `avg` over a bounded `ROWS` frame (SMA),
and a `lag`-based percentage-change local trend all execute with exact
values; a `WITH RECURSIVE` CTE carrying EMA state through its recursive
member (`SELECT ... FROM s JOIN e ON s.id = e.id + 1`) reproduces the
`alpha = 0.5` recurrence exactly (`100, 110, 100, 105, 92.5, 111.25,
118.125`), proving the family is *expressible but only as iteration-at-a-time
or quadratic forms*; a four-level nested-window streak composition
(sign, run-start via `lag`, running run-id, `row_number` per run) returns
the expected run length (all-positive fixture: `1..7`). A single-level
`MAX(...) OVER (ROWS n-1 PRECEDING)` bounded-frame composition executes but
computes a different profile (its inner frame anchors at the inner row, so
the emitted `frame_peak` at row 5 of the probe is the value 110 after the
120-peak rotated out) — this is the live counterexample for why the
bounded-frame drawdown cannot reuse the all-history/inner-anchored peaks
without frame-local recomputation. The results are recorded in the
handler reproduction log.

### 2.3 Root cause

1. **Registry absence.** `pkg/sql/plan/function/list_window.go` (window
   functions) and the aggregate framework expose
   `rank/row_number/dense_rank/percent_rank/ntile/cume_dist/lag/lead/first_value/last_value/nth_value`
   plus frame aggregates (`avg`, `sum`, `min`, `max`, stddev, ...). None of
   the requested names exist (`function_id.go` name map has no such entries;
   verified by search), so every probed call fails name resolution with
   error 20105 before any plan is built.
2. **Structural inexpressibility of the recursive family in ordinary
   windows.** The window operator computes each output row from *input*
   rows in a frame. An EMA-ratio recurrence `y_i = a*x_i + (1-a)*y_{i-1}`
   needs previous *output*. `ROW_NUMBER`-style helpers cannot synthesize
   cross-row output dependency without recursive CTEs (quadratic or
   iteration-at-a-time) or self-join expansions with row-dependent power
   products, all of which are either not vectorizable, O(p^2), or carry the
   restricted recursive-CTE execution shape. The same applies to Wilder,
   KAMA, the composed chains DEMA/TEMA/T3, and the bounded sliding-frame
   drawdown whose inner "prefix peak" is anchored at the outer frame start,
   a moving anchor no fixed window frame provides.
3. **No owner exists for per-partition sequential state.** The current
   window operator special-cases three shapes: value functions
   (`processValueFuncRange`), ranking functions (`processOrderFuncRange`),
   and frame aggregates (`processAggregateFuncRange`). A fourth shape,
   partition-materialized recursive sequences with strict warmup, does not
   exist; this design adds it as one coherent mechanism for all eleven
   kernels rather than eleven ad-hoc paths.

## 3. Scope and operator profiles

### 3.1 Native window-function kernels

All names are case-insensitive identifiers, matching the issue's probed
spelling (`mmaxDrawdown`, `cumPositiveStreak`, `ema`, `kama`). Period
parameters are required to be integer-foldable constants with value `n >= 1`
at bind time; `1 <= n <= 1024` is validated uniformly for every kernel in
this family (review-finding #4 disposition: the previous `n <= 100000` cap
admitted `O(p*n)` drawdown scans of up to 10^10 element visits for a
200000-row partition, which is an unjustifiable unbounded CPU budget; the
tightened cap covers the indicator working set — TA-Lib periods, daily and
intraday lookbacks, annual trading windows (252) — while keeping the
documented worst-case scan linear and small; see section 8). The value
argument `x` accepts all numeric types
(integers, unsigned, floats, decimals); arithmetic is performed in IEEE-754
`float64` and the indicator family returns `DOUBLE` (rationale: the family
is defined in float domain across TA-Lib/DolphinDB precedent, and smoothed
ratios lose exact-decimal semantics anyway; decimal inputs are converted at
evaluation). `cumPositiveStreak` returns non-nullable `BIGINT`.

Conventions below are the *family defaults*, followed by every kernel except
where a profile explicitly overrides them (`mmaxDrawdown` is the only
override; its axes are physical-row frame positions, defined in full in its
profile note). One window partition at a time:

- Rows are ordered by the window operator's `PARTITION BY`/`ORDER BY`
  sort. `i` (1-based) is the row position inside the sorted partition;
  `v_i` the argument value at that row.
- `u_1, u_2, ...` is the subsequence of non-NULL values in order; `m`
  counts non-NULL values seen so far at a given row (the *value axis*).
- **NULL inputs (default):** a row whose `v_i` is NULL outputs NULL for the
  smoothed/drawdown kernels, and internal state counts only non-NULL
  values (warmup is measured in non-NULL values, so long leading-NULL
  gaps do not corrupt state). Exception: `cumPositiveStreak` outputs
  `0` on a NULL input row (a NULL observation breaks the run; output
  stays dense and non-NULL). A NULL row occupies no state and does not
  advance the value axis *unless the profile defines a physical-row axis*
  (`mmaxDrawdown` does: its NULL rows still occupy frame slots, see its
  note).
- **Warmup (default):** every period-carrying kernel emits NULL until its
  profile below has a defined value; warmup is measured on the value axis
  except for `mmaxDrawdown` (physical frame axis). This is strict
  fixed-period warmup so migration tests can assert leading NULL bits
  exactly. `cumPositiveStreak` is dense (no warmup NULLs).
- **Partition isolation:** state resets at every `PARTITION BY` boundary.

| # | entry | params | output type | warmup / first defined value | recurrence (after warmup) |
|---|---|---|---|---|---|
| 1 | `sma(x, n)` | `x` numeric, `n` int const | `DOUBLE` | first `m = n` row: seed = mean of \(u_1..u_n\) | `y = (sum of last n non-NULL u)/n` |
| 2 | `wma(x, n)` | same | `DOUBLE` | first `m = n` row | linearly weighted mean over the last `n` non-NULL values with weight `k` for the k-th oldest of the block, `k = 1..n`, denominator `n(n+1)/2` |
| 3 | `trima(x, n)` | same | `DOUBLE` | first `m = n` row. Odd `n`: two `SMA` passes with period `p = (n+1)/2`. Even `n`: `SMA` period `q = n/2` then period `q+1` | defined as the double-`SMA` composition over the non-NULL stream, implemented as the folded single-pass triangular convolution with exact rational weights (equal to the composition in exact arithmetic; see the numerical-stability note) |
| 4 | `ema(x, n)` | same | `DOUBLE` | seed at `m = n`: `y = mean(u_1..u_n)` | `y = a*u_m + (1-a)*y_prev`, `a = 2/(n+1)` |
| 5 | `wilder(x, n)` | same | `DOUBLE` | seed at `m = n`: `y = mean(u_1..u_n)` | `y = a*u_m + (1-a)*y_prev`, `a = 1/n` (Wilder's smoothing) |
| 6 | `kama(x, n)` | same | `DOUBLE` | seed `y := u_n` at `m = n` (no output); outputs defined from `m = n+1` | `c = |u_m - u_(m-n)|`, `vol = Σ |u_k - u_(k-1)|` over the last `n` steps (computed fresh per row — see the numerical-stability note), `er = c/vol` (`er = 0` when `vol = 0`), `sc = (er*(2/3 - 2/31) + 2/31)^2`, `y = sc*u_m + (1-sc)*y_prev`; constants follow the Turtle convention fast period 2, slow period 30 |
| 7 | `dema(x, n)` | same | `DOUBLE` | `dema = 2*e1 - e2` where `e1` is an `ema` stream and `e2` an `ema` of `e1`'s stream; first defined where both exist (`m = 2n-1`) | maintain `e1`, `e2` recurrences |
| 8 | `tema(x, n)` | same | `DOUBLE` | `tema = 3*e1 - 3*e2 + e3` (three chained `ema` profiles); first defined at `m = 3n-2` | maintain three chained recurrences |
| 9 | `t3(x, n)` | same | `DOUBLE` | six chained `ema` profiles `e1..e6`; first defined at `m = 6n-5`; `v = 0.7` fixed; `t3 = -v^3*e6 + 3*v^2*(1+v)*e5 - 3*v*(1+v)^2*e4 + (1+v)^3*e3` (Tillson generalized-DEMA coefficients) | maintain six chained recurrences |
| 10 | `mmaxDrawdown(x, n)` | same | `DOUBLE` | physical-frame axis: rows with `i < n` output NULL; rows with NULL `v_i` output NULL; a full frame whose every prefix-peak is <= 0 (ratio regime undefined, e.g. an all-nonpositive frame) outputs NULL; otherwise the frame's max defined drawdown ratio (value domain in the profile note) | see profile note below |
| 11 | `cumPositiveStreak(x)` | `x` numeric | `BIGINT` (not null) | dense from row 1 | `v_i > 0` → `1 + prev`; `v_i <= 0` → `0`; `v_i IS NULL` → `0` |

**mmaxDrawdown profile note (family-default overrides).** This kernel uses
*physical-row axes*: frame membership, warmup, and eviction count partition
rows, exactly like the `ROWS n` window frame the issue calls "bounded"; the
value-axis family defaults apply here only through the row rule below. The
parameter `n` bounds a row frame, never a value count.

For row `i` (1-based physical position in the sorted partition) with bounded
frame `F_i = {max(1, i-n+1) .. i}` (physical rows, including NULL rows):

```
peak(j)  = max{ v_k : k in F_i, k <= j, v_k non-NULL }   (undefined if that set is empty)
dd(j)    = 1 - v_j/peak(j)        defined only when v_j is non-NULL and peak(j) > 0
mmaxDrawdown[i] =
    NULL                                       if v_i is NULL (row rule),
                                                or i < n (frame not yet full),
                                                or no j in F_i has a defined dd(j)
    max{ dd(j) : j in F_i, dd(j) defined }      otherwise
```

**Value domain (review-finding #1 disposition).** For every defined output:

- The ratio is nonnegative whenever defined, because `peak(j) >= v_j` and
  the denominator is positive.
- On a *nonnegative* series the output lies in `[0, 1]`: the upper endpoint
  is reachable — a frame peak of 100 followed by a 0-trough yields exactly
  `1 - 0/100 = 1.0` (total loss), so the earlier `[0,1)` claim was wrong.
- A *negative* trough following a positive frame-local peak yields more
  than 1: frame `[100, -20]` gives `1 - (-20)/100 = 1.2`. Intentional and
  deterministic (relative loss past total collapse); documented, not
  clamped, because clamping would hide data.
- The defined domain is unbounded above on finite inputs (r5): frame
  `[1, -10]` yields `1 - (-10)/1 = 11`, and a tiny positive peak under a
  huge negative value such as frame `[1e-308, -1e308]` computes the
  quotient `-1e308/1e-308 = -1e616`, which overflows to `-Inf` (r4
  bounded every ratio by `|1 - v/peak| <= 2` under the `peak > 0`
  guard; the `[1, -10]` vector falsifies that bound, and the r4 review
  replaced it with the checked ratio rule of the overflow contract
  below - the asymmetric-magnitude and tiny-positive-peak boundary
  vectors with their independent expected results/errors are pinned in
  section 9).
- *Ratio regime undefined -> NULL, never an arithmetic error:* when no
  in-frame `dd(j)` is defined — every visible prefix-peak <= 0
  (all-nonpositive frames such as `[0, 0]`, `[0, -5]`, `[-10, -20]`, or an
  all-NULL frame) — the output is NULL. Division by zero is unrepresentable:
  the `peak(j) > 0` guard precedes every division.
- The *ordered-pairs reading* — maximum of `1 - v_k/v_j` over in-frame
  pairs `j <= k` — equals this definition only on a nonnegative series
  (the equivalence needs the peak to dominate every candidate denominator);
  the peak-based closed form above is the definitional contract, the pairs
  phrasing is an intuition aid for the nonnegative case.

**NULL-axis semantics (review-finding #2 disposition).**

- *Own-row rule:* a row with NULL `v_i` outputs NULL (a row with no
  observation has no drawdown), even when the frame is otherwise full.
  `n = 3` over `[100, 90, NULL]` emits NULL at every row: rows 1-2 are not
  full frames, row 3 is full but its own value is NULL. The row rule wins
  over the frame formula at the emitting row, so the general NULL-in ->
  NULL-out convention and the frame-based value agree by definition.
- *Frame membership and eviction:* NULL rows occupy frame slots (they are
  real ticks) and evict like any other row: the peak structure skips their
  values, the window slides over them physically.
- *Interior NULLs* contribute nothing: `n = 3` over `[100, 90, NULL, 95]`
  gives row 4 frame `[90, NULL, 95]`: row-2 contribution `0`, row-3 none,
  row-4 peak `max(90, 95) = 95` and `dd = 0` -> output `0.0`.
- *Leading NULLs* do not delay warmup: `n = 3` over `[NULL, NULL, 100, 95]`
  emits `0.0` at row 3 (frame `[NULL, NULL, 100]`, peak 100) and `0.05` at
  row 4 (frame `[NULL, 100, 95]`, peak 100).
- These exact sequences are pinned in the validation plan (section 9).

- Frames are row-bounded by the parameter `n`, which is what makes the
  peaks *frame-local* instead of all-history peaks (the issue's stated
  requirement): a high peak rotating out of the frame can no longer
  elevate the value, while the cumulative-control recipe (all-history peak)
  keeps it. The single-nesting counterexample stands: composing
  `MAX(1 - v/peak) OVER (ROWS n-1 PRECEDING)` over one
  `MAX(v) OVER (ROWS n-1 PRECEDING)` anchors the inner frame at the inner
  row (`[j-n+1, j]`), not the outer frame start (`[i-n+1, j]`), and
  overestimates peaks.
- v1 algorithm is an `O(n)` per-row scan inside the emission loop; the
  accepted work budget, the in-scan cancellation cadence, and the deque
  refinement conditions live in section 8 (review-finding #4 disposition).

**Numerical-stability contract (r2 review finding: KAMA rolling
volatility; verified and generalized to every windowed finite sum).**

- **Invariants (pinned by UT against a higher-precision in-test oracle):**
  - KAMA's efficiency ratio satisfies `er <= 1 + 2^-50` (about
    `1 + 8.9e-16`) *under the selected compensated accumulation* (below),
    with `er = 1` obtained exactly on every review vector. The bound is
    derived, not guessed: with `vol` accurate to about one ulp of the true
    window sum (Neumaier) and `c` the correctly rounded difference, the
    composition `(c(1+u))/(V(1-u-w))` plus one division rounding is
    `<= 1 + 4*2^-53 + O(u^2)` with `u = 2^-53`, `w` the O(u^2·n)
    compensation residue (n <= 1024); the asserted `2^-50` carries about
    an eight-fold margin over the tightest derivation. The r3 assertion
    `er <= 1 + 2e-16` was FALSIFIED by the r3 review and is withdrawn:
    for `n = 4` over `[-1e16, 0, 1, 2, 3]`, uncompensated sequential
    summation computes `vol = 1e16` (the three +1 terms are absorbed under
    a 1e16-class leading term) while `c = fl(1e16+3) = 1e16+4`, giving
    `er = 1.0000000000000004` — four half-ulps, beyond the withdrawn
    bound; at `n = 1024` over `[-1e16, 0, 1, ..., 1023]` the excess
    reaches 461 ulps (both re-derived by this handler before revising).
    The runaway subtractive-running variant (`er = 2` on
    `[0, 1e16, 0, 1, 2]`, roughly 10^16 ulps beyond the new bound) is
    still forbidden outright.
  - `er` is never clamped: the bound is a verification assertion over an
    accurately computed volatility, not a runtime correction — preserving
    accurate volatility is the mechanism, per the r3-review action.
  - `sc = (er*(2/3 - 2/31) + 2/31)^2` stays within `[(2/31)^2, (2/3)^2]`
    up to the same slack, and every KAMA output is a convex combination
    `sc*u_m + (1-sc)*y_prev` — it must lie in the closed interval between
    the previous KAMA value and the current value, up to the same slack;
    the runaway variant's KAMA ≈ `-3.37e15` (billions outside the value
    range) remains forbidden.
- **Forbidden mechanism:** no cross-row *subtractive* running aggregate is
  permitted anywhere in the family (r2 review finding, unchanged), and no
  *uncompensated* summation is permitted inside a window pass either (r3
  review finding: exact telescoping does not transfer to a 2-ulp float
  bound under plain sequential accumulation).
- **Selected update method:** every windowed finite sum —
  `sma`(row mean), `wma`(weighted mean), `trima`(folded triangular
  convolution), and `kama`'s `vol` — is computed per row as ONE fresh pass
  over terms derived from the materialized source values
  (`ctr.aggVecs[idx].Vec[0]`, no derived running state), accumulated with
  **Neumaier compensated summation** (promoted from r3's follow-up note
  into the selected method) so the accumulated window sum is accurate to
  about one ulp of its true value; each diff term is computed as
  `|u_k - u_(k-1)|` directly from source data. Means and weighted means
  use the shared *normalized compensated mean* **procedure M**, defined,
  justified and given its explicit accuracy contract in the overflow
  contract below (r5: the withdrawn r4 mechanism weighted each term by
  `w_k/W` before summation; its accumulation is exact at neither edge -
  it can overflow the running sum before compensation acts, and it can
  annihilate subnormal-class terms before accumulation; see the r4
  disposition in section 11). Recurrence kernels (`ema`,
  `wilder`, `dema`, `tema`, `t3`, `kama`'s carrying update,
  `cumPositiveStreak`) keep `O(1)` multiplicative/counter states
  (inherent IIR shrinkage drift, coefficients in `[0,1]`); every
  recurrence *update* step is computed by the shared convex-update
  **procedure U** (r6; specified and justified in the range-safety
  contract below - the r5 wording admitted direct float updates with
  no verified contract at the domain floor).
- **Cost:** the fresh pass remains `O(min(n, rows))` per row with `O(1)`
  extra work per term for compensation; the section-8 budget is unchanged.
  A `math/big` exact-accumulation variant is NOT used at runtime (the
  compensation suffices); it is the *in-test higher-precision oracle*
  that recomputes every pinned vector exactly and validates the
  asserted bounds and exact expectations.
- Pinned deterministic vectors (added to section 9): the r2-review
  large-to-small transition `n = 2` over `[0, 1e16, 0, 1, 2]`
  (row 5 `vol = 2`, `er = 1`, `sc = 4/9`, KAMA ≈ `3.074e15`); the
  r3-review monotone vector `n = 4` over `[-1e16, 0, 1, 2, 3]` and its
  reversal `[3, 2, 1, 0, -1e16]` (Neumaier accumulation: `vol = fl(1e16+3)
  = 1e16+4`, `c = fl(1e16+3) = 1e16+4`, hence `er = 1` exactly — the
  uncompensated sequential variant computes 4-half-ulp `er` and must fail
  the assertion); the escalation `n = 1024` over
  `[-1e16, 0, 1, ..., 1023]` (Neumaier: `er = 1` exactly; sequential:
  461 ulps — forbidden); the nonflat-to-flat transition ending in equal
  values (`vol` exactly 0, `er = 0`, smooth-carry); and magnitude-drop
  windows for `sma`/`wma`/`trima` — every vector recomputed by the
  in-test higher-precision oracle to validate both the exact
  expectations and the asserted slack bounds.

**Overflow, underflow and non-finite contract (r3 review finding #2;
verified, then specified; the r5 round revises the r4 mechanisms that
the r4 review falsified).** The r3 draft accepted all numeric inputs while
specifying float64 means/differences/weighted sums without overflow
semantics; the review's counterexamples reproduce exactly (naive
sum-then-divide SMA on `[1e308, 1e308]` overflows to infinity although the
mean `1e308` is representable; KAMA `n = 2` over
`[1e308, -1e308, -1e308]` divides `Inf/Inf` into NaN and would poison the
recurrence). The r4 closure of that finding was then itself falsified by
the r4 review: on the mean side, the r4 pre-scaling overflows its own
running sum on `n = 11` identical `MAXFLOAT` inputs (`+Inf` accumulator,
`-Inf` compensation tail, `NaN` result, although the exact mean
`MAXFLOAT` is representable) and annihilates `n = 3` identical minimum
subnormals term-by-term (`fl(2^-1074 / 3) = 0` for every term, so the
mean computes as `0` although `2^-1074` is representable) - compensation
cannot recover rounding losses incurred before accumulation; on the
drawdown side, the r4 bound `|1 - v/peak| <= 2` is falsified by
`n = 2` over `[1, -10]` (which returns `11`) and by the asymmetric frame
`[1e-308, -1e308]` whose quotient rounds to `-Inf`. Every cited number
was re-derived independently by this handler before this revision. The
specified contract:

- **Input admission (finite values only).** Each argument row is checked
  when consumed: a value of `+Inf`, `-Inf` or `NaN` — including a decimal
  input whose float64 conversion is infinite beyond the finite double
  range — produces the documented execution error
  `input value is not finite for <fn> at row <partition position>` and
  fails the statement at the first occurrence. Non-finite values never
  enter kernel state, so recurrence poisoning is structurally impossible.
- **Range-safe arithmetic where finite results are supported:**
  - Means, weighted means and every recurrence seed (`sma`, `wma`,
    `trima`, the `ema`/`wilder` seed means, and the seeds inside the
    `dema`/`tema`/`t3` EMA chains) are computed by ONE shared algorithm,
    the *normalized compensated mean* **procedure M**, applied at every
    mean/seed site in the family (r4-review finding #2: the withdrawn
    r4 mechanism weighted each term by `w_k/W` before summation and
    claimed its accumulator stays inside the convex hull of the inputs,
    making mean overflow structurally impossible and every mean
    correctly rounded - both halves fail: `n = 11` identical
    `MAXFLOAT` inputs overflow the pre-scaled running sum itself
    (`+Inf` accumulator, `-Inf` compensation tail, `NaN` result, though
    the exact mean `MAXFLOAT` is representable), and `n = 3` identical
    minimum subnormals scale to zero term-by-term (`fl(2^-1074 / 3) = 0`
    for every term), losing the representable mean `2^-1074` before
    compensation can act - compensation cannot recover rounding losses
    incurred before accumulation). Procedure M, given the window's
    finite admitted terms `u_1..u_m` (`m <= 1024`) and its site's
    positive weight profile `w_k` with denominator `W` (all-ones weights
    with `W = m` for plain means; section 3.1 documents the exact
    rational integer weights of `wma`/`trima`):
    1. `T = max_k |u_k|`; if `T = 0`, return the exact zero result under
       the signed-zero rule (`-0` only when every term is `-0`; pinned
       in section 9).
    2. Let `s` be the power of two with `T*s` in `[2^-9, 2^-8)`, taken
       from the exponent of `T` alone (standard frexp idiom); `s` is
       never materialized as a floating value - each term is scaled
       through the ldexp-style multiply `y_k = u_k*s`, which is exact
       while the scaled value stays in range. Scaled normal terms are
       `< 2^-8` in magnitude; the only inexact scalings are terms scaled
       below the subnormal domain, and their total contribution is
       `< 2^-1065` over a full window (each such term `<= 2^-1075`) -
       far below the accuracy contract.
    3. Run the window pass over the normalized terms with Neumaier
       compensation (plain means sum the `y_k` directly; weighted sites
       sum the products `y_k*w_k` with exact integer weights - each
       product's single rounding is bounded by the accuracy contract's
       product term below). Every intermediate of this pass - terms,
       partial sums (`<= 1024*2^-8 = 4` for plain means; below `2^12`
       at the heaviest permitted weighted profile, `wma` with
       `n = 1024`) and the even smaller compensation tail - stays far
       inside the finite range: overflow is structurally impossible on
       finite admitted inputs, and no intermediate can become `Inf` or
       `NaN` (this normalization bound replaces the withdrawn
       convex-hull argument).
    4. Divide by the site's exact denominator LAST: one correctly
       rounded division (`m` for plain means, `W` for weighted sites);
       no per-term reciprocal is ever formed.
    5. Rescale the normalized mean by the exact power of two - one
       final IEEE multiply, exact whenever the true mean is
       representable at its scale.
    6. **Accuracy contract (explicit, replacing the withdrawn correctly-rounded
       claim):** with `T = max_k |u_k|`, the computed result `r`
       satisfies, for plain means, `|r - exact| <= 2^-40 * T +
       2^-1075`, and, for weighted sites, `|r - exact| <= 2^-51 * (the
       window's sum over k of `w_k*|u_k|`) + 2^-40 * T + 2^-1075`
       (r5-review finding #11: the r5 bounds carried no underflow
       rounding allowance, which is unsatisfiable when the exact mean
       lies in the underflow gap: `n = 2` over `[0, 2^-1074]` has
       exact mean `2^-1075`, so every float64 result - including the
       correctly rounded `0` - has error at least `2^-1075`, while
       the r5 declaration was `2^-1114`; procedure M returns exactly
       the correctly rounded `0` there, missing the r5 bound by
       `2^39`). The floor `2^-1075` is half the minimum-subnormal
       spacing and equals the procedure's single output-scale
       rounding (the final rescale multiply): the unavoidable grid
       error, not algorithm slack. The weighted product term bounds
       the per-term weight-product roundings, which compensation
       cannot recover because they precede accumulation; at the
       heaviest permitted profile (`wma`, `n = 1024`, `W = 524800`,
       uniform magnitudes) the composed T-scaled part stays below
       `2^-31 * T`, and typical windows sit orders of magnitude
       lower. The bound is absolute with respect to the window's
       input scale - cancellation can make the exact mean
       arbitrarily small, and the error remains bounded by the input
       magnitude plus the grid floor. Derivation: the compensated
       residue, the division and the T-scaled rescale contributions
       are `2^-52`-class of their (bounded) normalized scales, at
       most `2^-42 * T` unscaled through the `2^9` normalization
       factor; weighted per-term products contribute at most
       `2^-52` of the term scale (half-ulp); each T-scaled constant
       carries at least an eightfold margin over its derivation
       term, while the floor is tight by necessity - gap vectors
       reach `1 - 10^-9` of the bound. Pre-checks against the
       higher-precision oracle: the r5 runs (300 plain + 200
       weighted, `n = 1024` profile extremes) remain valid in their
       non-gap domain (worst `1.0e-4` of the T-scaled part), and
       the r6 run adds 800 bottom-seeded vectors (subnormal grid
       with half-grid-mean targeting, tiny normals, deep-subnormal
       clusters, mixed magnitudes; plain and weighted) with zero
       violations of the floor-augmented tier contract.
    7. **Exactness pins in the representable class** (verified before
       this revision, asserted exactly against the oracle in
       section 9): an `sma` window of 11 identical `MAXFLOAT` inputs
       returns exactly `MAXFLOAT` (the only inexact step is the
       compensated sum's final rounding, whose residue for this pinned
       vector lies far below the exact mean's half-ulp, so the
       by-denominator division restores the exact mean; the rescale is
       exact); a 3-term window of identical minimum subnormals
       `2^-1074` returns exactly `2^-1074` (scaling subnormals up is
       exact; `3*y`, the division by 3 and the rescale are all of
       exact class) - representable subnormal means are returned
       exactly whenever the exact round-trip is representable;
       `2 x 1e308 -> 1e308`; and mixed-sign `[MAXFLOAT, -MAXFLOAT] ->
       0` (the scaled terms are exact negatives). Exactly-rounded
       generality is NOT claimed beyond the pinned class; the accuracy
       inequality above is the contract.
       r6 floor pins with independent oracle expectations: `n = 2`
       over `[0, 2^-1074]` returns exactly `0` (the exact mean
       `2^-1075` is the tie midpoint; nearest-even is `0`; the
       asserted error equals the floor allowance - the pin
       demonstrates the augmented bound, it is not a failure);
       neighbors: `[2^-1074, 2^-1074] -> 2^-1074`, `[2^-1074,
       2*2^-1074] -> 2*2^-1074` (tie rounds to the even multiple),
       `[0, 3*2^-1074] -> 2*2^-1074`, `[2^-1074, -0.0] -> +0`;
       weighted neighbors: `[0, 2^-1074]` with `wma` weights
       `1, 2 -> 2^-1074` (nearest to two thirds of the grid unit)
       and `[2^-1074, 2^-1074]` with weights `1, 3 -> 2^-1074` (all
       verified before this revision; errors asserted against the
       floor-augmented tolerance in the oracle).
  - Every recurrence update step - `ema`, `wilder`, KAMA's
    carrying update, and every EMA stage of the `dema`/`tema`/`t3`
    chains - is computed by the shared *convex-update* **procedure
    U** (r5-review finding #12: the prior wording admitted direct
    per-row updates `fl(a*u) + fl((1-a)*y)`, which carry no verified
    contract at the domain floor. Reproduced: with `h = 2^-1074`,
    EMA `n = 3` and Wilder `n = 2` on constant `[h, h, ...]`
    streams following exact procedure-M seeds update as
    `fl(0.5*h) + fl(0.5*h) = 0 + 0 = 0` - the representable constant
    collapses - and constant `[3h, 3h, ...]` streams update to
    `4h`, above the input envelope, and stay there; on the
    subnormal grid the half coefficient `a = 0.5` fails
    systematically (every odd constant `X >= 3h` overshoots by a
    full grid unit; `X = h` collapses), and the collapse propagates
    to the chained consumers - `dema(3)` and `tema(3)` over
    constant `h` emit all-zero under the old mechanism). Procedure
    U, given the profile coefficient `a in [0,1]` (or KAMA's row
    value `sc`), the current input `u` and previous state `y`:
    1. endpoints: `lo = min(u, y)`, `hi = max(u, y)`;
    2. coefficients: `a_hat = fl(a)`, `b_hat = fl(1 - a)` of
       the true rational complement (never `fl(1 - a_hat)`;
       representable; their real sum can deviate from 1, so no
       counting argument is relied on - the guarantee is step 4;
       a 203,537-coefficient scan of the reachable alpha set -
       every ema `2/(n+1)` and wilder `1/n` value, a 2,002-point
       KAMA `sc` grid, 200,000 random doubles - found the OLD
       mechanism also produces no overflow at `u = y = MAXFLOAT`,
       so no naive top falsifier is claimed anywhere: the top
       contract is delivered structurally by U);
    3. products: `p = fl(a_hat*u)`, `q = fl(b_hat*y)`: on finite
       admitted inputs each product magnitude is at most
       `max(|u|, |y|)`, so no single product can overflow;
    4. compensated sum with a structural clamp: `t = p + q`; if
       `t` is infinite (reachable only from the addition at
       magnitude extremes), return the hull endpoint of `t`'s
       sign; otherwise add the two-term sum residue and clamp
       into the hull: `y_out = min(max(t + residue, lo), hi)`.
    **Range contract (structural):** `y_out` always lies in the
    closed hull `[lo, hi]` - the envelope and no-overflow claim
    that r5 asserted by counting is delivered by construction at
    every scale, including the subnormal floor, where constant
    `h` and `3h` streams carry exactly through every stage (the
    hull is the single point) and `ema(5)` constant-`MAXFLOAT`
    streams carry their seed exactly - procedure M seeds one ulp below
    `MAXFLOAT` (`wilder(3)` seeds at `MAXFLOAT` itself; design-approval
    advisory correction), and procedure U preserves it. No `Inf` or `NaN` is
    reachable on finite admitted inputs, and clamping only moves
    results toward the true convex value, never away (the exact
    value is inside the hull).
    **Underflow contract:** at the subnormal grid the two products
    each carry at most half a grid unit and the compensated add is
    exact on grid multiples, so mid-hull values round per the
    bottom grid - including to exactly `0` when the true convex
    value lies in the gap - while the clamp keeps every result
    bounded by the stream's own endpoints and constant streams
    carry exactly.
    **Accuracy contract (r7):** `|y_out - (a*u + (1-a)*y)| <= 2^-51 *
    max(|u|,|y|) + 2^-1074`. r6-review finding #13 falsified the
    previous `2^-52` constant in the normal range: on the reachable
    `wilder(x, 12)` stream, twelve `1.12` inputs seed exactly `1.12`
    (procedure M) and the next input `0.86` updates to
    `1.0983333333333332`, off the exact-rational `a = 1/12` value by
    `2.6830389761774615e-16`, past the old bound
    `2.486899575160351e-16` while sitting inside the hull (the clamp
    is a no-op there and cannot repair the bound); a concentrated
    same-binade scan over the reachable coefficient set reproduces
    `304` violations per `1,880,064` pairs, so the effect is
    systematic, and the r6 tightness claim is withdrawn. The
    corrected bound composes the three half-ulp-class contributions
    that the r6 wording missed, over the weighted mix
    `M' = a*|u| + (1-a)*|y|`, at most `M = max(|u|,|y|)`:
    (1) coefficient quantization - `|fl(a) - a| <= a*2^-53` and
    `|fl(1-a) - (1-a)| <= (1-a)*2^-53` - contributes at most
    `2^-53 * M'`; (2) the two product roundings - each product
    magnitude stays within `M'*(1 + 2^-53)` and rounds at most half
    an ulp - contribute at most `2^-53 * M' * (1 + 2^-53)` together;
    (3) the final two-term add - Fast2Sum's residue is exact for
    `|p| >= |q|` with `t` finite, so `t + residue` equals the exact
    `p + q` - rounds once, contributing at most
    `2^-53 * M' * (1 + 2^-53)^2`. Composed,
    `|y_raw - (a*u + (1-a)*y)| <= 3*2^-53*M'*(1 + 2^-53) < 2^-51*M`,
    and the hull clamp only moves results toward the exact value,
    so the bound survives clamping. The `2^-1074` floor is
    unchanged: at the subnormal grid the products and the residual
    sum are grid multiples - the add rounds exactly - so the floor
    carries the pair of product roundings. Pre-checked by the
    falsified stream itself (error `0.54` of the corrected bound
    `4.973799150320702e-16`), by the concentrated scan (zero
    corrected-bound violations in `1,880,064` pairs; worst ratio
    `0.583` at `a = 1/63`, `u = 1.015625`, `y = 1.0625`, error
    `2.749123680024197e-16`) and by a 100,000-vector doc-formula
    fuzz across subnormal, general and MAXFLOAT classes (zero
    hull/finiteness/accuracy violations; worst ratio `0.9926`,
    floor-governed subnormal class). The constant is asserted by
    derivation - a four-thirds margin over the composed worst
    case - and is not claimed tight.
    Cost: `O(1)` per update - two multiplies, one compensated
    two-term add, two comparisons; the section-8 class split is
    unchanged.
  - `mmaxDrawdown` is *not* magnitude-bounded: on finite admitted
    input its defined ratio `1 - v/peak` can be any representable value
    (frame `[1, -10]` returns `11`; frame `[1e-308, -1]` returns
    `1e308`), so the range rule is a checked guard, not a bound (the
    r4 `|1 - v/peak| <= 2` claim is withdrawn - falsified by `[1, -10]`
    and by the asymmetric frame `[1e-308, -1e308]` whose quotient
    rounds to `-Inf`): each emitted ratio checks its quotient and its
    difference for finiteness, and the first non-finite ratio fails the
    statement with the documented `mmaxDrawdown` range error below,
    while every representable ratio of any magnitude is emitted as a
    value.
- **Checked, documented range errors where the result regime is
  unrepresentable:**
  - `dema = 2e1 - e2`, `tema`, and `t3` combine chained EMAs with negative
    coefficients (e.g. `-v^3`, `-3v(1+v)^2`), so their true values reach
    up to `L·max|u|` with `L = 3` (`dema`), `7` (`tema`), `14`
    (`t3`, computed from the fixed `v = 0.7` coefficients) and intermediate
    terms scale with `L` as well. These kernels perform checked
    arithmetic under a documented input admission bound:
    `max|u| <= MAXFLOAT/L`; a partition exceeding it raises the
    documented error `input magnitude exceeds the finite range of <fn>
    (period n = <n>, bound MAXFLOAT/L)` instead of overflowing silently.
    Under the bound, all terms and every true result are representable,
    so a finite result is always produced where one exists.
  - KAMA's in-window differences (`|u_k - u_(k-1)|`, `|u_m - u_(m-n)|`)
    must be representable; a difference exceeding the finite range (e.g.
    the `±1e308` span vector) raises the documented KAMA range error
    before any division, so `er` is never computed from an infinite
    volatility and `NaN` outputs are structurally impossible. The
    compensated accumulation of `vol` itself is range-checked with the
    same error: with alternating `±5e307`-class inputs every in-window
    diff is finite while the four-term window volatility `4e308` is not,
    and the contract raises the documented range error there too rather
    than deflating `er` to zero from a saturated accumulator (recomputing
    such extreme windows in higher precision at runtime is rejected; the
    `math/big` path is the test oracle only).
    `mmaxDrawdown` carries its own checked-ratio rule (see the
    range-safe bullet above) instead of a magnitude bound:
    representable ratios of any magnitude are emitted, and the first
    non-finite quotient or difference in a frame fails the statement
    with the documented error `drawdown ratio is not finite for
    mmaxdrawdown at row <partition position>` - so no silent
    `Infinity` output is reachable on finite admitted inputs.
- **Underflow policy:** IEEE gradual underflow to subnormals is
  retained. Procedure M is the single deliberate scaling in the family,
  and its power-of-two normalization is exactly what returns
  representable subnormal means exactly (scaling tiny windows up is
  always exact; the `3 x 2^-1074 -> 2^-1074` pin), where the withdrawn
  r4 mechanism lost them term-by-term (`fl(2^-1074 / 3) = 0`). Tests
  pin subnormal inputs where the expected outputs are exactly
  representable, and - r6 - at the underflow-gap boundaries whose
  expected output is the correctly rounded nearest grid value, with
  the error asserted against the floor-augmented tolerance (a `0`
  result on `[0, 2^-1074]` is correct, not a loss); no expected
  output below the minimum subnormal is ever asserted as a value.
  Signed-zero handling: `cumPositiveStreak` is fixed by its `> 0`
  predicate (both `+0` and `-0` are tested and produce exit `0`), and
  procedure M's zero-window rule returns `-0` only when every term is
  `-0` (pinned in section 9).
- Every rule above carries an exact expected result or error in the
  section-9 validation plan, computed by the independent higher-precision
  oracle.

### 3.2 Documented SQL migration recipes (no new entries)

Delivered as a documentation page (`docs/sql/technical-indicators.md`) with
BVT-validated working SQL on base semantics (all verified in this
investigation on the base binary); the BVT keeps them checked-in so the
recipes cannot rot:

1. **Cumulative maximum drawdown ratio** — the issue's own control
   (`MAX(1 - v/running_peak) OVER (ORDER BY ... ROWS BETWEEN UNBOUNDED
   PRECEDING AND CURRENT ROW)`), asserting complete values `0,0,0.25,0.25`
   for the `100,120,90,110` fixture, not merely execution success.
2. **Simple moving average** — `AVG(x) OVER (ORDER BY k ROWS BETWEEN n-1
   PRECEDING AND CURRENT ROW)`; documents that partial frames (first
   `n-1` rows) average fewer values, which is the MySQL/ordinary-SQL
   profile and differs from the strict `sma()` warmup; users needing the
   strict profile use `sma(x, n)`.
3. **Local trend / rolling percentage change** — `x / LAG(x, n) OVER
   (ORDER BY k) - 1`, and the "share of up-ticks in the frame" variant
   `AVG(sign) OVER (ROWS n-1 PRECEDING)`.
4. **Bounded positive streak** — `LEAST(cum_streak, n)` post-composition
   applied to the streak mechanism, or the native `cumPositiveStreak` with
   a documented cap expression.

The recipes section explicitly marks entry availability separately
(entry names resolve to window functions as of this change) from
mathematical rewrite equivalences (the recipes above), per the issue's
acceptance wording.

### 3.3 Non-goals

- No TA-Lib/DolphinDB full-parameter parity, no `vfactor`/fast/slow period
  options for KAMA or T3 in v1 (constants fixed and documented; optionality
  would need an options-argument protocol with no current builtin).
- No performance claims or benchmark comparison (issue explicitly waives).
- No new aggregate (GROUP BY) forms, no persistent catalog, migration, or
  wire format changes.
- No RANGE/GROUPS frames for these kernels (parameter `n` defines extent).

## 4. SQL surface contract

For every kernel in section 3.1:

- Syntax: `kernel(arg-list) OVER ([PARTITION BY ...] ORDER BY ...)`.
- `OVER` is required (generic existing rule); with the entries registered,
  a missing `OVER` must report the existing "requires an OVER clause"
  window-function diagnostic, and **not** error 20105 — a BVT asserts this
  exact availability distinction on the four names probed in the issue.
- `ORDER BY` inside `OVER` is *required*; absence is a binding error with
  a message naming the function and the reason (series order is part of
  the operator's mathematical definition).
- Explicit `ROWS`/`RANGE`/`GROUPS` frame clauses are *rejected* with
  "window frame is not applicable to `<fn>`: the period is given by
  `<fn>`'s parameter" (decision: never silently ignore a user frame).
- `PARTITION BY` is optional and resets state per partition.
- Argument validation at bind time:
  - exactly 2 arguments `(x, n)` for the nine MA kernels and
    `mmaxDrawdown`; exactly 1 argument `(x)` for `cumPositiveStreak`;
  - `x` must be numeric; otherwise "argument 1 of `<fn>` must be numeric";
  - `n` must be an expression foldable to an integer constant within
    `[1, 1024]`; otherwise "period argument of `<fn>` must be a constant
    integer in [1, 1024]" (uniform cap; see section 8 for the budget that
    justifies it). Degenerate cases `n = 1` are legal and
    documented (`sma` passes the value through; `ema`/`wilder` with
    `a = 1` pass through after an exact first-value seed; `mmaxDrawdown`
    with `n = 1` emits `0` only when the frame's visible peak is positive,
    and — consistently with the section 3.1 regime rule — emits NULL when
    the single visible value is `0` or negative, because no positive peak
    exists; pinned case: `n = 1` over `[NULL, -1, 0, 1]` emits
    `[NULL, NULL, NULL, 0]` in both the UT and BVT plans).
- Row-value admission at execution time (r3-review finding #2; rows
  stream, so this is a per-row execution check, not a bind-time check):
  - a `+Inf`, `-Inf` or `NaN` argument value fails the statement with
    `input value is not finite for <fn> at row <partition position>`;
  - kernels whose combination arithmetic needs a heading room beyond the
    input envelope (`dema` with `L = 3`, `tema` with `L = 7`, `t3` with
    `L = 14` at the fixed `v = 0.7`) reject partitions where
    `max|u| > MAXFLOAT/L` with
    `input magnitude exceeds the finite range of <fn> (period n = <n>, bound MAXFLOAT/L)`;
  - a KAMA in-window difference (`|u_k - u_(k-1)|` or `|u_m - u_(m-n)|`)
    exceeding the finite double range fails the statement with the same
    error form before any division (so `Inf/Inf` NaN results are
    structurally impossible);
  - `mmaxDrawdown` checks every emitted ratio for finiteness (r5): a
    frame whose quotient `v/peak` or whose difference `1 - v/peak` is
    non-finite - the asymmetric-magnitude frame `[1e-308, -1e308]`
    computes `-1e308/1e-308 = -Inf` although both inputs are finite -
    fails the statement with `drawdown ratio is not finite for
    mmaxdrawdown at row <partition position>`; representable ratios of
    any magnitude (`11` on `[1, -10]`, `1e308` on `[1e-308, -1]`,
    `2.0` on the symmetric extreme frame) are emitted as values. No
    magnitude blanket exists: the withdrawn r4 `|1 - v/peak| <= 2`
    claim is removed from every claim site.
- Duplicate window expressions reuse the plan's existing window
  deduplication (same AST key); constants are not re-evaluated per row.

## 5. Design: architecture and owner paths

### 5.1 Registration (plan side)

| owner path | change |
|---|---|
| `pkg/sql/plan/function/function_id.go` | 11 new function-ID constants and 11 `allFunctionsByName` entries (`sma`, `wma`, `trima`, `ema`, `wilder`, `kama`, `dema`, `tema`, `t3`, `mmaxdrawdown`, `cumpositivestreak`); IDs allocated in the first free numeric block, verified unique at implementation time |
| `pkg/sql/plan/function/list_window.go` | 11 `FuncNew` entries in `supportedWindowInNewFramework`: `class: plan.Function_WIN_VALUE` (reusing the existing pb flag — no protobuf regeneration), `checkFn` enforcing section 4 argument shapes, one overload each with `isWin: true`, explicit `retType` (`DOUBLE`, nullable; `BIGINT` not-null for `cumPositiveStreak`) |
| `pkg/sql/plan/function/function.go` | new `GetFunctionIsWinIndicatorFunByName(name)` helper (function-ID set membership); extend `GetFunctionIgnoresWindowFrameByName` to include the indicator IDs so `bindWindowSpec` supplies the synthetic default frame for frame-less OVER clauses |
| `pkg/sql/plan/window_binder.go` | enforce the section 4 OVER contract: reject explicit frames for indicator names; require `ORDER BY`; keep the existing OVER-less diagnostic intact |

The functions are *not* aggregates: no `aggexec` kernel, no `WinIdOf*`, no
`aggName` — the window operator evaluates them directly (next subsection).
The `class: Function_WIN_VALUE` choice rejects itself in `processFuncRange`
first (indicator check precedes the value check), keeping the pb untouched.

### 5.2 Execution (operator side)

| owner path | change |
|---|---|
| `pkg/sql/colexec/window/window.go` | `processFuncRange` gains a first dispatch case `function.GetFunctionIsWinIndicatorFunByName(funcName) → processIndicatorFuncRange(...)`; state slot on `ctr` with explicit lifecycle |
| `pkg/sql/colexec/window/indicators.go` (new) | the eleven per-profile state machines (recurrence kernels keep `O(1)` member state and update through the shared procedure-U convex-update closure, r6; finite-sum kernels re-derive their window terms per row from the materialized argument vector per the numerical-stability contract, no subtractive running aggregates) and the vectorized emit loops |
| `pkg/sql/colexec/window/window_test.go` (new cases) / `indicators_test.go` (new) | focused operator-level UTs |

Execution model (mirrors existing operator invariants):

- The window operator materializes the whole logical partition into
  `ctr.bat` before eval and evaluates argument expressions into
  `ctr.aggVecs` (`evalAggVector`), exactly like the current value-function
  path; the constant `n` is read from `aggVecs[idx].Vec[1]` row 0 with the
  `ntile`/lag-offset validation precedent (constant-ness checked for every
  row, mirroring `validateLagLeadOffsets`).
- `processIndicatorFuncRange(idx, ap, proc, outputStart, outputEnd)`
  advances the per-partition sequence up to `outputEnd` (idempotent,
  incremental: the computed prefix is cached per (window-expression,
  partition generation) in `ctr`), then emits the `[outputStart, outputEnd)`
  slice. Chunked emit (`colexec.DefaultBatchSize`), LIMIT early-exit, and
  multi-chunk partitions follow the operator's existing contract: no
  recomputation of a previously emitted prefix; per-row cost follows the
  section-8 class split (`O(1)` recurrences and streak; `O(min(n, rows))`
  fresh finite sums and the `mmaxDrawdown` scan). Kernels reading back
  in-window source values (`sma`/`wma`/`trima`/KAMA-vol) read the
  materialized argument vector `aggVecs[idx].Vec[0]` directly — no
  derived running state (the numerical-stability contract).
- State lifecycle (review-finding #3 disposition; mirrors the operator's own
  generation discipline, citing `pkg/sql/colexec/window/window.go` and
  `types.go`):
  - *One state object per window expression on `ctr`*, owning the cached
    computed prefix, per (window expression, generation). A **generation**
    is one materialized-partition evaluation: every entry into the `eval`
    case starts one generation — this is the only status through which both
    partition modes flow, because unpartitioned windows accumulate via
    `receiveAll` (window.go Prepare: `ctr.status = receiveAll` when
    `PartitionBy` is empty) and partitioned windows re-enter `eval` per
    `receive`d partition batch. The `receive` branch alone is therefore not
    a reset point (r1's error); `eval` entry is, and it fires exactly once
    per partition exactly as the sibling discipline
    (`ctr.freeRunningAgg()` + `ctr.freeOrderedSetPartitionResults`, with the
    in-tree comment "a new materialized input batch starts a new aggregate
    generation ... closes reuse after an interrupted or failed generation")
    already requires.
  - *Reset points for the indicator prefix:* (1) at every `eval` entry,
    freed and zeroed before any recomputation, covering both partition
    modes, query re-execution on a reused operator (the pipeline re-enters
    `receiveAll`/`receive` -> `eval`), and interrupted/failed generations;
    (2) in `Window.Reset` (types.go), beside `freeRunningAgg`/
    `freeOrderedSetPartitionResults`, because an error exit from `Call`
    skips the normal end-of-generation cleanup (the in-tree Reset comment
    says exactly this) — this covers cancellation, allocation failure, and
    LIMIT-early-exit leftovers before the operator returns to its pool;
    (3) in `Window.Free` (types.go), beside the same calls, so pool reuse
    (`reuse.CreatePool[Window]` clears the struct on release) never sees
    stale prefixes through either path.
  - *Allocation and failure atomicity:* prefix storage is allocated through
    the process mpool under `ctr.allocation` accounting (same register as
    sibling result vectors), and freed at all three reset points. A failure
    mid-emit (allocation error, cancellation, injected error) abandons the
    generation: partial prefix and the partially built result vector are
    freed, and no partial state is reused — the next generation recomputes
    from zero, so a failed emit can never return stale or mixed-generation
    values.
  - No goroutines, no caches beyond the emitted prefix, no persistence or
    serialization (indicator state never crosses process boundaries; spill
    integration is out of scope, bounded per section 8).
- NULL handling executed through `vector.IsNull` scans; outputs are
  nullable `DOUBLE`/dense `BIGINT` vectors allocated via
  `ctr.allocation` accounting like the sibling paths.
- Cancellation: the emission loop calls `checkCanceled` at the existing
  per-emitted-row cadence, and the `mmaxDrawdown` per-row frame scan polls
  the same helper on a fixed inner cadence (every 64 frame elements), so
  cancellation is honored within bounded work instead of after a whole
  partition (review-finding #4 disposition; cadence pinned by the
  cancellation-latency UT in section 9).

### 5.3 Binder/optimizer interactions verified

- Nullability deduction (`query_builder.go` `exprProvenNotNullableWithColResolver`:
  `GetFunctionIsWinValueFunByName(...) → DeduceNotNullable`) — because the
  kernels are registered `WIN_VALUE`, the new IDs must deduce *nullable*
  results (leading warmup NULLs) except `cumPositiveStreak` (dense); a UT
  pins this so no downstream rule treats `ema(...)` as non-nullable.
- Window-count validation (`maxWindowsPerQueryBlock`) and nested-window
  rejection apply unchanged.
- Shuffle/partition selection paths that special-case window names
  (`GetFunctionIsWinOrderFunByName`/`GetFunctionIsWinValueFunByName` in
  `query_builder.go`) were inspected: the only value-function consumer is
  the nullability proof above; no pipeline-shape decisions key off the
  new names.

## 6. Alternatives considered

1. **Documentation-only with pure-SQL recipes (recursive CTE /
   `power`-product self-joins).** Rejected as the primary delivery: the
   recursive family's exact SQL forms were *verified to exist* on the base
   binary (a `WITH RECURSIVE` member can carry EMA state, and a self-join
   with `power` products reproduces the decayed-weight sum), but they are
   iteration-at-a-time or quadratic and not vectorizable — the
   reported financial workloads cannot use them at scale — and the four
   probed entry names would remain unresolved (the issue's acceptance
   explicitly separates availability from math). The recipes remain
   valuable as a documentation subset (section 3.2) and are validated by
   BVT, with the recursive-CTE EMA form included as the documented
   zero-kernel fallback.
2. **Native kernels only for the four probed names; recipes for the rest.**
   Rejected: `wma`, `trima`, and the composed chains are one shared
   mechanism once the sequential-state path exists; splitting the family
   would leave `DEMA/TEMA/T3/TRIMA/WMA/Wilder/SMA` users with the
   unscalable recipes and two different NULL/warmup doctrines inside one
   issue group.
3. **Aggexec window-kernel implementation (the `percentRankExec` state
   marshalling path).** Rejected: unnecessary serialization surface; the
   rank-family state machinery exists for distributed aggregate splitting
   scenarios that do not apply to partition-local sequences; the window
   operator already owns all four current shapes, all state it needs, and
   the evaluation chunking contract.
4. **UDA/plugin (Python UDF server or external process).** Rejected:
   performance, deployment, and dependency costs disproportionate to a
   pure-Go in-process kernel; MatrixOne's builtin path is the appropriate
   surface for SQL-visible functions.
5. **Doing nothing / deferring.** Rejected for this lifecycle: the
   defect is real, reproducible, owner paths are proven, and no technical
   dependency blocks implementation; `deferred`-label history reflects
   prioritization before this handler's investigation, not a blocker.

## 7. Compatibility, delivery, and operations

- **Additive only.** No existing function, catalog, wire, or persistence
  change. Old built binaries simply keep error 20105 for the new names;
  upgrading adds the names; dropping the feature (revert skews) removes
  them again — no mixed-version invariants (functions resolve at the
  executing CN's registry level, and a plan that references the new names
  fails at bind time on an older CN with the ordinary not-supported error).
- **Backward compatibility of error codes.** Scripts asserting 20105 for
  these names will see different errors after upgrade; this is the
  requested behavior change and is called out in the PR description.
- **Rollout:** feature is ungated (new-name only, no default-behavior
  change); no feature flag. Observability: standard operator analyzer
  allocation/rows accounting (the new path tags allocations like sibling
  paths); no new metrics (per mo-dev proportionality; no new long-lived
  state).
- **Security/tenancy:** kernels read only query-visible column values;
  no privilege, isolation, or trust-boundary change; validation bounds
  (`n <= 1024`, integer-constant periods) prevent accidental unbounded work.

## 8. Performance and resource bounds

- Kernel cost split (revised by the r2-review numerical-stability finding;
  method rationale in the section 3.1 stability contract): the recurrence
  kernels — `ema`, `wilder`, `dema`, `tema`, `t3`, KAMA's carrying update
  — and `cumPositiveStreak` are amortized `O(1)` per row with steady-state
  per-kernel memory `O(1)`; the windowed finite-sum kernels — `sma`,
  `wma`, `trima`, and KAMA's volatility term — and `mmaxDrawdown` are
  `O(min(n, rows))` per row, freshly computed from the materialized source
  values (no subtractive running aggregates, the numerical-stability
  values (no subtractive running aggregates, the numerical-stability
  invariant) with Neumaier compensation inside each window pass (the
  r3-review selected method; `O(1)` extra work per term, so the class
  bound is unchanged). Procedure M adds one compare-only magnitude
  scan before the normalized pass and one exact power-of-two scaling
  per term: the mean kernels make at most two `O(n)` window sweeps per
  row, so their recorded per-partition visit count is
  `<= 2 * p * n <= 2 * 1024 * p` - the same order and the same budget
  scale as the `p * n` drawdown line below. Every kernel therefore
  stays inside the same small `O(p * n)` per-partition visit class
  under the uniform `n <= 1024` cap (recorded worst cases: `p * n`
  for `mmaxDrawdown`, `2 * p * n` for the mean kernels), a fixed
  recorded budget in place of the r1 unbounded CPU hole.
  A `math/big` exact-accumulation variant
  remains out of the runtime (compensation suffices; the in-test
  higher-precision oracle validates the asserted bounds).
- `mmaxDrawdown` v1 is an `O(n)` scan per row in the current partition.
  **Accepted work budget (review-finding #4 disposition):** with the
  uniform parameter cap, one partition of `p` rows costs at most
  `p * n <= p * 1024` frame-element visits plus `O(p)` bookkeeping —
  e.g. p = 100,000 rows admits at worst ~1.0 x 10^8 scalar
  compare/track visits (on the order of 50-200ms of single-thread work;
  recorded, not gated, by the section-9 capacity probe), and p = 10,000
  admits ~10^7. The r1 cap (`n <= 100000`) is revoked: it admitted
  ~10^10 visits for a 200,000-row partition — three orders of magnitude
  beyond this budget — which is exactly the
  "bounded memory, unbounded CPU" hole the reviewer identified. The
  supported workload is explicitly stated: indicator/telemetry series
  with lookbacks at or under 1024 rows (every TA-Lib-style period fits;
  an annual 252-row trading window fits; sub-252-daily/weekly/intraday
  parameters fit). Requests above 1024 are rejected at bind time with
  the section-4 diagnostic, which is the documented admission control —
  not a silent slowdown.
- **Cancellation budget inside the scan:** the per-row frame scan divides
  only by `n`, so a cancel is honored within `O(64 + n_lookahead)` visits
  after arrival: the emission loop checks `checkCanceled` per emitted row,
  the inner scan every 64 elements (this tightens the r1 note, which had no
  in-scan polling point); both cadences are pinned by the
  cancellation-latency UT in section 9.
- Deque/staircase refinement (follow-up, conditions stated): a windowed
  `O(p)`-amortized structure exists (staircase of window prefix maxima
  with once-on/once-off stair lifecycle plus per-segment minima), but with
  the accepted budget it is not required for first delivery; the decision
  point is the first real customer workload that combines p > 10^5 with
  n = 1024 ask-rate pressure. Not deferred scope: it does not change any
  profile or SQL contract, only the internal scan.
- Peak transient memory: one cached emitted prefix per indicator window
  expression = `8 bytes × partition rows` — the same order as the
  operator's already-materialized partition batch (`ctr.bat`); freed on
  partition switch/done via existing container cleanup.
- Hot-path discipline: no per-row allocations beyond the result vector
  (allocated once per chunk); no goroutines, channels, locks, or global
  registries at runtime; expressions evaluate over the materialized batch
  like the sibling paths.

## 9. Validation plan (cheapest-first, per issue #29949 acceptance)

All acceptance items of the issue map to deterministic tests; no sleeps,
no large data, no performance claims.

### UT (focused component-level; `GOWORK=off go test`)

1. `pkg/sql/colexec/window/indicators_test.go` — golden vectors computed by
   an *independent naive reference implementation inside the test* (no
   shared code with kernels) for every kernel: recurrence correctness,
   seeding, exact warmup NULL bits, NULL-skip, partition reset, `n = 1`
   and `n = 2` boundaries, KAMA flat-input (`vol = 0`) edge, the issue
   fixture (drawdown–frame entry/exit rotation of the peak), monotone and
   descending sequences, random fuzz vs reference (fixed seeds).
   `mmaxDrawdown` additionally pins the review-driven boundaries with
   exact expected outputs:
   - value domain: `[100, 0]` -> `1.0` at the zero-trough row;
     `[100, -20]` -> `1.2` and `[1, -10]` -> `11` (negative troughs
     under a positive peak; `[1, -10]` is the r5 counterexample to the
     withdrawn `|1 - v/peak| <= 2` bound - the domain is unbounded
     above); frames
     `[0, 0]`, `[0, -5]`, `[-10, -20]` -> NULL (ratio regime undefined);
     no case may produce a division-by-zero error (the `peak > 0` guard is
     asserted by the all-nonpositive cases);
   - NULL axes: `n = 3` over `[100, 90, NULL]` -> NULL, NULL, NULL (own-row
     rule outranks a full frame); `[100, 90, NULL, 95]` -> NULL, NULL,
     NULL, `0.0` (interior NULL); `[NULL, NULL, 100, 95]` -> NULL, NULL,
     `0.0`, `0.05` (leading NULLs do not delay physical warmup);
     trailing NULL rows -> NULL rows;
   - singleton degenerate (r2 review finding): `n = 1` over
     `[NULL, -1, 0, 1]` -> `NULL, NULL, NULL, 0` (the `n = 1` zero output
     is restricted to positive peaks); positive singletons -> `0`;
   - parameter bound: `n = 1024` accepted, `n = 1025` rejected with the
     section-4 diagnostic at bind time (binder UT below executes the same
     boundary).
   Numerical-stability vectors (r2 + r3 review findings; section 3.1
   stability contract), each recomputed by an in-test higher-precision
   oracle (`math/big` floats) AND asserted against the derived invariants
   (`er <= 1 + 2^-50` — withdrawn r3 bound `1+2e-16` is NOT asserted; `sc`
   within `[(2/31)^2, (2/3)^2]` with the same slack; output inside the
   convex-combination range with the same slack):
   - large-to-small transition (r2-review vector): KAMA `n = 2` over
     `[0, 1e16, 0, 1, 2]` -> row 5 `vol = 2` (exact), `er = 1`,
     `sc = 4/9`, output ≈ `3.074e15`; the subtractive running-sum variant
     computes `vol = 1`, `er = 2`, `sc ≈ 1.6099`, output ≈
     `−3.37e15` and MUST fail these assertions — the vector is also run
     against that buggy variant in-test as the structural counterexample
     harness (it must not pass);
   - monotone vector and reversal (r3-review vectors): KAMA `n = 4` over
     `[-1e16, 0, 1, 2, 3]` and over `[3, 2, 1, 0, -1e16]` ->
     `er = 1` exactly under the selected Neumaier accumulation, with the
     uncompensated sequential variant of the first yielding the 4-half-ulp
     `er = 1.0000000000000004`, which fails the withdrawn
     `1 + 2e-16` bound-assertion when run as the counterexample harness
     (design-approval advisory correction, verified numerically: the same
     `er` sits inside the revised derived bound `1 + 2^-50`, so the
     derived-bound must-fail harness belongs to the escalation below);
     the reversal's sequential variant resolves to `er = 1` as well;
   - escalation: KAMA `n = 1024` over `[-1e16, 0, 1, ..., 1023]` ->
     `er = 1` exactly (sequential variant: `er = 1.0000000000001024`,
     461 ulps - the derived-bound must-fail counterexample harness);
   - nonflat-to-flat transition: a sequence ending in equal values (e.g.
     `[..., 3, 3, 3, 3]`) -> `vol` exactly `0`, `er = 0`, smooth-carry of
     the prior KAMA value;
   - magnitude-drop windows for `sma`/`wma`/`trima` (1e16-class window
     followed by small values), asserted against the oracle within the
     documented compensated-summation slack.

   Overflow / non-finite / underflow boundary vectors (r3-review
   finding #2 and r4-review findings #9/#10; section 3.1 range
   contract), each with an independent expected
   result or the exact documented error:
   - representable-mean overflow trap: `sma`/`ema`-seed over
     `[1e308, 1e308]` returns `1e308` (a sum-then-divide implementation
     yielding `inf` must fail this test);
   - procedure-M constant-input boundary counterexamples (r4-review
     vectors): an `sma` window of 11 identical `MAXFLOAT` inputs must
     return exactly `MAXFLOAT` - the withdrawn r4 pre-scaling
     mechanism returns `NaN` there (`+Inf` running sum, `-Inf`
     compensation tail) when run as the must-fail counterexample
     harness; a 3-term window of identical minimum subnormals
     `2^-1074` must return exactly `2^-1074` (a representable
     subnormal mean), where the withdrawn mechanism returns `0`
     (`fl(2^-1074 / 3) = 0` term-wise; harness must fail); mixed-sign
     `[MAXFLOAT, -MAXFLOAT] -> 0`; all-zero window -> `+0` and
     all-`-0` window -> `-0` (procedure-M zero-window rule);
     fixed-seed fuzz against the higher-precision oracle asserts the
     tiered accuracy contract (`|computed - exact| <= 2^-40*max|u|`
     for plain means, `<= 2^-51 * (sum_k w_k*|u_k|) + 2^-40*max|u|`
     for weighted sites, including the `n = 1024` profile
     extremes);
   - procedure-U seed-to-update transition vectors (r6; r5-review
     finding #12): EMA `n = 3` and Wilder `n = 2` over constant
     `[h, h, ...]` (`h = 2^-1074`) emit the exact constant at every
     defined row after the procedure-M seed - the direct-update
     mechanism used before r6 collapses the stream to all-zero and
     MUST fail as the counterexample harness; the same kernels over
     constant `3h` carry `3h` (the old mechanism overshoots to
     `4h`); the `a = 0.5` subnormal-grid sweep covers every odd
     constant `X = (2k+1)h >= 3h` (systematic old-mechanism
     failures); KAMA `n = 2` over constant `h` carries `h` end to
     end under U (flat-input `sc = (2/31)^2` regime); the chained
     consumers `dema(3)`, `tema(3)` and `t3(n = 2, v = 0.7)` over
     constant `h` and `3h` emit the exact constant at every defined
     row through all stages (the t3 combination's float coefficient
     sum is `1.0000000000000009` - the pins cover it); constant
     `MAXFLOAT` streams at `ema(5)` (`a = 1/3`, non-dyadic) and
     `wilder(3)` carry exactly (design-approval advisory correction, verified
     numerically: `ema(5)` seeds at the procedure-M value one ulp below
     `MAXFLOAT` - `1.7976931348623155e308` - and procedure U preserves it
     through every update, while `wilder(3)` seeds at `MAXFLOAT` itself);
     a nonconstant 100,000-vector fuzz
     (subnormal, general and MAXFLOAT classes, all four alpha
     families) asserts hull containment, finiteness and the
     corrected U accuracy bound against the oracle;
   - procedure-U normal-range accuracy vectors (r7; r6-review
     finding #13): the reachable 13-row `wilder(x, 12)` stream -
     twelve `1.12` inputs, exact `1.12` procedure-M seed, then
     `0.86` - emits `1.0983333333333332` at row 13, asserted
     against the exact-rational `a = 1/12` oracle under the
     corrected `2^-51*max(|u|,|y|) + 2^-1074` contract (error
     `2.6830389761774615e-16`, `0.54` of the bound); the r6
     `2^-52` constant asserted against the same vector is the
     must-fail counterexample harness, preserving the reviewer's
     falsification in-repo;
   - procedure-U concentrated same-binade scan (r7): grid pairs
     within the same binade (and across the negative and adjacent
     binades) over every reachable coefficient family - `wilder
     1/n`, `ema 2/(n+1)`, KAMA's rational `sc` values - assert
     the corrected bound against exact-rational oracles
     everywhere; the design scan covered `1,880,064` pairs with
     zero violations (worst ratio `0.583`, pinned worst vector
     `a = 1/63`, `u = 1.015625`, `y = 1.0625`, error
     `2.749123680024197e-16`) while the old `2^-52` assertion
     fails on `304` of them - the scan exists because the
     falsification is an alignment effect that the untargeted r6
     fuzz never found; the U oracle harness must take
     `b_hat = fl(1 - a)` of the true rational complement exactly
     (a `fl(1 - a_hat)` variant coincides on these vectors but is
     a trace-level deviation the pins do not certify);
   - non-finite input: `ema` over `[1, +Inf, 1]` and a `NaN` row fail the
     statement with `input value is not finite for ema at row 2` (and no
     partial-output or state-poisoning path is observable);
   - KAMA NaN trap: `n = 2` over `[1e308, -1e308, -1e308]` fails with the
     documented range error before any division (`Inf/Inf` NaN is
     structurally unreachable), and the volatility-overflow variant
     (`n = 4` over alternating `[5e307, -5e307, 5e307, -5e307, ...]`,
     window volatility `4e308` unrepresentable) fails with the same
     documented error instead of saturating;
   - combination-kernel bounds: `dema`/`tema`/`t3` at `max|u|` within
     `MAXFLOAT/L` produce finite values validated by the oracle;
     magnitudes above the bound fail with the documented `MAXFLOAT/L`
     error;
   - subnormal pins: exactly representable subnormal inputs with
     exact expected outputs (gradual underflow preserved; the
     procedure-M min-subnormal mean pin `3 x 2^-1074 -> 2^-1074` is
     the boundary case); `cumPositiveStreak`
     over `[-0.0, +0.0, 1]` -> `[0, 0, 1]` (signed-zero predicate);
   - procedure-M underflow-gap and subnormal-grid boundary vectors
     (r6; r5-review finding #11): with `m = 2^-1074`, an `n = 2`
     window over `[0, m]` returns exactly `0` - the exact mean
     `m/2` is the tie midpoint and nearest-even is `0` - and the
     asserted error equals the floor allowance `m/2` (the r5
     unaugmented bound `2^-1114` sat `2^39` below the minimum
     possible float error, so this vector is what proves the
     augmentation); neighbors `[m, m] -> m`, `[m, 2m] -> 2m` (tie
     to the even multiple), `[0, 3m] -> 2m`, `[m, -0.0] -> +0`;
     weighted neighbors `[0, m]` with `wma` weights `1, 2 -> m`
     (nearest to `2m/3`) and `[m, m]` with weights `1, 3 -> m`;
     the bottom-seeded oracle fuzz (subnormal grid with
     half-grid-mean targeting, tiny normals, deep-subnormal
     clusters, mixed magnitudes; plain and weighted shapes) asserts
     the floor-augmented tier contract;
   - extreme-finite drawdown (r5 vectors): `mmaxDrawdown` over
     `[1e308, -1e308]` returns the ratio `2.0` at the trough row,
     asserted exactly (the r1-review >1 domain); asymmetric-magnitude
     frames: `[1, -10]` -> `11` and `[1e-308, -1]` -> `1e308`
     (representable ratios of any magnitude are emitted as values),
     `[1e-308, -1e308]` fails the statement with `drawdown ratio is
     not finite for mmaxdrawdown at row 2` (the quotient
     `-1e308/1e-308` overflows to `-Inf`; the withdrawn r4 bound
     made silent `Infinity` possible here), and
     `[MAXFLOAT/4, -MAXFLOAT]` -> exactly `5.0` (finite quotient,
     representable difference - a value, not an error;
     design-approval advisory correction, verified numerically: the exact
     rational `1 - (-MAXFLOAT)/(MAXFLOAT/4)` is `1 + 4 = 5`, and
     `MAXFLOAT/4` is exactly representable, so the fixture must use
     `MAXFLOAT/4` - the earlier `[4.49e307, -MAXFLOAT]` vector rounds
     to `5.003770901697807` instead); tiny-positive-peak frames:
     `[1e-308, -1e-308]` -> `2.0` and `[5e-324, -5e-324]` -> `2.0`
     (lowest-normal and minimum-subnormal peaks; exact `2.0` at the
     smallest scales) - every vector is asserted against the
     higher-precision oracle with its independent expected value or
     the exact documented error;
2. `pkg/sql/colexec/window/window_test.go` — operator-level: chunked emit
   across 8192-row boundaries (prefix reuse, no recomputation), LIMIT
   early exit, partition switching resets state, empty partition, all-NULL
   partitions, cancellation error propagation, allocation-accounting
   reuse/free pairs. Review-driven lifecycle additions:
   - *generation isolation:* run the same materialized operator twice with
     different input batches (re-entering `eval`), asserting the second
     result equals a fresh computation of the second input only (a stale
     first-generation prefix must be impossible under both `receiveAll`
     and partitioned receive flows);
   - *failure/cancel-then-reuse:* fail (or cancel) one generation mid-emit,
     then drive another generation and assert correct results and no
     accounting leak (the failed generation frees its partial prefix via
     the `eval`/`Reset` discipline of section 5.2);
   - *Reset/Free coverage:* after `Window.Reset` and after `Window.Free`,
     no indicator prefix or allocation remains (checked through the
     container's accounting in `Free`, mirroring the existing
     freeRunningAgg assertions' placement);
   - *cancellation latency:* cancel during a `mmaxDrawdown` frame scan on a
     large-frame partition and assert the error surfaces after at most the
     documented cadence (one emitted-row check or one 64-element inner
     poll), not after the partition completes (section 8 budget);
   - *capacity probe (evidence, not a gate):* one recorded `go test -bench`
     run of the `mmaxDrawdown` path at `n = 1024`, `p = 65,536` rows
     reporting ns/op is captured in the PR evidence as the budget
     conformance measurement, together with one finite-sum kernel
     (`sma` at the same shape) for the section-8 class split; no CI
     pass/fail threshold on it.
3. Registration/binder UT (`pkg/sql/plan/function` suite pattern +
   existing window binder tests): 11 entries resolvable, class/flag bits
   (`isWindow`, `WIN_VALUE` not aggregate/order/null-deduce), argument
   count/type rejection, `n` const-domain validation including the
   `n = 1024`/`n = 1025` boundary and the exact section-4 diagnostic text,
   `ORDER BY`-required replication, explicit-frame rejection, OVER-less
   diagnostic, and
   `DeduceNotNullable` = false for warmup kernels / true for streak.

### BVT (SQL-level; in-repo suite `test/distributed/cases/function/`)

New case file `func_indicator_kernels.sql` + expected `.result`
(following the in-repo `func_window_nested` pattern), covering:

- **The issue's exact reproduction, red/green:** the four entry probes
  (`SELECT ema(CAST(1 AS DOUBLE),3);` style) fail on the base with error
  20105 (recorded as the base observation; kept as handler evidence) and
  after the change produce the OVER-clause diagnostic — availability is
  asserted *separately* from math, per acceptance.
- **The verified fixture:** the `100,120,90,110` cumulative-drawdown
  control asserting complete values `0,0,0.25,0.25`, row alignment and
  NULL bits; `mmaxDrawdown` complement asserting frame-local peaks with
  frame-entry/exit and a peak leaving the frame cannot keep the value
  elevated (counterexample: bounded ≠ all-history).
- Profile coverage per kernel (a compact partition with partitions,
  all-NULL rows, empty partitions, ties in a separate case with a
  documented determinism note, integer/float/null type boundary rows,
  `n = 1` degenerate rows). For `mmaxDrawdown` the BVT mirrors the UT
  boundary matrix through SQL: the `[100,0] -> 1.0`, `[100,-20] -> 1.2`,
  all-nonpositive-frames -> NULL value-domain rows, the
  `[100,90,NULL]`/`[100,90,NULL,95]`/`[NULL,NULL,100,95]` NULL-axis
  sequences with complete NULL bits, the singleton pin
  `n = 1` over `[NULL, -1, 0, 1]` -> `NULL, NULL, NULL, 0`, and the
  `n = 1025` rejection with its exact error text. The 1e16-magnitude
  KAMA stability vector and the small-value KAMA smoothness case stay in
  the UT (in-process determinism; huge-magnitude float rendering in
  result files is brittle), and the BVT keeps one small-magnitude KAMA
  row set exercising the same monotone/nonflat-to-flat transitions.
- The SQL migration recipes of section 3.2 asserted with exact values.

### Execution evidence record

- Base-clone reproduction: focused probes on the base binary (20105 class)
  + working control, this turn's logs retained under the handler record.
- Post-change: the same probe scripts against the fixed binary; focused
  UT packages green; the new BVT file executed through the shared
  `mo-tester` runner with an isolated report directory owned by this
  issue (mo-tester repository and shared configuration untouched).
- Broader-but-deferred: full `test/distributed` function suite runs (and
  mo-tester's full window matrix) remain CI's verification; the focused
  proof above is the discriminating evidence (per validation-evidence
  cost ladder).

## 10. Risks and open decisions (each resolved or owned)

| item | disposition |
|---|---|
| Sort ties: outputs depend on the concrete order of tied ORDER BY keys | Documented (deterministic within a plan; tests use unique keys; a ties case documents the contract instead of asserting folklore) |
| TA-Lib byte parity of warmup positions (e.g., published lookback folklore) | Not claimed; every profile's warmup is defined in closed form in section 3 and pinned by UT/BVT goldens |
| KAMA/T3 tunables (vfactor, fast/slow) | v1 constants fixed (0.7; 2/30) and documented; optionality needs an options-argument protocol with no builtin precedent — non-blocking follow-up with owner "feature roadmap", not this PR |
| `t3` name collision risk in future MySQL reserved words | Additive builtin namespace; acceptable; registration verified unique |
| Nullability misuse by optimizer rules | UT pins `DeduceNotNullable` behavior for all eleven IDs |
| Frame-rejection breaking user UX vs silent ignore | Decision: reject with actionable message (recorded in design decision log); recipes point users at `sma`/parameter semantics |
| `mmaxDrawdown` O(p·n) worst case (review finding #4) | Cap revoked from `n <= 100000` to uniform `n <= 1024` with an explicit accepted budget (`<= 1024*p` frame-element visits per partition, section 8), in-scan cancellation cadence, and a capacity probe recorded in the PR evidence; deque/staircase refinement recorded with its decision point (first workload demanding p > 10^5 at n = 1024) |
| `mmaxDrawdown` value domain and zero-divisor regimes (review finding #1) | Closed: `peak > 0` guard precedes every division; all-nonpositive frames emit NULL (no error, no division by zero); outputs on nonnegative series are exactly `[0,1]` (reachable 1.0) and may exceed 1 on negative troughs after positive peaks (documented, not clamped); ordered-pairs equivalence restricted to its nonnegative precondition |
| Drawdown NULL-axis consistency (review finding #2) | Closed: family defaults vs profile overrides made explicit; physical-row frame/warmup/eviction axis with a per-row NULL-emission rule; leading/interior/trailing NULL and warmup cases pinned with exact outputs in sections 3.1 and 9 |
| Indicator-state lifetime across reuse/failure (review finding #3) | Closed: generation = one `eval` entry; prefix freed at `eval` entry, `Window.Reset`, and `Window.Free` beside the existing `freeRunningAgg` discipline; failure atomicity defined; lifecycle UTs added in section 9 |
| Singleton drawdown vs positive-peak guard (r2 review finding #1) | Closed: the `n = 1` zero output restricted to positive singleton peaks (`[NULL,-1,0,1]` -> `NULL,NULL,NULL,0`), removing the r2 contradiction that admitted an incorrect n=1 fast path; pinned in UT and BVT |
| KAMA rolling-volatility numerical stability (r2 review finding #2) | Closed: subtractive running aggregates forbidden family-wide; finite sums (sma/wma/trima/KAMA vol) computed as one fresh per-row pass with Neumaier compensation (promoted to the selected method after the r3 review falsified the uncompensated 2-ulp bound) inside the existing `1024*p` budget; derived assertion er <= 1+2^-50 with er = 1 exactly on every review vector; runaway vector kept as the structural counterexample |
| Fresh-summation error bound consistency (r3 review finding #1) | Closed: the withdrawn er <= 1+2e-16 assertion is replaced by the derived 1+2^-50 bound under Neumaier accumulation (reviewer's n=4 vector: 4-half-ulp excess under plain sequential summation, reproduced exactly; n=1024: 461 ulps, reproduced; both resolve to er = 1 exactly under the selected method); monotone vectors and reversals pinned against a higher-precision in-test oracle; er is never clamped |
| Overflow/non-finite/underflow semantics (r3 review finding #2; r4-review findings #9/#10 corrected its r4 closure) | Closed: finite-only input admission with a documented execution error (no recurrence poisoning); the r4 scaled-mean closure was falsified and replaced by procedure M (power-of-two normalization makes mean overflow structurally impossible - normalized intermediates stay far inside the finite range, and the [1e308,1e308] case returns 1e308 exactly) with the explicit tiered accuracy contract (plain `2^-40*max|u|`, weighted `2^-51 * (sum_k w_k*|u_k|) + 2^-40*max|u|`); dema/tema/t3 carry checked arithmetic under documented MAXFLOAT/L admission bounds with documented range errors; KAMA diff-span guard prevents Inf/Inf NaN; mmaxDrawdown emits representable ratios of any magnitude and raises a documented range error on non-finite quotients; IEEE gradual underflow retained with exact-representable subnormal pins; extreme/subnormal/non-finite boundary cases added to the validation plan with independent expected results/errors |
| mmaxDrawdown ratio range (r4 review finding #9) | Closed (r5): the withdrawn `|1 - v/peak| <= 2` bound removed from every claim site (falsified by `[1, -10] -> 11` and by the `[1e-308, -1e308]` quotient `-Inf`); the defined domain documented unbounded above on finite inputs; representable ratios emitted as values (`11`, `1e308`, `2.0` pins); the first non-finite quotient/difference raises `drawdown ratio is not finite for mmaxdrawdown at row N`; asymmetric-magnitude and tiny-positive-peak vectors with independent expected values/errors pinned in section 9 |
| Mean/seed scale safety and accuracy (r4 review finding #10) | Closed (r5): the unsound convex-hull/correctly-rounded proof withdrawn (11 x MAXFLOAT -> NaN and 3 x 2^-1074 -> 0 under the r4 mechanism, both reproduced independently); replaced by procedure M - power-of-two normalization of the window by max magnitude, exact scaling, Neumaier accumulation with bounded intermediates, denominator division last, exact power-of-two rescale - with the tiered accuracy contract and exact results pinned on both constant-input boundary vectors, on `2 x 1e308 -> 1e308`, on mixed-sign `[MAXFLOAT, -MAXFLOAT] -> 0`, and on the zero-window signed-zero rules; applied uniformly to sma/wma/trima and every recurrence seed; the withdrawn mechanisms kept as must-fail counterexample harnesses |
| Mean accuracy contract at the underflow gap (r5 review finding #11) | Closed (r6): the T-scaled-only bound was unsatisfiable when the exact mean lies below half the minimum subnormal (`n = 2` over `[0, 2^-1074]`: exact mean `2^-1075`, every float64 result off by at least `2^-1075` while the r5 declaration was `2^-1114` - a `2^39` shortfall; procedure M returns the correctly rounded `0` there); both mean tiers gain the absolute underflow rounding allowance `+2^-1075` (the single output-scale rescale rounding; half a grid unit), with the gap pin, the subnormal-grid neighbor pins and the 800-vector bottom-seeded oracle fuzz added to section 9 |
| Recurrence contract at the subnormal floor (r5 review finding #12) | Closed (r6): the r5 direct-update wording had no verified contract - constant `[h, h, ...]` streams collapse to `0` for ema(3)/wilder(2) after exact seeds, constant `3h` streams overshoot to `4h` above the envelope (systematic for every odd subnormal constant under `a = 0.5`), and the chained dema/tema consumers emit all-zero on constant `h`; replaced by the shared procedure U (endpoints, rounded-coefficient products, compensated two-term sum, structural hull clamp) with the range guarantee delivered by construction (result always in the closed hull; no Inf/NaN on finite admitted inputs), the documented subnormal-floor behavior (constant streams carry exactly; mid-hull gap values may round to `0` inside the hull) and the accuracy bound `2^-52*max(|u|,|y|) + 2^-1074` (two product roundings - double the procedure-M floor; annotated r7: this constant was falsified by r6-review finding #13 and corrected to `2^-51*max + 2^-1074`, see the finding-13 row); seed-to-update, chained-consumer, constant-MAXFLOAT and 100,000-vector fuzz validation added to section 9; a 203,537-coefficient scan found no naive overflow at the top, so the top contract ships with U's clamp and no naive falsifier is claimed |
| Recurrence-update accuracy in the normal range (r6 review finding #13) | Closed (r7): the r6 `2^-52*max(|u|,|y|) + 2^-1074` U bound was falsified by the reachable `wilder(x,12)` 13-row stream (twelve `1.12` inputs seed `1.12` exactly, input `0.86` updates to `1.0983333333333332`, error `2.6830389761774615e-16` vs the old bound `2.486899575160351e-16`, inside the hull so the clamp cannot repair it) and reproduces systematically (`304` violations per `1,880,064` concentrated same-binade pairs); corrected to `2^-51*max(|u|,|y|) + 2^-1074` via the three half-ulp-class contributions over `M' = a*|u| + (1-a)*|y|` (coefficient quantization + both product roundings + the single final sum rounding), composed `3*2^-53*M'*(1+2^-53) < 2^-51*max`; the tightness claim is withdrawn (the bound is asserted by derivation with a four-thirds margin); the 13-row must-fail harness, the pinned scan worst vector (`a = 1/63`, `u = 1.015625`, `y = 1.0625`) and the doc-formula fuzz (`b_hat = fl(1-a)` of the true complement) are added to section 9 |
| Whether to also expose native cumulative drawdown / trend entries | v1: documented recipes only (already expressible); revisit with user feedback |

## 11. Design review status

- Gate classification: triggered (feature; public SQL surface; cross-package;
  size). This document is the required artifact.
- Design review history:
  - r1 (`technical-indicator-window-kernels-2026-10-23-r1`, SHA256
    `89a4cb8a68b78f1c95693440788193a836c227ad8605b4502d8fe32e6902b78c`):
    independent review returned REQUEST_CHANGES with four findings —
    (1) drawdown value domain inconsistent with its formula;
    (2) contradictory drawdown NULL semantics; (3) indicator state
    lifecycle not closed over query reuse/failure; (4) unjustified
    drawdown work/cancellation budget. All four were verified against
    the r1 text and the tree (the reviewer's code citations
    `window.go` receiveAll/eval and `types.go` Reset/Free were
    re-checked by this handler and stand).
  - r2 (`technical-indicator-window-kernels-2026-10-23-r2`, SHA256
    `494ac76dfaecb1fad125b11a512a8fa44f4d421f24d7dea6902054961671830b`):
    the r1 findings were resolved (sections 3.1, 4, 5.2, 8, 9 and the
    decision log); independent review then returned REQUEST_CHANGES with
    two further findings — (5) the r2 `n = 1` degenerate note claimed
    `mmaxDrawdown` was identically zero on single-value frames while the
    regime rules require NULL for nonpositive visible peaks (singleton
    `0` and `-1` had contradictory specified outputs); (6) KAMA's rolling
    volatility had no numerical-stability contract: an ordinary
    subtract-then-add cumulative update loses small diffs once the
    accumulator reaches 1e16-class magnitudes, and the r2 O(1)-running
    state claim rested on that unstable mechanism. Both findings were
    verified against the r2 text and by independent re-derivation of the
    reviewer's arithmetic (both stand exactly: buggy variant yields
    `vol = 1`, `er = 2`, `sc ≈ 1.6099`, KAMA ≈ `−3.37e15` on
    `[0, 1e16, 0, 1, 2]`; fresh summation yields `vol = 2`, `er = 1`,
    KAMA ≈ `3.074e15`).
  - r3 (`technical-indicator-window-kernels-2026-10-23-r3`, SHA256
    `076f78bf5766acfc76b6a75b826baff6d5108ad6774157e580b49a128be8896b`):
    findings (5) and (6) were resolved (section 4 positive-peak singleton
    restriction; section 3.1 numerical-stability contract); independent
    review then returned REQUEST_CHANGES with two further findings —
    (7) the r3 invariants asserted `er <= 1 + 2e-16` while selecting
    *uncompensated* fresh sequential summation; exact telescoping does
    not transfer to such a bound (review vectors: `n = 4` over
    `[-1e16, 0, 1, 2, 3]` gives a 4-half-ulp `er`; the `n = 1024`
    escalation over `[-1e16, 0, 1, ..., 1023]` reaches 461 ulps);
    (8) the contract accepted all numeric inputs and specified
    float64 means/differences/weighted sums without overflow semantics:
    `[1e308, 1e308]` SMA overflowed to infinity although the mean is
    representable, and KAMA over `[1e308, -1e308, -1e308]` produces
    `Inf/Inf` NaN and poisons recurrence state. Both findings were
    verified by this handler with independent float64 re-derivations
    (all four cited numbers reproduce exactly, including the 461-ulp
    escalation and the two overflow traps).
  - r4: finding (7) resolved by promoting Neumaier
    compensation from a follow-up note to the selected in-pass method
    (section 3.1: compensated fresh passes, derived bound
    `er <= 1 + 2^-50` with an eight-fold margin over the tightest
    derivation, `er = 1` exactly on every review vector, monotone and
    reversal vectors plus the `n = 1024` escalation pinned against a
    higher-precision in-test oracle, `er` never clamped, unresolved
    sequential variant kept as the counterexample harness); finding (8)
    resolved by the section 3.1 overflow/underflow/non-finite contract
    (finite-only input admission with a documented execution error,
    scaled compensated means making mean overflow structurally
    impossible (this r4 mechanism was itself falsified by the r4 review;
    r5 replaces it with procedure M - see finding (10) below), checked
    `MAXFLOAT/L` admission for dema/tema/t3 with
    documented range errors, the KAMA diff-span guard making NaN
    unreachable, IEEE gradual underflow with exact-representable
    subnormal pins) and the section-4 execution-time diagnostics, with
    section-9 boundary vectors carrying independent expected
    results/errors.
    The r4 review then returned REQUEST_CHANGES with two further
    findings - (9) the r4 text claimed the `peak > 0` guard bounds
    every drawdown ratio (`|1 - v/peak| <= 2`) and therefore omitted
    any range guard (falsified by `n = 2` over `[1, -10]`, which
    returns `11`, and by `[1e-308, -1e308]`, whose quotient rounds to
    `-Inf` on finite inputs; the planned symmetric `[1e308, -1e308]`
    test as drafted could not expose either case); (10) the r4 mean
    mechanism's safety proof was unsound: pre-scaling overflows its own
    running sum on `n = 11` identical `MAXFLOAT` inputs (`+Inf` sum,
    `-Inf` compensation tail, `NaN` result, although the exact mean is
    the representable `MAXFLOAT`), and pre-scales `n = 3` identical
    minimum subnormals to zero per term (`fl(2^-1074 / 3) = 0`), losing
    the representable mean `2^-1074` before compensation can act - so
    neither the claimed structural overflow-impossibility nor the
    correctly-rounded mean held, and the constant-input boundary
    counterexamples were missing from the plan. Both findings were
    verified by this handler with independent float64 re-derivation
    (every trajectory number reproduces exactly, under both
    pre-scaling realizations).
  - r5 (this revision): finding (9) resolved by removing the false
    bound from every claim site and defining the checked ratio rule -
    representable ratios of any magnitude are emitted as values
    (`[1, -10] -> 11`, `[1e-308, -1] -> 1e308`,
    `[1e308, -1e308] -> 2.0`, `[5e-324, -5e-324] -> 2.0`), and the
    first non-finite quotient or difference fails the statement with
    `drawdown ratio is not finite for mmaxdrawdown at row <partition
    position>` - with the asymmetric-magnitude and tiny-positive-peak
    vectors pinned in section 9; finding (10) resolved by the shared
    procedure M (power-of-two normalization of the window by its max
    magnitude, exact scaling, Neumaier accumulation with bounded
    intermediates, denominator division last, exact power-of-two
    rescale), with the explicit tiered accuracy contract, exact
    results pinned on both constant-input boundary vectors plus the
    `2 x 1e308`, mixed-sign and zero-window pins, uniform application
    to every window mean and recurrence seed, and the withdrawn r4
    mechanisms kept as must-fail counterexample harnesses.
    The r5 review then returned REQUEST_CHANGES with two further
    findings - (11) the r5 mean accuracy contract carried no
    underflow rounding allowance: for `n = 2` over
    `[0, 2^-1074]` the exact mean `2^-1075` lies in the underflow
    gap, so any float64 result - including the correctly rounded
    `0` - has error at least `2^-1075`, while the declared bound at
    that input scale is `2^-1114`; the r5 subnormal pins, restricted
    to exactly representable outputs, left that impossible
    contract untested; (12) the r5 recurrence wording specified
    direct weighted updates while asserting the input-magnitude
    envelope structurally: with `h = 2^-1074`, EMA `n = 3` and
    Wilder `n = 2` compute `fl(0.5*h) + fl(0.5*h) = 0` on
    constant-`h` streams after exact seeds (the representable
    result `h` is lost) while the seeds are protected by procedure
    M - the update step has no procedure - and constant `3h`
    streams update to `4h`, exceeding the envelope. Both findings
    were verified by this handler with independent float64
    re-derivation before revising (every cited number
    reproduces; the verification also found the overshoot
    systematic for every odd subnormal constant under `a = 0.5`
    and the chained `dema(3)`/`tema(3)` consumers collapsing
    all-zero on constant `h` under the old mechanism).
  - r6: finding (11) resolved by adding the
    absolute underflow rounding allowance `+2^-1075` to both
    procedure-M tiers - half the minimum-subnormal spacing, equal
    to the procedure's single output-scale rescale rounding, so a
    gap-class exact mean can satisfy the contract at all -
    justified in section 3.1 and pre-verified by the 800-vector
    bottom-seeded oracle fuzz (zero violations; gap vectors reach
    `1 - 10^-9` of the bound, proving the floor tight) plus the
    `[0, 2^-1074]` gap pin and the subnormal-grid neighbor pins in
    section 9; finding (12) resolved by specifying the shared
    procedure U for every recurrence update step (endpoints,
    rounded-coefficient products, compensated two-term sum,
    structural hull clamp) with the range contract delivered by
    construction rather than counting (constant `h`/`3h`/
    `MAXFLOAT` streams carry exactly; no `Inf`/`NaN` on finite
    admitted inputs), the documented subnormal-floor underflow
    contract, and the tight accuracy bound
    `2^-52*max(|u|,|y|) + 2^-1074` - pre-verified by the
    100,000-vector hull/finiteness/accuracy fuzz (zero
    violations, worst `0.99999` of the bound), the seed-to-update
    and chained-consumer validation set, and the 203,537-coefficient
    top scan (the old mechanism was found overflow-free at the top
    as well: no naive falsifier is claimed anywhere; the top
    contract is U's structural clamp). Annotated r7: the `2^-52`
    accuracy constant asserted in this bullet was falsified by
    r6-review finding #13; the correction is the r7 bullet below.
  - r7 (this revision): r6-review finding #13 - the r6
    procedure-U accuracy contract fails in the normal range - was
    verified before any edit and stands. On the reachable
    `wilder(x, 12)` 13-row stream (twelve `1.12` inputs, exact
    procedure-M seed `1.12`, next input `0.86`) procedure U
    returns `1.0983333333333332`, off the exact-rational
    `a = 1/12` value by `2.6830389761774615e-16`, past the r6
    bound `2.486899575160351e-16`, inside the hull, so the clamp
    cannot repair the bound; the reviewer's secondary sub-claim -
    that the bound also fails with the already-rounded coefficient
    as oracle - did not reproduce in the handler's verification
    (rounded-coefficient-oracle error `2.2287727219350016e-16`,
    within the r6 bound; both b-hat realizations coincide on this
    vector), which does not rescue the r6 contract, since the
    declared bound compares against the true-coefficient model;
    the handler's verification extended the falsification to `304`
    violations per `1,880,064` concentrated same-binade pairs over
    `204` reachable coefficients (worst at `a = 1/63`,
    `u = 1.015625`, `y = 1.0625`, error `2.749123680024197e-16`,
    old bound `2.3592239273284576e-16`) and surfaced that the r6
    pre-check fuzz had implemented `b_hat` as `fl(1 - a_hat)` - a
    trace-level deviation from the specified true-complement
    rounding that coincided on every sampled vector and on the
    falsifier, so no r6 fuzz conclusion is invalidated by it;
    every r7 check runs the exact doc formula. Resolution:
    correct the accuracy contract to `2^-51*max(|u|,|y|) +
    2^-1074` via the three half-ulp-class composition (section
    3.1), withdraw the tightness claim, and add the 13-row
    must-fail harness, the pinned scan worst vector and the
    doc-formula fuzz to section 9; the corrected bound holds the
    falsifier at `0.54` of its value, the whole scan at worst
    `0.583`, and the 100,000-vector doc-formula fuzz at worst
    `0.9926` (floor-governed subnormal class), with zero hull,
    finiteness or accuracy violations.
- Implementation is blocked until this revision passes independent review;
  the implementation PR must link this exact revision and file.
- Decision log: (a) native window-function family over aggexec or UDA
  alternatives (section 6); (b) `WIN_VALUE` class reuse with operator-level
  dispatch (no protobuf change); (c) strict fixed-period warmup with NULL
  emissions, NULL-in→NULL-out, state over non-NULL values as family
  defaults, with `mmaxDrawdown` overriding to physical-row axes (finding
  #2); (d) explicit frame rejection; (e) `ORDER BY` required; (f)
  recipes-only for the already-expressible operators; (g) `DOUBLE`
  kernel domain; (h) ratio regime guarded by `peak > 0` with NULL for
  undefined regimes, `[0,1]` on nonnegative series, >1 documented on
  negative troughs (finding #1), and the same guard limits the `n = 1`
  zero output to positive peaks (r2 finding #5); (i) generation-based
  state lifecycle reset at `eval`/`Reset`/`Free` (finding #3); (j) uniform
  `n ∈ [1,1024]` cap justified by an explicit accepted budget plus
  in-scan cancellation (finding #4); (k) no cross-row subtractive
  aggregates and no uncompensated in-pass summation; finite window sums
  are fresh per-row Neumaier-compensated passes with the derived
  `er <= 1+2^-50` assertion and exactly-1 expected values on the review
  vectors (r2 finding #6, tightened by r3 review finding #7); (l) `trima`
  implemented as the folded single-pass triangular convolution with
  exact rational weights, equal to the documented double-`SMA`
  composition; (m) finite-only input admission, scaled compensated
  means (superseded in r5 by procedure M, see (p)), checked
  `MAXFLOAT/L` bounds for the negative-coefficient
  combination kernels, and the KAMA diff-span guard — with documented
  execution errors for everything outside admission and IEEE gradual
  underflow within it (r3 review finding #8).
; (o) the mmaxDrawdown domain is
  unbounded above on finite inputs, and the first non-finite
  quotient/difference in a frame raises the documented `drawdown
  ratio is not finite` execution error, replacing the falsified
  `|1 - v/peak| <= 2` blanket (r4 review finding #9); (p) every
  mean/weighted-mean/seed site uses procedure M - power-of-two
  normalization of the window by its max magnitude, exact scaling,
  Neumaier accumulation with intermediates far inside the finite
  range, denominator division last, exact power-of-two rescale - with
  the tiered accuracy contract (plain means `2^-40*max|u|`, weighted
  sites `2^-51 * (sum_k w_k*|u_k|) + 2^-40*max|u|`) and exact results
  pinned for the two r4-review constant-input boundary vectors (r4
  review finding #10; the r6 underflow-floor allowance is added by
  decision (q)); (q) both procedure-M accuracy tiers carry the
  absolute underflow rounding allowance `+ 2^-1075` - half the
  minimum-subnormal spacing, equal to the procedure's single
  output-scale rescale rounding, and therefore unavoidable for
  gap means (`[0, 2^-1074]` exact mean `2^-1075`; unaugmented bound
  `2^-1114`, a `2^39` shortfall) - verified by the gap pin, the
  subnormal-grid neighbors and the 800-vector bottom-seeded
  oracle fuzz (r5 review finding #11); (r) every recurrence update
  step uses procedure U - endpoints from `(u, y)`,
  rounded-coefficient products, compensated two-term sum,
  structural hull clamp - delivering the envelope and no-Inf
  guarantees by construction at every scale, the documented
  subnormal-floor behavior (constant streams carry exactly;
  mid-hull gap values may round to `0` inside the hull), and the
  accuracy bound `2^-52*max(|u|,|y|) + 2^-1074` (one grid unit
  for the two product roundings - deliberately double the
  procedure-M half-grid floor, which has only the single
  output-scale rounding; both pre-verified tight; annotated r7:
  this accuracy bound was falsified by r6-review finding #13
  and is corrected by decision (s)) (r5 review finding #12); (s)
  the procedure-U accuracy contract is `2^-51*max(|u|,|y|) +
  2^-1074` - the three half-ulp-class contributions (the
  coefficient-quantization drift, the two product roundings, and
  the single final two-term-add rounding; Fast2Sum's residue is
  exact) compose over `M' = a*|u| + (1-a)*|y|`, at most
  `max(|u|,|y|)`, to `3*2^-53*M'*(1 + 2^-53) < 2^-51*max` -
  asserted by derivation with a four-thirds margin and pinned by
  the reachable `wilder(x, 12)` falsifier (error `0.54` of the
  bound), the same-binade scan worst vector (`0.583`) and the
  100,000-vector doc-formula fuzz (`0.9926`, floor-governed
  subnormal class; zero hull, finiteness or accuracy violations);
  the subnormal floor `2^-1074` is unchanged (products and the
  residual sum are grid multiples at the bottom grid, so the
  floor carries the pair of product roundings); the withdrawn r6
  `2^-52` constant remains in section 9 as the must-fail harness
  (r6 review finding #13).

### Post-approval advisory annotations (r7 PASS; revision identity unchanged)

The independent design review of revision
`technical-indicator-window-kernels-2026-10-24-r7` (SHA256
`a8233922edf26c77c6219ea34e1a2bfeaef7ad362a86c0d3ce4d4e03436fdca3`)
returned PASS with three advisory validation-plan corrections. All three
were verified numerically against exact arithmetic before implementation
and are folded into the section-9 pins and the section-3.1 range
statement in place, marked with "design-approval advisory correction":

1. `ema(5)` over a constant `MAXFLOAT` stream seeds at the procedure-M
   value one ulp below `MAXFLOAT` (`1.7976931348623155e308`), because the
   exact mean of five finite `MAXFLOAT` values is not representable;
   procedure U preserves that seed through every update. `wilder(3)` seeds
   at `MAXFLOAT` itself. The pins assert the seed, not `MAXFLOAT`.
2. The representable-difference drawdown needle is
   `[MAXFLOAT/4, -MAXFLOAT] -> exactly 5.0`; the earlier
   `[4.49e307, -MAXFLOAT]` vector rounds to `5.003770901697807`.
3. The KAMA `n = 4` monotone vector's uncompensated-sequential
   `er = 1.0000000000000004` sits inside the revised derived bound
   `1 + 2^-50` and is the must-fail harness of the withdrawn
   `1 + 2e-16` assertion only; the derived-bound must-fail harness is the
   `n = 1024` escalation (`er = 1.0000000000001024`, 461 ulps).

Implementation note (host determinism, no contract change): the Go
specification permits fusing floating-point operations across statements
on some architectures (arm64/ppc64 emit `FMADD` for `x*y + z`), which
changes procedure U's intermediate rounding sequence on such hosts. Every
product that feeds an addition in the kernels and in the test references
goes through a `//go:noinline` multiply barrier so the specified rounding
sequence of sections 3.1/9 holds on every host, including the pinned
`wilder(x, 12)` falsifier value `1.0983333333333332`.
