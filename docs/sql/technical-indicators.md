# Technical indicator and rolling risk functions

MatrixOne provides eleven window-function kernels for smoothed indicators,
rolling risk and streak analytics (`ema`, `wilder`, `sma`, `wma`, `trima`,
`kama`, `dema`, `tema`, `t3`, `mmaxDrawdown`, `cumPositiveStreak`), plus
documented SQL recipes for the profiles that ordinary SQL already expresses.

Entry-name availability and mathematical rewrites are two different things
(the issue for this feature separates them explicitly):

- **Native entries** resolve as window functions and compute a defined
  profile natively, per partition.
- **Recipes** below are pure-SQL equivalents for the profiles that ordinary
  window composition already expresses on any MatrixOne version. Use them
  when you want the ordinary-SQL semantics (or must run on a version without
  the native entries).

## SQL surface

```sql
-- period kernels: ten (x, n) entries
sma(price, 3) OVER (ORDER BY ts)
-- single-argument streak
cumPositiveStreak(price) OVER (ORDER BY ts)
```

- `OVER (...)` is required; a call without `OVER` reports the ordinary
  window-function diagnostic instead of a not-supported error.
- `ORDER BY` inside `OVER` is required - series order is part of each
  operator's mathematical definition.
- Explicit `ROWS`/`RANGE`/`GROUPS` frames are rejected: the period parameter
  defines the extent (`window frame is not applicable to <fn>: the period is
  given by <fn>'s parameter`).
- `PARTITION BY` is optional and resets every kernel's state at each
  boundary.
- The period `n` must be a constant integer in `[1, 1024]`; `n = 1` is legal
  (pass-through profiles; see the drawdown regime below).
- Inputs are numeric and converted to IEEE-754 `float64` at evaluation;
  each kernel returns `DOUBLE` (except `cumPositiveStreak`, which returns
  `BIGINT`).

## Semantics defaults

- **Value axis (`sma` ... `t3`):** rows measure consumed non-NULL observations.
  NULL observations emit NULL and are skipped by the recurrence; warmup emits
  NULL until the kernel's first defined value (`m = n` for the seeded means,
  `m = n+1` for `kama`, `m = 2n-1 / 3n-2 / 6n-5` for `dema`/`tema`/`t3`).
  State resets per partition.
- **`mmaxDrawdown`** measures physical rows instead (like a `ROWS` frame):
  frame membership, warmup and peak eviction count rows, NULL rows occupy
  frame slots but contribute no peaks, and a row whose own observation is
  NULL emits NULL. The output at row `i` is the maximum drawdown ratio
  `1 - v/peak` over the bounded frame `ROWS BETWEEN n-1 PRECEDING AND
  CURRENT ROW`, using the *frame-local* running peak with the `peak > 0`
  guard before every division; frames with no positive visible peak emit
  NULL (no error). Ratios are nonnegative whenever defined and may exceed 1
  on negative troughs below a positive frame peak (documented, not clamped;
  e.g. `[100, -20] -> 1.2`, `[1, -10] -> 11`).
- **`cumPositiveStreak(x)`** is dense (no warmup NULLs): `v > 0` extends the
  run of positive observations, `v <= 0` and NULL observations reset it to 0.

Numerical contract in brief: windowed finite sums are computed by a fresh
compensated (Neumaier) pass per row; recurrence updates are convex, stay
inside the input envelope, and carry constant streams exactly; inputs that
are not finite (or magnitudes outside the kernel's representable envelope,
e.g. above `MAXFLOAT/3` for `dema`) fail the statement with a documented
`invalid input` error instead of silently emitting `±Infinity`/`NaN`.

## Kernels

| entry | first defined | after warmup |
|---|---|---|
| `sma(x, n)` | `m = n` | mean of the last `n` non-NULL values |
| `wma(x, n)` | `m = n` | linear weight `1..n` over the block, denominator `n(n+1)/2` |
| `trima(x, n)` | `m = n` | double-SMA composition: odd `n` two `SMA((n+1)/2)` passes, even `n` `SMA(n/2)` then `SMA(n/2+1)` |
| `ema(x, n)` | `m = n` (seed: mean of first `n`) | `y = a*u + (1-a)*y`, `a = 2/(n+1)` |
| `wilder(x, n)` | `m = n` (seed: mean of first `n`) | `a = 1/n` (Wilder smoothing) |
| `kama(x, n)` | `m = n+1` (seed `y := u_n` at `m = n`) | efficiency-ratio smoothing, Turtle constants (fast 2, slow 30) |
| `dema(x, n)` | `m = 2n-1` | `2*e1 - e2` over two EMA stages |
| `tema(x, n)` | `m = 3n-2` | `3*e1 - 3*e2 + e3` over three EMA stages |
| `t3(x, n)` | `m = 6n-5` | Tillson generalized DEMA, `vfactor = 0.7` |
| `mmaxDrawdown(x, n)` | frame has `n` physical rows and a positive peak | bounded-frame maximum drawdown ratio |
| `cumPositiveStreak(x)` | dense from row 1 | run length of positive observations |

## Documented migration recipes

The recipes below run on base SQL window semantics (verified against a
pre-feature build) and are kept checked-in under
`test/distributed/cases/function/func_indicator_kernels.sql` so they cannot
rot.

### 1. Cumulative maximum drawdown ratio (the issue's control)

```sql
WITH s AS (
  SELECT id, price,
         MAX(price) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) peak
  FROM (SELECT 1 id, CAST(100 AS DOUBLE) price
        UNION ALL SELECT 2, 120
        UNION ALL SELECT 3, 90
        UNION ALL SELECT 4, 110) f)
SELECT id, MAX(CAST(1 AS DOUBLE) - price/peak)
         OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) dd
FROM s ORDER BY id;
-- dd: 0, 0, 0.25, 0.25
```

The cumulative profile keeps the all-history peak; use the native
`mmaxDrawdown(x, n)` for a *bounded frame* whose peak leaves the frame.

### 2. Simple moving average (ordinary-SQL profile)

```sql
SELECT id, AVG(x) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) sma3
FROM series ORDER BY id;
```

Ordinary SQL averages partial frames - the first `n-1` rows average fewer
values (the MySQL-style profile). The native `sma(x, n)` emits strict warmup
NULLs instead; choose per portability needs.

### 3. Local trend / rolling percentage change

```sql
SELECT id, x / LAG(x, 3) OVER (ORDER BY id) - 1 pct_change_3 FROM series ORDER BY id;

SELECT id, AVG(up) OVER (ORDER BY id ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) up_share_10
FROM (SELECT id, x,
             CASE WHEN x > LAG(x) OVER (ORDER BY id) THEN 1 ELSE 0 END up
      FROM series) t
ORDER BY id;
```

The up-tick sign is composed with `LAG` in a derived table: MatrixOne (like
the ordinary-SQL envelope it follows here) does not accept one window
function nested inside another window's argument list.

### 4. Bounded positive streak

```sql
-- native (dense); cap with an expression for a bounded window:
SELECT id, LEAST(cumPositiveStreak(x) OVER (ORDER BY id), 5) streak_max_5
FROM series ORDER BY id;
```

`cumPositiveStreak` counts consecutive strictly positive observations on the
value axis; cap it in a query-level expression to emulate a bounded window
streak.
