-- Technical-indicator window kernels (issue #29949).
-- Availability is asserted separately from math (the issue's acceptance
-- wording): the four probed entry names reject with the generic window
-- diagnostic, not a function-not-supported error, and the cumulative-control
-- recipe keeps its verified values.

drop database if exists ind_kernels_bvt;
create database ind_kernels_bvt;
use ind_kernels_bvt;

-- 1. The issue's four entry probes: window diagnostics, not 20105.
select ema(CAST(1 AS DOUBLE),3) result;
select kama(CAST(1 AS DOUBLE),3) result;
select mmaxDrawdown(CAST(1 AS DOUBLE),3) result;
select cumPositiveStreak(CAST(1 AS DOUBLE)) result;

-- 2. Availability binder surface: required ORDER BY, rejected frames and
-- the constant period domain [1, 1024].
drop table if exists ind_kernels_series;
create table ind_kernels_series(id int primary key, g int, x double, xi int);
insert into ind_kernels_series VALUES
  (1,1,100,100),(2,1,120,120),(3,1,90,90),(4,1,110,110),(5,1,80,80),(6,1,70,70),
  (11,2,10,10),(12,2,-20,-20),(13,2,30,30),(14,2,30,30),(15,2,30,30),
  (21,3,10,10),(22,3,20,20),(23,3,30,30),(24,3,30,30),(25,3,30,30),
  (31,4,NULL,NULL),(32,4,NULL,NULL),(33,4,NULL,NULL);

-- Sort ties: outputs depend on the concrete order of tied ORDER BY keys
-- (documented contract, not an assertion of one folklore order). Every
-- ordered series below uses the unique key `id`.

select id, ema(x, 3) over () from ind_kernels_series where g = 1;
select id, ema(x, 3) over (order by id rows between 2 preceding and current row) from ind_kernels_series where g = 1;
select id, ema(x, 1025) over (order by id) from ind_kernels_series where g = 3;
select id, ema(x, 0) over (order by id) from ind_kernels_series where g = 3;
select id, ema(x, 1024) over (order by id) from ind_kernels_series where g = 3;

-- folded constant periods: section 4 admits any expression folding to an
-- integer constant [1, 1024]; each accepted form's output must equal the
-- same kernel with the folded literal period in the profiles below.
select id, sma(x, abs(3)) over (order by id) from ind_kernels_series where g = 1 order by id;
select id, ema(x, 2+1) over (order by id) from ind_kernels_series where g = 3 order by id;
select id, wma(x, CAST(3 AS TINYINT)) over (order by id) from ind_kernels_series where g = 2 order by id;
select id, dema(x, CAST(2 AS UNSIGNED)) over (order by id) from ind_kernels_series where g = 3 order by id;
-- folded values outside the domain, non-integer folds, out-of-range
-- narrowing casts and non-constant periods reject with the pinned text
select id, ema(x, abs(1025)) over (order by id) from ind_kernels_series where g = 1;
select id, ema(x, ABS(1.5)) over (order by id) from ind_kernels_series where g = 1;
select id, ema(x, CAST(3 AS DOUBLE)) over (order by id) from ind_kernels_series where g = 1;
select id, ema(x, CAST(1000 AS TINYINT)) over (order by id) from ind_kernels_series where g = 1;
select id, ema(x, abs(x)) over (order by id) from ind_kernels_series where g = 1;
select id, ema(x, 512*3) over (order by id) from ind_kernels_series where g = 1;

-- shared-evaluator constant periods: any deterministic scalar outside the
-- automatic arithmetic/cast fold set is admitted by its evaluated integer
-- (review case: LENGTH('abc') and GREATEST(2,3)); non-constant scalar
-- inputs and evaluated values outside the domain reject with the pinned
-- text
select id, ema(x, LENGTH('abc')) over (order by id) from ind_kernels_series where g = 1 order by id;
select id, wma(x, GREATEST(2,3)) over (order by id) from ind_kernels_series where g = 2 order by id;
select id, ema(x, LENGTH(x)) over (order by id) from ind_kernels_series where g = 1;
select id, ema(x, GREATEST(2000,3)) over (order by id) from ind_kernels_series where g = 1;

-- 3. Per-kernel profiles with partitions and/reset:
--    g=1 the issue price fixture, g=2 mixed signs, g=3 nonflat-to-flat
--    (KAMA zero-volatility carry), g=4 an all-NULL partition.
select id, sma(x, 3) over (partition by g order by id) from ind_kernels_series order by g, id;
select id, wma(x, 3) over (partition by g order by id) from ind_kernels_series order by g, id;
select id, trima(x, 4) over (partition by g order by id) from ind_kernels_series order by g, id;
select id, ema(x, 3) over (partition by g order by id) from ind_kernels_series order by g, id;
select id, wilder(x, 3) over (partition by g order by id) from ind_kernels_series order by g, id;
select id, kama(x, 3) over (partition by g order by id) from ind_kernels_series order by g, id;
select id, dema(x, 3) over (partition by g order by id) from ind_kernels_series order by g, id;
select id, tema(x, 3) over (partition by g order by id) from ind_kernels_series order by g, id;
select id, t3(x, 2) over (partition by g order by id) from ind_kernels_series order by g, id;

-- chain warmup through a defined value: with the six-row partitions above,
-- tema(x, 3) (first defined at m = 3n-2 = 7 values) and t3(x, 2) (m = 6n-5
-- = 7) stay all-warmup NULLs by construction, so a dedicated eight-row
-- series (the issue price fixture extended) pins each chain's first
-- defined value and its subsequent update through SQL.
drop table if exists ind_kernels_chain;
create table ind_kernels_chain(id int primary key, x double);
insert into ind_kernels_chain values (1,100),(2,120),(3,90),(4,110),(5,80),(6,70),(7,95),(8,85);
select id, x, tema(x, 3) over (order by id) from ind_kernels_chain order by id;
select id, x, t3(x, 2) over (order by id) from ind_kernels_chain order by id;

-- integer input and integer/double boundaries
select id, cumPositiveStreak(xi) over (partition by g order by id) from ind_kernels_series order by g, id;
select id, cumPositiveStreak(x) over (partition by g order by id) from ind_kernels_series order by g, id;

-- constant literal x: every row of the series reads the one physical
-- element (const profile with the regular warmup NULLs)
select id, sma(CAST(2 AS DOUBLE), 3) over (order by id) from ind_kernels_series where g = 1 order by id;
select id, ema(CAST(2 AS DOUBLE), 2) over (order by id) from ind_kernels_series where g = 1 order by id;
select id, mmaxDrawdown(CAST(2 AS DOUBLE), 3) over (order by id) from ind_kernels_series where g = 1 order by id;
select id, cumPositiveStreak(CAST(2 AS DOUBLE)) over (order by id) from ind_kernels_series where g = 1 order by id;

-- n = 1 degenerate pass-through rows
select id, x, sma(x, 1) over (partition by g order by id), ema(x, 1) over (partition by g order by id), wilder(x, 1) over (partition by g order by id) from ind_kernels_series where g = 1 order by id;
select id, x, dema(x, 1) over (order by id), tema(x, 1) over (order by id) from ind_kernels_series where g = 3 order by id;

-- 4. mmaxDrawdown: frame-local peaks - the issue fixture, frame entry/exit.
select id, x, mmaxDrawdown(x, 4) over (order by id) from ind_kernels_series where g = 1 order by id;
select id, x, mmaxDrawdown(x, 3) over (order by id) from ind_kernels_series where g = 1 order by id;
-- a peak rotating out of the frame cannot keep the ratio elevated
select id, x, mmaxDrawdown(x, 2) over (order by id) from ind_kernels_series where g = 1 order by id;

-- value-domain rows: zero trough -> 1.0, negative troughs under positive
-- peaks -> >1, all-nonpositive frames -> NULL (no error, no division).
drop table if exists ind_kernels_dd;
create table ind_kernels_dd(case_id int, ord int, v double);
insert into ind_kernels_dd values (1,1,100),(1,2,0),(2,1,100),(2,2,-20),(3,1,1),(3,2,-10),
 (4,1,0),(4,2,0),(5,1,0),(5,2,-5),(6,1,-10),(6,2,-20),
 (7,1,100),(7,2,90),(7,3,NULL),(8,1,100),(8,2,90),(8,3,NULL),(8,4,95),
 (9,1,NULL),(9,2,NULL),(9,3,100),(9,4,95);
select case_id, ord, mmaxDrawdown(v, 3) over (partition by case_id order by ord) from ind_kernels_dd order by case_id, ord;
-- period 2 leaves the two-row value-domain cases warm only in their first
-- row, so the defined ratios of design section 9 - [100,0] -> 1,
-- [100,-20] -> 1.2, [1,-10] -> 11 - are asserted at the trough row (the
-- all-warmup NULL bits of the short cases and the period-3 NULL-axis
-- profiles stay pinned by the pass above).
select case_id, ord, mmaxDrawdown(v, 2) over (partition by case_id order by ord) from ind_kernels_dd order by case_id, ord;
-- singleton frames (n = 1): NULL on nonpositive peaks and NULL values
select case_id, ord, mmaxDrawdown(v, 1) over (partition by case_id order by ord) from ind_kernels_dd where case_id in (4,5,6,9) order by case_id, ord;
-- small-magnitude KAMA transitions (monotone and nonflat-to-flat) through SQL
select id, kama(x, 2) over (order by id), kama(x, 4) over (order by id) from ind_kernels_series where g = 3 order by id;

-- non-finite execution errors: documented invalid input, never silent Inf
select id, mmaxDrawdown(x, 2) over (order by id) from (select 1 id, CAST(1e-308 AS DOUBLE) x union all select 2, CAST(-1e308 AS DOUBLE)) t order by id;
select id, dema(x, 2) over (order by id) from (select 1 id, CAST(1.7976931348623157e308 AS DOUBLE) x union all select 2, CAST(1 AS DOUBLE)) t order by id;

-- 5. Documented recipes (section 3.2): verified values, kept checked-in.
-- cumulative maximum drawdown control: 0, 0, 0.25, 0.25
WITH a AS (SELECT id,price,MAX(price) OVER(ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) peak
FROM (
  SELECT 1 id,CAST(100 AS DOUBLE) price
  UNION ALL SELECT 2,120
  UNION ALL SELECT 3,90
  UNION ALL SELECT 4,110) fixture) SELECT id,MAX(CAST(1 AS DOUBLE)-price/peak) OVER(ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) result FROM a ORDER BY id;
-- bounded-frame SMA recipe: partial frames on the leading rows (ordinary-SQL
-- profile; sma(x, 3) emits strict warmup instead)
select id, x, avg(x) over (order by id rows between 2 preceding and current row) from ind_kernels_series where g = 1 order by id;
-- local trend / rolling share of up-ticks (section 3.2): the lag-composed
-- sign is materialized in a derived table - no window function may nest
-- inside another window's argument list
select id, x, x / lag(x, 3) over (order by id) - 1 pct_change_3 from ind_kernels_series where g = 1 order by id;
select id, x, avg(up) over (order by id rows between 9 preceding and current row) up_share_10
from (select id, x, case when x > lag(x) over (order by id) then 1 else 0 end up
      from ind_kernels_series where g = 1) t order by id;
-- bounded positive streak: native streak capped by an expression
select id, x, least(cumPositiveStreak(x) over (order by id), 2) streak_max_2 from ind_kernels_series where g = 1 order by id;

drop database ind_kernels_bvt;
