-- @label:bvt
-- Regression for #28944: allocation-accounted ordered percentiles must honor
-- agg_spill_mem, including the terminal Group -> MergeGroup handoff.
drop database if exists ordered_percentile_spill;
create database ordered_percentile_spill;
use ordered_percentile_spill;
create table t(v bigint);
insert into t select result from generate_series(1, 20000) g;
insert into t values (null);
set @percentile_saved_dop = @@max_dop;
set @percentile_saved_spill = @@agg_spill_mem;
set @@max_dop = 1;
set @@agg_spill_mem = 65536;
select percentile_cont(0.95) within group(order by v) as pc,
       percentile_disc(0.95) within group(order by v) as pd from t;
-- A resident control verifies interpolation and discrete rank are unchanged.
set @@agg_spill_mem = 536870912;
select percentile_cont(0.95) within group(order by v) as pc,
       percentile_disc(0.95) within group(order by v) as pd from t;
set @@agg_spill_mem = 65536;
-- @regex("SpillRows=[1-9][0-9]*", true)
-- @regex("SpillSize=[1-9][0-9.]* [KMGT]iB", true)
explain (analyze true)
select percentile_cont(0.95) within group(order by v),
       percentile_disc(0.95) within group(order by v) from t;
-- Parallel input, direction, endpoints and all-NULL groups use the same state.
set @@max_dop = 4;
select percentile_cont(0.5) within group(order by v) as pc,
       percentile_disc(0.95) within group(order by v desc) as pd_desc,
       percentile_cont(0) within group(order by v) as first_v,
       percentile_disc(1) within group(order by v) as last_v from t;
select percentile_cont(0.5) within group(order by v) as pc,
       percentile_disc(0.5) within group(order by v) as pd from t where v is null;
set @@max_dop = @percentile_saved_dop;
set @@agg_spill_mem = @percentile_saved_spill;
drop database ordered_percentile_spill;
