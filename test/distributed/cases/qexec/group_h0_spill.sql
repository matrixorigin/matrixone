-- @label:bvt

drop database if exists group_h0_spill;
create database group_h0_spill;
use group_h0_spill;

create table tiny_scalar (id int);
insert into tiny_scalar values (1);

-- Regression for issue #26453.
set @@agg_spill_mem = 256;
select count(*) from tiny_scalar;

-- Regression for issue #27698. Keep an ordinary aggregate beside DISTINCT so
-- the planner retains the saved-argument executor path. The 300 exact keys
-- cross the injected H0 threshold and must complete without one resident set.
select count(*), count(distinct result)
from generate_series(1, 300) g;

-- A normal byte threshold is the nearest control for the debug threshold.
set @@agg_spill_mem = 536870912;
select count(*) from tiny_scalar;
select count(*), count(distinct result)
from generate_series(1, 300) g;

set @@agg_spill_mem = 0;
drop database group_h0_spill;
