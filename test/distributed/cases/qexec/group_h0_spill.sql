-- @label:bvt

drop database if exists group_h0_spill;
create database group_h0_spill;
use group_h0_spill;

create table tiny_scalar (id int);
insert into tiny_scalar values (1);

-- Regression for issue #26453.
set @@agg_spill_mem = 256;
select count(*) from tiny_scalar;

-- A normal byte threshold is the nearest control for the debug threshold.
set @@agg_spill_mem = 536870912;
select count(*) from tiny_scalar;

set @@agg_spill_mem = 0;
drop database group_h0_spill;
