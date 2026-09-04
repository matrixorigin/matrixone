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

-- Grouped spill must externalize ordinary group state before applying exact
-- DISTINCT contributions. The sub-10K value is a deterministic group-count
-- threshold; four/three groups cross it without a large fixture.
set @@agg_spill_mem = 2;
select result % 4 as g, count(distinct result) as d
from generate_series(1, 300) g
group by g order by g;

-- A VARCHAR DISTINCT argument exercises the varlen canonical-key path. The
-- SELECT DISTINCT subquery is an independent relational oracle for the count.
select result % 2 as g, count(distinct concat('v', result % 17)) as d
from generate_series(1, 300) g
group by g order by g;
select g, count(*) as d
from (
    select distinct result % 2 as g, concat('v', result % 17) as v
    from generate_series(1, 300) g
) oracle
group by g order by g;

-- Multi-argument DISTINCT skips rows when either argument is NULL and must
-- preserve exact pair equality through spill.
select result % 3 as g,
       count(distinct result % 10,
             case when result % 7 = 0 then null else result % 5 end) as d
from generate_series(1, 300) g
group by g order by g;

-- Ordinary aggregate state and exact-key state share one generic spill/reload
-- lifecycle; none may be duplicated when group rows repeat across leaves.
select result % 4 as g, count(*) as rows, sum(result) as total,
       count(distinct result % 11) as d
from generate_series(1, 300) g
group by g order by g;

-- A normal byte threshold is the nearest control for the debug threshold.
set @@agg_spill_mem = 536870912;
select count(*) from tiny_scalar;
select count(*), count(distinct result)
from generate_series(1, 300) g;
select result % 4 as g, count(distinct result) as d
from generate_series(1, 300) g
group by g order by g;
select result % 2 as g, count(distinct concat('v', result % 17)) as d
from generate_series(1, 300) g
group by g order by g;
select result % 3 as g,
       count(distinct result % 10,
             case when result % 7 = 0 then null else result % 5 end) as d
from generate_series(1, 300) g
group by g order by g;
select result % 4 as g, count(*) as rows, sum(result) as total,
       count(distinct result % 11) as d
from generate_series(1, 300) g
group by g order by g;

set @@agg_spill_mem = 0;
drop database group_h0_spill;
