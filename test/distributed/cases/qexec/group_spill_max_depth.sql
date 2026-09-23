-- @label:bvt

-- A policy threshold below Group's fixed resident allocation floor must not
-- turn an admitted terminal spill leaf into a maximum-depth error.
set @group_spill_max_depth_old_dop = @@max_dop;
set @group_spill_max_depth_old_mem = @@agg_spill_mem;
set @@max_dop = 1;
set @@agg_spill_mem = 1;
select result % 4 as g, count(*) as n
from generate_series(1, 64) s
group by g order by g;

-- Exercise the byte-threshold form used by issue #27886.
set @@agg_spill_mem = 65536;
select result % 4 as g, count(*) as n
from generate_series(1, 64) s
group by g order by g;

-- Resident control for the same SQL result.
set @@agg_spill_mem = 1073741824;
select result % 4 as g, count(*) as n
from generate_series(1, 64) s
group by g order by g;

set @@agg_spill_mem = @group_spill_max_depth_old_mem;
set @@max_dop = @group_spill_max_depth_old_dop;
