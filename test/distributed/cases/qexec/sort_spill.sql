-- @label:bvt

drop database if exists qesortspill;
create database qesortspill;
use qesortspill;

drop table if exists t1;
create table t1(id int, grp int);
insert into t1 values (1,40),(2,10),(3,20),(4,10),(5,30),(6,20);
insert into t1
select result + 100, result % 50
from generate_series(1, 200000) g;

show variables like 'sort_spill_mem';
set @@sort_spill_mem = 1;
show variables like 'sort_spill_mem';

set @@max_dop = 1;

select id, grp from t1 where id <= 6 order by grp, id;
-- @ignore:0
-- @regex("SpillSize=", true)
explain (analyze true, check '["Sort", "SpillSize=", "Sort Key: t1.grp INTERNAL, t1.id INTERNAL"]')
select id, grp from t1 order by grp, id;

set @@max_dop = 0;
set @@sort_spill_mem = 0;
show variables like 'sort_spill_mem';

drop table if exists t1;
drop database if exists qesortspill;
