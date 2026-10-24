--- @metacmp(true)
-- Total derived grouping keys preserve partitions, outputs and diagnostics.
drop database if exists group_by_derived_keys;
create database group_by_derived_keys;
use group_by_derived_keys;
create table t(x integer, y smallint, b bigint);
insert into t values
(null,1,0),(null,1,0),(-2147483648,2,-9223372036854775808),
(2147483647,2,9223372036854775807),(0,null,1),(0,null,1),(3,4,3),(3,5,3);

select x,x-1,x-2,x-3,count(*) c from t
group by x,x-1,x-2,x-3 order by x;
select x-1 d,x,count(*) c from t group by 1,2
having d<3 and count(*)>1 order by d desc limit 3;
select x,y,cast(x as signed)+cast(y as signed) d,count(*) c from t
group by d,x,y order by x,y;
select d,sum(c) n from
(select x,x-1 d,count(*) c from t group by x,x-1) z group by d order by d;
select x,x-1,row_number() over(partition by x-1 order by x),count(*) c
from t group by x-1,x order by x;
select x,x-1,count(distinct y),sum(y) from t group by x,x-1 order by x;
select x,x-1,count(*) from t where false group by x,x-1;

-- An ungrouped dependency must still split otherwise equal x values.
select x,y-1 d,count(*) c from t group by x,y-1 order by x,d;
-- Runtime parameters retain their execution-time contract on repeated use.
prepare derived_group_param from 'select x,x-?,count(*) c from t group by x,x-? order by x';
set @shift=1;
execute derived_group_param using @shift,@shift;
set @shift=2;
execute derived_group_param using @shift,@shift;
deallocate prepare derived_group_param;

-- Errors remain visible even when HAVING would remove every group.
select b,b+1,count(*) from t group by b,b+1 having count(*)>100;
-- Diagnostics are per input row, so the two zero rows must emit two warnings.
select x,1/x,count(*) from t group by x,1/x order by x;
show warnings;

drop database group_by_derived_keys;
