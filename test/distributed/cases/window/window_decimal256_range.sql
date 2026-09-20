-- @suite
-- @case
-- @desc: Decimal256 RANGE bounds preserve constant and prepared offsets
-- @label:bvt
--- @metacmp(false)
drop database if exists window_decimal256_range;
create database window_decimal256_range;
use window_decimal256_range;

-- Equal numeric values with short/wide literal representations must agree.
select count(*) over (order by 1.25 range between 1 preceding and current row) as n;
select count(*) over (order by 1.25000000000000000000000000000000000000 range between 1 preceding and current row) as n;
create table t(id int primary key, k decimal(65,2), v int);
insert into t values (1,0,10),(2,1.25,20),(3,1.25,30),(4,3,40),(5,null,50),(6,null,60);
-- Constant storage still has logical rows in singleton partitions/empty frames.
select id,
count(*) over (order by 1.25000000000000000000000000000000000000 range between 1 following and 2 following) as empty_n,
count(*) over (partition by id order by 1.25000000000000000000000000000000000000 range between 1 preceding and 0 following) as singleton_n
from t order by id;
select id,
count(*) over (order by k range between 1.25 preceding and current row) as n,
sum(v) over (order by k range between 1.25 preceding and current row) as s,
cast(first_value(k) over (order by k range between 1.25 preceding and current row) as decimal(10,2)) as first_k
from t order by id;
select id,
count(*) over (order by k desc range between 1.75 preceding and current row) as n,
sum(v) over (order by k desc range between 1.75 preceding and current row) as s
from t order by id;
select id, count(*) over (order by k range between 0 preceding and 0 following) as peers
from t order by id;

prepare decimal_range from 'select id, sum(v) over (order by k range between ? preceding and current row) as s from t order by id';
set @decimal_offset = 1.25;
execute decimal_range using @decimal_offset;
set @decimal_offset = -1;
execute decimal_range using @decimal_offset;
set @decimal_offset = null;
execute decimal_range using @decimal_offset;
set @decimal_offset = 0;
execute decimal_range using @decimal_offset;
set @decimal_offset = 3;
execute decimal_range using @decimal_offset;
deallocate prepare decimal_range;
drop database window_decimal256_range;
