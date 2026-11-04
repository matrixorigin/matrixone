-- @suite
-- @case
-- @desc: Constant NULL window keys preserve partitions and peer groups
-- @label:bvt

drop database if exists window_constant_keys;
create database window_constant_keys;
use window_constant_keys;
create table t(id int,v int);
insert into t values(1,10),(1,20),(2,30);

-- Tied rows have unspecified source order; assert id and ranks, not v-to-rn assignment.
select id,row_number() over(partition by id order by null) as rn,
rank() over(partition by id order by null) as rk,
dense_rank() over(partition by id order by null) as dr
from t order by id,rn;

-- Typed NULL and non-NULL constants share the same boundary contract.
select id,row_number() over(partition by id order by cast(null as signed)) as rn
from t order by id,rn;
select id,row_number() over(partition by id order by 1) as rn
from t order by id,rn;

-- A constant partition key must not hide the following id partition key.
select id,row_number() over(partition by null,id order by null) as rn
from t order by id,rn;

-- A constant key before or after a real ordering key adds no peer boundaries.
select row_number() over(partition by null order by null) as rn
from t order by rn;

select id,v,rank() over(partition by id order by null,v) as rk,
dense_rank() over(partition by id order by v,null) as dr
from t order by id,v;

-- With no partition key, all rows are peers under the NULL ordering key.
select row_number() over(order by null) as rn,rank() over(order by null) as rk
from t order by rn;
select id,row_number() over(partition by id order by null) as rn
from t where id=2;
select id,row_number() over(partition by id order by null) as rn
from t where id<0;

-- Optimizer-selected Partition TopN preserves ROW_NUMBER and RANK ties.
select id,rn from (select id,row_number() over(partition by id order by null) as rn from t) s
where rn<=1 order by id;
select id,rk from (select id,rank() over(partition by id order by null) as rk from t) s
where rk<=1 order by id;
select id,v,rn from (select id,v,row_number() over(partition by id order by null,v) as rn from t) s
where rn<=1 order by id;

-- Force the physical external-sort path without increasing the fixture.
set @old_window_sort_spill_mem=@@sort_spill_mem;
set sort_spill_mem=1;
select id,row_number() over(partition by id order by null) as rn_spill
from t order by id,rn_spill;
select id,v,rank() over(partition by null,id order by null,v) as rk_spill,
dense_rank() over(partition by null,id order by v,null) as dr_spill
from t order by id,v;
select dense_rank() over(partition by null order by null) as dr_constant_spill
from t;
set sort_spill_mem=@old_window_sort_spill_mem;

-- Ordinary nullable ordering columns still use their concrete type semantics.
update t set v=null where id=2;
select id,row_number() over(partition by id order by v) as rn
from t order by id,rn;

drop database window_constant_keys;
show databases like 'window_constant_keys';
