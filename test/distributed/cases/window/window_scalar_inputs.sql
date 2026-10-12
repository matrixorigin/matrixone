-- @suite
-- @case
-- @desc: Scalar window inputs, domainless NULL peers, and scalar cardinality
-- @label:bvt

drop database if exists window_scalar_inputs;
create database window_scalar_inputs;
use window_scalar_inputs;
create table src(id int,v int);
insert into src values(1,10),(1,20),(2,30);

-- Ties are intentionally unordered: validate the entire partition/row-number set.
select id,rn from (
  select id,row_number() over(partition by id order by (select null)) rn from src
) w order by id,rn;
select id,rn from (
  select id,row_number() over(partition by id order by (select 1)) rn from src
) w order by id,rn;
select id,rn from (
  select id,row_number() over(partition by id order by null) rn from src
) w order by id,rn;

-- Default deduplication returns exactly one original row for each primary key.
select id,count(*) as n,
  sum(case when (id=1 and v in(10,20)) or (id=2 and v=30) then 1 else 0 end) as valid_rows
from (
  select id,v,row_number() over(partition by id order by (select null)) rn from src
) w where rn=1 group by id order by id;

-- Scalar inputs apply to partition membership and function arguments too.
select id,v,row_number() over(partition by (select src.id) order by v) as rn
from src order by id,v;
select id,v,sum((select src.v)) over(partition by id) as total
from src order by id,v;
select id,v,row_number() over(partition by id order by (select -src.v)) as rn
from src order by id,v;

-- Empty scalar results are NULL; more than one row/column remains an error.
select id,v,rank() over(partition by id order by (select v from src where id=99)) as r
from src order by id,v;
select row_number() over(order by (select v from src)) from src;
select row_number() over(order by (select 1,2)) from src;

-- Peer boundaries, stacked windows and RANGE frames survive NULL sort keys.
select id,v,
  rank() over(partition by id order by (select 1)) as r,
  dense_rank() over(partition by id order by null) as dr,
  sum(v) over(partition by id order by (select null)) as total
from src order by id,v;
select id,v,rank() over(order by id,null) as r from src order by id,v;
select id,v,row_number() over(partition by null order by v) as rn from src order by v;
select row_number() over(order by (select null)) as rn from src where id<0;
select id,sum(v),sum(sum(v)) over(order by (select max(v) from src)) as total
from src group by id order by id;

drop database window_scalar_inputs;
show databases like 'window_scalar_inputs';
