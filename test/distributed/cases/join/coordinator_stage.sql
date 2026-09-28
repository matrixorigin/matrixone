drop database if exists join_stage_placement;
create database join_stage_placement;
use join_stage_placement;
create table a(id int, k int, x int);
create table c(rid int, k int, v int);
insert into a values (1,1,1),(2,1,1),(3,2,NULL),(4,NULL,0);
insert into c values (11,1,1),(12,1,3),(13,2,NULL);

-- A local window probe must receive the real build map, not an empty cleanup result.
select d.id,d.rn,c.v
from (select id,k,row_number() over(order by id) as rn from a) d
left join c on d.k=c.k order by d.id,c.v;

-- A window build stays local while its input can be scanned remotely.
select a.id,d.v from a
left join (select k,v,row_number() over(partition by k order by rid desc) as rn from c) d
on a.k=d.k and d.rn=1 order by a.id;

-- Numeric normalization must not manufacture string literal metadata.
with d as (
    select distinct k,x,k is null as kn,coalesce(k,0) as kv,
        x is null as xn,coalesce(x,0) as xv from a
), f as (
    select d.kn,d.kv,d.xn,d.xv,coalesce(max(c.v),0) as result
    from d join c on c.k=d.k where c.rid>10
    group by d.kn,d.kv,d.xn,d.xv
)
select a.id,coalesce(f.result,0) from a left join f
on (a.k is null)=f.kn and coalesce(a.k,0)=f.kv
and (a.x is null)=f.xn and coalesce(a.x,0)=f.xv order by a.id;

-- Reuse and early completion preserve duplicate outer rows and NULL results.
prepare join_stage_query from 'select a.id,d.v from a left join (select k,v,row_number() over(partition by k order by rid desc) as rn from c) d on a.k=d.k and d.rn=1 order by a.id limit ?';
set @join_stage_limit=1;
execute join_stage_query using @join_stage_limit;
set @join_stage_limit=4;
execute join_stage_query using @join_stage_limit;
deallocate prepare join_stage_query;
set @join_stage_limit=NULL;

select d.id from (select id,k,row_number() over(order by id) as rn from a where id<0) d
left join c on d.k=c.k;
drop database join_stage_placement;
