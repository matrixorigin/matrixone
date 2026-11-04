-- ENUM/SET publish CHAR metadata while retaining label strings, including
-- empty labels and NULL. Ordinary strings and computed expressions are controls.
drop table if exists jdbc_enum_set_metadata;
create table jdbc_enum_set_metadata (
    id int primary key,
    e enum('a','bb',''),
    s set('x','yy'),
    c char(2),
    v varchar(2),
    u int unique
);
insert into jdbc_enum_set_metadata values
    (1, 'a', 'x,yy', 'ab', 'ab', 1), (2, '', '', '', '', null), (3, null, null, null, null, null);
-- @metacmp(true)
select e, s, c, v from jdbc_enum_set_metadata where 1 = 0;
-- @metacmp(true)
select e, s, c, v from jdbc_enum_set_metadata order by id;
-- @metacmp(true)
select e as enum_alias, s as set_alias
from (select id, e, s from jdbc_enum_set_metadata order by id limit 3) d order by id;
-- @metacmp(true)
select concat(e) as expr_e, cast(s as char) as expr_s from jdbc_enum_set_metadata where 1 = 0;
-- @metacmp(true)
select e, s from jdbc_enum_set_metadata where 1 = 0
union all select e, s from jdbc_enum_set_metadata where 1 = 0;
-- Functional dependency must retain field metadata across the internal aggregate rewrite.
-- @metacmp(true)
select e, s from jdbc_enum_set_metadata group by id order by id;
-- MIN/MAX and scalar subqueries retain CHAR widths without source-column flags.
-- @metacmp(true)
select min(e) as min_e, max(s) as max_s from jdbc_enum_set_metadata;
-- @metacmp(true)
select min(e) as min_e, max(s) as max_s from jdbc_enum_set_metadata where 1 = 0;
-- @metacmp(true)
select (select e from jdbc_enum_set_metadata where id = 1) as scalar_e,
       (select s from jdbc_enum_set_metadata where id = 1) as scalar_s;
-- @metacmp(true)
select (select e from jdbc_enum_set_metadata where 1 = 0) as scalar_e,
       (select s from jdbc_enum_set_metadata where 1 = 0) as scalar_s;
-- @metacmp(true)
select (select b.e from jdbc_enum_set_metadata b where b.id = a.id) as scalar_e,
       (select b.s from jdbc_enum_set_metadata b where b.id = a.id) as scalar_s
from jdbc_enum_set_metadata a order by id;
-- A derived aggregate exposes fields; a derived scalar subquery is an ordinary string.
-- @metacmp(true)
select e, s from (select min(e) e, max(s) s from jdbc_enum_set_metadata) d;
-- @metacmp(true)
select e, s from (select (select e from jdbc_enum_set_metadata where id = 1) e,
                        (select s from jdbc_enum_set_metadata where id = 1) s) d;
-- Internal grouping-set UNION ALL is not a user set-operation boundary.
-- @metacmp(true)
select e, s from jdbc_enum_set_metadata group by e, s with rollup order by e desc, s desc;
-- @metacmp(true)
select min(e) as min_e, max(s) as max_s from jdbc_enum_set_metadata group by id with rollup order by min_e, max_s;
-- DISTINCT ROLLUP and nonconstant window inputs materialize VARCHAR at execution.
-- @metacmp(true)
select distinct e, s from jdbc_enum_set_metadata group by e, s with rollup order by e, s;
-- @metacmp(true)
select min(e) over () as min_e, max(s) over () as max_s from jdbc_enum_set_metadata;
-- @metacmp(true)
select e, s from (select e, s from jdbc_enum_set_metadata group by e, s with rollup) d order by e desc, s desc;
-- Materialized labels retain width but cannot regain CHAR identity.
-- @metacmp(true)
select min(e) as min_e, max(s) as max_s
from (select e,s from jdbc_enum_set_metadata group by e,s with rollup) d;
-- @metacmp(true)
select min(e) over() as min_e, max(s) over() as max_s
from (select e,s from jdbc_enum_set_metadata group by e,s with rollup) d;
-- Identical branches must not be rewritten into DISTINCT ROLLUP metadata.
-- @metacmp(true)
select e,s from jdbc_enum_set_metadata group by e,s with rollup
union select e,s from jdbc_enum_set_metadata group by e,s with rollup order by e desc,s desc;
-- Static emptiness elides non-cursor materialization; runtime emptiness does not.
-- @metacmp(true)
select min(e) over() as min_e, max(s) over() as max_s from jdbc_enum_set_metadata limit 0;
-- @metacmp(true)
select min(e) over() as min_e, max(s) over() as max_s from jdbc_enum_set_metadata where false;
-- @metacmp(true)
select distinct e,s from jdbc_enum_set_metadata where false group by e,s with rollup;
-- @metacmp(true)
select min(e) over() as min_e, max(s) over() as max_s from jdbc_enum_set_metadata where id > 100;
-- @metacmp(true)
select min(e) over() as min_e, max(s) over() as max_s
from (select e,s from jdbc_enum_set_metadata group by e,s with rollup) d limit 0;
-- A global group emits one row even on empty input, without window materialization.
-- @metacmp(true)
select min(min(e)) over() as min_e, max(max(s)) over() as max_s from jdbc_enum_set_metadata where false;
-- @metacmp(true)
select min(min(e)) over() as min_e, max(max(s)) over() as max_s from jdbc_enum_set_metadata;
-- Constant window inputs differ from a final result truncated to one row.
-- @metacmp(true)
select min(e) over() as min_e, max(s) over() as max_s from jdbc_enum_set_metadata where id=1;
-- @metacmp(true)
select min(e) over() as min_e, max(s) over() as max_s
from (select e,s from jdbc_enum_set_metadata order by id limit 1) d;
-- @metacmp(true)
select min(e) over() as min_e, max(s) over() as max_s from jdbc_enum_set_metadata limit 1;
-- NULL rejection and join emptiness must respect the join's preserved side.
-- @metacmp(true)
select min(e) over() as min_e, max(s) over() as max_s from jdbc_enum_set_metadata where null;
-- @metacmp(true)
select min(l.e) over() as min_e, max(l.s) over() as max_s
from jdbc_enum_set_metadata l join jdbc_enum_set_metadata r on false;
-- @metacmp(true)
select min(l.e) over() as min_e, max(l.s) over() as max_s
from jdbc_enum_set_metadata l left join jdbc_enum_set_metadata r on false;
-- PARTITION and ordinary grouping preserve constant/empty input facts.
-- @metacmp(true)
select min(e) over(partition by id) as min_e,max(s) over(partition by id) as max_s
from jdbc_enum_set_metadata where id=1;
-- @metacmp(true)
select min(e) over(partition by id) as min_e,max(s) over(partition by id) as max_s
from jdbc_enum_set_metadata where false;
-- @metacmp(true)
select min(min(e)) over() as min_e,max(max(s)) over() as max_s
from jdbc_enum_set_metadata where id=1 group by id;
-- @metacmp(true)
select min(min(e)) over() as min_e,max(max(s)) over() as max_s
from jdbc_enum_set_metadata where false group by id;
-- Projection-generated joins and inner windows do not redefine the outer window block.
-- @metacmp(true)
select min(e) over() as min_e,max(s) over() as max_s,
       (select count(*) from jdbc_enum_set_metadata) as total
from jdbc_enum_set_metadata where id=1;
-- @metacmp(true)
select min(e) over() as min_e,max(s) over() as max_s
from (select id,e,s,row_number() over(order by id) as n
      from jdbc_enum_set_metadata order by id limit 1)d where n>0;
-- Existence providers must be proven constant, not rejected categorically.
-- @metacmp(true)
select min(l.e) over() as min_e,max(l.s) over() as max_s from jdbc_enum_set_metadata l
where l.id=1 and exists(select 1 from jdbc_enum_set_metadata r where r.id=1 and r.id=l.id);
-- @metacmp(true)
select min(l.e) over() as min_e,max(l.s) over() as max_s from jdbc_enum_set_metadata l
where l.id=1 and not exists(select 1 from jdbc_enum_set_metadata r where r.id=100);
-- Equivalent non-NULL key equalities differ from a nullable unique-key NULL probe.
-- @metacmp(true)
select min(e) over() as min_e,max(s) over() as max_s from jdbc_enum_set_metadata where id between 1 and 1;
-- @metacmp(true)
select min(e) over() as min_e,max(s) over() as max_s from jdbc_enum_set_metadata where id<=>1;
-- @metacmp(true)
select min(e) over() as min_e,max(s) over() as max_s from jdbc_enum_set_metadata where u<=>1;
-- @metacmp(true)
select min(e) over() as min_e,max(s) over() as max_s,count(*) over() as null_matches
from jdbc_enum_set_metadata where u<=>null;
-- A derived provider is not const merely because its underlying scan is a point lookup.
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_metadata where id=1
and exists(select 1 from (select id from jdbc_enum_set_metadata where id=1 limit 1)d);
-- Direct independent aggregate existence belongs to its own query block.
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_metadata where id=1
and exists(select count(*) from jdbc_enum_set_metadata);
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_metadata where id=1
and exists(select count(*) from jdbc_enum_set_metadata where false);
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_metadata where id=1
and not exists(select count(*) from jdbc_enum_set_metadata);
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_metadata where id=1
and exists(select 1 from (select count(*) as n from jdbc_enum_set_metadata)d);
-- A once-evaluated scalar may supply a constant key through a generated JOIN.
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_metadata
where id=(select min(id) from jdbc_enum_set_metadata);
-- A lossless widened key still admits the once-evaluated scalar value.
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_metadata
where id=(select count(*) from jdbc_enum_set_metadata);
-- Access eligibility is distinct from mathematical cast injectivity.
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_metadata
where cast(id as signed)=(select count(*) from jdbc_enum_set_metadata where id<=1);
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_metadata where cast(id as signed)=1;
-- Independent anti-existence and correlated global-group existence are not FROM providers.
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_metadata where id=1
and not exists(select 1 from (select id from jdbc_enum_set_metadata where id=100 limit 1)d);
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_metadata where id=1
and not exists(select 1 from (select id from jdbc_enum_set_metadata where id=1 limit 1)d);
-- @metacmp(true)
select min(l.e)over() as min_e,max(l.s)over() as max_s from jdbc_enum_set_metadata l where l.id=1
and exists(select count(*) from jdbc_enum_set_metadata r where r.id>=l.id);
-- @metacmp(true)
select min(l.e)over() as min_e,max(l.s)over() as max_s from jdbc_enum_set_metadata l where l.id=1
and not exists(select count(*) from jdbc_enum_set_metadata r where r.id>=l.id);
-- @metacmp(true)
select min(l.e)over() as min_e,max(l.s)over() as max_s from jdbc_enum_set_metadata l where l.id=1
and exists(select count(*) from jdbc_enum_set_metadata r where r.id>=l.id having count(*)>0);
-- Conjuncts compose through multiple generated scalar JOINs.
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_metadata
where id=(select min(id) from jdbc_enum_set_metadata) and id=(select min(id) from jdbc_enum_set_metadata);
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_metadata
where id=coalesce((select min(id) from jdbc_enum_set_metadata),1);
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_metadata
where id=(select min(id) from jdbc_enum_set_metadata where id=(select min(id) from jdbc_enum_set_metadata));
-- Rejecting BETWEEN includes its lowered conjunction and NULL comparisons.
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_metadata where id between 2 and 1;
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_metadata where id between null and 1;
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_metadata where id between 1 and null;
create table jdbc_enum_set_composite(a int,b int,e enum('a','bb',''),s set('x','yy'),primary key(a,b));
insert into jdbc_enum_set_composite select if(id=3,2,1),if(id=2,2,1),e,s from jdbc_enum_set_metadata;
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_composite
where a=(select min(id) from jdbc_enum_set_metadata) and b=(select min(id) from jdbc_enum_set_metadata);
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_composite
where a=(select min(id) from jdbc_enum_set_metadata);
drop table jdbc_enum_set_composite;
-- Outer key facts must stop at a materialized query-block boundary.
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from
(select id,e,s from jdbc_enum_set_metadata order by id limit 2)d
where id=(select min(id) from jdbc_enum_set_metadata);
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from
(select id,e,s from jdbc_enum_set_metadata order by id limit 2)d where id=1;
-- Existential consumers share local key facts, not arbitrary cardinality bounds.
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_metadata where id=1
and exists(select count(*) from jdbc_enum_set_metadata b where b.id=jdbc_enum_set_metadata.id group by b.e);
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_metadata where id=1
and exists(select 1 from jdbc_enum_set_metadata b where b.id=(select min(id) from jdbc_enum_set_metadata));
-- INNER ON admits constraints; a preserved outer input is a different contract.
-- @metacmp(true)
select min(a.e)over() as min_e,max(a.s)over() as max_s from jdbc_enum_set_metadata a
join (select id from jdbc_enum_set_metadata order by id limit 1)b on a.id=b.id;
-- @metacmp(true)
select min(a.e)over() as min_e,max(a.s)over() as max_s from jdbc_enum_set_metadata a
join (select id from jdbc_enum_set_metadata order by id limit 1)b on true where a.id=b.id;
-- @metacmp(true)
select min(a.e)over() as min_e,max(a.s)over() as max_s from jdbc_enum_set_metadata a
left join (select id from jdbc_enum_set_metadata order by id limit 1)b on a.id=b.id;
-- Normalize row-admission Boolean identities without rewriting execution.
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_metadata where id=1 or false;
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_metadata where id=1 or id=1;
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_metadata where id=1 or id=2;
-- Prepared range rejection uses this binding and must not taint later execution.
prepare enum_range_metadata from 'select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_metadata where id between ? and ?';
set @enum_lo=2;
set @enum_hi=1;
-- @metacmp(true)
execute enum_range_metadata using @enum_lo,@enum_hi;
set @enum_lo=1;
set @enum_hi=2;
-- @metacmp(true)
execute enum_range_metadata using @enum_lo,@enum_hi;
set @enum_lo=null;
-- @metacmp(true)
execute enum_range_metadata using @enum_lo,@enum_hi;
set @enum_lo=1;
set @enum_hi=null;
-- @metacmp(true)
execute enum_range_metadata using @enum_lo,@enum_hi;
set @enum_hi=2;
-- @metacmp(true)
execute enum_range_metadata using @enum_lo,@enum_hi;
deallocate prepare enum_range_metadata;
set @enum_lo=null;
set @enum_hi=null;
-- A point provider is not a constant output after conditional null-extension.
-- @metacmp(true)
select min(a.e)over() as min_e,max(a.s)over() as max_s from jdbc_enum_set_metadata a
left join (select id from jdbc_enum_set_metadata where id=1)b on a.id=b.id where a.id=coalesce(b.id,2);
-- @metacmp(true)
select min(a.e)over() as min_e,max(a.s)over() as max_s from jdbc_enum_set_metadata a
join (select id from jdbc_enum_set_metadata where id=1)b on a.id=b.id where a.id=coalesce(b.id,2);
-- @metacmp(true)
select min(a.e)over() as min_e,max(a.s)over() as max_s from jdbc_enum_set_metadata a
left join (select id from jdbc_enum_set_metadata where id=1)b on a.id=b.id where a.id=1;
-- Correlation consumes only an independently established outer const environment.
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_metadata where id=1
and exists(select 1 from jdbc_enum_set_metadata b where b.id=jdbc_enum_set_metadata.id);
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_metadata
where exists(select 1 from jdbc_enum_set_metadata b where b.id=jdbc_enum_set_metadata.id);
-- NOT identities must preserve UNKNOWN until the row-admission boundary.
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_metadata where not(not(id=1));
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_metadata where not(id<>1);
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_metadata where not(id=1 or null);
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_metadata where not(id<>1 and null);
-- Compare exact decimal bounds without a float conversion or scale truncation.
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_metadata where id between 2.5 and 1.5;
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s,count(*)over() as matches
from jdbc_enum_set_metadata where id between 1.5 and 2.5;
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_metadata where id between -1.5 and -2.5;
-- @metacmp(true)
select min(e)over() as min_e,max(s)over() as max_s from jdbc_enum_set_metadata where id between 2.50 and 1.500;
drop table jdbc_enum_set_metadata;
