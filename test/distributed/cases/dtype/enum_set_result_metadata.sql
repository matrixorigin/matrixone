-- ENUM/SET publish CHAR metadata while retaining label strings, including
-- empty labels and NULL. Ordinary strings and computed expressions are controls.
drop table if exists jdbc_enum_set_metadata;
create table jdbc_enum_set_metadata (
    id int primary key,
    e enum('a','bb',''),
    s set('x','yy'),
    c char(2),
    v varchar(2)
);
insert into jdbc_enum_set_metadata values
    (1, 'a', 'x,yy', 'ab', 'ab'), (2, '', '', '', ''), (3, null, null, null, null);
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
-- DISTINCT ROLLUP and window MIN/MAX materialize VARCHAR at execution.
-- @metacmp(true)
select distinct e, s from jdbc_enum_set_metadata group by e, s with rollup order by e, s;
-- @metacmp(true)
select min(e) over () as min_e, max(s) over () as max_s from jdbc_enum_set_metadata;
-- @metacmp(true)
select e, s from (select e, s from jdbc_enum_set_metadata group by e, s with rollup) d order by e desc, s desc;
drop table jdbc_enum_set_metadata;
