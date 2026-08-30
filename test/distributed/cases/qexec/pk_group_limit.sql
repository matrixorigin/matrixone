drop database if exists pk_group_limit_bvt;
create database pk_group_limit_bvt;
use pk_group_limit_bvt;

create table t (
    id int primary key,
    v int,
    u bigint unsigned,
    d decimal(10, 2),
    nullable_v int,
    s varchar(16)
);
insert into t values
    (1, 10, 100, 1.25, null, 'a'),
    (2, 20, 200, 2.50, 7, 'b'),
    (3, null, null, null, 9, null),
    (4, 40, 400, 4.75, 11, 'd');

-- Single-row aggregate laws after complete-PK grouping elimination.
select id, count(*), count(v), count(nullable_v),
       sum(v), avg(v), sum(u), avg(u), sum(d), avg(d),
       min(s), max(s), any_value(s)
from t
group by id
order by id
limit 4;

-- Bounded demand remains above WHERE and HAVING semantics.
select count(*) from (
    select id, count(*) as c
    from t
    where v >= 20
    group by id
    limit 1 offset 1
) q;

select id, sum(v)
from t
group by id
having sum(v) >= 20
order by id
limit 2;

-- Unsupported aggregate families retain the ordinary aggregate path.
select id, group_concat(s order by s separator '|')
from t
group by id
order by id
limit 4;

drop database pk_group_limit_bvt;
