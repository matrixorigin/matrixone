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

-- COUNT(*) is 1 for every complete-PK singleton group. Its all-tie ordering is
-- removable, including OFFSET, while public cardinality remains unchanged.
select count(*) from (
    select id, count(*) c
    from t
    group by id
    order by c desc
    limit 2 offset 1
) q;

-- HAVING is evaluated before bounded demand. Constant true admits every
-- singleton group; constant false admits none.
select count(*) from (
    select id, count(*) c
    from t
    group by id
    having count(*) = 1
    order by c
    limit 10
) q;

select count(*) from (
    select id, count(*) c
    from t
    group by id
    having count(*) <> 1
    order by c
    limit 10
) q;

-- A row-dependent tie breaker and nullable COUNT are counterexamples: their
-- Sort cannot be removed by the constant-key proof.
select id, count(*) c
from t
group by id
order by c desc, id desc
limit 2;

select id, count(nullable_v) c
from t
group by id
order by c
limit 1;

select id, sum(v) c
from t
group by id
order by c desc
limit 1;

-- INTERVAL is an internal (value, unit) representation, not a standalone
-- scalar. Public scalar/key boundaries reject it normally instead of allowing
-- planner or executor panics; a temporal consumer remains valid.
select interval 1 day;
select interval 1 day is null;
select count(interval 1 day) from t;
select id from t group by interval 1 day;
select id from t order by interval 1 day;
select row_number() over (partition by interval 1 day) from t limit 1;
select row_number() over (order by interval 1 day) from t limit 1;
select id, count(*) c from t group by id order by interval c day limit 10;
select date_add('2026-01-01', interval 1 day);
select count(date_add('2026-01-01', interval 1 day)) from t;

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

-- A singleton aggregate is eliminated only when its row expression is exact
-- over the complete input type domain. Wide DECIMAL AVG is promoted to
-- DECIMAL256 so it remains exact; floating-point SUM/AVG retain Aggregate to
-- preserve existing error and signed-zero behavior.
create table edge_values (
    id int primary key,
    wide decimal(38, 10),
    f float,
    dbl double
);
insert into edge_values values
    (1, 9999999999999999999999999999.9999999999, -0.0, -0.0),
    (2, 0, 0.0, 0.0),
    (3, null, null, null);

select count(*) from (
    select id, avg(wide) a
    from edge_values
    group by id
    limit 10
) q
where a is not null;

select id
from edge_values
group by id
having cast(sum(f) as varchar) = '0'
order by id
limit 10;

select id
from edge_values
group by id
having cast(avg(dbl) as varchar) = '0'
order by id
limit 10;

drop database pk_group_limit_bvt;
