--- @metacmp(true)
drop database if exists count_derived_sort;
create database count_derived_sort;
use count_derived_sort;

create table items (
    id int primary key,
    category varchar(32),
    name varchar(32),
    n int
);
insert into items values
    (1, 'a', 'x', null),
    (2, 'a', 'x', null),
    (3, 'a', null, 10),
    (4, 'b', 'y', 20),
    (5, null, 'z', null);

-- Scalar counts ignore unbounded input ordering, not NULLs or duplicates.
select count(*) as total from (select name from items order by name) s;
select count(1) as total from (select name from items order by name desc) s;
select count(*), count(n) from (select n from items order by n) s;
select count(*) as total from (select distinct category, name from items order by category, name) s;
select count(name) as total from (select distinct category, name from items order by category, name) s;
select count(distinct name) as total from (select name from items order by name) s;
select count(*), sum(n) from (select n from items order by n) s;
select count(*), count(n) from (select n from items where id < 0 order by id) s;

-- Multi-table DISTINCT must keep every tuple key, even if COUNT needs no columns.
create table categories (id int primary key, category varchar(32));
insert into categories values (1, 'a'), (2, 'b'), (3, 'a');
select count(*) as total from (
    select distinct i.category, i.name
    from items i join categories c on i.category = c.category
    order by i.category, i.name
) s;
select count(name) as total from (
    select distinct i.category, i.name
    from items i join categories c on i.category = c.category
    order by i.category, i.name
) s;

-- Which rows survive pagination is observable to counts and outer predicates.
select count(n) as total from (select n from items order by id desc limit 2) s;
select count(n) as total from (select n from items order by id desc limit 3 offset 1) s;
select count(*) as total from (select category from items order by id desc limit 2) s where category = 'a';
select count(rn) as total from (select row_number() over (order by id) as rn from items order by rn desc) s;
select id from items order by id desc;

-- An execution-time expression failure must not disappear behind a sort-only
-- alias. An invalid regular expression pattern is the execution-time oracle;
-- issue #30417 made invalid DECIMAL text lenient, so the previous cast-based
-- witness no longer errors. The regex error text is row-independent.
-- @regex('^error parsing regexp: missing closing', true)
select count(*) from (select regexp_substr(name, '[') as k from items order by k) s;

-- A LEFT JOIN branch kept alive only by the COUNT input ordering must not be
-- pruned together with its failing ON expression.
create table left_join_keepalive_l (id int primary key);
create table left_join_keepalive_r (id int primary key, bad varchar(32));
insert into left_join_keepalive_l values (1);
insert into left_join_keepalive_r values (1, 'bad');
select count(*) from (
    select l.id
    from left_join_keepalive_l l left join left_join_keepalive_r r
      on l.id = r.id and length(regexp_substr(r.bad, '[')) > 0
    order by r.id
) s;
-- The same shape with a total ON condition may still drop the ordering.
select count(*) as total from (
    select l.id
    from left_join_keepalive_l l left join left_join_keepalive_r r
      on l.id = r.id
    order by r.id
) s;

prepare count_sorted from 'select count(n) as total from (select n from items where id > ? order by id) s';
set @min_id = 1;
execute count_sorted using @min_id;
set @min_id = 4;
execute count_sorted using @min_id;
deallocate prepare count_sorted;

drop database count_derived_sort;
