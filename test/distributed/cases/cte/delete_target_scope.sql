drop database if exists cte_delete_target;
create database cte_delete_target;
use cte_delete_target;
create table t(id int primary key);
insert into t values (1),(2);

-- A target alias wins over an unrelated CTE with the same name.
begin;
with x as (select 99 as id) delete x from t as x where x.id=1;
select id from t order by id;
rollback;

-- An actual CTE target must neither panic nor delete the same-named base table.
with t as (select 1 as id) delete from t where id=1;
select id from t order by id;

-- The legacy multi-target planner must use the same target scope.
create table s(id int primary key);
insert into s values (1),(2);
with x as (select 99 as id), y as (select 99 as id)
delete x,y from t as x join s as y on x.id=y.id where x.id=1;
select id from t order by id;
select id from s order by id;
drop database cte_delete_target;
