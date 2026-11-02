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

-- A qualified explicit alias retains the existing source-table precedence,
-- including an alias spelled exactly like the base table's implicit name.
begin;
delete ignored.x from t as x where x.id=1;
select id from t order by id;
rollback;
begin;
with t as (select 99 as id) delete ignored.t from cte_delete_target.t as t where t.id=1;
select id from t order by id;
rollback;

-- An actual CTE target must neither panic nor delete the same-named base table.
with t as (select 1 as id) delete from t where id=1;
select id from t order by id;

-- The legacy multi-target planner must use the same target scope.
create table s(id int primary key);
insert into s values (1),(2);
-- Validate every target before any rows are deleted.
begin;
delete x, missing from t as x join s as y on x.id=y.id;
select id from t order by id;
rollback;
with x as (select 99 as id), y as (select 99 as id)
delete x,y from t as x join s as y on x.id=y.id where x.id=1;
select id from t order by id;
select id from s order by id;
-- An outgoing FK selects the legacy planner. A join without WHERE still
-- deletes only matching rows and must retain the other row's unique key.
create table p(id int primary key);
insert into p values (1),(2);
create table child(id int primary key,v int unique,pid int,
  constraint fk_child foreign key(pid) references p(id));
insert into child values (1,10,1),(2,20,2);
with x as (select 99 as id), q as (select 1 as id)
delete x from child as x join q on x.id=q.id;
select id,v from child order by id;
insert into child values (3,10,1);
insert into child values (4,20,2);
select id,v from child order by id;
drop database cte_delete_target;
