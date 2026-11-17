drop database if exists inherited_compound_db;
drop user if exists inherited_compound_reader;
drop role if exists inherited_compound_a, inherited_compound_b, inherited_compound_parent, inherited_compound_unused;
create database inherited_compound_db;
create table inherited_compound_db.a (yr int, amount int);
create table inherited_compound_db.b (yr int, amount int);
create table inherited_compound_db.denied (amount int);
insert into inherited_compound_db.a values (2024, 10), (2025, 20);
insert into inherited_compound_db.b values (2024, 2), (2025, 4);
insert into inherited_compound_db.denied values (99);
create role inherited_compound_a, inherited_compound_b, inherited_compound_parent, inherited_compound_unused;
grant connect on account * to inherited_compound_a;
grant select, insert on table inherited_compound_db.a to inherited_compound_a;
grant select on table inherited_compound_db.b to inherited_compound_b;
grant select on table inherited_compound_db.denied to inherited_compound_unused;
grant inherited_compound_b to inherited_compound_a;
create user inherited_compound_reader identified by '123456' default role inherited_compound_a;
grant inherited_compound_b, inherited_compound_unused to inherited_compound_reader;

-- 主角色和继承角色分别提供两张表的权限，关闭缓存仍应允许组合查询。
-- @session:id=1&user=sys:inherited_compound_reader&password=123456
set enable_privilege_cache = off;
select sum(amount) from inherited_compound_db.a;
select sum(amount) from inherited_compound_db.b;
select a.amount, b.amount from inherited_compound_db.a a join inherited_compound_db.b b on a.yr = b.yr order by a.yr;
with a24 as (select sum(amount) v from inherited_compound_db.a where yr = 2024),
     a25 as (select sum(amount) v from inherited_compound_db.a where yr = 2025),
     b24 as (select sum(amount) v from inherited_compound_db.b where yr = 2024),
     b25 as (select sum(amount) v from inherited_compound_db.b where yr = 2025)
select a24.v, a25.v, b24.v, b25.v from a24 cross join a25 cross join b24 cross join b25;
select count(*) from (
with a24 as (select sum(amount) v from inherited_compound_db.a where yr = 2024),
     a25 as (select sum(amount) v from inherited_compound_db.a where yr = 2025),
     b24 as (select sum(amount) v from inherited_compound_db.b where yr = 2024),
     b25 as (select sum(amount) v from inherited_compound_db.b where yr = 2025)
select a25.v / b25.v as ratio from a24 cross join a25 cross join b24 cross join b25
union all
select a24.v / b24.v as ratio from a24 cross join a25 cross join b24 cross join b25
) totals;

-- 仅直接授予用户但未被当前角色继承的权限不能参与合并；ALL/NONE 仍为无操作。
set secondary role all;
select sum(a.amount + d.amount) from inherited_compound_db.a a cross join inherited_compound_db.denied d;
set secondary role none;
select sum(a.amount + b.amount) from inherited_compound_db.a a cross join inherited_compound_db.b b;

-- 合并目标写权限和源表读权限，随后回滚以保持后续预期数据不变。
begin;
insert into inherited_compound_db.a select * from inherited_compound_db.b;
select count(*) from inherited_compound_db.a;
rollback;

-- 冷缓存、交换表顺序、单表预热及重复执行的判定保持一致。
set enable_privilege_cache = on;
set clear_privilege_cache = on;
select sum(a.amount + b.amount) from inherited_compound_db.a a cross join inherited_compound_db.b b;
set clear_privilege_cache = on;
select sum(b.amount + a.amount) from inherited_compound_db.b b cross join inherited_compound_db.a a;
set clear_privilege_cache = on;
select sum(amount) from inherited_compound_db.a;
select sum(amount) from inherited_compound_db.b;
select sum(a.amount + b.amount) from inherited_compound_db.a a cross join inherited_compound_db.b b;
select sum(a.amount + b.amount) from inherited_compound_db.a a cross join inherited_compound_db.b b;
set role inherited_compound_b;
select sum(a.amount + b.amount) from inherited_compound_db.a a cross join inherited_compound_db.b b;
set secondary role all;
select sum(amount) from inherited_compound_db.a;
set role inherited_compound_a;
select sum(a.amount + b.amount) from inherited_compound_db.a a cross join inherited_compound_db.b b;
-- @session

-- 将直接继承改为两层继承，权限需要跨不同深度保留。
revoke inherited_compound_b from inherited_compound_a;
grant inherited_compound_b to inherited_compound_parent;
grant inherited_compound_parent to inherited_compound_a;
-- @session:id=2&user=sys:inherited_compound_reader&password=123456
set enable_privilege_cache = off;
select sum(a.amount + b.amount) from inherited_compound_db.a a cross join inherited_compound_db.b b;
-- @session

-- 撤销底层表授权后，即使角色仍可达也不能读取该表。
revoke select on table inherited_compound_db.b from inherited_compound_b;
-- @session:id=3&user=sys:inherited_compound_reader&password=123456
select sum(a.amount + b.amount) from inherited_compound_db.a a cross join inherited_compound_db.b b;
select sum(amount) from inherited_compound_db.a;
-- @session

drop user inherited_compound_reader;
drop role inherited_compound_a, inherited_compound_parent, inherited_compound_b, inherited_compound_unused;
drop database inherited_compound_db;
