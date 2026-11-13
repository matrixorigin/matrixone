drop database if exists compound_role_db;
drop user if exists compound_reader;
drop role if exists compound_a, compound_b, compound_parent;
create database compound_role_db;
create table compound_role_db.a (yr int, amount int);
create table compound_role_db.b (yr int, amount int);
create table compound_role_db.denied (amount int);
insert into compound_role_db.a values (2024, 10), (2025, 20);
insert into compound_role_db.b values (2024, 2), (2025, 4);
create role compound_a, compound_b, compound_parent;
grant connect on account * to compound_a;
grant select, insert on table compound_role_db.a to compound_a;
grant select on table compound_role_db.b to compound_b;
create user compound_reader identified by '123456' default role compound_a;
grant compound_b to compound_reader;

-- 不同角色提供两张表的权限；缓存关闭也必须允许组合查询。
-- @session:id=1&user=sys:compound_reader&password=123456
set enable_privilege_cache = off;
set secondary role all;
select sum(amount) from compound_role_db.a;
select sum(amount) from compound_role_db.b;
select p.amount, r.amount from (select amount from compound_role_db.a where yr = 2025 limit 1) p cross join (select amount from compound_role_db.b where yr = 2025 limit 1) r;
with a24 as (select sum(amount) v from compound_role_db.a where yr = 2024),
     a25 as (select sum(amount) v from compound_role_db.a where yr = 2025),
     b24 as (select sum(amount) v from compound_role_db.b where yr = 2024),
     b25 as (select sum(amount) v from compound_role_db.b where yr = 2025)
select a24.v, a25.v, b24.v, b25.v from a24 cross join a25 cross join b24 cross join b25;
select count(*) from (
with a24 as (select sum(amount) v from compound_role_db.a where yr = 2024),
     a25 as (select sum(amount) v from compound_role_db.a where yr = 2025),
     b24 as (select sum(amount) v from compound_role_db.b where yr = 2024),
     b25 as (select sum(amount) v from compound_role_db.b where yr = 2025)
select a25.v / b25.v as ratio from a24 cross join a25 cross join b24 cross join b25
union all
select a24.v / b24.v as ratio from a24 cross join a25 cross join b24 cross join b25
) totals;
select sum(a.amount + d.amount) from compound_role_db.a a cross join compound_role_db.denied d;
-- 目标写权限与源表读权限可以组合；缺少源表权限仍拒绝。
begin;
insert into compound_role_db.a select * from compound_role_db.b;
select count(*) from compound_role_db.a;
rollback;
insert into compound_role_db.a select 2025, amount from compound_role_db.denied;
set secondary role none;
select sum(a.amount + b.amount) from compound_role_db.a a cross join compound_role_db.b b;
set secondary role all;

-- 冷缓存、单表预热和重复执行的结果一致。
set enable_privilege_cache = on;
set clear_privilege_cache = on;
select sum(a.amount + b.amount) from compound_role_db.a a cross join compound_role_db.b b;
set clear_privilege_cache = on;
select sum(b.amount + a.amount) from compound_role_db.b b cross join compound_role_db.a a;
set clear_privilege_cache = on;
select sum(amount) from compound_role_db.a;
select sum(amount) from compound_role_db.b;
select sum(a.amount + b.amount) from compound_role_db.a a cross join compound_role_db.b b;
select sum(a.amount + b.amount) from compound_role_db.a a cross join compound_role_db.b b;
-- PREPARE 只保留需求，每次 EXECUTE 必须重新判定角色权限。
prepare compound_stmt from 'select sum(a.amount + b.amount) from compound_role_db.a a cross join compound_role_db.b b';
execute compound_stmt;
execute compound_stmt;
set secondary role none;
execute compound_stmt;
set secondary role all;
execute compound_stmt;
deallocate prepare compound_stmt;
set role compound_b;
set secondary role none;
select sum(amount) from compound_role_db.a;
set role compound_a;
set secondary role all;
-- @session

-- 由主角色经两层继承获得另一张表的权限。
revoke compound_b from compound_reader;
grant compound_b to compound_parent;
grant compound_parent to compound_a;
-- @session:id=2&user=sys:compound_reader&password=123456
set enable_privilege_cache = off;
set secondary role none;
select sum(a.amount + b.amount) from compound_role_db.a a cross join compound_role_db.b b;
set enable_privilege_cache = on;
prepare compound_inherited from 'select sum(a.amount + b.amount) from compound_role_db.a a cross join compound_role_db.b b';
execute compound_inherited;
-- @session

-- 保留连接及 prepared 需求，撤权后不能沿用上一次成功项。
revoke select on table compound_role_db.b from compound_b;
grant all on table compound_role_db.a to compound_a;
-- @session:id=2&user=sys:compound_reader&password=123456
execute compound_inherited;
select sum(amount) from compound_role_db.a;
deallocate prepare compound_inherited;
-- @session
-- 新连接同样拒绝；一张表的 ALL 不能覆盖另一张表。
-- @session:id=3&user=sys:compound_reader&password=123456
set secondary role all;
select sum(a.amount + b.amount) from compound_role_db.a a cross join compound_role_db.b b;
-- @session

drop user compound_reader;
drop role compound_a, compound_parent, compound_b;
drop database compound_role_db;
