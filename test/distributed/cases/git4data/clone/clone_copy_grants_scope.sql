set global enable_privilege_cache = off;

-- This case focuses on object_type / privilege_level combinations.
-- COPY GRANTS should copy explicit table-object grants only. Broader effective
-- privileges such as db.* or *.* should not be materialized as B table grants.
drop account if exists acc_clone_copy_grants_scope;
create account acc_clone_copy_grants_scope admin_name "root1" identified by "111";

-- @session:id=1&user=acc_clone_copy_grants_scope:root1&password=111
drop database if exists cg_scope_src;
drop database if exists cg_scope_dst;

create database cg_scope_src;
create database cg_scope_dst;
create table cg_scope_src.src(a int primary key, b varchar(20));
insert into cg_scope_src.src values(1, 'one'), (2, 'two');

-- Explicit table grants:
--   cg_scope_r_dt stores privilege_level=d.t
--   cg_scope_r_t stores privilege_level=t
--   cg_scope_r_parent owns a grant that cg_scope_r_child inherits
create role cg_scope_r_dt, cg_scope_r_t, cg_scope_r_parent, cg_scope_r_child;
grant connect on account * to cg_scope_r_dt;
grant connect on account * to cg_scope_r_t;
grant connect on account * to cg_scope_r_child;
create user cg_scope_u_dt identified by '111' default role cg_scope_r_dt;
create user cg_scope_u_t identified by '111' default role cg_scope_r_t;
create user cg_scope_u_child identified by '111' default role cg_scope_r_child;

grant select on table cg_scope_src.src to cg_scope_r_dt with grant option;
use cg_scope_src;
grant select on table src to cg_scope_r_t;
grant select on table cg_scope_src.src to cg_scope_r_parent;
grant cg_scope_r_parent to cg_scope_r_child;

-- Broader effective privileges. These roles can access A, but their grants are
-- not bound to A's rel_logical_id and should not be copied as B table grants.
create role cg_scope_r_db_star, cg_scope_r_global_star, cg_scope_r_db_object, cg_scope_r_account_object;
grant connect on account * to cg_scope_r_db_star;
grant connect on account * to cg_scope_r_global_star;
grant connect on account * to cg_scope_r_db_object;
grant connect on account * to cg_scope_r_account_object;
create user cg_scope_u_db_star identified by '111' default role cg_scope_r_db_star;
create user cg_scope_u_global_star identified by '111' default role cg_scope_r_global_star;
create user cg_scope_u_db_object identified by '111' default role cg_scope_r_db_object;
create user cg_scope_u_account_object identified by '111' default role cg_scope_r_account_object;

grant select on table cg_scope_src.* to cg_scope_r_db_star;
grant select on table *.* to cg_scope_r_global_star;
grant create table on database cg_scope_src to cg_scope_r_db_object;
grant create database on account * to cg_scope_r_account_object;

create table cg_scope_src.same_plain clone cg_scope_src.src;
create table cg_scope_src.same_copy clone cg_scope_src.src copy grants;
create table cg_scope_dst.cross_copy clone cg_scope_src.src copy grants;

-- Direct table-object grants: source and COPY GRANTS targets should have the
-- explicit table grants; plain clone should have none.
select t.reldatabase, t.relname, count(rp.role_name) as direct_table_grants
from mo_catalog.mo_tables t
left join mo_catalog.mo_role_privs rp
  on rp.obj_type = 'table'
 and rp.obj_id = t.rel_logical_id
 and rp.role_name like 'cg_scope_r_%'
where t.reldatabase in ('cg_scope_src', 'cg_scope_dst')
  and t.relname in ('src', 'same_plain', 'same_copy', 'cross_copy')
group by t.reldatabase, t.relname
order by t.reldatabase, t.relname;

-- Explicit table grants copied to B keep their original privilege_level shape.
select t.reldatabase, t.relname, rp.role_name, rp.privilege_name, rp.privilege_level, rp.with_grant_option
from mo_catalog.mo_tables t
join mo_catalog.mo_role_privs rp
  on rp.obj_type = 'table'
 and rp.obj_id = t.rel_logical_id
where t.reldatabase in ('cg_scope_src', 'cg_scope_dst')
  and t.relname in ('src', 'same_copy', 'cross_copy')
  and rp.role_name like 'cg_scope_r_%'
order by t.reldatabase, t.relname, rp.role_name, rp.privilege_name;

-- Broader grants remain broader grants; COPY GRANTS does not expand them into
-- direct table grants on same_copy/cross_copy.
select role_name, obj_type, privilege_name, privilege_level
from mo_catalog.mo_role_privs
where role_name in ('cg_scope_r_db_star', 'cg_scope_r_global_star', 'cg_scope_r_db_object', 'cg_scope_r_account_object')
  and privilege_name <> 'connect'
order by role_name, obj_type, privilege_name, privilege_level;
-- @session

-- d.t explicit table grant is copied to both same-db and cross-db clone targets.
-- Plain clone remains inaccessible.
-- @session:id=2&user=acc_clone_copy_grants_scope:cg_scope_u_dt:cg_scope_r_dt&password=111
select * from cg_scope_src.src order by a;
select * from cg_scope_src.same_plain order by a;
select * from cg_scope_src.same_copy order by a;
select * from cg_scope_dst.cross_copy order by a;
-- @session

-- t explicit table grant is also copied. The copied grant uses the target
-- table obj_id, so it works even when the target is in another database.
-- @session:id=3&user=acc_clone_copy_grants_scope:cg_scope_u_t:cg_scope_r_t&password=111
select * from cg_scope_src.src order by a;
select * from cg_scope_src.same_copy order by a;
select * from cg_scope_dst.cross_copy order by a;
-- @session

-- db.* is an effective privilege on A. It naturally covers same-db plain/copy
-- targets, but COPY GRANTS does not make it cover the cross-db clone.
-- @session:id=4&user=acc_clone_copy_grants_scope:cg_scope_u_db_star:cg_scope_r_db_star&password=111
select * from cg_scope_src.src order by a;
select * from cg_scope_src.same_plain order by a;
select * from cg_scope_src.same_copy order by a;
select * from cg_scope_dst.cross_copy order by a;
-- @session

-- *.* is still effective everywhere, but it is not copied as a table-specific
-- grant. Runtime access succeeds because the original global grant covers all tables.
-- @session:id=5&user=acc_clone_copy_grants_scope:cg_scope_u_global_star:cg_scope_r_global_star&password=111
select * from cg_scope_src.src order by a;
select * from cg_scope_src.same_plain order by a;
select * from cg_scope_src.same_copy order by a;
select * from cg_scope_dst.cross_copy order by a;
-- @session

-- database/account object_type grants should not become table grants and do not
-- provide table SELECT access by themselves.
-- @session:id=6&user=acc_clone_copy_grants_scope:cg_scope_u_db_object:cg_scope_r_db_object&password=111
select * from cg_scope_src.src order by a;
select * from cg_scope_src.same_copy order by a;
-- @session
-- @session:id=7&user=acc_clone_copy_grants_scope:cg_scope_u_account_object:cg_scope_r_account_object&password=111
select * from cg_scope_src.src order by a;
select * from cg_scope_src.same_copy order by a;
-- @session

-- The grant is copied to cg_scope_r_parent. cg_scope_r_child gets access through
-- role inheritance, but no extra table grant is inserted for the child role.
-- @session:id=8&user=acc_clone_copy_grants_scope:cg_scope_u_child:cg_scope_r_child&password=111
select * from cg_scope_src.same_plain order by a;
select * from cg_scope_src.same_copy order by a;
select * from cg_scope_dst.cross_copy order by a;
-- @session

-- Final metadata check: child role has no direct table grant on clone targets.
-- @session:id=1&user=acc_clone_copy_grants_scope:root1&password=111
select t.reldatabase, t.relname, rp.role_name, rp.privilege_name
from mo_catalog.mo_tables t
join mo_catalog.mo_role_privs rp
  on rp.obj_type = 'table'
 and rp.obj_id = t.rel_logical_id
where t.reldatabase in ('cg_scope_src', 'cg_scope_dst')
  and t.relname in ('same_copy', 'cross_copy')
  and rp.role_name in ('cg_scope_r_parent', 'cg_scope_r_child')
order by t.reldatabase, t.relname, rp.role_name, rp.privilege_name;

drop user cg_scope_u_dt, cg_scope_u_t, cg_scope_u_child;
drop user cg_scope_u_db_star, cg_scope_u_global_star, cg_scope_u_db_object, cg_scope_u_account_object;
drop role cg_scope_r_dt, cg_scope_r_t, cg_scope_r_parent, cg_scope_r_child;
drop role cg_scope_r_db_star, cg_scope_r_global_star, cg_scope_r_db_object, cg_scope_r_account_object;
drop database cg_scope_src;
drop database cg_scope_dst;
-- @session

drop account if exists acc_clone_copy_grants_scope;
set global enable_privilege_cache = on;
