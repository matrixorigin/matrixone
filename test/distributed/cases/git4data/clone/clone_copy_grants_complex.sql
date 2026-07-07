set global enable_privilege_cache = off;

-- Isolate this case in its own account so role/user/grant names cannot collide
-- with other clone or privilege BVT cases.
drop account if exists acc_clone_copy_grants_complex;
drop database if exists cg_sys_copy_grants;
create account acc_clone_copy_grants_complex admin_name "root1" identified by "111";

-- @session:id=1&user=acc_clone_copy_grants_complex:root1&password=111
drop snapshot if exists sp_cg_src;
drop database if exists cg_src;
drop database if exists cg_dst;

create database cg_src;
create database cg_dst;
create table cg_src.src(a int primary key, b varchar(20));
insert into cg_src.src values(1, 'one'), (2, 'two');

-- Source table grants intentionally cover:
--   select with grant option, insert+select, delete, table all, and inherited role access.
-- cg_r_existing_dst is only granted on the pre-existing IF NOT EXISTS target.
create role cg_r_select, cg_r_insert, cg_r_delete, cg_r_all, cg_r_parent, cg_r_child, cg_r_existing_dst;
grant connect on account * to cg_r_select;
grant connect on account * to cg_r_insert;
grant connect on account * to cg_r_delete;
grant connect on account * to cg_r_all;
grant connect on account * to cg_r_child;

create user cg_u_select identified by '111' default role cg_r_select;
create user cg_u_insert identified by '111' default role cg_r_insert;
create user cg_u_delete identified by '111' default role cg_r_delete;
create user cg_u_all identified by '111' default role cg_r_all;
create user cg_u_child identified by '111' default role cg_r_child;

grant select on table cg_src.src to cg_r_select with grant option;
grant select, insert on table cg_src.src to cg_r_insert;
grant delete on table cg_src.src to cg_r_delete;
grant all on table cg_src.src to cg_r_all with grant option;
grant select on table cg_src.src to cg_r_parent;
grant cg_r_parent to cg_r_child;

-- Compare plain clone with COPY GRANTS across same-db, cross-db, and snapshot clones.
create table cg_src.dst_plain clone cg_src.src;
create table cg_src.dst_copy clone cg_src.src copy grants;
create table cg_dst.dst_cross clone cg_src.src copy grants;
create snapshot sp_cg_src for table cg_src src;
create table cg_dst.dst_snapshot clone cg_src.src {snapshot = "sp_cg_src"} copy grants;

-- Regression: IF NOT EXISTS means an existing destination is not created by
-- this statement, so COPY GRANTS must not mutate its existing grants.
create table cg_src.dst_if_not_exists(a int primary key, b varchar(20));
grant select on table cg_src.dst_if_not_exists to cg_r_existing_dst;
create table if not exists cg_src.dst_if_not_exists clone cg_src.src copy grants;
select t.reldatabase, t.relname, rp.role_name, rp.privilege_name, rp.privilege_level, rp.with_grant_option
from mo_catalog.mo_tables t
join mo_catalog.mo_role_privs rp
  on rp.obj_type = 'table'
 and rp.obj_id = t.rel_logical_id
where t.reldatabase = 'cg_src'
  and t.relname = 'dst_if_not_exists'
  and rp.role_name like 'cg_r_%'
order by rp.role_name, rp.privilege_name;

-- Structural check: plain clone should have no copied grants; every COPY GRANTS
-- target should get the same number of table-level grants as the source table.
select t.reldatabase, t.relname, count(rp.role_name) as copied_grants
from mo_catalog.mo_tables t
left join mo_catalog.mo_role_privs rp
  on rp.obj_type = 'table'
 and rp.obj_id = t.rel_logical_id
 and rp.role_name like 'cg_r_%'
where t.reldatabase in ('cg_src', 'cg_dst')
  and t.relname in ('src', 'dst_plain', 'dst_copy', 'dst_cross', 'dst_snapshot')
group by t.reldatabase, t.relname
order by t.reldatabase, t.relname;

-- Structural check: privilege kind, level, and with_grant_option must be preserved.
select t.reldatabase, t.relname, rp.role_name, rp.privilege_name, rp.privilege_level, rp.with_grant_option
from mo_catalog.mo_tables t
join mo_catalog.mo_role_privs rp
  on rp.obj_type = 'table'
 and rp.obj_id = t.rel_logical_id
where t.reldatabase in ('cg_src', 'cg_dst')
  and t.relname in ('src', 'dst_copy', 'dst_cross', 'dst_snapshot')
  and rp.role_name like 'cg_r_%'
order by t.reldatabase, t.relname, rp.role_name, rp.privilege_name;

-- Independence check: revoking from the source after clone must not remove
-- already-copied grants from clone targets.
revoke delete on table cg_src.src from cg_r_delete;

select t.reldatabase, t.relname, rp.role_name, rp.privilege_name
from mo_catalog.mo_tables t
join mo_catalog.mo_role_privs rp
  on rp.obj_type = 'table'
 and rp.obj_id = t.rel_logical_id
where t.reldatabase in ('cg_src', 'cg_dst')
  and t.relname in ('src', 'dst_copy', 'dst_cross', 'dst_snapshot')
  and rp.role_name = 'cg_r_delete'
order by t.reldatabase, t.relname, rp.role_name, rp.privilege_name;
-- @session

-- Runtime check: copied select grants allow reading COPY GRANTS targets, but
-- not the plain clone.
-- @session:id=2&user=acc_clone_copy_grants_complex:cg_u_select:cg_r_select&password=111
select * from cg_src.src order by a;
select * from cg_src.dst_plain order by a;
select * from cg_src.dst_copy order by a;
select * from cg_dst.dst_cross order by a;
select * from cg_dst.dst_snapshot order by a;
select * from cg_src.dst_if_not_exists order by a;
-- @session

-- Runtime check: insert requires select too for primary-key conflict checks, so
-- this role has both select and insert. Plain clone should still reject it.
-- @session:id=3&user=acc_clone_copy_grants_complex:cg_u_insert:cg_r_insert&password=111
insert into cg_src.dst_copy values(3, 'copy');
insert into cg_dst.dst_cross values(4, 'cross');
insert into cg_dst.dst_snapshot values(5, 'snapshot');
insert into cg_src.dst_plain values(6, 'plain');
-- @session

-- Runtime check: source delete was revoked, but copied delete grants on clone
-- targets should remain usable.
-- @session:id=4&user=acc_clone_copy_grants_complex:cg_u_delete:cg_r_delete&password=111
delete from cg_src.src where a = 1;
delete from cg_src.dst_copy where a = 1;
delete from cg_dst.dst_cross where a = 1;
delete from cg_dst.dst_snapshot where a = 1;
-- @session

-- Runtime check: role inheritance should work with copied grants. The grant is
-- copied to cg_r_parent; cg_r_child inherits it.
-- @session:id=5&user=acc_clone_copy_grants_complex:cg_u_child:cg_r_child&password=111
select * from cg_src.dst_copy order by a;
select * from cg_src.dst_plain order by a;
-- @session

-- Runtime check: copied table-all privilege should allow write operations.
-- @session:id=6&user=acc_clone_copy_grants_complex:cg_u_all:cg_r_all&password=111
update cg_src.dst_copy set b = 'all-copy' where a = 2;
select * from cg_src.dst_copy order by a;
-- @session

-- Final data-shape check after all role sessions have run.
-- @session:id=1&user=acc_clone_copy_grants_complex:root1&password=111
select * from cg_src.src order by a;
select * from cg_src.dst_plain order by a;
select * from cg_src.dst_copy order by a;
select * from cg_dst.dst_cross order by a;
select * from cg_dst.dst_snapshot order by a;
-- @session

-- Semantic restriction: COPY GRANTS cannot be used when the source snapshot
-- belongs to another account, even if TO ACCOUNT is not present.
drop snapshot if exists sp_cg_cross_account;
create snapshot sp_cg_cross_account for account acc_clone_copy_grants_complex;
create database cg_sys_copy_grants;
create table cg_sys_copy_grants.cross_snapshot clone cg_src.src {snapshot = "sp_cg_cross_account"} copy grants;
drop database cg_sys_copy_grants;
drop snapshot if exists sp_cg_cross_account;

-- @session:id=1&user=acc_clone_copy_grants_complex:root1&password=111
drop snapshot if exists sp_cg_src;
drop user cg_u_select, cg_u_insert, cg_u_delete, cg_u_all, cg_u_child;
drop role cg_r_select, cg_r_insert, cg_r_delete, cg_r_all, cg_r_child, cg_r_parent, cg_r_existing_dst;
drop database cg_src;
drop database cg_dst;
-- @session

-- Semantic restriction: COPY GRANTS cannot be combined with TO ACCOUNT because
-- grants are tenant-local metadata.
create database cg_sys_copy_grants;
create table cg_sys_copy_grants.src(a int);
create table cg_sys_copy_grants.dst clone cg_sys_copy_grants.src copy grants to account acc_clone_copy_grants_complex;
drop database cg_sys_copy_grants;

drop account if exists acc_clone_copy_grants_complex;
set global enable_privilege_cache = on;
