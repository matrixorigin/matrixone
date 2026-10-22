set global enable_privilege_cache = off;

-- cleanup
drop account if exists acc_v1;

-- setup: create tenant
create account acc_v1 admin_name = 'admin' identified by '111';

-- @session:id=1&user=acc_v1:admin&password=111
create role role_v1;
create user user_v1 identified by '111' default role role_v1;
grant role_v1 to user_v1;

create database db_v1;
use db_v1;
create table t1 (a int);
insert into t1 values (1);
create view v1 as select * from t1;

-- grant permissions
grant connect on account * to role_v1;
grant select on view db_v1.v1 to role_v1;

-- @session:id=2&user=acc_v1:user_v1&password=111
use db_v1;
-- verify select succeeds
select * from v1;
-- @session

-- back to tenant admin
-- @session:id=1&user=acc_v1:admin&password=111
revoke select on view db_v1.v1 from role_v1;
-- @session

-- verify revoked in NEW session to avoid session-level cache issues
-- @session:id=3&user=acc_v1:user_v1&password=111
use db_v1;
-- should fail
select * from v1;
-- @session

-- Index JOIN scans must keep the same view authorization as their base scan (#30785).
-- @session:id=1&user=acc_v1:admin&password=111
create table db_v1.t_index(id int primary key, a int, key idx_a(a));
insert into db_v1.t_index values (42, 1), (43, 2);
create view db_v1.v_index as select i.id from db_v1.t_index i force index for join(idx_a) join db_v1.t1 b on i.a = b.a;
grant select on view db_v1.v_index to role_v1;
-- @session

-- @session:id=4&user=acc_v1:user_v1&password=111
select * from db_v1.v_index;
select * from db_v1.t_index;
-- @session

-- A nested DEFINER may access the index view without granting that inner view to the caller.
-- @session:id=1&user=acc_v1:admin&password=111
create view db_v1.v_outer as select * from db_v1.v_index;
grant select on view db_v1.v_outer to role_v1;
revoke select on view db_v1.v_index from role_v1;
set view_security_type = 'INVOKER';
create view db_v1.v_invoker as select i.id from db_v1.t_index i force index for join(idx_a) join db_v1.t1 b on i.a = b.a;
set view_security_type = 'DEFINER';
grant select on view db_v1.v_invoker to role_v1;
-- @session

-- @session:id=5&user=acc_v1:user_v1&password=111
select * from db_v1.v_outer;
select * from db_v1.v_index;
select * from db_v1.v_invoker;
-- @session

-- Replacement preserves logical identity and the complete existing grant (#30786).
-- @session:id=1&user=acc_v1:admin&password=111
grant select on view db_v1.v1 to role_v1 with grant option;
set @old_physical_id = (select rel_id from mo_catalog.mo_tables where reldatabase = 'db_v1' and relname = 'v1');
set @old_logical_id = (select rel_logical_id from mo_catalog.mo_tables where reldatabase = 'db_v1' and relname = 'v1');
create or replace view db_v1.v1 as select a + 1 as a from db_v1.t1;
select rel_id <> @old_physical_id as physical_changed, rel_logical_id = @old_logical_id as logical_preserved from mo_catalog.mo_tables where reldatabase = 'db_v1' and relname = 'v1';
select count(*) as retained_grant from mo_catalog.mo_role_privs where role_name = 'role_v1' and obj_type = 'view' and obj_id = @old_logical_id and privilege_name = 'select' and with_grant_option = true;
-- @session

-- @session:id=6&user=acc_v1:user_v1&password=111
select * from db_v1.v1;
-- @session

-- Failed replacement and transaction rollback must preserve the previous definition/grant.
-- @session:id=1&user=acc_v1:admin&password=111
create or replace view db_v1.v1 as select * from db_v1.missing_table;
begin;
create or replace view db_v1.v1 as select a + 2 as a from db_v1.t1;
rollback;
create or replace view db_v1.t1 as select 7 as a;
select * from db_v1.t1;
-- @session

-- @session:id=7&user=acc_v1:user_v1&password=111
select * from db_v1.v1;
-- @session

-- A real DROP/CREATE must not inherit grants from the dropped view.
-- @session:id=1&user=acc_v1:admin&password=111
drop view db_v1.v1;
create view db_v1.v1 as select * from db_v1.t1;
select count(*) as recreated_view_grants from mo_catalog.mo_role_privs where role_name = 'role_v1' and obj_type = 'view' and obj_id = (select rel_logical_id from mo_catalog.mo_tables where reldatabase = 'db_v1' and relname = 'v1');
-- @session

-- @session:id=8&user=acc_v1:user_v1&password=111
select * from db_v1.v1;
-- @session

-- cleanup
-- @session:id=1&user=acc_v1:admin&password=111
drop database db_v1;
-- @session

drop account acc_v1;
set global enable_privilege_cache = on;
