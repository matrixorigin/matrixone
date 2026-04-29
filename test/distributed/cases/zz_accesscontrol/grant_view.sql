set global enable_privilege_cache = off;

-- cleanup
drop user if exists user_v1, user_no_db, user_invoker;
drop role if exists role_v1, role_no_db, role_invoker;
drop database if exists db_v1;

-- setup
create role role_v1;
create user user_v1 identified by '111' default role role_v1;
grant role_v1 to user_v1;

create database db_v1;
use db_v1;
create table t1 (a int, b varchar(20));
insert into t1 values (1, 'base');
create view v1 as select * from t1;
create view v_on_v as select * from v1;

grant select on view db_v1.v1 to role_v1;
grant connect on account * to role_v1;

-- @session:id=1&user=user_v1&password=111
select current_role();
use db_v1;
select * from v1;
-- @session

create role role_no_db;
create user user_no_db identified by '111' default role role_no_db;
grant role_no_db to user_no_db;
grant select on view db_v1.v1 to role_no_db;

-- @session:id=2&user=user_no_db&password=111
use db_v1;
select * from db_v1.v1;
-- @session

grant select on view db_v1.v_on_v to role_v1;

-- @session:id=1&user=user_v1&password=111
use db_v1;
select * from v_on_v;
-- @session

use db_v1;
set view_security_type = 'INVOKER';
create view v_invoker as select * from t1;
set view_security_type = 'DEFINER';

-- verify SHOW CREATE VIEW shows SQL SECURITY INVOKER even though DDL didn't have it
show create view v_invoker;

create role role_invoker;
create user user_invoker identified by '111' default role role_invoker;
grant role_invoker to user_invoker;
grant connect on account * to role_invoker;
grant select on view db_v1.v_invoker to role_invoker;

-- @session:id=3&user=user_invoker&password=111
use db_v1;
select * from v_invoker;
-- @session

grant select on table db_v1.t1 to role_invoker;

-- @session:id=3&user=user_invoker&password=111
use db_v1;
select * from v_invoker;
-- @session

grant all on view db_v1.v1 to role_v1;
show grants for role_v1;

revoke all on view db_v1.v1 from role_v1;
grant select on view db_v1.v1 to role_v1;
revoke select on view db_v1.v1 from role_v1;

-- @session:id=1&user=user_v1&password=111
use db_v1;
select * from v1;
-- @session

-- ============================================================
-- Regression: base table ALL/OWNERSHIP must NOT bypass view
-- ============================================================
drop role if exists role_bypass;
drop user if exists user_bypass;
create role role_bypass;
create user user_bypass identified by '111' default role role_bypass;
grant role_bypass to user_bypass;
grant connect on account * to role_bypass;
-- grant ALL on the base table, but nothing on the view
grant all on table db_v1.t1 to role_bypass;

-- @session:id=4&user=user_bypass&password=111
use db_v1;
-- should fail: no view privilege, even though base table has ALL
select * from v1;
-- @session

-- also test OWNERSHIP: grant ownership on base table
grant ownership on table db_v1.t1 to role_bypass;

-- @session:id=4&user=user_bypass&password=111
use db_v1;
-- should still fail: ownership on base table doesn't grant view access
select * from v1;
-- @session

drop user user_bypass;
drop role role_bypass;

-- ============================================================
-- Regression: DEFINER view with role inheritance
-- ============================================================
drop role if exists role_parent, role_child;
drop user if exists user_definer_inh, user_definer_reader;
create role role_parent;
create role role_child;
grant role_parent to role_child;
-- base table privilege on parent role (role_child inherits this)
grant select on table db_v1.t1 to role_parent;

create user user_definer_inh identified by '111' default role role_child;
grant role_child to user_definer_inh;
grant connect on account * to role_child;
-- let role_child create views in db_v1
grant create view on database db_v1 to role_child;
grant show tables on database db_v1 to role_child;

-- user_definer_inh creates the DEFINER view, so definer = role_child
-- role_child itself has SELECT on t1 via inheriting role_parent
-- @session:id=5&user=user_definer_inh&password=111
use db_v1;
create sql security definer view v_definer_inh as select * from t1;
-- @session

-- now a separate user with only view privilege should succeed,
-- because the definer (role_child) has SELECT on t1 via role_parent inheritance
create role role_reader;
create user user_definer_reader identified by '111' default role role_reader;
grant role_reader to user_definer_reader;
grant connect on account * to role_reader;
grant select on view db_v1.v_definer_inh to role_reader;

-- @session:id=7&user=user_definer_reader&password=111
use db_v1;
select * from v_definer_inh;
-- @session

drop user user_definer_inh;
drop user user_definer_reader;
drop role role_parent;
drop role role_child;
drop role role_reader;
drop view db_v1.v_definer_inh;

-- ============================================================
-- Regression: WGO object-level check
-- ============================================================
drop role if exists role_wgo_src, role_wgo_tgt;
drop user if exists user_wgo;
create role role_wgo_src;
create role role_wgo_tgt;
create user user_wgo identified by '111' default role role_wgo_src;
grant role_wgo_src to user_wgo;
grant connect on account * to role_wgo_src;

-- give WGO on v1 only
grant select on view db_v1.v1 to role_wgo_src with grant option;

-- user_wgo should be able to grant on v1
-- @session:id=6&user=user_wgo&password=111
grant select on view db_v1.v1 to role_wgo_tgt;
-- @session

-- but should NOT be able to grant on v_on_v (no WGO on that object)
-- @session:id=6&user=user_wgo&password=111
grant select on view db_v1.v_on_v to role_wgo_tgt;
-- @session

drop user user_wgo;
drop role role_wgo_src;
drop role role_wgo_tgt;

-- ============================================================
-- Regression: ALTER VIEW preserves SecurityType
-- ============================================================
use db_v1;
-- Use explicit DDL syntax so SQL SECURITY appears in the stored Stmt
create sql security invoker view v_alter_sec as select * from t1;

-- verify SHOW CREATE VIEW preserves SQL SECURITY from DDL
show create view v_alter_sec;

drop view v_alter_sec;

-- ============================================================
-- Regression: SHOW CREATE VIEW with SQL SECURITY in view body
-- should not confuse the output (body is just data, not DDL)
-- ============================================================
use db_v1;
create sql security invoker view v_body_trick as select 'sql security definer' as note from t1;

-- The header should still get SQL SECURITY INVOKER spliced in,
-- even though the view body contains the text 'sql security'
show create view v_body_trick;

drop view v_body_trick;

-- cleanup
drop user user_v1;
drop user user_no_db;
drop user user_invoker;
drop role role_v1;
drop role role_no_db;
drop role role_invoker;
drop database db_v1;
set global enable_privilege_cache = on;
