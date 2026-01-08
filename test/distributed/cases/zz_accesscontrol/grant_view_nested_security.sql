set global enable_privilege_cache = off;

-- cleanup
drop user if exists user_def_a, user_def_b, user_def_c, user_query;
drop role if exists role_def_a, role_def_b, role_def_c, role_query;
drop database if exists db_nested;

-- setup
create role role_def_a;
create role role_def_b;
create role role_def_c;
create role role_query;

create user user_def_a identified by '111' default role role_def_a;
create user user_def_b identified by '111' default role role_def_b;
create user user_def_c identified by '111' default role role_def_c;
create user user_query identified by '111' default role role_query;

grant role_def_a to user_def_a;
grant role_def_b to user_def_b;
grant role_def_c to user_def_c;
grant role_query to user_query;

create database db_nested;
use db_nested;
create table t_base(id int);
insert into t_base values (1), (2);

grant connect on account * to role_def_a;
grant connect on account * to role_def_b;
grant connect on account * to role_def_c;
grant connect on account * to role_query;

grant create view on database db_nested to role_def_a;
grant create view on database db_nested to role_def_b;
grant create view on database db_nested to role_def_c;

-- allow user_def_a to create v1
grant select on table db_nested.t_base to role_def_a;

-- @session:id=1&user=user_def_a&password=111
use db_nested;
set view_security_type = 'DEFINER';
create view v1 as select * from t_base;
-- @session

-- allow user_def_b to create v2
grant select on view db_nested.v1 to role_def_b;

-- @session:id=2&user=user_def_b&password=111
use db_nested;
set view_security_type = 'DEFINER';
create view v2 as select * from v1;
-- @session

-- allow user_def_c to create v3
grant select on view db_nested.v2 to role_def_c;

-- @session:id=3&user=user_def_c&password=111
use db_nested;
set view_security_type = 'DEFINER';
create view v3 as select * from v2;
-- @session

grant select on view db_nested.v2 to role_query;
grant select on view db_nested.v3 to role_query;

-- ==========================================================
-- Case 1: Root definer has table privilege, intermediate definer does not
-- ==========================================================
revoke select on table db_nested.t_base from role_def_a;
grant select on table db_nested.t_base to role_def_b;

-- @session:id=4&user=user_query&password=111
use db_nested;
select * from v2;
-- @session

-- ==========================================================
-- Case 2: Root definer lacks table privilege, intermediate definer has it
-- ==========================================================
revoke select on table db_nested.t_base from role_def_b;
grant select on table db_nested.t_base to role_def_a;

-- @session:id=5&user=user_query&password=111
use db_nested;
select * from v2;
-- @session

-- ==========================================================
-- Case 3: Three-level chain with only root definer holding table privilege
-- ==========================================================
revoke select on table db_nested.t_base from role_def_a;
grant select on table db_nested.t_base to role_def_c;

-- @session:id=6&user=user_query&password=111
use db_nested;
select * from v3;
-- @session

-- ==========================================================
-- Case 4: Three-level chain with only bottom definer holding table privilege
-- ==========================================================
revoke select on table db_nested.t_base from role_def_c;
grant select on table db_nested.t_base to role_def_a;

-- @session:id=7&user=user_query&password=111
use db_nested;
select * from v3;
-- @session

-- cleanup
drop user user_def_a;
drop user user_def_b;
drop user user_def_c;
drop user user_query;
drop role role_def_a;
drop role role_def_b;
drop role role_def_c;
drop role role_query;
drop database db_nested;
set global enable_privilege_cache = on;
