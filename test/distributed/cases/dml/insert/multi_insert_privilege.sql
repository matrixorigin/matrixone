-- Multi-table INSERT checks SELECT on the source and INSERT on every target
-- before writing any target.
drop database if exists multi_insert_privilege_db;
drop user if exists multi_insert_privilege_user;
drop user if exists multi_insert_no_select_user;
drop role if exists multi_insert_privilege_role;
drop role if exists multi_insert_no_select_role;

create database multi_insert_privilege_db;
create table multi_insert_privilege_db.src (id int primary key, amount int);
create table multi_insert_privilege_db.allowed_target (id int primary key, amount int);
create table multi_insert_privilege_db.denied_target (id int primary key, amount int);
insert into multi_insert_privilege_db.src values (1, 10);

create role multi_insert_privilege_role;
create user multi_insert_privilege_user identified by '111' default role multi_insert_privilege_role;
grant connect on account * to multi_insert_privilege_role;
grant select on table multi_insert_privilege_db.src to multi_insert_privilege_role;
grant insert on table multi_insert_privilege_db.allowed_target to multi_insert_privilege_role;
grant multi_insert_privilege_role to multi_insert_privilege_user;

create role multi_insert_no_select_role;
create user multi_insert_no_select_user identified by '111' default role multi_insert_no_select_role;
grant connect on account * to multi_insert_no_select_role;
grant insert on table multi_insert_privilege_db.allowed_target to multi_insert_no_select_role;
grant insert on table multi_insert_privilege_db.denied_target to multi_insert_no_select_role;
grant multi_insert_no_select_role to multi_insert_no_select_user;

-- INSERT on every target does not permit reading the source. The rejected
-- statement must leave both targets unchanged.
-- @session:id=2&user=sys:multi_insert_no_select_user:multi_insert_no_select_role&password=111
insert all
  into multi_insert_privilege_db.allowed_target (id, amount) values (id + 200, amount)
  into multi_insert_privilege_db.denied_target (id, amount) values (id + 200, amount)
select id, amount from multi_insert_privilege_db.src;
-- @session
select count(*) from multi_insert_privilege_db.allowed_target;
select count(*) from multi_insert_privilege_db.denied_target;

-- Control: INSERT-only access to the single target is sufficient. The role
-- deliberately has no SELECT privilege on the target itself.
-- @session:id=1&user=sys:multi_insert_privilege_user:multi_insert_privilege_role&password=111
insert all
  into multi_insert_privilege_db.allowed_target (id, amount) values (id, amount)
select id, amount from multi_insert_privilege_db.src;
-- @session
select * from multi_insert_privilege_db.allowed_target order by id;

-- The same role has no INSERT on denied_target. The statement must be denied
-- as a whole: allowed_target cannot receive the id + 100 row before the
-- privilege check for denied_target fails.
-- @session:id=1&user=sys:multi_insert_privilege_user:multi_insert_privilege_role&password=111
insert all
  into multi_insert_privilege_db.allowed_target (id, amount) values (id + 100, amount)
  into multi_insert_privilege_db.denied_target (id, amount) values (id + 100, amount)
select id, amount from multi_insert_privilege_db.src;
-- @session
select count(*) from multi_insert_privilege_db.allowed_target;
select count(*) from multi_insert_privilege_db.denied_target;

drop database multi_insert_privilege_db;
drop user multi_insert_privilege_user;
drop user multi_insert_no_select_user;
drop role multi_insert_privilege_role;
drop role multi_insert_no_select_role;
