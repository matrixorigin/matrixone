-- Table-level privileges govern SHOW metadata and are refreshed in the same session.
drop account if exists show_meta_acc;
create account show_meta_acc admin_name 'admin' identified by '111';
-- @session:id=1&user=show_meta_acc:admin&password=111
create database show_meta_db;
create table show_meta_db.allowed (id int);
create table show_meta_db.denied (id int);
create role show_meta_role;
create user show_meta_user identified by '111' default role show_meta_role;
grant connect on account * to show_meta_role;
grant select on table show_meta_db.allowed to show_meta_role;
-- @session
-- @session:id=2&user=show_meta_acc:show_meta_user:show_meta_role&password=111
select * from show_meta_db.allowed;
show tables from show_meta_db;
show columns from show_meta_db.allowed;
show create table show_meta_db.allowed;
show create table show_meta_db.denied;
show tables from show_meta_db where exists (select 1 from show_meta_db.denied);
show columns from show_meta_db.allowed where exists (select 1 from show_meta_db.denied);
prepare meta_tables from 'show tables from show_meta_db';
execute meta_tables;
-- @session
-- @session:id=1&user=show_meta_acc:admin&password=111
revoke select on table show_meta_db.allowed from show_meta_role;
-- @session
-- @session:id=2&user=show_meta_acc:show_meta_user:show_meta_role&password=111
show columns from show_meta_db.allowed;
show create table show_meta_db.allowed;
execute meta_tables;
-- @session
-- @session:id=1&user=show_meta_acc:admin&password=111
grant select on table show_meta_db.allowed to show_meta_role;
-- @session
-- @session:id=2&user=show_meta_acc:show_meta_user:show_meta_role&password=111
show columns from show_meta_db.allowed;
execute meta_tables;
deallocate prepare meta_tables;
-- @session
drop account show_meta_acc;
