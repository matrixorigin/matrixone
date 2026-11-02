drop database if exists issue_30677_db;
drop user if exists issue_30677_user;
drop user if exists issue_30677_actor;
drop user if exists issue_30677_target;
drop user if exists issue_30677_batch_a;
drop user if exists issue_30677_batch_b;
drop role if exists issue_30677_reader;
drop role if exists issue_30677_ungranted;
drop role if exists issue_30677_target_reader;
drop role if exists issue_30677_batch_reader;

create database issue_30677_db;
create table issue_30677_db.protected(value int);
insert into issue_30677_db.protected values (42);
create role issue_30677_reader;
create role issue_30677_ungranted;
create role issue_30677_target_reader;
create role issue_30677_batch_reader;
grant connect on account * to issue_30677_reader;
grant select on table issue_30677_db.protected to issue_30677_reader;
grant connect on account * to issue_30677_target_reader;
grant connect on account * to issue_30677_batch_reader;
create user issue_30677_user identified by 'pw30677';
create user issue_30677_actor identified by 'actor30677';
create user issue_30677_target identified by 'target30677';
create user issue_30677_batch_a identified by 'batch30677';
create user issue_30677_batch_b identified by 'batch30677';
grant issue_30677_reader to issue_30677_user;
grant issue_30677_target_reader to issue_30677_target;
grant issue_30677_batch_reader to issue_30677_batch_a;

-- A user without ALTER USER can select their own directly granted role.
-- The currently open session remains public; a fresh implicit login gets reader.
-- @session:id=91&user=sys:issue_30677_user&password=pw30677
select current_role();
set default role issue_30677_reader to issue_30677_user;
select current_role();
select * from issue_30677_db.protected;
-- @session

-- NONE stores public for future logins without changing the active role here.
-- @session:id=92&user=sys:issue_30677_user&password=pw30677
select current_role();
select * from issue_30677_db.protected;
set default role none to issue_30677_user;
select current_role();
select * from issue_30677_db.protected;
set default role issue_30677_ungranted to issue_30677_user;
-- @session

-- The next implicit login uses public again, and unprivileged other-user changes fail.
-- @session:id=93&user=sys:issue_30677_user&password=pw30677
select current_role();
select * from issue_30677_db.protected;
-- @session
-- @session:id=94&user=sys:issue_30677_actor&password=actor30677
set default role issue_30677_target_reader to issue_30677_target;
select current_role();
-- @session

select u.user_name, r.role_name from mo_catalog.mo_user u join mo_catalog.mo_role r on u.default_role = r.role_id where u.user_name in ('issue_30677_user', 'issue_30677_target') order by u.user_name;

-- Root can set another user's directly granted role after the unprivileged denial.
set default role issue_30677_target_reader to issue_30677_target;
select u.user_name, r.role_name from mo_catalog.mo_user u join mo_catalog.mo_role r on u.default_role = r.role_id where u.user_name = 'issue_30677_target';

-- ALL and multiple defaults are explicitly unsupported. Multi-target writes validate first.
set default role all to issue_30677_user;
set default role issue_30677_reader, issue_30677_batch_reader to issue_30677_user;
set default role issue_30677_batch_reader to issue_30677_batch_a, issue_30677_batch_b;
select u.user_name, r.role_name from mo_catalog.mo_user u join mo_catalog.mo_role r on u.default_role = r.role_id where u.user_name in ('issue_30677_batch_a', 'issue_30677_batch_b') order by u.user_name;

drop user issue_30677_user;
drop user issue_30677_actor;
drop user issue_30677_target;
drop user issue_30677_batch_a;
drop user issue_30677_batch_b;
drop role issue_30677_reader;
drop role issue_30677_ungranted;
drop role issue_30677_target_reader;
drop role issue_30677_batch_reader;
drop database issue_30677_db;
