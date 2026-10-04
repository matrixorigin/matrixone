drop user if exists issue27650_user;
drop role if exists issue27650_target;
drop role if exists issue27650_reader;
drop database if exists issue27650_db;

create role issue27650_reader;
create role issue27650_target;
create database issue27650_db;
create table issue27650_db.t(v int);
insert into issue27650_db.t values (27650);
create user issue27650_user identified by '123456' default role issue27650_reader;
grant connect on account * to issue27650_reader;
grant select on table issue27650_db.t to issue27650_reader with grant option;
grant issue27650_reader to issue27650_user;

-- Keep this connection and its prepared statement open across the main
-- session's REVOKE and re-grant.
-- @session:id=1&user=sys:issue27650_user:issue27650_reader&password=123456{
set session enable_privilege_cache = on;
select current_role();
prepare issue27650_stmt from 'select v from issue27650_db.t';
execute issue27650_stmt;
begin;
-- @session}

-- Exercise the cache OFF -> SET ROLE -> REVOKE -> ON transition on a second
-- already-open connection, including both text and prepared authorization.
-- @session:id=4&user=sys:issue27650_user:issue27650_reader&password=123456{
set session enable_privilege_cache = off;
set role issue27650_reader;
prepare issue27650_toggle_stmt from 'select v from issue27650_db.t';
execute issue27650_toggle_stmt;
-- @session}

revoke issue27650_reader from issue27650_user;
select count(*) from mo_catalog.mo_user_grant ug
join mo_catalog.mo_role r on ug.role_id = r.role_id
join mo_catalog.mo_user u on ug.user_id = u.user_id
where r.role_name = 'issue27650_reader' and u.user_name = 'issue27650_user';

-- @session:id=4&user=sys:issue27650_user:issue27650_reader&password=123456{
set session enable_privilege_cache = on;
select v from issue27650_db.t;
execute issue27650_toggle_stmt;
-- @session}

-- @session:id=1&user=sys:issue27650_user:issue27650_reader&password=123456{
set session clear_privilege_cache = on;
select v from issue27650_db.t;
execute issue27650_stmt;
rollback;
grant select on table issue27650_db.t to issue27650_target;
set role public;
select current_role();
set role issue27650_reader;
-- @session}

-- A new implicit-role connection must not regain the revoked role's table
-- privilege on its first protected statement.
-- @session:id=2&user=sys:issue27650_user&password=123456
set session enable_privilege_cache = on;
select v from issue27650_db.t;
-- @session

grant issue27650_reader to issue27650_user;
begin;
commit;

-- @session:id=1&user=sys:issue27650_user:issue27650_reader&password=123456{
set role issue27650_reader;
execute issue27650_stmt;
-- @session}

-- A new explicit-role connection is valid again after the committed re-grant.
-- @session:id=3&user=sys:issue27650_user:issue27650_reader&password=123456
set session enable_privilege_cache = on;
select current_role();
select v from issue27650_db.t;
-- @session

drop user if exists issue27650_user;
drop role if exists issue27650_target;
drop role if exists issue27650_reader;
drop database if exists issue27650_db;
