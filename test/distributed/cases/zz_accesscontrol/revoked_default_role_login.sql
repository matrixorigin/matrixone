set global enable_privilege_cache = on;
drop database if exists revoked_default_role_db;
drop user if exists revoked_default_role_user;
drop role if exists revoked_default_role_reader;
drop role if exists revoked_default_role_secondary;

create database revoked_default_role_db;
create table revoked_default_role_db.protected(value int);
insert into revoked_default_role_db.protected values (27651);
create role revoked_default_role_reader;
create role revoked_default_role_secondary;
create user revoked_default_role_user identified by '123456' default role revoked_default_role_reader;
grant connect on account * to revoked_default_role_reader;
grant connect on account * to revoked_default_role_secondary;
grant select on table revoked_default_role_db.protected to revoked_default_role_reader;
grant revoked_default_role_reader to revoked_default_role_user;
grant revoked_default_role_secondary to revoked_default_role_user;

-- Existing implicit session starts with the granted default role.
-- @session:id=2&user=sys:revoked_default_role_user&password=123456
select current_role();
select * from revoked_default_role_db.protected;
-- @session

-- Revoking a non-default secondary role must not disturb the valid default.
-- @session:id=6&user=sys:revoked_default_role_user&password=123456
set role revoked_default_role_secondary;
select current_role();
-- @session
revoke revoked_default_role_secondary from revoked_default_role_user;
-- @session:id=7&user=sys:revoked_default_role_user&password=123456
select current_role();
select * from revoked_default_role_db.protected;
set role revoked_default_role_secondary;
-- @session

revoke revoked_default_role_reader from revoked_default_role_user;

-- A new implicit session must fall back to public and cannot recover the role
-- through SET ROLE or use its protected table privilege.
-- @session:id=3&user=sys:revoked_default_role_user&password=123456
select current_role();
select * from revoked_default_role_db.protected;
set role revoked_default_role_reader;
-- @session

-- The separate existing-session revocation behavior is intentionally unchanged.
-- @session:id=2&user=sys:revoked_default_role_user&password=123456
select current_role();
select * from revoked_default_role_db.protected;
-- @session

grant revoked_default_role_reader to revoked_default_role_user;

-- Regranting restores the stored default for both implicit and explicit login.
-- @session:id=4&user=sys:revoked_default_role_user&password=123456
select current_role();
select * from revoked_default_role_db.protected;
-- @session
-- @session:id=5&user=sys:revoked_default_role_user:revoked_default_role_reader&password=123456
select current_role();
select * from revoked_default_role_db.protected;
-- @session

drop user revoked_default_role_user;
drop role revoked_default_role_reader;
drop role revoked_default_role_secondary;
drop database revoked_default_role_db;
