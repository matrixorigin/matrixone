set global enable_privilege_cache = off;
drop database if exists secondary_role_disabled_db;
drop user if exists secondary_role_disabled_user;
drop role if exists secondary_role_disabled_primary,secondary_role_disabled_reader,secondary_role_disabled_writer;

create database secondary_role_disabled_db;
create table secondary_role_disabled_db.t_read(a int);
create table secondary_role_disabled_db.t_write(a int);
insert into secondary_role_disabled_db.t_read values (1);
create role secondary_role_disabled_primary,secondary_role_disabled_reader,secondary_role_disabled_writer;
create user secondary_role_disabled_user identified by '123456' default role secondary_role_disabled_primary;
grant connect on account * to secondary_role_disabled_primary;
grant select on table secondary_role_disabled_db.t_read to secondary_role_disabled_reader;
grant insert on table secondary_role_disabled_db.t_write to secondary_role_disabled_writer;
grant secondary_role_disabled_reader,secondary_role_disabled_writer to secondary_role_disabled_user;

-- @session:id=2&user=sys:secondary_role_disabled_user:secondary_role_disabled_primary&password=123456
select current_role();
-- ALL/NONE remain syntax-compatible no-ops. Neither one can merge the
-- independently granted reader/writer roles into the current primary role.
set secondary role all;
select current_role();
select * from secondary_role_disabled_db.t_read;
insert into secondary_role_disabled_db.t_write values (1);
set secondary role none;
select current_role();
set role secondary_role_disabled_reader;
select current_role();
select * from secondary_role_disabled_db.t_read;
-- ALL also has no effect after an explicit role switch: reader cannot use the
-- separately granted writer privilege.
set secondary role all;
select current_role();
insert into secondary_role_disabled_db.t_write values (2);
set role secondary_role_disabled_writer;
select current_role();
insert into secondary_role_disabled_db.t_write values (3);
-- NONE does not switch back to the primary role or revoke the current role.
set secondary role none;
select current_role();
insert into secondary_role_disabled_db.t_write values (4);
select * from secondary_role_disabled_db.t_read;
-- @session

select * from secondary_role_disabled_db.t_write order by a;

drop database secondary_role_disabled_db;
drop user secondary_role_disabled_user;
drop role secondary_role_disabled_primary,secondary_role_disabled_reader,secondary_role_disabled_writer;
set global enable_privilege_cache = on;
