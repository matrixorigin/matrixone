set global enable_privilege_cache = off;
drop database if exists access_insert_db;
drop user if exists access_insert_user;
drop role if exists access_insert_role;

create database access_insert_db;
create table access_insert_db.t (
    id int primary key,
    note varchar(32) not null
);
create table access_insert_db.t_no_pk (
    id int,
    note varchar(32) not null
);
create table access_insert_db.src (
    id int,
    note varchar(32) not null
);
create table access_insert_db.src_secret (
    id int,
    note varchar(32) not null
);
create table access_insert_db.src_allowed (
    id int,
    note varchar(32) not null
);
insert into access_insert_db.src values (3, 'source-row');
insert into access_insert_db.src_secret values (4, 'secret-row');
insert into access_insert_db.src_allowed values (4, 'allowed-row');

create role access_insert_role;
create user access_insert_user identified by '123456' default role access_insert_role;
grant connect on account * to access_insert_role;
grant insert on table access_insert_db.t to access_insert_role;
grant insert on table access_insert_db.t_no_pk to access_insert_role;
grant select on table access_insert_db.src_allowed to access_insert_role;
grant access_insert_role to access_insert_user;

-- @session:id=2&user=sys:access_insert_user:access_insert_role&password=123456
select current_role();
-- The DEDUP target scan is an internal write implementation detail and must
-- not require SELECT on the target table.
insert into access_insert_db.t values (1, 'insert-only');
-- Control: a table without a key also accepts INSERT-only access.
insert into access_insert_db.t_no_pk values (2, 'no-pk');
-- A SQL-visible source scan still requires SELECT and must leave the target
-- unchanged when it is rejected.
insert into access_insert_db.t_no_pk select * from access_insert_db.src;
-- A user-visible DEDUP JOIN is also a source read. SELECT on src_allowed must
-- not hide the missing SELECT privilege on src_secret.
insert into access_insert_db.t_no_pk
select s.id, s.note
from access_insert_db.src_secret s dedup join access_insert_db.src_allowed a on s.id = a.id;
-- @session

select count(*) from access_insert_db.t;
select count(*) from access_insert_db.t_no_pk;

drop database access_insert_db;
drop user access_insert_user;
drop role access_insert_role;
set global enable_privilege_cache = on;
