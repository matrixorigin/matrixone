-- Historical data and schema do not preserve revoked execution privileges.
drop snapshot if exists view_metadata_auth_snapshot;
drop user if exists view_metadata_auth_reader;
drop role if exists view_metadata_auth_role;
drop database if exists view_metadata_auth;
create database view_metadata_auth;
create table view_metadata_auth.src (id int, label varchar(8));
insert into view_metadata_auth.src values (1, 'old');
create view view_metadata_auth.v as select id, label from view_metadata_auth.src;
create role view_metadata_auth_role;
create user view_metadata_auth_reader identified by '111' default role view_metadata_auth_role;
grant connect on account * to view_metadata_auth_role;
grant show tables on database view_metadata_auth to view_metadata_auth_role;
grant select on view view_metadata_auth.v to view_metadata_auth_role;
create snapshot view_metadata_auth_snapshot for account sys;
update view_metadata_auth.src set id = 2, label = 'new';
alter table view_metadata_auth.src modify column label varchar(32);

-- Current and historical results establish the two independent data domains.
-- @session:id=1&user=sys:view_metadata_auth_reader&password=111
select * from view_metadata_auth.v;
select * from view_metadata_auth.v {snapshot = 'view_metadata_auth_snapshot'};
select character_maximum_length from information_schema.columns
where table_schema = 'view_metadata_auth' and table_name = 'v' and column_name = 'label';
select character_maximum_length from information_schema.columns {snapshot = 'view_metadata_auth_snapshot'}
where table_schema = 'view_metadata_auth' and table_name = 'v' and column_name = 'label';
select * from view_metadata_auth.src;
-- @session

revoke select on view view_metadata_auth.v from view_metadata_auth_role;
-- SHOW visibility must not authorize either current or historical SELECT.
-- @session:id=2&user=sys:view_metadata_auth_reader&password=111
select * from view_metadata_auth.v;
select * from view_metadata_auth.v {snapshot = 'view_metadata_auth_snapshot'};
select character_maximum_length from information_schema.columns {snapshot = 'view_metadata_auth_snapshot'}
where table_schema = 'view_metadata_auth' and table_name = 'v' and column_name = 'label';
-- @session

grant select on view view_metadata_auth.v to view_metadata_auth_role;
-- A new grant permits the old data without silently promoting its snapshot.
-- @session:id=3&user=sys:view_metadata_auth_reader&password=111
select * from view_metadata_auth.v {snapshot = 'view_metadata_auth_snapshot'};
select * from view_metadata_auth.v;
-- @session

drop snapshot view_metadata_auth_snapshot;
drop user view_metadata_auth_reader;
drop role view_metadata_auth_role;
drop database view_metadata_auth;
