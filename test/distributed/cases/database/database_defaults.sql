drop database if exists defaults_a;
drop database if exists defaults_b;
drop database if exists defaults_clone;
drop snapshot if exists defaults_snapshot;
drop user if exists defaults_user;
drop role if exists defaults_role;

-- A missing target must fail.
alter database defaults_missing collate utf8mb4_bin;
create database defaults_a;
create database defaults_b;
use defaults_a;
create table old_values(v varchar(8));
insert into old_values values ('a'),('b'),('c'),('E'),('C'),('D');
select min(v), max(v) from old_values;

-- Changing B while using A must affect only B, including a new connection.
alter database defaults_b character set utf8mb4 collate utf8mb4_bin;
select schema_name, default_character_set_name, default_collation_name
from information_schema.schemata where schema_name in ('defaults_a','defaults_b') order by schema_name;
create table defaults_b.binary_values(v varchar(8));
insert into defaults_b.binary_values select v from old_values;
select collation(v), min(v), max(v) from defaults_b.binary_values group by collation(v);
show create database defaults_b;
show create table defaults_b.binary_values;
select @@collation_database, @@collation_server;
use defaults_b;
select @@character_set_database, @@collation_database, @@collation_server;
show variables like 'collation_database';
-- @session:id=1&user=sys:dump&password=111
use defaults_b;
create table cross_session(v varchar(8));
insert into cross_session values ('a'),('B');
select collation(v), min(v), max(v) from cross_session group by collation(v);
-- @session

-- Explicit table/column choices and existing tables keep their own defaults.
create table explicit_table(v varchar(8)) collate utf8mb4_general_ci;
create table explicit_column(v varchar(8) collate utf8mb4_general_ci);
create table copied_table like defaults_a.old_values;
create table selected_table as select v from defaults_a.old_values;
create temporary table temporary_values(v varchar(8));
insert into temporary_values values ('a'),('B');
select collation(v),min(v),max(v) from temporary_values group by collation(v);
drop table temporary_values;
alter table defaults_a.old_values add column added varchar(8);
select table_schema,table_name,column_name,collation_name from information_schema.columns
where table_schema in ('defaults_a','defaults_b') and column_name in ('v','added') order by table_schema,table_name,column_name;
select min(v),max(v) from defaults_a.old_values;

-- Both changes in one explicit transaction and rollback are observable.
begin;
alter schema default collate = utf8mb4_general_ci;
select @@collation_database;
create table inside_rollback(v varchar(8));
select collation_name from information_schema.columns where table_schema='defaults_b' and table_name='inside_rollback' and column_name='v';
rollback;
select @@collation_database;
begin;
alter database defaults_b character set utf8mb4;
commit;
select @@collation_database;
alter database defaults_b collate utf8mb4_bin;
alter database defaults_b collate utf8mb4_bin;

-- A prepared CREATE must bind the current database default at EXECUTE.
prepare defaults_create from 'create table defaults_b.prepared_values(v varchar(8))';
-- @session:id=1&user=sys:dump&password=111
alter database defaults_b collate utf8mb4_general_ci;
-- @session
execute defaults_create;
deallocate prepare defaults_create;
select collation_name from information_schema.columns where table_schema='defaults_b' and table_name='prepared_values' and column_name='v';
prepare defaults_alter from 'alter database defaults_b collate utf8mb4_bin';
execute defaults_alter;
deallocate prepare defaults_alter;
select @@collation_database;

-- Invalid options never change the persisted value.
alter database defaults_b character set latin1;
alter database defaults_b collate utf8mb4_0900_ai_ci;
alter database defaults_b collate utf8mb4_bin collate utf8mb4_general_ci;
alter database mo_catalog collate utf8mb4_bin;
select @@collation_database;

-- Database privilege is required at execution, including after PREPARE.
create role defaults_role;
create user defaults_user identified by '111' default role defaults_role;
grant connect on account * to defaults_role;
-- @session:id=2&user=sys:defaults_user:defaults_role&password=111
alter database defaults_b collate utf8mb4_general_ci;
-- @session
grant all on database defaults_b to defaults_role;
-- @session:id=2&user=sys:defaults_user:defaults_role&password=111
use defaults_b;
alter database collate utf8mb4_general_ci;
prepare denied_after_revoke from 'alter database defaults_b collate utf8mb4_bin';
-- @session
revoke all on database defaults_b from defaults_role;
-- @session:id=2&user=sys:defaults_user:defaults_role&password=111
execute denied_after_revoke;
deallocate prepare denied_after_revoke;
-- @session
select @@collation_database;
drop user defaults_user;
drop role defaults_role;

-- Snapshot/clone remap database identity and retain the source default.
alter database defaults_b collate utf8mb4_bin;
create snapshot defaults_snapshot for database defaults_b;
alter database defaults_b collate utf8mb4_general_ci;
create database defaults_clone clone defaults_b {snapshot='defaults_snapshot'};
create table defaults_clone.after_clone(v varchar(8));
select collation_name from information_schema.columns where table_schema='defaults_clone' and table_name='after_clone' and column_name='v';
restore table defaults_b.binary_values {snapshot='defaults_snapshot'};
select @@collation_database;
restore database defaults_b {snapshot='defaults_snapshot'};
use defaults_b;
select @@collation_database;
create table after_restore(v varchar(8));
insert into after_restore values ('a'),('B');
select min(v),max(v) from after_restore;
drop snapshot defaults_snapshot;
drop database defaults_clone;

-- Same-name recreation must not inherit the dropped database's row.
drop database defaults_b;
create database defaults_b;
use defaults_b;
select @@collation_database;
create table after_recreate(v varchar(8));
select collation_name from information_schema.columns where table_schema='defaults_b' and table_name='after_recreate' and column_name='v';
drop database defaults_b;
drop database defaults_a;
