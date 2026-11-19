-- A named account snapshot chooses the metadata namespace, not the principal.
drop snapshot if exists view_metadata_namespace_history;
drop snapshot if exists view_metadata_namespace_private;
drop account if exists view_metadata_namespace_reader;
create account view_metadata_namespace_reader admin_name 'admin' identified by '111';
-- @session:id=1&user=view_metadata_namespace_reader:admin&password=111
create database namespace_source;
create table namespace_source.src (x varchar(8));
insert into namespace_source.src values ('old');
create view namespace_source.v as select x from namespace_source.src;
-- @session
create snapshot view_metadata_namespace_history for account view_metadata_namespace_reader;
create snapshot view_metadata_namespace_private for account view_metadata_namespace_reader;
-- @session:id=2&user=view_metadata_namespace_reader:admin&password=111
alter table namespace_source.src modify x varchar(37);
update namespace_source.src set x='new';
create snapshot view_metadata_namespace_history for account;
select x from namespace_source.v {snapshot='view_metadata_namespace_history'};
select table_name, character_maximum_length from information_schema.columns {snapshot='view_metadata_namespace_history'} where table_schema='namespace_source' and column_name='x' order by table_name;
select count(*) from information_schema.columns {snapshot='view_metadata_namespace_private'};
-- @session
select x from namespace_source.src {snapshot='view_metadata_namespace_history'};
select x from namespace_source.v {snapshot='view_metadata_namespace_history'};
select table_name, character_maximum_length, current_account_id() from information_schema.columns {snapshot='view_metadata_namespace_history'} where table_schema='namespace_source' and column_name='x' order by table_name;
prepare view_metadata_namespace_stmt from 'select count(*), max(character_maximum_length) from information_schema.columns {snapshot=''view_metadata_namespace_history''} where table_schema=''namespace_source'' and column_name=''x''';
execute view_metadata_namespace_stmt;
execute view_metadata_namespace_stmt;
deallocate prepare view_metadata_namespace_stmt;
-- A non-admin system role must not gain tenant visibility via matching role IDs.
create user view_metadata_namespace_limited identified by '111';
-- @session:id=3&user=sys:view_metadata_namespace_limited&password=111
select count(*) from information_schema.columns {snapshot='view_metadata_namespace_history'} where table_schema='namespace_source';
-- @session
drop user view_metadata_namespace_limited;
-- @session:id=4&user=view_metadata_namespace_reader:admin&password=111
drop snapshot view_metadata_namespace_history;
drop database namespace_source;
-- @session
drop snapshot view_metadata_namespace_history;
drop snapshot view_metadata_namespace_private;
drop account view_metadata_namespace_reader;
