drop role if exists table_privileges_aggregate_role;
drop role if exists table_privileges_reference_role;
drop role if exists table_privileges_view_role;
drop database if exists table_privileges_metadata_db;

create database table_privileges_metadata_db;
create table table_privileges_metadata_db.t(id int);
create table table_privileges_metadata_db.reference_t(id int);
create view table_privileges_metadata_db.v as select * from table_privileges_metadata_db.t;
create role table_privileges_aggregate_role;
create role table_privileges_reference_role;
create role table_privileges_view_role;

grant all on table table_privileges_metadata_db.t to table_privileges_aggregate_role;
grant select on table table_privileges_metadata_db.t to table_privileges_aggregate_role with grant option;
select privilege_type, is_grantable
from information_schema.table_privileges
where grantee = 'table_privileges_aggregate_role'
  and table_schema = 'table_privileges_metadata_db'
  and table_name = 't'
order by privilege_type;

grant ownership on table table_privileges_metadata_db.t to table_privileges_aggregate_role;
select privilege_type, is_grantable
from information_schema.table_privileges
where grantee = 'table_privileges_aggregate_role'
  and table_schema = 'table_privileges_metadata_db'
  and table_name = 't'
order by privilege_type;

grant reference on table table_privileges_metadata_db.reference_t to table_privileges_reference_role;
select grantee, table_catalog, table_schema, table_name, privilege_type, is_grantable
from information_schema.table_privileges
where grantee = 'table_privileges_reference_role'
  and table_schema = 'table_privileges_metadata_db'
  and table_name = 'reference_t';

grant select on view table_privileges_metadata_db.v to table_privileges_view_role;
select grantee, table_catalog, table_schema, table_name, privilege_type, is_grantable
from information_schema.table_privileges
where grantee = 'table_privileges_view_role'
  and table_schema = 'table_privileges_metadata_db'
  and table_name = 'v';
revoke select on view table_privileges_metadata_db.v from table_privileges_view_role;
select grantee, table_catalog, table_schema, table_name, privilege_type, is_grantable
from information_schema.table_privileges
where grantee = 'table_privileges_view_role'
  and table_schema = 'table_privileges_metadata_db'
  and table_name = 'v';

drop role table_privileges_aggregate_role;
drop role table_privileges_reference_role;
drop role table_privileges_view_role;
drop database table_privileges_metadata_db;
