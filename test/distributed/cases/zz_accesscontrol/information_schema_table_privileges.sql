drop role if exists table_privileges_metadata_reader;
drop database if exists table_privileges_metadata_db;

create database table_privileges_metadata_db;
create table table_privileges_metadata_db.t(id int);
create role table_privileges_metadata_reader;
grant select on table table_privileges_metadata_db.t to table_privileges_metadata_reader;
grant insert on table table_privileges_metadata_db.t to table_privileges_metadata_reader with grant option;

select grantee, table_catalog, table_schema, table_name, privilege_type, is_grantable
from information_schema.table_privileges
where grantee = 'table_privileges_metadata_reader'
  and table_schema = 'table_privileges_metadata_db'
  and table_name = 't'
order by privilege_type;

revoke select on table table_privileges_metadata_db.t from table_privileges_metadata_reader;
select grantee, table_catalog, table_schema, table_name, privilege_type, is_grantable
from information_schema.table_privileges
where grantee = 'table_privileges_metadata_reader'
  and table_schema = 'table_privileges_metadata_db'
  and table_name = 't'
order by privilege_type;

drop role table_privileges_metadata_reader;
drop database table_privileges_metadata_db;
