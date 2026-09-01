-- @label:bvt
set session enable_privilege_cache = off;

drop database if exists table_privileges_db;
drop user if exists table_privileges_user;
drop role if exists table_privileges_reader, table_privileges_reader_renamed,
    table_privileges_wildcard, table_privileges_hidden;

select count(*) = 1 as canonical_view_uses_catalog_name
from mo_catalog.mo_tables
where account_id = current_account_id()
  and reldatabase = 'information_schema'
  and relname = 'table_privileges'
  and relkind = 'v';

select upper(column_name) as column_name, is_nullable
from information_schema.columns
where table_schema = 'information_schema'
  and table_name = 'table_privileges'
  and lower(column_name) in ('grantee', 'table_catalog', 'table_schema', 'table_name',
                             'privilege_type', 'is_grantable')
order by ordinal_position;

create database table_privileges_db;
create table table_privileges_db.t(id int);
create table table_privileges_db.hidden_t(id int);
create view table_privileges_db.v as select id from table_privileges_db.t;
create role table_privileges_reader, table_privileges_wildcard, table_privileges_hidden;
create user table_privileges_user identified by '123456' default role public;
grant connect on account * to table_privileges_reader;
grant select on table table_privileges_db.t to table_privileges_reader;
grant update on table table_privileges_db.t to table_privileges_reader with grant option;
grant select on view table_privileges_db.v to table_privileges_reader with grant option;
grant insert on table table_privileges_db.* to table_privileges_wildcard;
grant select on table table_privileges_db.hidden_t to table_privileges_hidden;
grant table_privileges_reader to table_privileges_user;

select grantee, table_catalog, table_schema, table_name, privilege_type, is_grantable
from information_schema.table_privileges
where table_schema = 'table_privileges_db'
order by table_name, grantee, privilege_type;

select count(*) = 0 as wildcard_grant_not_reported_as_direct
from information_schema.table_privileges
where table_schema = 'table_privileges_db'
  and grantee = 'table_privileges_wildcard';

-- @session:id=2&user=sys:table_privileges_user:table_privileges_reader&password=123456
set session enable_privilege_cache = off;
select grantee, table_catalog, table_schema, table_name, privilege_type, is_grantable
from information_schema.table_privileges
where table_schema = 'table_privileges_db'
order by table_name, privilege_type;
select count(*) = 0 as hidden_table_privilege_hidden
from information_schema.table_privileges
where table_schema = 'table_privileges_db'
  and table_name = 'hidden_t';
-- @session

alter role table_privileges_reader rename to table_privileges_reader_renamed;
select
    (select count(*) = 3 from information_schema.table_privileges
     where table_schema = 'table_privileges_db'
       and grantee = 'table_privileges_reader_renamed') as renamed_grantee_visible,
    (select count(*) = 0 from information_schema.table_privileges
     where table_schema = 'table_privileges_db'
       and grantee = 'table_privileges_reader') as stale_grantee_removed;
alter role table_privileges_reader_renamed rename to table_privileges_reader;

revoke update on table table_privileges_db.t from table_privileges_reader;
select count(*) = 0 as revoked_privilege_removed
from information_schema.table_privileges
where table_schema = 'table_privileges_db'
  and table_name = 't'
  and grantee = 'table_privileges_reader'
  and privilege_type = 'UPDATE';

drop view table_privileges_db.v;
select count(*) = 0 as dropped_view_privilege_removed
from information_schema.table_privileges
where table_schema = 'table_privileges_db'
  and table_name = 'v';

drop database table_privileges_db;
drop user table_privileges_user;
drop role if exists table_privileges_reader, table_privileges_reader_renamed,
    table_privileges_wildcard, table_privileges_hidden;
