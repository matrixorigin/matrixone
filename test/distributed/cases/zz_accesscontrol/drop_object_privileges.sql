drop database if exists drop_object_privileges_db;
drop role if exists drop_object_privileges_role;

create role drop_object_privileges_role;
grant create database on account * to drop_object_privileges_role;
create database drop_object_privileges_db;
create table drop_object_privileges_db.t1(id int);
create view drop_object_privileges_db.v1 as select 1 as id;

grant select on table drop_object_privileges_db.t1 to drop_object_privileges_role;
grant select on table drop_object_privileges_db.* to drop_object_privileges_role;
grant select on view drop_object_privileges_db.v1 to drop_object_privileges_role;
grant create table on database drop_object_privileges_db to drop_object_privileges_role;

set @drop_priv_db_id = (select dat_id from mo_catalog.mo_database
                         where datname = 'drop_object_privileges_db');
set @drop_priv_t1_id = (select rel_logical_id from mo_catalog.mo_tables
                         where reldatabase = 'drop_object_privileges_db' and relname = 't1');
set @drop_priv_v1_id = (select rel_logical_id from mo_catalog.mo_tables
                         where reldatabase = 'drop_object_privileges_db' and relname = 'v1');

select count(*) as object_grants_before_drop
from mo_catalog.mo_role_privs
where role_name = 'drop_object_privileges_role'
  and obj_id in (@drop_priv_db_id, @drop_priv_t1_id, @drop_priv_v1_id);

-- Dropping one table removes only grants tied to that table object.
drop table drop_object_privileges_db.t1;
select count(*) as dropped_table_grants
from mo_catalog.mo_role_privs
where role_name = 'drop_object_privileges_role'
  and obj_id = @drop_priv_t1_id;
select count(*) as database_scoped_grants
from mo_catalog.mo_role_privs
where role_name = 'drop_object_privileges_role'
  and obj_id = @drop_priv_db_id;
select count(*) as account_scoped_grants
from mo_catalog.mo_role_privs
where role_name = 'drop_object_privileges_role'
  and obj_id = 0;

-- A same-name object gets a new logical identity and no stale object-specific grant.
create table drop_object_privileges_db.t1(id int);
set @drop_priv_new_t1_id = (select rel_logical_id from mo_catalog.mo_tables
                             where reldatabase = 'drop_object_privileges_db' and relname = 't1');
select @drop_priv_new_t1_id <> @drop_priv_t1_id as logical_id_changed;
select count(*) as recreated_table_specific_grants
from mo_catalog.mo_role_privs
where role_name = 'drop_object_privileges_role'
  and obj_id = @drop_priv_new_t1_id;

grant select on table drop_object_privileges_db.t1 to drop_object_privileges_role;

-- Prepared DROP resolves the effective statement before transaction admission.
create table drop_object_privileges_db.prepared_t(id int);
grant select on table drop_object_privileges_db.prepared_t to drop_object_privileges_role;
set @drop_priv_prepared_id = (select rel_logical_id from mo_catalog.mo_tables
                               where reldatabase = 'drop_object_privileges_db' and relname = 'prepared_t');
prepare drop_priv_prepared from 'drop table drop_object_privileges_db.prepared_t';
execute drop_priv_prepared;
deallocate prepare drop_priv_prepared;
select count(*) as prepared_drop_grants
from mo_catalog.mo_role_privs
where role_name = 'drop_object_privileges_role'
  and obj_id = @drop_priv_prepared_id;

-- CREATE OR REPLACE VIEW cleans the old logical identity before publishing the replacement.
use drop_object_privileges_db;
create or replace view v1 as select 2 as id;
set @drop_priv_new_v1_id = (select rel_logical_id from mo_catalog.mo_tables
                             where reldatabase = 'drop_object_privileges_db' and relname = 'v1');
select @drop_priv_new_v1_id <> @drop_priv_v1_id as replaced_view_logical_id_changed;
select count(*) as replaced_view_old_grants
from mo_catalog.mo_role_privs
where role_name = 'drop_object_privileges_role'
  and obj_id = @drop_priv_v1_id;
grant select on view drop_object_privileges_db.v1 to drop_object_privileges_role;

select count(*) as object_grants_before_database_drop
from mo_catalog.mo_role_privs
where role_name = 'drop_object_privileges_role'
  and obj_id in (@drop_priv_db_id, @drop_priv_new_t1_id, @drop_priv_new_v1_id);

-- Dropping the database removes database-, table-, and view-scoped grants together.
drop database drop_object_privileges_db;
select count(*) as dropped_database_object_grants
from mo_catalog.mo_role_privs
where role_name = 'drop_object_privileges_role'
  and obj_id in (@drop_priv_db_id, @drop_priv_new_t1_id, @drop_priv_v1_id);
select count(*) as preserved_account_scoped_grants
from mo_catalog.mo_role_privs
where role_name = 'drop_object_privileges_role'
  and obj_id = 0;
select count(*) as dropped_catalog_objects
from (
    select dat_id as object_id from mo_catalog.mo_database where dat_id = @drop_priv_db_id
    union all
    select rel_logical_id as object_id from mo_catalog.mo_tables
    where rel_logical_id in (@drop_priv_new_t1_id, @drop_priv_new_v1_id)
) as dropped_objects;

drop role drop_object_privileges_role;
