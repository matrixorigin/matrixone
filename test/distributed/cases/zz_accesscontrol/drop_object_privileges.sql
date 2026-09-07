drop database if exists drop_object_privileges_db;
drop role if exists drop_object_privileges_role;

create role drop_object_privileges_role;
grant create database on account * to drop_object_privileges_role;
create database drop_object_privileges_db;
create table drop_object_privileges_db.t1(id int);
create view drop_object_privileges_db.v1 as select 1 as id;
create sequence drop_object_privileges_db.s1;

grant select on table drop_object_privileges_db.t1 to drop_object_privileges_role;
grant select on table drop_object_privileges_db.* to drop_object_privileges_role;
grant select on view drop_object_privileges_db.v1 to drop_object_privileges_role;
grant select on table drop_object_privileges_db.s1 to drop_object_privileges_role;
grant create table on database drop_object_privileges_db to drop_object_privileges_role;

set @drop_priv_db_id = (select dat_id from mo_catalog.mo_database
                         where datname = 'drop_object_privileges_db');
set @drop_priv_t1_id = (select rel_logical_id from mo_catalog.mo_tables
                         where reldatabase = 'drop_object_privileges_db' and relname = 't1');
set @drop_priv_v1_id = (select rel_logical_id from mo_catalog.mo_tables
                         where reldatabase = 'drop_object_privileges_db' and relname = 'v1');
set @drop_priv_s1_id = (select rel_logical_id from mo_catalog.mo_tables
                         where reldatabase = 'drop_object_privileges_db' and relname = 's1');

select count(*) as object_grants_before_drop
from mo_catalog.mo_role_privs
where role_name = 'drop_object_privileges_role'
  and obj_id in (@drop_priv_db_id, @drop_priv_t1_id, @drop_priv_v1_id, @drop_priv_s1_id);

-- ALTER SEQUENCE replaces storage while preserving its logical identity and grant.
use drop_object_privileges_db;
alter sequence s1 increment by 2;
set @drop_priv_altered_s1_id = (select rel_logical_id from mo_catalog.mo_tables
                                 where reldatabase = 'drop_object_privileges_db' and relname = 's1');
select @drop_priv_altered_s1_id = @drop_priv_s1_id as altered_sequence_logical_id_preserved;
select count(*) as altered_sequence_exact_grants
from mo_catalog.mo_role_privs
where role_name = 'drop_object_privileges_role'
  and obj_id = @drop_priv_altered_s1_id;

-- Sequences accept table-scoped grants and must clean their logical identity.
drop sequence drop_object_privileges_db.s1;
select count(*) as dropped_sequence_grants
from mo_catalog.mo_role_privs
where role_name = 'drop_object_privileges_role'
  and obj_id = @drop_priv_s1_id;

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

-- ALTER VIEW preserves the logical object identity and its exact-view grant.
alter view v1 as select 3 as id;
set @drop_priv_altered_v1_id = (select rel_logical_id from mo_catalog.mo_tables
                                 where reldatabase = 'drop_object_privileges_db' and relname = 'v1');
select @drop_priv_altered_v1_id = @drop_priv_new_v1_id as altered_view_logical_id_preserved;
select count(*) as altered_view_exact_grants
from mo_catalog.mo_role_privs
where role_name = 'drop_object_privileges_role'
  and obj_id = @drop_priv_altered_v1_id;

select count(*) as object_grants_before_database_drop
from mo_catalog.mo_role_privs
where role_name = 'drop_object_privileges_role'
  and obj_id in (@drop_priv_db_id, @drop_priv_new_t1_id, @drop_priv_new_v1_id);

-- Dropping the database removes database-, table-, and view-scoped grants together.
drop database drop_object_privileges_db;
select count(*) as dropped_database_object_grants
from mo_catalog.mo_role_privs
where role_name = 'drop_object_privileges_role'
  and obj_id in (@drop_priv_db_id, @drop_priv_new_t1_id, @drop_priv_new_v1_id);
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

-- Replacement DDL must qualify its internal DROP with the statement target,
-- not the session's current database, and must quote special identifiers.
drop database if exists replace_current_db;
drop database if exists replace_target_db;
create database replace_current_db;
create database replace_target_db;
create view replace_current_db.`select` as select 1 as id;
create view replace_target_db.`select` as select 2 as id;
create sequence replace_current_db.`order`;
create sequence replace_target_db.`order`;
use replace_current_db;
create or replace view replace_target_db.`select` as select 3 as id;
alter sequence replace_target_db.`order` increment by 2;
select id from replace_current_db.`select`;
select id from replace_target_db.`select`;
select count(*) as current_sequence_preserved from mo_catalog.mo_tables
where reldatabase = 'replace_current_db' and relname = 'order';
select count(*) as target_sequence_preserved from mo_catalog.mo_tables
where reldatabase = 'replace_target_db' and relname = 'order';
drop database replace_current_db;
drop database replace_target_db;

-- Prepared execution keeps its PREPARE-time database for authorization and
-- implicit ownership cleanup, even after USE changes the session database.
drop user if exists prepared_binding_user;
drop role if exists prepared_binding_role;
drop database if exists prepared_binding_d1;
drop database if exists prepared_binding_d2;
create role prepared_binding_role;
create user prepared_binding_user identified by '111' default role prepared_binding_role;
grant connect on account * to prepared_binding_role;
create database prepared_binding_d1;
create database prepared_binding_d2;
create table prepared_binding_d1.auth_t(id int);
create table prepared_binding_d2.auth_t(id int);
create table prepared_binding_d1.owner_t(id int);
create table prepared_binding_d2.owner_t(id int);
grant drop table on database prepared_binding_d1 to prepared_binding_role;

-- Only d1 is authorized here. EXECUTE must not check the unqualified AST
-- against the execute-time d2 database.
-- @session:id=1&user=sys:prepared_binding_user:prepared_binding_role&password=111
use prepared_binding_d1;
prepare prepared_auth_drop from 'drop table auth_t';
use prepared_binding_d2;
execute prepared_auth_drop;
deallocate prepare prepared_auth_drop;
-- @session

-- Permit d2 as well so this second execution isolates ownership cleanup: only
-- the PREPARE-time d1 ownership row may be revoked.
grant drop table on database prepared_binding_d2 to prepared_binding_role;
grant ownership on table prepared_binding_d1.owner_t to prepared_binding_role;
grant ownership on table prepared_binding_d2.owner_t to prepared_binding_role;
set @prepared_binding_d2_owner_id = (select rel_logical_id from mo_catalog.mo_tables
                                      where reldatabase = 'prepared_binding_d2' and relname = 'owner_t');
-- @session:id=2&user=sys:prepared_binding_user:prepared_binding_role&password=111
use prepared_binding_d1;
prepare prepared_owner_drop from 'drop table owner_t';
use prepared_binding_d2;
execute prepared_owner_drop;
deallocate prepare prepared_owner_drop;
-- @session

select count(*) as prepared_d1_tables_dropped
from mo_catalog.mo_tables
where reldatabase = 'prepared_binding_d1' and relname in ('auth_t', 'owner_t');
select count(*) as prepared_d2_tables_preserved
from mo_catalog.mo_tables
where reldatabase = 'prepared_binding_d2' and relname in ('auth_t', 'owner_t');
select count(*) as prepared_d2_ownership_preserved
from mo_catalog.mo_role_privs
where role_name = 'prepared_binding_role'
  and obj_id = @prepared_binding_d2_owner_id
  and privilege_name = 'table ownership';

drop user prepared_binding_user;
drop database prepared_binding_d1;
drop database prepared_binding_d2;
drop role prepared_binding_role;

-- Hidden index relations are implementation details and cannot receive grants.
drop database if exists internal_grant_db;
drop role if exists internal_grant_role;
create database internal_grant_db;
create table internal_grant_db.t(a int, index idx_a(a));
create role internal_grant_role;
set @internal_index_name = (select distinct i.index_table_name
                            from mo_catalog.mo_indexes i
                            join mo_catalog.mo_tables t on i.table_id = t.rel_id
                            join mo_catalog.mo_database d on i.database_id = d.dat_id
                            where d.datname = 'internal_grant_db'
                              and t.relname = 't' and i.name = 'idx_a');
set @internal_grant_sql = concat('grant select on table internal_grant_db.`',
                                  @internal_index_name, '` to internal_grant_role');
-- @regex("internal error",true)
prepare internal_grant_stmt from @internal_grant_sql;
drop database internal_grant_db;
drop role internal_grant_role;
