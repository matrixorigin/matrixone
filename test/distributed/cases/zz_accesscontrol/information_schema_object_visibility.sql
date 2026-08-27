-- @label:bvt
set global enable_privilege_cache = off;

drop database if exists metadata_visibility_db;
drop user if exists metadata_visibility_user;
drop role if exists metadata_visibility_primary, metadata_visibility_middle, metadata_visibility_reader;

create database metadata_visibility_db;
create table metadata_visibility_db.allowed_table (
    id int primary key,
    secret varchar(20) unique,
    payload int,
    constraint ck_allowed_payload check (payload >= 0)
) partition by hash(id) partitions 2;
create table metadata_visibility_db.hidden_parent (
    id int primary key
);
create table metadata_visibility_db.hidden_table (
    id int primary key,
    secret varchar(20) unique,
    payload int,
    constraint ck_hidden_payload check (payload >= 0),
    constraint fk_hidden_parent foreign key (id) references metadata_visibility_db.hidden_parent(id)
);
create view metadata_visibility_db.hidden_view as
select id, secret, payload from metadata_visibility_db.hidden_table;
create role metadata_visibility_primary, metadata_visibility_middle, metadata_visibility_reader;
create user metadata_visibility_user identified by '123456' default role metadata_visibility_primary;
grant connect on account * to metadata_visibility_primary;

-- @session:id=2&user=sys:metadata_visibility_user:metadata_visibility_primary&password=123456
select count(*) = 0 as tables_hidden
from information_schema.tables
where table_schema = 'metadata_visibility_db';
select count(*) = 0 as columns_hidden
from information_schema.columns
where table_schema = 'metadata_visibility_db';
select count(*) = 0 as statistics_hidden
from information_schema.statistics
where table_schema = 'metadata_visibility_db';
select count(*) = 0 as constraints_hidden
from information_schema.table_constraints
where table_schema = 'metadata_visibility_db';
select count(*) = 0 as schema_hidden
from information_schema.schemata
where schema_name = 'metadata_visibility_db';
select
    (select count(*) = 0 from information_schema.check_constraints
     where constraint_schema = 'metadata_visibility_db') as check_constraints_hidden,
    (select count(*) = 0 from information_schema.key_column_usage
     where table_schema = 'metadata_visibility_db') as key_column_usage_hidden,
    (select count(*) = 0 from information_schema.referential_constraints
     where constraint_schema = 'metadata_visibility_db') as referential_constraints_hidden,
    (select count(*) = 0 from information_schema.views
     where table_schema = 'metadata_visibility_db') as views_hidden,
    (select count(*) = 0 from information_schema.partitions
     where table_schema = 'metadata_visibility_db') as partitions_hidden;
select
    (select count(*) > 0 from information_schema.tables
     where table_schema = 'information_schema')
    and
    (select count(*) > 0 from information_schema.columns
     where table_schema = 'information_schema')
    and
    (select count(*) = 1 from information_schema.schemata
     where schema_name = 'information_schema') as system_metadata_visible;
-- @session

grant select on table metadata_visibility_db.allowed_table to metadata_visibility_reader;
grant metadata_visibility_reader to metadata_visibility_middle;
grant metadata_visibility_middle to metadata_visibility_primary;
grant select on table metadata_visibility_db.hidden_parent to metadata_visibility_primary;

-- @session:id=2&user=sys:metadata_visibility_user:metadata_visibility_primary&password=123456
select
    (select count(*) = 1 from information_schema.tables
     where table_schema = 'metadata_visibility_db' and table_name = 'allowed_table') as inherited_table_visible,
    (select count(*) = 1 from information_schema.tables
     where table_schema = 'metadata_visibility_db' and table_name = 'hidden_parent') as direct_table_visible,
    (select count(*) = 0 from information_schema.tables
     where table_schema = 'metadata_visibility_db' and table_name = 'hidden_table') as hidden_stays_hidden;
select
    (select count(*) = 3 from information_schema.columns
     where table_schema = 'metadata_visibility_db' and table_name = 'allowed_table') as allowed_columns_visible,
    (select count(*) = 0 from information_schema.columns
     where table_schema = 'metadata_visibility_db' and table_name = 'hidden_table') as hidden_columns_hidden;
select
    (select count(*) > 0 from information_schema.statistics
     where table_schema = 'metadata_visibility_db' and table_name = 'allowed_table') as allowed_statistics_visible,
    (select count(*) = 0 from information_schema.statistics
     where table_schema = 'metadata_visibility_db' and table_name = 'hidden_table') as hidden_statistics_hidden;
select
    (select count(*) > 0 from information_schema.table_constraints
     where table_schema = 'metadata_visibility_db' and table_name = 'allowed_table') as allowed_constraints_visible,
    (select count(*) = 1 from information_schema.table_constraints
     where table_schema = 'metadata_visibility_db' and table_name = 'allowed_table'
       and constraint_name = 'ck_allowed_payload') as allowed_check_visible,
    (select count(*) = 0 from information_schema.table_constraints
     where table_schema = 'metadata_visibility_db' and table_name = 'hidden_table') as hidden_constraints_hidden;
select
    (select count(*) = 1 from information_schema.check_constraints
     where constraint_schema = 'metadata_visibility_db' and constraint_name = 'ck_allowed_payload')
        as allowed_check_metadata_visible,
    (select count(*) > 0 from information_schema.partitions
     where table_schema = 'metadata_visibility_db' and table_name = 'allowed_table')
        as allowed_partition_metadata_visible,
    (select count(*) = 0 from information_schema.key_column_usage
     where table_schema = 'metadata_visibility_db' and table_name = 'hidden_table')
        as hidden_fk_columns_hidden,
    (select count(*) = 0 from information_schema.referential_constraints
     where constraint_schema = 'metadata_visibility_db' and table_name = 'hidden_table')
        as hidden_fk_constraint_hidden,
    (select count(*) = 0 from information_schema.views
     where table_schema = 'metadata_visibility_db' and table_name = 'hidden_view')
        as hidden_view_hidden;
select count(*) = 1 as inherited_schema_visible
from information_schema.schemata
where schema_name = 'metadata_visibility_db';
-- @session

alter role metadata_visibility_primary rename to metadata_visibility_primary_renamed;

-- @session:id=2&user=sys:metadata_visibility_user:metadata_visibility_primary&password=123456
select count(*) = 1 as table_visible_after_active_role_rename
from information_schema.tables
where table_schema = 'metadata_visibility_db' and table_name = 'allowed_table';
select count(*) = 3 as columns_visible_after_active_role_rename
from information_schema.columns
where table_schema = 'metadata_visibility_db' and table_name = 'allowed_table';
select count(*) > 0 as statistics_visible_after_active_role_rename
from information_schema.statistics
where table_schema = 'metadata_visibility_db' and table_name = 'allowed_table';
select count(*) > 0 as constraints_visible_after_active_role_rename
from information_schema.table_constraints
where table_schema = 'metadata_visibility_db' and table_name = 'allowed_table';
-- @session

alter role metadata_visibility_primary_renamed rename to metadata_visibility_primary;

-- @session:id=2&user=sys:metadata_visibility_user:metadata_visibility_primary&password=123456
prepare metadata_visibility_prepared from "select
    (select count(*) from information_schema.tables
     where table_schema = 'metadata_visibility_db' and table_name = 'allowed_table') as table_count,
    (select count(*) from information_schema.columns
     where table_schema = 'metadata_visibility_db' and table_name = 'allowed_table') as column_count,
    (select count(*) from information_schema.statistics
     where table_schema = 'metadata_visibility_db' and table_name = 'allowed_table') as statistic_count,
    (select count(*) from information_schema.table_constraints
     where table_schema = 'metadata_visibility_db' and table_name = 'allowed_table') as constraint_count";
execute metadata_visibility_prepared;
select
    (select count(*) from information_schema.tables
     where table_schema = 'metadata_visibility_db' and table_name = 'allowed_table') as table_count,
    (select count(*) from information_schema.columns
     where table_schema = 'metadata_visibility_db' and table_name = 'allowed_table') as column_count,
    (select count(*) from information_schema.statistics
     where table_schema = 'metadata_visibility_db' and table_name = 'allowed_table') as statistic_count,
    (select count(*) from information_schema.table_constraints
     where table_schema = 'metadata_visibility_db' and table_name = 'allowed_table') as constraint_count;
set role public;
-- @session

-- @session:id=2&user=sys:metadata_visibility_user:metadata_visibility_primary&password=123456
execute metadata_visibility_prepared;
select
    (select count(*) from information_schema.tables
     where table_schema = 'metadata_visibility_db' and table_name = 'allowed_table') as table_count,
    (select count(*) from information_schema.columns
     where table_schema = 'metadata_visibility_db' and table_name = 'allowed_table') as column_count,
    (select count(*) from information_schema.statistics
     where table_schema = 'metadata_visibility_db' and table_name = 'allowed_table') as statistic_count,
    (select count(*) from information_schema.table_constraints
     where table_schema = 'metadata_visibility_db' and table_name = 'allowed_table') as constraint_count;
select count(*) = 0 as schema_hidden_after_set_role
from information_schema.schemata
where schema_name = 'metadata_visibility_db';
deallocate prepare metadata_visibility_prepared;
set role metadata_visibility_primary;
-- @session

grant create database on account * to metadata_visibility_primary;

-- @session:id=2&user=sys:metadata_visibility_user:metadata_visibility_primary&password=123456
create database metadata_visibility_owned_db;
select count(*) = 1 as owned_schema_visible
from information_schema.schemata
where schema_name = 'metadata_visibility_owned_db';
-- @session

drop database metadata_visibility_owned_db;
grant show tables on database metadata_visibility_db to metadata_visibility_primary;

-- @session:id=2&user=sys:metadata_visibility_user:metadata_visibility_primary&password=123456
select count(*) = 1 as database_schema_visible
from information_schema.schemata
where schema_name = 'metadata_visibility_db';
select count(*) = 2 as database_tables_visible
from information_schema.tables
where table_schema = 'metadata_visibility_db'
  and table_name in ('allowed_table', 'hidden_table');
select count(*) = 6 as database_columns_visible
from information_schema.columns
where table_schema = 'metadata_visibility_db'
  and table_name in ('allowed_table', 'hidden_table');
select count(*) > 0 as database_statistics_visible
from information_schema.statistics
where table_schema = 'metadata_visibility_db';
select count(*) > 0 as database_constraints_visible
from information_schema.table_constraints
where table_schema = 'metadata_visibility_db';
select
    (select count(*) > 0 from information_schema.check_constraints
     where constraint_schema = 'metadata_visibility_db') as database_checks_visible,
    (select count(*) = 1 from information_schema.key_column_usage
     where table_schema = 'metadata_visibility_db'
       and table_name = 'hidden_table' and constraint_name = 'fk_hidden_parent') as database_fk_columns_visible,
    (select count(*) = 1 from information_schema.referential_constraints
     where constraint_schema = 'metadata_visibility_db'
       and table_name = 'hidden_table' and constraint_name = 'fk_hidden_parent') as database_fk_constraints_visible,
    (select count(*) = 1 from information_schema.views
     where table_schema = 'metadata_visibility_db' and table_name = 'hidden_view') as database_view_visible,
    (select count(*) > 0 from information_schema.partitions
     where table_schema = 'metadata_visibility_db' and table_name = 'allowed_table') as database_partitions_visible;
-- @session

select count(*) = 1 as admin_schema_visible
from information_schema.schemata
where schema_name = 'metadata_visibility_db';
select count(*) = 2 as admin_tables_visible
from information_schema.tables
where table_schema = 'metadata_visibility_db'
  and table_name in ('allowed_table', 'hidden_table');
select count(*) = 6 as admin_columns_visible
from information_schema.columns
where table_schema = 'metadata_visibility_db'
  and table_name in ('allowed_table', 'hidden_table');
select count(*) > 0 as admin_statistics_visible
from information_schema.statistics
where table_schema = 'metadata_visibility_db';
select count(*) > 0 as admin_constraints_visible
from information_schema.table_constraints
where table_schema = 'metadata_visibility_db';
select
    (select count(*) > 0 from information_schema.check_constraints
     where constraint_schema = 'metadata_visibility_db') as admin_checks_visible,
    (select count(*) = 1 from information_schema.key_column_usage
     where table_schema = 'metadata_visibility_db'
       and table_name = 'hidden_table' and constraint_name = 'fk_hidden_parent') as admin_fk_columns_visible,
    (select count(*) = 1 from information_schema.referential_constraints
     where constraint_schema = 'metadata_visibility_db'
       and table_name = 'hidden_table' and constraint_name = 'fk_hidden_parent') as admin_fk_constraints_visible,
    (select count(*) = 1 from information_schema.views
     where table_schema = 'metadata_visibility_db' and table_name = 'hidden_view') as admin_view_visible,
    (select count(*) > 0 from information_schema.partitions
     where table_schema = 'metadata_visibility_db' and table_name = 'allowed_table') as admin_partitions_visible;

drop database metadata_visibility_db;
drop user metadata_visibility_user;
drop role metadata_visibility_primary, metadata_visibility_middle, metadata_visibility_reader;
set global enable_privilege_cache = on;
