drop database if exists prepare_ddl_schema_change;
create database prepare_ddl_schema_change;
use prepare_ddl_schema_change;

create table prepared_t (
    id int primary key,
    v int
);

prepare add_column_stmt from
    'alter table prepared_t add column note varchar(20) default ''x''';
execute add_column_stmt;
execute add_column_stmt;
select column_name
from information_schema.columns
where table_schema = 'prepare_ddl_schema_change' and table_name = 'prepared_t'
order by ordinal_position;
alter table prepared_t drop column note;
execute add_column_stmt;
select column_name
from information_schema.columns
where table_schema = 'prepare_ddl_schema_change' and table_name = 'prepared_t'
order by ordinal_position;
deallocate prepare add_column_stmt;

create index idx_v on prepared_t(v);
prepare drop_column_stmt from 'alter table prepared_t drop column note';
execute drop_column_stmt;
execute drop_column_stmt;
select column_name
from information_schema.columns
where table_schema = 'prepare_ddl_schema_change' and table_name = 'prepared_t'
order by ordinal_position;
deallocate prepare drop_column_stmt;

prepare create_index_stmt from 'create index idx_created on prepared_t(v)';
execute create_index_stmt;
execute create_index_stmt;
show index from prepared_t;
deallocate prepare create_index_stmt;

prepare drop_index_stmt from 'drop index idx_created on prepared_t';
execute drop_index_stmt;
execute drop_index_stmt;
show index from prepared_t;
deallocate prepare drop_index_stmt;

create table truncate_t (id int primary key, v int);
insert into truncate_t values (1, 10), (2, 20);
prepare truncate_stmt from 'truncate table truncate_t';
create index idx_truncate_v on truncate_t(v);
execute truncate_stmt;
select count(*) from truncate_t;
insert into truncate_t values (3, 30);
select id from truncate_t where v = 30;
deallocate prepare truncate_stmt;

create table drop_parent (id int primary key);
prepare drop_table_stmt from 'drop table drop_parent';
create table drop_child (
    id int primary key,
    parent_id int,
    constraint fk_drop_parent foreign key (parent_id) references drop_parent(id)
);
execute drop_table_stmt;
select column_name
from information_schema.columns
where table_schema = 'prepare_ddl_schema_change' and table_name = 'drop_parent'
order by ordinal_position;
drop table drop_child;
execute drop_table_stmt;
deallocate prepare drop_table_stmt;

create table rename_t1 (a int);
create table rename_t2 (b int);
prepare rename_stmt from 'rename table rename_t1 to renamed_t1, rename_t2 to renamed_t2';
alter table rename_t2 add column c int;
execute rename_stmt;
select column_name
from information_schema.columns
where table_schema = 'prepare_ddl_schema_change' and table_name = 'renamed_t2'
order by ordinal_position;
deallocate prepare rename_stmt;

create table like_src (a int);
prepare like_stmt from 'create table like_dst like like_src';
alter table like_src add column b int;
execute like_stmt;
select column_name
from information_schema.columns
where table_schema = 'prepare_ddl_schema_change' and table_name = 'like_dst'
order by ordinal_position;
deallocate prepare like_stmt;

create table clone_src (a int);
insert into clone_src values (1);
prepare clone_stmt from 'create table clone_dst clone clone_src';
alter table clone_src add column b int default 2;
insert into clone_src values (3, 4);
execute clone_stmt;
select * from clone_dst order by a;
deallocate prepare clone_stmt;

create table fk_parent (id int primary key);
create table fk_child (pid int);
prepare alter_fk_stmt from
    'alter table fk_child add constraint fk_parent_ref foreign key (pid) references fk_parent(id)';
alter table fk_parent drop primary key;
execute alter_fk_stmt;
select count(*)
from information_schema.referential_constraints
where constraint_schema = 'prepare_ddl_schema_change' and constraint_name = 'fk_parent_ref';
deallocate prepare alter_fk_stmt;

set foreign_key_checks = 0;
prepare forward_fk_stmt from
    'create table forward_child (pid int, foreign key (pid) references forward_parent(id))';
create table forward_parent (id int primary key);
execute forward_fk_stmt;
select count(*)
from information_schema.referential_constraints
where constraint_schema = 'prepare_ddl_schema_change' and table_name = 'forward_child';
deallocate prepare forward_fk_stmt;

create table stale_forward_child (
    pid int,
    foreign key (pid) references stale_forward_parent(id)
);
prepare forward_parent_stmt from 'create table stale_forward_parent (id int primary key)';
drop table stale_forward_child;
create table stale_forward_child (new_pid bigint);
execute forward_parent_stmt;
select count(*)
from information_schema.referential_constraints
where constraint_schema = 'prepare_ddl_schema_change' and table_name = 'stale_forward_child';
deallocate prepare forward_parent_stmt;
set foreign_key_checks = 1;

create table view_src (a int, b int);
prepare view_stmt from 'create view prepared_view as select a from view_src';
alter table view_src drop column a;
execute view_stmt;
select count(*)
from information_schema.views
where table_schema = 'prepare_ddl_schema_change' and table_name = 'prepared_view';
deallocate prepare view_stmt;

prepare missing_drop_stmt from 'drop table if exists appears_later';
create table appears_later (a int);
execute missing_drop_stmt;
select count(*)
from information_schema.tables
where table_schema = 'prepare_ddl_schema_change' and table_name = 'appears_later';
deallocate prepare missing_drop_stmt;

create table pitr_target (a int);
prepare pitr_stmt from
    'create pitr prepared_pitr for table prepare_ddl_schema_change pitr_target range 1 ''d''';
drop table pitr_target;
create table pitr_target (b bigint);
execute pitr_stmt;
select count(*)
from mo_catalog.mo_pitr p
join mo_catalog.mo_tables t on p.obj_id = t.rel_id
where p.pitr_name = 'prepared_pitr'
  and t.reldatabase = 'prepare_ddl_schema_change'
  and t.relname = 'pitr_target';
deallocate prepare pitr_stmt;
drop pitr prepared_pitr;

create database prepared_drop_database;
prepare drop_database_stmt from 'drop database prepared_drop_database';
create publication prepared_publication database prepared_drop_database account all;
execute drop_database_stmt;
show databases like 'prepared_drop_database';
deallocate prepare drop_database_stmt;
drop publication prepared_publication;
drop database prepared_drop_database;

create database recreated_dependency_db;
prepare recreated_drop_stmt from 'drop table if exists recreated_dependency_db.t';
drop database recreated_dependency_db;
create database recreated_dependency_db;
create table recreated_dependency_db.t (a int);
execute recreated_drop_stmt;
select count(*)
from information_schema.tables
where table_schema = 'recreated_dependency_db' and table_name = 't';
deallocate prepare recreated_drop_stmt;
drop database recreated_dependency_db;

prepare temporary_stmt from 'create temporary table prepared_temporary (a int)';
execute temporary_stmt;
execute temporary_stmt;
show tables like 'prepared_temporary';
deallocate prepare temporary_stmt;

set foreign_key_checks = 0;
create table expanded_parent (id int primary key);
create table expanded_src (
    pid int,
    foreign key (pid) references expanded_parent(id)
);
prepare expanded_like_stmt from 'create table expanded_like like expanded_src';
alter table expanded_parent drop primary key;
execute expanded_like_stmt;
select count(*)
from information_schema.referential_constraints
where constraint_schema = 'prepare_ddl_schema_change' and table_name = 'expanded_like';
deallocate prepare expanded_like_stmt;
set foreign_key_checks = 1;

prepare missing_database_stmt from 'drop table if exists prepared_future_db.t';
create database prepared_future_db;
create table prepared_future_db.t (a int);
execute missing_database_stmt;
select count(*)
from information_schema.tables
where table_schema = 'prepared_future_db' and table_name = 't';
deallocate prepare missing_database_stmt;
drop database prepared_future_db;

set foreign_key_checks = 0;
prepare late_reverse_fk_stmt from 'create table late_reverse_parent (id int primary key)';
create database late_reverse_db;
create table late_reverse_db.late_reverse_child (
    pid int,
    foreign key (pid) references prepare_ddl_schema_change.late_reverse_parent(id)
);
execute late_reverse_fk_stmt;
select count(*)
from information_schema.referential_constraints
where constraint_schema = 'late_reverse_db' and table_name = 'late_reverse_child';
deallocate prepare late_reverse_fk_stmt;
drop database late_reverse_db;
set foreign_key_checks = 1;

create table snapshot_baseline (id int);
begin;
prepare snapshot_baseline_stmt from
    'alter table snapshot_baseline add column note int';
-- @session:id=1
use prepare_ddl_schema_change;
alter table snapshot_baseline add column note int;
-- @session
execute snapshot_baseline_stmt;
deallocate prepare snapshot_baseline_stmt;
rollback;

create table local_ddl_generation (id int);
begin;
prepare local_ddl_stmt from
    'alter table local_ddl_generation add column note int';
alter table local_ddl_generation add column note int;
execute local_ddl_stmt;
deallocate prepare local_ddl_stmt;
rollback;

drop database prepare_ddl_schema_change;
