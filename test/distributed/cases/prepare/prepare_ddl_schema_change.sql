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

drop database prepare_ddl_schema_change;
