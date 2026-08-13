drop snapshot if exists snapshot_scope_table;
drop snapshot if exists snapshot_scope_database;
drop database if exists snapshot_scope_a;
drop database if exists snapshot_scope_b;

create database snapshot_scope_a;
create database snapshot_scope_b;
create table snapshot_scope_a.t (id int primary key, v varchar(16));
create table snapshot_scope_a.other (id int primary key, v varchar(16));
create table snapshot_scope_b.t (id int primary key, v varchar(16));
insert into snapshot_scope_a.t values (1, 'a_before');
insert into snapshot_scope_a.other values (1, 'other_before');
insert into snapshot_scope_b.t values (1, 'b_before');

create snapshot snapshot_scope_table for table snapshot_scope_a t;
create snapshot snapshot_scope_database for database snapshot_scope_a;
update snapshot_scope_a.t set v = 'a_after';
update snapshot_scope_a.other set v = 'other_after';
update snapshot_scope_b.t set v = 'b_after';

select 'table_snapshot_control' as case_name, v from snapshot_scope_a.t{snapshot = 'snapshot_scope_table'};
select 'table_snapshot_same_database_other_table' as case_name, v from snapshot_scope_a.other{snapshot = 'snapshot_scope_table'};
select 'table_snapshot_other_database' as case_name, v from snapshot_scope_b.t{snapshot = 'snapshot_scope_table'};

select 'database_snapshot_table_control' as case_name, v from snapshot_scope_a.t{snapshot = 'snapshot_scope_database'};
select 'database_snapshot_other_table_control' as case_name, v from snapshot_scope_a.other{snapshot = 'snapshot_scope_database'};
select 'database_snapshot_other_database' as case_name, v from snapshot_scope_b.t{snapshot = 'snapshot_scope_database'};

show create table snapshot_scope_a.t {snapshot = 'snapshot_scope_table'};
show create table snapshot_scope_a.other {snapshot = 'snapshot_scope_table'};
show create table snapshot_scope_a.t {snapshot = 'snapshot_scope_database'};
show create table snapshot_scope_b.t {snapshot = 'snapshot_scope_database'};
show tables from snapshot_scope_a {snapshot = 'snapshot_scope_database'};
show tables from snapshot_scope_a {snapshot = 'snapshot_scope_table'};
show databases {snapshot = 'snapshot_scope_database'};
show databases {snapshot = 'snapshot_scope_table'};
show create database snapshot_scope_a {snapshot = 'snapshot_scope_database'};
show create database snapshot_scope_b {snapshot = 'snapshot_scope_database'};
show create database snapshot_scope_a {snapshot = 'snapshot_scope_table'};

restore table snapshot_scope_a.t{snapshot = 'snapshot_scope_database'};
select 'database_snapshot_restore_same_database' as case_name, v from snapshot_scope_a.t;
restore table snapshot_scope_b.t{snapshot = 'snapshot_scope_database'};
select 'database_snapshot_cross_database_restore_unchanged' as case_name, v from snapshot_scope_b.t;
restore table snapshot_scope_a.t{snapshot = 'snapshot_scope_table'};
select 'table_snapshot_restore_control' as case_name, v from snapshot_scope_a.t;

drop snapshot snapshot_scope_table;
drop snapshot snapshot_scope_database;
drop database snapshot_scope_a;
drop database snapshot_scope_b;
