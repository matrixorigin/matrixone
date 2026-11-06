-- Partitioned foreign keys must fail during planning, without leaving metadata.
drop database if exists issue_31317;
create database issue_31317;
use issue_31317;

create table parent_hash(a int primary key) partition by hash(a) partitions 2;
create table parent_range(a int primary key) partition by range(a)
  (partition p0 values less than (10), partition p1 values less than maxvalue);
create table parent_normal(a int primary key);
create table child_normal(a int);
create table child_hash(a int primary key) partition by hash(a) partitions 2;

create table bad_parent(a int, foreign key(a) references parent_hash(a));
create table bad_range(a int, foreign key(a) references parent_range(a));
alter table child_normal add constraint fk_parent foreign key(a) references parent_hash(a);
alter table child_normal add column extra int, add constraint fk_copy foreign key(a) references parent_hash(a);
alter table child_hash add constraint fk_child foreign key(a) references parent_normal(a);
create table bad_child(a int primary key, foreign key(a) references parent_normal(a)) partition by hash(a) partitions 2;
select count(*) from information_schema.columns where table_schema='issue_31317' and table_name='child_normal' and column_name='extra';
show create table child_normal;
show create table child_hash;

-- A deferred FK on a normal child is allowed, but must not bind to a
-- partitioned parent, and the failed parent creation must leave no table.
set foreign_key_checks=0;
create table bad_forward(a int primary key, foreign key(a) references missing_parent(a)) partition by hash(a) partitions 2;
create table deferred_child(a int, foreign key(a) references deferred_parent(a));
create table deferred_parent(a int primary key) partition by hash(a) partitions 2;
show tables like 'deferred_parent';
create table deferred_parent(a int primary key);
set foreign_key_checks=1;

-- Ordinary and self-referencing FKs retain their existing behavior.
create table good_child(a int, foreign key(a) references parent_normal(a));
create table self_fk(id int primary key, parent int, foreign key(parent) references self_fk(id));
insert into parent_normal values(1);
insert into good_child values(1);
insert into self_fk values(1,null),(2,1);
insert into good_child values(999);
select * from good_child order by a;
select * from self_fk order by id;
select 1;
drop database issue_31317;
