-- Foreign keys are not supported in conjunction with partitioning (MySQL 1506).
-- A partitioned parent, or adding a FK to an existing partitioned child, must
-- be rejected with that error instead of panicking in the engine.
drop database if exists fk_partitioned_table;
create database fk_partitioned_table;
use fk_partitioned_table;
create table plain_parent(a int primary key);
create table hash_parent(a int primary key) partition by hash(a) partitions 2;
create table range_parent(a int primary key) partition by range(a) (partition p0 values less than (10), partition p1 values less than maxvalue);
create table c1(a int, foreign key(a) references hash_parent(a));
create table c2(a int, constraint fk_c2 foreign key(a) references range_parent(a) on delete cascade);
create table c3(a int primary key, foreign key(a) references plain_parent(a)) partition by hash(a) partitions 2;
create table c4(a int);
alter table c4 add constraint fk_c4 foreign key(a) references hash_parent(a);
create table c5(a int primary key) partition by hash(a) partitions 2;
alter table c5 add constraint fk_c5 foreign key(a) references plain_parent(a);
show create table c5;
-- the server is still healthy and ordinary foreign keys keep working
create table c6(a int, foreign key(a) references plain_parent(a));
insert into plain_parent values (1);
insert into c6 values (1);
insert into c6 values (2);
insert into hash_parent values (1), (2);
select * from hash_parent order by a;
show tables;
-- a forward reference (child first, foreign_key_checks=0) must not bind to a
-- parent that is then created partitioned
set foreign_key_checks = 0;
create table fwd_child(a int, constraint fk_fwd foreign key(a) references fwd_parent(a));
create table fwd_parent(a int primary key) partition by hash(a) partitions 2;
set foreign_key_checks = 1;
show tables;
drop table fwd_child;
create table fwd_parent(a int primary key) partition by hash(a) partitions 2;
drop table fwd_parent;
drop database fk_partitioned_table;
