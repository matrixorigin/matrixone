drop database if exists fk_foreign_key_checks4;
create database fk_foreign_key_checks4;
use fk_foreign_key_checks4;

create table t1(a int primary key);

create table t2(b int, constraint c1 foreign key (b) references t1(a));

insert into t1 values (1),(2),(3);

insert into t2 values (1),(2),(3);

--error
insert into t2 values (4);

-- rename table name
alter table t1 RENAME t1copy;

--error
insert into t2 values (4);

--no error
insert into t1copy values (4);

--no error
insert into t2 values (4);

-- rename table name
alter table t2 RENAME t2copy;

--error
insert into t2copy values (5);

--no error
insert into t1copy values (5);

--no error
insert into t2copy values (5);

--rename table name
alter table t1copy RENAME t1;

--error
insert into t2copy values (6);

--no error
insert into t1 values (6);

--no error
insert into t2copy values (6);

--rename table name
alter table t2copy RENAME t2;

--error
insert into t2 values (7);

--no error
insert into t1 values (7);

--no error
insert into t2 values (7);

--rename column
alter table t1 RENAME COLUMN a to acopy;

--error
insert into t2 values (8);

--no error
insert into t1 values (8);

--no error
insert into t2 values (8);

--rename column
alter table t2 RENAME COLUMN b to bcopy;


--error
insert into t2 values (9);

--no error
insert into t1 values (9);

--no error
insert into t2 values (9);


--rename column
alter table t2 RENAME COLUMN bcopy to b;

--error
insert into t2 values (10);

--no error
insert into t1 values (10);

--no error
insert into t2 values (10);

--change column name
alter table t1 change column a acopy int;

--error
insert into t2 values (11);

--no error
insert into t1 values (11);

--no error
insert into t2 values (11);

--change column name
alter table t2 change column b bcopy int;

--error
insert into t2 values (12);

--no error
insert into t1 values (12);

--no error
insert into t2 values (12);


--change column name
alter table t1 change column acopy a int;

--error
insert into t2 values (13);

--no error
insert into t1 values (13);

--no error
insert into t2 values (13);


--change column name
alter table t2 change column bcopy b int;

--error
insert into t2 values (14);

--no error
insert into t1 values (14);

--no error
insert into t2 values (14);

--add column c
alter table t1 add column c int unique;


create table t3(c int, constraint c1 foreign key (c) references t1(c));
create table t4(b int, constraint c1 foreign key (b) references t1(a));

--rename table name
alter table t1 RENAME t1copy;

--error
insert into t2 values (15);
insert into t3 values (15);
insert into t4 values (15);

--no error
insert into t1copy values (15,15);

--no error
insert into t2 values (15);
insert into t3 values (15);
insert into t4 values (15);

--rename column a,c of t1copy
alter table t1copy RENAME COLUMN a TO acopy, RENAME COLUMN c TO ccopy;


--error
insert into t2 values (16);
insert into t3 values (16);
insert into t4 values (16);

--no error
insert into t1copy values (16,16);

--no error
insert into t2 values (16);
insert into t3 values (16);
insert into t4 values (16);

--rename column c of t3
alter table t3 RENAME COLUMN c TO ccopy;

--rename column b of t4
alter table t4 RENAME COLUMN b TO bcopy;

--error
insert into t2 values (17);
insert into t3 values (17);
insert into t4 values (17);

--no error
insert into t1copy values (17,17);

--no error
insert into t2 values (17);
insert into t3 values (17);
insert into t4 values (17);

--rename table
alter table t3 RENAME t3copy;
alter table t4 RENAME t4copy;

--error
insert into t2 values (18);
insert into t3copy values (18);
insert into t4copy values (18);

--no error
insert into t1copy values (18,18);

--no error
insert into t2 values (18);
insert into t3copy values (18);
insert into t4copy values (18);


--rename table & column
alter table t3copy RENAME t3;
alter table t3 RENAME COLUMN ccopy TO c;

--error
insert into t2 values (19);
insert into t3 values (19);
insert into t4copy values (19);

--no error
insert into t1copy values (19,19);

--no error
insert into t2 values (19);
insert into t3 values (19);
insert into t4copy values (19);

--rename table & column
alter table t1copy RENAME t1;
alter table t1 RENAME COLUMN acopy TO a, RENAME COLUMN ccopy TO c;

--error
insert into t2 values (20);
insert into t3 values (20);
insert into t4copy values (20);

--no error
insert into t1 values (20,20);

--no error
insert into t2 values (20);
insert into t3 values (20);
insert into t4copy values (20);

--rename table & column
alter table t4copy RENAME t4;
alter table t4 RENAME COLUMN bcopy TO b;

--error
insert into t2 values (21);
insert into t3 values (21);
insert into t4 values (21);

--no error
insert into t1 values (21,21);

--no error
insert into t2 values (21);
insert into t3 values (21);
insert into t4 values (21);

--error
drop table t1;

--drop foreign key
alter table t2 drop foreign key c1;

--error
drop table t1;

--no error
insert into t2 values (22);
--error
insert into t3 values (22);
insert into t4 values (22);

--no error
insert into t1 values (22,22);

--no error
insert into t2 values (22);
insert into t3 values (22);
insert into t4 values (22);

--drop foreign key
alter table t3 drop foreign key c1;

--error
drop table t1;

--no error
insert into t2 values (23);
insert into t3 values (23);
--error
insert into t4 values (23);

--no error
insert into t1 values (23,23);

--no error
insert into t2 values (23);
insert into t3 values (23);
insert into t4 values (23);

--drop foreign key
alter table t4 drop foreign key c1;

--no error
insert into t2 values (24);
insert into t3 values (24);
insert into t4 values (24);

--no error
drop table t1;

drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
drop table if exists t4;

drop database if exists fk_foreign_key_checks4;