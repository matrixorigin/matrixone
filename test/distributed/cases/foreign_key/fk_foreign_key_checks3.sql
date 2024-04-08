drop database if exists fk_foreign_key_checks3;
create database fk_foreign_key_checks3;
use fk_foreign_key_checks3;

SET FOREIGN_KEY_CHECKS=0;

create table t1( b int,
                 c int,
                 constraint `c1` foreign key `fk1` (b) references fk_foreign_key_checks3_db0.t2(a),
                 constraint `c2` foreign key `fk2` (c) references fk_foreign_key_checks3_db1.t3(c));

drop database if exists fk_foreign_key_checks3_db0;
create database fk_foreign_key_checks3_db0;
create table fk_foreign_key_checks3_db0.t2(a int primary key,b int);

drop database if exists fk_foreign_key_checks3_db1;
create database fk_foreign_key_checks3_db1;
create table fk_foreign_key_checks3_db1.t3(c int primary key,d int);

--no error
insert into t1 values (2,2);

SET FOREIGN_KEY_CHECKS=1;

--error
insert into t1 values (1,1);

insert into fk_foreign_key_checks3_db0.t2 values (1,1);
insert into fk_foreign_key_checks3_db1.t3 values (1,1);

--no error
insert into t1 values (1,1);

--error
drop table fk_foreign_key_checks3_db0.t2;
drop table fk_foreign_key_checks3_db1.t3;

--mysql error
drop database fk_foreign_key_checks3_db0;
drop database fk_foreign_key_checks3_db1;

SET FOREIGN_KEY_CHECKS=0;

--no error
drop database fk_foreign_key_checks3_db0;

--no error
drop database fk_foreign_key_checks3_db1;

--no error.
insert into t1 values (3,3);

SET FOREIGN_KEY_CHECKS=1;

--no error. but mysql error.
insert into t1 values (3,3);

--no error
create database fk_foreign_key_checks3_db0;
create table fk_foreign_key_checks3_db0.t2(a int primary key,b int);

--no error
create database fk_foreign_key_checks3_db1;
create table fk_foreign_key_checks3_db1.t3(c int primary key,d int);

--error
insert into t1 values (3,3);

--error
insert into t1 values (4,4);

--no error
insert into fk_foreign_key_checks3_db0.t2 values (4,4);
insert into fk_foreign_key_checks3_db1.t3 values (4,4);

--no error
insert into t1 values (4,4);

--error
insert into t1 values (3,3);

drop table fk_foreign_key_checks3.t1;

--no error
drop table fk_foreign_key_checks3_db0.t2;
drop table fk_foreign_key_checks3_db1.t3;

--no error
drop database fk_foreign_key_checks3_db0;
drop database fk_foreign_key_checks3_db1;

drop database if exists fk_foreign_key_checks3;