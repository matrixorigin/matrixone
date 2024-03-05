drop database if exists fk_foreign_key_checks;
create database fk_foreign_key_checks;
use fk_foreign_key_checks;

SET FOREIGN_KEY_CHECKS=0;

-- no error
-- fk forward reference
drop table if exists t1;
create table t1( b int, constraint `c1` foreign key `fk1` (b) references t2(a));

drop table if exists t2;
create table t2(a int primary key,b int);

-- no error
insert into t2 values (1,1),(2,2),(3,3);
insert into t1 values (1),(2),(3);

SET FOREIGN_KEY_CHECKS=1;

-- error. 1 is referred
delete from t2 where a = 1;

-- error. 4 does not exist in the t2
insert into t1 values (4);

-- no error
insert into t2 values (4,4),(5,5);

-- no error
insert into t1 values (4);

--error
delete from t2 where a = 4;

--no error
delete from t2 where a = 5;

--no error
drop table if exists t3;
create table t3( b int, constraint `c1` foreign key `fk1` (b) references t2(a));

--no error
insert into t3 values (1),(2),(3),(4);

--error
insert into t3 values (5);

--error
delete from t2 where a = 3;

--error
drop table t2;

SET FOREIGN_KEY_CHECKS=0;

--no error
insert into t1 values (5);
insert into t3 values (5);

--no error
delete from t2 where a = 3;

-- no error
drop table t2;

--no error
insert into t1 values (6);
insert into t3 values (6);

SET FOREIGN_KEY_CHECKS=1;

--no error
insert into t1 values (7);
insert into t3 values (7);

create table t2(a int primary key,b int);

--error
insert into t1 values (8);
insert into t3 values (8);

insert into t2 values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8);

--no error
insert into t1 values (8);
insert into t3 values (8);

--error
insert into t1 values (9);
insert into t3 values (9);

--error
delete from t2 where a = 3;

--no error
delete from t1 where b = 3;
delete from t3 where b = 3;

--no error
delete from t2 where a = 3;

--error
drop table t2;

--no error
drop table t1;

--error. t3 also refers to t2
drop table t2;

--error
insert into t3 values (9);

--error
insert into t3 values (3);

--no error
insert into t2 values (3,3);

--error
insert into t3 values (9);

--no error
insert into t3 values (3);

--error
drop table t2;

SET FOREIGN_KEY_CHECKS=0;

--no error
drop table t2;

--no error
insert into t3 values (9);

SET FOREIGN_KEY_CHECKS=1;

create table t2(a int primary key,b int);

--error
insert into t3 values (10);

--no error
insert into t2 values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9);

--no error
insert into t2 values (10,10);

--no error
insert into t3 values (10);

--error
delete from t2 where a = 10;

--error
drop table t2;

drop table t3;

--no error
delete from t2 where a = 10;

--no error
drop table t2;

SET FOREIGN_KEY_CHECKS=0;

create table t2(a int primary key,b int);

SET FOREIGN_KEY_CHECKS=1;

insert into t2 values (1,1),(2,2);

--no error
delete from t2 where a = 2;


drop table if exists t1;
drop table if exists t3;
drop table if exists t2;
drop database if exists fk_foreign_key_checks;