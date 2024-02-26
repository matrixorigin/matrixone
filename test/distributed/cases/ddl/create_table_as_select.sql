create database test;
use test;

create table t1(a int default 123, b char(5));
desc t1;
INSERT INTO t1 values (1, '1');
INSERT INTO t1 values (2, '2');
INSERT INTO t1 values (0x7fffffff, 'max');
select * from t1;

CREATE table t2 (c float) as select b, a from t1;
desc t2;
select * from t2;

CREATE table if not exists t2 (d float) as select b, a from t1;
desc t2;

CREATE table t3 (a bigint unsigned not null auto_increment primary key, c float) as select a, b from t1;
desc t3;
select * from t3;

CREATE table t4 (a tinyint) as select * from t1;

CREATE table t5 (a char(10)) as select * from t1;
desc t5;
select * from t5;

insert into t1 values (1, '1_1');
select * from t1;
CREATE table t6 (a int unique) as select * from t1;
drop table t6;

CREATE table t6 as select max(a) from t1;
desc t6;
select * from t6;

CREATE table t7 as select * from (select * from t1) as t;
desc t7;
select * from t7;

CREATE table t8 as select a as alias_a, 1 from t1;
desc t8;
select * from t8;

CREATE table t9 (index (a)) as select * from t1;
desc t9;
select * from t9;

drop table t1;
drop table t2;
drop table t3;
drop table t5;
drop table t6;
drop table t7;
drop table t8;
drop table t9;
drop database test;
