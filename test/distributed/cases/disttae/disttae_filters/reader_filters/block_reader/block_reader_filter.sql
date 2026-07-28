drop database if exists testdb;
create database testdb;
use testdb;

select enable_fault_injection();

create table t1(a int, b int, index(b));
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'testdb.t1');
insert into t1 select *, * from generate_series(1, 40960)g;
select a from t1 where b = 1;
select a from t1 where b between 1 and 3;
select a from t1 where b in (1,2,3);
drop table t1;

create table t2(a varchar, b varchar, index(b));
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'testdb.t2');
insert into t2 values('1','2'),('3','4'),('5','6'),('7','8'),('a','b'),('c','d'),('e','f'),('g','h');
insert into t2 select * from t2;
insert into t2 select * from t2;
insert into t2 select * from t2;
insert into t2 select * from t2;
insert into t2 select * from t2;
insert into t2 select * from t2;
insert into t2 select * from t2;
insert into t2 select * from t2;
insert into t2 select * from t2;
insert into t2 select * from t2;
insert into t2 select * from t2;
insert into t2 select * from t2;
insert into t2 select * from t2;
select count(*) from t2;
select distinct a from t2 where b = '2';
select distinct a from t2 where b between '2' and '6';
select distinct a from t2 where b in ('2','4','6');
drop table t2;

create table t3 (a float, b float, index(b));
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'testdb.t3');
insert into t3 select *, * from generate_series(1, 40960)g;
select a from t3 where b = 1;
select a from t3 where b between 1 and 3;
select a from t3 where b in (1,2,3);
drop table t3;

-- >, >=, <, <=
create table t4(a int primary key, b int);
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'testdb.t4');
insert into t4 select *, * from generate_series(1, 40960)g;
select b from t4 where a < 3;
select b from t4 where a <= 3;
select b from t4 where a > 1 and a < 3;
select b from t4 where a >= 1 and a < 3;
select b from t4 where a > 1 and a <= 3;
select b from t4 where a >= 1 and a <= 3;
select b from t4 where a < 3 and a = 3;
select b from t4 where a < 1 or a < 3;
select b from t4 where a <= 1 or a <= 3;
select b from t4 where a < 2 or a = 3;
select b from t4 where a < 3 or a = 2;
drop table t4;

create table t5(a varchar(64) primary key, b int);
insert into t5 select cast(result as varchar), result from generate_series(1, 8192) g;
select * from t5 where prefix_eq(a, '819') or a in ('20', '30', '40') order by a asc;

drop table t5;

drop database testdb;

select disable_fault_injection();