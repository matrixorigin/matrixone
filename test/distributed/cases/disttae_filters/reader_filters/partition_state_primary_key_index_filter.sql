drop database if exists testdb;
create database testdb;
use testdb;

-- case 1: serial_full(index, primary_key)
create table t1(a int primary key, b int, c int, index(b));
insert into t1 select *,*,* from generate_series(1, 1000000)g;
select c from t1 where a = 3;

delete from t1 where a = 3;
insert into t1 values(3,3,3);
delete from t1 where a = 3;
insert into t1 values(3,3,3);
delete from t1 where a = 3;
insert into t1 values(3,3,3);
select mo_ctl('dn','checkpoint','');
delete from t1 where a = 3;
insert into t1 values(3,3,3);
delete from t1 where a = 3;
insert into t1 values(3,3,3);
delete from t1 where a = 3;
insert into t1 values(3,3,3);

select c from t1 where b = 3;
select c from t1 where b in (1,2,3);
select c from t1 where a in (1,2,3);
drop table t1;

-- case 2: fake_col in
create table t2(a int, b int, c int, index(b));
insert into t2 select *,*,* from generate_series(1, 1000000)g;
select c from t2 where b = 1;
select c from t2 where b in (1,2,3);

drop table t2;


-- case 3: primary key in
create table t3(a int primary key, b int, c int);
insert into t3 select *,*,* from generate_series(1, 1000000)g;
select c from t3 where a in (1,2,3);

drop table t3;

drop database testdb;
