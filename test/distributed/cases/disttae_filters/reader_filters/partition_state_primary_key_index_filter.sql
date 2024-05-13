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
-- @ignore:0
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


-- case 4: prepare for insert
create table t4(a int, b int, c int, d int, primary key (a,b,c));
insert into t4 select *,*,*,* from generate_series(1, 1000000)g;
select d from t4 where a in (1,2,3);
prepare s1 from insert into t4(a, b, c, d) values(?, ?, ?, ?), (?, ?, ?, ?), (?, ?, ?, ?), (?, ?, ?, ?),(?, ?, ?, ?);
set @a = -1;
set @b = -2;
set @c = -3;
set @d = -4;
set @e =-5;
execute s1 using @a,@a,@a,@a, @b,@b,@b,@b, @c,@c,@c,@c, @d,@d,@d,@d, @e,@e,@e,@e;
select d from t4 where a in (-1,-2,-3,-4,-5);
drop table t4;
drop database testdb;
