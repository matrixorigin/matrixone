drop database if exists exec_test;
create database exec_test;

drop table if exists t1;
create table t1(a int);

insert into t1 values (1),(2),(3);

prepare st1 from 'select * from t1 where a = ?';
set @a=1;
execute st1 using @a;

explain execute st1 using @a;

explain verbose execute st1 using @a;

explain analyze execute st1 using @a;

explain analyze verbose execute st1 using @a;

drop table if exists t2;
create table t2(a int, b int);

insert into t2 values (1,1),(2,2),(3,3);

select * from t2;

prepare st2 from 'select * from t2 where a = ? and b = ?';

set @a = 1, @b=1;

execute st2 using @a,@b;

explain execute st2 using @a,@b;

explain verbose execute st2 using @a,@b;

explain analyze execute st2 using @a,@b;

explain analyze verbose execute st2 using @a,@b;

drop table if exists t1;
drop table if exists t2;
drop database if exists exec_test;