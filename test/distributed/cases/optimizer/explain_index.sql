drop database if exists d1;
create database d1;
use d1;
drop table if exists t1;
create table t1(c1 int, c2 int, c3 int, c4 int, c5 int, primary key(c1,c2));
insert into t1 select result,result*3, result%10000, result%5+1, result%10-1 from generate_series(1,100000)g;
create index t1i1 on t1(c3,c4,c5);
create unique index t1i2 on t1(c2,c5);
-- @separator:table
select mo_ctl('dn', 'flush', 'd1.t1');
select Sleep(1);
-- @separator:table
explain select c3,c4,c5 from t1 where c3=1;
select c3,c4,c5 from t1 where c3=1;
-- @separator:table
explain select count(*) from t1 where c3 <30;
select count(*) from t1 where c3<30;
-- @separator:table
explain select c3,c4,c5 from t1 where c3 in (1,5,10,20);
select c3,c4,c5 from t1 where c3 in (1,5,10,20);
-- @separator:table
explain select c3,c4,c5 from t1 where c3 between 4 and 7 and c5=5;
select c3,c4,c5 from t1 where c3 between 4 and 7 and c5=5;
-- @separator:table
explain select c2,c5 from t1 where c2=6;
select c2,c5 from t1 where c2=6;
-- @separator:table
explain select c2,c5 from t1 where c2 <10;
select c2,c5 from t1 where c2<10;
-- @separator:table
explain select c2,c5 from t1 where c2 in(11,15,110,210);
select c2,c5 from t1 where c2 in(11,15,110,210);
-- @separator:table
explain select c2,c5 from t1 where c2 between 1 and 17;
select c2,c5 from t1 where c2 between 1 and 17;
-- @separator:table
explain select * from t1 where c3=1;
select * from t1 where c3=1;
-- @separator:table
explain select * from t1 where c2=12;
select * from t1 where c2=12;
-- @separator:table
explain select count(*) from t1 where c3 between 100 and 200;
select count(*) from t1 where c3 between 100 and 200;
-- @separator:table
explain select count(*) from t1 where c3 <500;
select count(*) from t1 where c3 <500;
-- @separator:table
explain select count(*) from t1 where c3 in(1,13,15,90,99);
select count(*) from t1 where c3 in(1,13,15,90,99);
-- @separator:table
explain select count(*) from t1 where c3 between 1 and 100 and c5 <100;
select count(*) from t1 where c3 between 1 and 100 and c5 <100;
-- @separator:table
explain select count(*) from t1 where c3 between 100 and 200 and c5 =-1;
select count(*) from t1 where c3 between 100 and 200 and c5 =-1;
drop database d1;