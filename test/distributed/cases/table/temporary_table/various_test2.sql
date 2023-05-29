-- @skip:issue#7889
-- test auto_increment as primary key
drop table if exists t1;
create temporary table t1(
a bigint primary key auto_increment,
b varchar(10)
);
insert into t1(b) values ('bbb');
insert into t1 values (1, 'ccc');
insert into t1 values (3, 'ccc');
insert into t1(b) values ('bbb1111');
select * from t1 order by a;

-- test keyword distinct
drop table if exists t1;
create temporary table t1(
a int,
b varchar(10)
);
insert into t1 values (111, 'a'),(110, 'a'),(100, 'a'),(000, 'b'),(001, 'b'),(011,'b');
select distinct b from t1;
select distinct b, a from t1;
select count(distinct a) from t1;
select sum(distinct a) from t1;
select avg(distinct a) from t1;
select min(distinct a) from t1;
select max(distinct a) from t1;
drop table t1;