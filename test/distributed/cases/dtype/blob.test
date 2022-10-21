#Test cases of query with single table and operators
drop table if exists t1;
create table t1 (a blob);
insert into t1 values('abcdef');
insert into t1 values('_bcdef');
insert into t1 values('a_cdef');
insert into t1 values('ab_def');
insert into t1 values('abc_ef');
insert into t1 values('abcd_f');
insert into t1 values('abcde_');
select * from t1 where a like 'ab\_def' order by 1 asc;
select * from t1 where a not like 'a%' order by a desc;
select * from t1 where a like "\__cdef" order by 1 desc;
select * from t1 where a not like "%d_\_";
drop table if exists t1;

#Test group by
create table t1 (i int,a blob);
insert into t1 values(1,'123456');
insert into t1 values(2,'234567');
insert into t1 values(3,'345678');
SELECT * FROM t1;
select a from t1 where i > 1;
insert into t1 values(2,'123456');
insert into t1 values(6,'234567');
insert into t1 values(7,'123456');
select sum(i),a from t1 group by a;

#Test join
create table t2 (j int,b blob);
insert into t2 values(10,'123456');
insert into t2 values(11,'000001');
insert into t2 values(12,'000000');
select i,a from t1 inner join t2 on t1.a = t2.b order by i;
select i,a from t1 right join t2 on t1.a = t2.b order by i;
select i,a from t1 left join t2 on t1.a = t2.b order by i;

#Load file
SELECT load_file('$resources/file_test/normal.txt');
insert into t2 values(66,load_file("$resources/file_test/normal.txt"));
select * from t2;

#Length
SELECT LENGTH(a) FROM t1;
SELECT LENGTH(load_file("$resources/file_test/normal.txt"));
SELECT LENGTH(load_file("$resources/file_test/empty.txt"));
SELECT SUBSTRING(a,1,4), LENGTH(a) FROM t1 GROUP BY a;
drop table t1;
drop table t2;