#Empty函数在MySQL里没有，是模仿Clickhouse语义的函数，只能在MO里测试

#SELECT,data type
drop table if exists t1;
create table t1(a varchar(255),b char(255));
insert into t1 values('', 'abcd');
insert into t1 values('1111', '');
select empty(a),empty(b) from t1;
drop table t1;

#NULL,data type
drop table if exists t1;
create table t1(a varchar(255),b char(255));
insert into t1 values(NULL, NULL);
select empty(a),empty(b) from t1;
drop table t1;


