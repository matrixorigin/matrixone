-- test load data, integer numbers
drop table if exists t1;
create table t1(
col1 tinyint
);

-- load data
load data inline format='csv', data='1\n2\n' into table t1;
load data  inline format='csv', data=$XXX$
1
2
$XXX$ 
into table t1;
select * from t1;

-- test load data, text
drop table if exists t1;
create table t1(
col1 text
);

-- load data
load data inline format='csv', data='"1
 2"\n"2"\n' into table t1;

select * from t1;

-- test load data, Time and Date type
drop table if exists t4;
create table t4(
col1 date,
col2 datetime,
col3 timestamp,
col4 bool
);

-- load data
load data inline format='csv', data='1000-01-01,0001-01-01,1970-01-01 00:00:01,0
9999-12-31,9999-12-31,2038-01-19,1
' into table t4;
select * from t4;


create table t5(
col1 text
);
load data inline format='jsonline', data='{"col1":"good"}' , jsontype = 'object'  into table t5;
load data inline format='unknow', data='{"col1":"good"}' , jsontype = 'object'  into table t5;