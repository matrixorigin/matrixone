
-- test load data, integer numbers
drop table if exists t1;
create table t1(
col1 tinyint
);

-- load data
load data inline format='csv', data='1\n2\n' into table t1;
select * from t1;

