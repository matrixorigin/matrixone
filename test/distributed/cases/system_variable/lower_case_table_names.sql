create account a1 ADMIN_NAME 'admin1' IDENTIFIED BY 'test123';

-- @session:id=1&user=a1:admin1&password=test123
# default value is 1
select @@lower_case_table_names;

create database test;
use test;
create table t1 (a int);
insert into t1 values (1), (2), (3);
select * from t1;

select A from t1;
select a from t1;
select A from T1;
select tmp.a from t1 as tmp;
select TMP.a from t1 as tmp;
select tmp.aA from (select a as Aa from t1) as tmp;
select tmp.Aa from (select a as Aa from t1) as tmp;
select TMp.aA from (select a as Aa from t1) as tmp;
select TMp.Aa from (select a as Aa from t1) as tmp;

# set to 0
set global lower_case_table_names = 0;
-- @session

-- @session:id=2&user=a1:admin1&password=test123
# it's 0 now
select @@lower_case_table_names;

use test;
select A from t1;
select a from t1;
# can't find T1
select A from T1;
select tmp.a from t1 as tmp;
# can't find TMP
select TMP.a from t1 as tmp;
select tmp.aA from (select a as Aa from t1) as tmp;
select tmp.Aa from (select a as Aa from t1) as tmp;
# can't find TMp
select TMp.aA from (select a as Aa from t1) as tmp;
select TMp.Aa from (select a as Aa from t1) as tmp;

drop database test;
-- @session

-- @session:id=3&user=a1:admin1&password=test123
# it's 0 now
select @@lower_case_table_names;

# create table with lower_case_table_names = 0
create database test;
use test;
create table T1 (a int);
insert into T1 values (1), (2), (3);
select * from T1;

# set to 1
set global lower_case_table_names = 1;
-- @session

-- @session:id=4&user=a1:admin1&password=test123
# it's 1 now
select @@lower_case_table_names;

use test;
# select with lower_case_table_names = 0
# can't find T1 any more
select * from t1;
select * from T1;

# reset
drop database test;
-- @session

drop account a1;