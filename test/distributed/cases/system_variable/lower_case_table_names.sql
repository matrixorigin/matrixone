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


## alter table with lower_case_table_names = 0
-- @session:id=5&user=a1:admin1&password=test123
# set to 0
set global lower_case_table_names = 0;
-- @session

-- @session:id=6&user=a1:admin1&password=test123
# it's 0 now
select @@lower_case_table_names;
show variables like "%lower%";

create database if not exists test;
use test;
drop table if exists Tt;
drop table if exists TT;
create table Tt (Aa int);
insert into Tt values (1), (2), (3);
select Aa from Tt;
create table TT (c1 int);
show tables;
alter table TT add column c2 int; -- should work
alter table `TT` add column c3 int; -- should work as well
select * from TT;
select * from `TT`;
select * from Tt;
select * from `Tt`;

drop database test;
-- @session


-- @session:id=7&user=a1:admin1&password=test123
# it is 0 now
select @@lower_case_table_names;

select table_name from INFORMATION_SCHEMA.TABLES limit 0;
select table_name from INFORMATION_SCHEMA.tables limit 0;
select table_name from information_schema.TABLES limit 0;
select table_name from information_schema.tables limit 0;

use INFORMATION_SCHEMA;
use information_schema;
select table_name from TABLES limit 0;
select table_name from tables limit 0;

# reset to 1
set global lower_case_table_names = 1;
-- @session

-- @session:id=8&user=a1:admin1&password=test123
# default value is 1
select @@lower_case_table_names;

select table_name from INFORMATION_SCHEMA.TABLES limit 0;
select table_name from INFORMATION_SCHEMA.tables limit 0;
select table_name from information_schema.TABLES limit 0;
select table_name from information_schema.tables limit 0;

use INFORMATION_SCHEMA;
use information_schema;
select table_name from TABLES limit 0;
select table_name from tables limit 0;
-- @session

drop account a1;