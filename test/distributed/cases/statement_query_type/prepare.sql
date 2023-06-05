-- prepare
drop account if exists bvt_query_type;
create account if not exists `bvt_query_type` ADMIN_NAME 'admin' IDENTIFIED BY '123456';

-- prepare data
-- @session:id=1&user=bvt_query_type:admin:accountadmin&password=123456
drop database if exists test_db;
create database test_db;
use test_db;
drop table if exists test_table;
create table test_table(
col1 int,
col2 varchar
);

insert into test_table values (1,'a'),(2,'b'),(3,'c');

prepare s1 from select * from test_table where col1=?;
set @a=2;
execute s1 using @a;
begin;
execute s1 using @a;
rollback;

deallocate prepare s1;
-- @session

drop account if exists bvt_query_type;
