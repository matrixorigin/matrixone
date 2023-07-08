create account abc ADMIN_NAME 'admin' IDENTIFIED BY '123456';
-- @session:id=2&user=abc:admin&password=123456
create database test;
use test;
create table t1(a int);
insert into t1 values(1);
insert into t1 values(2);
insert into t1 values(3);
select * from t1;
a
1
2
3
-- @session
alter account abc restricted;
-- @session:id=3&user=abc:admin&password=123456
insert into test.t1 values(4);
delete from test.t1 where a > 1;
select * from test.t1;
show tables;
drop database test;
-- @session
alter account abc open;
-- @session:id=4&user=abc:admin&password=123456
insert into test.t1 values(4);
insert into test.t1 values(5);
select * from test.t1;
drop database test;
-- @session
drop account abc;