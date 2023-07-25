create account acc_idx ADMIN_NAME 'root' IDENTIFIED BY '123456';
-- @session:id=2&user=acc_idx:root&password=123456
create database test;
use test;
create table t1(a int);
insert into t1 values(1);
insert into t1 values(2);
insert into t1 values(3);
select * from t1;
-- @session
alter account acc_idx restricted;

-- @session:id=3&user=acc_idx:root&password=123456
use test;
insert into test.t1 values(4);
delete from test.t1 where a > 1;
select * from test.t1;
-- @session
alter account acc_idx open;

-- @session:id=4&user=acc_idx:root&password=123456
use test;
insert into test.t1 values(4);
insert into test.t1 values(5);
select * from test.t1;
drop database test;
-- @session

drop account acc_idx;