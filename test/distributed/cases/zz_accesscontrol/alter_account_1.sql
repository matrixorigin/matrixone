create account test_tenant_1 admin_name 'test_account' identified by '111';
-- @session:id=1&user=test_tenant_1:test_account&password=111
create database test;
use test;
create table t1(a int);
insert into t1 values(1);
insert into t1 values(2);
insert into t1 values(3);
select * from t1;
-- @session
alter account test_tenant_1 restricted;
-- @session:id=2&user=test_tenant_1:test_account&password=111
insert into test.t1 values(4);
delete from test.t1 where a > 1;
select * from test.t1;
show tables;
drop database test;
-- @session
alter account test_tenant_1 open;
-- @session:id=3&user=test_tenant_1:test_account&password=111
insert into test.t1 values(4);
insert into test.t1 values(5);
select * from test.t1;
drop database test;
-- @session
drop account test_tenant_1;