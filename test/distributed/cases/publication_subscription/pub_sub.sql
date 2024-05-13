drop account if exists test_tenant_1;
create account test_tenant_1 admin_name 'test_account' identified by '111';

drop database if exists republication01;
create database republication01;
use republication01;
create publication publication01 database republication01 account test_tenant_1 comment 'republish';
create table repub01(col1 int);
insert into repub01 values (1);

-- @session:id=1&user=test_tenant_1:test_account&password=111
drop database if exists resub01;
create database resub01 from sys publication publication01;
-- @ignore:3,5
show subscriptions all;
-- @session

drop database if exists database03;
create database database03;
use database03;
create table table01 (col1 int);
insert into table01 values (1);
insert into table01 select * from table01;
alter publication publication01 database database03;
-- @ignore:2,3
show publications;

-- @session:id=2&user=test_tenant_1:test_account&password=111
use resub01;
show tables;
select * from table01;
show columns from table01;
desc table01;
-- @ignore:10,11,12
show table status;
-- @session

alter publication publication01 database republication01;
-- @ignore:2,3
show publications;

-- @session:id=3&user=test_tenant_1:test_account&password=111
-- @ignore:3,5
show subscriptions all;
use resub01;
show tables;
show columns from repub01;
desc repub01;
select * from repub01;
-- @session

drop publication publication01;
drop database database03;
drop database republication01;
drop account test_tenant_1;