drop account if exists test_tenant_1;
create account test_tenant_1 admin_name 'test_account' identified by '111';

drop database if exists republication01;
create database republication01;
use republication01;
create publication publication01 database republication01 account test_tenant_1 comment 'republish';
create table repub01(col1 int);
create table repub_hidden(col1 int);
insert into repub01 values (1);

-- @session:id=1&user=test_tenant_1:test_account&password=111
drop database if exists resub01;
create database resub01 from sys publication publication01;
-- @ignore:5,7
show subscriptions all;
-- @session

drop database if exists database03;
create database database03;
use database03;
create table table01 (col1 int);
insert into table01 values (1);
insert into table01 select * from table01;
alter publication publication01 database database03;
-- @ignore:5,6
show publications;

-- @session:id=2&user=test_tenant_1:test_account&password=111
use resub01;
show tables;
select * from table01;
show columns from table01;
desc table01;
-- @ignore:3,4,5,7,9,10,11,12
show table status;
-- @session

alter publication publication01 database republication01 table repub01;
-- @ignore:5,6
show publications;

-- @session:id=3&user=test_tenant_1:test_account&password=111
-- @ignore:5,7
show subscriptions all;
-- @session

create publication publication02 database republication01 table repub01 account test_tenant_1;

-- @session:id=3&user=test_tenant_1:test_account&password=111
create database resub02 from sys publication publication02;
select table_schema, table_name from information_schema.tables
where table_schema in ('resub01', 'resub02') order by table_schema, table_name;
select table_schema, table_name, column_name from information_schema.columns
where table_schema in ('resub01', 'resub02') order by table_schema, table_name, ordinal_position;
use resub01;
show tables;
select table_schema, table_name from information_schema.tables
where table_schema = 'resub01' order by table_name;
select auto_increment from information_schema.tables
where table_schema = 'resub01' and table_name = 'repub01';
-- @ignore:14,15,16,19,20
select * from information_schema.tables
where table_schema = 'resub01' and table_name = 'repub01';
select table_schema, table_name, column_name, ordinal_position from information_schema.columns
where table_schema = 'resub01' order by table_name, ordinal_position;
select * from mo_subscription_tables();
create database subscription_metadata_wrapper;
create view subscription_metadata_wrapper.v as select * from mo_subscription_columns();
drop database subscription_metadata_wrapper;
show columns from repub01;
desc repub01;
select * from repub01;
-- @session

drop publication publication01;
drop publication publication02;

-- @session:id=4&user=test_tenant_1:test_account&password=111
select count(*) from information_schema.tables where table_schema = 'resub01';
select count(*) from information_schema.columns where table_schema = 'resub01';
-- @session

drop database database03;
drop database republication01;
drop account test_tenant_1;
