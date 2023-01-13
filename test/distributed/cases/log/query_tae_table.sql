
create account `query_tae_table` ADMIN_NAME 'admin' IDENTIFIED BY '123456';

-- @session:id=2&user=query_tae_table:admin:accountadmin&password=123456
drop database if exists `query_tae_table`;
create database `query_tae_table`;
use query_tae_table;
drop table if exists `query_tae_table`;
set @uuid_drop_table = last_uuid();
create table query_tae_table (i int);
set @uuid_create_table = last_uuid();
insert into query_tae_table values (1);
set @uuid_insert_table = last_uuid();
-- @session

select sleep(16);

select account from system.statement_info where statement_id = @uuid_create_table;
