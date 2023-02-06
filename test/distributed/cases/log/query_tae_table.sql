
-- this case only success in linux, not in drawin
create account if not exists `query_tae_table` ADMIN_NAME 'admin' IDENTIFIED BY '123456';

-- @session:id=1&user=query_tae_table:admin:accountadmin&password=123456
drop database if exists `query_tae_table`;
create database `query_tae_table`;
use query_tae_table;
drop table if exists `query_tae_table`;
set @uuid_drop_table = last_uuid();
create table query_tae_table (i int);
set @uuid_create_table = last_uuid();
insert into query_tae_table values (1);
set @uuid_insert_table = last_uuid();

select sleep(16);

select account from system.statement_info where statement_id = @uuid_create_table;

select account, statement from system.statement_info where statement = 'insert into query_tae_table values (1)' and statement_id = @uuid_insert_table limit 1;
-- @session


-- case: select span_kind issue #7571
select span_kind from system.rawlog where `raw_item` = "span_info" and span_name = "NOT_EXIST_SPAN" limit 1;

-- clean
drop account `query_tae_table`;
