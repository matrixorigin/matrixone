
-- this case only success in linux, not in drawin
create account if not exists `query_tae_table` ADMIN_NAME 'admin' IDENTIFIED BY '123456';

-- @session:id=1&user=query_tae_table:admin:accountadmin&password=123456
select sleep(10);
drop database if exists `query_tae_table`;
create database `query_tae_table`;
/*issue_8168*/use query_tae_table;select syntax error stmt;
use query_tae_table;
drop table if exists `query_tae_table`;
set @uuid_drop_table = last_uuid();
create table query_tae_table (i int);
set @uuid_create_table = last_uuid();
insert into query_tae_table values (1);
set @uuid_insert_table = last_uuid();

-- hide secret keys
create user u identified by '123456';
set @uuid_hide_1 = last_uuid();
create user if not exists abc1 identified by '123', abc2 identified by '234', abc3 identified by '111', abc3 identified by '222';
set @uuid_hide_2 = last_uuid();
create external table t (a int) URL s3option{'endpoint'='s3.us-west-2.amazonaws.com', 'access_key_id'='123', 'secret_access_key'='123', 'bucket'='test', 'filepath'='*.txt', 'region'='us-west-2'};
set @uuid_hide_3 = last_uuid();

-- hide /* cloud_user */
/* cloud_user */select 1;
set @uuid_hide_4 = last_uuid();

select sleep(16);

select account from system.statement_info where statement_id = @uuid_create_table;

-- hide result
select statement from system.statement_info where statement_id in (@uuid_hide_1, @uuid_hide_2, @uuid_hide_3, @uuid_hide_4);

select account, statement from system.statement_info where statement = 'insert into query_tae_table values (1)' and statement_id = @uuid_insert_table limit 1;
-- @session

-- case: select span_kind issue #7571
select IF(span_kind="internal", 1, IF(span_kind="statement", 1, IF(span_kind="session", 1, IF(span_kind="remote", 1, 0)))) as exist from system.rawlog where `raw_item` = "log_info" limit 1;

-- case: fix issue 8168, with syntax error
select status, err_code, error from system.statement_info where account = 'query_tae_table' and statement in ('use query_tae_table', 'select syntax error stmt', '/*issue_8168*/use query_tae_table') and status != 'Running' order by request_at desc limit 3;

-- clean
drop account `query_tae_table`;
