
-- this case only success in linux, not in drawin
create account if not exists `bvt_query_stmt` ADMIN_NAME 'admin' IDENTIFIED BY '123456';

-- @session:id=1&user=bvt_query_stmt:admin:accountadmin&password=123456
drop database if exists `bvt_query_stmt`;
create database `bvt_query_stmt`;
-- case 1
/*issue_8168*/use bvt_query_stmt;select syntax error stmt;
use bvt_query_stmt;
drop table if exists `bvt_query_stmt`;
set @uuid_drop_table = last_uuid();
-- case 2
create table bvt_query_stmt (i int);
set @uuid_create_table = last_uuid();
-- case 3
insert into bvt_query_stmt values (1);
set @uuid_insert_table = last_uuid();

-- case 4
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

-- @session
-- End case

-- for issue 14,836 and 15,988, move 'result check' into ../zz_statement_query_type/query_stmt.sql

-- clean
drop account `bvt_query_stmt`;
