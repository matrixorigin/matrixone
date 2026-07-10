-- table-level publication should work when the source database name needs identifier quoting
drop account if exists acc_cn_pub_bvt;
create account acc_cn_pub_bvt admin_name = 'test_account' identified by '111';

drop publication if exists pub_cn_table_bvt;
drop database if exists `目标数据_bvt`;
create database `目标数据_bvt`;
use `目标数据_bvt`;
create table resume_data (id int primary key, name varchar(20));
create table extra_data (id int primary key, note varchar(20));
create table hidden_data (id int primary key, note varchar(20));
insert into resume_data values (1, 'alice'), (2, 'bob');
insert into extra_data values (7, 'extra');
insert into hidden_data values (99, 'hidden');

create publication pub_cn_table_bvt database `目标数据_bvt` table resume_data account acc_cn_pub_bvt;
alter publication pub_cn_table_bvt database `目标数据_bvt` table resume_data, extra_data;

-- @session:id=1&user=acc_cn_pub_bvt:test_account&password=111
drop database if exists sub_cn_table_bvt;
create database sub_cn_table_bvt from sys publication pub_cn_table_bvt;
use sub_cn_table_bvt;
show tables;
select * from hidden_data;
select * from resume_data order by id;
select * from extra_data order by id;
drop database sub_cn_table_bvt;
-- @session

drop publication pub_cn_table_bvt;
drop database `目标数据_bvt`;
drop account acc_cn_pub_bvt;
