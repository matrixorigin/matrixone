select mo_cpu("total") >= mo_cpu("available");
select mo_memory("total") >= mo_memory("available");
select * from information_schema.files limit 1;

use system;
show tables;
select * from statement_info limit 0;
select * from span_info limit 0;
select * from rawlog limit 0;
select * from log_info limit 0;
select * from error_info limit 0;


create database db01;
use db01;
create table table01 (id int);
insert into table01 values(1);
select * from table01;
select * from table01 limit 0;
create table table02 (id int);
insert into table02 select * from table01 limit 0;
(select * from table01 limit 0) union all (select * from table02 limit 0);

drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';
-- @session:id=1&user=acc01:test_account&password=111
use system;
show tables;
select * from statement_info limit 0;
create database db01;
use db01;
create table table01 (id int);
insert into table01 values(1);
select * from table01;
select * from table01 limit 0;
create table table02 (id int);
insert into table02 select * from table01 limit 0;
(select * from table01 limit 0) union all (select * from table02 limit 0);
-- @session
select * from system.statement_info limit 0;

select * from db01.table01 limit 0;
drop database db01;
drop account if exists acc01;
