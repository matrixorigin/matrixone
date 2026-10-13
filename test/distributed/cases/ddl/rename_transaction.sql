-- @suit
-- @case
-- @desc: RENAME TABLE uses a MySQL implicit transaction boundary (#29295).
-- @label:bvt
drop database if exists rename_transaction;
create database rename_transaction;
use rename_transaction;
create table markers(id int primary key);
create table nc_workflow_executions(id int);
begin;
insert into markers values(1);
rename table nc_workflow_executions to nc_automation_executions;
rollback;
select * from markers order by id;
show tables;
begin;
insert into markers values(2);
prepare r from 'rename table nc_automation_executions to renamed';
execute r;
rollback;
deallocate prepare r;
select * from markers order by id;
show tables;
set autocommit=0;
insert into markers values(3);
rename table renamed to intermediate, intermediate to final_name;
insert into markers values(4);
rollback;
select * from markers order by id;
select @@autocommit;
show tables;
set autocommit=1;
-- Legal quoted names must survive the idxcron metadata update even without jobs (#29864).
create table `old\`(id int);
insert into `old\` values(7);
rename table `old\` to `new\`;
select id from `new\`;
drop table `new\`;
drop database rename_transaction;
