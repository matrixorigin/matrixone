-- @suite
-- @case
-- @desc: issue #31163 - fresh tables must not fabricate UPDATE_TIME/CHECK_TIME in information_schema.TABLES
-- @label:bvt

drop database if exists qa_bug31163;
create database qa_bug31163;
create table qa_bug31163.t(id int primary key, v int);
create view qa_bug31163.v1 as select id from qa_bug31163.t;
select count(*) from information_schema.tables where table_schema='qa_bug31163' and create_time is not null and update_time is null and check_time is null;
select count(*) from information_schema.tables where table_schema='qa_bug31163' and table_type='BASE TABLE' and update_time is null and check_time is null;
select count(*) from information_schema.tables where table_schema='qa_bug31163' and (update_time is not null or check_time is not null);
select count(*) from information_schema.tables where table_schema='qa_bug31163' and create_time is null;
drop database qa_bug31163;
