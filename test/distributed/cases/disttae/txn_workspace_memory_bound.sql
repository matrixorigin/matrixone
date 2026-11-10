drop database if exists bvt_workspace_memory;
create database bvt_workspace_memory;
use bvt_workspace_memory;
create table t(pk varchar(128) primary key, v int);
insert into t values ('one',1),('two',2);
select enable_fault_injection();
select add_fault_point('fj/cn/workspace_allocation_limit',':::','echo',1,'bvt_workspace_memory');
-- @regex('.*allocation account capacity exceeded.*', true)
insert into t values ('three',3);
select count(*) as n, sum(v) as total from t;
-- @regex('.*allocation account capacity exceeded.*', true)
delete from t where pk='one';
select count(*) as n, sum(v) as total from t;
-- @regex('.*allocation account capacity exceeded.*', true)
update t set v=10 where pk='two';
select count(*) as n, sum(v) as total from t;
select remove_fault_point('fj/cn/workspace_allocation_limit');
select disable_fault_injection();
update t set v=10 where pk='two';
delete from t where pk='one';
insert into t values ('three',3);
select count(*) as n, sum(v) as total from t;
drop database bvt_workspace_memory;
