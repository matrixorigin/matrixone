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
-- Earlier uncommitted rows and the account must survive statement rollback.
create table copy_tx(v tinyint, payload mediumtext);
create table rowid_tx(v tinyint);
select enable_fault_injection();
select add_fault_point('fj/cn/workspace_allocation_limit',':::','echo',65536,'bvt_workspace_memory');
begin;
insert into copy_tx values (1,'prior');
-- @regex('.*allocation account capacity exceeded.*', true)
insert into copy_tx values (9,repeat('x',131072));
select count(*) as n, sum(v) as total from copy_tx;
insert into copy_tx values (3,'later');
commit;
select count(*) as n, sum(v) as total from copy_tx;
begin;
insert into rowid_tx values (1);
-- The narrow source fits; 4096 generated rowids exceed the account limit.
-- @regex('.*allocation account capacity exceeded.*', true)
insert into rowid_tx select cast(9 as tinyint) from generate_series(1,4096) g;
select count(*) as n, sum(v) as total from rowid_tx;
insert into rowid_tx values (3);
commit;
select count(*) as n, sum(v) as total from rowid_tx;
select remove_fault_point('fj/cn/workspace_allocation_limit');
select disable_fault_injection();
drop database bvt_workspace_memory;
