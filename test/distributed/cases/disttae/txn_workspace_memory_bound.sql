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
-- One rowid buffer fits this budget; a second simultaneous copy does not.
create table fits_tx(v tinyint);
select enable_fault_injection();
select add_fault_point('fj/cn/workspace_allocation_limit',':::','echo',100000,'bvt_workspace_memory');
insert into fits_tx select cast(1 as tinyint) from generate_series(1,2048) g;
select count(*) as n, sum(v) as total from fits_tx;
-- @regex('.*allocation account capacity exceeded.*', true)
insert into fits_tx select cast(9 as tinyint) from generate_series(1,4096) g;
select count(*) as n, sum(v) as total from fits_tx;
select remove_fault_point('fj/cn/workspace_allocation_limit');
select disable_fault_injection();
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
-- Commit merges thirty small batches without copying obsolete rowids.
create table merge_tx(v bigint primary key);
select enable_fault_injection();
select add_fault_point('fj/cn/workspace_allocation_limit',':::','echo',200000,'bvt_workspace_memory');
begin;
insert into merge_tx select result + 0 from generate_series(0,63) g;
insert into merge_tx select result + 64 from generate_series(0,63) g;
insert into merge_tx select result + 128 from generate_series(0,63) g;
insert into merge_tx select result + 192 from generate_series(0,63) g;
insert into merge_tx select result + 256 from generate_series(0,63) g;
insert into merge_tx select result + 320 from generate_series(0,63) g;
insert into merge_tx select result + 384 from generate_series(0,63) g;
insert into merge_tx select result + 448 from generate_series(0,63) g;
insert into merge_tx select result + 512 from generate_series(0,63) g;
insert into merge_tx select result + 576 from generate_series(0,63) g;
insert into merge_tx select result + 640 from generate_series(0,63) g;
insert into merge_tx select result + 704 from generate_series(0,63) g;
insert into merge_tx select result + 768 from generate_series(0,63) g;
insert into merge_tx select result + 832 from generate_series(0,63) g;
insert into merge_tx select result + 896 from generate_series(0,63) g;
insert into merge_tx select result + 960 from generate_series(0,63) g;
insert into merge_tx select result + 1024 from generate_series(0,63) g;
insert into merge_tx select result + 1088 from generate_series(0,63) g;
insert into merge_tx select result + 1152 from generate_series(0,63) g;
insert into merge_tx select result + 1216 from generate_series(0,63) g;
insert into merge_tx select result + 1280 from generate_series(0,63) g;
insert into merge_tx select result + 1344 from generate_series(0,63) g;
insert into merge_tx select result + 1408 from generate_series(0,63) g;
insert into merge_tx select result + 1472 from generate_series(0,63) g;
insert into merge_tx select result + 1536 from generate_series(0,63) g;
insert into merge_tx select result + 1600 from generate_series(0,63) g;
insert into merge_tx select result + 1664 from generate_series(0,63) g;
insert into merge_tx select result + 1728 from generate_series(0,63) g;
insert into merge_tx select result + 1792 from generate_series(0,63) g;
insert into merge_tx select result + 1856 from generate_series(0,63) g;
commit;
select count(*) as n, count(distinct v) as unique_values, sum(v) as total from merge_tx;
select remove_fault_point('fj/cn/workspace_allocation_limit');
select disable_fault_injection();
drop database bvt_workspace_memory;
