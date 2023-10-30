--test mo_locks, mo_transactions

drop database if exists sv_db1;
create database sv_db1;
use sv_db1;

-- @session:id=1{
use sv_db1;
create table t1(a int);
begin;
insert into t1 values (1);
select sleep(10);
-- @session}

select count(*) > 0 from mo_locks() l;
select count(*) > 0 from mo_transactions() t join mo_locks() l where t.txn_id = l.txn_id;

-- @session:id=1{
commit;
-- @session}

drop database if exists sv_db1;