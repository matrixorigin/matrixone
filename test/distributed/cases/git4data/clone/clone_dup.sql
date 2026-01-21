-- @session:id=1&user=sys:root&password=111
create database repro;
use repro;
create table t1 (a int primary key, b int);
insert into t1 values (1,10),(2,20),(3,30),(4,40);
-- @ignore:0
select mo_ctl('dn','flush','repro.t1');
-- @session

-- @session:id=2&user=sys:root&password=111
use repro;
begin;
create table t2 clone t1;
-- @session

-- @session:id=1&user=sys:root&password=111
update t1 set b = b + 1 where a = 1;
select a, count(*) as cnt from t1 group by a having cnt > 1;
-- @session

-- @session:id=2&user=sys:root&password=111
commit;
-- @session

-- @session:id=1&user=sys:root&password=111
drop database repro;
-- @session