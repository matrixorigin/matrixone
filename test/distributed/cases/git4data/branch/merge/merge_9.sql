drop database if exists test;
create database test;
use test;

-- ================================================================
-- MERGE Test 9: transactions are rejected
-- ================================================================

create table t1 (a int, b int, primary key(a));
insert into t1 values (1,1);

create table t2 (a int, b int, primary key(a));
insert into t2 values (1,1),(2,2);

-- MERGE inside an explicit transaction must be rejected with a clear error
begin;
data branch merge t2 into t1 when conflict accept;
rollback;

-- t1 stays unchanged, only (1,1)
select * from t1 order by a;

-- MERGE inside an implicit transaction (autocommit=0) is also rejected
set autocommit = 0;
insert into t1 values (3,3);
data branch merge t2 into t1 when conflict accept;
rollback;
set autocommit = 1;

-- t1 stays unchanged, only (1,1)
select * from t1 order by a;

drop table t1;
drop table t2;

drop database test;
