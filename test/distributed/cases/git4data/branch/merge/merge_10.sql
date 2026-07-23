drop database if exists test;
create database test;
use test;

-- MERGE reuses explicit transactions, but is rejected when autocommit is disabled
create table t1 (a int, b int, primary key(a));
insert into t1 values (1,1);
create table t2 (a int, b int, primary key(a));
insert into t2 values (1,1),(2,2);

begin;
data branch merge t2 into t1 when conflict accept;
rollback;
select * from t1 order by a;

begin;
data branch merge t2 into t1 when conflict accept;
commit;
select * from t1 order by a;

delete from t1 where a = 2;
set autocommit = 0;
data branch merge t2 into t1 when conflict accept;
rollback;
set autocommit = 1;
select * from t1 order by a;

drop table t1;
drop table t2;
drop database test;
