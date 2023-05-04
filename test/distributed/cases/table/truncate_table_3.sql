drop table if exists t1;
create table t1(a int);
insert into t1 values(1),(2),(3),(4);
select count(*) from t1;

begin;
select count(*) from t1;
truncate t1;
select count(*) from t1;
show columns from t1;
create table t2(a int,b int);
--t1 t2
show tables;
insert into t2 values (1,2),(2,3);
rollback;
show tables;
select count(*) from t1;

begin;
truncate t1;
select count(*) from t1;
show columns from t1;
create table t2(a int,b int);
-- t1,t2
show tables;
insert into t2 values (1,2),(2,3);
rollback;
show tables;
select count(*) from t1;

begin;
truncate t1;
select count(*) from t1;
show columns from t1;
create table t2(a int,b int);
--t1,t2
show tables;
insert into t2 values (1,2),(2,3);
truncate t1;
truncate t1;
commit;
show tables;
select count(*) from t1;
select count(*) from t2;