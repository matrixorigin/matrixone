drop table if exists t1;
create table t1(a int);
show tables;

begin;
show tables;
show columns from t1;
drop table t1;
show tables;
create table t1(a int,b int);
show tables;
show columns from t1;
create table t2(a int);
show tables;
show columns from t2;
rollback;
show tables;
show columns from t1;

begin;
show tables;
show columns from t1;
drop table t1;
show tables;
create table t1(a int, b int);
show tables;
show columns from t1;
create table t2(a int);
show tables;
commit;
show tables;

drop table if exists t1;
drop table if exists t2;
