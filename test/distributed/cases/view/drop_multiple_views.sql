-- DROP VIEW with a list of views (#31910)
drop database if exists drop_multi_view_db;
drop database if exists drop_multi_view_db2;
create database drop_multi_view_db;
create database drop_multi_view_db2;
use drop_multi_view_db;
create table t (a int);
insert into t values (1), (2);
create view v1 as select a from t;
create view v2 as select a from t;
create view v3 as select a + 1 as b from t;
drop view v1, v2;
show full tables;
-- IF EXISTS: missing views are skipped, a same-named base table is untouched
drop view if exists v3, v_missing, t;
show full tables;
select * from t order by a;
-- without IF EXISTS the whole statement fails and nothing is dropped
create view v1 as select a from t;
drop view v1, v_missing;
drop view v1, t;
show full tables;
select * from v1 order by a;
-- views in different databases
create table drop_multi_view_db2.t2 (a int);
create view drop_multi_view_db2.w as select a from drop_multi_view_db2.t2;
drop view drop_multi_view_db.v1, drop_multi_view_db2.w;
show full tables;
show full tables from drop_multi_view_db2;
drop database drop_multi_view_db;
drop database drop_multi_view_db2;
