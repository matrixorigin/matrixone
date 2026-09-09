--- @metacmp(false)

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
-- TRUNCATE commits the preceding transaction and starts a fresh one. The
-- table creation and following insert therefore remain after ROLLBACK.
-- @sortkey:0
show tables;
insert into t2 values (1,2),(2,3);
rollback;
-- @sortkey:0
show tables;
select count(*) from t1;

begin;
truncate t1;
select count(*) from t1;
show columns from t1;
-- t2 was committed by the previous TRUNCATE boundary; this insert runs in
-- the new transaction and remains after its ROLLBACK as well.
-- @sortkey:0
show tables;
insert into t2 values (1,2),(2,3);
rollback;
-- @sortkey:0
show tables;
select count(*) from t1;

begin;
truncate t1;
select count(*) from t1;
show columns from t1;
-- A second truncate in the same session must not resurrect the old workspace.
-- @sortkey:0
show tables;
insert into t2 values (1,2),(2,3);
truncate t1;
truncate t1;
commit;
-- @sortkey:0
show tables;
select count(*) from t1;
select count(*) from t2;
