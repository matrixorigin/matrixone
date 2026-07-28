-- @suite
-- @case
-- @desc: regression test for #24408 — chained RENAME TABLE (MySQL atomic swap pattern)

drop database if exists test_rename_chain;
create database test_rename_chain;
use test_rename_chain;

-- 3-pair atomic swap
create table t_live (id int);
create table t_shadow (id int);
insert into t_live values (100);
insert into t_shadow values (200);

rename table t_live to t_tmp, t_shadow to t_live, t_tmp to t_shadow;

select * from t_live;
select * from t_shadow;

-- 2-pair rename (non-conflicting)
drop table t_live, t_shadow;
create table a (id int);
create table b (id int);
insert into a values (1);
insert into b values (2);

rename table a to c, b to a;

select * from a;
select * from c;

-- real conflict should still error
drop table a, c;
create table x (id int);
create table y (id int);
rename table x to y;

-- double-rename same source should error
drop table if exists x, y;
create table m (id int);
create table n (id int);
rename table m to xx, m to yy;

drop database test_rename_chain;
