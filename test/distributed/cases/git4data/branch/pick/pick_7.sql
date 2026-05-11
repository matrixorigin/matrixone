drop database if exists test;
create database test;
use test;

-- ================================================================
-- PICK Test 7: Multiple sequential picks (incremental cherry-pick)
-- ================================================================

-- Simulate a real workflow: feature branch with multiple changes,
-- cherry-pick them one by one into main

create table main_tbl (a int, b varchar(20), primary key(a));
insert into main_tbl values (1,'init'),(2,'init'),(3,'init');

data branch create table feature_tbl from main_tbl;

-- feature branch makes multiple independent changes
insert into feature_tbl values (4,'feature-a');
insert into feature_tbl values (5,'feature-b');
insert into feature_tbl values (6,'feature-c');
update feature_tbl set b = 'fix-1' where a = 1;
update feature_tbl set b = 'fix-2' where a = 2;

-- check all changes
data branch diff feature_tbl against main_tbl;

-- cherry-pick feature-a first
data branch pick feature_tbl into main_tbl keys(4);
select * from main_tbl order by a asc;

-- cherry-pick the fix for pk=1
data branch pick feature_tbl into main_tbl keys(1);
select * from main_tbl order by a asc;

-- cherry-pick feature-c (skip feature-b)
data branch pick feature_tbl into main_tbl keys(6);
select * from main_tbl order by a asc;

-- check remaining diff: should show pk=2(fix-2) and pk=5(feature-b)
data branch diff feature_tbl against main_tbl;

-- pick the rest
data branch pick feature_tbl into main_tbl keys(2,5);
select * from main_tbl order by a asc;

-- diff should be empty now
data branch diff feature_tbl against main_tbl;

drop table main_tbl;
drop table feature_tbl;

drop database test;
