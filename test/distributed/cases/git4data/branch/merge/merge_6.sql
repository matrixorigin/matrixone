drop database if exists test;
create database test;
use test;

-- case 6: no primary key with duplicates, nulls, and empty strings
create table t1 (a int, b int, note varchar(20));
insert into t1 values
	(1, 10, 'dup'),
	(1, 10, 'dup'),
	(1, 10, 'dup'),
	(1, 10, 'dup'),
	(2, 20, null),
	(3, 30, 'keep'),
	(4, 40, ''),
	(5, 50, 'Case');

data branch create table t2 from t1;
delete from t2 where a = 1 and b = 10 and note = 'dup' limit 1;
delete from t2 where a = 1 and b = 10 and note = 'dup' limit 1;
delete from t2 where a = 2 and b = 20 and note is null;
update t2 set note = '' where a = 3 and b = 30 and note = 'keep';
update t2 set note = 'case' where a = 5 and b = 50 and note = 'Case';
insert into t2 values (6, 60, 'new'), (7, 70, null);

data branch diff t2 against t1;
data branch merge t2 into t1;

select * from t1 order by a, b, note;
data branch diff t2 against t1;

drop table t1;
drop table t2;

-- case 6b: composite primary key variant
create table t1 (a int, b int, note varchar(20), primary key (a, b));
insert into t1 values
	(1, 10, 'dup'),
	(1, 11, 'dup'),
	(1, 12, 'dup'),
	(1, 13, 'dup'),
	(2, 20, null),
	(3, 30, 'keep'),
	(4, 40, ''),
	(5, 50, 'Case');

data branch create table t2 from t1;
delete from t2 where a = 1 and b = 10 and note = 'dup';
delete from t2 where a = 1 and b = 11 and note = 'dup';
delete from t2 where a = 2 and b = 20 and note is null;
update t2 set note = '' where a = 3 and b = 30 and note = 'keep';
update t2 set note = 'case' where a = 5 and b = 50 and note = 'Case';
insert into t2 values (6, 60, 'new'), (7, 70, null);

data branch diff t2 against t1;
data branch merge t2 into t1;

select * from t1 order by a, b, note;
data branch diff t2 against t1;

drop table t1;
drop table t2;
drop database test;
