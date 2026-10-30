-- A literal DEFAULT that cannot be converted to the column type is
-- ER_INVALID_DEFAULT (1067) in CREATE TABLE and every ALTER TABLE form.
drop database if exists default_literal_cast;
create database default_literal_cast;
use default_literal_cast;

create table t_bad1 (id int, c int default 'abc');
create table t_bad2 (id int, c int default '12x');
create table t_bad3 (id int, c int default 99999999999);
create table t_bad4 (id int, c tinyint default 300);
create table t_bad5 (id int, c tinyint unsigned default -1);
create table t_bad6 (id int, c date default 'abc');
create table t_bad7 (id int, c datetime default 'not a time');
create table t_bad8 (id int, c decimal(5,2) default 'abc');

create table t (id int, c int default 1, d date default '2026-10-30');
alter table t alter column c set default 'abc';
alter table t alter column c set default '12x';
alter table t alter column c set default 99999999999;
alter table t alter column d set default 'abc';
alter table t modify column c int default 'abc';
alter table t change column c c2 int default 'abc';
alter table t add column c3 tinyint default 300;

-- valid literals keep working
alter table t alter column c set default '42';
alter table t add column c4 tinyint default -128;
insert into t(id) values (1);
select * from t;

drop database default_literal_cast;
