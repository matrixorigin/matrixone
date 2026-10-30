-- ENUM/SET DDL validation: a constant DEFAULT must be a member (ER_INVALID_DEFAULT 1067)
-- and a definition must not list the same member twice (ER_DUPLICATED_VALUE_IN_TYPE 1291).
drop database if exists enum_set_ddl_validation;
create database enum_set_ddl_validation;
use enum_set_ddl_validation;
set @saved_sql_mode = @@sql_mode;
set sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- non-member constant defaults are rejected at CREATE TABLE
create table t_bad1 (id int, e enum('a','b') default 'zz');
create table t_bad2 (id int, s set('a','b') default 'c');
create table t_bad3 (id int, s set('a','b') default 'a,c');
create table t_bad4 (id int, e enum('a','b') default '');
create table t_bad5 (id int, e enum('a','b') default 3);
create table t_bad6 (id int, s set('a','b') default 4);
create table t_bad7 (id int, e enum('a','b') not null default 'c');

-- members, case-insensitive and trailing-space spellings, NULL and expressions are accepted
create table t_ok (
  id int,
  e1 enum('a','b') default 'b',
  e2 enum('a','b') default 'A',
  e3 enum('a','b') default 'a  ',
  e4 enum('a','b') default null,
  e5 enum('','a') default '',
  s1 set('a','b') default 'b,a',
  s2 set('a','b') default ''
);
insert into t_ok(id) values (1);
select id, e1, e2, e3, e4, e5, s1, s2 from t_ok;

-- ALTER TABLE paths
create table t_alt (id int, e enum('a','b') default 'a', s set('x','y') default 'x');
alter table t_alt alter column e set default 'zz';
alter table t_alt alter column s set default 'z';
alter table t_alt modify column e enum('a','b') default 'q';
alter table t_alt change column e e2 enum('a','b') default 'q';
alter table t_alt add column e3 enum('x','y') default 'z';
alter table t_alt alter column e set default 'b';
alter table t_alt add column e4 enum('x','y') default 'y';
insert into t_alt(id) values (1);
select * from t_alt;
show create table t_alt;

-- duplicate members
create table t_dup1 (id int, e enum('a','a'));
create table t_dup2 (id int, e enum('a','b','A'));
create table t_dup4 (id int, s set('a','b','a'));
create table t_dup5 (id int, s set('x','X'));
create table t_dup_ok (id int, e enum('a','A') collate utf8mb4_bin, e2 enum('a','ab',' a'));
alter table t_dup_ok add column e3 enum('p','q','p');
alter table t_dup_ok modify column e2 enum('a','ab','a');
insert into t_dup_ok values (1, 'A', ' a');
select id, e2 from t_dup_ok;

-- non-strict sql_mode: like MySQL, a duplicate ENUM member is only a 1291
-- diagnostic; SET duplicates stay an error because MO cannot store them
set sql_mode = '';
create table t_ns_dup (id int, e enum('a','b','A'));
show warnings;
insert into t_ns_dup values (1, 'b');
select * from t_ns_dup;
create table t_ns_dup_set (id int, s set('a','a'));
create table t_ns_bad (id int, e enum('a','b') default 'zz');
set sql_mode = @saved_sql_mode;

drop database enum_set_ddl_validation;
