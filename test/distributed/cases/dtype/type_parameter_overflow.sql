-- Oversized type arguments must fail before they can wrap into AST widths.
drop database if exists type_parameter_overflow;
create database type_parameter_overflow;
use type_parameter_overflow;

create table rejected (a varchar(4294967296));
create table rejected (a varchar(4294967297));
create table rejected (a varchar(2147483648));
create table rejected (a char(4294967297));
create table rejected (a binary(4294967297));
create table rejected (a varbinary(4294967297));
create table rejected (a bit(4294967297));
create table rejected (a int(4294967297));
create table rejected (a float(4294967297));
create table rejected (a double(4294967297,0));
create table rejected (a decimal(4294967297));
create table rejected (a decimal(4294967297,0));
create table rejected (a float(10,4294967297));
create table rejected (a double(10,4294967295));
create table rejected (a decimal(10,4294967297));
create table rejected (a timestamp(4294967297));
show tables like 'rejected';

-- Rejected ALTER statements must preserve the original schema and data.
create table retained (a varchar(10));
insert into retained values ('abcdefghij');
alter table retained add column b varchar(4294967297);
alter table retained modify column a varchar(4294967297);
alter table retained change column a renamed varchar(4294967297);
alter table retained modify column a varchar(4294967297), add column b int;
show create table retained;
select a from retained;

-- CAST and prepared SQL share the guarded parameter productions.
select cast('abc' as char(4294967297));
select cast(1 as decimal(10,4294967297));
prepare rejected_stmt from 'create table rejected (a varchar(4294967297))';
show tables like 'rejected';
select cast('abc' as char(3));

-- Valid zero/default/explicit parameters still create usable columns.
create table valid_parameters (v varchar(10), d decimal, ts timestamp(0));
insert into valid_parameters values ('abcdefghij', 12, '2026-01-01 12:34:56');
select v, d, ts from valid_parameters;

use mo_catalog;
drop database type_parameter_overflow;
